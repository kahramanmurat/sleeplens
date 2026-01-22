import streamlit as st
import pandas as pd
import snowflake.connector
import yaml
import os

# Page Config
st.set_page_config(page_title="SleepLens Dashboard", layout="wide")

# Function to load credentials
def get_snowflake_creds(profile_path="transformations/dbt/profiles.yml"):
    if not os.path.exists(profile_path):
        st.error(f"Profile not found at {profile_path}")
        return None
        
    with open(profile_path, 'r') as f:
        profiles = yaml.safe_load(f)
    
    # Assuming 'sleeplens' profile and 'dev' target
    creds = profiles['sleeplens']['outputs']['dev']
    return creds

# Function to load data from Snowflake
@st.cache_data
def load_data_from_snowflake():
    creds = get_snowflake_creds()
    if not creds:
        return None, None
    
    # Handle account/host connection logic similar to ingestion script
    account = creds['account']
    if '#' in account:
        account = account.split('#')[0].strip()
    
    host = creds.get('host')
    if not host:
         if '.' not in account:
             host = f"{account}.snowflakecomputing.com"
         else:
             host = account

    try:
        conn = snowflake.connector.connect(
            user=creds['user'],
            password=creds['password'],
            account=account,
            host=host,
            warehouse=creds['warehouse'],
            database=creds['database'],
            role=creds.get('role')
        )
        
        # Unified Query: Join Summary and Events
        # Using LEFT JOIN to ensure we keep studies even if they have no events (assume 0 events)
        query = f"""
            SELECT 
                s.STUDY_ID, 
                s.STUDY_DATE,
                s.TOTAL_SLEEP_TIME_MIN, 
                s.REM_PERCENTAGE, 
                s.DEEP_MIN, 
                s.LIGHT_MIN,
                s.AGE_GROUP,
                s.SEX,
                ZEROIFNULL(e.TOTAL_EVENTS) as TOTAL_EVENTS, 
                ZEROIFNULL(e.APNEA_COUNT) as APNEA_COUNT, 
                ZEROIFNULL(e.HYPOPNEA_COUNT) as HYPOPNEA_COUNT, 
                ZEROIFNULL(e.AROUSAL_COUNT) as AROUSAL_COUNT,
                e.AVG_EVENT_DURATION_SEC
            FROM {creds['database']}.RAW_ANALYTICS.FCT_SLEEP_SUMMARY s
            LEFT JOIN {creds['database']}.RAW_ANALYTICS.FCT_EVENT_RATES e
                ON s.STUDY_ID = e.STUDY_ID
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        # --- Derived Metrics ---
        # AHI = (Apneas + Hypopneas) / (Total Sleep Time Hours)
        df['AHI'] = (df['APNEA_COUNT'] + df['HYPOPNEA_COUNT']) / (df['TOTAL_SLEEP_TIME_MIN'] / 60)
        
        # Severity Category
        def categorize_ahi(ahi):
            if ahi < 5: return 'Normal'
            elif ahi < 15: return 'Mild'
            elif ahi < 30: return 'Moderate'
            else: return 'Severe'
        
        df['AHI_Severity'] = df['AHI'].apply(categorize_ahi)
        
        return df
        
    except Exception as e:
        st.error(f"Snowflake Connection Error: {e}")
        return None

st.title("SleepLens Clinical Dashboard 🩺")

df = load_data_from_snowflake()

if df is not None:
    # --- Sidebar Filters ---
    st.sidebar.title("📊 Dataset Stats")
    
    # Global Metrics
    global_start_date = df['STUDY_DATE'].min()
    global_end_date = df['STUDY_DATE'].max()
    global_patients = df['STUDY_ID'].nunique()
    
    st.sidebar.markdown(f"**Unique Patients:** {global_patients}")
    st.sidebar.markdown(f"**Data Range:**")
    st.sidebar.caption(f"{global_start_date} to {global_end_date}")
    st.sidebar.markdown("---")

    st.sidebar.header("Cohort Filter")
    
    # Date Range Filter
    # Ensure STUDY_DATE is datetime for comparison
    df['STUDY_DATE'] = pd.to_datetime(df['STUDY_DATE']).dt.date
    
    min_date = df['STUDY_DATE'].min()
    max_date = df['STUDY_DATE'].max()
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    selected_sex = st.sidebar.multiselect("Sex", df['SEX'].unique(), default=df['SEX'].unique())
    selected_age = st.sidebar.multiselect("Age Group", df['AGE_GROUP'].unique(), default=df['AGE_GROUP'].unique())
    
    # Filter Data
    if len(date_range) == 2:
        start_date, end_date = date_range
        mask = (df['STUDY_DATE'] >= start_date) & (df['STUDY_DATE'] <= end_date) & \
               (df['SEX'].isin(selected_sex)) & \
               (df['AGE_GROUP'].isin(selected_age))
    else:
        mask = (df['SEX'].isin(selected_sex)) & (df['AGE_GROUP'].isin(selected_age))

    df_filtered = df[mask]
    
    # --- Tabs ---
    tab1, tab2, tab3, tab4 = st.tabs(["🏥 Overview", "📊 Population Health", "👤 Patient Details", "🧠 Spectral Analysis"])
    
    # --- Tab 1: Overview ---
    with tab1:
        col1, col2, col3, col4 = st.columns(4)
        
        total_studies = len(df_filtered)
        avg_ahi = df_filtered['AHI'].mean()
        severe_percent = (len(df_filtered[df_filtered['AHI_Severity'] == 'Severe']) / total_studies) * 100 if total_studies > 0 else 0
        avg_sleep = df_filtered['TOTAL_SLEEP_TIME_MIN'].mean() / 60
        
        col1.metric("Total Sleep Studies", total_studies)
        col2.metric("Avg AHI", f"{avg_ahi:.1f}")
        col3.metric("Severe Apnea %", f"{severe_percent:.1f}%")
        col4.metric("Avg Sleep Time (hrs)", f"{avg_sleep:.1f}")
        
        st.divider()
        
        # Sleep Architecture
        st.subheader("Cohor Sleep Architecture")
        # Average time in each stage
        avg_rem = df_filtered['REM_PERCENTAGE'].mean()
        # Estimate percentages for others derived from mins (approximate for vis)
        avg_deep_min = df_filtered['DEEP_MIN'].mean()
        avg_light_min = df_filtered['LIGHT_MIN'].mean()
        avg_total_min = df_filtered['TOTAL_SLEEP_TIME_MIN'].mean()
        
        if avg_total_min > 0:
            stage_data = pd.DataFrame({
                'Stage': ['REM', 'Deep', 'Light'],
                'Percentage': [
                    avg_rem, 
                    (avg_deep_min/avg_total_min)*100, 
                    (avg_light_min/avg_total_min)*100
                ]
            })
            st.bar_chart(stage_data.set_index('Stage'))
        else:
            st.info("No data for sleep architecture.")

    # --- Tab 2: Population Health ---
    with tab2:
        col_p1, col_p2 = st.columns(2)
        
        with col_p1:
            st.subheader("AHI Severity Distribution")
            severity_counts = df_filtered['AHI_Severity'].value_counts()
            st.bar_chart(severity_counts)
            
        with col_p2:
            st.subheader("Age Group vs Average AHI")
            age_ahi = df_filtered.groupby('AGE_GROUP')['AHI'].mean()
            st.bar_chart(age_ahi)
            
        st.subheader("Clinical Correlation: AHI vs Sleep Duration")
        st.scatter_chart(
            df_filtered,
            x='TOTAL_SLEEP_TIME_MIN',
            y='AHI',
            color='AGE_GROUP',
            size='AROUSAL_COUNT'
        )

    # --- Tab 3: Patient Details ---
    with tab3:
        st.subheader("Patient Drill-Down")
        
        # Patient Selector (searchable)
        patient_id = st.selectbox("Select Patient ID", df_filtered['STUDY_ID'].unique())
        
        if patient_id:
            pt_data = df_filtered[df_filtered['STUDY_ID'] == patient_id].iloc[0]
            
            # Scorecard
            pc1, pc2, pc3 = st.columns(3)
            
            severity_color = {
                "Normal": "green",
                "Mild": "blue", 
                "Moderate": "orange",
                "Severe": "red"
            }
            
            pc1.markdown(f"**AHI Score**: :QA-color[{pt_data['AHI']:.1f}]")
            pc1.caption(f"Severity: {pt_data['AHI_Severity']}")
            
            pc2.metric("Total Events", int(pt_data['TOTAL_EVENTS']))
            pc2.metric("Arousals", int(pt_data['AROUSAL_COUNT']))
            
            pc3.metric("Sleep Duration", f"{pt_data['TOTAL_SLEEP_TIME_MIN']/60:.1f} hrs")
            
            st.divider()
            
            # Detailed Breakdown vs Cohort
            st.markdown("#### Comparison to Cohort Average")
            
            comparison_df = pd.DataFrame({
                "Metric": ["AHI", "REM %", "Deep Sleep (min)"],
                "Patient": [pt_data['AHI'], pt_data['REM_PERCENTAGE'], pt_data['DEEP_MIN']],
                "Cohort Avg": [avg_ahi, avg_rem, avg_deep_min]
            })
            st.dataframe(comparison_df.set_index("Metric"))

    # --- Tab 4: Spectral Analysis (New) ---
    with tab4:
        st.subheader("🔬 EEG Spectral Analysis (PhysioNet Data)")
        
        # Fetch signal analytics
        @st.cache_data
        def load_signal_data():
            creds = get_snowflake_creds()
            if not creds: return None
            
            try:
                conn = snowflake.connector.connect(
                    user=creds['user'],
                    password=creds['password'],
                    account=creds['account'].split('#')[0].strip(), # Quick fix reuse logic
                    host=creds.get('host'),
                    warehouse=creds['warehouse'],
                    database=creds['database'],
                    role=creds.get('role')
                )
                query = f"""
                    SELECT * FROM {creds['database']}.RAW_ANALYTICS.FCT_SIGNAL_ANALYTICS
                """
                df_sig = pd.read_sql(query, conn)
                conn.close()
                return df_sig
            except:
                return None

        df_signal = load_signal_data()
        
        if df_signal is not None and not df_signal.empty:
            # Filter by Subject
            # For PhysioNet we might only have PHYS_000 for now
            subjects = df_signal['SUBJECT_ID'].unique()
            sel_subj = st.selectbox("Select Research Subject", subjects)
            
            subj_data = df_signal[df_signal['SUBJECT_ID'] == sel_subj]
            
            st.markdown("### 1. Global Spectral Power Distribution")
            st.caption("Which brainwaves dominate each sleep stage?")
            
            # Prepare for bar chart: Stacked bands per stage
            # Melt df
            bands = ['AVG_DELTA_REL', 'AVG_THETA_REL', 'AVG_ALPHA_REL', 'AVG_BETA_REL', 'AVG_GAMMA_REL']
            chart_data = subj_data.melt(id_vars=['SLEEP_STAGE'], value_vars=bands, var_name='Band', value_name='Relative Power')
            
            # Clean names
            chart_data['Band'] = chart_data['Band'].str.replace('AVG_', '').str.replace('_REL', '')
            
            st.vega_lite_chart(chart_data, {
                'mark': 'bar',
                'encoding': {
                    'x': {'field': 'SLEEP_STAGE', 'type': 'nominal', 'axis': {'labelAngle': 0}},
                    'y': {'field': 'Relative Power', 'type': 'quantitative', 'stack': 'normalize'},
                    'color': {'field': 'Band', 'type': 'nominal'},
                    'tooltip': ['SLEEP_STAGE', 'Band', 'Relative Power']
                }
            }, use_container_width=True)
            
            st.divider()
            
            # --- Temporal Analysis (Drill Down) ---
            st.markdown("### 2. Sleep Architecture (Hypnogram) & Trends")
            
            # Fetch raw epoch data for this subject from Staging (epoch-level)
            @st.cache_data
            def load_epoch_data(subject_id):
                creds = get_snowflake_creds()
                if not creds: return None
                try:
                    conn = snowflake.connector.connect(
                        user=creds['user'],
                        password=creds['password'],
                        account=creds['account'].split('#')[0].strip(),
                        host=creds.get('host'),
                        warehouse=creds['warehouse'],
                        database=creds['database'],
                        role=creds.get('role')
                    )
                    # Query Staging for detailed time-series
                    query = f"""
                        SELECT EPOCH_ID, SLEEP_STAGE, DELTA_REL, TIMESTAMP
                        FROM {creds['database']}.RAW_STAGING.STG_SIGNAL_FEATURES
                        WHERE SUBJECT_ID = '{subject_id}'
                        ORDER BY EPOCH_ID ASC
                    """
                    df_epoch = pd.read_sql(query, conn)
                    conn.close()
                    return df_epoch
                except Exception as e:
                    st.error(f"Error loading epoch data: {e}")
                    return None

            df_epochs = load_epoch_data(sel_subj)
            
            if df_epochs is not None and not df_epochs.empty:
                # Ordering Sleep Stages for Y-Axis
                stage_order = ["Sleep stage W", "Sleep stage R", "Sleep stage 1", "Sleep stage 2", "Sleep stage 3", "Sleep stage 4"]
                
                # Chart 1: Hypnogram
                st.markdown("**Hypnogram**: Progression of sleep stages through the night.")
                st.vega_lite_chart(df_epochs, {
                    'mark': {'type': 'line', 'interpolate': 'step-after'},
                    'encoding': {
                        'x': {'field': 'EPOCH_ID', 'type': 'quantitative', 'title': 'Epoch (Time)'},
                        'y': {'field': 'SLEEP_STAGE', 'type': 'ordinal', 'sort': stage_order, 'title': 'Stage'},
                        'color': {'field': 'SLEEP_STAGE', 'type': 'nominal'},
                        'tooltip': ['EPOCH_ID', 'SLEEP_STAGE', 'TIMESTAMP']
                    },
                    'height': 200
                }, use_container_width=True)
                
                # Chart 2: Delta Power Trend
                st.markdown("**Delta Power Trend**: Deep sleep intensity (0.5-4Hz).")
                st.caption("Spikes in Delta power correspond to restorative deep sleep.")
                st.line_chart(df_epochs.set_index('EPOCH_ID')['DELTA_REL'], color='#FF4B4B') # Red for power
                
            else:
                st.info("Detailed temporal data not available for this subject.")

        else:
            st.warning("No spectral data found. Ensure 'fetch_physionet.py' pipeline has run.")

else:
    st.warning("Could not load data from Snowflake. Check your credentials and ensure the pipeline has run.")


