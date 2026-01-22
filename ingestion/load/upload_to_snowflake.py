import os
import argparse
import snowflake.connector
import yaml
import glob

def get_credentials(profiles_dir, profile_name, target_name=None):
    profiles_path = os.path.join(profiles_dir, 'profiles.yml')
    if not os.path.exists(profiles_path):
        raise FileNotFoundError(f"profiles.yml not found at {profiles_path}")

    with open(profiles_path, 'r') as f:
        profiles = yaml.safe_load(f)
    
    profile = profiles.get(profile_name)
    if not profile:
        raise ValueError(f"Profile {profile_name} not found in {profiles_path}")
    
    if target_name is None:
        target_name = profile.get('target')
    
    target = profile['outputs'].get(target_name)
    if not target:
        raise ValueError(f"Target {target_name} not found in profile {profile_name}")
    
    return target

def upload_data(date, profiles_dir='~/.dbt', profile='sleeplens', target=None):
    # Expand user path
    profiles_dir = os.path.expanduser(profiles_dir)
    creds = get_credentials(profiles_dir, profile, target)
    
    # Connect
    account = creds['account']
    if '#' in account:
        account = account.split('#')[0]
    account = account.strip()
    
    # Check if host is explicitly provided in profile
    host = creds.get('host')
    if not host:
        # Fallback: append suffix if not present
        if '.' not in account:
             host = f"{account}.snowflakecomputing.com"
        else:
             host = account

    print(f"Connecting to Snowflake: Account={account}, Host={host}, User={creds['user']}")
    conn = snowflake.connector.connect(
        user=creds['user'],
        password=creds['password'],
        account=account,
        host=host,
        warehouse=creds['warehouse'],
        database=creds['database'],
        schema=creds['schema'],
        role=creds.get('role')
    )
    cursor = conn.cursor()
    
    try:
        # Set context explicitly
        print(f"Using Database: {creds['database']}, Warehouse: {creds['warehouse']}")
        cursor.execute(f"USE WAREHOUSE {creds['warehouse']}")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {creds['database']}")
        cursor.execute(f"USE DATABASE {creds['database']}")
        
        # Create Schema
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {creds['database']}.RAW")
        cursor.execute(f"USE SCHEMA {creds['database']}.RAW")
        
        # Create Tables
        print("Creating tables if not exist...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS SLEEP_STUDIES (
            study_id STRING,
            study_date DATE,
            total_sleep_time_min FLOAT,
            rem_min FLOAT,
            deep_min FLOAT,
            light_min FLOAT,
            age_group STRING,
            sex STRING
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS EVENTS (
            study_id STRING,
            event_type STRING,
            duration_sec FLOAT,
            study_datetime TIMESTAMP
        )
        """)
        
        # Create Named File Format (Robustness Fix)
        cursor.execute("""
        CREATE FILE FORMAT IF NOT EXISTS SLEEPLENS_CSV
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        """)

        # Define base path
        base_path = "/app/data/raw"
        
        # Upload Sleep Studies
        studies_path = f"{base_path}/sleep_studies/dt={date}/*.csv"
        files = glob.glob(studies_path)
        if files:
            print(f"Found {len(files)} sleep study files.")
            for file_path in files:
                print(f"Uploading {file_path}...")
                # PUT file to stage
                cursor.execute(f"PUT file://{file_path} @%SLEEP_STUDIES AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
                # COPY INTO using Named Format
                # Spark writes columns alphabetically by default, so we must map them explicitly
                # CSV: age_group, deep_min, light_min, rem_min, sex, study_date, study_id, total_sleep_time_min
                cursor.execute("""
                COPY INTO SLEEP_STUDIES 
                (age_group, deep_min, light_min, rem_min, sex, study_date, study_id, total_sleep_time_min)
                FROM @%SLEEP_STUDIES
                FILE_FORMAT = (FORMAT_NAME = 'SLEEPLENS_CSV')
                PURGE = TRUE
                """)
                print("Loaded into SLEEP_STUDIES.")
        else:
            print(f"No sleep study files found for {date}")

        # Upload Events
        events_path = f"{base_path}/events/dt={date}/*.csv"
        files = glob.glob(events_path)
        if files:
            print(f"Found {len(files)} event files.")
            for file_path in files:
                print(f"Uploading {file_path}...")
                cursor.execute(f"PUT file://{file_path} @%EVENTS AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
                
                # CSV: duration_sec, event_type, study_datetime, study_id
                cursor.execute("""
                COPY INTO EVENTS 
                (duration_sec, event_type, study_datetime, study_id)
                FROM @%EVENTS
                FILE_FORMAT = (FORMAT_NAME = 'SLEEPLENS_CSV')
                PURGE = TRUE
                """)
                print("Loaded into EVENTS.")
        else:
            print(f"No event files found for {date}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--profiles-dir", default="/root/.dbt")
    args = parser.parse_args()
    
    upload_data(args.date, profiles_dir=args.profiles_dir)
