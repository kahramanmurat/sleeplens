import pandas as pd
import numpy as np
import argparse
import os
from datetime import datetime, timedelta

def generate_synthetic_data(date_str, output_dir):
    """Generates synthetic sleep study and event data."""
    print(f"Generating synthetic data for date: {date_str}")
    
    # Constants
    num_studies = 50
    study_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    
    # Generate Sleep Summaries
    study_ids = [f"STUDY_{i:04d}" for i in range(num_studies)]
    
    summaries = []
    events = []
    
    for study_id in study_ids:
        # Sleep Summary Data
        total_sleep_time_min = np.random.normal(380, 60) # ~6.3 hours mean
        rem_min = total_sleep_time_min * np.random.uniform(0.15, 0.25)
        deep_min = total_sleep_time_min * np.random.uniform(0.10, 0.20)
        light_min = total_sleep_time_min - rem_min - deep_min
        
        summaries.append({
            "study_id": study_id,
            "study_date": study_date,
            "total_sleep_time_min": round(total_sleep_time_min, 1),
            "rem_min": round(rem_min, 1),
            "deep_min": round(deep_min, 1),
            "light_min": round(light_min, 1),
            "age_group": np.random.choice(["18-30", "31-50", "51-70", "71+"]),
            "sex": np.random.choice(["M", "F"])
        })
        
        # Generate Events (Apnea/Hypopnea)
        num_events = int(np.random.exponential(15)) # Some have many, some few
        for _ in range(num_events):
            event_type = np.random.choice(["apnea", "hypopnea", "arousal"], p=[0.3, 0.4, 0.3])
            duration_sec = np.random.uniform(10, 60)
            events.append({
                "study_id": study_id,
                "event_type": event_type,
                "duration_sec": round(duration_sec, 1),
                "study_datetime": datetime.combine(study_date, datetime.min.time()) + timedelta(hours=np.random.randint(22, 30)) # Late night to next morning
            })
            
    # Create DataFrames
    df_summary = pd.DataFrame(summaries)
    df_events = pd.DataFrame(events)
    
    # Save to CSV with partition structure
    # Structure: 
    # output_dir/sleep_studies/dt=YYYY-MM-DD/filename.csv
    # output_dir/events/dt=YYYY-MM-DD/filename.csv
    
    studies_partition_dir = os.path.join(output_dir, "sleep_studies", f"dt={date_str}")
    events_partition_dir = os.path.join(output_dir, "events", f"dt={date_str}")
    
    os.makedirs(studies_partition_dir, exist_ok=True)
    os.makedirs(events_partition_dir, exist_ok=True)
    
    summary_path = os.path.join(studies_partition_dir, f"sleep_summary_{date_str}.csv")
    events_path = os.path.join(events_partition_dir, f"events_{date_str}.csv")
    
    df_summary.to_csv(summary_path, index=False)
    df_events.to_csv(events_path, index=False)
    
    print(f"Saved synthetic data to {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Public/Synthetic Sleep Data")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--output_dir", type=str, default="data/raw", help="Output directory")
    
    args = parser.parse_args()
    generate_synthetic_data(args.date, args.output_dir)
