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
            "study_id": str(study_id),
            "study_date": study_date,
            "total_sleep_time_min": float(round(total_sleep_time_min, 1)),
            "rem_min": float(round(rem_min, 1)),
            "deep_min": float(round(deep_min, 1)),
            "light_min": float(round(light_min, 1)),
            "age_group": str(np.random.choice(["18-30", "31-50", "51-70", "71+"])),
            "sex": str(np.random.choice(["M", "F"]))
        })
        
        # Generate Events (Apnea/Hypopnea)
        num_events = int(np.random.exponential(15)) # Some have many, some few
        for _ in range(num_events):
            event_type = str(np.random.choice(["apnea", "hypopnea", "arousal"], p=[0.3, 0.4, 0.3]))
            duration_sec = float(np.random.uniform(10, 60))
            events.append({
                "study_id": str(study_id),
                "event_type": event_type,
                "duration_sec": round(duration_sec, 1),
                "study_datetime": datetime.combine(study_date, datetime.min.time()) + timedelta(hours=int(np.random.randint(22, 30))) # Late night to next morning
            })
            
    # Create Spark Session
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("SleepLensIngestion") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # Create PySpark DataFrames
    # Define simple schemas could be inferred, but explicit is safer
    df_summary = spark.createDataFrame(summaries)
    df_events = spark.createDataFrame(events)
    
    # Save to CSV using Spark
    # We write to the exact partitioning folder expected by the loader
    
    studies_partition_path = os.path.join(output_dir, "sleep_studies", f"dt={date_str}")
    events_partition_path = os.path.join(output_dir, "events", f"dt={date_str}")
    
    print(f"Writing Sleep Data via Spark to {studies_partition_path}")
    df_summary.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(studies_partition_path)
        
    print(f"Writing Event Data via Spark to {events_partition_path}")
    df_events.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(events_partition_path)
        
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Public/Synthetic Sleep Data")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--output_dir", type=str, default="data/raw", help="Output directory")
    
    args = parser.parse_args()
    generate_synthetic_data(args.date, args.output_dir)
