from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when
import argparse

def process_bronze_to_silver(spark, input_path, output_path):
    """
    Reads bronze data, normalizes event types, and writes to silver.
    """
    print(f"Processing Bronze Data from {input_path}")
    
    try:
        df = spark.read.option("header", "true").csv(input_path) # Assuming CSV for now, could be Parquet in real lake
    except Exception as e:
        print(f"Data not found at {input_path}, skipping. Error: {e}")
        return

    # Normalize event types
    # Map rare event types or typos to 'other' if needed, or strictly keep valid ones
    df_clean = (
        df.withColumn("event_type_norm",
            when(col("event_type").isin("apnea", "hypopnea", "arousal"), col("event_type"))
            .otherwise("other")
        )
        # Type casting
        .withColumn("duration_sec", col("duration_sec").cast("float"))
        .withColumn("study_datetime", col("study_datetime").cast("timestamp"))
        .withColumn("event_date", to_date(col("study_datetime")))
    )

    print(f"Writing Silver Data to {output_path}")
    df_clean.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--bucket", default="sleeplens-data-lake")
    parser.add_argument("--local_data", action="store_true", help="Use local file system paths")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("SleepLens-BronzeToSilver").getOrCreate()
    
    if args.local_data:
        # Local paths (mapped specific to the container mount point)
        base_path = "/app/data"
        bronze_path = f"{base_path}/raw/events/dt={args.date}/"
        silver_path = f"{base_path}/silver/events/dt={args.date}/"
    else:
        # S3 paths
        bronze_path = f"s3a://{args.bucket}/raw/events/dt={args.date}/" 
        silver_path = f"s3a://{args.bucket}/silver/events/dt={args.date}/"
    
    process_bronze_to_silver(spark, bronze_path, silver_path)
