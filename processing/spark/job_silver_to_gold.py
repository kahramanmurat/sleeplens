from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as fsum, avg, when
import argparse

def process_silver_to_gold(spark, input_path, output_path):
    """
    Reads silver data, aggregates by study, and writes to gold.
    """
    print(f"Processing Silver Data from {input_path}")
    
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Data not found at {input_path}. Error: {e}")
        return

    # Aggregate to study-level summary
    gold = (
        df.groupBy("study_id", "event_date")
        .agg(
            count("*").alias("total_events"),
            fsum(when(col("event_type_norm") == "apnea", 1).otherwise(0)).alias("apnea_count"),
            fsum(when(col("event_type_norm") == "hypopnea", 1).otherwise(0)).alias("hypopnea_count"),
            fsum(when(col("event_type_norm") == "arousal", 1).otherwise(0)).alias("arousal_count"),
            avg("duration_sec").alias("avg_event_duration_sec")
        )
    )

    print(f"Writing Gold Data to {output_path}")
    gold.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--bucket", default="sleeplens-data-lake")
    parser.add_argument("--local_data", action="store_true", help="Use local file system paths")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("SleepLens-SilverToGold").getOrCreate()
    
    if args.local_data:
        base_path = "/app/data"
        silver_path = f"{base_path}/silver/events/dt={args.date}/"
        gold_path = f"{base_path}/gold/events/dt={args.date}/"
    else:
        silver_path = f"s3a://{args.bucket}/silver/events/dt={args.date}/"
        gold_path = f"s3a://{args.bucket}/gold/events/dt={args.date}/"
    
    process_silver_to_gold(spark, silver_path, gold_path)
