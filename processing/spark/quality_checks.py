from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse
import sys

def run_quality_checks(spark, input_path):
    """
    Runs basic quality checks on the data.
    """
    print(f"Running quality checks on {input_path}")
    
    try:
        df = spark.read.parquet(input_path)
    except:
        # Fallback to CSV if parquet fails (e.g. testing raw)
        try:
             df = spark.read.option("header", "true").csv(input_path)
        except Exception as e:
             print(f"Could not read data: {e}")
             return False

    # Check 1: No null study_ids
    null_studies = df.filter(col("study_id").isNull()).count()
    if null_studies > 0:
        print(f"FAILED: Found {null_studies} records with null study_id")
        return False
        
    # Check 2: Event duration positive (for event data)
    if "duration_sec" in df.columns:
        invalid_duration = df.filter(col("duration_sec") <= 0).count()
        if invalid_duration > 0:
            print(f"FAILED: Found {invalid_duration} events with non-positive duration")
            return False

    print("PASSED: All quality checks passed.")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("SleepLens-QualityChecks").getOrCreate()
    
    success = run_quality_checks(spark, args.path)
    if not success:
        sys.exit(1)
