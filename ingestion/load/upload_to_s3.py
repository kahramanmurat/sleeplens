import argparse
import boto3
import os
import sys

def upload_to_s3(local_dir, bucket_name, s3_prefix, date_str):
    """Uploads files from a local directory to S3."""
    print(f"Uploading from {local_dir} to s3://{bucket_name}/{s3_prefix}/dt={date_str}/")
    
    s3 = boto3.client('s3')
    
    # Check if directory exists
    if not os.path.exists(local_dir):
        print(f"Error: Directory {local_dir} does not exist.")
        return

    # Filter files relevant to the date
    files_to_upload = [f for f in os.listdir(local_dir) if date_str in f or "sample" in f]
    
    if not files_to_upload:
         print(f"No files found in {local_dir} for date {date_str}")
         return

    for file_name in files_to_upload:
        local_path = os.path.join(local_dir, file_name)
        s3_key = f"{s3_prefix}/dt={date_str}/{file_name}"
        
        try:
            print(f"Uploading {file_name} to {s3_key}...")
            # Un-comment the actual upload line when credentials are set
            # s3.upload_file(local_path, bucket_name, s3_key)
            print(f"Successfully uploaded {file_name} (simulated)")
        except Exception as e:
            print(f"Failed to upload {file_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload Data to S3")
    parser.add_argument("--local_dir", type=str, required=True, help="Local directory containing files")
    parser.add_argument("--bucket", type=str, default="sleeplens-data-lake", help="Target S3 bucket")
    parser.add_argument("--prefix", type=str, default="raw", help="Target S3 prefix")
    parser.add_argument("--date", type=str, required=True, help="Date partition (YYYY-MM-DD)")
    
    args = parser.parse_args()
    upload_to_s3(args.local_dir, args.bucket, args.prefix, args.date)
