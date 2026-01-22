#!/bin/bash
set -e

DATE=${1:-"2026-01-21"}

echo "Running SleepLens Pipeline via Docker for date: $DATE"

# Build the docker images first
echo "Building Docker images..."
docker compose -f docker/docker-compose.yml build

# 1. Ingestion
echo "Fetching Data (in Docker)..."
docker compose -f docker/docker-compose.yml run --rm spark python3 ingestion/extract/fetch_public_summary.py --date $DATE

echo "Uploading to S3 (Simulated) (in Docker)..."
docker compose -f docker/docker-compose.yml run --rm spark python3 ingestion/load/upload_to_s3.py --local_dir data/raw --date $DATE

# 2. Processing
echo "Running Spark Jobs (in Docker)..."
docker compose -f docker/docker-compose.yml run --rm spark python3 processing/spark/job_bronze_to_silver.py --date $DATE --local_data
docker compose -f docker/docker-compose.yml run --rm spark python3 processing/spark/job_silver_to_gold.py --date $DATE --local_data

# 3. Quality Checks
echo "Running Quality Checks (in Docker)..."
docker compose -f docker/docker-compose.yml run --rm spark python3 processing/spark/quality_checks.py --path "/app/data/silver/events/dt=$DATE/"

echo "Pipeline Run Complete."
