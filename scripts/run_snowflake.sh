#!/bin/bash
set -e
DATE=${1:-"2026-01-21"}

echo "Running SleepLens Snowflake Pipeline for date: $DATE"

# 1. Build
echo "Building Docker images..."
docker compose -f docker/docker-compose.yml build

# 2. Generate Data (Local Mode)
echo "Generating Data..."
docker compose -f docker/docker-compose.yml run --rm spark python3 ingestion/extract/fetch_public_summary.py --date $DATE

# 3. Upload to Snowflake
echo "Uploading to Snowflake..."
docker compose -f docker/docker-compose.yml run --rm --entrypoint python3 dbt /app/ingestion/load/upload_to_snowflake.py --date $DATE

# 4. Run dbt
echo "Running dbt transformations..."
docker compose -f docker/docker-compose.yml run --rm dbt run

# 5. Test
echo "Running dbt tests..."
docker compose -f docker/docker-compose.yml run --rm dbt test

echo "Snowflake Pipeline Complete."
