#!/bin/bash
# Wrapper to run run_local.sh for a range of dates
START_DATE="2026-01-01"
END_DATE="2026-01-05"

current_date="$START_DATE"
while [ "$current_date" != "$END_DATE" ]; do
  ./scripts/run_local.sh $current_date
  current_date=$(date -I -v+1d -f "%Y-%m-%d" "$current_date") 
  # Note: date command syntax varies by OS (BSD vs GNU). The above is for BSD/Mac.
  # For Linux/GNU: current_date=$(date -d "$current_date + 1 day" +%Y-%m-%d)
done
