-- Copy into Raw Sleep Summary
COPY INTO RAW.raw_sleep_studies
FROM @sleeplens_s3_stage/raw/dt={{ run_date }}/
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
PATTERN = '.*sleep_summary.*';

-- Copy into Raw Events
COPY INTO RAW.raw_events
FROM @sleeplens_s3_stage/raw/dt={{ run_date }}/
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
PATTERN = '.*events.*';
