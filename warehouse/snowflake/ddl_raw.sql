CREATE SCHEMA IF NOT EXISTS RAW;

CREATE TABLE IF NOT EXISTS RAW.raw_sleep_studies (
    study_id STRING,
    study_date DATE,
    total_sleep_time_min FLOAT,
    rem_min FLOAT,
    deep_min FLOAT,
    light_min FLOAT,
    age_group STRING,
    sex STRING,
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.raw_events (
    study_id STRING,
    event_type STRING,
    duration_sec FLOAT,
    study_datetime TIMESTAMP_NTZ,
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.raw_events_silver (
    study_id STRING,
    event_type_norm STRING,
    duration_sec FLOAT,
    study_datetime TIMESTAMP_NTZ,
    event_date DATE,
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
