-- Generic SQL for table creation (Reference)
CREATE TABLE IF NOT EXISTS sleep_studies (
    study_id VARCHAR(50),
    study_date DATE,
    total_sleep_time_min DECIMAL(10,2),
    rem_min DECIMAL(10,2),
    deep_min DECIMAL(10,2),
    light_min DECIMAL(10,2),
    age_group VARCHAR(20),
    sex VARCHAR(1)
);

CREATE TABLE IF NOT EXISTS sleep_events (
    study_id VARCHAR(50),
    event_type VARCHAR(50),
    duration_sec DECIMAL(10,2),
    study_datetime TIMESTAMP
);
