CREATE SCHEMA IF NOT EXISTS ADMIN;

CREATE OR REPLACE FILE FORMAT ADMIN.csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '');

CREATE OR REPLACE FILE FORMAT ADMIN.parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';

-- Replace with actual Bucket URL and IAM credentials/Storage Integration
CREATE OR REPLACE STAGE ADMIN.sleeplens_s3_stage
  URL='s3://sleeplens-data-lake/'
  -- STORAGE_INTEGRATION = my_s3_int
  -- or
  -- CREDENTIALS=(AWS_KEY_ID='...' AWS_SECRET_KEY='...')
  FILE_FORMAT = ADMIN.csv_format;
