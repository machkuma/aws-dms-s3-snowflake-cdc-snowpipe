CREATE OR REPLACE STORAGE INTEGRATION s3_dms_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-account-id>:role/snowflake_s3_dms_read_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://dms-s3-parquet-target-v1/');

-- DESC INTEGRATION s3_dms_int;