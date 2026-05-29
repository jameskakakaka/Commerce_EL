

-- Airflow will run this script every single day.
-- Takes the newly processed CSVs from S3 and loads them into our table.

COPY INTO fakeshop_transactions
-- Tell Snowflake to pull from our external stage (which Terraform will build to point to our S3 bucket)
FROM @transaction_s3_stage/processed/ 

-- We only want to load CSV files, and we want to skip the header row
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' -- Just in case our product titles have commas in them!
)

-- This ensures we don't load the same file twice. If Snowflake has seen the file before, it skips it.
ON_ERROR = 'CONTINUE'
PURGE = FALSE;