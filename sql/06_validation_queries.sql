## DMS → S3 Parquet notes

- Use migration type: Full load + ongoing CDC.
- S3 target prep mode: "Do nothing" to avoid DeleteObject errors unless IAM allows deletes.
- CDC batching can delay file creation; Snowpipe triggers on object create events.

-- Pipe status
SELECT SYSTEM$PIPE_STATUS('DMS_CDC_DB.RAW.ORDERS_PIPE');
SELECT SYSTEM$PIPE_STATUS('DMS_CDC_DB.RAW.CUSTOMERS_PIPE');

-- Check latest ingested rows
SELECT * FROM DMS_CDC_DB.RAW.ORDERS_RAW ORDER BY dms_ts DESC LIMIT 10;
SELECT * FROM DMS_CDC_DB.RAW.CUSTOMERS_RAW ORDER BY dms_ts DESC LIMIT 10;

-- Curated validation
SELECT * FROM DMS_CDC_DB.CURATED.FACT_ORDERS ORDER BY last_ingested_ts DESC LIMIT 10;
SELECT * FROM DMS_CDC_DB.CURATED.DIM_CUSTOMERS ORDER BY last_ingested_ts DESC LIMIT 10;