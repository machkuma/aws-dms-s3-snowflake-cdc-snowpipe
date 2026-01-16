-- Streams
CREATE OR REPLACE STREAM DMS_CDC_DB.RAW.CUSTOMERS_RAW_STM
ON TABLE DMS_CDC_DB.RAW.CUSTOMERS_RAW;

CREATE OR REPLACE STREAM DMS_CDC_DB.RAW.ORDERS_RAW_STM
ON TABLE DMS_CDC_DB.RAW.ORDERS_RAW;

-- Curated tables
CREATE OR REPLACE TABLE DMS_CDC_DB.CURATED.DIM_CUSTOMERS (
  customer_id NUMBER PRIMARY KEY,
  full_name STRING,
  email STRING,
  city STRING,
  state_cd STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  last_ingested_ts TIMESTAMP
);

CREATE OR REPLACE TABLE DMS_CDC_DB.CURATED.FACT_ORDERS (
  order_id NUMBER PRIMARY KEY,
  customer_id NUMBER,
  order_amount NUMBER(18,2),
  status STRING,
  order_ts TIMESTAMP,
  updated_at TIMESTAMP,
  last_ingested_ts TIMESTAMP
);

-- Customers task (deduped MERGE)
CREATE OR REPLACE TASK DMS_CDC_DB.CURATED.TASK_MERGE_CUSTOMERS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON */1 * * * * UTC'
WHEN SYSTEM$STREAM_HAS_DATA('DMS_CDC_DB.RAW.CUSTOMERS_RAW_STM')
AS
MERGE INTO DMS_CDC_DB.CURATED.DIM_CUSTOMERS t
USING (
  SELECT customer_id, full_name, email, city, state_cd, created_at, updated_at, ingest_ts
  FROM (
    SELECT
      customer_id, full_name, email, city, state_cd, created_at, updated_at,
      COALESCE(dms_ts, updated_at, created_at, CURRENT_TIMESTAMP()) AS ingest_ts,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COALESCE(dms_ts, updated_at, created_at) DESC) rn
    FROM DMS_CDC_DB.RAW.CUSTOMERS_RAW_STM
  )
  WHERE rn = 1
) s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET
  full_name = s.full_name,
  email = s.email,
  city = s.city,
  state_cd = s.state_cd,
  created_at = s.created_at,
  updated_at = s.updated_at,
  last_ingested_ts = s.ingest_ts
WHEN NOT MATCHED THEN INSERT (customer_id, full_name, email, city, state_cd, created_at, updated_at, last_ingested_ts)
VALUES (s.customer_id, s.full_name, s.email, s.city, s.state_cd, s.created_at, s.updated_at, s.ingest_ts);

-- Orders task (deduped MERGE)
CREATE OR REPLACE TASK DMS_CDC_DB.CURATED.TASK_MERGE_ORDERS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON */1 * * * * UTC'
WHEN SYSTEM$STREAM_HAS_DATA('DMS_CDC_DB.RAW.ORDERS_RAW_STM')
AS
MERGE INTO DMS_CDC_DB.CURATED.FACT_ORDERS t
USING (
  SELECT order_id, customer_id, order_amount, status, order_ts, updated_at, ingest_ts
  FROM (
    SELECT
      order_id, customer_id, order_amount, status, order_ts, updated_at,
      COALESCE(dms_ts, updated_at, order_ts, CURRENT_TIMESTAMP()) AS ingest_ts,
      ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COALESCE(dms_ts, updated_at, order_ts) DESC) rn
    FROM DMS_CDC_DB.RAW.ORDERS_RAW_STM
  )
  WHERE rn = 1
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  customer_id = s.customer_id,
  order_amount = s.order_amount,
  status = s.status,
  order_ts = s.order_ts,
  updated_at = s.updated_at,
  last_ingested_ts = s.ingest_ts
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_amount, status, order_ts, updated_at, last_ingested_ts)
VALUES (s.order_id, s.customer_id, s.order_amount, s.status, s.order_ts, s.updated_at, s.ingest_ts);

-- Start tasks
ALTER TASK DMS_CDC_DB.CURATED.TASK_MERGE_CUSTOMERS RESUME;
ALTER TASK DMS_CDC_DB.CURATED.TASK_MERGE_ORDERS RESUME;