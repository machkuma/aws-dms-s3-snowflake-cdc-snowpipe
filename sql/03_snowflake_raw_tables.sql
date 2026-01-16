CREATE OR REPLACE TABLE DMS_CDC_DB.RAW.CUSTOMERS_RAW (
  customer_id NUMBER,
  full_name STRING,
  email STRING,
  city STRING,
  state_cd STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  dms_ts TIMESTAMP
);

CREATE OR REPLACE TABLE DMS_CDC_DB.RAW.ORDERS_RAW (
  order_id NUMBER,
  customer_id NUMBER,
  order_amount NUMBER(18,2),
  status STRING,
  order_ts TIMESTAMP,
  updated_at TIMESTAMP,
  dms_ts TIMESTAMP
);