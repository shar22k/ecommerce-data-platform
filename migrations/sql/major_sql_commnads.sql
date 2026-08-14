
-- ============================================================================
-- E-COMMERCE DATA PLATFORM
-- Consolidated SQL Migration / Setup / Validation Script
--
-- IMPORTANT:
-- This file contains SQL used across TWO database engines:
--   1) PostgreSQL  - source system setup / test data changes
--   2) Snowflake   - warehouse, schemas, stages, RAW tables, COPY, validation
--
-- Run only the section for the correct database engine.
-- ============================================================================


-- ============================================================================
-- SECTION A - POSTGRESQL SOURCE SYSTEM
-- ============================================================================

-- Connect example:
-- psql -h localhost -p 5432 -U ecommerce -d ecommerce_db -W

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    customer_unique_id VARCHAR(64),
    customer_zip_code_prefix VARCHAR(20),
    customer_city VARCHAR(255),
    customer_state VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id VARCHAR(64),
    order_item_id INTEGER,
    product_id VARCHAR(64),
    seller_id VARCHAR(64),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(18,2),
    freight_value NUMERIC(18,2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS order_payments (
    order_id VARCHAR(64),
    payment_sequential INTEGER,
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    payment_value NUMERIC(18,2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(64) PRIMARY KEY,
    product_category_name VARCHAR(255),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g NUMERIC(18,2),
    product_length_cm NUMERIC(18,2),
    product_height_cm NUMERIC(18,2),
    product_width_cm NUMERIC(18,2)
);

-- SCD Type 2 test update
UPDATE customers
SET customer_city = 'test_city', customer_state = 'TS'
WHERE customer_id = '06b8999e2fba1a1fbc88172c00ba8bc7';

SELECT customer_id, customer_city, customer_state
FROM customers
WHERE customer_id = '06b8999e2fba1a1fbc88172c00ba8bc7';

-- Restore customer after SCD Type 2 test
UPDATE customers
SET customer_city = 'franca', customer_state = 'SP'
WHERE customer_id = '06b8999e2fba1a1fbc88172c00ba8bc7';

SELECT customer_id, customer_city, customer_state
FROM customers
WHERE customer_id = '06b8999e2fba1a1fbc88172c00ba8bc7';

-- Incremental dbt test order
INSERT INTO orders (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
VALUES (
    'incremental_test_order_001',
    '06b8999e2fba1a1fbc88172c00ba8bc7',
    'delivered',
    '2018-10-19 10:00:00',
    '2018-10-19 10:05:00',
    '2018-10-20 09:00:00',
    '2018-10-22 15:00:00',
    '2018-10-25 00:00:00'
)
ON CONFLICT (order_id) DO NOTHING;

SELECT order_id, customer_id, order_purchase_timestamp
FROM orders
WHERE order_id = 'incremental_test_order_001';


-- ============================================================================
-- SECTION B - SNOWFLAKE PLATFORM SETUP
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS ECOMMERCE_WH;
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.RAW;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.ANALYTICS;

-- Role grants used by dbt / development
GRANT USAGE ON WAREHOUSE ECOMMERCE_WH TO ROLE DEV;
GRANT USAGE ON DATABASE ECOMMERCE_DB TO ROLE DEV;
GRANT USAGE ON SCHEMA ECOMMERCE_DB.RAW TO ROLE DEV;
GRANT USAGE ON SCHEMA ECOMMERCE_DB.STAGING TO ROLE DEV;
GRANT USAGE ON SCHEMA ECOMMERCE_DB.ANALYTICS TO ROLE DEV;
GRANT CREATE TABLE ON SCHEMA ECOMMERCE_DB.STAGING TO ROLE DEV;
GRANT CREATE VIEW ON SCHEMA ECOMMERCE_DB.STAGING TO ROLE DEV;
GRANT CREATE TABLE ON SCHEMA ECOMMERCE_DB.ANALYTICS TO ROLE DEV;
GRANT CREATE VIEW ON SCHEMA ECOMMERCE_DB.ANALYTICS TO ROLE DEV;

-- Storage integration. Replace placeholder with the IAM role ARN actually used.
CREATE STORAGE INTEGRATION IF NOT EXISTS ECOMMERCE_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<AWS_IAM_ROLE_ARN>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://data-engineering-project-kv/');

DESC INTEGRATION ECOMMERCE_S3_INTEGRATION;

CREATE FILE FORMAT IF NOT EXISTS ECOMMERCE_DB.RAW.PARQUET_FORMAT
    TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE
    URL = 's3://data-engineering-project-kv/silver/'
    STORAGE_INTEGRATION = ECOMMERCE_S3_INTEGRATION
    FILE_FORMAT = ECOMMERCE_DB.RAW.PARQUET_FORMAT;

-- RAW tables
CREATE TABLE IF NOT EXISTS ECOMMERCE_DB.RAW.CUSTOMERS (
    CUSTOMER_ID VARCHAR,
    CUSTOMER_UNIQUE_ID VARCHAR,
    CUSTOMER_ZIP_CODE_PREFIX VARCHAR,
    CUSTOMER_CITY VARCHAR,
    CUSTOMER_STATE VARCHAR,
    _INGESTED_AT TIMESTAMP_NTZ,
    _BATCH_ID VARCHAR,
    _SOURCE_SYSTEM VARCHAR,
    _SOURCE_TABLE VARCHAR,
    _SILVER_PROCESSED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS ECOMMERCE_DB.RAW.ORDERS (
    ORDER_ID VARCHAR,
    CUSTOMER_ID VARCHAR,
    ORDER_STATUS VARCHAR,
    ORDER_PURCHASE_TIMESTAMP TIMESTAMP_NTZ,
    ORDER_APPROVED_AT TIMESTAMP_NTZ,
    ORDER_DELIVERED_CARRIER_DATE TIMESTAMP_NTZ,
    ORDER_DELIVERED_CUSTOMER_DATE TIMESTAMP_NTZ,
    ORDER_ESTIMATED_DELIVERY_DATE TIMESTAMP_NTZ,
    _INGESTED_AT TIMESTAMP_NTZ,
    _BATCH_ID VARCHAR,
    _SOURCE_SYSTEM VARCHAR,
    _SOURCE_TABLE VARCHAR,
    _SILVER_PROCESSED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS ECOMMERCE_DB.RAW.ORDER_ITEMS (
    ORDER_ID VARCHAR,
    ORDER_ITEM_ID NUMBER,
    PRODUCT_ID VARCHAR,
    SELLER_ID VARCHAR,
    SHIPPING_LIMIT_DATE TIMESTAMP_NTZ,
    PRICE NUMBER(18,2),
    FREIGHT_VALUE NUMBER(18,2),
    _INGESTED_AT TIMESTAMP_NTZ,
    _BATCH_ID VARCHAR,
    _SOURCE_SYSTEM VARCHAR,
    _SOURCE_TABLE VARCHAR,
    _SILVER_PROCESSED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS ECOMMERCE_DB.RAW.ORDER_PAYMENTS (
    ORDER_ID VARCHAR,
    PAYMENT_SEQUENTIAL NUMBER,
    PAYMENT_TYPE VARCHAR,
    PAYMENT_INSTALLMENTS NUMBER,
    PAYMENT_VALUE NUMBER(18,2),
    _INGESTED_AT TIMESTAMP_NTZ,
    _BATCH_ID VARCHAR,
    _SOURCE_SYSTEM VARCHAR,
    _SOURCE_TABLE VARCHAR,
    _SILVER_PROCESSED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS ECOMMERCE_DB.RAW.PRODUCTS (
    PRODUCT_ID VARCHAR,
    PRODUCT_CATEGORY_NAME VARCHAR,
    PRODUCT_NAME_LENGTH NUMBER,
    PRODUCT_DESCRIPTION_LENGTH NUMBER,
    PRODUCT_PHOTOS_QTY NUMBER,
    PRODUCT_WEIGHT_G NUMBER(18,2),
    PRODUCT_LENGTH_CM NUMBER(18,2),
    PRODUCT_HEIGHT_CM NUMBER(18,2),
    PRODUCT_WIDTH_CM NUMBER(18,2),
    _INGESTED_AT TIMESTAMP_NTZ,
    _BATCH_ID VARCHAR,
    _SOURCE_SYSTEM VARCHAR,
    _SOURCE_TABLE VARCHAR,
    _SILVER_PROCESSED_AT TIMESTAMP_NTZ
);


-- ============================================================================
-- SECTION C - SNOWFLAKE RAW LOADS
-- ============================================================================

-- Inspect Silver stage
LIST @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/customers/;
LIST @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/orders/;
LIST @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/order_items/;
LIST @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/order_payments/;
LIST @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/products/;

-- Preview SCD2 customer directly from Silver
SELECT
    $1:customer_id::VARCHAR AS customer_id,
    $1:customer_city::VARCHAR AS customer_city,
    $1:customer_state::VARCHAR AS customer_state
FROM @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/customers/
(
    FILE_FORMAT => 'ECOMMERCE_DB.RAW.PARQUET_FORMAT',
    PATTERN => '.*[.]parquet'
)
WHERE $1:customer_id::VARCHAR = '06b8999e2fba1a1fbc88172c00ba8bc7';

-- CUSTOMERS
TRUNCATE TABLE ECOMMERCE_DB.RAW.CUSTOMERS;

COPY INTO ECOMMERCE_DB.RAW.CUSTOMERS (
    CUSTOMER_ID,
    CUSTOMER_UNIQUE_ID,
    CUSTOMER_ZIP_CODE_PREFIX,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    _INGESTED_AT,
    _BATCH_ID,
    _SOURCE_SYSTEM,
    _SOURCE_TABLE,
    _SILVER_PROCESSED_AT
)
FROM (
    SELECT
        $1:customer_id::VARCHAR,
        $1:customer_unique_id::VARCHAR,
        $1:customer_zip_code_prefix::VARCHAR,
        $1:customer_city::VARCHAR,
        $1:customer_state::VARCHAR,
        $1:_ingested_at::TIMESTAMP_NTZ,
        $1:_batch_id::VARCHAR,
        $1:_source_system::VARCHAR,
        $1:_source_table::VARCHAR,
        $1:_silver_processed_at::TIMESTAMP_NTZ
    FROM @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/customers/
)
FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DB.RAW.PARQUET_FORMAT')
PATTERN = '.*[.]parquet'
FORCE = TRUE;

-- ORDERS
TRUNCATE TABLE ECOMMERCE_DB.RAW.ORDERS;

COPY INTO ECOMMERCE_DB.RAW.ORDERS (
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_STATUS,
    ORDER_PURCHASE_TIMESTAMP,
    ORDER_APPROVED_AT,
    ORDER_DELIVERED_CARRIER_DATE,
    ORDER_DELIVERED_CUSTOMER_DATE,
    ORDER_ESTIMATED_DELIVERY_DATE,
    _INGESTED_AT,
    _BATCH_ID,
    _SOURCE_SYSTEM,
    _SOURCE_TABLE,
    _SILVER_PROCESSED_AT
)
FROM (
    SELECT
        $1:order_id::VARCHAR,
        $1:customer_id::VARCHAR,
        $1:order_status::VARCHAR,
        $1:order_purchase_timestamp::TIMESTAMP_NTZ,
        $1:order_approved_at::TIMESTAMP_NTZ,
        $1:order_delivered_carrier_date::TIMESTAMP_NTZ,
        $1:order_delivered_customer_date::TIMESTAMP_NTZ,
        $1:order_estimated_delivery_date::TIMESTAMP_NTZ,
        $1:_ingested_at::TIMESTAMP_NTZ,
        $1:_batch_id::VARCHAR,
        $1:_source_system::VARCHAR,
        $1:_source_table::VARCHAR,
        $1:_silver_processed_at::TIMESTAMP_NTZ
    FROM @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/orders/
)
FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DB.RAW.PARQUET_FORMAT')
PATTERN = '.*[.]parquet'
FORCE = TRUE;

-- ORDER_ITEMS
TRUNCATE TABLE ECOMMERCE_DB.RAW.ORDER_ITEMS;

COPY INTO ECOMMERCE_DB.RAW.ORDER_ITEMS (
    ORDER_ID,
    ORDER_ITEM_ID,
    PRODUCT_ID,
    SELLER_ID,
    SHIPPING_LIMIT_DATE,
    PRICE,
    FREIGHT_VALUE,
    _INGESTED_AT,
    _BATCH_ID,
    _SOURCE_SYSTEM,
    _SOURCE_TABLE,
    _SILVER_PROCESSED_AT
)
FROM (
    SELECT
        $1:order_id::VARCHAR,
        $1:order_item_id::NUMBER,
        $1:product_id::VARCHAR,
        $1:seller_id::VARCHAR,
        $1:shipping_limit_date::TIMESTAMP_NTZ,
        $1:price::NUMBER(18,2),
        $1:freight_value::NUMBER(18,2),
        $1:_ingested_at::TIMESTAMP_NTZ,
        $1:_batch_id::VARCHAR,
        $1:_source_system::VARCHAR,
        $1:_source_table::VARCHAR,
        $1:_silver_processed_at::TIMESTAMP_NTZ
    FROM @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/order_items/
)
FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DB.RAW.PARQUET_FORMAT')
PATTERN = '.*[.]parquet'
FORCE = TRUE;

-- ORDER_PAYMENTS
TRUNCATE TABLE ECOMMERCE_DB.RAW.ORDER_PAYMENTS;

COPY INTO ECOMMERCE_DB.RAW.ORDER_PAYMENTS (
    ORDER_ID,
    PAYMENT_SEQUENTIAL,
    PAYMENT_TYPE,
    PAYMENT_INSTALLMENTS,
    PAYMENT_VALUE,
    _INGESTED_AT,
    _BATCH_ID,
    _SOURCE_SYSTEM,
    _SOURCE_TABLE,
    _SILVER_PROCESSED_AT
)
FROM (
    SELECT
        $1:order_id::VARCHAR,
        $1:payment_sequential::NUMBER,
        $1:payment_type::VARCHAR,
        $1:payment_installments::NUMBER,
        $1:payment_value::NUMBER(18,2),
        $1:_ingested_at::TIMESTAMP_NTZ,
        $1:_batch_id::VARCHAR,
        $1:_source_system::VARCHAR,
        $1:_source_table::VARCHAR,
        $1:_silver_processed_at::TIMESTAMP_NTZ
    FROM @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/order_payments/
)
FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DB.RAW.PARQUET_FORMAT')
PATTERN = '.*[.]parquet'
FORCE = TRUE;

-- PRODUCTS
TRUNCATE TABLE ECOMMERCE_DB.RAW.PRODUCTS;

COPY INTO ECOMMERCE_DB.RAW.PRODUCTS (
    PRODUCT_ID,
    PRODUCT_CATEGORY_NAME,
    PRODUCT_NAME_LENGTH,
    PRODUCT_DESCRIPTION_LENGTH,
    PRODUCT_PHOTOS_QTY,
    PRODUCT_WEIGHT_G,
    PRODUCT_LENGTH_CM,
    PRODUCT_HEIGHT_CM,
    PRODUCT_WIDTH_CM,
    _INGESTED_AT,
    _BATCH_ID,
    _SOURCE_SYSTEM,
    _SOURCE_TABLE,
    _SILVER_PROCESSED_AT
)
FROM (
    SELECT
        $1:product_id::VARCHAR,
        $1:product_category_name::VARCHAR,
        $1:product_name_length::NUMBER,
        $1:product_description_length::NUMBER,
        $1:product_photos_qty::NUMBER,
        $1:product_weight_g::NUMBER(18,2),
        $1:product_length_cm::NUMBER(18,2),
        $1:product_height_cm::NUMBER(18,2),
        $1:product_width_cm::NUMBER(18,2),
        $1:_ingested_at::TIMESTAMP_NTZ,
        $1:_batch_id::VARCHAR,
        $1:_source_system::VARCHAR,
        $1:_source_table::VARCHAR,
        $1:_silver_processed_at::TIMESTAMP_NTZ
    FROM @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/products/
)
FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DB.RAW.PARQUET_FORMAT')
PATTERN = '.*[.]parquet'
FORCE = TRUE;


-- ============================================================================
-- SECTION D - VALIDATION / TROUBLESHOOTING QUERIES
-- ============================================================================

SELECT COUNT(*) AS raw_customers FROM ECOMMERCE_DB.RAW.CUSTOMERS;
SELECT COUNT(*) AS raw_orders FROM ECOMMERCE_DB.RAW.ORDERS;
SELECT COUNT(*) AS raw_order_items FROM ECOMMERCE_DB.RAW.ORDER_ITEMS;
SELECT COUNT(*) AS raw_order_payments FROM ECOMMERCE_DB.RAW.ORDER_PAYMENTS;
SELECT COUNT(*) AS raw_products FROM ECOMMERCE_DB.RAW.PRODUCTS;

SELECT customer_id, customer_city, customer_state
FROM ECOMMERCE_DB.RAW.CUSTOMERS
WHERE customer_id = '06b8999e2fba1a1fbc88172c00ba8bc7';

SELECT COUNT(*) AS customers FROM ECOMMERCE_DB.STAGING.STG_CUSTOMERS;
SELECT COUNT(*) AS orders FROM ECOMMERCE_DB.STAGING.STG_ORDERS;

SELECT COUNT(*) AS matching_rows
FROM ECOMMERCE_DB.STAGING.STG_ORDERS o
JOIN ECOMMERCE_DB.STAGING.STG_CUSTOMERS c
    ON o.customer_id = c.customer_id;

SELECT COUNT(*) AS matches_on_unique_id
FROM ECOMMERCE_DB.STAGING.STG_ORDERS o
JOIN ECOMMERCE_DB.STAGING.STG_CUSTOMERS c
    ON o.customer_id = c.customer_unique_id;

SELECT o.order_id, o.customer_id AS order_customer_id
FROM ECOMMERCE_DB.STAGING.STG_ORDERS o
LEFT JOIN ECOMMERCE_DB.STAGING.STG_CUSTOMERS c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
LIMIT 10;

SELECT
    $1:customer_id::VARCHAR AS customer_id,
    $1:customer_city::VARCHAR AS customer_city,
    $1:customer_state::VARCHAR AS customer_state
FROM @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/customers/
(
    FILE_FORMAT => 'ECOMMERCE_DB.RAW.PARQUET_FORMAT',
    PATTERN => '.*[.]parquet'
)
LIMIT 10;

SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'ECOMMERCE_DB.RAW.CUSTOMERS',
        START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
    )
)
ORDER BY LAST_LOAD_TIME DESC;


-- ============================================================================
-- SECTION E - SCD TYPE 2 / DBT SNAPSHOT VALIDATION
-- ============================================================================

SELECT
    customer_id,
    customer_city,
    customer_state,
    dbt_valid_from,
    dbt_valid_to
FROM ECOMMERCE_DB.ANALYTICS.CUSTOMERS_SNAPSHOT
WHERE customer_id = '06b8999e2fba1a1fbc88172c00ba8bc7'
ORDER BY dbt_valid_from;

SELECT COUNT(*) FROM ECOMMERCE_DB.ANALYTICS.DIM_CUSTOMERS;

SELECT customer_id, COUNT(*) AS record_count
FROM ECOMMERCE_DB.ANALYTICS.DIM_CUSTOMERS
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT
    customer_id,
    customer_city,
    customer_state,
    dbt_valid_from,
    dbt_valid_to
FROM ECOMMERCE_DB.ANALYTICS.DIM_CUSTOMERS
WHERE customer_id = '06b8999e2fba1a1fbc88172c00ba8bc7';


-- ============================================================================
-- SECTION F - FACT / INCREMENTAL MODEL VALIDATION
-- ============================================================================

SELECT COUNT(*) AS fact_orders_count
FROM ECOMMERCE_DB.ANALYTICS.FACT_ORDERS;

SELECT order_id, COUNT(*) AS cnt
FROM ECOMMERCE_DB.ANALYTICS.FACT_ORDERS
GROUP BY order_id
HAVING COUNT(*) > 1;

SELECT MAX(order_purchase_timestamp)
FROM ECOMMERCE_DB.ANALYTICS.FACT_ORDERS;

SELECT
    $1:order_id::VARCHAR AS order_id,
    $1:customer_id::VARCHAR AS customer_id,
    $1:order_purchase_timestamp::TIMESTAMP_NTZ AS order_purchase_timestamp
FROM @ECOMMERCE_DB.RAW.ECOMMERCE_SILVER_STAGE/orders/
(
    FILE_FORMAT => 'ECOMMERCE_DB.RAW.PARQUET_FORMAT',
    PATTERN => '.*[.]parquet'
)
WHERE $1:order_id::VARCHAR = 'incremental_test_order_001';

SELECT order_id, customer_id, order_purchase_timestamp
FROM ECOMMERCE_DB.RAW.ORDERS
WHERE order_id = 'incremental_test_order_001';

SELECT COUNT(*) AS before_count
FROM ECOMMERCE_DB.ANALYTICS.FACT_ORDERS;

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    total_items,
    total_order_value
FROM ECOMMERCE_DB.ANALYTICS.FACT_ORDERS
WHERE order_id = 'incremental_test_order_001';

SELECT COUNT(*) AS after_count
FROM ECOMMERCE_DB.ANALYTICS.FACT_ORDERS;

-- Final platform checks
SELECT COUNT(*) AS raw_orders FROM ECOMMERCE_DB.RAW.ORDERS;
SELECT COUNT(*) AS fact_orders FROM ECOMMERCE_DB.ANALYTICS.FACT_ORDERS;

SELECT order_id, customer_id, order_purchase_timestamp
FROM ECOMMERCE_DB.ANALYTICS.FACT_ORDERS
WHERE order_id = 'incremental_test_order_001';

-- ============================================================================
-- END OF FILE
-- ============================================================================




