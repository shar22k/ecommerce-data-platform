import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization


PRIVATE_KEY_PATH = Path.home() / ".ssh" / "snowflake" / "rsa_key.p8"


def load_private_key():
    with open(PRIVATE_KEY_PATH, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def main():
    private_key = load_private_key()

    conn = snowflake.connector.connect(
        account="NDNJDWF-SV86672",
        user="shar06k",
        private_key=private_key,
        warehouse="ECOMMERCE_WH",
        database="ECOMMERCE_DB",
        schema="RAW",
        role="DEV",
    )

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            TRUNCATE TABLE ECOMMERCE_DB.RAW.ORDERS
            """
        )

        cursor.execute(
            """
            COPY INTO ECOMMERCE_DB.RAW.ORDERS
            (
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
            FROM
            (
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
            FILE_FORMAT = (
                FORMAT_NAME = 'ECOMMERCE_DB.RAW.PARQUET_FORMAT'
            )
            PATTERN = '.*[.]parquet'
            FORCE = TRUE
            """
        )

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM ECOMMERCE_DB.RAW.ORDERS
            """
        )

        row_count = cursor.fetchone()[0]

        print(f"RAW.ORDERS row count: {row_count}")

        if row_count == 0:
            raise RuntimeError("Snowflake RAW.ORDERS load produced 0 rows")

        print("Snowflake orders load completed successfully")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
