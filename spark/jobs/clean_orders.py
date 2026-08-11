from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.utils.spark_session import get_spark_session
from spark.utils.s3_paths import (
    bronze_path,
    silver_path,
    rejected_path,
)
from ingestion.utils.logger import get_logger


logger = get_logger(__name__)


VALID_ORDER_STATUSES = [
    "delivered",
    "shipped",
    "canceled",
    "cancelled",
    "invoiced",
    "processing",
    "approved",
    "unavailable",
    "created",
]


TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def clean_orders():
    spark = get_spark_session(
        app_name="CleanOrders"
    )

    try:
        source_path = bronze_path(
            "orders"
        )

        target_path = silver_path(
            "orders"
        )

        reject_path = rejected_path(
            "orders"
        )

        logger.info(
            "Reading Bronze orders from %s",
            source_path,
        )

        # ---------------------------------------------------------
        # 1. Read Bronze history
        # ---------------------------------------------------------

        bronze_df = (
            spark.read
            .parquet(source_path)
        )

        bronze_count = bronze_df.count()

        logger.info(
            "Bronze order rows=%s",
            bronze_count,
        )

        print()
        print(
            f"Bronze rows: {bronze_count}"
        )

        # ---------------------------------------------------------
        # 2. Basic string normalization
        # ---------------------------------------------------------

        cleaned_df = (
            bronze_df
            .withColumn(
                "order_id",
                F.trim(
                    F.col("order_id")
                ),
            )
            .withColumn(
                "customer_id",
                F.trim(
                    F.col("customer_id")
                ),
            )
            .withColumn(
                "order_status",
                F.lower(
                    F.trim(
                        F.col("order_status")
                    )
                ),
            )
        )

        # ---------------------------------------------------------
        # 3. Standardize timestamp columns
        # ---------------------------------------------------------

        for column_name in TIMESTAMP_COLUMNS:
            if column_name in cleaned_df.columns:
                cleaned_df = (
                    cleaned_df
                    .withColumn(
                        column_name,
                        F.to_timestamp(
                            F.col(column_name)
                        ),
                    )
                )

        # ---------------------------------------------------------
        # 4. Normalize canceled/cancelled
        # ---------------------------------------------------------

        cleaned_df = (
            cleaned_df
            .withColumn(
                "order_status",
                F.when(
                    F.col("order_status")
                    == "cancelled",
                    F.lit("canceled"),
                ).otherwise(
                    F.col("order_status")
                ),
            )
        )

        # ---------------------------------------------------------
        # 5. Validation rules
        # ---------------------------------------------------------

        invalid_order_id = (
            F.col("order_id").isNull()
            |
            (
                F.length(
                    F.col("order_id")
                ) == 0
            )
        )

        invalid_customer_id = (
            F.col("customer_id").isNull()
            |
            (
                F.length(
                    F.col("customer_id")
                ) == 0
            )
        )

        invalid_status = (
            F.col("order_status").isNull()
            |
            (
                ~F.col("order_status").isin(
                    VALID_ORDER_STATUSES
                )
            )
        )

        invalid_condition = (
            invalid_order_id
            |
            invalid_customer_id
            |
            invalid_status
        )

        # ---------------------------------------------------------
        # 6. Rejected records
        # ---------------------------------------------------------

        rejected_df = (
            cleaned_df
            .filter(
                invalid_condition
            )
            .withColumn(
                "_rejection_reason",
                F.when(
                    invalid_order_id,
                    F.lit(
                        "invalid_order_id"
                    ),
                )
                .when(
                    invalid_customer_id,
                    F.lit(
                        "invalid_customer_id"
                    ),
                )
                .when(
                    invalid_status,
                    F.lit(
                        "invalid_order_status"
                    ),
                )
                .otherwise(
                    F.lit(
                        "unknown_validation_failure"
                    )
                ),
            )
        )

        # ---------------------------------------------------------
        # 7. Valid records
        # ---------------------------------------------------------

        valid_df = (
            cleaned_df
            .filter(
                ~invalid_condition
            )
        )

        valid_before_dedup_count = (
            valid_df.count()
        )

        # ---------------------------------------------------------
        # 8. Deduplicate Bronze history
        #
        # Keep latest ingestion for each order_id.
        # ---------------------------------------------------------

        order_window = (
            Window
            .partitionBy(
                "order_id"
            )
            .orderBy(
                F.col(
                    "_ingested_at"
                ).desc(),
                F.col(
                    "_batch_id"
                ).desc(),
            )
        )

        deduplicated_df = (
            valid_df
            .withColumn(
                "_row_number",
                F.row_number().over(
                    order_window
                ),
            )
            .filter(
                F.col(
                    "_row_number"
                ) == 1
            )
            .drop(
                "_row_number"
            )
        )

        # ---------------------------------------------------------
        # 9. Add Silver metadata
        # ---------------------------------------------------------

        silver_df = (
            deduplicated_df
            .withColumn(
                "_silver_processed_at",
                F.current_timestamp(),
            )
        )

        rejected_df = (
            rejected_df
            .withColumn(
                "_silver_processed_at",
                F.current_timestamp(),
            )
        )

        # ---------------------------------------------------------
        # 10. Metrics
        # ---------------------------------------------------------

        rejected_count = (
            rejected_df.count()
        )

        silver_count = (
            silver_df.count()
        )

        duplicates_removed = (
            valid_before_dedup_count
            - silver_count
        )

        print()
        print(
            "Order transformation summary"
        )
        print(
            "----------------------------"
        )
        print(
            f"Bronze rows            : {bronze_count}"
        )
        print(
            f"Valid before dedup     : {valid_before_dedup_count}"
        )
        print(
            f"Duplicates removed     : {duplicates_removed}"
        )
        print(
            f"Rejected rows          : {rejected_count}"
        )
        print(
            f"Silver rows            : {silver_count}"
        )

        logger.info(
            "Orders metrics "
            "bronze=%s "
            "valid_before_dedup=%s "
            "duplicates_removed=%s "
            "rejected=%s "
            "silver=%s",
            bronze_count,
            valid_before_dedup_count,
            duplicates_removed,
            rejected_count,
            silver_count,
        )

        # ---------------------------------------------------------
        # 11. Write Silver
        # ---------------------------------------------------------

        logger.info(
            "Writing Silver orders to %s",
            target_path,
        )

        (
            silver_df.write
            .mode("overwrite")
            .option(
                "compression",
                "snappy",
            )
            .parquet(
                target_path
            )
        )

        # ---------------------------------------------------------
        # 12. Write rejected rows if present
        # ---------------------------------------------------------

        if rejected_count > 0:

            logger.info(
                "Writing rejected orders to %s",
                reject_path,
            )

            (
                rejected_df.write
                .mode("overwrite")
                .option(
                    "compression",
                    "snappy",
                )
                .parquet(
                    reject_path
                )
            )

        else:

            logger.info(
                "No rejected order records"
            )

        # ---------------------------------------------------------
        # 13. Display schema/sample
        # ---------------------------------------------------------

        print()
        print(
            "Silver orders schema:"
        )

        silver_df.printSchema()

        print()
        print(
            "Silver orders sample:"
        )

        columns_to_show = [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "_ingested_at",
            "_silver_processed_at",
        ]

        existing_columns = [
            column
            for column in columns_to_show
            if column in silver_df.columns
        ]

        silver_df.select(
            *existing_columns
        ).show(
            10,
            truncate=False,
        )

        logger.info(
            "Orders Silver transformation "
            "completed successfully"
        )

    except Exception:

        logger.exception(
            "Orders Silver transformation failed"
        )

        raise

    finally:

        spark.stop()

        logger.info(
            "Spark session stopped"
        )


if __name__ == "__main__":
    clean_orders()

