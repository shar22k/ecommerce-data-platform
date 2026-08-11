from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.utils.spark_session import (
    get_spark_session,
)
from spark.utils.s3_paths import (
    bronze_path,
    silver_path,
    rejected_path,
)
from ingestion.utils.logger import get_logger


logger = get_logger(__name__)


def clean_customers():
    spark = get_spark_session(
        app_name="CleanCustomers"
    )

    try:
        # ---------------------------------------------------------
        # 1. Define S3 paths
        # ---------------------------------------------------------

        source_path = bronze_path(
            "customers"
        )

        target_path = silver_path(
            "customers"
        )

        reject_path = rejected_path(
            "customers"
        )

        logger.info(
            "Reading Bronze customers from %s",
            source_path,
        )

        # ---------------------------------------------------------
        # 2. Read entire Bronze history
        # ---------------------------------------------------------

        bronze_df = (
            spark.read
            .parquet(source_path)
        )

        bronze_count = bronze_df.count()

        logger.info(
            "Bronze customer rows=%s",
            bronze_count,
        )

        print(
            f"Bronze rows: {bronze_count}"
        )

        # ---------------------------------------------------------
        # 3. Standardize customer fields
        # ---------------------------------------------------------

        cleaned_df = (
            bronze_df

            # Trim IDs
            .withColumn(
                "customer_id",
                F.trim(
                    F.col("customer_id")
                ),
            )

            .withColumn(
                "customer_unique_id",
                F.trim(
                    F.col("customer_unique_id")
                ),
            )

            # Normalize city
            .withColumn(
                "customer_city",
                F.lower(
                    F.trim(
                        F.col("customer_city")
                    )
                ),
            )

            # Normalize state
            .withColumn(
                "customer_state",
                F.upper(
                    F.trim(
                        F.col("customer_state")
                    )
                ),
            )

            # Keep ZIP as string
            .withColumn(
                "customer_zip_code_prefix",
                F.col(
                    "customer_zip_code_prefix"
                ).cast("string"),
            )
        )

        # ---------------------------------------------------------
        # 4. Identify invalid rows
        #
        # customer_id is our source primary key.
        # customer_unique_id should also exist.
        # ---------------------------------------------------------

        invalid_condition = (
            F.col("customer_id").isNull()
            |
            (
                F.length(
                    F.col("customer_id")
                ) == 0
            )
            |
            F.col(
                "customer_unique_id"
            ).isNull()
            |
            (
                F.length(
                    F.col(
                        "customer_unique_id"
                    )
                ) == 0
            )
        )

        rejected_df = (
            cleaned_df
            .filter(
                invalid_condition
            )
            .withColumn(
                "_rejection_reason",
                F.when(
                    F.col(
                        "customer_id"
                    ).isNull(),
                    F.lit(
                        "customer_id_null"
                    ),
                )
                .when(
                    F.length(
                        F.col(
                            "customer_id"
                        )
                    ) == 0,
                    F.lit(
                        "customer_id_empty"
                    ),
                )
                .when(
                    F.col(
                        "customer_unique_id"
                    ).isNull(),
                    F.lit(
                        "customer_unique_id_null"
                    ),
                )
                .otherwise(
                    F.lit(
                        "customer_unique_id_empty"
                    )
                ),
            )
        )

        valid_df = (
            cleaned_df
            .filter(
                ~invalid_condition
            )
        )

        # ---------------------------------------------------------
        # 5. Deduplicate
        #
        # Bronze contains multiple ingestion batches.
        #
        # Keep the newest version of each customer_id,
        # based on _ingested_at.
        # ---------------------------------------------------------

        customer_window = (
            Window
            .partitionBy(
                "customer_id"
            )
            .orderBy(
                F.col(
                    "_ingested_at"
                ).desc()
            )
        )

        deduplicated_df = (
            valid_df
            .withColumn(
                "_row_number",
                F.row_number().over(
                    customer_window
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
        # 6. Add Silver metadata
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
        # 7. Metrics
        # ---------------------------------------------------------

        valid_before_dedup_count = (
            valid_df.count()
        )

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

        logger.info(
            "Customer transformation metrics "
            "bronze=%s valid_before_dedup=%s "
            "silver=%s rejected=%s "
            "duplicates_removed=%s",
            bronze_count,
            valid_before_dedup_count,
            silver_count,
            rejected_count,
            duplicates_removed,
        )

        print()
        print("Customer transformation summary")
        print("--------------------------------")
        print(
            f"Bronze rows            : "
            f"{bronze_count}"
        )
        print(
            f"Valid before dedup      : "
            f"{valid_before_dedup_count}"
        )
        print(
            f"Duplicates removed      : "
            f"{duplicates_removed}"
        )
        print(
            f"Rejected rows           : "
            f"{rejected_count}"
        )
        print(
            f"Silver rows             : "
            f"{silver_count}"
        )

        # ---------------------------------------------------------
        # 8. Write Silver customers
        # ---------------------------------------------------------

        logger.info(
            "Writing Silver customers to %s",
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
        # 9. Write rejected records only if they exist
        # ---------------------------------------------------------

        if rejected_count > 0:

            logger.info(
                "Writing rejected customer rows to %s",
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
                "No rejected customer records found"
            )

        # ---------------------------------------------------------
        # 10. Display result
        # ---------------------------------------------------------

        print()
        print("Silver schema:")

        silver_df.printSchema()

        print()
        print("Silver sample:")

        silver_df.select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "_ingested_at",
            "_silver_processed_at",
        ).show(
            10,
            truncate=False,
        )

        logger.info(
            "Customer Silver transformation "
            "completed successfully"
        )

    except Exception:

        logger.exception(
            "Customer Silver transformation failed"
        )

        raise

    finally:

        spark.stop()

        logger.info(
            "Spark session stopped"
        )


if __name__ == "__main__":
    clean_customers()


