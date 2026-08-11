from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.utils.spark_session import get_spark_session
from spark.utils.s3_paths import bronze_path, silver_path, rejected_path
from ingestion.utils.logger import get_logger

logger = get_logger(__name__)


VALID_PAYMENT_TYPES = [
    "credit_card",
    "boleto",
    "voucher",
    "debit_card",
    "not_defined",
]


def clean_order_payments():
    spark = get_spark_session("CleanOrderPayments")

    try:
        source_path = bronze_path("order_payments")
        target_path = silver_path("order_payments")
        reject_path = rejected_path("order_payments")

        bronze_df = spark.read.parquet(source_path)
        bronze_count = bronze_df.count()

        cleaned_df = (
            bronze_df
            .withColumn("order_id", F.trim("order_id"))
            .withColumn(
                "payment_type",
                F.lower(F.trim("payment_type"))
            )
            .withColumn(
                "payment_sequential",
                F.col("payment_sequential").cast("integer")
            )
            .withColumn(
                "payment_installments",
                F.col("payment_installments").cast("integer")
            )
            .withColumn(
                "payment_value",
                F.col("payment_value").cast("double")
            )
        )

        invalid_order = (
            F.col("order_id").isNull() |
            (F.length("order_id") == 0)
        )

        invalid_sequence = (
            F.col("payment_sequential").isNull() |
            (F.col("payment_sequential") <= 0)
        )

        invalid_installments = (
            F.col("payment_installments").isNull() |
            (F.col("payment_installments") < 0)
        )

        invalid_value = (
            F.col("payment_value").isNull() |
            (F.col("payment_value") < 0)
        )

        invalid_type = (
            F.col("payment_type").isNull() |
            (~F.col("payment_type").isin(VALID_PAYMENT_TYPES))
        )

        invalid_condition = (
            invalid_order |
            invalid_sequence |
            invalid_installments |
            invalid_value |
            invalid_type
        )

        rejected_df = (
            cleaned_df
            .filter(invalid_condition)
            .withColumn(
                "_rejection_reason",
                F.when(invalid_order, F.lit("invalid_order_id"))
                .when(invalid_sequence, F.lit("invalid_payment_sequential"))
                .when(invalid_installments, F.lit("invalid_payment_installments"))
                .when(invalid_value, F.lit("invalid_payment_value"))
                .when(invalid_type, F.lit("invalid_payment_type"))
                .otherwise(F.lit("unknown_validation_failure"))
            )
        )

        valid_df = cleaned_df.filter(~invalid_condition)

        valid_before_dedup = valid_df.count()

        window = (
            Window
            .partitionBy("order_id", "payment_sequential")
            .orderBy(
                F.col("_ingested_at").desc(),
                F.col("_batch_id").desc()
            )
        )

        silver_df = (
            valid_df
            .withColumn("_row_number", F.row_number().over(window))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
            .withColumn("_silver_processed_at", F.current_timestamp())
        )

        rejected_df = rejected_df.withColumn(
            "_silver_processed_at",
            F.current_timestamp()
        )

        rejected_count = rejected_df.count()
        silver_count = silver_df.count()
        duplicates_removed = valid_before_dedup - silver_count

        print("\nOrder Payments transformation summary")
        print("-------------------------------------")
        print(f"Bronze rows        : {bronze_count}")
        print(f"Valid before dedup : {valid_before_dedup}")
        print(f"Duplicates removed : {duplicates_removed}")
        print(f"Rejected rows      : {rejected_count}")
        print(f"Silver rows        : {silver_count}")

        (
            silver_df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(target_path)
        )

        if rejected_count > 0:
            (
                rejected_df.write
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet(reject_path)
            )

        logger.info("Order Payments Silver transformation completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    clean_order_payments()


