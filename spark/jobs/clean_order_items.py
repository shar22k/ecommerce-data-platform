from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.utils.spark_session import get_spark_session
from spark.utils.s3_paths import bronze_path, silver_path, rejected_path
from ingestion.utils.logger import get_logger

logger = get_logger(__name__)


def clean_order_items():
    spark = get_spark_session("CleanOrderItems")

    try:
        source_path = bronze_path("order_items")
        target_path = silver_path("order_items")
        reject_path = rejected_path("order_items")

        bronze_df = spark.read.parquet(source_path)
        bronze_count = bronze_df.count()

        cleaned_df = (
            bronze_df
            .withColumn("order_id", F.trim("order_id"))
            .withColumn("product_id", F.trim("product_id"))
            .withColumn("seller_id", F.trim("seller_id"))
            .withColumn("order_item_id", F.col("order_item_id").cast("integer"))
            .withColumn("price", F.col("price").cast("double"))
            .withColumn("freight_value", F.col("freight_value").cast("double"))
        )

        if "shipping_limit_date" in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(
                "shipping_limit_date",
                F.to_timestamp("shipping_limit_date"),
            )

        invalid_order_id = (
            F.col("order_id").isNull() |
            (F.length("order_id") == 0)
        )

        invalid_product_id = (
            F.col("product_id").isNull() |
            (F.length("product_id") == 0)
        )

        invalid_seller_id = (
            F.col("seller_id").isNull() |
            (F.length("seller_id") == 0)
        )

        invalid_item_id = (
            F.col("order_item_id").isNull() |
            (F.col("order_item_id") <= 0)
        )

        invalid_price = (
            F.col("price").isNull() |
            (F.col("price") < 0)
        )

        invalid_freight = (
            F.col("freight_value").isNull() |
            (F.col("freight_value") < 0)
        )

        invalid_condition = (
            invalid_order_id |
            invalid_product_id |
            invalid_seller_id |
            invalid_item_id |
            invalid_price |
            invalid_freight
        )

        rejected_df = (
            cleaned_df
            .filter(invalid_condition)
            .withColumn(
                "_rejection_reason",
                F.when(invalid_order_id, F.lit("invalid_order_id"))
                .when(invalid_product_id, F.lit("invalid_product_id"))
                .when(invalid_seller_id, F.lit("invalid_seller_id"))
                .when(invalid_item_id, F.lit("invalid_order_item_id"))
                .when(invalid_price, F.lit("invalid_price"))
                .when(invalid_freight, F.lit("invalid_freight_value"))
                .otherwise(F.lit("unknown_validation_failure"))
            )
        )

        valid_df = cleaned_df.filter(~invalid_condition)

        valid_before_dedup = valid_df.count()

        window = (
            Window
            .partitionBy("order_id", "order_item_id")
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

        print("\nOrder Items transformation summary")
        print("----------------------------------")
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

        logger.info("Order Items Silver transformation completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    clean_order_items()


