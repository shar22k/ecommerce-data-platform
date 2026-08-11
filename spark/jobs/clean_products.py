from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.utils.spark_session import get_spark_session
from spark.utils.s3_paths import bronze_path, silver_path, rejected_path
from ingestion.utils.logger import get_logger

logger = get_logger(__name__)


def clean_products():
    spark = get_spark_session("CleanProducts")

    try:
        source_path = bronze_path("products")
        target_path = silver_path("products")
        reject_path = rejected_path("products")

        bronze_df = spark.read.parquet(source_path)
        bronze_count = bronze_df.count()

        cleaned_df = (
            bronze_df
            .withColumn("product_id", F.trim("product_id"))
        )

        if "product_category_name" in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(
                "product_category_name",
                F.lower(F.trim("product_category_name"))
            )

        numeric_columns = [
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ]

        for column_name in numeric_columns:
            if column_name in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    column_name,
                    F.col(column_name).cast("double")
                )

        invalid_product = (
            F.col("product_id").isNull() |
            (F.length("product_id") == 0)
        )

        negative_dimension_conditions = []

        for column_name in [
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ]:
            if column_name in cleaned_df.columns:
                negative_dimension_conditions.append(
                    F.col(column_name) < 0
                )

        invalid_dimensions = F.lit(False)

        for condition in negative_dimension_conditions:
            invalid_dimensions = (
                invalid_dimensions | condition
            )

        invalid_condition = (
            invalid_product |
            invalid_dimensions
        )

        rejected_df = (
            cleaned_df
            .filter(invalid_condition)
            .withColumn(
                "_rejection_reason",
                F.when(
                    invalid_product,
                    F.lit("invalid_product_id")
                )
                .when(
                    invalid_dimensions,
                    F.lit("negative_product_dimension")
                )
                .otherwise(
                    F.lit("unknown_validation_failure")
                )
            )
        )

        valid_df = cleaned_df.filter(~invalid_condition)

        valid_before_dedup = valid_df.count()

        window = (
            Window
            .partitionBy("product_id")
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

        print("\nProducts transformation summary")
        print("-------------------------------")
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

        logger.info("Products Silver transformation completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    clean_products()


