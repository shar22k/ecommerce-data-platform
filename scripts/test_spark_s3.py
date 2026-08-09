from spark.utils.spark_session import (
    get_spark_session,
)
from spark.utils.s3_paths import (
    bronze_path,
)


def main():
    spark = get_spark_session(
        app_name="TestSparkS3"
    )

    try:
        path = bronze_path(
            "customers"
        )

        print(
            f"Reading Bronze data from: {path}"
        )

        df = (
            spark.read
            .parquet(path)
        )

        print(
            "Schema:"
        )

        df.printSchema()

        row_count = df.count()

        print(
            f"Row count: {row_count}"
        )

        print(
            "Sample rows:"
        )

        df.show(
            5,
            truncate=False,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


