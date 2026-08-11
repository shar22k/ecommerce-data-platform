import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from ingestion.utils.logger import get_logger


load_dotenv()

logger = get_logger(__name__)


AWS_REGION = os.getenv("AWS_REGION")


def get_spark_session(
    app_name: str = "EcommerceDataPlatform",
) -> SparkSession:
    """
    Create a local SparkSession configured
    to read/write Amazon S3 using S3A.
    """

    if not AWS_REGION:
        raise ValueError(
            "AWS_REGION is missing from .env"
        )

    logger.info(
        "Creating Spark session app_name=%s",
        app_name,
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # -------------------------------------------------
        # Hadoop S3A connector
        # -------------------------------------------------
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.5.0",
        )

        # -------------------------------------------------
        # Basic Spark configuration
        # -------------------------------------------------
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy",
        )

        .getOrCreate()
    )

    hadoop_conf = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    # -------------------------------------------------
    # Tell Hadoop to use S3A
    # -------------------------------------------------
    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    # -------------------------------------------------
    # Use AWS environment variables
    # -------------------------------------------------
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        (
            "org.apache.hadoop.fs.s3a."
            "auth.IAMInstanceCredentialsProvider,"
            "org.apache.hadoop.fs.s3a."
            "SimpleAWSCredentialsProvider"
        ),
    )

    # -------------------------------------------------
    # AWS region
    # -------------------------------------------------
    hadoop_conf.set(
        "fs.s3a.endpoint.region",
        AWS_REGION,
    )

    logger.info(
        "Spark session created "
        "spark_version=%s",
        spark.version,
    )

    return spark


