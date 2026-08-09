import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from ingestion.utils.logger import get_logger


load_dotenv()

logger = get_logger(__name__)


S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")


def get_s3_client():
    """
    Create and return a configured boto3 S3 client.

    Boto3 will read AWS credentials from the
    environment loaded from .env.
    """

    if not S3_BUCKET_NAME:
        raise ValueError(
            "S3_BUCKET_NAME is not configured in .env"
        )

    if not AWS_REGION:
        raise ValueError(
            "AWS_REGION is not configured in .env"
        )

    return boto3.client(
        "s3",
        region_name=AWS_REGION,
    )


def test_s3_connection() -> bool:
    """
    Verify that the application can access
    the configured S3 bucket.
    """

    client = get_s3_client()

    try:
        client.head_bucket(
            Bucket=S3_BUCKET_NAME,
        )

        logger.info(
            "Successfully connected to S3 bucket=%s",
            S3_BUCKET_NAME,
        )

        return True

    except ClientError:
        logger.exception(
            "Failed to access S3 bucket=%s",
            S3_BUCKET_NAME,
        )

        raise


def upload_file_to_s3(
    local_file: str,
    s3_key: str,
) -> str:
    """
    Upload a local file to S3.

    Returns the S3 URI.
    """

    local_path = Path(local_file)

    if not local_path.exists():
        raise FileNotFoundError(
            f"Local file does not exist: {local_file}"
        )

    client = get_s3_client()

    logger.info(
        "Uploading local_file=%s to "
        "bucket=%s key=%s",
        local_file,
        S3_BUCKET_NAME,
        s3_key,
    )

    try:
        client.upload_file(
            Filename=str(local_path),
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
        )

    except ClientError:
        logger.exception(
            "Failed uploading file=%s "
            "to s3://%s/%s",
            local_file,
            S3_BUCKET_NAME,
            s3_key,
        )

        raise

    s3_uri = (
        f"s3://{S3_BUCKET_NAME}/{s3_key}"
    )

    logger.info(
        "Upload completed s3_uri=%s",
        s3_uri,
    )

    return s3_uri


def verify_s3_object(
    s3_key: str,
) -> bool:
    """
    Verify that an uploaded object exists in S3.
    """

    client = get_s3_client()

    try:
        response = client.head_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
        )

        size = response.get(
            "ContentLength",
            0,
        )

        logger.info(
            "S3 object verified "
            "bucket=%s key=%s size_bytes=%s",
            S3_BUCKET_NAME,
            s3_key,
            size,
        )

        return True

    except ClientError:
        logger.exception(
            "Unable to verify S3 object "
            "bucket=%s key=%s",
            S3_BUCKET_NAME,
            s3_key,
        )

        raise


def build_bronze_s3_key(
    table_name: str,
    ingestion_time,
    filename: str,
) -> str:
    """
    Build the Bronze S3 object key.

    Example:

    bronze/orders/year=2026/month=08/day=09/file.parquet
    """

    return (
        f"bronze/"
        f"{table_name}/"
        f"year={ingestion_time.year}/"
        f"month={ingestion_time.month:02d}/"
        f"day={ingestion_time.day:02d}/"
        f"{filename}"
    )


