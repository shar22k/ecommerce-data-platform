
import os

from dotenv import load_dotenv


load_dotenv()


S3_BUCKET_NAME = os.getenv(
    "S3_BUCKET_NAME"
)


if not S3_BUCKET_NAME:
    raise ValueError(
        "S3_BUCKET_NAME is missing from .env"
    )


def bronze_path(
    table_name: str,
) -> str:
    return (
        f"s3a://{S3_BUCKET_NAME}/"
        f"bronze/{table_name}/"
    )


def silver_path(
    table_name: str,
) -> str:
    return (
        f"s3a://{S3_BUCKET_NAME}/"
        f"silver/{table_name}/"
    )


def rejected_path(
    table_name: str,
) -> str:
    return (
        f"s3a://{S3_BUCKET_NAME}/"
        f"silver/rejected/{table_name}/"
    )

