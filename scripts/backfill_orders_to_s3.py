from pathlib import Path
from ingestion.utils.s3 import (

    upload_file_to_s3,
    verify_s3_object,
)


LOCAL_BRONZE_ROOT = Path(
    "data/bronze/orders"
)


def main():
    if not LOCAL_BRONZE_ROOT.exists():
        raise FileNotFoundError(
            f"Local Bronze orders folder not found: "
            f"{LOCAL_BRONZE_ROOT}"
        )

    parquet_files = list(
        LOCAL_BRONZE_ROOT.rglob("*.parquet")
    )

    if not parquet_files:
        raise RuntimeError(
            "No local orders Parquet files found."
        )

    print(
        f"Found {len(parquet_files)} "
        f"orders Parquet files."
    )

    uploaded = 0

    for local_file in parquet_files:

        relative_path = local_file.relative_to(
            LOCAL_BRONZE_ROOT
        )

        s3_key = (
            f"bronze/orders/"
            f"{relative_path.as_posix()}"
        )

        print()
        print(
            f"Uploading: {local_file}"
        )
        print(
            f"To: s3://data-engineering-project-kv/"
            f"{s3_key}"
        )

        upload_file_to_s3(
            local_file=str(local_file),
            s3_key=s3_key,
        )

        verify_s3_object(
            s3_key=s3_key,
        )

        uploaded += 1

    print()
    print(
        f"Backfill completed. "
        f"Uploaded {uploaded} files."
    )


if __name__ == "__main__":
    main()

