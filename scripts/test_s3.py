from ingestion.utils.s3 import test_s3_connection


def main():
    test_s3_connection()

    print(
        "S3 connection test successful."
    )


if __name__ == "__main__":
    main()
