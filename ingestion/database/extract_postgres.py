import argparse
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from psycopg2 import sql

from ingestion.database.postgres import (
    get_postgres_connection,
)
from ingestion.utils.logger import get_logger
from ingestion.utils.s3 import (
    build_bronze_s3_key,
    upload_file_to_s3,
    verify_s3_object,
)
from ingestion.utils.validation import (
    count_batch_parquet_rows,
    validate_row_count,
)
from ingestion.utils.watermark import (
    get_watermark,
    update_watermark,
)


logger = get_logger(__name__)

CONFIG_PATH = Path("config/tables.yaml")


def load_table_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Config file not found: {CONFIG_PATH}"
        )

    with CONFIG_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        config = yaml.safe_load(file)

    if not config or "tables" not in config:
        raise ValueError(
            f"Invalid config file. "
            f"Expected 'tables' section in {CONFIG_PATH}"
        )

    return config["tables"]


def build_full_query(
    connection,
    table_name: str,
) -> str:

    query = sql.SQL(
        "SELECT * FROM {}"
    ).format(
        sql.Identifier(table_name)
    )

    return query.as_string(connection)


def build_incremental_query(
    connection,
    table_name: str,
    watermark_column: str,
    watermark_value: Optional[str],
) -> tuple[str, Optional[tuple]]:

    if watermark_value is None:

        logger.info(
            "No existing watermark found for table=%s. "
            "Performing initial full extraction.",
            table_name,
        )

        query = sql.SQL(
            "SELECT * FROM {}"
        ).format(
            sql.Identifier(table_name)
        )

        return (
            query.as_string(connection),
            None,
        )

    query = sql.SQL(
        """
        SELECT *
        FROM {}
        WHERE {} > %s
        ORDER BY {} ASC
        """
    ).format(
        sql.Identifier(table_name),
        sql.Identifier(watermark_column),
        sql.Identifier(watermark_column),
    )

    return (
        query.as_string(connection),
        (watermark_value,),
    )


def get_full_source_count(
    connection,
    table_name: str,
) -> int:

    count_query = sql.SQL(
        "SELECT COUNT(*) FROM {}"
    ).format(
        sql.Identifier(table_name)
    )

    with connection.cursor() as cursor:
        cursor.execute(count_query)
        result = cursor.fetchone()

    return result[0]


def get_incremental_source_count(
    connection,
    table_name: str,
    watermark_column: str,
    watermark_value: Optional[str],
) -> int:

    if watermark_value is None:
        return get_full_source_count(
            connection,
            table_name,
        )

    count_query = sql.SQL(
        """
        SELECT COUNT(*)
        FROM {}
        WHERE {} > %s
        """
    ).format(
        sql.Identifier(table_name),
        sql.Identifier(watermark_column),
    )

    with connection.cursor() as cursor:
        cursor.execute(
            count_query,
            (watermark_value,),
        )

        result = cursor.fetchone()

    return result[0]


def get_max_watermark(
    connection,
    table_name: str,
    watermark_column: str,
):

    query = sql.SQL(
        """
        SELECT MAX({})
        FROM {}
        """
    ).format(
        sql.Identifier(watermark_column),
        sql.Identifier(table_name),
    )

    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()

    if not result:
        return None

    return result[0]


def create_output_directory(
    table_name: str,
    ingestion_time: datetime,
) -> str:

    output_dir = os.path.join(
        "data",
        "bronze",
        table_name,
        f"year={ingestion_time.year}",
        f"month={ingestion_time.month:02d}",
        f"day={ingestion_time.day:02d}",
    )

    os.makedirs(
        output_dir,
        exist_ok=True,
    )

    return output_dir


def extract_table(
    table_name: str,
    load_type: str,
    chunk_size: int = 50_000,
    watermark_column: Optional[str] = None,
) -> None:

    connection = None

    try:

        logger.info(
            "Starting extraction "
            "table=%s load_type=%s",
            table_name,
            load_type,
        )

        connection = get_postgres_connection()

        previous_watermark = None
        query_params = None

        # ---------------------------------------------------------
        # Determine extraction mode
        # ---------------------------------------------------------

        if load_type == "full":

            source_row_count = get_full_source_count(
                connection,
                table_name,
            )

            query_string = build_full_query(
                connection,
                table_name,
            )

        elif load_type == "incremental":

            if not watermark_column:
                raise ValueError(
                    f"watermark_column is required "
                    f"for incremental table '{table_name}'"
                )

            previous_watermark = get_watermark(
                table_name
            )

            logger.info(
                "Current saved watermark "
                "table=%s watermark=%s",
                table_name,
                previous_watermark,
            )

            source_row_count = (
                get_incremental_source_count(
                    connection=connection,
                    table_name=table_name,
                    watermark_column=watermark_column,
                    watermark_value=previous_watermark,
                )
            )

            query_string, query_params = (
                build_incremental_query(
                    connection=connection,
                    table_name=table_name,
                    watermark_column=watermark_column,
                    watermark_value=previous_watermark,
                )
            )

        else:
            raise ValueError(
                f"Unsupported load_type='{load_type}'"
            )

        logger.info(
            "Rows expected for extraction "
            "table=%s rows=%s",
            table_name,
            source_row_count,
        )

        # ---------------------------------------------------------
        # Nothing new for incremental table
        # ---------------------------------------------------------

        if (
            load_type == "incremental"
            and source_row_count == 0
        ):

            logger.info(
                "No new rows found "
                "table=%s watermark=%s",
                table_name,
                previous_watermark,
            )

            return

        # ---------------------------------------------------------
        # Batch metadata
        # ---------------------------------------------------------

        batch_id = str(
            uuid.uuid4()
        )

        ingestion_time = datetime.now(
            timezone.utc
        )

        output_dir = create_output_directory(
            table_name=table_name,
            ingestion_time=ingestion_time,
        )

        chunk_number = 0
        total_rows = 0

        uploaded_s3_keys = []

        read_sql_kwargs = {
            "sql": query_string,
            "con": connection,
            "chunksize": chunk_size,
        }

        if query_params is not None:
            read_sql_kwargs["params"] = query_params

        # ---------------------------------------------------------
        # Extract chunks
        # ---------------------------------------------------------

        for dataframe in pd.read_sql_query(
            **read_sql_kwargs
        ):

            chunk_number += 1

            row_count = len(
                dataframe
            )

            total_rows += row_count

            # -----------------------------------------------------
            # Technical metadata
            # -----------------------------------------------------

            dataframe["_ingested_at"] = (
                ingestion_time
            )

            dataframe["_batch_id"] = (
                batch_id
            )

            dataframe["_source_system"] = (
                "postgres"
            )

            dataframe["_source_table"] = (
                table_name
            )

            filename = (
                f"{table_name}_"
                f"{batch_id}_"
                f"part_{chunk_number}.parquet"
            )

            local_file = os.path.join(
                output_dir,
                filename,
            )

            # -----------------------------------------------------
            # Write local temporary Bronze file
            # -----------------------------------------------------

            dataframe.to_parquet(
                local_file,
                engine="pyarrow",
                index=False,
            )

            logger.info(
                "Local Bronze written "
                "table=%s chunk=%s rows=%s file=%s",
                table_name,
                chunk_number,
                row_count,
                local_file,
            )

            # -----------------------------------------------------
            # Build S3 Bronze key
            # -----------------------------------------------------

            s3_key = build_bronze_s3_key(
                table_name=table_name,
                ingestion_time=ingestion_time,
                filename=filename,
            )

            # -----------------------------------------------------
            # Upload to S3
            # -----------------------------------------------------

            upload_file_to_s3(
                local_file=local_file,
                s3_key=s3_key,
            )

            # -----------------------------------------------------
            # Verify uploaded object
            # -----------------------------------------------------

            verify_s3_object(
                s3_key=s3_key,
            )

            uploaded_s3_keys.append(
                s3_key
            )

        # ---------------------------------------------------------
        # Validate local batch row count
        # ---------------------------------------------------------

        bronze_row_count = (
            count_batch_parquet_rows(
                output_dir=output_dir,
                table_name=table_name,
                batch_id=batch_id,
            )
        )

        validate_row_count(
            source_row_count=source_row_count,
            bronze_row_count=bronze_row_count,
            table_name=table_name,
        )

        logger.info(
            "Row count validation successful "
            "table=%s source=%s bronze=%s",
            table_name,
            source_row_count,
            bronze_row_count,
        )

        # ---------------------------------------------------------
        # Update incremental watermark
        # only after:
        #
        # extraction success
        # local write success
        # S3 upload success
        # S3 verification success
        # row count validation success
        # ---------------------------------------------------------

        if load_type == "incremental":

            latest_watermark = get_max_watermark(
                connection=connection,
                table_name=table_name,
                watermark_column=watermark_column,
            )

            if latest_watermark is not None:

                watermark_string = (
                    latest_watermark.isoformat(
                        sep=" "
                    )
                )

                update_watermark(
                    table_name=table_name,
                    watermark_value=watermark_string,
                )

                logger.info(
                    "Watermark updated "
                    "table=%s old=%s new=%s",
                    table_name,
                    previous_watermark,
                    watermark_string,
                )

        logger.info(
            "Extraction completed "
            "table=%s load_type=%s "
            "rows=%s chunks=%s "
            "s3_objects=%s "
            "batch_id=%s",
            table_name,
            load_type,
            total_rows,
            chunk_number,
            len(uploaded_s3_keys),
            batch_id,
        )

    except Exception:

        logger.exception(
            "Extraction failed "
            "table=%s load_type=%s",
            table_name,
            load_type,
        )

        raise

    finally:

        if connection:

            connection.close()

            logger.info(
                "PostgreSQL connection closed "
                "table=%s",
                table_name,
            )


def run_table(
    config_name: str,
) -> None:

    tables = load_table_config()

    if config_name not in tables:

        available_tables = ", ".join(
            tables.keys()
        )

        raise ValueError(
            f"Table '{config_name}' not found. "
            f"Available tables: {available_tables}"
        )

    table_config = tables[
        config_name
    ]

    source_table = table_config[
        "source_table"
    ]

    load_type = table_config.get(
        "load_type",
        "full",
    )

    chunk_size = table_config.get(
        "chunk_size",
        50_000,
    )

    watermark_column = table_config.get(
        "watermark_column"
    )

    extract_table(
        table_name=source_table,
        load_type=load_type,
        chunk_size=chunk_size,
        watermark_column=watermark_column,
    )


def run_all_tables() -> None:

    tables = load_table_config()

    logger.info(
        "Starting ingestion for %s tables",
        len(tables),
    )

    failed_tables = []

    for config_name in tables:

        try:

            run_table(
                config_name
            )

        except Exception:

            failed_tables.append(
                config_name
            )

            logger.exception(
                "Table ingestion failed "
                "config_name=%s",
                config_name,
            )

    if failed_tables:

        raise RuntimeError(
            "Ingestion failed for tables: "
            f"{failed_tables}"
        )

    logger.info(
        "All configured tables completed successfully"
    )


def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "PostgreSQL to S3 Bronze "
            "e-commerce ingestion pipeline"
        )
    )

    parser.add_argument(
        "table",
        nargs="?",
        help="Table name from config/tables.yaml",
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Extract all configured tables",
    )

    args = parser.parse_args()

    if args.all and args.table:
        parser.error(
            "Use either table name or --all."
        )

    if args.all:
        run_all_tables()
        return

    if args.table:
        run_table(
            args.table
        )
        return

    parser.print_help()


if __name__ == "__main__":
    main()

