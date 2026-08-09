from pathlib import Path

import pandas as pd


def count_batch_parquet_rows(
    output_dir: str,
    table_name: str,
    batch_id: str,
) -> int:
    """
    Count rows written to Parquet files
    belonging only to the current batch.
    """

    path = Path(output_dir)

    parquet_files = list(
        path.glob(
            f"{table_name}_{batch_id}_part_*.parquet"
        )
    )

    if not parquet_files:
        return 0

    total_rows = 0

    for parquet_file in parquet_files:
        dataframe = pd.read_parquet(
            parquet_file
        )

        total_rows += len(dataframe)

    return total_rows


def validate_row_count(
    source_row_count: int,
    bronze_row_count: int,
    table_name: str,
) -> None:
    """
    Validate that all expected source rows
    were written to Bronze.
    """

    if source_row_count != bronze_row_count:
        raise ValueError(
            f"Row count validation failed for '{table_name}'. "
            f"Source={source_row_count}, "
            f"Bronze={bronze_row_count}"
        )
