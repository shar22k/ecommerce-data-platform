import json
from pathlib import Path
from typing import Optional


WATERMARK_FILE = Path("data/watermarks.json")


def load_watermarks() -> dict:
    """
    Load all saved table watermarks.

    Example:
    {
        "orders": "2018-10-17 17:30:18"
    }
    """

    if not WATERMARK_FILE.exists():
        return {}

    with WATERMARK_FILE.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def get_watermark(
    table_name: str,
) -> Optional[str]:
    """
    Return the saved watermark for a table.

    Returns None when the table has never been
    incrementally loaded before.
    """

    watermarks = load_watermarks()

    return watermarks.get(table_name)


def update_watermark(
    table_name: str,
    watermark_value: str,
) -> None:
    """
    Save/update the watermark for a table.
    """

    WATERMARK_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    watermarks = load_watermarks()

    watermarks[table_name] = watermark_value

    with WATERMARK_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            watermarks,
            file,
            indent=2,
        )

