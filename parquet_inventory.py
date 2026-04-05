from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

MONTH_NAME_TO_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "dicember": 12,
}


def _infer_month_alias(filename: str) -> str | None:
    stem = Path(filename).stem.lower()

    numeric_match = re.search(r"(20\d{2})[-_](\d{1,3})", stem)
    if numeric_match:
        year, month_raw = numeric_match.groups()
        try:
            month_int = int(month_raw)
        except ValueError:
            month_int = -1
        if 1 <= month_int <= 12:
            return f"{year}-{month_int:02d}"

    month_name_match = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december|dicember)[-_](20\d{2})",
        stem,
    )
    if not month_name_match:
        return None

    month_name, year = month_name_match.groups()
    month_int = MONTH_NAME_TO_NUM.get(month_name)
    if month_int is None:
        return None
    return f"{year}-{month_int:02d}"


def inspect_parquet_inventory(parquet_files: List[Path]) -> Dict[str, Any]:
    import duckdb
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    month_to_files: Dict[str, List[str]] = {}

    con = duckdb.connect(database=":memory:")
    try:
        for parquet_path in parquet_files:
            file_sql = str(parquet_path).replace("'", "''")
            min_ts, max_ts, row_count = con.execute(
                f"""
                SELECT
                  MIN(pickup_datetime) AS min_pickup_datetime,
                  MAX(pickup_datetime) AS max_pickup_datetime,
                  COUNT(*) AS row_count
                FROM read_parquet('{file_sql}')
                """
            ).fetchone()
            inferred_month = _infer_month_alias(parquet_path.name)
            if inferred_month:
                month_to_files.setdefault(inferred_month, []).append(parquet_path.name)
            rows.append(
                {
                    "filename": parquet_path.name,
                    "path": str(parquet_path),
                    "min_pickup_datetime": None if min_ts is None else str(min_ts),
                    "max_pickup_datetime": None if max_ts is None else str(max_ts),
                    "row_count": int(row_count or 0),
                    "month_inferred_from_filename": inferred_month,
                }
            )
    finally:
        con.close()

    for month, files in month_to_files.items():
        if len(files) > 1:
            warnings.append(f"Duplicate month alias detected for {month}: {', '.join(sorted(files))}")

    for idx, left in enumerate(rows):
        for right in rows[idx + 1 :]:
            if (
                left.get("min_pickup_datetime")
                and left.get("max_pickup_datetime")
                and left["min_pickup_datetime"] == right.get("min_pickup_datetime")
                and left["max_pickup_datetime"] == right.get("max_pickup_datetime")
            ):
                warnings.append(
                    "Potential duplicate parquet date span: "
                    f"{left['filename']} and {right['filename']} share "
                    f"{left['min_pickup_datetime']} to {left['max_pickup_datetime']}"
                )

    return {
        "rows": rows,
        "warnings": sorted(set(warnings)),
        "warning_count": len(set(warnings)),
    }
