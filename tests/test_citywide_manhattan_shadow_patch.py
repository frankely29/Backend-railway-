from __future__ import annotations

import math
from pathlib import Path

import duckdb
import pytest

from zone_earnings_engine import build_zone_earnings_shadow_sql
from zone_mode_profiles import ZONE_MODE_PROFILES


def _write_citywide_manhattan_patch_parquet(path: Path) -> None:
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE TABLE trips (
          PULocationID INTEGER,
          DOLocationID INTEGER,
          pickup_datetime TIMESTAMP,
          request_datetime TIMESTAMP,
          driver_pay DOUBLE,
          trip_time DOUBLE,
          trip_miles DOUBLE,
          shared_match_flag INTEGER,
          shared_request_flag INTEGER
        )
        """
    )

    rows: list[tuple[int, int, str, str, float, float, float, int, int]] = []

    def add_rows(
        pu: int,
        do: int,
        pickup_ts: str,
        request_ts: str,
        pay: float,
        trip_time: float,
        trip_miles: float,
        n: int,
    ) -> None:
        for _ in range(n):
            rows.append((pu, do, pickup_ts, request_ts, pay, trip_time, trip_miles, 0, 0))

    # Manhattan trap zone 101: busy, short, same-zone, weak continuation.
    add_rows(101, 101, "2025-01-06 08:05:00", "2025-01-06 08:00:00", 8.0, 480.0, 1.2, 12)
    add_rows(101, 101, "2025-01-06 08:25:00", "2025-01-06 08:20:00", 8.5, 510.0, 1.4, 10)

    # Manhattan escape zone 102: similarly busy, better trip mix and continuation.
    add_rows(102, 201, "2025-01-06 08:05:00", "2025-01-06 08:00:00", 20.0, 1500.0, 7.5, 8)
    add_rows(102, 202, "2025-01-06 08:07:00", "2025-01-06 08:02:00", 18.0, 1320.0, 6.0, 4)
    add_rows(102, 201, "2025-01-06 08:25:00", "2025-01-06 08:20:00", 19.0, 1440.0, 6.8, 7)
    add_rows(102, 202, "2025-01-06 08:27:00", "2025-01-06 08:22:00", 17.5, 1260.0, 5.4, 3)

    # Queens control zone 201: moderate / balanced profile.
    add_rows(201, 202, "2025-01-06 08:06:00", "2025-01-06 08:01:00", 13.0, 900.0, 4.0, 4)
    add_rows(201, 201, "2025-01-06 08:09:00", "2025-01-06 08:04:00", 10.0, 600.0, 2.2, 3)
    add_rows(201, 202, "2025-01-06 08:26:00", "2025-01-06 08:21:00", 13.5, 960.0, 4.3, 4)
    add_rows(201, 201, "2025-01-06 08:29:00", "2025-01-06 08:24:00", 9.5, 540.0, 1.8, 3)

    # Queens destination/support zone 202.
    add_rows(202, 201, "2025-01-06 08:08:00", "2025-01-06 08:03:00", 12.0, 840.0, 3.6, 3)
    add_rows(202, 202, "2025-01-06 08:11:00", "2025-01-06 08:06:00", 10.5, 660.0, 2.4, 2)
    add_rows(202, 201, "2025-01-06 08:28:00", "2025-01-06 08:23:00", 12.2, 900.0, 3.8, 3)
    add_rows(202, 202, "2025-01-06 08:31:00", "2025-01-06 08:26:00", 10.0, 600.0, 2.1, 2)

    con.executemany(
        """
        INSERT INTO trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    con.execute(f"COPY trips TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def _make_citywide_shadow_duckdb(tmp_path: Path) -> tuple[duckdb.DuckDBPyConnection, Path]:
    parquet_path = tmp_path / "citywide_manhattan_patch.parquet"
    _write_citywide_manhattan_patch_parquet(parquet_path)

    con = duckdb.connect(database=":memory:")
    con.execute("CREATE TEMP TABLE zone_geometry_metrics (PULocationID INTEGER, zone_area_sq_miles DOUBLE, centroid_latitude DOUBLE)")
    con.execute("INSERT INTO zone_geometry_metrics VALUES (101, 0.20, 40.770)")
    con.execute("INSERT INTO zone_geometry_metrics VALUES (102, 0.20, 40.770)")
    con.execute("INSERT INTO zone_geometry_metrics VALUES (201, 0.20, 40.740)")
    con.execute("INSERT INTO zone_geometry_metrics VALUES (202, 0.20, 40.740)")

    con.execute(
        "CREATE TEMP TABLE zone_metadata (PULocationID INTEGER, zone_name VARCHAR, borough_name VARCHAR, airport_excluded BOOLEAN)"
    )
    con.execute("INSERT INTO zone_metadata VALUES (101, 'Manhattan Trap Zone', 'Manhattan', FALSE)")
    con.execute("INSERT INTO zone_metadata VALUES (102, 'Manhattan Escape Zone', 'Manhattan', FALSE)")
    con.execute("INSERT INTO zone_metadata VALUES (201, 'Queens Control Zone', 'Queens', FALSE)")
    con.execute("INSERT INTO zone_metadata VALUES (202, 'Queens Destination Zone', 'Queens', FALSE)")

    return con, parquet_path


def _run_shadow_sql(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> list[dict]:
    sql = build_zone_earnings_shadow_sql(
        [str(parquet_path)],
        bin_minutes=20,
        min_trips_per_window=1,
        profile=ZONE_MODE_PROFILES["citywide_v2"],
        citywide_v3_profile=ZONE_MODE_PROFILES["citywide_v3"],
        manhattan_profile=ZONE_MODE_PROFILES["manhattan_v2"],
        bronx_wash_heights_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v2"],
        queens_profile=ZONE_MODE_PROFILES["queens_v2"],
        brooklyn_profile=ZONE_MODE_PROFILES["brooklyn_v2"],
        staten_island_profile=ZONE_MODE_PROFILES["staten_island_v2"],
        manhattan_v3_profile=ZONE_MODE_PROFILES["manhattan_v3"],
        bronx_wash_heights_v3_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v3"],
        queens_v3_profile=ZONE_MODE_PROFILES["queens_v3"],
        brooklyn_v3_profile=ZONE_MODE_PROFILES["brooklyn_v3"],
        staten_island_v3_profile=ZONE_MODE_PROFILES["staten_island_v3"],
        available_columns={"request_datetime", "shared_match_flag", "shared_request_flag"},
    )
    cur = con.execute(sql)
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    borough_by_zone = {
        int(row[0]): row[1]
        for row in con.execute("SELECT PULocationID, borough_name FROM zone_metadata").fetchall()
    }
    for row in rows:
        row.setdefault("borough_name", borough_by_zone.get(int(row["PULocationID"])))
    return rows


def _row_by_zone(rows: list[dict], zone_id: int, dow_m: int = 0, bin_start_min: int = 480) -> dict:
    matches = [
        row
        for row in rows
        if int(row["PULocationID"]) == zone_id and int(row["dow_m"]) == dow_m and int(row["bin_start_min"]) == bin_start_min
    ]
    assert matches, f"Missing row for zone={zone_id}, dow_m={dow_m}, bin_start_min={bin_start_min}"
    assert len(matches) == 1, f"Expected one row for zone={zone_id}, got {len(matches)}"
    return matches[0]


def test_citywide_manhattan_patch_trap_zone_scores_below_escape_zone(tmp_path: Path) -> None:
    con, parquet_path = _make_citywide_shadow_duckdb(tmp_path)
    rows = _run_shadow_sql(con, parquet_path)

    trap = _row_by_zone(rows, 101)
    escape = _row_by_zone(rows, 102)

    assert trap["borough_name"] == "Manhattan"
    assert escape["borough_name"] == "Manhattan"

    assert float(trap["short_trip_share_3mi_12min"]) > float(escape["short_trip_share_3mi_12min"])
    assert float(trap["same_zone_dropoff_share"]) > float(escape["same_zone_dropoff_share"])
    assert float(escape["balanced_trip_share"]) > float(trap["balanced_trip_share"])
    assert float(escape["long_trip_share_20plus"]) > float(trap["long_trip_share_20plus"])
    assert float(escape["downstream_next_value_raw"]) > float(trap["downstream_next_value_raw"])
    assert float(escape["earnings_shadow_score_citywide_v3"]) > float(trap["earnings_shadow_score_citywide_v3"])
    assert float(escape["earnings_shadow_rating_citywide_v3"]) > float(trap["earnings_shadow_rating_citywide_v3"])

    con.close()


def test_citywide_manhattan_patch_exposes_manhattan_only_debug_fields(tmp_path: Path) -> None:
    con, parquet_path = _make_citywide_shadow_duckdb(tmp_path)
    rows = _run_shadow_sql(con, parquet_path)

    trap = _row_by_zone(rows, 101)
    escape = _row_by_zone(rows, 102)

    for row in (trap, escape):
        for field_name in (
            "manhattan_core_saturation_proxy_n_shadow",
            "manhattan_core_saturation_penalty_n_shadow",
            "citywide_manhattan_saturation_discount_factor_shadow",
            "earnings_shadow_positive_citywide_v3",
            "earnings_shadow_negative_citywide_v3",
            "earnings_shadow_trip_mix_positive_citywide_v3",
            "earnings_shadow_saturation_penalty_citywide_v3",
        ):
            assert field_name in row
            value = row[field_name]
            assert value is not None
            assert math.isfinite(float(value))

    assert float(trap["citywide_manhattan_saturation_discount_factor_shadow"]) < 1.0
    assert float(escape["citywide_manhattan_saturation_discount_factor_shadow"]) <= 1.0
    assert float(trap["earnings_shadow_saturation_penalty_citywide_v3"]) > 0.0

    con.close()


def test_citywide_manhattan_patch_does_not_spill_manhattan_discount_into_queens(tmp_path: Path) -> None:
    con, parquet_path = _make_citywide_shadow_duckdb(tmp_path)
    rows = _run_shadow_sql(con, parquet_path)

    queens_control = _row_by_zone(rows, 201)
    assert queens_control["borough_name"] == "Queens"
    assert float(queens_control["manhattan_core_saturation_proxy_n_shadow"]) == pytest.approx(0.0)
    assert float(queens_control["manhattan_core_saturation_penalty_n_shadow"]) == pytest.approx(0.0)
    assert float(queens_control["citywide_manhattan_saturation_discount_factor_shadow"]) == pytest.approx(1.0)

    con.close()


def test_citywide_manhattan_patch_keeps_citywide_and_manhattan_mode_scores_separate(tmp_path: Path) -> None:
    # citywide_v3 is the default no-mode path; manhattan_v3 is the Manhattan Mode path.
    # This patch is intended to improve only citywide_v3 behavior in core Manhattan.
    con, parquet_path = _make_citywide_shadow_duckdb(tmp_path)
    rows = _run_shadow_sql(con, parquet_path)

    row = _row_by_zone(rows, 101)

    for field_name in (
        "earnings_shadow_score_citywide_v3",
        "earnings_shadow_score_manhattan_v3",
        "earnings_shadow_rating_citywide_v3",
        "earnings_shadow_rating_manhattan_v3",
    ):
        assert field_name in row
        value = row[field_name]
        assert value is not None
        assert math.isfinite(float(value))

    con.close()
