from __future__ import annotations

import json
import math
from pathlib import Path

import duckdb

from build_hotspot import build_hotspots_frames
from zone_earnings_engine import build_zone_earnings_shadow_sql
from zone_geometry_metrics import build_zone_geometry_metrics_rows
from zone_mode_profiles import ZONE_MODE_PROFILES


def _write_sample_geojson(path: Path) -> None:
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "LocationID": 46,
                    "zone": "City Island",
                    "borough": "Bronx",
                },
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [[[-73.79, 40.84], [-73.78, 40.84], [-73.78, 40.85], [-73.79, 40.85], [-73.79, 40.84]]],
                        [[[-73.80, 40.83], [-73.795, 40.83], [-73.795, 40.835], [-73.80, 40.835], [-73.80, 40.83]]],
                    ],
                },
            },
            {
                "type": "Feature",
                "properties": {
                    "LocationID": 132,
                    "zone": "JFK Airport",
                    "borough": "Queens",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-73.9, 40.6], [-73.85, 40.6], [-73.85, 40.65], [-73.9, 40.65], [-73.9, 40.6]]],
                },
            },
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_sample_parquet(path: Path) -> None:
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE TABLE trips AS
        SELECT * FROM (
            VALUES
                (46, 132, TIMESTAMP '2025-01-06 08:05:00', TIMESTAMP '2025-01-06 08:00:00', 22.0, 900.0, 4.0, 0, 0),
                (46, 132, TIMESTAMP '2025-01-06 08:25:00', TIMESTAMP '2025-01-06 08:20:00', 18.0, 840.0, 3.5, 0, 0)
        ) AS t(PULocationID, DOLocationID, pickup_datetime, request_datetime, driver_pay, trip_time, trip_miles, shared_match_flag, shared_request_flag)
        """
    )
    con.execute(f"COPY trips TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def test_build_zone_geometry_metrics_rows_city_island_area(tmp_path: Path) -> None:
    zones_geojson_path = tmp_path / "taxi_zones.geojson"
    _write_sample_geojson(zones_geojson_path)

    rows = build_zone_geometry_metrics_rows(zones_geojson_path)
    row_by_id = {int(row["PULocationID"]): row for row in rows}

    assert 46 in row_by_id
    assert row_by_id[46]["zone_area_sq_miles"] is not None
    assert float(row_by_id[46]["zone_area_sq_miles"]) > 0.0


def test_shadow_sql_generates_finite_popup_metrics_for_non_airport_zone(tmp_path: Path) -> None:
    parquet_path = tmp_path / "sample.parquet"
    _write_sample_parquet(parquet_path)

    con = duckdb.connect(database=":memory:")
    con.execute("CREATE TEMP TABLE zone_geometry_metrics (PULocationID INTEGER, zone_area_sq_miles DOUBLE, centroid_latitude DOUBLE)")
    con.execute("INSERT INTO zone_geometry_metrics VALUES (46, 0.25, 40.845)")
    con.execute("CREATE TEMP TABLE zone_metadata (PULocationID INTEGER, zone_name VARCHAR, borough_name VARCHAR, airport_excluded BOOLEAN)")
    con.execute("INSERT INTO zone_metadata VALUES (46, 'City Island', 'Bronx', FALSE)")
    con.execute("INSERT INTO zone_metadata VALUES (132, 'JFK Airport', 'Queens', TRUE)")

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
    rows = [dict(zip(cols, row)) for row in cur.fetchall() if int(row[0]) == 46]

    assert rows, "Expected at least one shadow row for City Island"
    for row in rows:
        for field_name in (
            "pickups_now",
            "pickups_next",
            "zone_area_sq_miles",
            "pickups_per_sq_mile_now",
            "pickups_per_sq_mile_next",
        ):
            value = row[field_name]
            assert value is not None
            assert math.isfinite(float(value))
        assert float(row["pickups_now"]) >= 0.0
        assert float(row["pickups_next"]) >= 0.0
        assert float(row["zone_area_sq_miles"]) > 0.0
        assert float(row["pickups_per_sq_mile_now"]) >= 0.0
        assert float(row["pickups_per_sq_mile_next"]) >= 0.0

    con.close()


def test_build_hotspots_frames_handles_city_island_popup_metrics(tmp_path: Path) -> None:
    zones_geojson_path = tmp_path / "taxi_zones.geojson"
    out_dir = tmp_path / "frames"
    parquet_path = tmp_path / "sample.parquet"

    _write_sample_geojson(zones_geojson_path)
    _write_sample_parquet(parquet_path)

    result = build_hotspots_frames(
        parquet_files=[parquet_path],
        zones_geojson_path=zones_geojson_path,
        out_dir=out_dir,
        bin_minutes=20,
        min_trips_per_window=1,
    )

    assert result["count"] >= 1
    frame_path = out_dir / "frame_000000.json"
    assert frame_path.exists()

    frame = json.loads(frame_path.read_text(encoding="utf-8"))
    city_island = next(
        feat for feat in frame["polygons"]["features"] if int(feat["properties"]["LocationID"]) == 46
    )
    props = city_island["properties"]
    for field_name in (
        "pickups_now_shadow",
        "next_pickups_shadow",
        "zone_area_sq_miles_shadow",
        "pickups_per_sq_mile_now_shadow",
        "pickups_per_sq_mile_next_shadow",
    ):
        value = props[field_name]
        assert value is not None
        assert math.isfinite(float(value))

    assert float(props["pickups_now_shadow"]) >= 0.0
    assert float(props["next_pickups_shadow"]) >= 0.0
    assert float(props["zone_area_sq_miles_shadow"]) > 0.0
    assert float(props["pickups_per_sq_mile_now_shadow"]) >= 0.0
    assert float(props["pickups_per_sq_mile_next_shadow"]) >= 0.0
