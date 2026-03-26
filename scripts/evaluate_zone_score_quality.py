from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional

import duckdb

from build_hotspot import ensure_zones_geojson, is_airport_zone
from zone_earnings_engine import build_zone_earnings_shadow_sql
from zone_geometry_metrics import build_zone_geometry_metrics_rows
from zone_mode_profiles import ZONE_MODE_PROFILES

LIVE_PROFILES: List[str] = [
    "citywide_v3",
    "manhattan_v3",
    "bronx_wash_heights_v3",
    "queens_v3",
    "brooklyn_v3",
    "staten_island_v3",
]


def _avg(values: List[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return None
    return float(mean(clean))


def _decile_for_rating(rating: Any) -> Optional[int]:
    if rating is None:
        return None
    try:
        rating_i = int(rating)
    except Exception:
        return None
    return max(1, min(10, int(math.ceil(rating_i / 10.0))))


def _generally_trending(values: List[Optional[float]], direction: str) -> Dict[str, Any]:
    pairs: List[tuple[float, float]] = []
    for left, right in zip(values, values[1:]):
        if left is None or right is None:
            continue
        pairs.append((float(left), float(right)))

    if not pairs:
        return {
            "generally": None,
            "direction": direction,
            "comparisons": 0,
            "matching": 0,
            "ratio": None,
        }

    if direction == "up":
        matching = sum(1 for left, right in pairs if right >= left)
    else:
        matching = sum(1 for left, right in pairs if right <= left)

    ratio = matching / len(pairs)
    return {
        "generally": bool(ratio >= 0.70),
        "direction": direction,
        "comparisons": len(pairs),
        "matching": matching,
        "ratio": ratio,
    }


def _build_shadow_rows(data_dir: Path, bin_minutes: int, min_trips_per_window: int) -> List[Dict[str, Any]]:
    parquet_files = sorted(str(path) for path in data_dir.glob("*.parquet") if path.is_file())
    if not parquet_files:
        raise RuntimeError(f"No parquet files found in {data_dir}")

    zones_geojson_path = ensure_zones_geojson(data_dir)
    zone_rows = build_zone_geometry_metrics_rows(zones_geojson_path)
    zones = json.loads(zones_geojson_path.read_text(encoding="utf-8"))

    name_by_id: Dict[int, str] = {}
    borough_by_id: Dict[int, str] = {}
    for feature in zones.get("features", []):
        props = feature.get("properties") or {}
        zid = props.get("LocationID")
        if zid is None:
            continue
        try:
            zid_int = int(zid)
        except Exception:
            continue
        zone_name = props.get("zone") or props.get("Zone") or props.get("name") or props.get("Name") or ""
        borough = props.get("borough") or props.get("Borough") or props.get("boro") or props.get("Boro") or ""
        name_by_id[zid_int] = str(zone_name) if zone_name is not None else ""
        borough_by_id[zid_int] = str(borough) if borough is not None else ""

    con = duckdb.connect(database=":memory:")
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_files)
    schema_rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{parquet_sql}])").fetchall()
    available_columns = {str(row[0]) for row in schema_rows}

    con.execute("CREATE TEMP TABLE zone_geometry_metrics (PULocationID INTEGER, zone_area_sq_miles DOUBLE, centroid_latitude DOUBLE)")
    if zone_rows:
        con.executemany(
            "INSERT INTO zone_geometry_metrics (PULocationID, zone_area_sq_miles, centroid_latitude) VALUES (?, ?, ?)",
            [
                (
                    int(row["PULocationID"]),
                    None if row["zone_area_sq_miles"] is None else float(row["zone_area_sq_miles"]),
                    None if row.get("centroid_latitude") is None else float(row["centroid_latitude"]),
                )
                for row in zone_rows
            ],
        )

    con.execute("CREATE TEMP TABLE zone_metadata (PULocationID INTEGER, zone_name VARCHAR, borough_name VARCHAR, airport_excluded BOOLEAN)")
    con.executemany(
        "INSERT INTO zone_metadata (PULocationID, zone_name, borough_name, airport_excluded) VALUES (?, ?, ?, ?)",
        [
            (
                int(zid),
                str(name_by_id.get(zid, "") or ""),
                str(borough_by_id.get(zid, "") or ""),
                bool(is_airport_zone(zid, name_by_id.get(zid, ""), borough_by_id.get(zid, ""))),
            )
            for zid in sorted(name_by_id.keys())
        ],
    )

    shadow_sql = build_zone_earnings_shadow_sql(
        parquet_files,
        bin_minutes=int(bin_minutes),
        min_trips_per_window=int(min_trips_per_window),
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
        available_columns=available_columns,
    )

    rows = con.execute(shadow_sql).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _profile_report(rows: List[Dict[str, Any]], profile_name: str) -> Dict[str, Any]:
    rating_field = f"earnings_shadow_rating_{profile_name}"
    deciles: Dict[int, Dict[str, List[Optional[float]]]] = {
        decile: {
            "pickups_now": [],
            "pickups_next": [],
            "pickups_per_sq_mile_now": [],
            "pickups_per_sq_mile_next": [],
            "median_driver_pay": [],
            "short_trip_share": [],
            "same_zone_dropoff_share": [],
            "downstream_value": [],
        }
        for decile in range(1, 11)
    }
    row_counts: Dict[int, int] = {decile: 0 for decile in range(1, 11)}

    for row in rows:
        decile = _decile_for_rating(row.get(rating_field))
        if decile is None:
            continue
        row_counts[decile] += 1
        bucket = deciles[decile]
        bucket["pickups_now"].append(row.get("pickups_now"))
        bucket["pickups_next"].append(row.get("pickups_next"))
        bucket["pickups_per_sq_mile_now"].append(row.get("pickups_per_sq_mile_now"))
        bucket["pickups_per_sq_mile_next"].append(row.get("pickups_per_sq_mile_next"))
        bucket["median_driver_pay"].append(row.get("median_driver_pay"))
        bucket["short_trip_share"].append(row.get("short_trip_share_3mi_12min"))
        bucket["same_zone_dropoff_share"].append(row.get("same_zone_dropoff_share"))
        bucket["downstream_value"].append(row.get("downstream_next_value_raw"))

    decile_rows: List[Dict[str, Any]] = []
    pickups_next_series: List[Optional[float]] = []
    density_next_series: List[Optional[float]] = []
    short_trip_series: List[Optional[float]] = []
    same_zone_series: List[Optional[float]] = []

    for decile in range(1, 11):
        agg = {
            "decile": decile,
            "row_count": row_counts[decile],
            "avg_pickups_now": _avg(deciles[decile]["pickups_now"]),
            "avg_pickups_next": _avg(deciles[decile]["pickups_next"]),
            "avg_pickups_per_sq_mile_now": _avg(deciles[decile]["pickups_per_sq_mile_now"]),
            "avg_pickups_per_sq_mile_next": _avg(deciles[decile]["pickups_per_sq_mile_next"]),
            "avg_median_driver_pay": _avg(deciles[decile]["median_driver_pay"]),
            "avg_short_trip_share": _avg(deciles[decile]["short_trip_share"]),
            "avg_same_zone_dropoff_share": _avg(deciles[decile]["same_zone_dropoff_share"]),
            "avg_downstream_value": _avg(deciles[decile]["downstream_value"]),
        }
        decile_rows.append(agg)
        pickups_next_series.append(agg["avg_pickups_next"])
        density_next_series.append(agg["avg_pickups_per_sq_mile_next"])
        short_trip_series.append(agg["avg_short_trip_share"])
        same_zone_series.append(agg["avg_same_zone_dropoff_share"])

    return {
        "profile": profile_name,
        "rating_field": rating_field,
        "total_rows": int(sum(row_counts.values())),
        "deciles": decile_rows,
        "monotonic_diagnostics": {
            "pickups_next_generally_rises": _generally_trending(pickups_next_series, "up"),
            "pickups_per_sq_mile_next_generally_rises": _generally_trending(density_next_series, "up"),
            "short_trip_share_generally_falls": _generally_trending(short_trip_series, "down"),
            "same_zone_dropoff_share_generally_falls": _generally_trending(same_zone_series, "down"),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate live zone score quality using decile diagnostics.")
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--bin-minutes", type=int, default=20)
    parser.add_argument("--min-trips-per-window", type=int, default=25)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    rows = _build_shadow_rows(
        data_dir=args.data_dir,
        bin_minutes=int(args.bin_minutes),
        min_trips_per_window=int(args.min_trips_per_window),
    )

    report = {
        "profiles": [_profile_report(rows, profile) for profile in LIVE_PROFILES],
        "settings": {
            "data_dir": str(args.data_dir),
            "bin_minutes": int(args.bin_minutes),
            "min_trips_per_window": int(args.min_trips_per_window),
            "profile_names": LIVE_PROFILES,
        },
    }

    payload = json.dumps(report, indent=2, sort_keys=False)
    print(payload)

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(payload + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
