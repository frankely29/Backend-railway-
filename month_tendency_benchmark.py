from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import duckdb

BRONX_WASH_HEIGHTS_CORRIDOR_ZONE_IDS = {
    41, 42, 74, 75, 116, 127, 128, 151, 152, 166, 243, 244,
}
AIRPORT_ZONE_IDS = {1, 132, 138}
MANHATTAN_CORE_MAX_LATITUDE = 40.795


FAMILY_SPECS: List[Dict[str, str]] = [
    {
        "key": "citywide_all",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_citywide_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_citywide_v2 AS DOUBLE))",
        "predicate": "NOT zm.airport_excluded",
        "rating_field_family": "citywide_visible",
    },
    {
        "key": "auto_manhattan_citywide",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_citywide_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_citywide_v2 AS DOUBLE))",
        "predicate": "zm.is_manhattan = TRUE AND zm.in_bronx_wash_heights_corridor = FALSE AND NOT zm.airport_excluded",
        "rating_field_family": "citywide_visible",
    },
    {
        "key": "auto_bronx_wash_heights_citywide",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_citywide_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_citywide_v2 AS DOUBLE))",
        "predicate": "(zm.is_bronx = TRUE OR zm.in_bronx_wash_heights_corridor = TRUE) AND NOT zm.airport_excluded",
        "rating_field_family": "citywide_visible",
    },
    {
        "key": "auto_queens_citywide",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_citywide_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_citywide_v2 AS DOUBLE))",
        "predicate": "zm.is_queens = TRUE AND NOT zm.airport_excluded",
        "rating_field_family": "citywide_visible",
    },
    {
        "key": "auto_brooklyn_citywide",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_citywide_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_citywide_v2 AS DOUBLE))",
        "predicate": "zm.is_brooklyn = TRUE AND NOT zm.airport_excluded",
        "rating_field_family": "citywide_visible",
    },
    {
        "key": "mode_manhattan",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_manhattan_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_manhattan_v2 AS DOUBLE))",
        "predicate": "zm.is_manhattan = TRUE AND zm.in_bronx_wash_heights_corridor = FALSE AND zm.centroid_latitude IS NOT NULL AND zm.centroid_latitude <= 40.795 AND NOT zm.airport_excluded",
        "rating_field_family": "manhattan_visible",
    },
    {
        "key": "mode_bronx_wash_heights",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_bronx_wash_heights_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_bronx_wash_heights_v2 AS DOUBLE))",
        "predicate": "(zm.is_bronx = TRUE OR zm.in_bronx_wash_heights_corridor = TRUE) AND NOT zm.airport_excluded",
        "rating_field_family": "bronx_wash_heights_visible",
    },
    {
        "key": "mode_queens",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_queens_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_queens_v2 AS DOUBLE))",
        "predicate": "zm.is_queens = TRUE AND NOT zm.airport_excluded",
        "rating_field_family": "queens_visible",
    },
    {
        "key": "mode_brooklyn",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_brooklyn_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_brooklyn_v2 AS DOUBLE))",
        "predicate": "zm.is_brooklyn = TRUE AND NOT zm.airport_excluded",
        "rating_field_family": "brooklyn_visible",
    },
    {
        "key": "mode_staten_island",
        "rating_expr": "COALESCE(TRY_CAST(e.earnings_shadow_rating_staten_island_v3 AS DOUBLE), TRY_CAST(e.earnings_shadow_rating_staten_island_v2 AS DOUBLE))",
        "predicate": "zm.is_staten_island = TRUE AND NOT zm.airport_excluded",
        "rating_field_family": "staten_island_visible",
    },
]


def _normalized_borough_name(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalized_zone_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _is_airport_zone(location_id: Any, zone_name: Any, borough_name: Any) -> bool:
    try:
        if int(location_id) in AIRPORT_ZONE_IDS:
            return True
    except Exception:
        pass
    normalized_text = " ".join(
        part
        for part in (_normalized_zone_text(zone_name), _normalized_borough_name(borough_name))
        if part
    )
    return any(token in normalized_text for token in ("airport", "jfk", "la guardia", "laguardia", "newark", "ewr"))


def _iter_geometry_points(geometry: Any):
    if not isinstance(geometry, dict):
        return
    coords = geometry.get("coordinates")
    gtype = str(geometry.get("type") or "")
    if not coords:
        return
    if gtype == "Point":
        if len(coords) >= 2:
            yield float(coords[0]), float(coords[1])
        return

    def _walk(node: Any):
        if isinstance(node, (list, tuple)):
            if len(node) >= 2 and isinstance(node[0], (int, float)) and isinstance(node[1], (int, float)):
                yield float(node[0]), float(node[1])
            else:
                for child in node:
                    yield from _walk(child)

    yield from _walk(coords)


def _geometry_centroid_latitude(geometry: Any) -> float | None:
    points = list(_iter_geometry_points(geometry))
    if not points:
        return None
    return sum(lat for _, lat in points) / len(points)


def _zone_metadata_rows(zones_geojson_path: Path) -> List[Tuple[int, bool, bool, bool, bool, bool, bool, float | None]]:
    payload = json.loads(zones_geojson_path.read_text(encoding="utf-8"))
    features = payload.get("features") if isinstance(payload, dict) else None
    if not isinstance(features, list):
        return []

    rows: List[Tuple[int, bool, bool, bool, bool, bool, bool, float | None]] = []
    for feature in features:
        props = (feature or {}).get("properties") or {}
        geometry = (feature or {}).get("geometry")
        location_id_raw = props.get("LocationID") or props.get("location_id") or props.get("OBJECTID")
        try:
            location_id = int(location_id_raw)
        except Exception:
            continue

        borough = _normalized_borough_name(props.get("borough") or props.get("Borough"))
        zone_name = props.get("zone") or props.get("Zone")
        centroid_latitude = _geometry_centroid_latitude(geometry)

        rows.append(
            (
                location_id,
                "manhattan" in borough,
                "bronx" in borough,
                "queens" in borough,
                "brooklyn" in borough,
                "staten" in borough,
                bool(_is_airport_zone(location_id, zone_name, borough)),
                None if centroid_latitude is None else float(centroid_latitude),
            )
        )
    return rows


def build_month_tendency_benchmark(
    *,
    exact_store_path: Path,
    zones_geojson_path: Path,
    month_key: str,
    bin_minutes: int = 20,
) -> Dict[str, Any]:
    if not exact_store_path.exists() or exact_store_path.stat().st_size <= 0:
        raise FileNotFoundError(f"Missing month exact store: {exact_store_path}")
    if not zones_geojson_path.exists() or zones_geojson_path.stat().st_size <= 0:
        raise FileNotFoundError(f"Missing zones geojson: {zones_geojson_path}")

    zone_rows = _zone_metadata_rows(zones_geojson_path)
    con = duckdb.connect(database=str(exact_store_path), read_only=True)
    try:
        con.execute(
            """
            CREATE TEMP TABLE zone_month_tendency_meta (
                PULocationID INTEGER,
                is_manhattan BOOLEAN,
                is_bronx BOOLEAN,
                is_queens BOOLEAN,
                is_brooklyn BOOLEAN,
                is_staten_island BOOLEAN,
                airport_excluded BOOLEAN,
                centroid_latitude DOUBLE,
                in_bronx_wash_heights_corridor BOOLEAN
            )
            """
        )
        if zone_rows:
            con.executemany(
                """
                INSERT INTO zone_month_tendency_meta (
                    PULocationID,
                    is_manhattan,
                    is_bronx,
                    is_queens,
                    is_brooklyn,
                    is_staten_island,
                    airport_excluded,
                    centroid_latitude,
                    in_bronx_wash_heights_corridor
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        int(row[0]),
                        bool(row[1]),
                        bool(row[2]),
                        bool(row[3]),
                        bool(row[4]),
                        bool(row[5]),
                        bool(row[6]),
                        row[7],
                        int(row[0]) in BRONX_WASH_HEIGHTS_CORRIDOR_ZONE_IDS,
                    )
                    for row in zone_rows
                ],
            )

        union_queries: List[str] = []
        for spec in FAMILY_SPECS:
            union_queries.append(
                f"""
                SELECT
                    '{spec['key']}' AS family_key,
                    AVG(rating_value) AS average_rating,
                    COUNT(*) AS sample_zone_frames
                FROM (
                    SELECT {spec['rating_expr']} AS rating_value
                    FROM exact_shadow_rows e
                    INNER JOIN zone_month_tendency_meta zm ON zm.PULocationID = e.PULocationID
                    WHERE {spec['predicate']}
                ) filtered
                WHERE rating_value IS NOT NULL
                """
            )
        aggregate_sql = "\nUNION ALL\n".join(union_queries)
        rows = con.execute(aggregate_sql).fetchall()
    finally:
        con.close()

    stats_by_family: Dict[str, Dict[str, Any]] = {
        str(row[0]): {
            "average_rating": None if row[1] is None else round(float(row[1]), 2),
            "sample_zone_frames": int(row[2] or 0),
        }
        for row in rows
    }

    families_payload: Dict[str, Dict[str, Any]] = {}
    for spec in FAMILY_SPECS:
        family_key = spec["key"]
        stats = stats_by_family.get(family_key) or {}
        sample_count = int(stats.get("sample_zone_frames") or 0)
        average_rating = stats.get("average_rating") if sample_count > 0 else None
        families_payload[family_key] = {
            "average_rating": average_rating,
            "sample_zone_frames": sample_count,
            "rating_field_family": spec["rating_field_family"],
        }

    return {
        "version": "month_tendency_benchmark_v1",
        "basis": "current_visible_zone_score_vs_active_month_average",
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "month_key": str(month_key).strip(),
        "bin_minutes": int(bin_minutes),
        "families": families_payload,
    }
