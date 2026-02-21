import os
import json
from datetime import datetime, timezone

import duckdb

# Strict bucket colors (MANDATORY)
COLOR_GREEN = "#00b050"   # Best
COLOR_BLUE  = "#1f5cff"   # Medium
COLOR_SKY   = "#66ccff"   # Normal
COLOR_RED   = "#e60000"   # Avoid

def ensure_taxi_zones_geojson(data_dir: str):
    # We do not auto-download here; you upload once to /data
    # Expected path:
    # /data/taxi_zones.geojson
    return

def _read_taxi_zones_geojson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        gj = json.load(f)

    feats = gj.get("features", [])
    if not feats:
        raise RuntimeError("taxi_zones.geojson has no features")

    zone_by_id = {}
    for ft in feats:
        props = ft.get("properties") or {}
        loc = (
            props.get("LocationID")
            or props.get("location_id")
            or props.get("locationid")
            or props.get("OBJECTID")
            or props.get("objectid")
        )
        if loc is None:
            continue
        try:
            loc = int(loc)
        except:
            continue
        zone_by_id[loc] = ft

    if not zone_by_id:
        raise RuntimeError("Could not find LocationID in taxi_zones.geojson properties")

    return gj, zone_by_id

def _bucket_color(rating: int, normal_lo: int, medium_lo: int, best_lo: int) -> str:
    if rating < normal_lo:
        return COLOR_RED
    if rating < medium_lo:
        return COLOR_SKY
    if rating < best_lo:
        return COLOR_BLUE
    return COLOR_GREEN

def build_hotspots(
    data_dir: str,
    taxi_zones_geojson_path: str,
    output_path: str,
    bin_minutes: int = 20,
    min_trips_per_window: int = 10,
    normal_lo: int = 40,
    medium_lo: int = 60,
    best_lo: int = 80,
):
    # Load taxi zone geometry
    _, zone_by_id = _read_taxi_zones_geojson(taxi_zones_geojson_path)

    # IMPORTANT: Match YOUR parquet naming
    pattern = os.path.join(data_dir, "fhvhv_tripdata_*.parquet")

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")

    # Assumes columns:
    # pickup_datetime and PULocationID (standard for fhvhv_tripdata)
    query = f"""
    WITH base AS (
      SELECT
        TRY_CAST(pickup_datetime AS TIMESTAMP) AS pickup_ts,
        TRY_CAST(PULocationID AS INTEGER) AS pu
      FROM read_parquet('{pattern}')
      WHERE pickup_datetime IS NOT NULL AND PULocationID IS NOT NULL
    ),
    binned AS (
      SELECT
        pu,
        date_trunc('minute', pickup_ts)
          - (EXTRACT(MINUTE FROM pickup_ts)::INTEGER % {int(bin_minutes)}) * INTERVAL 1 MINUTE AS bin_start,
        COUNT(*) AS trips
      FROM base
      GROUP BY pu, bin_start
    )
    SELECT bin_start, pu, trips
    FROM binned
    ORDER BY bin_start, pu
    """
    rows = con.execute(query).fetchall()

    windows = {}
    for bin_start, pu, trips in rows:
        if bin_start is None or pu is None:
            continue
        key = bin_start.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        windows.setdefault(key, []).append((int(pu), int(trips)))

    timeline = sorted(windows.keys())
    frames = []

    for t in timeline:
        zone_trips = windows[t]  # [(zone_id, trips)]

        filtered = [(z, c) for (z, c) in zone_trips if c >= min_trips_per_window]
        if not filtered:
            min_c, max_c = 0, 0
        else:
            counts = [c for _, c in filtered]
            min_c, max_c = min(counts), max(counts)

        def to_rating(count: int) -> int:
            # If zone has too few trips, treat it as lowest bucket
            if count < min_trips_per_window:
                return 1
            if max_c <= min_c:
                return 50
            x = (count - min_c) / (max_c - min_c)
            r = int(round(1 + x * 99))
            return max(1, min(100, r))

        # Build the polygons for THIS time window
        out_features = []
        for zone_id, ft in zone_by_id.items():
            count = 0
            for z, c in zone_trips:
                if z == zone_id:
                    count = c
                    break

            rating = to_rating(count)
            fill = _bucket_color(rating, normal_lo, medium_lo, best_lo)

            props = dict(ft.get("properties") or {})
            props["zone_id"] = zone_id
            props["trips"] = count
            props["rating"] = rating
            props["bucket"] = (
                "Best" if fill == COLOR_GREEN else
                "Medium" if fill == COLOR_BLUE else
                "Normal" if fill == COLOR_SKY else
                "Avoid"
            )

            # No confusing outlines: border == fill color
            props["style"] = {
                "color": fill,
                "weight": 1,
                "opacity": 0.9,
                "fillColor": fill,
                "fillOpacity": 0.45,
            }

            zone_name = props.get("zone") or props.get("Zone") or props.get("ZONE") or "Zone"
            borough = props.get("borough") or props.get("Borough") or ""
            props["popup"] = (
                f"<b>{zone_name}</b><br>"
                f"{borough}<br>"
                f"<b>Trips:</b> {count}<br>"
                f"<b>Rating:</b> {rating}/100<br>"
                f"<b>Level:</b> {props['bucket']}"
            )

            out_features.append({
                "type": "Feature",
                "geometry": ft.get("geometry"),
                "properties": props
            })

        frames.append({
            "time": t,
            "polygons": {"type": "FeatureCollection", "features": out_features}
        })

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "bin_minutes": bin_minutes,
        "min_trips_per_window": min_trips_per_window,
        "thresholds": {
            "normal_lo": normal_lo,
            "medium_lo": medium_lo,
            "best_lo": best_lo
        },
        "timeline": timeline,
        "frames": frames
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    return {
        "timeline_steps": len(timeline),
        "frames": len(frames),
        "generated_at": payload["generated_at"],
        "pattern": pattern
    }
