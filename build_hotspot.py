from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta
import json
import duckdb


def ensure_zones_geojson(data_dir: Path, force: bool = False) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    geojson_path = data_dir / "taxi_zones.geojson"
    if geojson_path.exists() and geojson_path.stat().st_size > 0 and not force:
        return geojson_path
    raise RuntimeError("Missing /data/taxi_zones.geojson. Upload it via POST /upload_zones_geojson.")


def color_bucket_from_rating(rating: int) -> str:
    # STRICT buckets
    r = int(rating)
    if r >= 80:
        return "#00b050"  # Green = Best
    if r >= 60:
        return "#0066ff"  # Blue = Medium
    if r >= 40:
        return "#66ccff"  # Sky = Normal
    return "#e60000"      # Red = Avoid


def build_hotspots_frames(
    parquet_files: List[Path],
    zones_geojson_path: Path,
    out_dir: Path,
    bin_minutes: int = 20,
    min_trips_per_window: int = 10,
) -> Dict[str, Any]:
    """
    Writes:
      /data/frames/timeline.json
      /data/frames/frame_000000.json
      /data/frames/frame_000001.json
      ...
    This avoids a huge single JSON and keeps iPhone fast.
    Scoring: 80% busy + 20% driver_pay (tips ignored).
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load zone geometry (LocationID -> geometry)
    zones = json.loads(zones_geojson_path.read_text(encoding="utf-8"))
    geom_by_id: Dict[int, Any] = {}
    for f in zones.get("features", []):
        props = f.get("properties") or {}
        zid = props.get("LocationID")
        if zid is None:
            continue
        try:
            zid_int = int(zid)
        except Exception:
            continue
        geom_by_id[zid_int] = f.get("geometry")

    if not geom_by_id:
        raise RuntimeError("taxi_zones.geojson missing usable LocationID geometry.")

    # DuckDB setup: allow spill to disk on Railway volume
    tmp_dir = out_dir.parent / "duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA temp_directory='{str(tmp_dir).replace(\"'\", \"''\")}'")
    con.execute("PRAGMA enable_progress_bar=false")
    # Threads: DuckDB picks a default; you can hard-set if you want:
    # con.execute("PRAGMA threads=4")

    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    # We produce one row per (window, zone) with rating + geometry fields.
    sql = f"""
    WITH base AS (
      SELECT
        CAST(PULocationID AS INTEGER) AS PULocationID,
        pickup_datetime,
        TRY_CAST(driver_pay AS DOUBLE) AS driver_pay
      FROM read_parquet([{parquet_sql}])
      WHERE PULocationID IS NOT NULL AND pickup_datetime IS NOT NULL
    ),
    t AS (
      SELECT
        PULocationID,
        CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,  -- 0=Sun..6=Sat
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay
      FROM base
    ),
    binned AS (
      SELECT
        PULocationID,
        CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,  -- Mon=0..Sun=6
        CAST(FLOOR((hour_i*60 + minute_i) / {int(bin_minutes)}) * {int(bin_minutes)} AS INTEGER) AS bin_start_min,
        driver_pay
      FROM t
    ),
    agg AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        COUNT(*) AS pickups,
        AVG(driver_pay) AS avg_driver_pay
      FROM binned
      GROUP BY 1,2,3
      HAVING COUNT(*) >= {int(min_trips_per_window)}
    ),
    mm AS (
      SELECT
        *,
        MIN(pickups) OVER (PARTITION BY dow_m, bin_start_min) AS min_pickups,
        MAX(pickups) OVER (PARTITION BY dow_m, bin_start_min) AS max_pickups,
        MIN(avg_driver_pay) OVER (PARTITION BY dow_m, bin_start_min) AS min_pay,
        MAX(avg_driver_pay) OVER (PARTITION BY dow_m, bin_start_min) AS max_pay
      FROM agg
    ),
    scored AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        pickups,
        avg_driver_pay,
        CASE
          WHEN max_pickups IS NULL OR min_pickups IS NULL OR max_pickups = min_pickups THEN 0.0
          ELSE (pickups - min_pickups) * 1.0 / (max_pickups - min_pickups)
        END AS vol_n,
        CASE
          WHEN max_pay IS NULL OR min_pay IS NULL OR max_pay = min_pay THEN 0.0
          ELSE (avg_driver_pay - min_pay) * 1.0 / (max_pay - min_pay)
        END AS pay_n
      FROM mm
    )
    SELECT
      PULocationID,
      dow_m,
      bin_start_min,
      pickups,
      avg_driver_pay,
      CAST(
        ROUND(
          1 + 99 * LEAST(
            GREATEST((0.80*vol_n + 0.20*pay_n), 0.0),
            1.0
          )
        )
        AS INTEGER
      ) AS rating
    FROM scored
    ORDER BY dow_m, bin_start_min, PULocationID;
    """

    rows = con.execute(sql).fetchall()
    if not rows:
        raise RuntimeError("No data after filtering. Lower min_trips_per_window.")

    # Build windows in chronological order; write each frame as its own file.
    week_start = datetime(2025, 1, 6, 0, 0, 0)  # Monday baseline
    timeline = []
    frame_count = 0

    current_key = None
    current_features = []
    current_time_iso = None

    def flush_frame():
        nonlocal frame_count, current_features, current_time_iso
        if current_time_iso is None:
            return
        timeline.append(current_time_iso)
        frame_path = out_dir / f"frame_{frame_count:06d}.json"
        payload = {
            "time": current_time_iso,
            "polygons": {"type": "FeatureCollection", "features": current_features},
        }
        frame_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        frame_count += 1
        current_features = []
        current_time_iso = None

    for (zid, dow_m, bin_start_min, pickups, avg_pay, rating) in rows:
        key = (int(dow_m), int(bin_start_min))
        if current_key is None:
            current_key = key

        if key != current_key:
            flush_frame()
            current_key = key

        hour = int(bin_start_min // 60)
        minute = int(bin_start_min % 60)
        ts = week_start + timedelta(days=int(dow_m), hours=hour, minutes=minute)
        current_time_iso = ts.strftime("%Y-%m-%dT%H:%M:%S")

        geom = geom_by_id.get(int(zid))
        if not geom:
            continue

        fill = color_bucket_from_rating(int(rating))
        current_features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "LocationID": int(zid),
                "rating": int(rating),
                "pickups": int(pickups),
                "avg_driver_pay": None if avg_pay is None else float(avg_pay),
                "avg_tips": None,
                "style": {
                    "color": fill,
                    "opacity": 0,
                    "weight": 0,
                    "fillColor": fill,
                    "fillOpacity": 0.82
                }
            }
        })

    flush_frame()

    # Write timeline index
    (out_dir / "timeline.json").write_text(
        json.dumps({"timeline": timeline, "count": len(timeline)}, separators=(",", ":")),
        encoding="utf-8"
    )

    return {"ok": True, "frames_dir": str(out_dir), "count": len(timeline)}