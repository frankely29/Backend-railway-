from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import json
import duckdb


def ensure_zones_geojson(data_dir: Path, force: bool = False) -> Path:
    """
    Railway-safe approach:
    - You upload /data/taxi_zones.geojson once (stored in the Railway volume).
    - No geopandas/fiona/pyproj needed.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    geojson_path = data_dir / "taxi_zones.geojson"
    if geojson_path.exists() and geojson_path.stat().st_size > 0 and not force:
        return geojson_path
    raise RuntimeError("Missing /data/taxi_zones.geojson. Upload it via POST /upload_zones_geojson.")


def color_bucket_from_rating(rating: int) -> str:
    """
    STRICT mandatory rules:
      Green = Best
      Blue = Medium
      Sky  = Normal
      Red  = Avoid
    """
    r = int(rating)
    if r >= 80:
        return "#00b050"  # Green
    if r >= 60:
        return "#0066ff"  # Blue
    if r >= 40:
        return "#66ccff"  # Sky
    return "#e60000"      # Red


def build_hotspots_json(
    parquet_files: List[Path],
    zones_geojson_path: Path,
    out_path: Path,
    bin_minutes: int = 20,
    min_trips_per_window: int = 10,
) -> None:
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

    con = duckdb.connect(database=":memory:")

    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    # DuckDB does everything: aggregate + normalize + rating (1..100)
    sql = f"""
    WITH base AS (
      SELECT
        CAST(PULocationID AS INTEGER) AS PULocationID,
        pickup_datetime,
        TRY_CAST(driver_pay AS DOUBLE) AS driver_pay,
        TRY_CAST(tips AS DOUBLE) AS tips
      FROM read_parquet([{parquet_sql}])
      WHERE PULocationID IS NOT NULL AND pickup_datetime IS NOT NULL
    ),
    t AS (
      SELECT
        PULocationID,
        CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,  -- 0=Sun..6=Sat
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay,
        tips
      FROM base
    ),
    binned AS (
      SELECT
        PULocationID,
        CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,  -- Mon=0..Sun=6
        CAST(FLOOR((hour_i*60 + minute_i) / {int(bin_minutes)}) * {int(bin_minutes)} AS INTEGER) AS bin_start_min,
        driver_pay,
        tips
      FROM t
    ),
    agg AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        COUNT(*) AS pickups,
        AVG(driver_pay) AS avg_driver_pay,
        AVG(tips) AS avg_tips
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
        MAX(avg_driver_pay) OVER (PARTITION BY dow_m, bin_start_min) AS max_pay,
        MIN(avg_tips) OVER (PARTITION BY dow_m, bin_start_min) AS min_tips,
        MAX(avg_tips) OVER (PARTITION BY dow_m, bin_start_min) AS max_tips
      FROM agg
    ),
    scored AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        pickups,
        avg_driver_pay,
        avg_tips,

        CASE
          WHEN max_pickups IS NULL OR min_pickups IS NULL OR max_pickups = min_pickups THEN 0.0
          ELSE (pickups - min_pickups) * 1.0 / (max_pickups - min_pickups)
        END AS vol_n,

        CASE
          WHEN max_pay IS NULL OR min_pay IS NULL OR max_pay = min_pay THEN 0.0
          ELSE (avg_driver_pay - min_pay) * 1.0 / (max_pay - min_pay)
        END AS pay_n,

        CASE
          WHEN max_tips IS NULL OR min_tips IS NULL OR max_tips = min_tips THEN 0.0
          ELSE (avg_tips - min_tips) * 1.0 / (max_tips - min_tips)
        END AS tip_n
      FROM mm
    )
    SELECT
      PULocationID,
      dow_m,
      bin_start_min,
      pickups,
      avg_driver_pay,
      avg_tips,
      CAST(ROUND(1 + 99 * LEAST(GREATEST((0.60*vol_n + 0.30*pay_n + 0.10*tip_n), 0.0), 1.0)) AS INTEGER) AS rating
    FROM scored;
    """

    rows = con.execute(sql).fetchall()
    if not rows:
        raise RuntimeError("No data after filtering. Lower min_trips_per_window.")

    # Group rows by (dow_m, bin_start_min)
    by_window: Dict[Tuple[int, int], list] = {}
    for r in rows:
        zid = int(r[0])
        dow_m = int(r[1])
        bin_start_min = int(r[2])
        pickups = int(r[3])
        avg_pay = r[4]   # can be None
        avg_tip = r[5]   # can be None
        rating = int(r[6])

        by_window.setdefault((dow_m, bin_start_min), []).append(
            (zid, pickups, avg_pay, avg_tip, rating)
        )

    # Baseline week (typical week pattern). Frontend matches by day/time-of-week.
    week_start = datetime(2025, 1, 6, 0, 0, 0)  # Monday
    timeline: List[str] = []
    frames: List[Dict[str, Any]] = []

    for (dow_m, bin_start_min) in sorted(by_window.keys()):
        hour = int(bin_start_min // 60)
        minute = int(bin_start_min % 60)

        ts = week_start + timedelta(days=int(dow_m), hours=hour, minutes=minute)
        ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S")

        feats = []
        for (zid, pickups, avg_pay, avg_tip, rating) in by_window[(dow_m, bin_start_min)]:
            geom = geom_by_id.get(zid)
            if not geom:
                continue

            fill = color_bucket_from_rating(rating)

            feats.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "LocationID": zid,
                    "rating": int(rating),
                    "pickups": int(pickups),
                    "avg_driver_pay": None if avg_pay is None else float(avg_pay),
                    "avg_tips": None if avg_tip is None else float(avg_tip),
                    "style": {
                        # ✅ no outlines + stronger fill so colors look correct on phone
                        "color": fill,
                        "opacity": 0,
                        "weight": 0,
                        "fillColor": fill,
                        "fillOpacity": 0.82
                    }
                }
            })

        timeline.append(ts_iso)
        frames.append({
            "time": ts_iso,
            "polygons": {"type": "FeatureCollection", "features": feats}
        })

    out = {"timeline": timeline, "frames": frames}
    out_path.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")