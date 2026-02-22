from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import json
import duckdb


def ensure_zones_geojson(data_dir: Path, force: bool = False) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    geojson_path = data_dir / "taxi_zones.geojson"
    if geojson_path.exists() and geojson_path.stat().st_size > 0 and not force:
        return geojson_path
    raise RuntimeError("Missing /data/taxi_zones.geojson. Upload it via POST /upload_zones_geojson.")


def bucket_name_from_rating(rating: int) -> str:
    r = int(rating)
    if r >= 80:
        return "green"   # Best
    if r >= 60:
        return "blue"    # Medium
    if r >= 40:
        return "sky"     # Normal
    if r >= 30:
        return "yellow"  # Below Normal
    return "red"         # Avoid


def color_from_bucket(bucket: str) -> str:
    if bucket == "green":
        return "#00b050"
    if bucket == "blue":
        return "#0066ff"
    if bucket == "sky":
        return "#66ccff"
    if bucket == "yellow":
        return "#ffd400"
    return "#e60000"


def _median_int(vals: List[int]) -> int | None:
    if not vals:
        return None
    vals_sorted = sorted(vals)
    n = len(vals_sorted)
    mid = n // 2
    if n % 2 == 1:
        return int(vals_sorted[mid])
    return int(round((vals_sorted[mid - 1] + vals_sorted[mid]) / 2))


def _avg_int(vals: List[int]) -> int | None:
    if not vals:
        return None
    return int(round(sum(vals) / len(vals)))


def build_hotspots_frames(
    parquet_files: List[Path],
    zones_geojson_path: Path,
    out_dir: Path,
    bin_minutes: int = 20,
    min_trips_per_window: int = 25,
) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)

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
        raise RuntimeError("taxi_zones.geojson missing usable properties.LocationID geometry.")

    tmp_dir = out_dir.parent / "duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA enable_progress_bar=false")
    con.execute(f"PRAGMA temp_directory='{tmp_dir.as_posix()}'")

    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

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
        CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay
      FROM base
    ),
    binned AS (
      SELECT
        PULocationID,
        CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,
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

    win AS (
      SELECT
        *,
        LN(1 + pickups) AS log_pickups,
        MIN(LN(1 + pickups)) OVER (PARTITION BY dow_m, bin_start_min) AS min_log_pickups,
        MAX(LN(1 + pickups)) OVER (PARTITION BY dow_m, bin_start_min) AS max_log_pickups,
        MIN(avg_driver_pay) OVER (PARTITION BY dow_m, bin_start_min) AS min_pay,
        MAX(avg_driver_pay) OVER (PARTITION BY dow_m, bin_start_min) AS max_pay
      FROM agg
    ),
    win_scored AS (
      SELECT
        PULocationID, dow_m, bin_start_min, pickups, avg_driver_pay,
        CASE
          WHEN max_log_pickups IS NULL OR min_log_pickups IS NULL OR max_log_pickups = min_log_pickups THEN 0.0
          ELSE (log_pickups - min_log_pickups) * 1.0 / (max_log_pickups - min_log_pickups)
        END AS vol_n,
        CASE
          WHEN max_pay IS NULL OR min_pay IS NULL OR max_pay = min_pay THEN 0.0
          ELSE (avg_driver_pay - min_pay) * 1.0 / (max_pay - min_pay)
        END AS pay_n
      FROM win
    ),

    zone_base AS (
      SELECT
        PULocationID,
        LN(1 + AVG(pickups)) AS base_log_pickups,
        AVG(avg_driver_pay) AS base_pay
      FROM agg
      GROUP BY 1
    ),
    zone_mm AS (
      SELECT
        *,
        MIN(base_log_pickups) OVER () AS min_base_log_pickups,
        MAX(base_log_pickups) OVER () AS max_base_log_pickups,
        MIN(base_pay) OVER () AS min_base_pay,
        MAX(base_pay) OVER () AS max_base_pay
      FROM zone_base
    ),
    zone_norm AS (
      SELECT
        PULocationID,
        CASE
          WHEN max_base_log_pickups IS NULL OR min_base_log_pickups IS NULL OR max_base_log_pickups = min_base_log_pickups THEN 0.0
          ELSE (base_log_pickups - min_base_log_pickups) * 1.0 / (max_base_log_pickups - min_base_log_pickups)
        END AS base_vol_n,
        CASE
          WHEN max_base_pay IS NULL OR min_base_pay IS NULL OR max_base_pay = min_base_pay THEN 0.0
          ELSE (base_pay - min_base_pay) * 1.0 / (max_base_pay - min_base_pay)
        END AS base_pay_n
      FROM zone_mm
    ),

    final AS (
      SELECT
        w.PULocationID,
        w.dow_m,
        w.bin_start_min,
        w.pickups,
        w.avg_driver_pay,
        (0.80*w.vol_n + 0.20*w.pay_n) AS moment_score,
        (0.80*z.base_vol_n + 0.20*z.base_pay_n) AS base_score,
        LEAST(1.0, w.pickups / 50.0) AS conf
      FROM win_scored w
      JOIN zone_norm z USING (PULocationID)
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
            GREATEST(((0.70*moment_score + 0.30*base_score) * (0.50 + 0.50*conf)), 0.0),
            1.0
          )
        ) AS INTEGER
      ) AS rating
    FROM final
    ORDER BY dow_m, bin_start_min, PULocationID;
    """

    cur = con.execute(sql)

    week_start = datetime(2025, 1, 6, 0, 0, 0)
    timeline: List[str] = []
    frame_count = 0

    current_key: Tuple[int, int] | None = None
    current_features: List[Dict[str, Any]] = []
    current_time_iso: str | None = None

    counts = {"green": 0, "blue": 0, "sky": 0, "yellow": 0, "red": 0}
    etas: Dict[str, List[int]] = {"green": [], "blue": [], "sky": [], "yellow": [], "red": []}

    def flush_frame():
        nonlocal frame_count, current_features, current_time_iso, counts, etas
        if current_time_iso is None:
            return

        eta_summary = {}
        for k in ["green", "blue", "sky", "yellow", "red"]:
            eta_summary[k] = {
                "median": _median_int(etas[k]),
                "avg": _avg_int(etas[k]),
            }

        timeline.append(current_time_iso)
        frame_path = out_dir / f"frame_{frame_count:06d}.json"
        payload = {
            "time": current_time_iso,
            "summary": {
                **counts,
                "eta": eta_summary
            },
            "polygons": {"type": "FeatureCollection", "features": current_features},
        }
        frame_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")

        frame_count += 1
        current_features = []
        current_time_iso = None
        counts = {"green": 0, "blue": 0, "sky": 0, "yellow": 0, "red": 0}
        etas = {"green": [], "blue": [], "sky": [], "yellow": [], "red": []}

    total_rows = 0
    any_rows = False

    while True:
        batch = cur.fetchmany(5000)
        if not batch:
            break
        any_rows = True

        for (zid, dow_m, bin_start_min, pickups, avg_pay, rating) in batch:
            total_rows += 1
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

            r = int(rating)
            bucket = bucket_name_from_rating(r)
            fill = color_from_bucket(bucket)
            counts[bucket] += 1

            p = int(pickups)
            eta_min = int(max(1, min(60, round(bin_minutes / max(1, p)))))
            etas[bucket].append(eta_min)

            current_features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "LocationID": int(zid),
                    "rating": r,
                    "bucket": bucket,
                    "pickups": p,
                    "eta_minutes": eta_min,
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

    if not any_rows:
        raise RuntimeError("No data after filtering. Lower min_trips_per_window.")

    flush_frame()

    (out_dir / "timeline.json").write_text(
        json.dumps({"timeline": timeline, "count": len(timeline)}, separators=(",", ":")),
        encoding="utf-8"
    )

    return {"ok": True, "count": len(timeline), "frames_dir": str(out_dir), "rows": total_rows}