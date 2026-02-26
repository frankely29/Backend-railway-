from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import json
import duckdb


REQUIRED_COLS = ["PULocationID", "pickup_datetime", "driver_pay", "trip_miles", "trip_time"]


def ensure_zones_geojson(data_dir: Path, force: bool = False) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    geojson_path = data_dir / "taxi_zones.geojson"
    if geojson_path.exists() and geojson_path.stat().st_size > 0 and not force:
        return geojson_path
    raise RuntimeError("Missing /data/taxi_zones.geojson. Upload it via POST /upload_zones_geojson.")


def bucket_and_color_from_rating(rating: int) -> tuple[str, str]:
    """
    STRICT bucket order requested:
      Green  = Highest
      Purple = High
      Blue   = Medium
      Sky    = Normal
      Yellow = Below Normal
      Red    = Very Low / Avoid
    """
    r = int(rating)

    if r >= 90:
        return "green", "#00b050"
    if r >= 80:
        return "purple", "#8000ff"
    if r >= 65:
        return "blue", "#0066ff"
    if r >= 45:
        return "sky", "#66ccff"
    if r >= 25:
        return "yellow", "#ffd400"
    return "red", "#e60000"


def build_hotspots_frames(
    parquet_files: List[Path],
    zones_geojson_path: Path,
    out_dir: Path,
    bin_minutes: int = 20,
    min_trips_per_window: int = 25,
) -> Dict[str, Any]:
    """
    Writes:
      /data/frames/timeline.json
      /data/frames/frame_000000.json ... etc

    Each frame contains:
      - time
      - polygons FeatureCollection
      - each feature has:
          LocationID, zone_name, borough,
          rating, bucket,
          pickups,
          avg_driver_pay,
          avg_trip_miles,
          avg_trip_time_sec,
          earnings_per_hour,
          style(fillColor)

    SINGLE LOGIC ONLY (NO FALLBACK):
      Requires columns: PULocationID, pickup_datetime, driver_pay, trip_miles, trip_time
      trip_time is treated as seconds (matches your debug sample values).
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------
    # Load zone geometry + names
    # ----------------------------
    zones = json.loads(zones_geojson_path.read_text(encoding="utf-8"))

    geom_by_id: Dict[int, Any] = {}
    name_by_id: Dict[int, str] = {}
    borough_by_id: Dict[int, str] = {}

    for f in zones.get("features", []):
        props = f.get("properties") or {}
        zid = props.get("LocationID")
        if zid is None:
            continue
        try:
            zid_int = int(zid)
        except Exception:
            continue

        geom = f.get("geometry")
        if geom:
            geom_by_id[zid_int] = geom

        zone_name = props.get("zone") or props.get("Zone") or props.get("name") or props.get("Name") or ""
        borough = props.get("borough") or props.get("Borough") or props.get("boro") or props.get("Boro") or ""

        name_by_id[zid_int] = str(zone_name) if zone_name is not None else ""
        borough_by_id[zid_int] = str(borough) if borough is not None else ""

    if not geom_by_id:
        raise RuntimeError("taxi_zones.geojson missing usable properties.LocationID geometry.")

    # ----------------------------
    # DuckDB (spill to volume)
    # ----------------------------
    tmp_dir = out_dir.parent / "duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA enable_progress_bar=false")
    con.execute(f"PRAGMA temp_directory='{tmp_dir.as_posix()}'")

    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    # ----------------------------
    # SINGLE LOGIC SCORE
    #
    # We compute 3 drivers of profit strategy:
    #   1) busy (pickups) - keeps you working
    #   2) earnings_per_hour (sum pay / sum time) - true money per hour
    #   3) long trips (avg_trip_miles) - favors longer rides
    #
    # Normalization uses percentile rank per-window (robust to outliers).
    # Baseline uses percentile rank across zones (robust).
    # Confidence uses pickups scaling (more trips -> more trust).
    # ----------------------------
    sql = f"""
    WITH base AS (
      SELECT
        CAST(PULocationID AS INTEGER) AS PULocationID,
        pickup_datetime,
        TRY_CAST(driver_pay AS DOUBLE) AS driver_pay,
        TRY_CAST(trip_miles AS DOUBLE) AS trip_miles,
        TRY_CAST(trip_time AS DOUBLE) AS trip_time_sec
      FROM read_parquet([{parquet_sql}])
      WHERE
        PULocationID IS NOT NULL
        AND pickup_datetime IS NOT NULL
        AND driver_pay IS NOT NULL
        AND trip_miles IS NOT NULL
        AND trip_time IS NOT NULL
    ),

    cleaned AS (
      SELECT
        PULocationID,
        pickup_datetime,
        driver_pay,
        trip_miles,
        trip_time_sec
      FROM base
      WHERE
        driver_pay >= 0
        AND trip_miles >= 0
        AND trip_time_sec > 0
        AND trip_time_sec < 6*3600  -- sanity: ignore anything longer than 6 hours
        AND trip_miles < 200        -- sanity
    ),

    t AS (
      SELECT
        PULocationID,
        CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,  -- 0=Sun..6=Sat
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay,
        trip_miles,
        trip_time_sec
      FROM cleaned
    ),

    binned AS (
      SELECT
        PULocationID,
        CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,  -- Mon=0..Sun=6
        CAST(FLOOR((hour_i*60 + minute_i) / {int(bin_minutes)}) * {int(bin_minutes)} AS INTEGER) AS bin_start_min,
        driver_pay,
        trip_miles,
        trip_time_sec
      FROM t
    ),

    agg AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        COUNT(*) AS pickups,
        AVG(driver_pay) AS avg_driver_pay,
        AVG(trip_miles) AS avg_trip_miles,
        AVG(trip_time_sec) AS avg_trip_time_sec,
        SUM(driver_pay) AS sum_driver_pay,
        SUM(trip_time_sec) AS sum_trip_time_sec,
        CASE
          WHEN SUM(trip_time_sec) <= 0 THEN NULL
          ELSE SUM(driver_pay) / (SUM(trip_time_sec) / 3600.0)
        END AS earnings_per_hour
      FROM binned
      GROUP BY 1,2,3
      HAVING COUNT(*) >= {int(min_trips_per_window)}
    ),

    -- Per-window percentile ranks
    win AS (
      SELECT
        *,
        LN(1 + pickups) AS log_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY LN(1 + pickups)) AS rn_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY earnings_per_hour) AS rn_eph,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY avg_trip_miles) AS rn_miles,
        COUNT(*) OVER (PARTITION BY dow_m, bin_start_min) AS n_in_window
      FROM agg
      WHERE earnings_per_hour IS NOT NULL
    ),

    win_scored AS (
      SELECT
        PULocationID, dow_m, bin_start_min,
        pickups, avg_driver_pay, avg_trip_miles, avg_trip_time_sec, earnings_per_hour,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_pickups - 1) * 1.0 / (n_in_window - 1) END AS vol_n,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_eph - 1) * 1.0 / (n_in_window - 1) END AS eph_n,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_miles - 1) * 1.0 / (n_in_window - 1) END AS miles_n
      FROM win
    ),

    -- Baseline per-zone, ranked across zones
    zone_base AS (
      SELECT
        PULocationID,
        LN(1 + AVG(pickups)) AS base_log_pickups,
        AVG(earnings_per_hour) AS base_eph,
        AVG(avg_trip_miles) AS base_miles
      FROM agg
      WHERE earnings_per_hour IS NOT NULL
      GROUP BY 1
    ),

    zone_ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY base_log_pickups) AS rn_base_pickups,
        ROW_NUMBER() OVER (ORDER BY base_eph) AS rn_base_eph,
        ROW_NUMBER() OVER (ORDER BY base_miles) AS rn_base_miles,
        COUNT(*) OVER () AS n_zones
      FROM zone_base
    ),

    zone_norm AS (
      SELECT
        PULocationID,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_pickups - 1) * 1.0 / (n_zones - 1) END AS base_vol_n,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_eph - 1) * 1.0 / (n_zones - 1) END AS base_eph_n,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_miles - 1) * 1.0 / (n_zones - 1) END AS base_miles_n
      FROM zone_ranked
    ),

    final AS (
      SELECT
        w.PULocationID,
        w.dow_m,
        w.bin_start_min,
        w.pickups,
        w.avg_driver_pay,
        w.avg_trip_miles,
        w.avg_trip_time_sec,
        w.earnings_per_hour,

        -- weights: stay busy + money/hr + long trips
        (0.55*w.vol_n + 0.30*w.eph_n + 0.15*w.miles_n) AS moment_score,
        (0.55*z.base_vol_n + 0.30*z.base_eph_n + 0.15*z.base_miles_n) AS base_score,

        -- confidence by pickup count
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
      avg_trip_miles,
      avg_trip_time_sec,
      earnings_per_hour,
      CAST(
        ROUND(
          1 + 99 * LEAST(
            GREATEST(
              ((0.70*moment_score + 0.30*base_score) * (0.50 + 0.50*conf)),
              0.0
            ),
            1.0
          )
        ) AS INTEGER
      ) AS rating
    FROM final
    ORDER BY dow_m, bin_start_min, PULocationID;
    """

    # IMPORTANT: if required columns are missing, DuckDB will throw.
    # We want that (single logic only).
    cur = con.execute(sql)

    # timeline labels (Mon-based week anchor)
    week_start = datetime(2025, 1, 6, 0, 0, 0)  # Monday anchor
    timeline: List[str] = []
    frame_count = 0

    current_key: Tuple[int, int] | None = None
    current_features: List[Dict[str, Any]] = []
    current_time_iso: str | None = None

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

    total_rows = 0
    any_rows = False

    while True:
        batch = cur.fetchmany(5000)
        if not batch:
            break
        any_rows = True

        for (zid, dow_m, bin_start_min, pickups, avg_pay, avg_miles, avg_time_sec, eph, rating) in batch:
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

            zid_i = int(zid)
            geom = geom_by_id.get(zid_i)
            if not geom:
                continue

            r = int(rating)
            bucket, fill = bucket_and_color_from_rating(r)

            current_features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "LocationID": zid_i,
                    "zone_name": name_by_id.get(zid_i, ""),
                    "borough": borough_by_id.get(zid_i, ""),
                    "rating": r,
                    "bucket": bucket,
                    "pickups": int(pickups),
                    "avg_driver_pay": None if avg_pay is None else float(avg_pay),
                    "avg_trip_miles": None if avg_miles is None else float(avg_miles),
                    "avg_trip_time_sec": None if avg_time_sec is None else float(avg_time_sec),
                    "earnings_per_hour": None if eph is None else float(eph),
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