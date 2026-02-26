from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import json
import duckdb


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


def _detect_columns(con: duckdb.DuckDBPyConnection, parquet_sql: str) -> Dict[str, Optional[str]]:
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{parquet_sql}])").fetchall()
    cols = [r[0] for r in rows]
    lower = {c.lower(): c for c in cols}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n.lower() in lower:
                return lower[n.lower()]
        return None

    # Your dataset confirmed these exist:
    return {
        "PULocationID": pick("PULocationID", "pulocationid"),
        "pickup_datetime": pick("pickup_datetime", "Pickup_datetime"),
        "driver_pay": pick("driver_pay", "Driver_pay"),
        "trip_miles": pick("trip_miles", "Trip_miles"),
        "trip_time": pick("trip_time", "Trip_time"),
    }


def _guess_trip_time_unit(con: duckdb.DuckDBPyConnection, parquet_sql: str, trip_time_col: str) -> str:
    """
    TLC FHV trip_time is usually seconds. We auto-detect:
      - if median > 200 -> seconds
      - else -> minutes
    """
    try:
        med = con.execute(
            f"""
            SELECT approx_quantile(TRY_CAST({trip_time_col} AS DOUBLE), 0.5)
            FROM read_parquet([{parquet_sql}])
            WHERE {trip_time_col} IS NOT NULL
            LIMIT 100000
            """
        ).fetchone()[0]
        if med is None:
            return "seconds"
        return "seconds" if float(med) > 200 else "minutes"
    except Exception:
        return "seconds"


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
      /data/frames/frame_000000.json ...

    Each feature includes (always):
      LocationID, zone_name, borough, rating, bucket, pickups, avg_driver_pay

    If time/miles exist, also includes:
      avg_trip_miles, avg_trip_minutes, pay_per_hour_zone

    SAFE FALLBACK:
      If time/miles missing -> rating uses your old logic (busy + pay only).
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

    cols = _detect_columns(con, parquet_sql)

    if not cols["PULocationID"] or not cols["pickup_datetime"]:
        raise RuntimeError(f"Missing required columns. Detected: {cols}")

    has_pay = cols["driver_pay"] is not None
    has_miles = cols["trip_miles"] is not None
    has_time = cols["trip_time"] is not None

    trip_time_unit = "seconds"
    if has_time and cols["trip_time"]:
        trip_time_unit = _guess_trip_time_unit(con, parquet_sql, cols["trip_time"])

    # Convert trip_time to minutes safely
    # - if unit == seconds => /60
    # - if unit == minutes => use as-is
    time_to_minutes_expr = "NULL::DOUBLE AS trip_minutes"
    if has_time and cols["trip_time"]:
        if trip_time_unit == "seconds":
            time_to_minutes_expr = f"(TRY_CAST({cols['trip_time']} AS DOUBLE) / 60.0) AS trip_minutes"
        else:
            time_to_minutes_expr = f"(TRY_CAST({cols['trip_time']} AS DOUBLE)) AS trip_minutes"

    miles_expr = "NULL::DOUBLE AS trip_miles"
    if has_miles and cols["trip_miles"]:
        miles_expr = f"(TRY_CAST({cols['trip_miles']} AS DOUBLE)) AS trip_miles"

    pay_expr = "NULL::DOUBLE AS driver_pay"
    if has_pay and cols["driver_pay"]:
        pay_expr = f"(TRY_CAST({cols['driver_pay']} AS DOUBLE)) AS driver_pay"

    # ----------------------------
    # SQL build
    #
    # OLD LOGIC (fallback):
    #   moment_score = 0.85*vol + 0.15*pay
    #
    # NEW LOGIC (when time/miles available):
    #   still "busy first", but adds:
    #     - pay_per_hour_zone (based on pay + duration)
    #     - long trip signal (miles + minutes)
    #
    # All normalizations are percentile-rank based (robust to airport outliers).
    # ----------------------------

    use_enhanced = (has_time or has_miles)  # if either exists, compute what we can

    # weights: keep busy dominant
    # if a metric missing, we exclude it and re-normalize weights in SQL
    w_vol = 0.70
    w_pay = 0.10
    w_hourly = 0.10 if has_time and has_pay else 0.0
    w_long = 0.10 if has_time or has_miles else 0.0

    # renormalize
    w_sum = w_vol + w_pay + w_hourly + w_long
    w_vol /= w_sum
    w_pay /= w_sum
    if w_sum > 0:
        w_hourly /= w_sum
        w_long /= w_sum

    # For long trip signal:
    # - if both miles and minutes exist: long_n = 0.6*miles_n + 0.4*mins_n
    # - if only one exists: use that one
    long_expr = "0.0"
    if has_miles and has_time:
        long_expr = "(0.6*miles_n + 0.4*mins_n)"
    elif has_miles:
        long_expr = "miles_n"
    elif has_time:
        long_expr = "mins_n"

    sql = f"""
    WITH base AS (
      SELECT
        CAST({cols["PULocationID"]} AS INTEGER) AS PULocationID,
        CAST({cols["pickup_datetime"]} AS TIMESTAMP) AS pickup_datetime,
        {pay_expr},
        {miles_expr},
        {time_to_minutes_expr}
      FROM read_parquet([{parquet_sql}])
      WHERE {cols["PULocationID"]} IS NOT NULL AND {cols["pickup_datetime"]} IS NOT NULL
    ),
    t AS (
      SELECT
        PULocationID,
        CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,  -- 0=Sun..6=Sat
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay,
        trip_miles,
        trip_minutes
      FROM base
    ),
    binned AS (
      SELECT
        PULocationID,
        CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,  -- Mon=0..Sun=6
        CAST(FLOOR((hour_i*60 + minute_i) / {int(bin_minutes)}) * {int(bin_minutes)} AS INTEGER) AS bin_start_min,
        driver_pay,
        trip_miles,
        trip_minutes
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
        AVG(trip_minutes) AS avg_trip_minutes
      FROM binned
      GROUP BY 1,2,3
      HAVING COUNT(*) >= {int(min_trips_per_window)}
    ),

    -- compute hourly estimate if we have pay + minutes
    agg2 AS (
      SELECT
        *,
        CASE
          WHEN avg_driver_pay IS NULL OR avg_trip_minutes IS NULL OR avg_trip_minutes <= 0.001 THEN NULL
          ELSE avg_driver_pay * 60.0 / avg_trip_minutes
        END AS pay_per_hour_zone
      FROM agg
    ),

    -- window ranks (percentile based)
    win AS (
      SELECT
        *,
        LN(1 + pickups) AS log_pickups,

        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY LN(1 + pickups)) AS rn_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY avg_driver_pay) AS rn_pay,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY pay_per_hour_zone) AS rn_hourly,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY avg_trip_miles) AS rn_miles,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY avg_trip_minutes) AS rn_mins,

        COUNT(*) OVER (PARTITION BY dow_m, bin_start_min) AS n_in_window
      FROM agg2
    ),
    win_scored AS (
      SELECT
        PULocationID, dow_m, bin_start_min,
        pickups, avg_driver_pay, avg_trip_miles, avg_trip_minutes, pay_per_hour_zone,

        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_pickups - 1) * 1.0 / (n_in_window - 1) END AS vol_n,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_pay - 1) * 1.0 / (n_in_window - 1) END AS pay_n,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_hourly - 1) * 1.0 / (n_in_window - 1) END AS hourly_n,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_miles - 1) * 1.0 / (n_in_window - 1) END AS miles_n,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_mins - 1) * 1.0 / (n_in_window - 1) END AS mins_n
      FROM win
    ),

    -- zone baseline ranks (percentile based)
    zone_base AS (
      SELECT
        PULocationID,
        LN(1 + AVG(pickups)) AS base_log_pickups,
        AVG(avg_driver_pay) AS base_pay,
        AVG(avg_trip_miles) AS base_miles,
        AVG(avg_trip_minutes) AS base_mins,
        AVG(pay_per_hour_zone) AS base_hourly
      FROM agg2
      GROUP BY 1
    ),
    zone_ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY base_log_pickups) AS rn_base_pickups,
        ROW_NUMBER() OVER (ORDER BY base_pay) AS rn_base_pay,
        ROW_NUMBER() OVER (ORDER BY base_hourly) AS rn_base_hourly,
        ROW_NUMBER() OVER (ORDER BY base_miles) AS rn_base_miles,
        ROW_NUMBER() OVER (ORDER BY base_mins) AS rn_base_mins,
        COUNT(*) OVER () AS n_zones
      FROM zone_base
    ),
    zone_norm AS (
      SELECT
        PULocationID,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_pickups - 1) * 1.0 / (n_zones - 1) END AS base_vol_n,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_pay - 1) * 1.0 / (n_zones - 1) END AS base_pay_n,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_hourly - 1) * 1.0 / (n_zones - 1) END AS base_hourly_n,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_miles - 1) * 1.0 / (n_zones - 1) END AS base_miles_n,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_mins - 1) * 1.0 / (n_zones - 1) END AS base_mins_n
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
        w.avg_trip_minutes,
        w.pay_per_hour_zone,

        -- long trip signal
        {long_expr} AS long_n,

        -- busy-first scoring, includes hourly/long when available
        ({w_vol}*w.vol_n + {w_pay}*w.pay_n + {w_hourly}*w.hourly_n + {w_long}*({long_expr})) AS moment_score,

        -- baseline equivalents (use same weights)
        ({w_vol}*z.base_vol_n + {w_pay}*z.base_pay_n + {w_hourly}*z.base_hourly_n
          + {w_long}*(
            CASE
              WHEN {1 if (has_miles and has_time) else 0} = 1 THEN (0.6*z.base_miles_n + 0.4*z.base_mins_n)
              WHEN {1 if has_miles else 0} = 1 THEN z.base_miles_n
              WHEN {1 if has_time else 0} = 1 THEN z.base_mins_n
              ELSE 0.0
            END
          )
        ) AS base_score,

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
      avg_trip_minutes,
      pay_per_hour_zone,
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

        for (zid, dow_m, bin_start_min, pickups, avg_pay, avg_miles, avg_mins, pph, rating) in batch:
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

            props: Dict[str, Any] = {
                "LocationID": zid_i,
                "zone_name": name_by_id.get(zid_i, ""),
                "borough": borough_by_id.get(zid_i, ""),
                "rating": r,
                "bucket": bucket,
                "pickups": int(pickups),
                "avg_driver_pay": None if avg_pay is None else float(avg_pay),
            }

            # Only include new metrics if they exist (fallback safe)
            if avg_miles is not None:
                props["avg_trip_miles"] = float(avg_miles)
            else:
                props["avg_trip_miles"] = None

            if avg_mins is not None:
                props["avg_trip_minutes"] = float(avg_mins)
            else:
                props["avg_trip_minutes"] = None

            if pph is not None:
                props["pay_per_hour_zone"] = float(pph)
            else:
                props["pay_per_hour_zone"] = None

            props["debug_trip_time_unit"] = trip_time_unit  # helps you verify unit used

            props["avg_tips"] = None  # keep as you had it

            props["style"] = {
                "color": fill,
                "opacity": 0,
                "weight": 0,
                "fillColor": fill,
                "fillOpacity": 0.82
            }

            current_features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": props,
            })

    if not any_rows:
        raise RuntimeError("No data after filtering. Lower min_trips_per_window.")

    flush_frame()

    (out_dir / "timeline.json").write_text(
        json.dumps({"timeline": timeline, "count": len(timeline)}, separators=(",", ":")),
        encoding="utf-8"
    )

    con.close()

    return {
        "ok": True,
        "count": len(timeline),
        "frames_dir": str(out_dir),
        "rows": total_rows,
        "detected": {
            "PULocationID": cols["PULocationID"],
            "pickup_datetime": cols["pickup_datetime"],
            "driver_pay": cols["driver_pay"],
            "trip_miles": cols["trip_miles"],
            "trip_time": cols["trip_time"],
            "trip_time_unit_used": trip_time_unit,
        },
        "weights_used": {
            "vol": round(w_vol, 4),
            "pay": round(w_pay, 4),
            "hourly": round(w_hourly, 4),
            "long": round(w_long, 4),
        },
    }