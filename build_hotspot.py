from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import json
import duckdb


# NYC TLC minimums you gave (used only when driver_pay missing AND we have miles+minutes)
MIN_PER_MILE = 1.241
MIN_PER_MINUTE = 0.659


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


def _detect_columns(con: duckdb.DuckDBPyConnection, parquet_sql: str) -> Dict[str, Any]:
    """
    Detect available columns in the parquet(s) and decide which to use.

    Returns a dict with:
      - columns_all (lowercased)
      - chosen: pulocationid, pickup_datetime, driver_pay, trip_miles, trip_minutes (optional)
      - trip_minutes_unit: "minutes" | "seconds" | None
      - samples: quick stats for chosen optional columns
    """
    # read schema
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{parquet_sql}])").fetchall()
    cols = [str(r[0]) for r in rows]
    cols_l = [c.lower() for c in cols]
    set_l = set(cols_l)

    def pick(*cands: str) -> Optional[str]:
        for cand in cands:
            if cand.lower() in set_l:
                # return the original column name (exact case) if possible
                idx = cols_l.index(cand.lower())
                return cols[idx]
        return None

    chosen = {
        "pulocationid": pick("PULocationID", "pu_location_id", "pulocation_id"),
        "pickup_datetime": pick("pickup_datetime", "tpep_pickup_datetime", "pickup_time", "trip_pickup_datetime"),
        "driver_pay": pick("driver_pay", "driverpay", "base_driver_pay", "driver_compensation"),
        # distance candidates
        "trip_miles": pick("trip_miles", "trip_distance", "trip_distance_miles", "distance_miles", "distance"),
        # duration candidates
        "trip_minutes": pick("trip_minutes", "trip_duration_minutes", "duration_minutes", "trip_time_minutes", "minutes"),
    }

    # If no explicit minutes col, try seconds-ish duration columns
    if chosen["trip_minutes"] is None:
        sec_col = pick(
            "trip_seconds", "trip_duration_seconds", "duration_seconds", "trip_time_seconds",
            "trip_time", "duration", "seconds"
        )
        if sec_col is not None:
            chosen["trip_minutes"] = sec_col  # we'll infer unit below

    # Infer unit for trip_minutes if it came from a generic "duration/trip_time" column
    trip_minutes_unit: Optional[str] = None
    samples: Dict[str, Any] = {}

    if chosen["trip_minutes"]:
        nm = chosen["trip_minutes"].lower()
        if "sec" in nm or "second" in nm:
            trip_minutes_unit = "seconds"
        elif "min" in nm or "minute" in nm:
            trip_minutes_unit = "minutes"
        else:
            # try to infer from typical magnitudes (quick sample)
            try:
                q = f"""
                SELECT
                  approx_quantile(TRY_CAST("{chosen["trip_minutes"]}" AS DOUBLE), 0.50) AS p50,
                  approx_quantile(TRY_CAST("{chosen["trip_minutes"]}" AS DOUBLE), 0.90) AS p90
                FROM read_parquet([{parquet_sql}])
                WHERE "{chosen["trip_minutes"]}" IS NOT NULL
                """
                p50, p90 = con.execute(q).fetchone()
                samples["trip_minutes_p50_raw"] = None if p50 is None else float(p50)
                samples["trip_minutes_p90_raw"] = None if p90 is None else float(p90)

                # crude but safe:
                # if p90 is huge (e.g. > 600), it's almost certainly seconds
                # if p90 is modest (e.g. < 240), it's probably minutes
                if p90 is not None and float(p90) > 600:
                    trip_minutes_unit = "seconds"
                else:
                    trip_minutes_unit = "minutes"
            except Exception:
                trip_minutes_unit = "minutes"

    # small samples for miles too
    if chosen["trip_miles"]:
        try:
            q = f"""
            SELECT
              approx_quantile(TRY_CAST("{chosen["trip_miles"]}" AS DOUBLE), 0.50) AS p50,
              approx_quantile(TRY_CAST("{chosen["trip_miles"]}" AS DOUBLE), 0.90) AS p90
            FROM read_parquet([{parquet_sql}])
            WHERE "{chosen["trip_miles"]}" IS NOT NULL
            """
            p50, p90 = con.execute(q).fetchone()
            samples["trip_miles_p50"] = None if p50 is None else float(p50)
            samples["trip_miles_p90"] = None if p90 is None else float(p90)
        except Exception:
            pass

    # sample pay too
    if chosen["driver_pay"]:
        try:
            q = f"""
            SELECT
              approx_quantile(TRY_CAST("{chosen["driver_pay"]}" AS DOUBLE), 0.50) AS p50,
              approx_quantile(TRY_CAST("{chosen["driver_pay"]}" AS DOUBLE), 0.90) AS p90
            FROM read_parquet([{parquet_sql}])
            WHERE "{chosen["driver_pay"]}" IS NOT NULL
            """
            p50, p90 = con.execute(q).fetchone()
            samples["driver_pay_p50"] = None if p50 is None else float(p50)
            samples["driver_pay_p90"] = None if p90 is None else float(p90)
        except Exception:
            pass

    return {
        "columns_all": cols_l,
        "chosen": chosen,
        "trip_minutes_unit": trip_minutes_unit,
        "samples": samples,
    }


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
      - each feature has properties:
          LocationID, zone_name, borough,
          rating, bucket,
          pickups,
          avg_driver_pay,
          (optional if inputs exist) avg_trip_miles, avg_trip_minutes,
          (optional) total_driver_pay, pay_per_hour_zone,
          style(fillColor)

    Fallback guarantee:
      - If trip time/miles are missing, we do your old logic exactly (pickups + driver_pay).
      - If driver_pay is missing but miles+minutes exist, we estimate pay using your minimum rates.
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

    detected = _detect_columns(con, parquet_sql)
    chosen = detected["chosen"]

    if not chosen["pulocationid"] or not chosen["pickup_datetime"]:
        raise RuntimeError(
            "Missing required columns. Need PULocationID + pickup_datetime (or equivalent). "
            f"Detected chosen={chosen}"
        )

    have_pay = chosen["driver_pay"] is not None
    have_miles = chosen["trip_miles"] is not None
    have_minutes = chosen["trip_minutes"] is not None
    minutes_unit = detected["trip_minutes_unit"] if have_minutes else None

    # Build expressions safely
    pu_col = f'"{chosen["pulocationid"]}"'
    dt_col = f'"{chosen["pickup_datetime"]}"'

    pay_expr = "NULL"
    if have_pay:
        pay_expr = f'TRY_CAST("{chosen["driver_pay"]}" AS DOUBLE)'

    miles_expr = "NULL"
    if have_miles:
        miles_expr = f'TRY_CAST("{chosen["trip_miles"]}" AS DOUBLE)'

    minutes_expr = "NULL"
    if have_minutes:
        raw = f'TRY_CAST("{chosen["trip_minutes"]}" AS DOUBLE)'
        if minutes_unit == "seconds":
            minutes_expr = f"({raw} / 60.0)"
        else:
            minutes_expr = raw

    # If driver_pay missing but we have miles+minutes, estimate pay
    # If driver_pay exists, keep it as the main pay metric (old behavior)
    estimated_pay_expr = pay_expr
    if (not have_pay) and have_miles and have_minutes:
        estimated_pay_expr = f"""
        (
          ({miles_expr})*{MIN_PER_MILE} +
          ({minutes_expr})*{MIN_PER_MINUTE}
        )
        """

    # For scoring "pay", prefer pay_per_hour_zone when we can compute it
    # (This is still data-driven; if pay missing, it becomes NULL and falls back.)
    # We'll compute pay_per_hour_zone = SUM(estimated_pay)/hours_in_bin
    hours_in_bin = float(bin_minutes) / 60.0

    # Optional: long-trip signal
    use_long_trip = have_miles  # only if we have miles

    # Weights: keep your old logic unless miles exists.
    # Old: 0.85 volume + 0.15 pay
    # New (only if miles exists): 0.75 volume + 0.15 pay + 0.10 long_trip
    if use_long_trip:
        w_vol, w_pay, w_mi = 0.75, 0.15, 0.10
    else:
        w_vol, w_pay, w_mi = 0.85, 0.15, 0.0

    # ----------------------------
    # SQL build
    # ----------------------------
    sql = f"""
    WITH base AS (
      SELECT
        CAST({pu_col} AS INTEGER) AS PULocationID,
        {dt_col} AS pickup_datetime,
        {estimated_pay_expr} AS driver_pay_calc,
        {miles_expr} AS trip_miles,
        {minutes_expr} AS trip_minutes
      FROM read_parquet([{parquet_sql}])
      WHERE {pu_col} IS NOT NULL AND {dt_col} IS NOT NULL
    ),
    t AS (
      SELECT
        PULocationID,
        CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,  -- 0=Sun..6=Sat
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay_calc,
        trip_miles,
        trip_minutes
      FROM base
    ),
    binned AS (
      SELECT
        PULocationID,
        CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,  -- Mon=0..Sun=6
        CAST(FLOOR((hour_i*60 + minute_i) / {int(bin_minutes)}) * {int(bin_minutes)} AS INTEGER) AS bin_start_min,
        driver_pay_calc,
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
        AVG(driver_pay_calc) AS avg_driver_pay,
        SUM(driver_pay_calc) AS total_driver_pay,
        AVG(trip_miles) AS avg_trip_miles,
        AVG(trip_minutes) AS avg_trip_minutes
      FROM binned
      GROUP BY 1,2,3
      HAVING COUNT(*) >= {int(min_trips_per_window)}
    ),

    scored_inputs AS (
      SELECT
        *,
        (total_driver_pay / {hours_in_bin}) AS pay_per_hour_zone,
        LN(1 + pickups) AS log_pickups
      FROM agg
    ),

    -- Per-window ranks (percentile-rank style, airport-safe)
    win AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY log_pickups) AS rn_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY COALESCE(pay_per_hour_zone, avg_driver_pay)) AS rn_pay,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY COALESCE(avg_trip_miles, 0.0)) AS rn_miles,
        COUNT(*) OVER (PARTITION BY dow_m, bin_start_min) AS n_in_window
      FROM scored_inputs
    ),
    win_scored AS (
      SELECT
        PULocationID, dow_m, bin_start_min,
        pickups, avg_driver_pay, total_driver_pay, pay_per_hour_zone, avg_trip_miles, avg_trip_minutes,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_pickups - 1) * 1.0 / (n_in_window - 1) END AS vol_n,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_pay - 1) * 1.0 / (n_in_window - 1) END AS pay_n,
        CASE WHEN n_in_window <= 1 THEN 0.0 ELSE (rn_miles - 1) * 1.0 / (n_in_window - 1) END AS miles_n
      FROM win
    ),

    -- Baseline per-zone (rank-based, airport-safe)
    zone_base AS (
      SELECT
        PULocationID,
        LN(1 + AVG(pickups)) AS base_log_pickups,
        AVG(COALESCE(pay_per_hour_zone, avg_driver_pay)) AS base_pay_metric,
        AVG(avg_trip_miles) AS base_trip_miles
      FROM scored_inputs
      GROUP BY 1
    ),
    zone_ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY base_log_pickups) AS rn_base_pickups,
        ROW_NUMBER() OVER (ORDER BY base_pay_metric) AS rn_base_pay,
        ROW_NUMBER() OVER (ORDER BY COALESCE(base_trip_miles, 0.0)) AS rn_base_miles,
        COUNT(*) OVER () AS n_zones
      FROM zone_base
    ),
    zone_norm AS (
      SELECT
        PULocationID,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_pickups - 1) * 1.0 / (n_zones - 1) END AS base_vol_n,
        CASE WHEN n_zones <= 1 THEN 0.0 ELSE (rn_base_pay - 1) * 1.0 / (n_zones - 1) END AS base_pay_n,
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
        w.total_driver_pay,
        w.pay_per_hour_zone,
        w.avg_trip_miles,
        w.avg_trip_minutes,

        ({w_vol}*w.vol_n + {w_pay}*w.pay_n + {w_mi}*w.miles_n) AS moment_score,
        ({w_vol}*z.base_vol_n + {w_pay}*z.base_pay_n + {w_mi}*z.base_miles_n) AS base_score,

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
      total_driver_pay,
      pay_per_hour_zone,
      avg_trip_miles,
      avg_trip_minutes,
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
            "meta": {
                "bin_minutes": int(bin_minutes),
                "min_trips_per_window": int(min_trips_per_window),
                "columns_chosen": chosen,
                "trip_minutes_unit": minutes_unit,
                "weights": {"vol": w_vol, "pay": w_pay, "miles": w_mi},
            },
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

        for (zid, dow_m, bin_start_min, pickups, avg_pay, total_pay, pay_per_hr, avg_mi, avg_min, rating) in batch:
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

                    # NEW (safe): may be null if columns missing
                    "total_driver_pay": None if total_pay is None else float(total_pay),
                    "pay_per_hour_zone": None if pay_per_hr is None else float(pay_per_hr),
                    "avg_trip_miles": None if avg_mi is None else float(avg_mi),
                    "avg_trip_minutes": None if avg_min is None else float(avg_min),

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
        json.dumps({
            "timeline": timeline,
            "count": len(timeline),
            "meta": {
                "bin_minutes": int(bin_minutes),
                "min_trips_per_window": int(min_trips_per_window),
                "columns_chosen": chosen,
                "trip_minutes_unit": minutes_unit,
                "weights": {"vol": w_vol, "pay": w_pay, "miles": w_mi},
                "samples": detected.get("samples", {}),
            }
        }, separators=(",", ":")),
        encoding="utf-8"
    )

    return {
        "ok": True,
        "count": len(timeline),
        "frames_dir": str(out_dir),
        "rows": total_rows,
        "debug": {
            "columns_chosen": chosen,
            "trip_minutes_unit": minutes_unit,
            "weights": {"vol": w_vol, "pay": w_pay, "miles": w_mi},
            "samples": detected.get("samples", {}),
        }
    }