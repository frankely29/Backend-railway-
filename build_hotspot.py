from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import json
import duckdb

from zone_earnings_engine import build_zone_earnings_shadow_sql
from zone_mode_profiles import ZONE_MODE_PROFILES

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
          LocationID, zone_name, borough, rating, bucket, pickups, avg_driver_pay, style(fillColor)

    FACTS + REALISM GUARANTEE:
      - Per-window normalization is percentile-rank based (NOT min/max), so airports cannot flatten the city.
      - Baseline per-zone normalization is ALSO percentile-rank based (NOT global min/max),
        so airports cannot compress baseline scores either.
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
    schema_rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{parquet_sql}])").fetchall()
    available_columns = {str(row[0]) for row in schema_rows}

    # ----------------------------
    # SQL build
    #
    # - "busy" drives score (pickups) more than pay, as you requested.
    # - Per-window normalization uses percentile rank (no max/min) so airports don't dominate.
    # - Baseline per-zone normalization ALSO uses percentile rank (no global min/max),
    #   so airports cannot compress base_score either.
    # - Confidence scales down low-sample windows (still data-driven).
    # ----------------------------
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

    -- ----------------------------
    -- Per-window percentile-rank normalization (robust to airport outliers)
    -- ----------------------------
    win AS (
      SELECT
        *,
        LN(1 + pickups) AS log_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY LN(1 + pickups)) AS rn_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY avg_driver_pay) AS rn_pay,
        COUNT(*) OVER (PARTITION BY dow_m, bin_start_min) AS n_in_window
      FROM agg
    ),
    win_scored AS (
      SELECT
        PULocationID, dow_m, bin_start_min, pickups, avg_driver_pay,
        CASE
          WHEN n_in_window <= 1 THEN 0.0
          ELSE (rn_pickups - 1) * 1.0 / (n_in_window - 1)
        END AS vol_n,
        CASE
          WHEN n_in_window <= 1 THEN 0.0
          ELSE (rn_pay - 1) * 1.0 / (n_in_window - 1)
        END AS pay_n
      FROM win
    ),

    -- ----------------------------
    -- Baseline per-zone (historical typical level)
    -- IMPORTANT CHANGE: baseline normalization uses percentile ranks (NOT min/max)
    -- so airports cannot compress baseline for all other zones.
    -- ----------------------------
    zone_base AS (
      SELECT
        PULocationID,
        LN(1 + AVG(pickups)) AS base_log_pickups,
        AVG(avg_driver_pay) AS base_pay
      FROM agg
      GROUP BY 1
    ),
    zone_ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY base_log_pickups) AS rn_base_pickups,
        ROW_NUMBER() OVER (ORDER BY base_pay) AS rn_base_pay,
        COUNT(*) OVER () AS n_zones
      FROM zone_base
    ),
    zone_norm AS (
      SELECT
        PULocationID,
        CASE
          WHEN n_zones <= 1 THEN 0.0
          ELSE (rn_base_pickups - 1) * 1.0 / (n_zones - 1)
        END AS base_vol_n,
        CASE
          WHEN n_zones <= 1 THEN 0.0
          ELSE (rn_base_pay - 1) * 1.0 / (n_zones - 1)
        END AS base_pay_n
      FROM zone_ranked
    ),

    final AS (
      SELECT
        w.PULocationID,
        w.dow_m,
        w.bin_start_min,
        w.pickups,
        w.avg_driver_pay,

        -- Your rule: mostly busy, some driver pay
        (0.85*w.vol_n + 0.15*w.pay_n) AS moment_score,
        (0.85*z.base_vol_n + 0.15*z.base_pay_n) AS base_score,

        -- confidence: more pickups -> more trust in moment_score
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

    shadow_sql = build_zone_earnings_shadow_sql(
        parquet_list,
        bin_minutes=int(bin_minutes),
        min_trips_per_window=int(min_trips_per_window),
        profile=ZONE_MODE_PROFILES["citywide_v2"],
        manhattan_profile=ZONE_MODE_PROFILES["manhattan_v2"],
        bronx_wash_heights_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v2"],
        queens_profile=ZONE_MODE_PROFILES["queens_v2"],
        brooklyn_profile=ZONE_MODE_PROFILES["brooklyn_v2"],
        staten_island_profile=ZONE_MODE_PROFILES["staten_island_v2"],
        available_columns=available_columns,
    )

    shadow_rows = con.execute(shadow_sql).fetchall()
    shadow_by_key: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
    for row in shadow_rows:
        (
            pu_id,
            s_dow_m,
            s_bin_start_min,
            pickups_now,
            pickups_next,
            median_driver_pay,
            median_pay_per_min,
            median_pay_per_mile,
            median_request_to_pickup_min,
            short_trip_share,
            shared_ride_share,
            downstream_next_value_raw,
            demand_now_n,
            demand_next_n,
            pay_n,
            pay_per_min_n,
            pay_per_mile_n,
            pickup_friction_penalty_n,
            short_trip_penalty_n,
            shared_ride_penalty_n,
            downstream_value_n,
            earnings_shadow_score_citywide_v2,
            earnings_shadow_confidence_citywide_v2,
            earnings_shadow_rating_citywide_v2,
            earnings_shadow_bucket_citywide_v2,
            earnings_shadow_color_citywide_v2,
            earnings_shadow_score_manhattan_v2,
            earnings_shadow_confidence_manhattan_v2,
            earnings_shadow_rating_manhattan_v2,
            earnings_shadow_bucket_manhattan_v2,
            earnings_shadow_color_manhattan_v2,
            earnings_shadow_score_bronx_wash_heights_v2,
            earnings_shadow_confidence_bronx_wash_heights_v2,
            earnings_shadow_rating_bronx_wash_heights_v2,
            earnings_shadow_bucket_bronx_wash_heights_v2,
            earnings_shadow_color_bronx_wash_heights_v2,
            earnings_shadow_score_queens_v2,
            earnings_shadow_confidence_queens_v2,
            earnings_shadow_rating_queens_v2,
            earnings_shadow_bucket_queens_v2,
            earnings_shadow_color_queens_v2,
            earnings_shadow_score_brooklyn_v2,
            earnings_shadow_confidence_brooklyn_v2,
            earnings_shadow_rating_brooklyn_v2,
            earnings_shadow_bucket_brooklyn_v2,
            earnings_shadow_color_brooklyn_v2,
            earnings_shadow_score_staten_island_v2,
            earnings_shadow_confidence_staten_island_v2,
            earnings_shadow_rating_staten_island_v2,
            earnings_shadow_bucket_staten_island_v2,
            earnings_shadow_color_staten_island_v2,
        ) = row
        shadow_by_key[(int(pu_id), int(s_dow_m), int(s_bin_start_min))] = {
            "next_pickups_shadow": None if pickups_next is None else int(pickups_next),
            "median_driver_pay_shadow": None if median_driver_pay is None else float(median_driver_pay),
            "median_pay_per_min_shadow": None if median_pay_per_min is None else float(median_pay_per_min),
            "median_pay_per_mile_shadow": None if median_pay_per_mile is None else float(median_pay_per_mile),
            "median_request_to_pickup_min_shadow": None if median_request_to_pickup_min is None else float(median_request_to_pickup_min),
            "short_trip_share_shadow": None if short_trip_share is None else float(short_trip_share),
            "shared_ride_share_shadow": None if shared_ride_share is None else float(shared_ride_share),
            "downstream_value_shadow": None if downstream_next_value_raw is None else float(downstream_next_value_raw),
            "demand_now_n_shadow": None if demand_now_n is None else float(demand_now_n),
            "demand_next_n_shadow": None if demand_next_n is None else float(demand_next_n),
            "pay_n_shadow": None if pay_n is None else float(pay_n),
            "pay_per_min_n_shadow": None if pay_per_min_n is None else float(pay_per_min_n),
            "pay_per_mile_n_shadow": None if pay_per_mile_n is None else float(pay_per_mile_n),
            "pickup_friction_penalty_n_shadow": None if pickup_friction_penalty_n is None else float(pickup_friction_penalty_n),
            "short_trip_penalty_n_shadow": None if short_trip_penalty_n is None else float(short_trip_penalty_n),
            "shared_ride_penalty_n_shadow": None if shared_ride_penalty_n is None else float(shared_ride_penalty_n),
            "downstream_value_n_shadow": None if downstream_value_n is None else float(downstream_value_n),
            "earnings_shadow_score_citywide_v2": None if earnings_shadow_score_citywide_v2 is None else float(earnings_shadow_score_citywide_v2),
            "earnings_shadow_confidence_citywide_v2": None if earnings_shadow_confidence_citywide_v2 is None else float(earnings_shadow_confidence_citywide_v2),
            "earnings_shadow_rating_citywide_v2": None if earnings_shadow_rating_citywide_v2 is None else int(earnings_shadow_rating_citywide_v2),
            "earnings_shadow_bucket_citywide_v2": earnings_shadow_bucket_citywide_v2,
            "earnings_shadow_color_citywide_v2": earnings_shadow_color_citywide_v2,
            "earnings_shadow_score_manhattan_v2": None if earnings_shadow_score_manhattan_v2 is None else float(earnings_shadow_score_manhattan_v2),
            "earnings_shadow_confidence_manhattan_v2": None if earnings_shadow_confidence_manhattan_v2 is None else float(earnings_shadow_confidence_manhattan_v2),
            "earnings_shadow_rating_manhattan_v2": None if earnings_shadow_rating_manhattan_v2 is None else int(earnings_shadow_rating_manhattan_v2),
            "earnings_shadow_bucket_manhattan_v2": earnings_shadow_bucket_manhattan_v2,
            "earnings_shadow_color_manhattan_v2": earnings_shadow_color_manhattan_v2,
            "earnings_shadow_score_bronx_wash_heights_v2": None if earnings_shadow_score_bronx_wash_heights_v2 is None else float(earnings_shadow_score_bronx_wash_heights_v2),
            "earnings_shadow_confidence_bronx_wash_heights_v2": None if earnings_shadow_confidence_bronx_wash_heights_v2 is None else float(earnings_shadow_confidence_bronx_wash_heights_v2),
            "earnings_shadow_rating_bronx_wash_heights_v2": None if earnings_shadow_rating_bronx_wash_heights_v2 is None else int(earnings_shadow_rating_bronx_wash_heights_v2),
            "earnings_shadow_bucket_bronx_wash_heights_v2": earnings_shadow_bucket_bronx_wash_heights_v2,
            "earnings_shadow_color_bronx_wash_heights_v2": earnings_shadow_color_bronx_wash_heights_v2,
            "earnings_shadow_score_queens_v2": None if earnings_shadow_score_queens_v2 is None else float(earnings_shadow_score_queens_v2),
            "earnings_shadow_confidence_queens_v2": None if earnings_shadow_confidence_queens_v2 is None else float(earnings_shadow_confidence_queens_v2),
            "earnings_shadow_rating_queens_v2": None if earnings_shadow_rating_queens_v2 is None else int(earnings_shadow_rating_queens_v2),
            "earnings_shadow_bucket_queens_v2": earnings_shadow_bucket_queens_v2,
            "earnings_shadow_color_queens_v2": earnings_shadow_color_queens_v2,
            "earnings_shadow_score_brooklyn_v2": None if earnings_shadow_score_brooklyn_v2 is None else float(earnings_shadow_score_brooklyn_v2),
            "earnings_shadow_confidence_brooklyn_v2": None if earnings_shadow_confidence_brooklyn_v2 is None else float(earnings_shadow_confidence_brooklyn_v2),
            "earnings_shadow_rating_brooklyn_v2": None if earnings_shadow_rating_brooklyn_v2 is None else int(earnings_shadow_rating_brooklyn_v2),
            "earnings_shadow_bucket_brooklyn_v2": earnings_shadow_bucket_brooklyn_v2,
            "earnings_shadow_color_brooklyn_v2": earnings_shadow_color_brooklyn_v2,
            "earnings_shadow_score_staten_island_v2": None if earnings_shadow_score_staten_island_v2 is None else float(earnings_shadow_score_staten_island_v2),
            "earnings_shadow_confidence_staten_island_v2": None if earnings_shadow_confidence_staten_island_v2 is None else float(earnings_shadow_confidence_staten_island_v2),
            "earnings_shadow_rating_staten_island_v2": None if earnings_shadow_rating_staten_island_v2 is None else int(earnings_shadow_rating_staten_island_v2),
            "earnings_shadow_bucket_staten_island_v2": earnings_shadow_bucket_staten_island_v2,
            "earnings_shadow_color_staten_island_v2": earnings_shadow_color_staten_island_v2,
        }

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

            zid_i = int(zid)
            geom = geom_by_id.get(zid_i)
            if not geom:
                continue

            r = int(rating)
            bucket, fill = bucket_and_color_from_rating(r)
            shadow_props = shadow_by_key.get((zid_i, int(dow_m), int(bin_start_min)), {})

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
                    "avg_tips": None,
                    "style": {
                        "color": fill,
                        "opacity": 0,
                        "weight": 0,
                        "fillColor": fill,
                        "fillOpacity": 0.82
                    },
                    "next_pickups_shadow": shadow_props.get("next_pickups_shadow"),
                    "median_driver_pay_shadow": shadow_props.get("median_driver_pay_shadow"),
                    "median_pay_per_min_shadow": shadow_props.get("median_pay_per_min_shadow"),
                    "median_pay_per_mile_shadow": shadow_props.get("median_pay_per_mile_shadow"),
                    "median_request_to_pickup_min_shadow": shadow_props.get("median_request_to_pickup_min_shadow"),
                    "short_trip_share_shadow": shadow_props.get("short_trip_share_shadow"),
                    "shared_ride_share_shadow": shadow_props.get("shared_ride_share_shadow"),
                    "downstream_value_shadow": shadow_props.get("downstream_value_shadow"),
                    "demand_now_n_shadow": shadow_props.get("demand_now_n_shadow"),
                    "demand_next_n_shadow": shadow_props.get("demand_next_n_shadow"),
                    "pay_n_shadow": shadow_props.get("pay_n_shadow"),
                    "pay_per_min_n_shadow": shadow_props.get("pay_per_min_n_shadow"),
                    "pay_per_mile_n_shadow": shadow_props.get("pay_per_mile_n_shadow"),
                    "pickup_friction_penalty_n_shadow": shadow_props.get("pickup_friction_penalty_n_shadow"),
                    "short_trip_penalty_n_shadow": shadow_props.get("short_trip_penalty_n_shadow"),
                    "shared_ride_penalty_n_shadow": shadow_props.get("shared_ride_penalty_n_shadow"),
                    "downstream_value_n_shadow": shadow_props.get("downstream_value_n_shadow"),
                    "earnings_shadow_score_citywide_v2": shadow_props.get("earnings_shadow_score_citywide_v2"),
                    "earnings_shadow_confidence_citywide_v2": shadow_props.get("earnings_shadow_confidence_citywide_v2"),
                    "earnings_shadow_rating_citywide_v2": shadow_props.get("earnings_shadow_rating_citywide_v2"),
                    "earnings_shadow_bucket_citywide_v2": shadow_props.get("earnings_shadow_bucket_citywide_v2"),
                    "earnings_shadow_color_citywide_v2": shadow_props.get("earnings_shadow_color_citywide_v2"),
                    "earnings_shadow_score_manhattan_v2": shadow_props.get("earnings_shadow_score_manhattan_v2"),
                    "earnings_shadow_confidence_manhattan_v2": shadow_props.get("earnings_shadow_confidence_manhattan_v2"),
                    "earnings_shadow_rating_manhattan_v2": shadow_props.get("earnings_shadow_rating_manhattan_v2"),
                    "earnings_shadow_bucket_manhattan_v2": shadow_props.get("earnings_shadow_bucket_manhattan_v2"),
                    "earnings_shadow_color_manhattan_v2": shadow_props.get("earnings_shadow_color_manhattan_v2"),
                    "earnings_shadow_score_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_score_bronx_wash_heights_v2"),
                    "earnings_shadow_confidence_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_confidence_bronx_wash_heights_v2"),
                    "earnings_shadow_rating_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_rating_bronx_wash_heights_v2"),
                    "earnings_shadow_bucket_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_bucket_bronx_wash_heights_v2"),
                    "earnings_shadow_color_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_color_bronx_wash_heights_v2"),
                    "earnings_shadow_score_queens_v2": shadow_props.get("earnings_shadow_score_queens_v2"),
                    "earnings_shadow_confidence_queens_v2": shadow_props.get("earnings_shadow_confidence_queens_v2"),
                    "earnings_shadow_rating_queens_v2": shadow_props.get("earnings_shadow_rating_queens_v2"),
                    "earnings_shadow_bucket_queens_v2": shadow_props.get("earnings_shadow_bucket_queens_v2"),
                    "earnings_shadow_color_queens_v2": shadow_props.get("earnings_shadow_color_queens_v2"),
                    "earnings_shadow_score_brooklyn_v2": shadow_props.get("earnings_shadow_score_brooklyn_v2"),
                    "earnings_shadow_confidence_brooklyn_v2": shadow_props.get("earnings_shadow_confidence_brooklyn_v2"),
                    "earnings_shadow_rating_brooklyn_v2": shadow_props.get("earnings_shadow_rating_brooklyn_v2"),
                    "earnings_shadow_bucket_brooklyn_v2": shadow_props.get("earnings_shadow_bucket_brooklyn_v2"),
                    "earnings_shadow_color_brooklyn_v2": shadow_props.get("earnings_shadow_color_brooklyn_v2"),
                    "earnings_shadow_score_staten_island_v2": shadow_props.get("earnings_shadow_score_staten_island_v2"),
                    "earnings_shadow_confidence_staten_island_v2": shadow_props.get("earnings_shadow_confidence_staten_island_v2"),
                    "earnings_shadow_rating_staten_island_v2": shadow_props.get("earnings_shadow_rating_staten_island_v2"),
                    "earnings_shadow_bucket_staten_island_v2": shadow_props.get("earnings_shadow_bucket_staten_island_v2"),
                    "earnings_shadow_color_staten_island_v2": shadow_props.get("earnings_shadow_color_staten_island_v2"),
                }
            })

    if not any_rows:
        raise RuntimeError("No data after filtering. Lower min_trips_per_window.")

    flush_frame()

    (out_dir / "timeline.json").write_text(
        json.dumps({"timeline": timeline, "count": len(timeline)}, separators=(",", ":")),
        encoding="utf-8"
    )
    (out_dir / "scoring_shadow_manifest.json").write_text(
        json.dumps(
            {
                "engine_version": "team-joseo-score-v2-shadow",
                "engine_release": "team-joseo-score-v2-final-live",
                "source": "HVFHV",
                "bin_minutes": int(bin_minutes),
                "active_shadow_profile": "citywide_v2",
                "active_shadow_profiles": ["citywide_v2", "manhattan_v2", "bronx_wash_heights_v2", "queens_v2", "brooklyn_v2", "staten_island_v2"],
                "visible_profiles_live": ["citywide_v2", "manhattan_v2", "bronx_wash_heights_v2", "queens_v2", "brooklyn_v2", "staten_island_v2"],
                "base_color_truth": "tlc_hvfhs_earnings_opportunity",
                "community_caution_truth": "team_joseo_presence_only",
                "presence_logic_changed": False,
                "notes": [
                    "Base colors reflect Team Joseo earnings opportunity derived from HVFHV/Taxi Zone data.",
                    "Community crowding caution is separate and based only on Team Joseo live presence.",
                    "No real-time presence timing was changed by the score rollout.",
                ],
                "shadow_fields": [
                    "next_pickups_shadow",
                    "median_driver_pay_shadow",
                    "median_pay_per_min_shadow",
                    "median_pay_per_mile_shadow",
                    "median_request_to_pickup_min_shadow",
                    "short_trip_share_shadow",
                    "shared_ride_share_shadow",
                    "downstream_value_shadow",
                    "demand_now_n_shadow",
                    "demand_next_n_shadow",
                    "pay_n_shadow",
                    "pay_per_min_n_shadow",
                    "pay_per_mile_n_shadow",
                    "pickup_friction_penalty_n_shadow",
                    "short_trip_penalty_n_shadow",
                    "shared_ride_penalty_n_shadow",
                    "downstream_value_n_shadow",
                    "earnings_shadow_score_citywide_v2",
                    "earnings_shadow_confidence_citywide_v2",
                    "earnings_shadow_rating_citywide_v2",
                    "earnings_shadow_bucket_citywide_v2",
                    "earnings_shadow_color_citywide_v2",
                    "earnings_shadow_score_manhattan_v2",
                    "earnings_shadow_confidence_manhattan_v2",
                    "earnings_shadow_rating_manhattan_v2",
                    "earnings_shadow_bucket_manhattan_v2",
                    "earnings_shadow_color_manhattan_v2",
                    "earnings_shadow_score_bronx_wash_heights_v2",
                    "earnings_shadow_confidence_bronx_wash_heights_v2",
                    "earnings_shadow_rating_bronx_wash_heights_v2",
                    "earnings_shadow_bucket_bronx_wash_heights_v2",
                    "earnings_shadow_color_bronx_wash_heights_v2",
                    "earnings_shadow_score_queens_v2",
                    "earnings_shadow_confidence_queens_v2",
                    "earnings_shadow_rating_queens_v2",
                    "earnings_shadow_bucket_queens_v2",
                    "earnings_shadow_color_queens_v2",
                    "earnings_shadow_score_brooklyn_v2",
                    "earnings_shadow_confidence_brooklyn_v2",
                    "earnings_shadow_rating_brooklyn_v2",
                    "earnings_shadow_bucket_brooklyn_v2",
                    "earnings_shadow_color_brooklyn_v2",
                    "earnings_shadow_score_staten_island_v2",
                    "earnings_shadow_confidence_staten_island_v2",
                    "earnings_shadow_rating_staten_island_v2",
                    "earnings_shadow_bucket_staten_island_v2",
                    "earnings_shadow_color_staten_island_v2",
                ],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    return {"ok": True, "count": len(timeline), "frames_dir": str(out_dir), "rows": total_rows}
