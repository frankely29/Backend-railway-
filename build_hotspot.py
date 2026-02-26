# FULL UPDATED FILE
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
    raise RuntimeError("Missing /data/taxi_zones.geojson.")


def bucket_and_color_from_rating(rating: int) -> tuple[str, str]:
    r = int(rating)
    if r >= 90: return "green", "#00b050"
    if r >= 80: return "purple", "#8000ff"
    if r >= 65: return "blue", "#0066ff"
    if r >= 45: return "sky", "#66ccff"
    if r >= 25: return "yellow", "#ffd400"
    return "red", "#e60000"


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
    name_by_id: Dict[int, str] = {}
    borough_by_id: Dict[int, str] = {}

    for f in zones.get("features", []):
        props = f.get("properties") or {}
        zid = props.get("LocationID")
        if zid is None:
            continue
        zid_int = int(zid)
        geom_by_id[zid_int] = f.get("geometry")
        name_by_id[zid_int] = str(props.get("zone") or "")
        borough_by_id[zid_int] = str(props.get("borough") or "")

    tmp_dir = out_dir.parent / "duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA temp_directory='{tmp_dir.as_posix()}'")

    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    # Detect columns
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{parquet_sql}])").fetchall()
    cols = {r[0].lower() for r in desc}

    has_time = "trip_time" in cols
    has_miles = "trip_miles" in cols

    sql = f"""
    WITH base AS (
      SELECT
        CAST(PULocationID AS INTEGER) AS PULocationID,
        pickup_datetime,
        TRY_CAST(driver_pay AS DOUBLE) AS driver_pay,
        {"TRY_CAST(trip_time AS DOUBLE)" if has_time else "NULL"} AS trip_time_sec,
        {"TRY_CAST(trip_miles AS DOUBLE)" if has_miles else "NULL"} AS trip_miles
      FROM read_parquet([{parquet_sql}])
      WHERE PULocationID IS NOT NULL
    ),
    t AS (
      SELECT
        PULocationID,
        CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay,
        trip_time_sec,
        trip_miles
      FROM base
    ),
    binned AS (
      SELECT
        PULocationID,
        CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,
        CAST(FLOOR((hour_i*60 + minute_i) / {bin_minutes}) * {bin_minutes} AS INTEGER) AS bin_start_min,
        driver_pay,
        trip_time_sec,
        trip_miles
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
        AVG(trip_time_sec)/60.0 AS avg_trip_minutes,
        CASE
          WHEN SUM(trip_time_sec) > 0
          THEN SUM(driver_pay) / (SUM(trip_time_sec)/3600.0)
          ELSE NULL
        END AS driver_pay_per_hour
      FROM binned
      GROUP BY 1,2,3
      HAVING COUNT(*) >= {min_trips_per_window}
    ),
    win AS (
      SELECT *,
        LN(1+pickups) AS log_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m,bin_start_min ORDER BY LN(1+pickups)) rn_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m,bin_start_min ORDER BY driver_pay_per_hour) rn_hourly,
        ROW_NUMBER() OVER (PARTITION BY dow_m,bin_start_min ORDER BY avg_trip_miles) rn_miles,
        COUNT(*) OVER (PARTITION BY dow_m,bin_start_min) n
      FROM agg
    ),
    scored AS (
      SELECT *,
        (rn_pickups-1)/(n-1) AS vol_n,
        (rn_hourly-1)/(n-1) AS hourly_n,
        (rn_miles-1)/(n-1) AS miles_n
      FROM win
    )
    SELECT
      PULocationID,dow_m,bin_start_min,pickups,
      avg_driver_pay,avg_trip_miles,avg_trip_minutes,
      driver_pay_per_hour,
      CAST(
        ROUND(
          1 + 99 *
          LEAST(
            GREATEST(
              (0.50*hourly_n + 0.35*vol_n + 0.15*miles_n),
              0.0
            ),
            1.0
          )
        ) AS INTEGER
      ) rating
    FROM scored
    ORDER BY dow_m,bin_start_min,PULocationID;
    """

    cur = con.execute(sql)

    week_start = datetime(2025,1,6,0,0,0)
    timeline=[]
    frame_count=0
    current_key=None
    current_features=[]
    current_time_iso=None

    def flush():
        nonlocal frame_count,current_features,current_time_iso
        if not current_time_iso: return
        timeline.append(current_time_iso)
        frame_path = out_dir / f"frame_{frame_count:06d}.json"
        frame_path.write_text(json.dumps({
            "time":current_time_iso,
            "polygons":{"type":"FeatureCollection","features":current_features}
        },separators=(",",":")))
        frame_count+=1
        current_features=[]
        current_time_iso=None

    while True:
        batch = cur.fetchmany(5000)
        if not batch: break

        for row in batch:
            zid,dow_m,bin_start,pickups,avg_pay,avg_mi,avg_min,pay_hr,rating=row
            key=(dow_m,bin_start)
            if current_key is None: current_key=key
            if key!=current_key:
                flush()
                current_key=key

            ts=week_start+timedelta(days=dow_m,hours=bin_start//60,minutes=bin_start%60)
            current_time_iso=ts.strftime("%Y-%m-%dT%H:%M:%S")

            bucket,fill=bucket_and_color_from_rating(rating)

            current_features.append({
                "type":"Feature",
                "geometry":geom_by_id.get(int(zid)),
                "properties":{
                    "LocationID":int(zid),
                    "zone_name":name_by_id.get(int(zid),""),
                    "borough":borough_by_id.get(int(zid),""),
                    "rating":int(rating),
                    "bucket":bucket,
                    "pickups":int(pickups),
                    "avg_driver_pay":avg_pay,
                    "avg_trip_miles":avg_mi,
                    "avg_trip_minutes":avg_min,
                    "driver_pay_per_hour":pay_hr,
                    "style":{
                        "color":fill,
                        "opacity":0,
                        "weight":0,
                        "fillColor":fill,
                        "fillOpacity":0.82
                    }
                }
            })

    flush()

    (out_dir/"timeline.json").write_text(
        json.dumps({"timeline":timeline,"count":len(timeline)},separators=(",",":"))
    )

    return {"ok":True,"count":len(timeline)}