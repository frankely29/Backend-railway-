from pathlib import Path
from typing import List
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import json


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def score_to_color_bucket(rating: int) -> str:
    """
    Your strict rules:
    Green = Best
    Blue = Medium
    Sky = Normal
    Red = Avoid
    """
    r = int(rating)
    if r >= 75:
        return "#00b050"  # green
    if r >= 55:
        return "#0066ff"  # blue
    if r >= 35:
        return "#66ccff"  # sky
    return "#e60000"      # red


def build_hotspots_json(
    parquet_files: List[Path],
    out_path: Path,
    bin_minutes: int = 20,
    min_trips_per_window: int = 10,
) -> None:
    """
    Output JSON schema:
      {
        "timeline": [...iso...],
        "frames": [
          {
            "time": "...iso...",
            "polygons": { "type":"FeatureCollection", "features":[...] }
          }
        ]
      }

    Requires zone geometry in: /data/taxi_zones.geojson
    Each feature in taxi_zones.geojson must include:
      properties.LocationID (or location_id) matching PULocationID.
    """

    data_dir = out_path.parent
    zones_path = data_dir / "taxi_zones.geojson"
    if not zones_path.exists():
        raise RuntimeError("Missing /data/taxi_zones.geojson (needed to draw zone polygons).")

    zones_geo = json.loads(zones_path.read_text(encoding="utf-8"))

    # Map LocationID -> geometry
    geom_by_id = {}
    for f in zones_geo.get("features", []):
        props = f.get("properties", {}) or {}
        zid = props.get("LocationID", props.get("location_id", props.get("locationid")))
        if zid is None:
            continue
        try:
            zid = int(zid)
        except Exception:
            continue
        geom_by_id[zid] = f.get("geometry")

    if not geom_by_id:
        raise RuntimeError("taxi_zones.geojson has no usable LocationID keys.")

    con = duckdb.connect(database=":memory:")
    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    # Aggregate by DOW + 20-min bin + PULocationID
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
        EXTRACT('dow' FROM pickup_datetime) AS dow_i,
        EXTRACT('hour' FROM pickup_datetime) AS hour_i,
        EXTRACT('minute' FROM pickup_datetime) AS minute_i,
        driver_pay,
        tips
      FROM base
    ),
    binned AS (
      SELECT
        PULocationID,
        CAST(dow_i AS INTEGER) AS dow_i,
        CAST(FLOOR((hour_i*60 + minute_i) / {int(bin_minutes)}) * {int(bin_minutes)} AS INTEGER) AS bin_start_min,
        driver_pay,
        tips
      FROM t
    )
    SELECT
      PULocationID,
      dow_i,
      bin_start_min,
      COUNT(*) AS pickups,
      AVG(driver_pay) AS avg_driver_pay,
      AVG(tips) AS avg_tips
    FROM binned
    GROUP BY 1,2,3
    HAVING COUNT(*) >= {int(min_trips_per_window)};
    """

    df = con.execute(sql).df()
    if df.empty:
        raise RuntimeError("No rows after filtering. Try lower min_trips_per_window.")

    # Normalize DOW: Mon=0..Sun=6 (convert from DuckDB: Sun=0..Sat=6)
    df["dow_i"] = df["dow_i"].astype(int)
    df["dow_m"] = df["dow_i"].apply(lambda d: 6 if d == 0 else d - 1)

    # Score per window
    def minmax(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce")
        mn = s.min(skipna=True)
        mx = s.max(skipna=True)
        if pd.isna(mn) or pd.isna(mx) or mx == mn:
            return pd.Series([0.0] * len(s), index=s.index)
        return (s - mn) / (mx - mn)

    df["vol_n"] = df.groupby(["dow_m", "bin_start_min"])["pickups"].transform(minmax)
    df["pay_n"] = df.groupby(["dow_m", "bin_start_min"])["avg_driver_pay"].transform(minmax)
    df["tip_n"] = df.groupby(["dow_m", "bin_start_min"])["avg_tips"].transform(minmax)

    df["score01"] = 0.60 * df["vol_n"] + 0.30 * df["pay_n"] + 0.10 * df["tip_n"]
    df["rating"] = (1 + (99 * df["score01"].clip(0, 1))).round().astype(int)

    # Build timeline frames
    week_start = datetime(2025, 1, 6, 0, 0, 0)  # Monday
    frames = []
    timeline = []

    for (dow_m, bin_start_min), g in df.groupby(["dow_m", "bin_start_min"]):
        hour = int(bin_start_min // 60)
        minute = int(bin_start_min % 60)

        ts = week_start + timedelta(days=int(dow_m), hours=hour, minutes=minute)
        ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S")

        features = []
        for _, r in g.iterrows():
            zid = int(r["PULocationID"])
            geom = geom_by_id.get(zid)
            if not geom:
                continue

            rating = int(r["rating"])
            fill = score_to_color_bucket(rating)

            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "LocationID": zid,
                    "rating": rating,
                    "pickups": int(r["pickups"]),
                    "avg_driver_pay": None if pd.isna(r["avg_driver_pay"]) else float(r["avg_driver_pay"]),
                    "avg_tips": None if pd.isna(r["avg_tips"]) else float(r["avg_tips"]),
                    "style": {
                        "color": fill,
                        "weight": 0,          # no outlines
                        "fillColor": fill,
                        "fillOpacity": 0.55
                    }
                }
            })

        fc = {"type": "FeatureCollection", "features": features}
        frames.append({"time": ts_iso, "polygons": fc})
        timeline.append(ts_iso)

    out = {"timeline": timeline, "frames": frames}
    out_path.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")