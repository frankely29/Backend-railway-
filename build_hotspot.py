from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta
import zipfile
import json

import duckdb
import pandas as pd
import geopandas as gpd
import requests


# Official NYC TLC taxi zones (same dataset you were using before)
TAXI_ZONES_ZIP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"


def ensure_zones_geojson(data_dir: Path, force: bool = False) -> Path:
    """
    Downloads taxi_zones.zip and converts it to /data/taxi_zones.geojson
    Runs once and then stays persistent in the Railway Volume.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    geojson_path = data_dir / "taxi_zones.geojson"
    if geojson_path.exists() and geojson_path.stat().st_size > 0 and not force:
        return geojson_path

    zip_path = data_dir / "taxi_zones.zip"
    if (not zip_path.exists()) or zip_path.stat().st_size == 0 or force:
        r = requests.get(TAXI_ZONES_ZIP_URL, timeout=120)
        r.raise_for_status()
        zip_path.write_bytes(r.content)

    extract_dir = data_dir / "taxi_zones_extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)

    # Extract zip
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    shp_files = list(extract_dir.rglob("*.shp"))
    if not shp_files:
        raise RuntimeError("Could not find .shp after extracting taxi_zones.zip")

    # Read shapefile, convert to WGS84, write GeoJSON
    gdf = gpd.read_file(shp_files[0])
    if "LocationID" not in gdf.columns:
        # some versions have different casing
        low = {c.lower(): c for c in gdf.columns}
        if "locationid" in low:
            gdf = gdf.rename(columns={low["locationid"]: "LocationID"})
        else:
            raise RuntimeError(f"Shapefile missing LocationID column. Columns: {list(gdf.columns)}")

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    gdf = gdf.to_crs(epsg=4326)

    geojson_path.write_text(gdf.to_json(), encoding="utf-8")
    return geojson_path


def color_bucket_from_rating(rating: int) -> str:
    """
    STRICT mandatory rules:
      Green = Best
      Blue = Medium
      Sky = Normal
      Red = Avoid
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
    """
    Output schema (stable):
    {
      "timeline": ["YYYY-MM-DDTHH:MM:SS", ...],
      "frames": [
        {
          "time": "...",
          "polygons": { "type":"FeatureCollection", "features":[...Feature...] }
        }
      ]
    }

    Each Feature has:
      properties: { LocationID, rating (1–100), pickups, avg_driver_pay, avg_tips, style{fillColor...} }
    """

    # Load zone geometry
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
        raise RuntimeError("taxi_zones.geojson did not contain usable LocationID geometry.")

    con = duckdb.connect(database=":memory:")

    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    # Aggregate pickups/pay/tips per (dow, 20-min bin, zone)
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
        EXTRACT('dow' FROM pickup_datetime) AS dow_i,      -- 0=Sun..6=Sat
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
        raise RuntimeError("No data after filtering. Lower min_trips_per_window.")

    # Convert dow to Monday=0..Sunday=6
    df["dow_i"] = df["dow_i"].astype(int)
    df["dow_m"] = df["dow_i"].apply(lambda d: 6 if d == 0 else d - 1)

    # Score per window using minmax normalization across zones in same window
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

    df["score01"] = (0.60 * df["vol_n"]) + (0.30 * df["pay_n"]) + (0.10 * df["tip_n"])
    df["rating"] = (1 + (99 * df["score01"].clip(0, 1))).round().astype(int)

    # Build frames
    week_start = datetime(2025, 1, 6, 0, 0, 0)  # Monday baseline
    timeline: List[str] = []
    frames: List[Dict[str, Any]] = []

    for (dow_m, bin_start_min), g in df.groupby(["dow_m", "bin_start_min"]):
        hour = int(bin_start_min // 60)
        minute = int(bin_start_min % 60)

        ts = week_start + timedelta(days=int(dow_m), hours=hour, minutes=minute)
        ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S")

        feats = []
        for _, r in g.iterrows():
            zid = int(r["PULocationID"])
            geom = geom_by_id.get(zid)
            if not geom:
                continue

            rating = int(r["rating"])
            fill = color_bucket_from_rating(rating)

            feats.append({
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
                        "weight": 0,           # NO OUTLINE (you said you don’t want it)
                        "fillColor": fill,
                        "fillOpacity": 0.55
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