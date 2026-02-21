from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import traceback

from build_hotspot import (
    ensure_zones_geojson,
    build_hotspots_json,
)

app = FastAPI()

# CORS so GitHub Pages (different domain) can fetch Railway endpoints
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten later if you want
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Railway persistent volume mount
DATA_DIR = Path("/data")
ZONES_GEOJSON = DATA_DIR / "taxi_zones.geojson"
OUT_PATH = DATA_DIR / "hotspots_20min.json"


@app.get("/")
def root():
    return {"status": "ok", "hint": "Use /docs for endpoints"}


@app.get("/status")
def status():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    parquets = sorted([p.name for p in DATA_DIR.glob("fhvhv_tripdata_*.parquet")])
    has_zones = ZONES_GEOJSON.exists() and ZONES_GEOJSON.stat().st_size > 0
    has_output = OUT_PATH.exists() and OUT_PATH.stat().st_size > 0
    return {
        "status": "ok",
        "data_dir": str(DATA_DIR),
        "parquets": parquets,
        "zones_geojson": ZONES_GEOJSON.name,
        "zones_present": has_zones,
        "output": OUT_PATH.name,
        "has_output": has_output,
        "output_mb": round(OUT_PATH.stat().st_size / 1024 / 1024, 2) if has_output else 0,
    }


@app.post("/upload_parquet")
async def upload_parquet(file: UploadFile = File(...)):
    """
    Upload parquet files into the Railway volume (/data).
    Example filenames:
      fhvhv_tripdata_2025-09.parquet
      fhvhv_tripdata_2025-10.parquet
      fhvhv_tripdata_2025-11.parquet
    """
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        out_path = DATA_DIR / file.filename
        content = await file.read()
        out_path.write_bytes(content)
        return {"saved": str(out_path), "size_mb": round(len(content) / 1024 / 1024, 2)}
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.post("/upload_zones_geojson")
async def upload_zones_geojson(file: UploadFile = File(...)):
    """
    ONE-TIME SETUP (recommended):
    Upload taxi_zones.geojson into /data so Railway doesn't need geopandas/fiona/pyproj.

    IMPORTANT:
    - The file you upload will be saved as /data/taxi_zones.geojson
    """
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        content = await file.read()
        out_path = DATA_DIR / "taxi_zones.geojson"
        out_path.write_bytes(content)
        return {"saved": str(out_path), "size_mb": round(len(content) / 1024 / 1024, 2)}
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


def _generate_impl(bin_minutes: int = 20, min_trips_per_window: int = 10):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Zones must already exist (uploaded once)
    ensure_zones_geojson(DATA_DIR, force=False)

    parquets = sorted(DATA_DIR.glob("fhvhv_tripdata_*.parquet"))
    if not parquets:
        return JSONResponse(
            {"error": "No parquet files found in /data. Upload first via /upload_parquet."},
            status_code=400,
        )

    build_hotspots_json(
        parquet_files=parquets,
        zones_geojson_path=ZONES_GEOJSON,
        out_path=OUT_PATH,
        bin_minutes=bin_minutes,
        min_trips_per_window=min_trips_per_window,
    )

    return {
        "ok": True,
        "output": OUT_PATH.name,
        "size_mb": round(OUT_PATH.stat().st_size / 1024 / 1024, 2),
    }


@app.post("/generate")
def generate(bin_minutes: int = 20, min_trips_per_window: int = 10):
    """
    Builds /data/hotspots_20min.json from parquet(s) in /data.
    """
    try:
        return _generate_impl(bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/generate")
def generate_get(bin_minutes: int = 20, min_trips_per_window: int = 10):
    """
    Same as POST /generate, but GET-friendly for quick testing in a browser.
    """
    try:
        return _generate_impl(bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/hotspots_20min.json")
def get_hotspots():
    if not OUT_PATH.exists():
        return JSONResponse(
            {"error": "hotspots_20min.json not generated yet. Call /generate first."},
            status_code=404,
        )
    return FileResponse(
        str(OUT_PATH),
        media_type="application/json",
        filename="hotspots_20min.json",
    )