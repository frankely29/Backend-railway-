from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import traceback
import json

from build_hotspot import ensure_zones_geojson, build_hotspots_frames

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = Path("/data")
ZONES_GEOJSON = DATA_DIR / "taxi_zones.geojson"
FRAMES_DIR = DATA_DIR / "frames"
TIMELINE_PATH = FRAMES_DIR / "timeline.json"


@app.get("/status")
def status():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    parquets = sorted([p.name for p in DATA_DIR.glob("fhvhv_tripdata_*.parquet")])
    has_zones = ZONES_GEOJSON.exists() and ZONES_GEOJSON.stat().st_size > 0
    has_timeline = TIMELINE_PATH.exists() and TIMELINE_PATH.stat().st_size > 0
    return {
        "status": "ok",
        "data_dir": str(DATA_DIR),
        "parquets": parquets,
        "zones_geojson": ZONES_GEOJSON.name,
        "zones_present": has_zones,
        "frames_dir": str(FRAMES_DIR),
        "has_timeline": has_timeline,
    }


@app.post("/upload_parquet")
async def upload_parquet(file: UploadFile = File(...)):
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
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        content = await file.read()
        out_path = DATA_DIR / "taxi_zones.geojson"
        out_path.write_bytes(content)
        return {"saved": str(out_path), "size_mb": round(len(content) / 1024 / 1024, 2)}
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/generate")
def generate_get(bin_minutes: int = 20, min_trips_per_window: int = 10):
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        ensure_zones_geojson(DATA_DIR, force=False)

        parquets = sorted(DATA_DIR.glob("fhvhv_tripdata_*.parquet"))
        if not parquets:
            return JSONResponse(
                {"error": "No parquet files found in /data. Upload first via /upload_parquet."},
                status_code=400,
            )

        # Keeps ALL your data; DuckDB will spill temp to /data/duckdb_tmp
        result = build_hotspots_frames(
            parquet_files=parquets,
            zones_geojson_path=ZONES_GEOJSON,
            out_dir=FRAMES_DIR,
            bin_minutes=bin_minutes,
            min_trips_per_window=min_trips_per_window,
        )
        return result
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/timeline")
def timeline():
    try:
        if not TIMELINE_PATH.exists():
            return JSONResponse({"error": "timeline not ready. Call /generate first."}, status_code=404)
        return FileResponse(str(TIMELINE_PATH), media_type="application/json", filename="timeline.json")
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/frame/{idx}")
def frame(idx: int):
    try:
        frame_path = FRAMES_DIR / f"frame_{idx:06d}.json"
        if not frame_path.exists():
            return JSONResponse({"error": "frame not found", "idx": idx}, status_code=404)
        return FileResponse(str(frame_path), media_type="application/json", filename=frame_path.name)
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)