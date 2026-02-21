from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import traceback
import json
from typing import Any, Dict, Optional

from build_hotspot import ensure_zones_geojson, build_hotspots_json

app = FastAPI()

# CORS so GitHub Pages + iPhone Safari can call Railway
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = Path("/data")
ZONES_GEOJSON = DATA_DIR / "taxi_zones.geojson"
OUT_PATH = DATA_DIR / "hotspots_20min.json"

_cache_mtime: Optional[float] = None
_cache_payload: Optional[Dict[str, Any]] = None


def _load_hotspots_cached() -> Dict[str, Any]:
    global _cache_mtime, _cache_payload
    if not OUT_PATH.exists():
        raise FileNotFoundError("hotspots_20min.json not generated yet")
    mtime = OUT_PATH.stat().st_mtime
    if _cache_payload is None or _cache_mtime != mtime:
        _cache_payload = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        _cache_mtime = mtime
    return _cache_payload


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


def _generate_impl(bin_minutes: int = 20, min_trips_per_window: int = 10):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # zones must exist in volume
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

    return {"ok": True, "output": OUT_PATH.name, "size_mb": round(OUT_PATH.stat().st_size / 1024 / 1024, 2)}


@app.post("/generate")
def generate(bin_minutes: int = 20, min_trips_per_window: int = 10):
    try:
        return _generate_impl(bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/generate")
def generate_get(bin_minutes: int = 20, min_trips_per_window: int = 10):
    try:
        return _generate_impl(bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


# ✅ Railway-only endpoints used by the frontend
@app.get("/timeline")
def timeline():
    try:
        payload = _load_hotspots_cached()
        tl = payload.get("timeline") or []
        return {"timeline": tl, "count": len(tl)}
    except FileNotFoundError:
        return JSONResponse({"error": "timeline not ready. Call /generate first."}, status_code=404)
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/frame/{idx}")
def frame(idx: int):
    try:
        payload = _load_hotspots_cached()
        frames = payload.get("frames") or []
        if idx < 0 or idx >= len(frames):
            return JSONResponse({"error": "idx out of range", "idx": idx, "count": len(frames)}, status_code=400)
        return frames[idx]
    except FileNotFoundError:
        return JSONResponse({"error": "timeline not ready. Call /generate first."}, status_code=404)
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


# optional compatibility endpoint
@app.get("/hotspots_20min.json")
def get_hotspots():
    if not OUT_PATH.exists():
        return JSONResponse({"error": "hotspots_20min.json not generated yet. Call /generate first."}, status_code=404)
    return FileResponse(str(OUT_PATH), media_type="application/json", filename="hotspots_20min.json")