from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import traceback
import json
import threading
import time

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
JOB_STATUS_PATH = DATA_DIR / "generate_status.json"

_generate_lock = threading.Lock()
_generate_thread = None


def _write_status(payload: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    JOB_STATUS_PATH.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def _read_status() -> dict:
    if not JOB_STATUS_PATH.exists():
        return {"state": "idle"}
    try:
        return json.loads(JOB_STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"state": "unknown"}


def _generator_worker(bin_minutes: int, min_trips_per_window: int):
    started = time.time()
    try:
        _write_status({
            "state": "running",
            "bin_minutes": bin_minutes,
            "min_trips_per_window": min_trips_per_window,
            "started_at_unix": started
        })

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        ensure_zones_geojson(DATA_DIR, force=False)

        parquets = sorted(DATA_DIR.glob("fhvhv_tripdata_*.parquet"))
        if not parquets:
            _write_status({"state": "error", "error": "No parquet files found in /data."})
            return

        # Keep ALL your parquet months
        result = build_hotspots_frames(
            parquet_files=parquets,
            zones_geojson_path=ZONES_GEOJSON,
            out_dir=FRAMES_DIR,
            bin_minutes=bin_minutes,
            min_trips_per_window=min_trips_per_window,
        )

        _write_status({
            "state": "done",
            "result": result,
            "finished_at_unix": time.time(),
            "duration_sec": round(time.time() - started, 2),
            "has_timeline": TIMELINE_PATH.exists()
        })

    except Exception as e:
        _write_status({
            "state": "error",
            "error": str(e),
            "trace": traceback.format_exc()
        })


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
        "generate_status": _read_status(),
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


# ✅ This returns immediately (prevents Railway timeout)
@app.get("/generate")
def generate_async(bin_minutes: int = 20, min_trips_per_window: int = 25):
    global _generate_thread

    if not _generate_lock.acquire(blocking=False):
        return JSONResponse({"ok": False, "state": "running", "message": "Generate already running."}, status_code=202)

    try:
        # if a previous thread exists but died, we just start a new one
        _generate_thread = threading.Thread(
            target=_generator_worker,
            args=(bin_minutes, min_trips_per_window),
            daemon=True
        )
        _generate_thread.start()
        return JSONResponse({"ok": True, "state": "started", "bin_minutes": bin_minutes, "min_trips_per_window": min_trips_per_window}, status_code=202)
    finally:
        # release immediately so status calls aren't blocked
        _generate_lock.release()


@app.get("/generate_status")
def generate_status():
    return _read_status()


@app.get("/timeline")
def timeline():
    if not TIMELINE_PATH.exists():
        return JSONResponse({"error": "timeline not ready. Call /generate first."}, status_code=404)
    return FileResponse(str(TIMELINE_PATH), media_type="application/json", filename="timeline.json")


@app.get("/frame/{idx}")
def frame(idx: int):
    frame_path = FRAMES_DIR / f"frame_{idx:06d}.json"
    if not frame_path.exists():
        return JSONResponse({"error": "frame not found", "idx": idx}, status_code=404)
    return FileResponse(str(frame_path), media_type="application/json", filename=frame_path.name)