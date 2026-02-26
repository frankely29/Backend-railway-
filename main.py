from __future__ import annotations

import os
import json
import time
import threading
from pathlib import Path
from typing import Dict, Any, List

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from build_hotspot import ensure_zones_geojson, build_hotspots_frames

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
FRAMES_DIR = Path(os.environ.get("FRAMES_DIR", str(DATA_DIR / "frames")))
TIMELINE_PATH = FRAMES_DIR / "timeline.json"
DEBUG_META_PATH = FRAMES_DIR / "debug_meta.json"

DEFAULT_BIN_MINUTES = int(os.environ.get("DEFAULT_BIN_MINUTES", "20"))
DEFAULT_MIN_TRIPS_PER_WINDOW = int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25"))

LOCK_PATH = DATA_DIR / ".generate.lock"

_state_lock = threading.Lock()
_generate_state: Dict[str, Any] = {
    "state": "idle",
    "bin_minutes": None,
    "min_trips_per_window": None,
    "started_at_unix": None,
    "finished_at_unix": None,
    "duration_sec": None,
    "result": None,
    "error": None,
    "trace": None,
}

app = FastAPI(title="NYC TLC Hotspot Backend", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _list_parquets() -> List[Path]:
    if not DATA_DIR.exists():
        return []
    return sorted([p for p in DATA_DIR.glob("*.parquet") if p.is_file()])


def _has_frames() -> bool:
    try:
        return TIMELINE_PATH.exists() and TIMELINE_PATH.stat().st_size > 0
    except Exception:
        return False


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_lock() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOCK_PATH.write_text(str(int(time.time())), encoding="utf-8")


def _clear_lock() -> None:
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink()
    except Exception:
        pass


def _lock_is_present() -> bool:
    return LOCK_PATH.exists()


def _set_state(**kwargs):
    with _state_lock:
        _generate_state.update(kwargs)


def _get_state() -> Dict[str, Any]:
    with _state_lock:
        return dict(_generate_state)


def _generate_worker(bin_minutes: int, min_trips_per_window: int) -> None:
    start = time.time()
    _set_state(
        state="running",
        bin_minutes=bin_minutes,
        min_trips_per_window=min_trips_per_window,
        started_at_unix=start,
        finished_at_unix=None,
        duration_sec=None,
        result=None,
        error=None,
        trace=None,
    )

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        FRAMES_DIR.mkdir(parents=True, exist_ok=True)

        zones_path = ensure_zones_geojson(DATA_DIR, force=False)

        parquets = _list_parquets()
        if not parquets:
            raise RuntimeError("No .parquet files found in /data. Upload via POST /upload_parquet.")

        result = build_hotspots_frames(
            parquet_files=parquets,
            zones_geojson_path=zones_path,
            out_dir=FRAMES_DIR,
            bin_minutes=bin_minutes,
            min_trips_per_window=min_trips_per_window,
        )

        end = time.time()
        _set_state(
            state="done",
            finished_at_unix=end,
            duration_sec=round(end - start, 2),
            result=result,
        )

    except Exception as e:
        end = time.time()
        import traceback
        _set_state(
            state="error",
            finished_at_unix=end,
            duration_sec=round(end - start, 2),
            error=str(e),
            trace=traceback.format_exc(),
        )
    finally:
        _clear_lock()


def start_generate(bin_minutes: int, min_trips_per_window: int) -> Dict[str, Any]:
    st = _get_state()
    if st["state"] in ("started", "running"):
        return {
            "ok": True,
            "state": st["state"],
            "bin_minutes": st["bin_minutes"],
            "min_trips_per_window": st["min_trips_per_window"],
        }

    if _lock_is_present():
        _set_state(state="running", bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)
        return {"ok": True, "state": "running", "bin_minutes": bin_minutes, "min_trips_per_window": min_trips_per_window}

    _write_lock()
    _set_state(state="started", bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)

    t = threading.Thread(target=_generate_worker, args=(bin_minutes, min_trips_per_window), daemon=True)
    t.start()

    return {"ok": True, "state": "started", "bin_minutes": bin_minutes, "min_trips_per_window": min_trips_per_window}


@app.on_event("startup")
def auto_generate_if_missing():
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        FRAMES_DIR.mkdir(parents=True, exist_ok=True)

        if _has_frames():
            _set_state(state="done", bin_minutes=DEFAULT_BIN_MINUTES, min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW, result={"ok": True})
            return

        zones_ok = (DATA_DIR / "taxi_zones.geojson").exists()
        parquets_ok = len(_list_parquets()) > 0

        if zones_ok and parquets_ok:
            start_generate(DEFAULT_BIN_MINUTES, DEFAULT_MIN_TRIPS_PER_WINDOW)
        else:
            _set_state(state="idle")

    except Exception:
        _set_state(state="idle")


# ----------------------------
# Public routes (existing)
# ----------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "NYC TLC Hotspot Backend", "endpoints": ["/status", "/generate", "/generate_status", "/timeline", "/frame/{idx}", "/debug/health", "/debug/schema", "/debug/frames", "/debug/frame/{idx}"]}


@app.get("/status")
def status():
    parquets = [p.name for p in _list_parquets()]
    zones_path = DATA_DIR / "taxi_zones.geojson"
    return {
        "status": "ok",
        "data_dir": str(DATA_DIR),
        "parquets": parquets,
        "zones_geojson": zones_path.name if zones_path.exists() else None,
        "zones_present": zones_path.exists(),
        "frames_dir": str(FRAMES_DIR),
        "has_timeline": _has_frames(),
        "generate_state": _get_state(),
    }


@app.get("/generate")
def generate_get(bin_minutes: int = DEFAULT_BIN_MINUTES, min_trips_per_window: int = DEFAULT_MIN_TRIPS_PER_WINDOW):
    return start_generate(bin_minutes, min_trips_per_window)


@app.get("/generate_status")
def generate_status():
    return _get_state()


@app.get("/timeline")
def timeline():
    if not _has_frames():
        raise HTTPException(status_code=409, detail="timeline not ready. Call /generate first.")
    return _read_json(TIMELINE_PATH)


@app.get("/frame/{idx}")
def frame(idx: int):
    if not _has_frames():
        raise HTTPException(status_code=409, detail="timeline not ready. Call /generate first.")
    p = FRAMES_DIR / f"frame_{idx:06d}.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"frame not found: {idx}")
    return _read_json(p)


@app.post("/upload_zones_geojson")
async def upload_zones_geojson(file: UploadFile = File(...)):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = DATA_DIR / "taxi_zones.geojson"

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty upload.")

    try:
        obj = json.loads(content.decode("utf-8", errors="strict"))
        if obj.get("type") not in ("FeatureCollection", "Feature"):
            raise ValueError("Not a GeoJSON FeatureCollection/Feature")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid GeoJSON: {e}")

    target.write_bytes(content)
    return {"saved": str(target), "size_mb": round(target.stat().st_size / (1024 * 1024), 2)}


@app.post("/upload_parquet")
async def upload_parquet(file: UploadFile = File(...)):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    filename = (file.filename or "upload.parquet").replace("\\", "/").split("/")[-1]
    if not filename.lower().endswith(".parquet"):
        raise HTTPException(status_code=400, detail="File must be .parquet")

    target = DATA_DIR / filename
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty upload.")

    target.write_bytes(content)
    return {"saved": str(target), "size_mb": round(target.stat().st_size / (1024 * 1024), 2)}


# ----------------------------
# DEBUG routes (NEW)
# ----------------------------
@app.get("/debug/health")
def debug_health():
    return {
        "ok": True,
        "has_timeline": _has_frames(),
        "parquet_files": [p.name for p in _list_parquets()],
        "zones_present": (DATA_DIR / "taxi_zones.geojson").exists(),
        "state": _get_state().get("state"),
    }


@app.get("/debug/schema")
def debug_schema():
    """
    Shows parquet columns Railway is actually reading.
    """
    import duckdb

    parqs = _list_parquets()
    if not parqs:
        raise HTTPException(status_code=404, detail="No parquet files found in /data")

    con = duckdb.connect(database=":memory:")
    parquet_sql = ", ".join("'" + str(p).replace("'", "''") + "'" for p in parqs)
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{parquet_sql}])").fetchall()
    cols = [str(r[0]) for r in desc]

    lower = {c.lower() for c in cols}
    return {
        "parquets": [p.name for p in parqs],
        "column_count": len(cols),
        "columns": cols,
        "has_driver_pay": "driver_pay" in lower,
        "has_pickup_datetime": "pickup_datetime" in lower,
        "has_pulocationid": "pulocationid" in lower,
        "has_trip_time": ("trip_time" in lower),
        "has_trip_miles": ("trip_miles" in lower),
    }


@app.get("/debug/frames")
def debug_frames():
    """
    Shows build metadata (weights, detected flags, skips).
    """
    if not DEBUG_META_PATH.exists():
        raise HTTPException(status_code=404, detail="debug_meta.json not found. Run /generate first.")
    return _read_json(DEBUG_META_PATH)


@app.get("/debug/frame/{idx}")
def debug_frame(idx: int):
    """
    Validates a single frame for missing fields.
    """
    p = FRAMES_DIR / f"frame_{idx:06d}.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"frame not found: {idx}")

    frame = _read_json(p)
    feats = (frame.get("polygons") or {}).get("features") or []

    missing_geom = 0
    missing_rating = 0
    missing_bucket = 0
    missing_pickups = 0
    missing_pay = 0

    for f in feats:
        if not f.get("geometry"):
            missing_geom += 1
        props = f.get("properties") or {}
        if props.get("rating") is None:
            missing_rating += 1
        if not props.get("bucket"):
            missing_bucket += 1
        if props.get("pickups") is None:
            missing_pickups += 1
        if props.get("avg_driver_pay") is None:
            missing_pay += 1

    return {
        "idx": idx,
        "time": frame.get("time"),
        "feature_count": len(feats),
        "missing_geometry": missing_geom,
        "missing_rating": missing_rating,
        "missing_bucket": missing_bucket,
        "missing_pickups": missing_pickups,
        "missing_avg_driver_pay": missing_pay,
        "note": "If trip_time/trip_miles are missing in schema, those extra fields will not exist by design (fallback mode)."
    }