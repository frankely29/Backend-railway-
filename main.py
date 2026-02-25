from __future__ import annotations

import os
import json
import time
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from build_hotspot import ensure_zones_geojson, build_hotspots_frames

# ----------------------------
# Paths (Railway volume)
# ----------------------------
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
FRAMES_DIR = Path(os.environ.get("FRAMES_DIR", str(DATA_DIR / "frames")))
TIMELINE_PATH = FRAMES_DIR / "timeline.json"

# Defaults for auto-generate (Option A)
DEFAULT_BIN_MINUTES = int(os.environ.get("DEFAULT_BIN_MINUTES", "20"))
DEFAULT_MIN_TRIPS_PER_WINDOW = int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25"))

# A simple file lock to prevent multi-start concurrency (best-effort)
LOCK_PATH = DATA_DIR / ".generate.lock"

# ----------------------------
# Presence / Friends (in-memory)
# NOTE: This is in-memory per Railway instance.
# If you run multiple instances, you need Redis/DB.
# ----------------------------
PRESENCE_TIMEOUT_MS = int(os.environ.get("PRESENCE_TIMEOUT_MS", str(30 * 60 * 1000)))  # 30 minutes
_presence_lock = threading.Lock()
_presence_store: Dict[str, Dict[str, Any]] = {}  # username -> presence dict

class PresencePayload(BaseModel):
    username: str
    session_token: str
    lat: Optional[float] = None
    lng: Optional[float] = None
    heading: Optional[float] = None
    ts: Optional[int] = None  # ms since epoch


def _now_ms() -> int:
    return int(time.time() * 1000)


def _presence_cleanup_locked(now_ms: int) -> None:
    # caller holds _presence_lock
    dead = []
    for u, d in _presence_store.items():
        if now_ms - int(d.get("updated_at_ms", 0)) > PRESENCE_TIMEOUT_MS:
            dead.append(u)
    for u in dead:
        _presence_store.pop(u, None)


def _presence_snapshot() -> Dict[str, Any]:
    now = _now_ms()
    with _presence_lock:
        _presence_cleanup_locked(now)

        online = []
        users = []
        for u, d in _presence_store.items():
            online.append(u)
            lat = d.get("lat", None)
            lng = d.get("lng", None)
            if lat is not None and lng is not None:
                users.append(
                    {
                        "username": u,
                        "lat": lat,
                        "lng": lng,
                        "heading": d.get("heading", None),
                        "updated_at_ms": d.get("updated_at_ms", None),
                    }
                )

        # stable sort for nicer UI
        online.sort(key=lambda x: x.lower())
        users.sort(key=lambda x: x["username"].lower())

        return {"online": online, "users": users, "timeout_ms": PRESENCE_TIMEOUT_MS}


# In-memory job state
_state_lock = threading.Lock()
_generate_state: Dict[str, Any] = {
    "state": "idle",  # idle | started | running | done | error
    "bin_minutes": None,
    "min_trips_per_window": None,
    "started_at_unix": None,
    "finished_at_unix": None,
    "duration_sec": None,
    "result": None,
    "error": None,
    "trace": None,
}

# ----------------------------
# App
# ----------------------------
app = FastAPI(title="NYC TLC Hotspot Backend", version="1.0")

# Allow GitHub Pages frontend to call Railway backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock down later if you want
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Utilities
# ----------------------------
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

        # ensure zones exist
        zones_path = ensure_zones_geojson(DATA_DIR, force=False)

        # ensure at least one parquet exists
        parquets = _list_parquets()
        if not parquets:
            raise RuntimeError("No .parquet files found in /data. Upload via POST /upload_parquet.")

        # build frames
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
    # Avoid double runs
    st = _get_state()
    if st["state"] in ("started", "running"):
        return {
            "ok": True,
            "state": st["state"],
            "bin_minutes": st["bin_minutes"],
            "min_trips_per_window": st["min_trips_per_window"],
        }

    # File lock guard (best-effort across restarts)
    if _lock_is_present():
        _set_state(state="running", bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)
        return {
            "ok": True,
            "state": "running",
            "bin_minutes": bin_minutes,
            "min_trips_per_window": min_trips_per_window,
        }

    _write_lock()
    _set_state(state="started", bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)

    t = threading.Thread(target=_generate_worker, args=(bin_minutes, min_trips_per_window), daemon=True)
    t.start()

    return {
        "ok": True,
        "state": "started",
        "bin_minutes": bin_minutes,
        "min_trips_per_window": min_trips_per_window,
    }


# ----------------------------
# Option A: Auto-generate ONCE if missing
# ----------------------------
@app.on_event("startup")
def auto_generate_if_missing():
    """
    - If /data/frames/timeline.json exists -> do nothing
    - Else -> start generation in background using defaults (if inputs exist)
    """
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        FRAMES_DIR.mkdir(parents=True, exist_ok=True)

        if _has_frames():
            # Fill state with defaults for nicer status output
            try:
                tl = _read_json(TIMELINE_PATH)
                _set_state(
                    state="done",
                    bin_minutes=DEFAULT_BIN_MINUTES,
                    min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
                    result={"ok": True, "count": tl.get("count")},
                )
            except Exception:
                _set_state(
                    state="done",
                    bin_minutes=DEFAULT_BIN_MINUTES,
                    min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
                    result={"ok": True},
                )
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
# Routes
# ----------------------------
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "NYC TLC Hotspot Backend",
        "endpoints": [
            "/status",
            "/generate",
            "/generate_status",
            "/timeline",
            "/frame/{idx}",
            "/presence/list",
            "/presence/signin",
            "/presence/update",
            "/presence/signout",
        ],
    }


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
        "presence": _presence_snapshot(),
    }


@app.get("/generate")
def generate_get(bin_minutes: int = DEFAULT_BIN_MINUTES, min_trips_per_window: int = DEFAULT_MIN_TRIPS_PER_WINDOW):
    # Starts generation async
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
    """
    Upload the TLC taxi zones geojson file.
    Must be valid GeoJSON content.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = DATA_DIR / "taxi_zones.geojson"

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty upload.")

    # Basic validation: should parse as JSON
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
    """
    Upload a parquet month file into /data.
    """
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
# Friends / Presence endpoints
# ----------------------------
@app.get("/presence/list")
def presence_list():
    """
    Returns:
      - online: list of usernames currently online (even if no GPS yet)
      - users: list of users that have lat/lng so frontend can draw markers
    """
    return _presence_snapshot()


@app.post("/presence/signin")
def presence_signin(p: PresencePayload):
    """
    Sign in (creates/refreshes a session).
    We accept session_token from the frontend; backend stores it and requires it for update/signout.
    """
    now = _now_ms()
    with _presence_lock:
        _presence_cleanup_locked(now)

        # If user exists with different token, overwrite (latest wins)
        _presence_store[p.username] = {
            "username": p.username,
            "session_token": p.session_token,
            "lat": None,
            "lng": None,
            "heading": None,
            "updated_at_ms": now,
        }
    return {"ok": True}


@app.post("/presence/update")
def presence_update(p: PresencePayload):
    """
    Update location/heading. Requires matching session_token.
    """
    now = p.ts if isinstance(p.ts, int) and p.ts > 0 else _now_ms()

    with _presence_lock:
        _presence_cleanup_locked(now)

        d = _presence_store.get(p.username)
        if not d:
            return {"ok": False, "error": "not_signed_in"}

        if d.get("session_token") != p.session_token:
            return {"ok": False, "error": "bad_session"}

        # Update GPS fields (allow None, but keep last known if None)
        if p.lat is not None and p.lng is not None:
            d["lat"] = float(p.lat)
            d["lng"] = float(p.lng)

        if p.heading is not None:
            try:
                d["heading"] = float(p.heading)
            except Exception:
                pass

        d["updated_at_ms"] = now

    return {"ok": True}


@app.post("/presence/signout")
def presence_signout(p: PresencePayload):
    """
    Sign out. Requires matching session_token.
    """
    with _presence_lock:
        d = _presence_store.get(p.username)
        if not d:
            return {"ok": True}

        if d.get("session_token") != p.session_token:
            # don't allow random signouts
            return {"ok": False, "error": "bad_session"}

        _presence_store.pop(p.username, None)

    return {"ok": True}