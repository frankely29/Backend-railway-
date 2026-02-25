from __future__ import annotations

import os
import json
import time
import threading
from pathlib import Path
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

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

# Presence settings
PRESENCE_IDLE_SEC = int(os.environ.get("PRESENCE_IDLE_SEC", "1800"))  # 30 min default

# A simple file lock to prevent multi-start concurrency (best-effort)
LOCK_PATH = DATA_DIR / ".generate.lock"

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
# Presence state (in-memory)
# ----------------------------
_presence_lock = threading.Lock()
# users[name] = {"name": str, "lat": float, "lng": float, "heading": float|None, "moving": bool|None, "last_seen": float}
_users: Dict[str, Dict[str, Any]] = {}
_sockets: Dict[str, WebSocket] = {}  # name -> websocket


# ----------------------------
# App
# ----------------------------
app = FastAPI(title="NYC TLC Hotspot Backend", version="1.0")

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
        return {"ok": True, "state": st["state"], "bin_minutes": st["bin_minutes"], "min_trips_per_window": st["min_trips_per_window"]}

    if _lock_is_present():
        _set_state(state="running", bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)
        return {"ok": True, "state": "running", "bin_minutes": bin_minutes, "min_trips_per_window": min_trips_per_window}

    _write_lock()
    _set_state(state="started", bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)

    t = threading.Thread(target=_generate_worker, args=(bin_minutes, min_trips_per_window), daemon=True)
    t.start()

    return {"ok": True, "state": "started", "bin_minutes": bin_minutes, "min_trips_per_window": min_trips_per_window}

# ----------------------------
# Presence helpers
# ----------------------------
def _now() -> float:
    return time.time()

def _cleanup_inactive_locked(now: float) -> List[str]:
    """Call with _presence_lock held."""
    removed = []
    for name, info in list(_users.items()):
        last_seen = float(info.get("last_seen") or 0.0)
        if now - last_seen > PRESENCE_IDLE_SEC:
            removed.append(name)
            _users.pop(name, None)
            ws = _sockets.pop(name, None)
            # Don't await close here; just drop it; disconnect handler will clean further
    return removed

def _snapshot_users_locked() -> List[Dict[str, Any]]:
    """Call with _presence_lock held."""
    out = []
    for name, info in _users.items():
        out.append({
            "name": name,
            "lat": info.get("lat"),
            "lng": info.get("lng"),
            "heading": info.get("heading"),
            "moving": info.get("moving"),
            "last_seen": info.get("last_seen"),
        })
    return out

async def _broadcast_users():
    # Create payload once
    with _presence_lock:
        now = _now()
        _cleanup_inactive_locked(now)
        users_payload = _snapshot_users_locked()
        sockets = list(_sockets.values())

    msg = {"type": "users", "users": users_payload, "server_time": now}
    text = json.dumps(msg, separators=(",", ":"))

    # Best-effort send
    for ws in sockets:
        try:
            await ws.send_text(text)
        except Exception:
            # ignore send failures; disconnect handler will remove later
            pass

# Background cleanup loop
def _presence_janitor():
    while True:
        time.sleep(15)
        try:
            # We can’t await broadcast from a normal thread easily.
            # Instead: just cleanup stale entries here; broadcast on next client message.
            with _presence_lock:
                _cleanup_inactive_locked(_now())
        except Exception:
            pass

@app.on_event("startup")
def on_startup():
    # Option A auto-generate if missing
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        FRAMES_DIR.mkdir(parents=True, exist_ok=True)

        if _has_frames():
            _set_state(state="done", result={"ok": True, "count": _read_json(TIMELINE_PATH).get("count")})
        else:
            zones_ok = (DATA_DIR / "taxi_zones.geojson").exists()
            parquets_ok = len(_list_parquets()) > 0
            if zones_ok and parquets_ok:
                start_generate(DEFAULT_BIN_MINUTES, DEFAULT_MIN_TRIPS_PER_WINDOW)
            else:
                _set_state(state="idle")
    except Exception:
        _set_state(state="idle")

    # Start presence janitor thread
    t = threading.Thread(target=_presence_janitor, daemon=True)
    t.start()

# ----------------------------
# API Endpoints
# ----------------------------
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
        "presence_idle_sec": PRESENCE_IDLE_SEC,
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
# WebSocket: realtime user presence
# ----------------------------
@app.websocket("/ws")
async def ws_presence(websocket: WebSocket, name: str = Query(default="")):
    name = (name or "").strip()

    # Accept first so we can send errors cleanly
    await websocket.accept()

    # Validate username
    if not name or len(name) < 2 or len(name) > 20:
        await websocket.send_text(json.dumps({"type": "error", "error": "Username must be 2–20 chars."}))
        await websocket.close(code=1008)
        return

    # Enforce unique usernames
    with _presence_lock:
        # Cleanup inactive first
        _cleanup_inactive_locked(_now())

        if name in _sockets:
            await websocket.send_text(json.dumps({"type": "error", "error": "Username already taken. Pick another."}))
            await websocket.close(code=1008)
            return

        _sockets[name] = websocket
        # Create user entry (no location yet)
        _users[name] = {"name": name, "lat": None, "lng": None, "heading": None, "moving": None, "last_seen": _now()}

    # Broadcast join
    await _broadcast_users()

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            mtype = (msg.get("type") or "").strip()

            if mtype == "ping":
                with _presence_lock:
                    if name in _users:
                        _users[name]["last_seen"] = _now()
                # optional: send pong
                await websocket.send_text(json.dumps({"type": "pong", "t": _now()}, separators=(",", ":")))
                continue

            if mtype == "signout":
                # remove immediately
                with _presence_lock:
                    _users.pop(name, None)
                    _sockets.pop(name, None)
                await _broadcast_users()
                await websocket.close(code=1000)
                return

            if mtype == "loc":
                lat = msg.get("lat")
                lng = msg.get("lng")
                heading = msg.get("heading")
                moving = msg.get("moving")

                # Basic validation
                try:
                    lat_f = float(lat)
                    lng_f = float(lng)
                except Exception:
                    continue

                if not (-90 <= lat_f <= 90 and -180 <= lng_f <= 180):
                    continue

                head_f = None
                if heading is not None:
                    try:
                        head_f = float(heading)
                    except Exception:
                        head_f = None

                mov_b = None
                if moving is not None:
                    mov_b = bool(moving)

                with _presence_lock:
                    if name in _users:
                        _users[name].update({
                            "lat": lat_f,
                            "lng": lng_f,
                            "heading": head_f,
                            "moving": mov_b,
                            "last_seen": _now(),
                        })

                await _broadcast_users()
                continue

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        with _presence_lock:
            _users.pop(name, None)
            _sockets.pop(name, None)
        try:
            await _broadcast_users()
        except Exception:
            pass