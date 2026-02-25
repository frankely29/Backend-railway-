from __future__ import annotations

import os
import json
import time
import threading
import asyncio
import uuid
from pathlib import Path
from typing import Dict, Any, List

from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect
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
# Friends / Presence (in-memory)
# ----------------------------
# NOTE: This is "best effort" realtime presence. If the Railway service restarts,
# the in-memory presence list resets (users will reconnect automatically).
_presence_lock = asyncio.Lock()
_presence_users: Dict[str, Dict[str, Any]] = {}  # client_id -> {username, lat, lng, heading, updated_at_unix}
_presence_sockets: Dict[str, WebSocket] = {}     # client_id -> websocket
PRESENCE_TTL_SEC = int(os.environ.get("PRESENCE_TTL_SEC", str(30 * 60)))  # 30 minutes

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


# ----------------------------
# Presence helpers
# ----------------------------
async def _presence_broadcast() -> None:
    """Broadcast active users to all connected clients."""
    async with _presence_lock:
        users = [
            {
                "client_id": cid,
                "username": u.get("username"),
                "lat": u.get("lat"),
                "lng": u.get("lng"),
                "heading": u.get("heading"),
                "updated_at_unix": u.get("updated_at_unix"),
            }
            for cid, u in _presence_users.items()
        ]
        sockets = list(_presence_sockets.items())

    payload = json.dumps({"type": "users", "users": users})
    dead: List[str] = []
    for cid, ws in sockets:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(cid)

    if dead:
        async with _presence_lock:
            for cid in dead:
                _presence_sockets.pop(cid, None)
                _presence_users.pop(cid, None)


async def _presence_cleanup_loop() -> None:
    """Remove inactive users every 30s."""
    while True:
        await asyncio.sleep(30)
        now = time.time()
        removed = False
        async with _presence_lock:
            stale = [
                cid for cid, u in _presence_users.items()
                if (now - float(u.get("updated_at_unix", 0))) > PRESENCE_TTL_SEC
            ]
            for cid in stale:
                _presence_users.pop(cid, None)
                ws = _presence_sockets.pop(cid, None)
                try:
                    if ws:
                        await ws.close()
                except Exception:
                    pass
                removed = True
        if removed:
            await _presence_broadcast()


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
# Start presence cleanup loop
# ----------------------------
@app.on_event("startup")
async def _start_presence_cleanup():
    asyncio.create_task(_presence_cleanup_loop())


# ----------------------------
# Routes
# ----------------------------
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "NYC TLC Hotspot Backend",
        "endpoints": ["/status", "/generate", "/generate_status", "/timeline", "/frame/{idx}", "/ws"],
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
        "presence_ttl_sec": PRESENCE_TTL_SEC,
    }


# ----------------------------
# WebSocket: realtime friends
# ----------------------------
@app.websocket("/ws")
async def ws_presence(websocket: WebSocket):
    await websocket.accept()
    client_id: str | None = None

    try:
        while True:
            msg = await websocket.receive_text()
            try:
                obj = json.loads(msg)
            except Exception:
                continue

            mtype = obj.get("type")
            if mtype == "hello":
                client_id = str(obj.get("client_id") or "")
                if not client_id:
                    client_id = str(uuid.uuid4())

                username = str(obj.get("username") or "Friend")[:24]

                async with _presence_lock:
                    _presence_sockets[client_id] = websocket
                    _presence_users.setdefault(client_id, {})
                    _presence_users[client_id].update(
                        {
                            "username": username,
                            "lat": _presence_users.get(client_id, {}).get("lat"),
                            "lng": _presence_users.get(client_id, {}).get("lng"),
                            "heading": _presence_users.get(client_id, {}).get("heading"),
                            "updated_at_unix": time.time(),
                        }
                    )

                await _presence_broadcast()

            elif mtype in ("pos", "ping"):
                cid = str(obj.get("client_id") or "")
                if not cid:
                    continue
                client_id = cid

                async with _presence_lock:
                    u = _presence_users.get(cid)
                    if not u:
                        u = {"username": str(obj.get("username") or "Friend")[:24]}
                        _presence_users[cid] = u

                    # update position if provided
                    if "lat" in obj and "lng" in obj:
                        try:
                            u["lat"] = float(obj.get("lat"))
                            u["lng"] = float(obj.get("lng"))
                        except Exception:
                            pass
                    if "heading" in obj:
                        try:
                            u["heading"] = float(obj.get("heading"))
                        except Exception:
                            pass

                    # update username if provided
                    if obj.get("username"):
                        u["username"] = str(obj.get("username"))[:24]

                    u["updated_at_unix"] = time.time()
                    _presence_sockets[cid] = websocket

                await _presence_broadcast()

            elif mtype == "signout":
                cid = str(obj.get("client_id") or "")
                if cid:
                    async with _presence_lock:
                        _presence_users.pop(cid, None)
                        _presence_sockets.pop(cid, None)
                    await _presence_broadcast()
                break

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if client_id:
            async with _presence_lock:
                ws0 = _presence_sockets.get(client_id)
                if ws0 is websocket:
                    _presence_sockets.pop(client_id, None)
                    _presence_users.pop(client_id, None)
            try:
                await _presence_broadcast()
            except Exception:
                pass


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