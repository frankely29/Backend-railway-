import os
import json
import time
import asyncio
import threading
from pathlib import Path
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, HTTPException, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Your generator (already in your repo)
from build_hotspot import generate_hotspot_frames, ensure_zones_geojson


# ----------------------------
# Paths / Config
# ----------------------------
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data")).resolve()
FRAMES_DIR = DATA_DIR / "frames"
TIMELINE_PATH = FRAMES_DIR / "timeline.json"

DEFAULT_BIN_MINUTES = int(os.environ.get("BIN_MINUTES", "20"))
DEFAULT_MIN_TRIPS_PER_WINDOW = int(os.environ.get("MIN_TRIPS_PER_WINDOW", "15"))

# Friends feature config
INACTIVITY_SECONDS = 30 * 60  # 30 minutes


# ----------------------------
# App
# ----------------------------
app = FastAPI(title="NYC TLC Hotspot Backend")

# CORS (GitHub Pages -> Railway)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # simple for now; you can restrict later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Helpers (JSON + state)
# ----------------------------
_STATE_PATH = DATA_DIR / "generate_state.json"
_state_lock = threading.Lock()


def _set_state(**kwargs):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with _state_lock:
        cur = {}
        if _STATE_PATH.exists():
            try:
                cur = json.loads(_STATE_PATH.read_text("utf-8"))
            except Exception:
                cur = {}
        cur.update(kwargs)
        cur["ts"] = int(time.time())
        _STATE_PATH.write_text(json.dumps(cur, indent=2), encoding="utf-8")


def _get_state():
    if not _STATE_PATH.exists():
        return {"state": "idle"}
    try:
        return json.loads(_STATE_PATH.read_text("utf-8"))
    except Exception:
        return {"state": "idle"}


def _read_json(path: Path):
    return json.loads(path.read_text("utf-8"))


def _has_frames():
    return TIMELINE_PATH.exists()


def _list_parquets() -> List[Path]:
    if not DATA_DIR.exists():
        return []
    return sorted([p for p in DATA_DIR.glob("*.parquet") if p.is_file()])


# ----------------------------
# Generator runner
# ----------------------------
_gen_thread = None
_gen_lock = threading.Lock()


def _generate_job(bin_minutes: int, min_trips_per_window: int):
    try:
        _set_state(state="running", bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window, error=None)

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        FRAMES_DIR.mkdir(parents=True, exist_ok=True)

        # Ensure zones exist (generator expects taxi_zones.geojson)
        ensure_zones_geojson(DATA_DIR)

        # Generate frames/timeline
        generate_hotspot_frames(
            data_dir=DATA_DIR,
            out_dir=FRAMES_DIR,
            bin_minutes=bin_minutes,
            min_trips_per_window=min_trips_per_window,
        )

        _set_state(state="done")
    except Exception as e:
        _set_state(state="error", error=str(e))


def start_generate(bin_minutes: int, min_trips_per_window: int):
    global _gen_thread
    with _gen_lock:
        st = _get_state().get("state", "idle")
        if st == "running":
            return {"ok": True, "state": "running"}

        _gen_thread = threading.Thread(
            target=_generate_job,
            args=(bin_minutes, min_trips_per_window),
            daemon=True,
        )
        _gen_thread.start()
        return {"ok": True, "state": "running"}


@app.on_event("startup")
async def on_startup():
    """
    Auto-generate frames on boot if possible.
    This prevents the frontend from dying with 'timeline not ready'.
    """
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        FRAMES_DIR.mkdir(parents=True, exist_ok=True)

        # If already generated, do nothing.
        if _has_frames():
            _set_state(state="done")
            return

        # If parquet files exist, start generation automatically.
        if len(_list_parquets()) > 0:
            start_generate(DEFAULT_BIN_MINUTES, DEFAULT_MIN_TRIPS_PER_WINDOW)
        else:
            _set_state(state="idle")
    except Exception:
        _set_state(state="idle")


# ----------------------------
# FRIENDS / PRESENCE (WebSocket)
# ----------------------------
class ConnectionManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.active: Dict[str, WebSocket] = {}   # username -> websocket
        self.users: Dict[str, Dict[str, Any]] = {}  # username -> {lat,lng,heading,last_seen}

    async def connect(self, ws: WebSocket):
        await ws.accept()

    async def disconnect_username(self, username: str):
        async with self._lock:
            self.active.pop(username, None)
            self.users.pop(username, None)

    async def upsert_user(self, username: str, payload: Dict[str, Any]):
        now = time.time()
        lat = payload.get("lat")
        lng = payload.get("lng")
        heading = payload.get("heading")
        # keep it simple: store what we have
        async with self._lock:
            if username not in self.users:
                self.users[username] = {}
            if lat is not None and lng is not None:
                self.users[username]["lat"] = float(lat)
                self.users[username]["lng"] = float(lng)
            if heading is not None:
                try:
                    self.users[username]["heading"] = float(heading)
                except Exception:
                    pass
            self.users[username]["last_seen"] = now

    async def set_socket(self, username: str, ws: WebSocket):
        async with self._lock:
            self.active[username] = ws
            if username not in self.users:
                self.users[username] = {"last_seen": time.time()}

    async def broadcast_roster(self):
        async with self._lock:
            roster = []
            for u, info in self.users.items():
                # Only broadcast users with coordinates
                if "lat" in info and "lng" in info:
                    roster.append({
                        "username": u,
                        "lat": info.get("lat"),
                        "lng": info.get("lng"),
                        "heading": info.get("heading", 0),
                        "last_seen": info.get("last_seen", 0),
                    })
            sockets = list(self.active.items())

        msg = {"type": "roster", "users": roster}
        dead = []
        for username, ws in sockets:
            try:
                await ws.send_text(json.dumps(msg))
            except Exception:
                dead.append(username)

        # cleanup dead sockets
        if dead:
            async with self._lock:
                for u in dead:
                    self.active.pop(u, None)
                    self.users.pop(u, None)

    async def cleanup_inactive(self):
        now = time.time()
        removed = []
        async with self._lock:
            for u, info in list(self.users.items()):
                if now - float(info.get("last_seen", 0)) > INACTIVITY_SECONDS:
                    removed.append(u)
                    self.users.pop(u, None)
                    self.active.pop(u, None)
        if removed:
            await self.broadcast_roster()


manager = ConnectionManager()


@app.on_event("startup")
async def start_presence_cleanup_loop():
    async def _loop():
        while True:
            try:
                await manager.cleanup_inactive()
            except Exception:
                pass
            await asyncio.sleep(30)

    asyncio.create_task(_loop())


@app.websocket("/ws")
async def ws_presence(ws: WebSocket):
    """
    Client flow:
    - Connect ws://.../ws
    - Send {"type":"hello","username":"Frankelly","lat":..,"lng":..,"heading":..}
    - Then keep sending {"type":"pos","lat":..,"lng":..,"heading":..} every ~5-10s (or on GPS updates)
    - Server broadcasts {"type":"roster","users":[...]} to everyone
    """
    await manager.connect(ws)

    username = None
    try:
        # First message must be hello with username
        raw = await ws.receive_text()
        msg = json.loads(raw)

        if msg.get("type") != "hello":
            await ws.send_text(json.dumps({"type": "error", "error": "first message must be hello"}))
            return

        username = (msg.get("username") or "").strip()
        if not username:
            await ws.send_text(json.dumps({"type": "error", "error": "username required"}))
            return

        # Reserve username socket
        await manager.set_socket(username, ws)
        await manager.upsert_user(username, msg)
        await manager.broadcast_roster()

        # Main loop
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            t = msg.get("type")

            if t == "pos":
                await manager.upsert_user(username, msg)
                await manager.broadcast_roster()
            elif t == "signout":
                await manager.disconnect_username(username)
                await manager.broadcast_roster()
                return
            else:
                # ignore unknown
                pass

    except WebSocketDisconnect:
        if username:
            await manager.disconnect_username(username)
            await manager.broadcast_roster()
    except Exception:
        if username:
            await manager.disconnect_username(username)
            await manager.broadcast_roster()


@app.get("/presence")
def presence_debug():
    """
    Optional debug endpoint to see who the server thinks is active.
    """
    st = _get_state()
    return {
        "generate_state": st,
        "inactivity_seconds": INACTIVITY_SECONDS,
    }


@app.post("/signout")
def signout(username: str):
    """
    Optional REST signout if you want it.
    WebSocket signout is better.
    """
    # This only removes server state; client should also stop sending.
    # We can't await here, so we just mark as old by clearing file-based state.
    # For real-time, use ws {"type":"signout"}.
    return {"ok": True, "hint": "Use WebSocket signout for immediate removal."}


# ----------------------------
# Existing routes (unchanged)
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
            "/ws (friends realtime)"
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
        # match what your frontend expects: must generate first
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