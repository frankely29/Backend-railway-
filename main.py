import os
import json
import time
import threading
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ----------------------------
# Config
# ----------------------------
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "/data/cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TIMELINE_PATH = CACHE_DIR / "timeline.json"
FRAMES_DIR = CACHE_DIR / "frames"
FRAMES_DIR.mkdir(parents=True, exist_ok=True)

# script that builds the timeline + frames
BUILD_SCRIPT = os.environ.get("BUILD_SCRIPT", "build_hotspot.py")

# ----------------------------
# App
# ----------------------------
app = FastAPI(title="NYC TLC Hotspot Backend", version="1.0")

# CORS (Github Pages -> Railway)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ok for now; can lock down later
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Utilities
# ----------------------------

# ----------------------------
# Friends / Presence (Option A: in-memory)
# ----------------------------
# NOTE: This is intentionally simple (no database). On Railway, if you run
# multiple instances, each instance will have its own memory.

PRESENCE_TTL_SEC = int(os.environ.get("PRESENCE_TTL_SEC", str(30 * 60)))  # 30 minutes

_presence_lock = threading.Lock()
_presence: Dict[str, Dict[str, Any]] = {}


class PresenceHeartbeat(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=64)
    username: str = Field(..., min_length=1, max_length=32)
    lat: float
    lng: float
    heading: float | None = None
    speed_mph: float | None = None


class PresenceSignout(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=64)


def _presence_cleanup(now: float | None = None) -> int:
    """Remove inactive users. Returns number removed."""
    if now is None:
        now = time.time()
    removed = 0
    with _presence_lock:
        dead = [uid for uid, rec in _presence.items() if (now - float(rec.get("last_seen", 0))) > PRESENCE_TTL_SEC]
        for uid in dead:
            _presence.pop(uid, None)
            removed += 1
    return removed


def _presence_snapshot(now: float | None = None) -> List[Dict[str, Any]]:
    if now is None:
        now = time.time()
    _presence_cleanup(now)
    with _presence_lock:
        out: List[Dict[str, Any]] = []
        for uid, rec in _presence.items():
            out.append(
                {
                    "user_id": uid,
                    "username": rec.get("username"),
                    "lat": rec.get("lat"),
                    "lng": rec.get("lng"),
                    "heading": rec.get("heading"),
                    "speed_mph": rec.get("speed_mph"),
                    "last_seen_unix": rec.get("last_seen"),
                    "expires_in_sec": max(0, int(PRESENCE_TTL_SEC - (now - float(rec.get("last_seen", 0))))),
                }
            )
        return out


def safe_read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def write_json(path: Path, obj: Any):
    path.write_text(json.dumps(obj, ensure_ascii=False))


def frames_count() -> int:
    if not FRAMES_DIR.exists():
        return 0
    return len(list(FRAMES_DIR.glob("frame_*.json")))


# ----------------------------
# Background generate runner
# ----------------------------
_generate_lock = threading.Lock()
_generate_status: Dict[str, Any] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "ok": None,
    "error": None,
    "frames": 0,
}


def _run_generate():
    global _generate_status
    with _generate_lock:
        _generate_status.update(
            {
                "running": True,
                "started_at": time.time(),
                "finished_at": None,
                "ok": None,
                "error": None,
                "frames": 0,
            }
        )

    try:
        # run build_hotspot.py
        cmd = ["python", BUILD_SCRIPT]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if proc.returncode != 0:
            raise RuntimeError(
                f"Generate failed ({proc.returncode}).\nSTDOUT:\n{proc.stdout[-2000:]}\nSTDERR:\n{proc.stderr[-2000:]}"
            )

        # validate output
        timeline = safe_read_json(TIMELINE_PATH)
        if not timeline:
            raise RuntimeError("Generate completed but timeline.json is missing/empty.")

        cnt = frames_count()
        if cnt == 0:
            raise RuntimeError("Generate completed but no frames were produced.")

        with _generate_lock:
            _generate_status.update(
                {
                    "running": False,
                    "finished_at": time.time(),
                    "ok": True,
                    "frames": cnt,
                }
            )
    except Exception as e:
        with _generate_lock:
            _generate_status.update(
                {
                    "running": False,
                    "finished_at": time.time(),
                    "ok": False,
                    "error": str(e),
                    "frames": frames_count(),
                }
            )


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
            # Friends / Presence
            "/presence",
            "/presence/heartbeat",
            "/presence/signout",
        ],
    }


# ----------------------------
# Friends / Presence API
# ----------------------------
@app.get("/presence")
def get_presence():
    """Return currently active users (auto-expires after PRESENCE_TTL_SEC)."""
    return {"ttl_sec": PRESENCE_TTL_SEC, "users": _presence_snapshot()}


@app.post("/presence/heartbeat")
def presence_heartbeat(payload: PresenceHeartbeat):
    """Upsert a user's last known position and return current active users.

    Frontend should call this every ~5-15 seconds while the map is open.
    If the user becomes inactive (no heartbeats), they auto-disappear after PRESENCE_TTL_SEC.
    """
    now = time.time()
    rec = {
        "username": payload.username.strip()[:32],
        "lat": float(payload.lat),
        "lng": float(payload.lng),
        "heading": None if payload.heading is None else float(payload.heading),
        "speed_mph": None if payload.speed_mph is None else float(payload.speed_mph),
        "last_seen": now,
    }
    with _presence_lock:
        _presence[payload.user_id] = rec
    return {"ttl_sec": PRESENCE_TTL_SEC, "users": _presence_snapshot(now)}


@app.post("/presence/signout")
def presence_signout(payload: PresenceSignout):
    """Explicit signout (used by Sign Out button)."""
    with _presence_lock:
        _presence.pop(payload.user_id, None)
    return {"ok": True, "ttl_sec": PRESENCE_TTL_SEC, "users": _presence_snapshot()}


@app.get("/status")
def status():
    timeline = safe_read_json(TIMELINE_PATH)
    return {
        "data_dir": str(DATA_DIR),
        "cache_dir": str(CACHE_DIR),
        "timeline_ready": bool(timeline),
        "frames": frames_count(),
        "generate_status": _generate_status,
    }


@app.post("/generate")
def generate():
    # start generation in background thread
    with _generate_lock:
        if _generate_status.get("running"):
            return {"ok": True, "running": True, "message": "Generate already running."}

    t = threading.Thread(target=_run_generate, daemon=True)
    t.start()
    return {"ok": True, "running": True, "message": "Generate started."}


@app.get("/generate_status")
def generate_status():
    return _generate_status


@app.get("/timeline")
def get_timeline():
    timeline = safe_read_json(TIMELINE_PATH)
    if not timeline:
        return JSONResponse(status_code=400, content={"error": "timeline not ready. Call /generate first."})
    return timeline


@app.get("/frame/{idx}")
def get_frame(idx: int):
    frame_path = FRAMES_DIR / f"frame_{idx}.json"
    if not frame_path.exists():
        return JSONResponse(status_code=404, content={"error": f"frame {idx} not found"})
    try:
        return json.loads(frame_path.read_text())
    except Exception:
        return JSONResponse(status_code=500, content={"error": f"frame {idx} invalid json"})


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