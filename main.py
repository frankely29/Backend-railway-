from __future__ import annotations

import os
import json
import time
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
import uuid

from fastapi import FastAPI, UploadFile, File, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware

from build_hotspot import ensure_zones_geojson, build_hotspots_frames

# ----------------------------
# NEW: Community DB (SQLite on Railway volume)
# ----------------------------
from sqlalchemy import create_engine, Column, String, DateTime, Float, Text
from sqlalchemy.orm import declarative_base, sessionmaker

try:
    import jwt  # PyJWT
except Exception:
    jwt = None

try:
    from passlib.context import CryptContext
except Exception:
    CryptContext = None


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
# In-memory job state
# ----------------------------
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
app = FastAPI(title="NYC TLC Hotspot Backend", version="1.1")

# Allow GitHub Pages frontend to call Railway backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock down later if you want
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Utilities (existing)
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
        return {"ok": True, "state": "running", "bin_minutes": bin_minutes, "min_trips_per_window": min_trips_per_window}

    _write_lock()
    _set_state(state="started", bin_minutes=bin_minutes, min_trips_per_window=min_trips_per_window)

    t = threading.Thread(target=_generate_worker, args=(bin_minutes, min_trips_per_window), daemon=True)
    t.start()

    return {"ok": True, "state": "started", "bin_minutes": bin_minutes, "min_trips_per_window": min_trips_per_window}


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


# ==========================================================
# NEW: Community/Auth system (SQLite on volume)
# ==========================================================
COMMUNITY_DB_PATH = Path(os.environ.get("COMMUNITY_DB_PATH", str(DATA_DIR / "community.db")))
COMMUNITY_DB_URL = f"sqlite:///{COMMUNITY_DB_PATH}"

JWT_SECRET = os.environ.get("JWT_SECRET", "CHANGE_ME_IN_RAILWAY_ENV")
JWT_ALG = "HS256"
JWT_EXPIRE_DAYS = int(os.environ.get("JWT_EXPIRE_DAYS", "30"))
TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "7"))
POLICE_TTL_MIN = int(os.environ.get("POLICE_TTL_MIN", "30"))

Base = declarative_base()
engine = create_engine(COMMUNITY_DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto") if CryptContext else None

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def _require_deps():
    if jwt is None:
        raise HTTPException(500, "Missing dependency: PyJWT (add to requirements.txt)")
    if pwd_ctx is None:
        raise HTTPException(500, "Missing dependency: passlib[bcrypt] (add to requirements.txt)")

def _hash_pw(pw: str) -> str:
    _require_deps()
    return pwd_ctx.hash(pw)

def _verify_pw(pw: str, hashed: str) -> bool:
    _require_deps()
    return pwd_ctx.verify(pw, hashed)

def _create_token(user_id: str) -> str:
    _require_deps()
    now = utcnow()
    payload = {
        "sub": user_id,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=JWT_EXPIRE_DAYS)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)

def _user_id_from_auth(authorization: Optional[str]) -> str:
    _require_deps()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing token")
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        return str(payload.get("sub"))
    except Exception:
        raise HTTPException(401, "Invalid token")

class User(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    role = Column(String, nullable=False, default="user")  # admin|user
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    last_login_at = Column(DateTime(timezone=True), nullable=True)

class SubscriptionState(Base):
    __tablename__ = "subscription_state"
    user_id = Column(String, primary_key=True)
    trial_start = Column(DateTime(timezone=True), nullable=False)
    trial_end = Column(DateTime(timezone=True), nullable=False)
    status = Column(String, nullable=False, default="trial")  # trial|active|expired

class Presence(Base):
    __tablename__ = "presence"
    user_id = Column(String, primary_key=True)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    heading = Column(Float, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)

class PoliceReport(Base):
    __tablename__ = "police_reports"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    zone_id = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)

class PickupLog(Base):
    __tablename__ = "pickup_logs"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False)
    zone_id = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)

@app.on_event("startup")
def init_community_db():
    # Ensure volume dir exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)

# ----------------------------
# NEW: Auth routes
# ----------------------------
@app.post("/auth/signup")
def auth_signup(payload: Dict[str, Any]):
    _require_deps()
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password") or ""
    display_name = (payload.get("display_name") or "").strip()

    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email")
    if len(password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    if not display_name:
        raise HTTPException(400, "Display name required")

    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.email == email).first()
        if existing:
            raise HTTPException(400, "Email already in use")

        u = User(email=email, password_hash=_hash_pw(password), display_name=display_name, role="user")
        db.add(u)
        db.flush()

        now = utcnow()
        sub = SubscriptionState(
            user_id=u.id,
            trial_start=now,
            trial_end=now + timedelta(days=TRIAL_DAYS),
            status="trial",
        )
        db.add(sub)
        db.commit()

        return {"token": _create_token(u.id)}
    finally:
        db.close()

@app.post("/auth/login")
def auth_login(payload: Dict[str, Any]):
    _require_deps()
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password") or ""

    db = SessionLocal()
    try:
        u = db.query(User).filter(User.email == email).first()
        if not u or not _verify_pw(password, u.password_hash):
            raise HTTPException(401, "Invalid credentials")

        u.last_login_at = utcnow()
        db.commit()

        return {"token": _create_token(u.id)}
    finally:
        db.close()

@app.get("/me")
def me(authorization: Optional[str] = Header(default=None)):
    uid = _user_id_from_auth(authorization)
    db = SessionLocal()
    try:
        u = db.query(User).filter(User.id == uid).first()
        if not u:
            raise HTTPException(401, "User not found")

        sub = db.query(SubscriptionState).filter(SubscriptionState.user_id == uid).first()
        now = utcnow()
        trial_active = bool(sub and now <= sub.trial_end)

        return {
            "id": u.id,
            "email": u.email,
            "display_name": u.display_name,
            "role": u.role,
            "trial_end": sub.trial_end.isoformat() if sub else None,
            "trial_active": trial_active,
            "subscription_status": sub.status if sub else "trial",
        }
    finally:
        db.close()

# ----------------------------
# NEW: Presence routes (everyone visible city-wide)
# ----------------------------
@app.post("/presence/update")
def presence_update(payload: Dict[str, Any], authorization: Optional[str] = Header(default=None)):
    uid = _user_id_from_auth(authorization)

    try:
        lat = float(payload.get("lat"))
        lng = float(payload.get("lng"))
    except Exception:
        raise HTTPException(400, "lat/lng required")
    heading = payload.get("heading", None)

    db = SessionLocal()
    try:
        row = db.query(Presence).filter(Presence.user_id == uid).first()
        now = utcnow()
        if not row:
            row = Presence(user_id=uid, lat=lat, lng=lng, heading=heading, updated_at=now)
            db.add(row)
        else:
            row.lat = lat
            row.lng = lng
            row.heading = heading
            row.updated_at = now
        db.commit()
        return {"ok": True}
    finally:
        db.close()

@app.get("/presence/all")
def presence_all(max_age_sec: int = 60):
    cutoff = utcnow() - timedelta(seconds=max_age_sec)

    db = SessionLocal()
    try:
        # Join manually
        pres = db.query(Presence).filter(Presence.updated_at >= cutoff).all()
        user_ids = [p.user_id for p in pres]
        users = db.query(User).filter(User.id.in_(user_ids)).all() if user_ids else []
        umap = {u.id: u for u in users}

        out = []
        for p in pres:
            u = umap.get(p.user_id)
            out.append({
                "user_id": p.user_id,
                "display_name": (u.display_name if u else "Unknown"),
                "lat": p.lat,
                "lng": p.lng,
                "heading": p.heading,
                "updated_at": p.updated_at.isoformat(),
            })
        return out
    finally:
        db.close()

# ----------------------------
# NEW: Police + Pickup routes
# ----------------------------
@app.post("/events/police")
def report_police(payload: Dict[str, Any], authorization: Optional[str] = Header(default=None)):
    uid = _user_id_from_auth(authorization)

    try:
        lat = float(payload.get("lat"))
        lng = float(payload.get("lng"))
    except Exception:
        raise HTTPException(400, "lat/lng required")

    zone_id = payload.get("zone_id", None)
    now = utcnow()

    db = SessionLocal()
    try:
        r = PoliceReport(
            user_id=uid,
            lat=lat,
            lng=lng,
            zone_id=(str(zone_id) if zone_id is not None else None),
            created_at=now,
            expires_at=now + timedelta(minutes=POLICE_TTL_MIN),
        )
        db.add(r)
        db.commit()
        return {"ok": True, "expires_at": r.expires_at.isoformat()}
    finally:
        db.close()

@app.get("/events/police")
def get_police():
    now = utcnow()
    db = SessionLocal()
    try:
        rows = db.query(PoliceReport).filter(PoliceReport.expires_at >= now).all()
        return [{
            "id": r.id,
            "lat": r.lat,
            "lng": r.lng,
            "zone_id": r.zone_id,
            "created_at": r.created_at.isoformat(),
            "expires_at": r.expires_at.isoformat(),
        } for r in rows]
    finally:
        db.close()

@app.post("/events/pickup")
def log_pickup(payload: Dict[str, Any], authorization: Optional[str] = Header(default=None)):
    uid = _user_id_from_auth(authorization)

    zone_id = payload.get("zone_id", None)
    if zone_id is None:
        raise HTTPException(400, "zone_id required")

    try:
        lat = float(payload.get("lat"))
        lng = float(payload.get("lng"))
    except Exception:
        raise HTTPException(400, "lat/lng required")

    db = SessionLocal()
    try:
        p = PickupLog(user_id=uid, zone_id=str(zone_id), lat=lat, lng=lng, created_at=utcnow())
        db.add(p)
        db.commit()
        return {"ok": True}
    finally:
        db.close()


# ==========================================================
# EXISTING ROUTES (unchanged)
# ==========================================================
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "NYC TLC Hotspot Backend",
        "endpoints": [
            "/status", "/generate", "/generate_status", "/timeline", "/frame/{idx}",
            # new:
            "/auth/signup", "/auth/login", "/me",
            "/presence/update", "/presence/all",
            "/events/police", "/events/pickup",
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
        "community_db": str(COMMUNITY_DB_PATH),
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