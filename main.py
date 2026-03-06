from __future__ import annotations

import hmac
import json
import os
import sqlite3
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from build_hotspot import ensure_zones_geojson, build_hotspots_frames
from core import (
    _auth_user_from_request,
    _clean_display_name,
    _db,
    _db_exec,
    _db_lock,
    _db_query_all,
    _db_query_one,
    _hash_password,
    _sql,
    DB_BACKEND,
    _make_token,
    _require_jwt_secret,
    require_user,
)

# =========================================================
# Paths (Railway volume)
# =========================================================
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
FRAMES_DIR = Path(os.environ.get("FRAMES_DIR", str(DATA_DIR / "frames")))
TIMELINE_PATH = FRAMES_DIR / "timeline.json"

DEFAULT_BIN_MINUTES = int(os.environ.get("DEFAULT_BIN_MINUTES", "20"))
DEFAULT_MIN_TRIPS_PER_WINDOW = int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25"))

LOCK_PATH = DATA_DIR / ".generate.lock"


# Auth / Admin config
JWT_SECRET = os.environ.get("JWT_SECRET", "")  # REQUIRED (set in Railway)
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "").strip().lower()
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
ADMIN_BOOTSTRAP_TOKEN = os.environ.get("ADMIN_BOOTSTRAP_TOKEN", "").strip()

TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "7"))
TOKEN_TTL_SECONDS = int(os.environ.get("TOKEN_TTL_SECONDS", str(30 * 24 * 3600)))  # 30 days
PRESENCE_STALE_SECONDS = int(os.environ.get("PRESENCE_STALE_SECONDS", "300"))  # 5 min
EVENT_DEFAULT_WINDOW_SECONDS = int(os.environ.get("EVENT_DEFAULT_WINDOW_SECONDS", str(24 * 3600)))  # 24h

# =========================================================
# In-memory job state (hotspot generate)
# =========================================================
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

# =========================================================
# App
# =========================================================
app = FastAPI(title="NYC TLC Hotspot Backend", version="2.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock down later
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# Utilities: frames
# =========================================================
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

    return {"ok": True, "state": "started", "bin_minutes": bin_minutes, "min_trips_per_window": min_trips_per_window}


# =========================================================
# Community DB (SQLite)
# =========================================================
def _try_alter(sqlite_sql: str, postgres_sql: Optional[str] = None) -> None:
    """Best-effort schema updates for SQLite and Postgres."""
    sql = postgres_sql if DB_BACKEND == "postgres" and postgres_sql else sqlite_sql
    with _db_lock:
        conn = _db()
        try:
            try:
                conn.cursor().execute(_sql(sql))
                conn.commit()
            except Exception:
                conn.rollback()
        finally:
            conn.close()


def _db_init() -> None:
    if DB_BACKEND == "postgres":
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS users (
              id BIGSERIAL PRIMARY KEY,
              email TEXT NOT NULL UNIQUE,
              pass_salt TEXT NOT NULL,
              pass_hash TEXT NOT NULL,
              is_admin INTEGER NOT NULL DEFAULT 0,
              is_disabled INTEGER NOT NULL DEFAULT 0,
              created_at BIGINT NOT NULL,
              trial_expires_at BIGINT NOT NULL
            );
            """
        )

        _try_alter(
            "ALTER TABLE users ADD COLUMN display_name TEXT;",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT;",
        )
        _try_alter(
            "ALTER TABLE users ADD COLUMN ghost_mode INTEGER NOT NULL DEFAULT 0;",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS ghost_mode INTEGER NOT NULL DEFAULT 0;",
        )

        _db_exec(
            """
            UPDATE users
            SET display_name = COALESCE(display_name, split_part(email, '@', 1))
            WHERE display_name IS NULL OR btrim(display_name) = '';
            """
        )

        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS presence (
              user_id BIGINT PRIMARY KEY,
              lat DOUBLE PRECISION NOT NULL,
              lng DOUBLE PRECISION NOT NULL,
              heading DOUBLE PRECISION,
              accuracy DOUBLE PRECISION,
              updated_at BIGINT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS events (
              id BIGSERIAL PRIMARY KEY,
              type TEXT NOT NULL,
              user_id BIGINT NOT NULL,
              lat DOUBLE PRECISION NOT NULL,
              lng DOUBLE PRECISION NOT NULL,
              text TEXT,
              zone_id INTEGER,
              created_at BIGINT NOT NULL,
              expires_at BIGINT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(type, created_at);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);")

        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS chat_messages (
              id BIGSERIAL PRIMARY KEY,
              room TEXT NOT NULL DEFAULT 'global',
              user_id BIGINT NOT NULL,
              display_name TEXT,
              message TEXT NOT NULL,
              created_at BIGINT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _try_alter(
            "ALTER TABLE chat_messages ADD COLUMN room TEXT NOT NULL DEFAULT 'global';",
            "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS room TEXT NOT NULL DEFAULT 'global';",
        )
        _db_exec("UPDATE chat_messages SET room='global' WHERE room IS NULL OR btrim(room)='';")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_id ON chat_messages(id);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_room_id ON chat_messages(room, id);")
        return

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT NOT NULL UNIQUE,
          pass_salt TEXT NOT NULL,
          pass_hash TEXT NOT NULL,
          is_admin INTEGER NOT NULL DEFAULT 0,
          is_disabled INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL,
          trial_expires_at INTEGER NOT NULL
        );
        """
    )

    _try_alter("ALTER TABLE users ADD COLUMN display_name TEXT;")
    _try_alter("ALTER TABLE users ADD COLUMN ghost_mode INTEGER NOT NULL DEFAULT 0;")

    _db_exec(
        """
        UPDATE users
        SET display_name = COALESCE(display_name, substr(email, 1, instr(email, '@')-1))
        WHERE display_name IS NULL OR trim(display_name) = '';
        """
    )

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS presence (
          user_id INTEGER PRIMARY KEY,
          lat REAL NOT NULL,
          lng REAL NOT NULL,
          heading REAL,
          accuracy REAL,
          updated_at INTEGER NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          type TEXT NOT NULL,
          user_id INTEGER NOT NULL,
          lat REAL NOT NULL,
          lng REAL NOT NULL,
          text TEXT,
          zone_id INTEGER,
          created_at INTEGER NOT NULL,
          expires_at INTEGER NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(type, created_at);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);")

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS chat_messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          room TEXT NOT NULL DEFAULT 'global',
          user_id INTEGER NOT NULL,
          display_name TEXT,
          message TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _try_alter("ALTER TABLE chat_messages ADD COLUMN room TEXT NOT NULL DEFAULT 'global';")
    _db_exec("UPDATE chat_messages SET room='global' WHERE room IS NULL OR trim(room)='';")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_id ON chat_messages(id);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_room_id ON chat_messages(room, id);")


# =========================================================
# Auth helpers (no external deps)
# =========================================================
def require_admin(req: Request) -> sqlite3.Row:
    user = _auth_user_from_request(req)
    if int(user["is_admin"]) != 1:
        raise HTTPException(status_code=403, detail="Admin only")
    return user


def _is_first_user() -> bool:
    row = _db_query_one("SELECT COUNT(*) AS c FROM users")
    return int(row["c"]) == 0 if row else True


def _ensure_admin_seed() -> None:
    """
    Optional: if ADMIN_EMAIL + ADMIN_PASSWORD are set, ensure that admin exists.
    This gives you control without needing to 'sign up' as a regular user.
    """
    if not ADMIN_EMAIL or not ADMIN_PASSWORD:
        return

    existing = _db_query_one("SELECT * FROM users WHERE lower(email)=lower(?) LIMIT 1", (ADMIN_EMAIL,))
    if existing:
        if int(existing["is_admin"]) != 1:
            _db_exec("UPDATE users SET is_admin=1, is_disabled=0 WHERE id=?", (int(existing["id"]),))
        # ensure display_name exists
        _db_exec(
            """
            UPDATE users
            SET display_name = COALESCE(display_name, substr(email, 1, instr(email, '@')-1))
            WHERE id=?;
            """,
            (int(existing["id"]),),
        )
        return

    now = int(time.time())
    trial_expires = now + TRIAL_DAYS * 86400
    salt, ph = _hash_password(ADMIN_PASSWORD)
    display_name = ADMIN_EMAIL.split("@")[0] if "@" in ADMIN_EMAIL else "Admin"
    _db_exec(
        """
        INSERT INTO users(email, pass_salt, pass_hash, is_admin, is_disabled, created_at, trial_expires_at, display_name, ghost_mode)
        VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (ADMIN_EMAIL, salt, ph, 1, 0, now, trial_expires, display_name, 0),
    )


from chat import router as chat_router

app.include_router(chat_router)

# =========================================================
# Startup
# =========================================================
@app.on_event("startup")
def startup():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    _db_init()
    _ensure_admin_seed()

    # Auto-fill generate state if frames already exist
    try:
        if _has_frames():
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


# =========================================================
# Core routes
# =========================================================
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
            "/auth/signup",
            "/auth/login",
            "/me",
            "/me/update",
            "/presence/update",
            "/presence/all",
            "/events/police",
            "/events/pickup",
            "/chat/send",
            "/chat/recent",
            "/chat/since",
            "/admin/users",
            "/admin/users/disable",
            "/admin/users/reset_password",
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
        "community_db": os.environ.get("COMMUNITY_DB", str(DATA_DIR / "community.db")),
        "trial_days": TRIAL_DAYS,
        "auth_enabled": bool(JWT_SECRET and len(JWT_SECRET) >= 24),
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


# =========================================================
# AUTH + COMMUNITY
# =========================================================
class SignupPayload(BaseModel):
    email: str
    password: str
    display_name: Optional[str] = None
    bootstrap_token: Optional[str] = None


class LoginPayload(BaseModel):
    email: str
    password: str


def _decide_admin_for_signup(email: str, bootstrap_token: Optional[str]) -> int:
    is_admin = 0

    # First user is always admin (so you never lose control)
    if _is_first_user():
        is_admin = 1

    # If ADMIN_EMAIL matches, force admin
    if ADMIN_EMAIL and email == ADMIN_EMAIL:
        is_admin = 1

    # Optional bootstrap token can also grant admin
    if ADMIN_BOOTSTRAP_TOKEN and bootstrap_token and bootstrap_token == ADMIN_BOOTSTRAP_TOKEN:
        is_admin = 1

    return is_admin


@app.post("/auth/signup")
def auth_signup(payload: SignupPayload):
    _require_jwt_secret()

    email = (payload.email or "").strip().lower()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email")
    if not payload.password or len(payload.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 chars")

    now = int(time.time())
    trial_expires = now + TRIAL_DAYS * 86400

    is_admin = _decide_admin_for_signup(email, payload.bootstrap_token)
    display_name = _clean_display_name(payload.display_name or "", email)

    salt, ph = _hash_password(payload.password)

    try:
        _db_exec(
            """
            INSERT INTO users(email, pass_salt, pass_hash, is_admin, is_disabled, created_at, trial_expires_at, display_name, ghost_mode)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (email, salt, ph, is_admin, 0, now, trial_expires, display_name, 0),
        )
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Email already exists")

    # IMPORTANT: return token so frontend signup works immediately
    row = _db_query_one("SELECT * FROM users WHERE lower(email)=lower(?) LIMIT 1", (email,))
    if not row:
        raise HTTPException(status_code=500, detail="Signup created user but cannot load it")

    exp = now + TOKEN_TTL_SECONDS
    token = _make_token({"uid": int(row["id"]), "email": email, "exp": exp})

    return {
        "ok": True,
        "created": True,
        "token": token,
        "id": int(row["id"]),
        "email": email,
        "display_name": row["display_name"],
        "ghost_mode": bool(int(row.get("ghost_mode", 0))) if hasattr(row, "get") else bool(int(row["ghost_mode"])) if "ghost_mode" in row.keys() else False,
        "is_admin": bool(is_admin),
        "trial_expires_at": trial_expires,
        "exp": exp,
    }


@app.post("/auth/login")
def auth_login(payload: LoginPayload):
    _require_jwt_secret()

    email = (payload.email or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Invalid email")

    row = _db_query_one("SELECT * FROM users WHERE lower(email)=lower(?) LIMIT 1", (email,))
    if not row:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if int(row["is_disabled"]) == 1:
        raise HTTPException(status_code=403, detail="Account disabled")

    salt = row["pass_salt"]
    _, check = _hash_password(payload.password, salt_b64=salt)
    if not hmac.compare_digest(check, row["pass_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # ensure display_name exists (in case older row)
    dn = (row["display_name"] or "").strip() if "display_name" in row.keys() else ""
    if not dn:
        dn = _clean_display_name("", email)
        _db_exec("UPDATE users SET display_name=? WHERE id=?", (dn, int(row["id"])))

    now = int(time.time())
    exp = now + TOKEN_TTL_SECONDS
    token = _make_token({"uid": int(row["id"]), "email": email, "exp": exp})

    ghost = bool(int(row["ghost_mode"])) if "ghost_mode" in row.keys() and row["ghost_mode"] is not None else False

    return {
        "ok": True,
        "token": token,
        "id": int(row["id"]),
        "email": email,
        "display_name": dn,
        "ghost_mode": ghost,
        "is_admin": bool(int(row["is_admin"])),
        "trial_expires_at": int(row["trial_expires_at"]),
        "exp": exp,
    }


@app.get("/me")
def me(user: sqlite3.Row = Depends(require_user)):
    dn = (user["display_name"] or "").strip() if "display_name" in user.keys() else ""
    if not dn:
        dn = _clean_display_name("", user["email"])
    ghost = bool(int(user["ghost_mode"])) if "ghost_mode" in user.keys() and user["ghost_mode"] is not None else False
    return {
        "ok": True,
        "id": int(user["id"]),
        "email": user["email"],
        "display_name": dn,
        "ghost_mode": ghost,
        "is_admin": bool(int(user["is_admin"])),
        "trial_expires_at": int(user["trial_expires_at"]),
    }


class MeUpdatePayload(BaseModel):
    display_name: Optional[str] = None
    ghost_mode: Optional[bool] = None


@app.post("/me/update")
def me_update(payload: MeUpdatePayload, user: sqlite3.Row = Depends(require_user)):
    # optional endpoint (safe): update username and/or ghost mode
    new_dn = None
    if payload.display_name is not None:
        new_dn = _clean_display_name(payload.display_name, user["email"])

    new_ghost = None
    if payload.ghost_mode is not None:
        new_ghost = 1 if bool(payload.ghost_mode) else 0

    if new_dn is None and new_ghost is None:
        return {"ok": True, "updated": False}

    if new_dn is not None and new_ghost is not None:
        _db_exec("UPDATE users SET display_name=?, ghost_mode=? WHERE id=?", (new_dn, new_ghost, int(user["id"])))
    elif new_dn is not None:
        _db_exec("UPDATE users SET display_name=? WHERE id=?", (new_dn, int(user["id"])))
    else:
        _db_exec("UPDATE users SET ghost_mode=? WHERE id=?", (new_ghost, int(user["id"])))

    row = _db_query_one("SELECT id, email, display_name, ghost_mode, is_admin, trial_expires_at FROM users WHERE id=? LIMIT 1", (int(user["id"]),))
    if not row:
        return {"ok": True, "updated": True}

    return {
        "ok": True,
        "updated": True,
        "id": int(row["id"]),
        "email": row["email"],
        "display_name": (row["display_name"] or _clean_display_name("", row["email"])),
        "ghost_mode": bool(int(row["ghost_mode"])) if row["ghost_mode"] is not None else False,
    }


# =========================================================
# PRESENCE
# =========================================================
class PresencePayload(BaseModel):
    lat: float
    lng: float
    heading: Optional[float] = None
    accuracy: Optional[float] = None


@app.post("/presence/update")
def presence_update(payload: PresencePayload, user: sqlite3.Row = Depends(require_user)):
    now = int(time.time())

    if payload.accuracy is not None and float(payload.accuracy) > 50:
        return {"ok": True}

    # if ghost mode is on, we still accept updates but do not show in /presence/all
    _db_exec(
        """
        INSERT INTO presence(user_id, lat, lng, heading, accuracy, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          lat=excluded.lat,
          lng=excluded.lng,
          heading=excluded.heading,
          accuracy=excluded.accuracy,
          updated_at=excluded.updated_at
        """,
        (int(user["id"]), float(payload.lat), float(payload.lng), payload.heading, payload.accuracy, now),
    )
    return {"ok": True}


@app.get("/presence/all")
def presence_all(
    max_age_sec: int = PRESENCE_STALE_SECONDS,
    viewer: sqlite3.Row = Depends(require_user),  # REQUIRE AUTH (frontend already sends token)
):
    cutoff = int(time.time()) - max(5, min(3600, int(max_age_sec)))
    rows = _db_query_all(
        """
        SELECT
          p.user_id,
          u.email,
          u.display_name,
          u.ghost_mode,
          p.lat,
          p.lng,
          p.heading,
          p.accuracy,
          p.updated_at
        FROM presence p
        LEFT JOIN users u ON u.id = p.user_id
        WHERE p.updated_at >= ?
          AND COALESCE(u.ghost_mode, 0) = 0
        """,
        (cutoff,),
    )

    items: List[Dict[str, Any]] = []
    for r in rows:
        email = (r["email"] or "").strip()
        dn = (r["display_name"] or "").strip()
        if not dn:
            dn = _clean_display_name("", email or "Driver")

        items.append(
            {
                "user_id": int(r["user_id"]),
                "email": email,
                "display_name": dn,
                "lat": float(r["lat"]),
                "lng": float(r["lng"]),
                "heading": float(r["heading"]) if r["heading"] is not None else None,
                "accuracy": float(r["accuracy"]) if r["accuracy"] is not None else None,
                "updated_at": int(r["updated_at"]),
                "updated_at_unix": int(r["updated_at"]),
            }
        )

    return {"ok": True, "count": len(items), "items": items}


# =========================================================
# EVENTS
# =========================================================
class PolicePayload(BaseModel):
    lat: float
    lng: float
    note: Optional[str] = ""


class PickupPayload(BaseModel):
    lat: float
    lng: float
    zone_id: Optional[int] = None
    # Frontend may send extra fields (frame_time, location_id, zone_name, borough, ts_unix) – pydantic ignores extras by default.


class ChatSendPayload(BaseModel):
    message: str


def _clean_chat_message(message: str) -> str:
    cleaned = (message or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    if len(cleaned) > 280:
        raise HTTPException(status_code=400, detail="Message too long (max 280)")
    return cleaned


@app.post("/chat/send")
def chat_send(payload: ChatSendPayload, user: sqlite3.Row = Depends(require_user)):
    now = int(time.time())
    message = _clean_chat_message(payload.message)
    display_name = _clean_display_name(user["display_name"] or "", user["email"])

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            insert_sql = """
                INSERT INTO chat_messages(user_id, display_name, message, created_at)
                VALUES (?, ?, ?, ?)
            """
            if DB_BACKEND == "postgres":
                cur.execute(_sql(insert_sql + " RETURNING id"), (int(user["id"]), display_name, message, now))
                row = cur.fetchone()
                new_id = int(row["id"])
            else:
                cur.execute(_sql(insert_sql), (int(user["id"]), display_name, message, now))
                new_id = int(cur.lastrowid)
            conn.commit()
        finally:
            conn.close()

    return {"ok": True, "id": new_id, "created_at": now, "display_name": display_name}


@app.get("/chat/recent")
def chat_recent(limit: int = 50, user: sqlite3.Row = Depends(require_user)):
    safe_limit = max(1, min(200, int(limit)))
    rows = _db_query_all(
        """
        SELECT id, user_id, display_name, message, created_at
        FROM chat_messages
        ORDER BY id DESC
        LIMIT ?
        """,
        (safe_limit,),
    )
    items = [dict(r) for r in reversed(rows)]
    return {"ok": True, "items": items}


@app.get("/chat/since")
def chat_since(after_id: int = 0, limit: int = 50, user: sqlite3.Row = Depends(require_user)):
    safe_after_id = max(0, int(after_id))
    safe_limit = max(1, min(200, int(limit)))
    rows = _db_query_all(
        """
        SELECT id, user_id, display_name, message, created_at
        FROM chat_messages
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (safe_after_id, safe_limit),
    )
    items = [dict(r) for r in rows]
    return {"ok": True, "items": items}


@app.post("/events/police")
def report_police(payload: PolicePayload, user: sqlite3.Row = Depends(require_user)):
    now = int(time.time())
    expires = now + EVENT_DEFAULT_WINDOW_SECONDS
    txt = (payload.note or "").strip()
    _db_exec(
        """
        INSERT INTO events(type, user_id, lat, lng, text, zone_id, created_at, expires_at)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        ("police", int(user["id"]), float(payload.lat), float(payload.lng), txt, None, now, expires),
    )
    return {"ok": True}


@app.get("/events/police")
def get_police(window_sec: int = 6 * 3600):
    now = int(time.time())
    cutoff = now - max(300, min(7 * 24 * 3600, int(window_sec)))
    rows = _db_query_all(
        """
        SELECT id, lat, lng, text, created_at, expires_at
        FROM events
        WHERE type='police' AND created_at >= ? AND expires_at >= ?
        ORDER BY created_at DESC
        LIMIT 200
        """,
        (cutoff, now),
    )
    items = [dict(r) for r in rows]
    return {"ok": True, "count": len(items), "items": items}


@app.post("/events/pickup")
def log_pickup(payload: PickupPayload, user: sqlite3.Row = Depends(require_user)):
    now = int(time.time())
    expires = now + EVENT_DEFAULT_WINDOW_SECONDS
    _db_exec(
        """
        INSERT INTO events(type, user_id, lat, lng, text, zone_id, created_at, expires_at)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        ("pickup", int(user["id"]), float(payload.lat), float(payload.lng), "", payload.zone_id, now, expires),
    )
    return {"ok": True}


# =========================================================
# ADMIN (manage all accounts)
# =========================================================
@app.get("/admin/users")
def admin_users(admin: sqlite3.Row = Depends(require_admin)):
    rows = _db_query_all(
        """
        SELECT id, email, display_name, ghost_mode, is_admin, is_disabled, created_at, trial_expires_at
        FROM users
        ORDER BY created_at ASC
        """
    )
    items = [dict(r) for r in rows]
    return {"ok": True, "count": len(items), "items": items}


class AdminDisablePayload(BaseModel):
    user_id: int
    disabled: bool


@app.post("/admin/users/disable")
def admin_disable_user(payload: AdminDisablePayload, admin: sqlite3.Row = Depends(require_admin)):
    _db_exec("UPDATE users SET is_disabled=? WHERE id=?", (1 if payload.disabled else 0, int(payload.user_id)))
    return {"ok": True}


class AdminResetPayload(BaseModel):
    user_id: int
    new_password: str


@app.post("/admin/users/reset_password")
def admin_reset_password(payload: AdminResetPayload, admin: sqlite3.Row = Depends(require_admin)):
    if not payload.new_password or len(payload.new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 chars")
    salt, ph = _hash_password(payload.new_password)
    _db_exec("UPDATE users SET pass_salt=?, pass_hash=? WHERE id=?", (salt, ph, int(payload.user_id)))
    return {"ok": True}
