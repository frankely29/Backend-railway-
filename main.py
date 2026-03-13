from __future__ import annotations

import hmac
import hashlib
import json
import math
import os
import sqlite3
import threading
import time
import traceback
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pyproj import Transformer
from shapely.geometry import MultiPolygon, Point, Polygon, mapping, shape
from shapely.ops import transform, unary_union

from build_hotspot import ensure_zones_geojson, build_hotspots_frames
from hotspot_experiments import (
    log_micro_bins,
    log_recommendation_outcome,
    log_zone_bins,
    prune_experiment_tables,
)
from hotspot_scoring import score_zones
from micro_hotspot_scoring import score_micro_hotspots
from admin_routes import router as admin_router
from admin_mutation_routes import router as admin_mutation_router
from admin_test_routes import router as admin_test_router
from admin_trips_routes import router as admin_trips_router
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
MAX_AVATAR_DATA_URL_LENGTH = int(os.environ.get("MAX_AVATAR_DATA_URL_LENGTH", "20000"))
ALLOWED_MAP_IDENTITY_MODES = {"name", "avatar"}

PICKUP_ZONE_HOTSPOT_MIN_POINTS = 5  # Keep 5-dot minimum to avoid pickup noise.
PICKUP_ZONE_HOTSPOT_MAX_POINTS = 100
PICKUP_ZONE_HOTSPOT_CELL_SIZE_M = 135
PICKUP_ZONE_HOTSPOT_RADIUS_M = 240
PICKUP_ZONE_HOTSPOT_SIGMA_M = 155
PICKUP_ZONE_HOTSPOT_SIMPLIFY_M = 18
HOTSPOT_RECENT_LOOKBACK_SECONDS = 6 * 3600
HOTSPOT_TIMESLOT_BIN_MINUTES = 20

_pickup_zone_geom_cache: Optional[Dict[int, Dict[str, Any]]] = None
_pickup_zone_geom_cache_mtime: Optional[float] = None
_pickup_zone_hotspot_feature_cache: Dict[int, Dict[str, Any]] = {}
_pickup_zone_score_cache: Dict[int, float] = {}
_pickup_zone_hotspot_cache_lock = threading.Lock()
_to_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
_to_4326 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

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

app.include_router(admin_router)
app.include_router(admin_mutation_router)
app.include_router(admin_trips_router)
app.include_router(admin_test_router)

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
              is_admin BOOLEAN NOT NULL DEFAULT FALSE,
              is_disabled BOOLEAN NOT NULL DEFAULT FALSE,
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
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS ghost_mode BOOLEAN NOT NULL DEFAULT FALSE;",
        )
        _try_alter(
            "ALTER TABLE users ADD COLUMN avatar_url TEXT;",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT;",
        )
        _try_alter(
            "ALTER TABLE users ADD COLUMN map_identity_mode TEXT NOT NULL DEFAULT 'name';",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS map_identity_mode TEXT NOT NULL DEFAULT 'name';",
        )
        _try_alter(
            "ALTER TABLE users ADD COLUMN is_suspended INTEGER NOT NULL DEFAULT 0;",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_suspended BOOLEAN NOT NULL DEFAULT FALSE;",
        )

        _db_exec(
            """
            ALTER TABLE users
            -- Convert is_admin to boolean and reset default
            ALTER COLUMN is_admin TYPE BOOLEAN USING (CASE WHEN lower(is_admin::text) IN ('1', 't', 'true') THEN TRUE ELSE FALSE END),
            ALTER COLUMN is_admin SET DEFAULT FALSE,
            -- Convert is_disabled to boolean and reset default
            ALTER COLUMN is_disabled TYPE BOOLEAN USING (CASE WHEN lower(is_disabled::text) IN ('1', 't', 'true') THEN TRUE ELSE FALSE END),
            ALTER COLUMN is_disabled SET DEFAULT FALSE,
            -- Drop ghost_mode default before converting type
            ALTER COLUMN ghost_mode DROP DEFAULT,
            ALTER COLUMN ghost_mode TYPE BOOLEAN USING (CASE WHEN lower(ghost_mode::text) IN ('1', 't', 'true') THEN TRUE ELSE FALSE END),
            ALTER COLUMN ghost_mode SET DEFAULT FALSE,
            -- Convert is_suspended to boolean and reset default
            ALTER COLUMN is_suspended TYPE BOOLEAN USING (CASE WHEN lower(is_suspended::text) IN ('1', 't', 'true') THEN TRUE ELSE FALSE END),
            ALTER COLUMN is_suspended SET DEFAULT FALSE
            """
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
            UPDATE users
            SET map_identity_mode = 'name'
            WHERE map_identity_mode IS NULL OR btrim(map_identity_mode) = '';
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
            CREATE TABLE IF NOT EXISTS pickup_logs (
              id BIGSERIAL PRIMARY KEY,
              user_id BIGINT NOT NULL,
              lat DOUBLE PRECISION NOT NULL,
              lng DOUBLE PRECISION NOT NULL,
              zone_id INTEGER,
              zone_name TEXT,
              borough TEXT,
              frame_time TEXT,
              created_at BIGINT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_pickup_logs_created_at ON pickup_logs(created_at DESC);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_pickup_logs_zone_time ON pickup_logs(zone_id, created_at DESC);")

        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS hotspot_experiment_bins (
              id BIGSERIAL PRIMARY KEY,
              bin_time BIGINT NOT NULL,
              zone_id INTEGER NOT NULL,
              final_score DOUBLE PRECISION NOT NULL,
              confidence DOUBLE PRECISION NOT NULL,
              historical_component DOUBLE PRECISION NOT NULL,
              live_component DOUBLE PRECISION NOT NULL,
              same_timeslot_component DOUBLE PRECISION NOT NULL,
              density_penalty DOUBLE PRECISION NOT NULL,
              weighted_trip_count DOUBLE PRECISION NOT NULL,
              unique_driver_count INTEGER NOT NULL,
              recommended BOOLEAN NOT NULL DEFAULT FALSE
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_hotspot_experiment_bins_time ON hotspot_experiment_bins(bin_time DESC);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_hotspot_experiment_bins_zone_time ON hotspot_experiment_bins(zone_id, bin_time DESC);")

        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS micro_hotspot_experiment_bins (
              id BIGSERIAL PRIMARY KEY,
              bin_time BIGINT NOT NULL,
              zone_id INTEGER NOT NULL,
              cluster_id TEXT NOT NULL,
              final_score DOUBLE PRECISION NOT NULL,
              confidence DOUBLE PRECISION NOT NULL,
              weighted_trip_count DOUBLE PRECISION NOT NULL,
              unique_driver_count INTEGER NOT NULL,
              crowding_penalty DOUBLE PRECISION NOT NULL,
              recommended BOOLEAN NOT NULL DEFAULT FALSE
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_micro_hotspot_experiment_bins_time ON micro_hotspot_experiment_bins(bin_time DESC);")

        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS recommendation_outcomes (
              id BIGSERIAL PRIMARY KEY,
              user_id BIGINT,
              recommended_at BIGINT NOT NULL,
              zone_id INTEGER NOT NULL,
              cluster_id TEXT,
              score DOUBLE PRECISION NOT NULL,
              confidence DOUBLE PRECISION NOT NULL,
              converted_to_trip BOOLEAN,
              minutes_to_trip DOUBLE PRECISION
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_recommendation_outcomes_time ON recommendation_outcomes(recommended_at DESC);")

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
    _try_alter("ALTER TABLE users ADD COLUMN avatar_url TEXT;")
    _try_alter("ALTER TABLE users ADD COLUMN map_identity_mode TEXT NOT NULL DEFAULT 'name';")
    _try_alter("ALTER TABLE users ADD COLUMN is_suspended INTEGER NOT NULL DEFAULT 0;")

    _db_exec(
        """
        UPDATE users
        SET display_name = COALESCE(display_name, substr(email, 1, instr(email, '@')-1))
        WHERE display_name IS NULL OR trim(display_name) = '';
        """
    )
    _db_exec(
        """
        UPDATE users
        SET map_identity_mode = 'name'
        WHERE map_identity_mode IS NULL OR trim(map_identity_mode) = '';
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
        CREATE TABLE IF NOT EXISTS pickup_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          lat REAL NOT NULL,
          lng REAL NOT NULL,
          zone_id INTEGER,
          zone_name TEXT,
          borough TEXT,
          frame_time TEXT,
          created_at INTEGER NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_pickup_logs_created_at ON pickup_logs(created_at DESC);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_pickup_logs_zone_time ON pickup_logs(zone_id, created_at DESC);")

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS hotspot_experiment_bins (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          bin_time INTEGER NOT NULL,
          zone_id INTEGER NOT NULL,
          final_score REAL NOT NULL,
          confidence REAL NOT NULL,
          historical_component REAL NOT NULL,
          live_component REAL NOT NULL,
          same_timeslot_component REAL NOT NULL,
          density_penalty REAL NOT NULL,
          weighted_trip_count REAL NOT NULL,
          unique_driver_count INTEGER NOT NULL,
          recommended INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_hotspot_experiment_bins_time ON hotspot_experiment_bins(bin_time DESC);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_hotspot_experiment_bins_zone_time ON hotspot_experiment_bins(zone_id, bin_time DESC);")

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS micro_hotspot_experiment_bins (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          bin_time INTEGER NOT NULL,
          zone_id INTEGER NOT NULL,
          cluster_id TEXT NOT NULL,
          final_score REAL NOT NULL,
          confidence REAL NOT NULL,
          weighted_trip_count REAL NOT NULL,
          unique_driver_count INTEGER NOT NULL,
          crowding_penalty REAL NOT NULL,
          recommended INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_micro_hotspot_experiment_bins_time ON micro_hotspot_experiment_bins(bin_time DESC);")

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS recommendation_outcomes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER,
          recommended_at INTEGER NOT NULL,
          zone_id INTEGER NOT NULL,
          cluster_id TEXT,
          score REAL NOT NULL,
          confidence REAL NOT NULL,
          converted_to_trip INTEGER,
          minutes_to_trip REAL
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_recommendation_outcomes_time ON recommendation_outcomes(recommended_at DESC);")

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
    if _flag_to_int(user["is_admin"]) != 1:
        raise HTTPException(status_code=403, detail="Admin only")
    return user


def _is_first_user() -> bool:
    row = _db_query_one("SELECT COUNT(*) AS c FROM users")
    return int(row["c"]) == 0 if row else True


def _is_bool_column(table: str, column: str) -> bool:
    """
    Return True when a Postgres column is defined as boolean.
    SQLite stores booleans as integers, so always returns False there.
    """
    if DB_BACKEND != "postgres":
        return False
    try:
        row = _db_query_one(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name=? AND column_name=?
            LIMIT 1
            """,
            (table, column),
        )
        data_type = str(row["data_type"]).lower().strip() if row and row["data_type"] is not None else ""
        return data_type.startswith("bool")
    except Exception:
        return False


def _flag_to_int(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if value is None:
        return 0
    return int(value)


def _normalize_map_identity_mode(value: Optional[str]) -> str:
    mode = (value or "").strip().lower()
    if mode not in ALLOWED_MAP_IDENTITY_MODES:
        raise HTTPException(status_code=400, detail="map_identity_mode must be 'name' or 'avatar'")
    return mode


def _normalize_avatar_url(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    avatar = value.strip()
    if avatar == "":
        return None
    if len(avatar) > MAX_AVATAR_DATA_URL_LENGTH:
        raise HTTPException(status_code=400, detail="avatar_url is too large")
    if not avatar.startswith("data:image/"):
        raise HTTPException(status_code=400, detail="avatar_url must be an image data URL")
    if "," not in avatar:
        raise HTTPException(status_code=400, detail="avatar_url must be a valid data URL")
    return avatar


def _ensure_admin_seed() -> None:
    """
    Optional: if ADMIN_EMAIL + ADMIN_PASSWORD are set, ensure that admin exists.
    This gives you control without needing to 'sign up' as a regular user.
    """
    if not ADMIN_EMAIL or not ADMIN_PASSWORD:
        return

    existing = _db_query_one("SELECT * FROM users WHERE lower(email)=lower(?) LIMIT 1", (ADMIN_EMAIL,))
    if existing:
        if _flag_to_int(existing["is_admin"]) != 1:
            admin_is_bool = _is_bool_column("users", "is_admin")
            disabled_is_bool = _is_bool_column("users", "is_disabled")
            is_admin_val = True if admin_is_bool else 1
            is_disabled_val = False if disabled_is_bool else 0
            _db_exec(
                "UPDATE users SET is_admin=?, is_disabled=? WHERE id=?",
                (is_admin_val, is_disabled_val, int(existing["id"])),
            )
        # ensure display_name exists: use SQLite functions for SQLite, PostgreSQL functions for Postgres
        if DB_BACKEND == "postgres":
            _db_exec(
                """
                UPDATE users
                SET display_name = COALESCE(display_name, split_part(email, '@', 1))
                WHERE id=?;
                """,
                (int(existing["id"]),),
            )
        else:
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
    # Insert admin user with values that match live column types.
    admin_is_bool = _is_bool_column("users", "is_admin")
    disabled_is_bool = _is_bool_column("users", "is_disabled")
    ghost_is_bool = _is_bool_column("users", "ghost_mode")
    is_admin_val = True if admin_is_bool else 1
    is_disabled_val = False if disabled_is_bool else 0
    ghost_mode_val = False if ghost_is_bool else 0
    _db_exec(
        """
        INSERT INTO users(email, pass_salt, pass_hash, is_admin, is_disabled, created_at, trial_expires_at, display_name, ghost_mode)
        VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (ADMIN_EMAIL, salt, ph, is_admin_val, is_disabled_val, now, trial_expires, display_name, ghost_mode_val),
    )


from chat import router as chat_router
from leaderboard_db import init_leaderboard_schema
from leaderboard_models import LeaderboardMetric, LeaderboardPeriod
from leaderboard_routes import router as leaderboard_router
from leaderboard_service import (
    get_best_current_badge_for_user,
    get_best_current_badges_for_users,
    get_lifetime_totals_for_user,
    get_level_progress_from_lifetime_miles,
    get_my_rank,
    get_overview_for_user,
)
from leaderboard_tracker import increment_pickup_count, record_presence_heartbeat

app.include_router(chat_router)
app.include_router(leaderboard_router)

# =========================================================
# Startup
# =========================================================
@app.on_event("startup")
def startup():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    _db_init()
    init_leaderboard_schema()
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
            "/events/pickups/recent",
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

    admin_is_bool = _is_bool_column("users", "is_admin")
    disabled_is_bool = _is_bool_column("users", "is_disabled")
    ghost_is_bool = _is_bool_column("users", "ghost_mode")
    is_admin_val = (True if is_admin else False) if admin_is_bool else (1 if is_admin else 0)
    is_disabled_val = False if disabled_is_bool else 0
    ghost_mode_val = False if ghost_is_bool else 0

    try:
        _db_exec(
            """
            INSERT INTO users(email, pass_salt, pass_hash, is_admin, is_disabled, created_at, trial_expires_at, display_name, ghost_mode)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (email, salt, ph, is_admin_val, is_disabled_val, now, trial_expires, display_name, ghost_mode_val),
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
        "ghost_mode": bool(_flag_to_int(row.get("ghost_mode", 0))) if hasattr(row, "get") else bool(_flag_to_int(row["ghost_mode"])) if "ghost_mode" in row.keys() else False,
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
    if _flag_to_int(row["is_disabled"]) == 1:
        raise HTTPException(status_code=403, detail="Account disabled")

    # Trim any whitespace/newlines on stored salt and hash; some databases
    # (notably Postgres) may store trailing spaces, causing a mismatch.
    salt = (row["pass_salt"] or "").strip()
    stored_hash = (row["pass_hash"] or "").strip()
    _, check = _hash_password(payload.password, salt_b64=salt)
    matched_legacy = False
    if not hmac.compare_digest(check, stored_hash):
        _, legacy_check = _hash_password(payload.password, salt_b64=salt, iterations=100_000)
        if not hmac.compare_digest(legacy_check, stored_hash):
            raise HTTPException(status_code=401, detail="Invalid email or password")
        matched_legacy = True

    if matched_legacy:
        _, upgraded_hash = _hash_password(payload.password, salt_b64=salt)
        _db_exec("UPDATE users SET pass_hash=? WHERE id=?", (upgraded_hash, int(row["id"])))

    # ensure display_name exists (in case older row)
    dn = (row["display_name"] or "").strip() if "display_name" in row.keys() else ""
    if not dn:
        dn = _clean_display_name("", email)
        _db_exec("UPDATE users SET display_name=? WHERE id=?", (dn, int(row["id"])))

    now = int(time.time())
    exp = now + TOKEN_TTL_SECONDS
    token = _make_token({"uid": int(row["id"]), "email": email, "exp": exp})

    ghost = bool(_flag_to_int(row["ghost_mode"])) if "ghost_mode" in row.keys() and row["ghost_mode"] is not None else False

    return {
        "ok": True,
        "token": token,
        "id": int(row["id"]),
        "email": email,
        "display_name": dn,
        "ghost_mode": ghost,
        "is_admin": bool(_flag_to_int(row["is_admin"])),
        "trial_expires_at": int(row["trial_expires_at"]),
        "exp": exp,
    }


@app.get("/me")
def me(user: sqlite3.Row = Depends(require_user)):
    dn = (user["display_name"] or "").strip() if "display_name" in user.keys() else ""
    if not dn:
        dn = _clean_display_name("", user["email"])
    ghost = bool(_flag_to_int(user["ghost_mode"])) if "ghost_mode" in user.keys() and user["ghost_mode"] is not None else False
    map_identity_mode = (user["map_identity_mode"] or "").strip().lower() if "map_identity_mode" in user.keys() and user["map_identity_mode"] is not None else "name"
    if map_identity_mode not in ALLOWED_MAP_IDENTITY_MODES:
        map_identity_mode = "name"

    best_badge = get_best_current_badge_for_user(int(user["id"]))

    return {
        "ok": True,
        "id": int(user["id"]),
        "email": user["email"],
        "display_name": dn,
        "avatar_url": user["avatar_url"] if "avatar_url" in user.keys() else None,
        "map_identity_mode": map_identity_mode,
        "ghost_mode": ghost,
        "is_admin": bool(_flag_to_int(user["is_admin"])),
        "trial_expires_at": int(user["trial_expires_at"]),
        "leaderboard_badge_code": best_badge.get("leaderboard_badge_code"),
    }


@app.get("/drivers/{user_id}/profile")
def driver_profile(user_id: int, viewer: sqlite3.Row = Depends(require_user)):
    _ = viewer
    target = _db_query_one(
        "SELECT id, email, display_name, avatar_url, is_disabled FROM users WHERE id=? LIMIT 1",
        (int(user_id),),
    )
    if not target or _flag_to_int(target["is_disabled"]) == 1:
        raise HTTPException(status_code=404, detail="Driver not found")

    target_user_id = int(target["id"])
    display_name = _clean_display_name(target["display_name"] or "", target["email"])

    overview = get_overview_for_user(target_user_id) or {}
    daily = overview.get("daily") or {}
    weekly = overview.get("weekly") or {}
    monthly = overview.get("monthly") or {}
    yearly = overview.get("yearly") or {}
    miles_rank_data = get_my_rank(target_user_id, LeaderboardMetric.miles, LeaderboardPeriod.daily)
    hours_rank_data = get_my_rank(target_user_id, LeaderboardMetric.hours, LeaderboardPeriod.daily)
    best_badge = get_best_current_badge_for_user(target_user_id)
    lifetime_totals = get_lifetime_totals_for_user(target_user_id)
    progression = get_level_progress_from_lifetime_miles(lifetime_totals.get("miles", 0.0))

    miles_rank = miles_rank_data.get("row", {}).get("rank_position") if miles_rank_data.get("row") else None
    hours_rank = hours_rank_data.get("row", {}).get("rank_position") if hours_rank_data.get("row") else None

    return {
        "ok": True,
        "user": {
            "id": target_user_id,
            "display_name": display_name,
            "avatar_url": target["avatar_url"] if target["avatar_url"] else None,
            "leaderboard_badge_code": best_badge.get("leaderboard_badge_code"),
        },
        "daily": {
            "miles": daily.get("miles", 0),
            "hours": daily.get("hours", 0),
            "miles_rank": miles_rank,
            "hours_rank": hours_rank,
        },
        "weekly": {
            "miles": weekly.get("miles", 0),
            "hours": weekly.get("hours", 0),
        },
        "monthly": {
            "miles": monthly.get("miles", 0),
            "hours": monthly.get("hours", 0),
        },
        "yearly": {
            "miles": yearly.get("miles", 0),
            "hours": yearly.get("hours", 0),
        },
        "progression": progression,
    }


class MeUpdatePayload(BaseModel):
    display_name: Optional[str] = None
    ghost_mode: Optional[bool] = None
    avatar_url: Optional[str] = None
    map_identity_mode: Optional[str] = None


class ChangePasswordPayload(BaseModel):
    old_password: str
    new_password: str


@app.post("/me/update")
def me_update(payload: MeUpdatePayload, user: sqlite3.Row = Depends(require_user)):
    # optional endpoint (safe): update username and/or ghost mode
    new_dn = None
    if payload.display_name is not None:
        new_dn = _clean_display_name(payload.display_name, user["email"])

    new_ghost = None
    if payload.ghost_mode is not None:
        ghost_is_bool = _is_bool_column("users", "ghost_mode")
        new_ghost = bool(payload.ghost_mode) if ghost_is_bool else (1 if bool(payload.ghost_mode) else 0)

    fields_set = payload.__fields_set__ if hasattr(payload, "__fields_set__") else set()
    update_avatar = "avatar_url" in fields_set
    new_avatar = _normalize_avatar_url(payload.avatar_url) if update_avatar else None

    update_map_identity_mode = "map_identity_mode" in fields_set
    new_map_identity_mode = _normalize_map_identity_mode(payload.map_identity_mode) if update_map_identity_mode else None

    if new_dn is None and new_ghost is None and not update_avatar and not update_map_identity_mode:
        return {"ok": True, "updated": False}

    updates: List[str] = []
    args: List[Any] = []
    if new_dn is not None:
        updates.append("display_name=?")
        args.append(new_dn)
    if new_ghost is not None:
        updates.append("ghost_mode=?")
        args.append(new_ghost)
    if update_avatar:
        updates.append("avatar_url=?")
        args.append(new_avatar)
    if update_map_identity_mode:
        updates.append("map_identity_mode=?")
        args.append(new_map_identity_mode)

    if updates:
        args.append(int(user["id"]))
        _db_exec(f"UPDATE users SET {', '.join(updates)} WHERE id=?", tuple(args))

    row = _db_query_one("SELECT id, email, display_name, ghost_mode, avatar_url, map_identity_mode, is_admin, trial_expires_at FROM users WHERE id=? LIMIT 1", (int(user["id"]),))
    if not row:
        return {"ok": True, "updated": True}

    map_identity_mode = (row["map_identity_mode"] or "").strip().lower() if row["map_identity_mode"] is not None else "name"
    if map_identity_mode not in ALLOWED_MAP_IDENTITY_MODES:
        map_identity_mode = "name"

    return {
        "ok": True,
        "updated": True,
        "id": int(row["id"]),
        "email": row["email"],
        "display_name": (row["display_name"] or _clean_display_name("", row["email"])),
        "avatar_url": row["avatar_url"],
        "map_identity_mode": map_identity_mode,
        "ghost_mode": bool(_flag_to_int(row["ghost_mode"])) if row["ghost_mode"] is not None else False,
    }


@app.post("/me/change_password")
async def change_password(payload: ChangePasswordPayload, user: dict = Depends(require_user)):
    # Look up current salt and hash for this user
    row = _db_query_one("SELECT pass_salt, pass_hash FROM users WHERE id=?", (user["id"],))
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    # Trim stored values and verify current password
    salt = (row["pass_salt"] or "").strip()
    stored_hash = (row["pass_hash"] or "").strip()
    _, check = _hash_password(payload.old_password, salt_b64=salt)
    if not hmac.compare_digest(check, stored_hash):
        raise HTTPException(status_code=401, detail="Incorrect current password")
    # Update to new salt and hash
    new_salt, new_hash = _hash_password(payload.new_password)
    _db_exec("UPDATE users SET pass_salt=?, pass_hash=? WHERE id=?", (new_salt, new_hash, user["id"]))
    return {"ok": True}


@app.post("/me/delete_account")
async def delete_account(user: dict = Depends(require_user)):
    uid = user["id"]
    # Remove related data
    _db_exec("DELETE FROM presence WHERE user_id=?", (uid,))
    _db_exec("DELETE FROM chat_messages WHERE user_id=?", (uid,))
    _db_exec("DELETE FROM events WHERE user_id=?", (uid,))
    # Finally remove the user
    _db_exec("DELETE FROM users WHERE id=?", (uid,))
    return {"ok": True}


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
    record_presence_heartbeat(int(user["id"]), float(payload.lat), float(payload.lng), payload.heading)
    return {"ok": True}


@app.get("/presence/all")
def presence_all(
    max_age_sec: int = PRESENCE_STALE_SECONDS,
    viewer: sqlite3.Row = Depends(require_user),  # REQUIRE AUTH (frontend already sends token)
):
    cutoff = int(time.time()) - max(5, min(3600, int(max_age_sec)))
    # Filter out ghost_mode enabled users.
    rows = _db_query_all(
        """
        SELECT
          p.user_id,
          u.email,
          u.display_name,
          u.avatar_url,
          u.map_identity_mode,
          u.ghost_mode,
          p.lat,
          p.lng,
          p.heading,
          p.accuracy,
          p.updated_at
        FROM presence p
        LEFT JOIN users u ON u.id = p.user_id
        WHERE p.updated_at >= ?
          AND (u.ghost_mode IS NULL OR CAST(u.ghost_mode AS INTEGER) = 0)
        """,
        (cutoff,),
    )

    badge_by_user = get_best_current_badges_for_users([int(r["user_id"]) for r in rows])

    items: List[Dict[str, Any]] = []
    for r in rows:
        best_badge = badge_by_user.get(int(r["user_id"]), {})
        email = (r["email"] or "").strip()
        dn = (r["display_name"] or "").strip()
        if not dn:
            dn = _clean_display_name("", email or "Driver")

        items.append(
            {
                "user_id": int(r["user_id"]),
                "email": email,
                "display_name": dn,
                "avatar_url": r["avatar_url"],
                "map_identity_mode": (str(r["map_identity_mode"]).strip().lower() if r["map_identity_mode"] is not None and str(r["map_identity_mode"]).strip().lower() in ALLOWED_MAP_IDENTITY_MODES else "name"),
                "lat": float(r["lat"]),
                "lng": float(r["lng"]),
                "heading": float(r["heading"]) if r["heading"] is not None else None,
                "accuracy": float(r["accuracy"]) if r["accuracy"] is not None else None,
                "updated_at": int(r["updated_at"]),
                "updated_at_unix": int(r["updated_at"]),
                "leaderboard_badge_code": best_badge.get("leaderboard_badge_code"),
            }
        )

    return {"ok": True, "count": len(items), "items": items}


@app.get("/presence/summary")
def presence_summary(
    max_age_sec: int = PRESENCE_STALE_SECONDS,
    viewer: sqlite3.Row = Depends(require_user),  # REQUIRE AUTH (same as /presence/all)
):
    cutoff = int(time.time()) - max(5, min(3600, int(max_age_sec)))

    counts = _db_query_one(
        """
        SELECT
          COUNT(*) AS online_count,
          SUM(CASE WHEN COALESCE(u.ghost_mode, FALSE) THEN 1 ELSE 0 END) AS ghosted_count
        FROM presence p
        LEFT JOIN users u ON u.id = p.user_id
        WHERE p.updated_at >= ?
        """,
        (cutoff,),
    )

    online_count = int(counts["online_count"] or 0) if counts else 0
    ghosted_count = int(counts["ghosted_count"] or 0) if counts else 0

    return {
        "ok": True,
        "online_count": online_count,
        "ghosted_count": ghosted_count,
        "visible_count": max(0, online_count - ghosted_count),
    }


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
    zone_name: Optional[str] = None
    borough: Optional[str] = None
    frame_time: Optional[str] = None


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
                INSERT INTO chat_messages(room, user_id, display_name, message, created_at)
                VALUES (?, ?, ?, ?, ?)
            """
            if DB_BACKEND == "postgres":
                cur.execute(_sql(insert_sql + " RETURNING id"), ("global", int(user["id"]), display_name, message, now))
                row = cur.fetchone()
                new_id = int(row["id"])
            else:
                cur.execute(_sql(insert_sql), ("global", int(user["id"]), display_name, message, now))
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
        WHERE room = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        ("global", safe_limit),
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
        WHERE room = ? AND id > ?
        ORDER BY id ASC
        LIMIT ?
        """,
        ("global", safe_after_id, safe_limit),
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
    zone_name = (payload.zone_name or "").strip() or None
    borough = (payload.borough or "").strip() or None
    frame_time = (payload.frame_time or "").strip() or None

    _db_exec(
        """
        INSERT INTO pickup_logs(user_id, lat, lng, zone_id, zone_name, borough, frame_time, created_at)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (int(user["id"]), float(payload.lat), float(payload.lng), payload.zone_id, zone_name, borough, frame_time, now),
    )

    _db_exec(
        """
        INSERT INTO events(type, user_id, lat, lng, text, zone_id, created_at, expires_at)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        ("pickup", int(user["id"]), float(payload.lat), float(payload.lng), "", payload.zone_id, now, expires),
    )
    increment_pickup_count(int(user["id"]))
    return {"ok": True}




def _load_pickup_zone_geometries() -> Dict[int, Dict[str, Any]]:
    global _pickup_zone_geom_cache, _pickup_zone_geom_cache_mtime
    zones_path = DATA_DIR / "taxi_zones.geojson"
    try:
        mtime = zones_path.stat().st_mtime
    except Exception:
        print("[warn] taxi_zones.geojson not available for pickup hotspots")
        _pickup_zone_geom_cache = {}
        _pickup_zone_geom_cache_mtime = None
        return {}

    if _pickup_zone_geom_cache is not None and _pickup_zone_geom_cache_mtime == mtime:
        return _pickup_zone_geom_cache

    parsed: Dict[int, Dict[str, Any]] = {}
    try:
        raw = json.loads(zones_path.read_text(encoding="utf-8"))
        for feature in raw.get("features", []):
            props = feature.get("properties", {}) or {}
            geom_data = feature.get("geometry")
            if not geom_data:
                continue
            try:
                zone_id = int(props.get("LocationID"))
            except Exception:
                continue
            try:
                geom = shape(geom_data)
            except Exception:
                continue
            if geom.is_empty:
                continue
            if not isinstance(geom, (Polygon, MultiPolygon)):
                continue
            parsed[zone_id] = {
                "zone_name": (props.get("zone") or "").strip(),
                "borough": (props.get("borough") or "").strip(),
                "geometry": geom,
            }
    except Exception:
        print("[warn] Failed to parse taxi_zones.geojson for pickup hotspots", traceback.format_exc())
        parsed = {}

    _pickup_zone_geom_cache = parsed
    _pickup_zone_geom_cache_mtime = mtime
    return parsed


def _pickup_zone_recent_points(
    zone_ids: List[int], max_points_per_zone: int = PICKUP_ZONE_HOTSPOT_MAX_POINTS
) -> Dict[int, List[Dict[str, Any]]]:
    clean_zone_ids: List[int] = []
    for z in zone_ids:
        try:
            clean_zone_ids.append(int(z))
        except Exception:
            continue
    if not clean_zone_ids:
        return {}

    cap = max(1, min(PICKUP_ZONE_HOTSPOT_MAX_POINTS, int(max_points_per_zone)))
    clean_zone_ids = list(dict.fromkeys(clean_zone_ids))[:256]
    placeholders = ",".join(["?"] * len(clean_zone_ids))

    sql = f"""
        WITH ranked AS (
            SELECT
                id,
                zone_id,
                zone_name,
                borough,
                user_id,
                lat,
                lng,
                created_at,
                ROW_NUMBER() OVER (PARTITION BY zone_id ORDER BY created_at DESC, id DESC) AS rn
            FROM pickup_logs
            WHERE zone_id IN ({placeholders})
        )
        SELECT id, zone_id, zone_name, borough, user_id, lat, lng, created_at
        FROM ranked
        WHERE rn <= ?
        ORDER BY zone_id ASC, created_at DESC, id DESC
    """
    rows = _db_query_all(sql, tuple(clean_zone_ids + [cap]))
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        item = dict(row)
        zid = item.get("zone_id")
        if zid is None:
            continue
        grouped[int(zid)].append(item)
    return dict(grouped)


def _pickup_zone_signature(point_rows: List[Dict[str, Any]]) -> str:
    if not point_rows:
        return hashlib.sha1(b"0||0").hexdigest()
    ids: List[str] = []
    for row in point_rows:
        try:
            ids.append(str(int(row.get("id"))))
        except Exception:
            ids.append("0")
    latest_created_at = int(point_rows[0].get("created_at") or 0)
    payload = f"{len(point_rows)}|{','.join(ids)}|{latest_created_at}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _build_pickup_zone_hotspot_feature(
    zone_id: int, zone_meta: Dict[str, Any], point_rows: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if len(point_rows) < PICKUP_ZONE_HOTSPOT_MIN_POINTS:
        return None

    zone_geom = zone_meta.get("geometry")
    if zone_geom is None or zone_geom.is_empty:
        return None

    try:
        zone_proj = transform(_to_3857.transform, zone_geom)
    except Exception:
        return None
    if zone_proj.is_empty:
        return None

    point_entries: List[Dict[str, Any]] = []
    for idx, row in enumerate(point_rows):
        try:
            lng = float(row["lng"])
            lat = float(row["lat"])
            x, y = _to_3857.transform(lng, lat)
        except Exception:
            continue
        n = len(point_rows)
        if n <= 1:
            recency_weight = 1.0
        else:
            recency_weight = 1.0 - 0.65 * (idx / (n - 1))
        point_entries.append(
            {
                "x": x,
                "y": y,
                "lat": lat,
                "lng": lng,
                "weight": recency_weight,
                "created_at": int(row.get("created_at") or 0),
            }
        )
    if len(point_entries) < PICKUP_ZONE_HOTSPOT_MIN_POINTS:
        return None

    sigma = float(PICKUP_ZONE_HOTSPOT_SIGMA_M)
    radius = float(PICKUP_ZONE_HOTSPOT_RADIUS_M)
    radius_sq = radius * radius
    cell_size = float(PICKUP_ZONE_HOTSPOT_CELL_SIZE_M)

    minx, miny, maxx, maxy = zone_proj.bounds
    start_x = minx + (cell_size / 2.0)
    start_y = miny + (cell_size / 2.0)
    cols = max(1, int(math.ceil((maxx - minx) / cell_size)))
    rows_n = max(1, int(math.ceil((maxy - miny) / cell_size)))

    cell_scores: Dict[Tuple[int, int], float] = {}
    peak_score = 0.0
    peak_key: Optional[Tuple[int, int]] = None

    for gy in range(rows_n):
        cy = start_y + gy * cell_size
        for gx in range(cols):
            cx = start_x + gx * cell_size
            center = Point(cx, cy)
            if not zone_proj.covers(center):
                continue
            score = 0.0
            for pe in point_entries:
                dx = cx - pe["x"]
                dy = cy - pe["y"]
                dist_sq = (dx * dx) + (dy * dy)
                if dist_sq > radius_sq:
                    continue
                distance_weight = math.exp(-(dist_sq) / (2.0 * sigma * sigma))
                score += pe["weight"] * distance_weight
            if score <= 0.0:
                continue
            key = (gx, gy)
            cell_scores[key] = score
            if score > peak_score:
                peak_score = score
                peak_key = key

    if peak_key is None or peak_score <= 0.0:
        return None

    threshold = peak_score * 0.60
    selected = {k for k, v in cell_scores.items() if v >= threshold}
    if peak_key not in selected:
        selected.add(peak_key)

    connected: set[Tuple[int, int]] = set()
    q = deque([peak_key])
    while q:
        cur = q.popleft()
        if cur in connected or cur not in selected:
            continue
        connected.add(cur)
        cx, cy = cur
        for nx in (cx - 1, cx, cx + 1):
            for ny in (cy - 1, cy, cy + 1):
                nkey = (nx, ny)
                if nkey != cur and nkey in selected and nkey not in connected:
                    q.append(nkey)

    if not connected:
        return None

    cell_polys: List[Polygon] = []
    half = cell_size / 2.0
    for gx, gy in connected:
        cx = start_x + gx * cell_size
        cy = start_y + gy * cell_size
        cell_polys.append(
            Polygon(
                [
                    (cx - half, cy - half),
                    (cx + half, cy - half),
                    (cx + half, cy + half),
                    (cx - half, cy + half),
                ]
            )
        )

    hotspot_proj = unary_union(cell_polys)
    hotspot_proj = hotspot_proj.buffer(35).buffer(-18)
    clipped_proj = hotspot_proj.intersection(zone_proj)

    if clipped_proj.is_empty:
        px = start_x + peak_key[0] * cell_size
        py = start_y + peak_key[1] * cell_size
        clipped_proj = Point(px, py).buffer(160).intersection(zone_proj)

    if clipped_proj.is_empty:
        return None

    pre_simplified = clipped_proj
    simplified = clipped_proj.simplify(PICKUP_ZONE_HOTSPOT_SIMPLIFY_M, preserve_topology=True)
    if not simplified.is_empty:
        simplified = simplified.intersection(zone_proj)
        if not simplified.is_empty:
            clipped_proj = simplified
        else:
            clipped_proj = pre_simplified
    else:
        clipped_proj = pre_simplified

    hotspot_ll = transform(_to_4326.transform, clipped_proj)
    if hotspot_ll.is_empty:
        return None

    sample_size = len(point_entries)
    avg_lat = sum(p["lat"] for p in point_entries) / sample_size
    avg_lng = sum(p["lng"] for p in point_entries) / sample_size
    latest_created_at = max(p["created_at"] for p in point_entries)
    intensity = min(1.0, max(0.25, peak_score / max(1.0, 0.55 * sample_size)))
    signature = _pickup_zone_signature(point_rows)

    return {
        "type": "Feature",
        "geometry": mapping(hotspot_ll),
        "properties": {
            "zone_id": zone_id,
            "zone_name": zone_meta.get("zone_name") or (point_rows[0].get("zone_name") or ""),
            "borough": zone_meta.get("borough") or (point_rows[0].get("borough") or ""),
            "sample_size": sample_size,
            "max_points_per_zone": PICKUP_ZONE_HOTSPOT_MAX_POINTS,
            "min_points": PICKUP_ZONE_HOTSPOT_MIN_POINTS,
            "peak_score": peak_score,
            "threshold_score": threshold,
            "avg_lat": avg_lat,
            "avg_lng": avg_lng,
            "latest_created_at": latest_created_at,
            "intensity": intensity,
            "hotspot_method": "recency_weighted_density_grid",
            "signature": signature,
        },
    }


def _build_fallback_pickup_zone_hotspot_feature(
    zone_id: int,
    zone_meta: Dict[str, Any],
    point_rows: List[Dict[str, Any]],
    micro_payload: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not micro_payload:
        return None

    zone_geom = zone_meta.get("geometry")
    if zone_geom is None or zone_geom.is_empty:
        return None

    strongest = micro_payload[0] if isinstance(micro_payload[0], dict) else None
    if not strongest:
        return None

    try:
        center_lat = float(strongest["center_lat"])
        center_lng = float(strongest["center_lng"])
        raw_radius_m = float(strongest.get("radius_m") or 0.0)
    except Exception:
        return None
    radius_m = min(180.0, max(80.0, raw_radius_m if raw_radius_m > 0 else 120.0))

    try:
        zone_proj = transform(_to_3857.transform, zone_geom)
    except Exception:
        return None
    if zone_proj.is_empty:
        return None

    center_x, center_y = _to_3857.transform(center_lng, center_lat)
    fallback_proj = Point(center_x, center_y).buffer(radius_m).intersection(zone_proj)
    if fallback_proj.is_empty:
        return None

    simplified = fallback_proj.simplify(max(6.0, PICKUP_ZONE_HOTSPOT_SIMPLIFY_M / 2.0), preserve_topology=True)
    if not simplified.is_empty:
        simplified = simplified.intersection(zone_proj)
        if not simplified.is_empty:
            fallback_proj = simplified

    hotspot_ll = transform(_to_4326.transform, fallback_proj)
    if hotspot_ll.is_empty:
        return None

    latest_created_at = 0
    if point_rows:
        latest_created_at = max(int(r.get("created_at") or 0) for r in point_rows)

    return {
        "type": "Feature",
        "geometry": mapping(hotspot_ll),
        "properties": {
            "zone_id": zone_id,
            "zone_name": zone_meta.get("zone_name") or ((point_rows[0].get("zone_name") if point_rows else "") or ""),
            "borough": zone_meta.get("borough") or ((point_rows[0].get("borough") if point_rows else "") or ""),
            "sample_size": len(point_rows),
            "max_points_per_zone": PICKUP_ZONE_HOTSPOT_MAX_POINTS,
            "min_points": PICKUP_ZONE_HOTSPOT_MIN_POINTS,
            "latest_created_at": latest_created_at,
            "intensity": float(strongest.get("intensity") or 0.0),
            "hotspot_method": "micro_hotspot_fallback_clip",
            "signature": _pickup_zone_signature(point_rows),
        },
    }


def _current_timeslot_bin(now_ts: int, bin_minutes: int = HOTSPOT_TIMESLOT_BIN_MINUTES) -> int:
    dt = time.gmtime(now_ts)
    return int((dt.tm_hour * 60 + dt.tm_min) // max(1, int(bin_minutes)))


def _active_visible_driver_count() -> int:
    cutoff = int(time.time()) - max(30, PRESENCE_STALE_SECONDS)
    row = _db_query_one(
        """
        SELECT COUNT(*) AS c
        FROM presence p
        LEFT JOIN users u ON u.id = p.user_id
        WHERE p.updated_at >= ?
          AND (u.ghost_mode IS NULL OR CAST(u.ghost_mode AS INTEGER) = 0)
        """,
        (cutoff,),
    )
    return int(row["c"] or 0) if row else 0


def _pickup_zone_same_timeslot_support(zone_ids: List[int], now_ts: int) -> Dict[int, float]:
    if not zone_ids:
        return {}
    slot = _current_timeslot_bin(now_ts)
    lookback = now_ts - (14 * 24 * 3600)
    placeholders = ",".join(["?"] * len(zone_ids))
    sql = f"""
        SELECT zone_id, COUNT(*) AS c
        FROM pickup_logs
        WHERE zone_id IN ({placeholders})
          AND created_at >= ?
          AND CAST(((created_at % 86400) / 60) / ? AS INTEGER) = ?
        GROUP BY zone_id
    """
    params = tuple(list(zone_ids) + [lookback, HOTSPOT_TIMESLOT_BIN_MINUTES, slot])
    rows = _db_query_all(sql, params)
    out: Dict[int, float] = {int(z): 0.0 for z in zone_ids}
    for r in rows:
        out[int(r["zone_id"])] = float(r["c"])
    return out


def _pickup_zone_historical_support(zone_ids: List[int], now_ts: int) -> Dict[int, float]:
    if not zone_ids:
        return {}
    lookback = now_ts - (14 * 24 * 3600)
    placeholders = ",".join(["?"] * len(zone_ids))
    rows = _db_query_all(
        f"""
        SELECT zone_id, COUNT(*) AS c
        FROM pickup_logs
        WHERE zone_id IN ({placeholders})
          AND created_at >= ?
        GROUP BY zone_id
        """,
        tuple(list(zone_ids) + [lookback]),
    )
    out: Dict[int, float] = {int(z): 0.0 for z in zone_ids}
    for r in rows:
        out[int(r["zone_id"])] = float(r["c"])
    return out


def _pickup_zone_density_penalty(zone_ids: List[int]) -> Dict[int, float]:
    if not zone_ids:
        return {}
    cutoff = int(time.time()) - max(30, PRESENCE_STALE_SECONDS)
    placeholders = ",".join(["?"] * len(zone_ids))
    rows = _db_query_all(
        f"""
        SELECT pl.zone_id, COUNT(DISTINCT p.user_id) AS c
        FROM presence p
        LEFT JOIN pickup_logs pl ON pl.user_id = p.user_id
        WHERE p.updated_at >= ?
          AND pl.zone_id IN ({placeholders})
          AND pl.created_at >= ?
        GROUP BY pl.zone_id
        """,
        tuple([cutoff] + list(zone_ids) + [cutoff - 3600]),
    )
    out: Dict[int, float] = {int(z): 0.0 for z in zone_ids}
    for r in rows:
        out[int(r["zone_id"])] = float(r["c"])
    return out


def _build_zone_micro_hotspots_payload(
    zone_id: int,
    point_rows: List[Dict[str, Any]],
    historical_support: float,
    same_timeslot_support: float,
    density_penalty: float,
    now_ts: int,
) -> List[Dict[str, Any]]:
    clusters = score_micro_hotspots(
        now_ts=now_ts,
        zone_id=zone_id,
        point_rows=point_rows,
        historical_zone_support=historical_support,
        same_timeslot_support=same_timeslot_support,
        density_penalty=density_penalty,
        top_n=3,
    )
    if clusters:
        log_micro_bins(_db_exec, bin_time=now_ts, rows=clusters)
    payload: List[Dict[str, Any]] = []
    for c in clusters:
        payload.append(
            {
                "cluster_id": c.cluster_id,
                "zone_id": c.zone_id,
                "center_lat": c.center_lat,
                "center_lng": c.center_lng,
                "radius_m": c.radius_m,
                "intensity": c.intensity,
                "confidence": c.confidence,
                "weighted_trip_count": c.weighted_trip_count,
                "event_count": int(getattr(c, "event_count", 0)),
                "unique_driver_count": c.unique_driver_count,
                "crowding_penalty": c.crowding_penalty,
                "recommended": c.recommended,
            }
        )
    return payload


def _pickup_zone_hotspots(zone_ids: List[int]) -> Dict[str, Any]:
    empty = {"type": "FeatureCollection", "features": [], "orphan_micro_hotspots": []}
    try:
        now_ts = int(time.time())
        clean_zone_ids: List[int] = []
        for z in zone_ids:
            try:
                clean_zone_ids.append(int(z))
            except Exception:
                continue
        if not clean_zone_ids:
            return empty

        zone_geoms = _load_pickup_zone_geometries()
        if not zone_geoms:
            return empty

        zone_points = _pickup_zone_recent_points(clean_zone_ids, PICKUP_ZONE_HOTSPOT_MAX_POINTS)
        historical_support = _pickup_zone_historical_support(clean_zone_ids, now_ts)
        same_timeslot_support = _pickup_zone_same_timeslot_support(clean_zone_ids, now_ts)
        density_penalty_by_zone = _pickup_zone_density_penalty(clean_zone_ids)
        active_driver_count = _active_visible_driver_count()

        zone_scores = score_zones(
            now_ts=now_ts,
            zone_points=zone_points,
            historical_by_zone=historical_support,
            same_timeslot_by_zone=same_timeslot_support,
            density_by_zone=density_penalty_by_zone,
            active_driver_count=active_driver_count,
            previous_scores=_pickup_zone_score_cache,
        )

        log_zone_bins(_db_exec, bin_time=now_ts, rows=zone_scores.values())
        prune_experiment_tables(_db_exec, now_ts=now_ts)

        features: List[Dict[str, Any]] = []
        orphan_micro_hotspots: List[Dict[str, Any]] = []
        requested_zone_ids = set(clean_zone_ids)
        for zone_id in clean_zone_ids:
            pts = zone_points.get(zone_id, [])
            zone_data = zone_geoms.get(zone_id)
            if not zone_data:
                continue

            micro_payload = _build_zone_micro_hotspots_payload(
                zone_id,
                pts,
                historical_support.get(zone_id, 0.0),
                same_timeslot_support.get(zone_id, 0.0),
                density_penalty_by_zone.get(zone_id, 0.0),
                now_ts,
            )

            signature = _pickup_zone_signature(pts)
            with _pickup_zone_hotspot_cache_lock:
                cached = _pickup_zone_hotspot_feature_cache.get(zone_id)

            feature = None
            if cached and cached.get("signature") == signature and cached.get("feature"):
                feature = cached["feature"]
            elif len(pts) >= PICKUP_ZONE_HOTSPOT_MIN_POINTS:
                try:
                    feature = _build_pickup_zone_hotspot_feature(zone_id, zone_data, pts)
                except Exception:
                    print(f"[warn] Failed to generate pickup zone hotspot for zone {zone_id}", traceback.format_exc())
                    feature = None

            if not feature:
                # Fallback hotspot polygons are for qualified zones only.
                if len(pts) >= PICKUP_ZONE_HOTSPOT_MIN_POINTS:
                    feature = _build_fallback_pickup_zone_hotspot_feature(zone_id, zone_data, pts, micro_payload)
            if not feature:
                # Micro-hotspots must still surface even if polygon generation fails.
                if micro_payload:
                    orphan_micro_hotspots.extend([item for item in micro_payload if isinstance(item, dict)])
                with _pickup_zone_hotspot_cache_lock:
                    _pickup_zone_hotspot_feature_cache.pop(zone_id, None)
                continue

            score = zone_scores.get(zone_id)

            props = feature.setdefault("properties", {})
            props["signature"] = signature
            if score is not None:
                _pickup_zone_score_cache[zone_id] = score.final_score
                props["hotspot_score"] = score.final_score
                props["final_score"] = score.final_score
                props["confidence"] = score.confidence
                props["live_strength"] = score.live_strength
                props["density_penalty"] = score.density_penalty
                props["weighted_trip_count"] = score.weighted_trip_count
                props["unique_driver_count"] = score.unique_driver_count
                props["recommended"] = score.recommended
                if score.recommended:
                    log_recommendation_outcome(
                        _db_exec,
                        recommended_at=now_ts,
                        zone_id=zone_id,
                        score=score.final_score,
                        confidence=score.confidence,
                        cluster_id=None,
                    )
            props["micro_hotspots"] = micro_payload

            with _pickup_zone_hotspot_cache_lock:
                _pickup_zone_hotspot_feature_cache[zone_id] = {
                    "signature": signature,
                    "feature": feature,
                }
            features.append(feature)

        with _pickup_zone_hotspot_cache_lock:
            stale_zone_ids = [zid for zid in list(_pickup_zone_hotspot_feature_cache.keys()) if zid not in requested_zone_ids]
            for zid in stale_zone_ids:
                _pickup_zone_hotspot_feature_cache.pop(zid, None)

        # Preserve orphan micro-hotspots for frontend compatibility.
        return {"type": "FeatureCollection", "features": features, "orphan_micro_hotspots": orphan_micro_hotspots}
    except Exception:
        print("[warn] Failed to generate pickup zone hotspots", traceback.format_exc())
        return empty


def _pickup_zone_stats(zone_ids: List[int], sample_limit: int = 100) -> List[Dict[str, Any]]:
    clean_zone_ids: List[int] = []
    for z in zone_ids:
        try:
            clean_zone_ids.append(int(z))
        except Exception:
            continue

    if not clean_zone_ids:
        return []

    safe_sample_limit = max(1, min(100, int(sample_limit)))
    clean_zone_ids = clean_zone_ids[:256]
    placeholders = ",".join(["?"] * len(clean_zone_ids))

    sql = f"""
        WITH ranked AS (
            SELECT
                zone_id,
                zone_name,
                borough,
                user_id,
                lat,
                lng,
                created_at,
                ROW_NUMBER() OVER (PARTITION BY zone_id ORDER BY created_at DESC, id DESC) AS rn
            FROM pickup_logs
            WHERE zone_id IN ({placeholders})
        )
        SELECT
            zone_id,
            MAX(COALESCE(zone_name, '')) AS zone_name,
            MAX(COALESCE(borough, '')) AS borough,
            COUNT(*) AS sample_size,
            AVG(lat) AS avg_lat,
            AVG(lng) AS avg_lng,
            MAX(created_at) AS latest_created_at
        FROM ranked
        WHERE rn <= ?
        GROUP BY zone_id
    """

    rows = _db_query_all(sql, tuple(clean_zone_ids + [safe_sample_limit]))
    stats: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["sample_limit"] = safe_sample_limit
        stats.append(item)
    return stats


def _flatten_zone_micro_hotspots(zone_hotspots: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Preserve nested micro_hotspots for per-zone context while also exposing a top-level
    # compatibility payload expected by existing frontend clients.
    flattened: List[Dict[str, Any]] = []
    features = zone_hotspots.get("features") if isinstance(zone_hotspots, dict) else None
    if isinstance(features, list):
        for feature in features:
            if not isinstance(feature, dict):
                continue
            props = feature.get("properties")
            if not isinstance(props, dict):
                continue
            micro_hotspots = props.get("micro_hotspots")
            if not isinstance(micro_hotspots, list):
                continue
            flattened.extend([item for item in micro_hotspots if isinstance(item, dict)])
    orphan = zone_hotspots.get("orphan_micro_hotspots") if isinstance(zone_hotspots, dict) else None
    if isinstance(orphan, list):
        flattened.extend([item for item in orphan if isinstance(item, dict)])
    return flattened


@app.get("/events/pickups/recent")
def get_recent_pickups(
    limit: int = 30,
    zone_sample_limit: int = 100,
    zone_id: Optional[int] = None,
    min_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lat: Optional[float] = None,
    max_lng: Optional[float] = None,
    viewer: sqlite3.Row = Depends(require_user),
):
    safe_limit = max(1, min(200, int(limit)))
    safe_zone_sample_limit = max(1, min(100, int(zone_sample_limit)))
    sql = """
        SELECT id, lat, lng, zone_id, zone_name, borough, frame_time, created_at
        FROM pickup_logs
        WHERE 1=1
    """
    params: List[Any] = []

    if zone_id is not None:
        sql += " AND zone_id = ?"
        params.append(int(zone_id))

    bbox = [min_lat, min_lng, max_lat, max_lng]
    if all(v is not None for v in bbox):
        lo_lat = min(float(min_lat), float(max_lat))
        hi_lat = max(float(min_lat), float(max_lat))
        lo_lng = min(float(min_lng), float(max_lng))
        hi_lng = max(float(min_lng), float(max_lng))
        sql += " AND lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?"
        params.extend([lo_lat, hi_lat, lo_lng, hi_lng])

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(safe_limit)

    rows = _db_query_all(sql, tuple(params))
    items = [dict(r) for r in rows]

    zone_ids_for_stats: List[int] = []
    if zone_id is not None:
        zone_ids_for_stats = [int(zone_id)]
    else:
        stats_sql = "SELECT DISTINCT zone_id FROM pickup_logs WHERE zone_id IS NOT NULL"
        stats_params: List[Any] = []
        if all(v is not None for v in bbox):
            lo_lat = min(float(min_lat), float(max_lat))
            hi_lat = max(float(min_lat), float(max_lat))
            lo_lng = min(float(min_lng), float(max_lng))
            hi_lng = max(float(min_lng), float(max_lng))
            stats_sql += " AND lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?"
            stats_params.extend([lo_lat, hi_lat, lo_lng, hi_lng])
        stats_rows = _db_query_all(stats_sql, tuple(stats_params))
        zone_ids_for_stats = [int(dict(r)["zone_id"]) for r in stats_rows if dict(r).get("zone_id") is not None]

    zone_stats = _pickup_zone_stats(zone_ids_for_stats, sample_limit=safe_zone_sample_limit)
    hotspot_zone_ids = [int(z.get("zone_id")) for z in zone_stats if z.get("zone_id") is not None]
    try:
        zone_hotspots = _pickup_zone_hotspots(hotspot_zone_ids)
    except Exception:
        print("[warn] Failed to attach pickup zone hotspots", traceback.format_exc())
        zone_hotspots = {"type": "FeatureCollection", "features": []}
    micro_hotspots = _flatten_zone_micro_hotspots(zone_hotspots)
    zone_features = zone_hotspots.get("features") if isinstance(zone_hotspots, dict) else []
    zone_hotspot_count = len(zone_features) if isinstance(zone_features, list) else 0
    nested_micro_hotspot_count = 0
    orphan_micro_hotspot_count = 0
    if isinstance(zone_features, list):
        for feature in zone_features:
            if not isinstance(feature, dict):
                continue
            props = feature.get("properties")
            if not isinstance(props, dict):
                continue
            nested = props.get("micro_hotspots")
            if isinstance(nested, list):
                nested_micro_hotspot_count += len([item for item in nested if isinstance(item, dict)])
    orphan = zone_hotspots.get("orphan_micro_hotspots") if isinstance(zone_hotspots, dict) else None
    if isinstance(orphan, list):
        orphan_micro_hotspot_count = len([item for item in orphan if isinstance(item, dict)])
    return {
        "ok": True,
        "count": len(items),
        "items": items,
        "zone_stats": zone_stats,
        "zone_hotspots": zone_hotspots,
        "micro_hotspots": micro_hotspots,
        "micro_hotspot_debug": {
            "zone_hotspot_count": zone_hotspot_count,
            "nested_micro_hotspot_count": nested_micro_hotspot_count,
            "orphan_micro_hotspot_count": orphan_micro_hotspot_count,
            "top_level_micro_hotspot_count": len(micro_hotspots),
        },
    }


# =========================================================
# ADMIN (manage all accounts)
# =========================================================
class AdminDisablePayload(BaseModel):
    user_id: int
    disabled: bool


@app.post("/admin/users/disable")
def admin_disable_user(payload: AdminDisablePayload, admin: sqlite3.Row = Depends(require_admin)):
    disabled_is_bool = _is_bool_column("users", "is_disabled")
    disabled_value = bool(payload.disabled) if disabled_is_bool else (1 if payload.disabled else 0)
    _db_exec("UPDATE users SET is_disabled=? WHERE id=?", (disabled_value, int(payload.user_id)))
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
