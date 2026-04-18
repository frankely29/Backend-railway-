from __future__ import annotations

import hmac
import hashlib
import gzip
import json
import math
import os
import errno
import copy
import duckdb
import re
import shutil
import sqlite3
import threading
import time
import traceback
import uuid
from collections import defaultdict, deque
from decimal import Decimal
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from timeline_time_utils import to_frontend_local_iso

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Depends, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pyproj import Transformer
from shapely.geometry import MultiPolygon, Point, Polygon, mapping, shape
from shapely.ops import transform, unary_union
from starlette.middleware.base import BaseHTTPMiddleware
from hotspot_experiments import (
    log_micro_bins,
    log_zone_bins,
    prune_experiment_tables,
)
from pickup_hotspot_intelligence import (
    build_cross_zone_merged_hotspots,
    build_hotspot_quality_modifier,
    build_zone_historical_anchor_components,
    build_zone_historical_anchor_points,
    convert_historical_components_to_emittable_shapes,
    determine_zone_hotspot_limit,
    get_zone_or_hotspot_outcome_modifier,
    sculpt_hotspot_shapes_from_recent_points,
)
from assistant_outlook_engine import (
    HORIZON_BINS_DEFAULT,
    build_assistant_outlook_frame_bucket_from_loader,
    get_assistant_outlook_payload_from_frame_bucket,
)
from driver_guidance_engine import (
    build_driver_guidance,
    load_zone_centroid_lookup,
    load_driver_activity_snapshot,
    resolve_current_zone_from_position,
)
from hotspot_models import MicroHotspotScoreResult
from hotspot_scoring import score_zones
from artifact_freshness import evaluate_artifact_freshness
from exact_history_freshness import (
    active_month_freshness_report,
    load_month_build_meta,
)
from artifact_storage_service import cleanup_artifact_storage, get_artifact_storage_report
from artifact_db_store import (
    delete_generated_artifact,
    ensure_generated_artifact_store_schema,
    generated_artifact_present,
    generated_artifact_report,
    load_generated_artifact,
    load_generated_artifact_metadata,
    save_generated_artifact,
)
from parquet_inventory import inspect_parquet_inventory
from avatar_assets import (
    AVATAR_THUMB_MIME,
    avatar_thumb_path,
    avatar_thumb_url,
    avatar_version_for_data_url,
    normalize_avatar_data_url,
    persist_avatar_thumb,
)
from admin_routes import router as admin_router
from account_runtime import delete_account_runtime_data
from admin_mutation_routes import router as admin_mutation_router
from admin_test_routes import router as admin_test_router
from admin_trips_routes import router as admin_trips_router
from core import (
    _clean_display_name,
    _db,
    _db_exec,
    _db_lock,
    _db_query_all,
    _db_query_one,
    _enforce_user_not_blocked,
    _hash_password,
    _sql,
    DB_BACKEND,
    POSTGRES_POOL_MAX,
    POSTGRES_POOL_MIN,
    _make_token,
    _user_block_state,
    _require_jwt_secret,
    ENFORCE_TRIAL,
    require_user as core_require_user,
)

# =========================================================
# Paths (Railway volume)
# =========================================================
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
FRAMES_DIR = Path(os.environ.get("FRAMES_DIR", str(DATA_DIR / "frames")))
TIMELINE_PATH = FRAMES_DIR / "timeline.json"
ASSISTANT_OUTLOOK_PATH = FRAMES_DIR / "assistant_outlook.json"
EXACT_HISTORY_DIR = DATA_DIR / "exact_history"
EXACT_HISTORY_DB_PATH = EXACT_HISTORY_DIR / "exact_shadow.duckdb"
EXACT_HISTORY_MONTHS_DIR = EXACT_HISTORY_DIR / "months"
EXACT_HISTORY_MONTHS_BUILDING_DIR = EXACT_HISTORY_DIR / "months.__building__"
EXACT_HISTORY_MONTHS_BACKUP_DIR = EXACT_HISTORY_DIR / "months.__backup__"
MONTH_MANIFEST_PATH = EXACT_HISTORY_DIR / "month_manifest.json"
DAY_TENDENCY_DIR = DATA_DIR / "day_tendency"
DAY_TENDENCY_MODEL_PATH = DAY_TENDENCY_DIR / "model.json"
NYC_TZ = ZoneInfo("America/New_York")
DAY_TENDENCY_VERSION = "borough_tendency_v2"
DAY_TENDENCY_CONTEXT_LOCAL_COHORT_WEIGHTS = {
    "borough_weekday_bin": 1.00,
    "borough_bin": 0.72,
    "borough_baseline": 0.46,
}
DAY_TENDENCY_CONTEXT_GLOBAL_COHORT_WEIGHTS = {
    "global_bin": 0.40,
    "global_baseline": 0.22,
}
DAY_TENDENCY_CONTEXT_WEAKNESS_SCORE_START = 60.0
DAY_TENDENCY_CONTEXT_WEAKNESS_SCORE_SPAN = 35.0
DAY_TENDENCY_CONTEXT_GLOBAL_PENALTY_CAP = 3
DAY_TENDENCY_CONTEXT_LOCAL_PENALTY_CAP = 5
DAY_TENDENCY_CONTEXT_TOTAL_PENALTY_CAP = 8
DAY_TENDENCY_CONTEXT_BUCKET_DROP_CAP = 1

DEFAULT_BIN_MINUTES = int(os.environ.get("DEFAULT_BIN_MINUTES", "20"))
DEFAULT_MIN_TRIPS_PER_WINDOW = int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25"))
AUTO_GENERATE_ON_STARTUP = str(os.environ.get("AUTO_GENERATE_ON_STARTUP", "0")).strip().lower() in ("1", "true", "yes", "on")

LOCK_PATH = DATA_DIR / ".generate.lock"


# Auth / Admin config
JWT_SECRET = os.environ.get("JWT_SECRET", "")  # REQUIRED (set in Railway)
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "").strip().lower()
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
ADMIN_BOOTSTRAP_TOKEN = os.environ.get("ADMIN_BOOTSTRAP_TOKEN", "").strip()
DEBUG_VERBOSE_LOGS = str(os.environ.get("DEBUG_VERBOSE_LOGS", "0")).strip().lower() in ("1", "true", "yes", "on")
CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED = (
    str(os.environ.get("CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED", "0"))
    .strip()
    .lower()
    in {"1", "true", "yes", "on"}
)

TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "7"))
TOKEN_TTL_SECONDS = int(os.environ.get("TOKEN_TTL_SECONDS", str(30 * 24 * 3600)))  # 30 days
PRESENCE_STALE_SECONDS = int(os.environ.get("PRESENCE_STALE_SECONDS", "300"))  # 5 min
EVENT_DEFAULT_WINDOW_SECONDS = int(os.environ.get("EVENT_DEFAULT_WINDOW_SECONDS", str(24 * 3600)))  # 24h
MAX_AVATAR_DATA_URL_LENGTH = int(os.environ.get("MAX_AVATAR_DATA_URL_LENGTH", "20000"))
ALLOWED_MAP_IDENTITY_MODES = {"name", "avatar"}
RESPONSE_GZIP_MIN_BYTES = int(os.environ.get("RESPONSE_GZIP_MIN_BYTES", "1024"))
PRESENCE_COMMUNITY_ACCURACY_MAX_METERS = int(
    os.environ.get("PRESENCE_COMMUNITY_ACCURACY_MAX_METERS", "120")
)
PRESENCE_VIEWPORT_CACHE_TTL_SECONDS = float(os.environ.get("PRESENCE_VIEWPORT_CACHE_TTL_SECONDS", "3"))
PRESENCE_VIEWPORT_CACHE_MAX = int(os.environ.get("PRESENCE_VIEWPORT_CACHE_MAX", "128"))
PRESENCE_DELTA_MAX_LIMIT = int(os.environ.get("PRESENCE_DELTA_MAX_LIMIT", "500"))
PRESENCE_SNAPSHOT_MAX_LIMIT = int(os.environ.get("PRESENCE_SNAPSHOT_MAX_LIMIT", "1200"))
AVATAR_THUMB_IMMUTABLE_CACHE_SECONDS = int(os.environ.get("AVATAR_THUMB_IMMUTABLE_CACHE_SECONDS", str(30 * 24 * 3600)))
AVATAR_BACKFILL_BATCH_SIZE = int(os.environ.get("AVATAR_BACKFILL_BATCH_SIZE", "25"))
STORAGE_CLEANUP_INTERVAL_SECONDS = int(os.environ.get("STORAGE_CLEANUP_INTERVAL_SECONDS", "21600"))
MONTH_BUILD_STALE_DIR_MAX_AGE_SEC = int(os.environ.get("MONTH_BUILD_STALE_DIR_MAX_AGE_SEC", "1800"))
MONTH_BUILD_FAILURE_BACKOFF_SEC = int(os.environ.get("MONTH_BUILD_FAILURE_BACKOFF_SEC", "30"))

# Auto-rebuild backoff is persisted to disk so Railway redeploys during development
# do not re-trigger the same rebuild repeatedly. Default 10 minutes; override via env.
AUTO_RETIRED_REBUILD_BACKOFF_SEC = int(os.environ.get("AUTO_RETIRED_REBUILD_BACKOFF_SEC", "600"))

# Module-level once-per-process guard. Resets naturally on each Railway deploy.
# Prevents duplicate auto-rebuild triggers if startup ever runs twice in a single process.
_auto_rebuild_triggered_this_boot: bool = False

TRAP_CANDIDATE_PROMOTION_READINESS_CONFIG: Dict[str, Dict[str, Any]] = {
    "citywide_v3_trap_candidate": {
        "min_observations": 500,
        "min_recurring_demotions": 8,
        "max_abs_average_delta": 0.08,
        "target_average_delta_low": -0.06,
        "target_average_delta_high": -0.002,
        "max_positive_extreme": 0.16,
        "max_negative_extreme": -0.30,
        "require_demotions_gt_promotions": True,
        "recommended_next_phase": "limited_citywide_live_promotion",
    },
    "manhattan_v3_trap_candidate": {
        "min_observations": 250,
        "min_recurring_demotions": 5,
        "max_abs_average_delta": 0.09,
        "target_average_delta_low": -0.07,
        "target_average_delta_high": -0.002,
        "max_positive_extreme": 0.18,
        "max_negative_extreme": -0.32,
        "require_demotions_gt_promotions": True,
        "recommended_next_phase": "hold_for_manual_review",
    },
    "bronx_wash_heights_v3_trap_candidate": {
        "min_observations": 180,
        "min_recurring_demotions": 4,
        "max_abs_average_delta": 0.10,
        "target_average_delta_low": -0.08,
        "target_average_delta_high": -0.002,
        "max_positive_extreme": 0.18,
        "max_negative_extreme": -0.34,
        "require_demotions_gt_promotions": True,
        "recommended_next_phase": "hold_for_manual_review",
    },
    "queens_v3_trap_candidate": {
        "min_observations": 220,
        "min_recurring_demotions": 5,
        "max_abs_average_delta": 0.10,
        "target_average_delta_low": -0.08,
        "target_average_delta_high": -0.002,
        "max_positive_extreme": 0.18,
        "max_negative_extreme": -0.34,
        "require_demotions_gt_promotions": True,
        "recommended_next_phase": "hold_for_manual_review",
    },
    "brooklyn_v3_trap_candidate": {
        "min_observations": 220,
        "min_recurring_demotions": 5,
        "max_abs_average_delta": 0.10,
        "target_average_delta_low": -0.08,
        "target_average_delta_high": -0.002,
        "max_positive_extreme": 0.18,
        "max_negative_extreme": -0.34,
        "require_demotions_gt_promotions": True,
        "recommended_next_phase": "hold_for_manual_review",
    },
    "staten_island_v3_trap_candidate": {
        "min_observations": 80,
        "min_recurring_demotions": 2,
        "max_abs_average_delta": 0.12,
        "target_average_delta_low": -0.09,
        "target_average_delta_high": 0.01,
        "max_positive_extreme": 0.20,
        "max_negative_extreme": -0.35,
        "require_demotions_gt_promotions": False,
        "recommended_next_phase": "hold_for_manual_review",
    },
}

PICKUP_ZONE_HOTSPOT_MIN_POINTS = 5  # Keep 5-dot minimum to avoid pickup noise.
PICKUP_ZONE_HOTSPOT_MAX_POINTS = 100
PICKUP_ZONE_HOTSPOT_CELL_SIZE_M = 135
PICKUP_ZONE_HOTSPOT_RADIUS_M = 240
PICKUP_ZONE_HOTSPOT_SIGMA_M = 155
PICKUP_ZONE_HOTSPOT_SIMPLIFY_M = 18
PICKUP_ZONE_SECOND_HOTSPOT_MIN_POINTS = 8
PICKUP_ZONE_SECOND_COMPONENT_MIN_POINTS = 3
PICKUP_ZONE_SECOND_COMPONENT_MIN_SCORE_RATIO = 0.45
HOTSPOT_RECENT_LOOKBACK_SECONDS = 6 * 3600
HOTSPOT_TIMESLOT_BIN_MINUTES = 20

_pickup_zone_geom_cache: Optional[Dict[int, Dict[str, Any]]] = None
_pickup_zone_geom_cache_mtime: Optional[float] = None
_pickup_zone_geom_missing_warned = False
_pickup_zone_geom_parse_warned = False
_pickup_zone_hotspot_feature_cache: Dict[int, Dict[str, Any]] = {}
_pickup_zone_score_cache: Dict[int, float] = {}
_pickup_zone_hotspot_cache_lock = threading.Lock()
_timeline_cache_entry: Dict[str, Dict[str, Any]] = {}
_timeline_cache_lock = threading.Lock()
_assistant_outlook_frame_bucket_cache: Dict[str, Dict[str, Any]] = {}
_assistant_outlook_frame_bucket_order: deque[str] = deque()
_assistant_outlook_frame_bucket_lock = threading.Lock()
ASSISTANT_OUTLOOK_FRAME_BUCKET_CACHE_MAX = 6
_assistant_outlook_legacy_artifact_pruned = False
_frame_cache: Dict[Tuple[str, int], Dict[str, Any]] = {}
_frame_cache_order: deque[Tuple[str, int]] = deque()
_frame_cache_lock = threading.Lock()
_frame_builds_in_progress: Dict[Tuple[str, str], Dict[str, Any]] = {}
_frame_builds_in_progress_lock = threading.Lock()
FRAME_CACHE_MAX = 8
ARTIFACT_CACHE_CONTROL = "public, max-age=60"
PICKUP_RECENT_CACHE_TTL_SECONDS = float(os.environ.get("PICKUP_RECENT_CACHE_TTL_SECONDS", "5"))
PICKUP_LAST_GOOD_OVERLAY_TTL_SECONDS = float(os.environ.get("PICKUP_LAST_GOOD_OVERLAY_TTL_SECONDS", "45"))
PICKUP_RECENT_CACHE_MAX = int(os.environ.get("PICKUP_RECENT_CACHE_MAX", "64"))
PICKUP_HOTSPOT_CACHE_TTL_SECONDS = float(os.environ.get("PICKUP_HOTSPOT_CACHE_TTL_SECONDS", "180"))
PICKUP_HOTSPOT_CACHE_STALE_SECONDS = float(os.environ.get("PICKUP_HOTSPOT_CACHE_STALE_SECONDS", "900"))
PICKUP_SCORE_CACHE_TTL_SECONDS = float(os.environ.get("PICKUP_SCORE_CACHE_TTL_SECONDS", "15"))
PICKUP_EXPERIMENT_PRUNE_INTERVAL_SECONDS = float(os.environ.get("PICKUP_EXPERIMENT_PRUNE_INTERVAL_SECONDS", "300"))
RECOMMENDATION_OUTCOME_MATURITY_SECONDS = 5400
OUTCOME_SETTLEMENT_SWEEP_INTERVAL_SECONDS = 120
_pickup_recent_cache: Dict[str, Dict[str, Any]] = {}
_pickup_recent_cache_lock = threading.Lock()
_pickup_recent_last_good_overlay_cache: Dict[str, Dict[str, Any]] = {}
_pickup_recent_last_good_overlay_lock = threading.Lock()
_pickup_zone_score_bundle_cache: Dict[str, Dict[str, Any]] = {}
_pickup_zone_score_bundle_lock = threading.Lock()
_pickup_zone_maintenance_lock = threading.Lock()
_pickup_last_experiment_prune_monotonic = 0.0
_outcome_settlement_lock = threading.Lock()
_last_outcome_settlement_monotonic = 0.0
_presence_viewport_cache: Dict[str, Dict[str, Any]] = {}
_presence_viewport_cache_lock = threading.Lock()
_presence_cursor_lock = threading.Lock()
_presence_last_change_cursor_ms = 0
_avatar_backfill_started = False
_perf_metrics_lock = threading.Lock()
_perf_metrics: Dict[str, int] = defaultdict(int)
_cleanup_last_startup_removed_count = 0
_cleanup_last_startup_freed_bytes_estimate = 0
_cleanup_last_periodic_removed_count = 0
_cleanup_last_periodic_freed_bytes_estimate = 0
_cleanup_last_periodic_ran_at_unix = 0
_reconcile_last_periodic_deleted_paths: List[str] = []
_reconcile_last_periodic_ran_at_unix = 0
_last_failed_month_key: Optional[str] = None
_last_failed_at_unix: Optional[int] = None
_last_failed_error: Optional[str] = None
_last_attestation_run_unix_by_month: Dict[str, int] = {}
_last_attestation_report_by_month: Dict[str, Dict[str, Any]] = {}
_attestation_control_lock = threading.Lock()
_attestation_thread_by_month: Dict[str, threading.Thread] = {}
_attestation_state_by_month: Dict[str, Dict[str, Any]] = {}
ATTESTATION_REENTRY_THROTTLE_SECONDS = int(os.environ.get("ATTESTATION_REENTRY_THROTTLE_SECONDS", "45"))
_to_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
_to_4326 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

# =========================================================
# In-memory job state (hotspot generate)
# =========================================================
_state_lock = threading.Lock()
_generate_control_lock = threading.Lock()
_generate_thread: Optional[threading.Thread] = None
_generate_state: Dict[str, Any] = {
    "state": "idle",  # idle | started | running | done | error
    "bin_minutes": None,
    "min_trips_per_window": None,
    "run_token": None,
    "month_key": None,
    "build_all_months": False,
    "include_day_tendency": False,
    "build_review_artifacts": False,
    "started_at_unix": None,
    "finished_at_unix": None,
    "duration_sec": None,
    "result": None,
    "error": None,
    "trace": None,
}
_parquet_inventory_snapshot: Dict[str, Any] = {"rows": [], "warnings": [], "warning_count": 0}

# =========================================================
# App
# =========================================================
app = FastAPI(title="NYC TLC Hotspot Backend", version="2.2")


def _split_env_origins(*names: str) -> list[str]:
    origins: list[str] = []
    for name in names:
        raw = os.environ.get(name, "")
        if not raw:
            continue
        for value in raw.split(","):
            origin = value.strip().rstrip("/")
            if origin and origin not in origins:
                origins.append(origin)
    return origins


def _debug_log(*args: Any) -> None:
    if DEBUG_VERBOSE_LOGS:
        print(*args)


def _record_perf_metric(name: str, increment: int = 1) -> None:
    with _perf_metrics_lock:
        _perf_metrics[name] += int(increment)


def _perf_metric_snapshot() -> Dict[str, int]:
    with _perf_metrics_lock:
        return dict(_perf_metrics)


def _merge_vary_header(existing: Optional[str], value: str) -> str:
    vary_values = [item.strip() for item in (existing or "").split(",") if item.strip()]
    lowered = {item.lower() for item in vary_values}
    if value.lower() not in lowered:
        vary_values.append(value)
    return ", ".join(vary_values) if vary_values else value


class SelectiveGZipMiddleware(BaseHTTPMiddleware):
    @staticmethod
    def _rebuilt_headers(headers: Dict[str, str], *, gzip_applied: bool) -> Dict[str, str]:
        rebuilt = dict(headers)
        rebuilt.pop("Content-Length", None)
        rebuilt.pop("content-length", None)
        rebuilt["Vary"] = _merge_vary_header(rebuilt.get("Vary") or rebuilt.get("vary"), "Accept-Encoding")
        if gzip_applied:
            rebuilt["Content-Encoding"] = "gzip"
        return rebuilt

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        accept_encoding = request.headers.get("accept-encoding", "")
        if "gzip" not in accept_encoding.lower():
            return response
        if request.method.upper() == "HEAD":
            return response
        if request.url.path.startswith("/chat/audio/"):
            return response
        if response.status_code < 200 or response.status_code in {204, 206, 304}:
            return response
        if response.headers.get("content-encoding"):
            return response
        if response.headers.get("accept-ranges"):
            return response

        body = b""
        async for chunk in response.body_iterator:
            body += chunk

        content_type = (response.headers.get("content-type") or "").lower()
        should_compress = len(body) >= RESPONSE_GZIP_MIN_BYTES and not content_type.startswith("audio/")
        payload = gzip.compress(body) if should_compress else body
        headers = self._rebuilt_headers(dict(response.headers), gzip_applied=should_compress)
        if should_compress:
            _record_perf_metric("gzip.responses")

        return Response(
            content=payload,
            status_code=response.status_code,
            headers=headers,
            media_type=None,
        )


def _cors_allow_origins() -> list[str]:
    configured = _split_env_origins(
        "CORS_ALLOW_ORIGINS",
        "FRONTEND_ORIGIN",
        "FRONTEND_ORIGINS",
        "FRONTEND_URL",
        "APP_URL",
        "WEB_URL",
    )
    defaults = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]
    for origin in defaults:
        if origin not in configured:
            configured.append(origin)
    return configured

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allow_origins(),
    allow_origin_regex=r"https://([a-zA-Z0-9-]+\.)*(railway\.app|up\.railway\.app)",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Type", "Content-Length", "Accept-Ranges", "Content-Range"],
)
app.add_middleware(SelectiveGZipMiddleware)

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


def _month_key_for_datetime(dt: datetime) -> str:
    return f"{int(dt.year):04d}-{int(dt.month):02d}"


def _parse_month_key(month_key: str) -> Tuple[int, int]:
    raw = str(month_key or "").strip()
    match = re.fullmatch(r"(\d{4})-(\d{2})", raw)
    if not match:
        raise ValueError(f"Invalid month_key: {raw!r}")
    year, month = int(match.group(1)), int(match.group(2))
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month_key: {raw!r}")
    return year, month


def _safe_parse_month_key(month_key: str) -> Optional[Tuple[int, int]]:
    try:
        return _parse_month_key(month_key)
    except Exception:
        return None


def _month_key_from_parquet_filename(path: Path) -> Optional[str]:
    stem = path.stem.lower()
    numeric_match = re.search(r"(20\d{2})[-_](\d{1,2})", stem)
    if numeric_match:
        year = int(numeric_match.group(1))
        month = int(numeric_match.group(2))
        if 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"
    month_name_map = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
        "dicember": 12,
    }
    name_match = re.search(r"(january|february|march|april|may|june|july|august|september|october|november|december|dicember)[-_](20\d{2})", stem)
    if name_match:
        month = month_name_map.get(name_match.group(1))
        year = int(name_match.group(2))
        if month:
            return f"{year:04d}-{month:02d}"
    return None


def _month_key_from_parquet_data(path: Path) -> Optional[str]:
    con = duckdb.connect(database=":memory:")
    try:
        file_sql = str(path).replace("'", "''")
        min_ts = con.execute(
            f"SELECT MIN(pickup_datetime) FROM read_parquet('{file_sql}')"
        ).fetchone()[0]
    finally:
        con.close()
    if min_ts is None:
        return None
    if isinstance(min_ts, str):
        try:
            min_ts = datetime.fromisoformat(min_ts)
        except Exception:
            return None
    if isinstance(min_ts, datetime):
        return _month_key_for_datetime(min_ts.astimezone(NYC_TZ) if min_ts.tzinfo else min_ts)
    return None


def _group_parquets_by_month(parquets: List[Path]) -> Dict[str, List[Path]]:
    grouped: Dict[str, List[Path]] = defaultdict(list)
    for path in parquets:
        month_key = _month_key_from_parquet_filename(path) or _month_key_from_parquet_data(path)
        if month_key:
            grouped[month_key].append(path)
    return {key: sorted(value) for key, value in sorted(grouped.items())}


def _month_dir(month_key: str) -> Path:
    return EXACT_HISTORY_MONTHS_DIR / month_key


def _month_timeline_path(month_key: str) -> Path:
    return _month_dir(month_key) / "timeline.json"


def _month_store_path(month_key: str) -> Path:
    return _month_dir(month_key) / "exact_shadow.duckdb"


def _auto_rebuild_state_path(month_key: str) -> Path:
    """Returns /data/exact_history/months/{mk}/.auto_rebuild_state.json."""
    return _month_dir(str(month_key).strip()) / ".auto_rebuild_state.json"


def _read_auto_rebuild_state(month_key: str) -> Dict[str, Any]:
    """
    Reads the persisted auto-rebuild attempt state for a given month.
    Returns {"last_attempt_unix": int} or {} if unreadable/missing.
    Never raises — callers treat empty dict as "no recent attempt."
    """
    path = _auto_rebuild_state_path(month_key)
    try:
        if not path.exists() or not path.is_file() or path.stat().st_size <= 0:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {}
        return payload
    except Exception:
        return {}


def _write_auto_rebuild_state(month_key: str) -> None:
    """
    Writes the current timestamp to the auto-rebuild state file atomically.
    Called JUST BEFORE triggering an auto-rebuild so that even if the process
    dies or a new Railway deploy starts, the next startup sees the recent attempt.
    Never raises — failure is logged but non-fatal.
    """
    path = _auto_rebuild_state_path(month_key)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_attempt_unix": int(time.time()),
            "month_key": str(month_key).strip(),
            "backoff_sec": int(AUTO_RETIRED_REBUILD_BACKOFF_SEC),
        }
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp_path, path)
    except Exception:
        # Non-fatal: if we can't write the state file, worst case is the next
        # startup might re-attempt. The in-process _auto_rebuild_triggered_this_boot
        # flag still protects within this process.
        traceback.print_exc()


def _month_frame_cache_dir(month_key: str) -> Path:
    return _month_dir(month_key) / "frame_cache"


def _month_frame_cache_file(month_key: str, frame_index: int, frame_time: str) -> Path:
    safe_time = re.sub(r"[^0-9A-Za-z]+", "_", str(frame_time or "").strip()).strip("_")
    suffix = safe_time[:64] if safe_time else "unknown"
    return _month_frame_cache_dir(month_key) / f"frame_{int(frame_index):05d}_{suffix}.json"


def _month_build_meta_path(month_key: str) -> Path:
    return _month_dir(month_key) / "build_meta.json"


def _month_manifest_entry_payload(month_key: str, *, existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    baseline = dict(existing or {})
    timeline_path = _month_timeline_path(mk)
    frame_cache_dir = _month_frame_cache_dir(mk)
    freshness = _active_month_freshness(mk)
    build_meta = freshness.get("build_meta") or {}
    expected = freshness.get("expected") or {}
    timeline: List[str] = []
    if timeline_path.exists() and timeline_path.is_file() and timeline_path.stat().st_size > 0:
        try:
            timeline_payload = _read_json(timeline_path)
            timeline = list(timeline_payload.get("timeline") or [])
        except Exception:
            timeline = []
    baseline.update(
        {
            "month_key": mk,
            "source_parquet_filenames": [p.name for p in _source_parquets_for_month(mk)],
            "source_parquet_files": build_meta.get("source_parquet_files") or [p.name for p in _source_parquets_for_month(mk)],
            "source_of_truth": build_meta.get("source_of_truth"),
            "store_exists": bool(_month_store_path(mk).exists()),
            "timeline_exists": bool(timeline_path.exists() and timeline_path.is_file() and timeline_path.stat().st_size > 0),
            "frame_cache_dir_present": bool(frame_cache_dir.exists() and frame_cache_dir.is_dir()),
            "first_frame_datetime": build_meta.get("first_frame_datetime") or (timeline[0] if timeline else baseline.get("first_frame_datetime")),
            "last_frame_datetime": build_meta.get("last_frame_datetime") or (timeline[-1] if timeline else baseline.get("last_frame_datetime")),
            "frame_count": int(build_meta.get("frame_count") or len(timeline) or baseline.get("frame_count") or 0),
            "built_at_unix": build_meta.get("built_at_unix") or baseline.get("built_at_unix"),
            "build_meta_present": bool(freshness.get("build_meta_present")),
            "bin_minutes": build_meta.get("bin_minutes") or int(DEFAULT_BIN_MINUTES),
            "min_trips_per_window": build_meta.get("min_trips_per_window") or int(DEFAULT_MIN_TRIPS_PER_WINDOW),
            "code_dependency_hash": build_meta.get("code_dependency_hash"),
            "source_data_hash": build_meta.get("source_data_hash"),
            "artifact_signature": build_meta.get("artifact_signature"),
            "expected_artifact_signature": expected.get("artifact_signature"),
            "artifact_signature_matches": bool(freshness.get("artifact_signature_match")),
            "attested_via": build_meta.get("attested_via"),
            "exact_store_retired": bool(build_meta.get("retired_exact_store")),
            "exact_store_retired_reason": build_meta.get("exact_store_retired_reason"),
            "bootstrapped_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
    )
    return baseline


def _ensure_month_manifest_entry(month_key: str) -> None:
    mk = str(month_key or "").strip()
    if not _safe_parse_month_key(mk):
        return
    manifest = _load_month_manifest()
    months_manifest: Dict[str, Dict[str, Any]] = dict((manifest.get("months") or {}))
    existing = dict(months_manifest.get(mk) or {})
    months_manifest[mk] = _month_manifest_entry_payload(mk, existing=existing)
    _persist_month_manifest(months_manifest)


def _ensure_month_live_bootstrap(month_key: str, *, force_timeline_bootstrap: bool = False) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    if not _safe_parse_month_key(mk):
        return {"ok": False, "reason": "invalid_month_key", "month_key": mk}
    source_parquets = _source_parquets_for_month(mk)
    source_parquet_exists = bool(source_parquets)
    timeline_path = _month_timeline_path(mk)
    timeline_exists = bool(timeline_path.exists() and timeline_path.is_file() and timeline_path.stat().st_size > 0)
    bootstrapped = False
    if source_parquet_exists and (force_timeline_bootstrap or not timeline_exists):
        _bootstrap_month_partition(mk, bin_minutes=int(DEFAULT_BIN_MINUTES))
        timeline_exists = bool(timeline_path.exists() and timeline_path.is_file() and timeline_path.stat().st_size > 0)
        bootstrapped = True
    frame_cache_dir = _month_frame_cache_dir(mk)
    frame_cache_dir.mkdir(parents=True, exist_ok=True)
    _ensure_month_manifest_entry(mk)
    _maybe_promote_parquet_live_authority(mk)
    return {
        "ok": bool(source_parquet_exists and timeline_exists and frame_cache_dir.exists()),
        "month_key": mk,
        "source_parquet_exists": source_parquet_exists,
        "timeline_exists": timeline_exists,
        "frame_cache_dir_exists": bool(frame_cache_dir.exists() and frame_cache_dir.is_dir()),
        "bootstrapped_timeline": bootstrapped,
    }


def _build_single_frame_from_exact_store(month_key: str, frame_time: str) -> Dict[str, Any]:
    from build_hotspot import build_single_frame_from_exact_store, ensure_zones_geojson

    zones_path = ensure_zones_geojson(DATA_DIR, force=False)
    return build_single_frame_from_exact_store(
        exact_store_path=_month_store_path(month_key),
        zones_geojson_path=zones_path,
        frame_time=str(frame_time),
        bin_minutes=int(DEFAULT_BIN_MINUTES),
    )


def _attestation_state_snapshot(month_key: str) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    with _attestation_control_lock:
        current = dict(_attestation_state_by_month.get(mk) or {})
        thread = _attestation_thread_by_month.get(mk)
    in_progress = bool(current.get("attestation_in_progress"))
    if in_progress and thread and not thread.is_alive():
        in_progress = False
        with _attestation_control_lock:
            latest = dict(_attestation_state_by_month.get(mk) or {})
            latest["attestation_in_progress"] = False
            _attestation_state_by_month[mk] = latest
            current = latest
    return {
        "month_key": mk or None,
        "attestation_in_progress": in_progress,
        "attestation_started_at_unix": current.get("attestation_started_at_unix"),
        "attestation_finished_at_unix": current.get("attestation_finished_at_unix"),
        "attestation_last_result": current.get("attestation_last_result"),
        "attestation_last_error": current.get("attestation_last_error"),
    }


def _month_attestation_needed(month_key: str) -> bool:
    state = _month_bootstrap_state(month_key)
    if bool(state.get("authoritative_kind") == "parquet_live" and state.get("authoritative_fresh")):
        return False
    if bool(state.get("source_of_truth") == "exact_store") and not bool(state.get("authoritative_fresh")):
        return True
    return bool(
        state.get("store_exists")
        and state.get("timeline_exists")
        and (not state.get("build_meta_present"))
        and state.get("source_parquet_exists")
    )


def _write_month_build_meta_atomic(month_key: str, payload: Dict[str, Any]) -> Path:
    mk = str(month_key or "").strip()
    target = _month_build_meta_path(mk)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(".json.tmp")
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    with temp_path.open("wb") as handle:
        handle.write(encoded)
        handle.flush()
        try:
            os.fsync(handle.fileno())
        except OSError:
            pass
    os.replace(temp_path, target)
    return target


def _retire_obsolete_exact_store(month_key: str, reason: str) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    store_path = _month_store_path(mk)
    build_meta_path = _month_build_meta_path(mk)
    removed_store = False
    removed_old_build_meta = False
    # Uploaded parquet files are the permanent source of truth for monthly history.
    # Retirement cleanup must only remove obsolete derived artifacts for a month.
    if store_path.exists():
        store_path.unlink(missing_ok=True)
        removed_store = True
    old_meta = load_month_build_meta(EXACT_HISTORY_MONTHS_DIR, mk)
    if build_meta_path.exists() and isinstance(old_meta, dict) and str(old_meta.get("source_of_truth") or "").strip() == "exact_store":
        build_meta_path.unlink(missing_ok=True)
        removed_old_build_meta = True
    removed_frame_cache_count = _purge_month_frame_cache(mk)
    return {
        "removed_store": removed_store,
        "removed_old_build_meta": removed_old_build_meta,
        "removed_frame_cache_count": int(removed_frame_cache_count),
        "reason": str(reason or "").strip() or "retired",
    }


def _write_parquet_live_build_meta(month_key: str, reason: str, extra: Optional[dict] = None) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    timeline_path = _month_timeline_path(mk)
    timeline_payload = _read_json(timeline_path) if timeline_path.exists() else {}
    timeline = [_to_frontend_local_iso(item) for item in (timeline_payload.get("timeline") or [])]
    freshness = _active_month_freshness(mk)
    expected = freshness.get("expected") or {}
    now_unix = int(time.time())
    payload: Dict[str, Any] = {
        "month_key": mk,
        "built_at_unix": now_unix,
        "bin_minutes": int(DEFAULT_BIN_MINUTES),
        "min_trips_per_window": int(DEFAULT_MIN_TRIPS_PER_WINDOW),
        "source_parquet_files": [path.name for path in _source_parquets_for_month(mk)],
        "code_dependency_hash": expected.get("code_dependency_hash"),
        "source_data_hash": expected.get("source_data_hash"),
        "artifact_signature": expected.get("artifact_signature"),
        "frame_count": len(timeline),
        "first_frame_datetime": timeline[0] if timeline else None,
        "last_frame_datetime": timeline[-1] if timeline else None,
        "source_of_truth": "parquet_live",
        "authoritative_reason": str(reason or "").strip() or "parquet_live_authoritative",
        "retired_exact_store": False,
        "exact_store_retired_reason": "no_exact_store_present",
    }
    if isinstance(extra, dict):
        payload.update(extra)
    target = _write_month_build_meta_atomic(mk, payload)
    _ensure_month_manifest_entry(mk)
    return {"month_key": mk, "build_meta_path": str(target), "payload": payload}


def _normalize_frame_payload_for_compare(frame_payload: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    rows: Dict[int, Dict[str, Any]] = {}
    features = (((frame_payload or {}).get("polygons") or {}).get("features") or [])
    for feature in features:
        props = (feature or {}).get("properties") if isinstance(feature, dict) else {}
        if not isinstance(props, dict):
            continue
        try:
            location_id = int(props.get("LocationID"))
        except Exception:
            continue
        style = props.get("style") if isinstance(props.get("style"), dict) else {}
        normalized: Dict[str, Any] = {
            "rating": props.get("rating"),
            "bucket": props.get("bucket"),
            "style.fillColor": style.get("fillColor"),
            "earnings_shadow_rating_citywide_v3": props.get("earnings_shadow_rating_citywide_v3"),
            "earnings_shadow_bucket_citywide_v3": props.get("earnings_shadow_bucket_citywide_v3"),
            "earnings_shadow_color_citywide_v3": props.get("earnings_shadow_color_citywide_v3"),
        }
        for key, value in props.items():
            if not isinstance(key, str):
                continue
            if key.startswith("earnings_shadow_rating_") or key.startswith("earnings_shadow_bucket_") or key.startswith("earnings_shadow_color_"):
                if "_v3" in key:
                    normalized[key] = value
        rows[location_id] = normalized
    return rows


def _attestation_sample_frame_times(timeline: List[str], active_frame_time: Optional[str]) -> List[str]:
    if not timeline:
        return []
    indexes = [0, int((len(timeline) - 1) * 0.25), int((len(timeline) - 1) * 0.5), int((len(timeline) - 1) * 0.75), len(timeline) - 1]
    sampled: List[str] = []
    seen: set[str] = set()
    for idx in indexes:
        frame_time = _to_frontend_local_iso(timeline[max(0, min(idx, len(timeline) - 1))])
        if frame_time and frame_time not in seen:
            seen.add(frame_time)
            sampled.append(frame_time)
    visible = _to_frontend_local_iso(active_frame_time) if active_frame_time else ""
    if visible and visible in set(timeline) and visible not in seen:
        sampled.append(visible)
    return sampled


def attest_existing_month_store_against_current_code(month_key: str, active_frame_time: Optional[str] = None) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    timeline_path = _month_timeline_path(mk)
    store_path = _month_store_path(mk)
    build_meta_path = _month_build_meta_path(mk)
    if not (
        store_path.exists()
        and store_path.is_file()
        and store_path.stat().st_size > 0
        and timeline_path.exists()
        and timeline_path.is_file()
        and timeline_path.stat().st_size > 0
        and (not build_meta_path.exists() or build_meta_path.stat().st_size <= 0)
    ):
        return {"ok": False, "month_key": mk, "attested": False, "reason": "preconditions_not_met"}
    timeline_payload = _read_json(timeline_path)
    timeline = [_to_frontend_local_iso(item) for item in (timeline_payload.get("timeline") or [])]
    sample_times = _attestation_sample_frame_times(timeline, active_frame_time)
    if not sample_times:
        return {"ok": False, "month_key": mk, "attested": False, "reason": "missing_sample_frames"}
    mismatches: List[Dict[str, Any]] = []
    for frame_time in sample_times:
        parquet_frame = _build_single_frame_for_month(mk, frame_time)
        store_frame = _build_single_frame_from_exact_store(mk, frame_time)
        left = _normalize_frame_payload_for_compare(parquet_frame)
        right = _normalize_frame_payload_for_compare(store_frame)
        if left != right:
            sample_location_ids = sorted(list((set(left.keys()) & set(right.keys()))))[:6]
            sample_diffs: List[Dict[str, Any]] = []
            for location_id in sample_location_ids:
                if left.get(location_id) != right.get(location_id):
                    sample_diffs.append(
                        {
                            "location_id": int(location_id),
                            "parquet_props": left.get(location_id),
                            "store_props": right.get(location_id),
                        }
                    )
            mismatches.append(
                {
                    "frame_time": frame_time,
                    "left_zone_count": len(left),
                    "right_zone_count": len(right),
                    "example_location_ids": sorted(list(set(left.keys()) ^ set(right.keys())))[:10],
                    "sample_property_mismatches": sample_diffs[:3],
                }
            )
    if mismatches:
        retired = _retire_obsolete_exact_store(mk, reason="sample_mismatch")
        promoted = _write_parquet_live_build_meta(
            mk,
            reason="sample_mismatch_auto_promotion",
            extra={
                "retired_exact_store": bool(retired.get("removed_store")),
                "exact_store_retired_reason": "sample_mismatch",
                "attested_via": "parquet_live_authoritative",
                "sampled_mismatch_summary": {
                    "sample_frame_times": sample_times,
                    "mismatch_count": len(mismatches),
                },
            },
        )
        report = {
            "ok": False,
            "month_key": mk,
            "attested": True,
            "reason": "sample_mismatch",
            "sample_frame_times": sample_times,
            "mismatches": mismatches,
            "parquet_live_promoted": True,
            "retirement": retired,
            "build_meta_written": promoted.get("build_meta_path"),
        }
        _last_attestation_report_by_month[mk] = report
        return report

    freshness = _active_month_freshness(mk)
    expected = freshness.get("expected") or {}
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    source_parquet_files = [path.name for path in _source_parquets_for_month(mk)]
    build_meta_payload = {
        "month_key": mk,
        "built_at_unix": int(now_utc.timestamp()),
        "built_at_utc": now_utc.isoformat(),
        "bin_minutes": int(DEFAULT_BIN_MINUTES),
        "min_trips_per_window": int(DEFAULT_MIN_TRIPS_PER_WINDOW),
        "source_parquet_files": source_parquet_files,
        "first_frame_datetime": timeline[0] if timeline else None,
        "last_frame_datetime": timeline[-1] if timeline else None,
        "frame_count": len(timeline),
        "code_dependency_hash": expected.get("code_dependency_hash"),
        "source_data_hash": expected.get("source_data_hash"),
        "artifact_signature": expected.get("artifact_signature"),
        "source_of_truth": "exact_store",
        "attested_via": "sampled_frame_equivalence",
        "attestation_sample_frame_times": sample_times,
    }
    _write_month_build_meta_atomic(mk, build_meta_payload)
    _ensure_month_manifest_entry(mk)
    removed = _purge_month_frame_cache(mk)
    report = {
        "ok": True,
        "month_key": mk,
        "attested": True,
        "build_meta_written": str(build_meta_path),
        "frame_cache_files_removed": int(removed),
        "sample_frame_times": sample_times,
        "attested_via": "sampled_frame_equivalence",
    }
    _last_attestation_report_by_month[mk] = report
    return report


def _run_active_month_attestation(month_key: str, active_frame_time: Optional[str] = None) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    if not _safe_parse_month_key(mk):
        return {"ok": False, "month_key": mk or None, "reason": "invalid_month_key", "attested": False}
    if not _month_attestation_needed(mk):
        return {"ok": False, "month_key": mk, "reason": "not_pending", "attested": False}
    _last_attestation_run_unix_by_month[mk] = int(time.time())
    try:
        report = attest_existing_month_store_against_current_code(mk, active_frame_time=active_frame_time)
    except Exception as exc:
        report = {
            "ok": False,
            "month_key": mk,
            "attested": False,
            "reason": "exception",
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
    _last_attestation_report_by_month[mk] = report
    return report


def _attestation_worker(month_key: str, active_frame_time: Optional[str] = None) -> None:
    mk = str(month_key or "").strip()
    started_at = int(time.time())
    with _attestation_control_lock:
        state = dict(_attestation_state_by_month.get(mk) or {})
        state.update(
            {
                "attestation_in_progress": True,
                "attestation_started_at_unix": started_at,
                "attestation_finished_at_unix": None,
                "attestation_last_result": None,
                "attestation_last_error": None,
            }
        )
        _attestation_state_by_month[mk] = state
    try:
        report = _run_active_month_attestation(mk, active_frame_time=active_frame_time)
        if report.get("ok"):
            last_error = None
        else:
            last_error = report.get("error") or report.get("reason")
    except Exception as exc:
        report = {"ok": False, "month_key": mk, "attested": False, "reason": "exception", "error": str(exc)}
        last_error = str(exc)
    finished_at = int(time.time())
    with _attestation_control_lock:
        latest = dict(_attestation_state_by_month.get(mk) or {})
        latest.update(
            {
                "attestation_in_progress": False,
                "attestation_started_at_unix": started_at,
                "attestation_finished_at_unix": finished_at,
                "attestation_last_result": "success" if report.get("ok") else "failed",
                "attestation_last_error": last_error,
            }
        )
        _attestation_state_by_month[mk] = latest
        thread = _attestation_thread_by_month.get(mk)
        if thread is threading.current_thread():
            _attestation_thread_by_month.pop(mk, None)
    _last_attestation_report_by_month[mk] = report


def _queue_active_month_attestation(month_key: Optional[str], *, active_frame_time: Optional[str] = None) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    if not _safe_parse_month_key(mk):
        return {"ok": False, "month_key": mk or None, "reason": "invalid_month_key", "queued": False}
    if not _month_attestation_needed(mk):
        return {"ok": False, "month_key": mk, "reason": "not_pending", "queued": False}
    now_unix = int(time.time())
    result: Dict[str, Any]
    with _attestation_control_lock:
        state = dict(_attestation_state_by_month.get(mk) or {})
        thread = _attestation_thread_by_month.get(mk)
        in_progress = bool(state.get("attestation_in_progress") and thread and thread.is_alive())
        if in_progress:
            result = {"ok": False, "month_key": mk, "reason": "in_progress", "queued": False}
            return {**result, **{
                "attestation_in_progress": True,
                "attestation_started_at_unix": state.get("attestation_started_at_unix"),
                "attestation_finished_at_unix": state.get("attestation_finished_at_unix"),
                "attestation_last_result": state.get("attestation_last_result"),
                "attestation_last_error": state.get("attestation_last_error"),
            }}
        last_finished = int(state.get("attestation_finished_at_unix") or 0)
        if last_finished and (now_unix - last_finished) < int(ATTESTATION_REENTRY_THROTTLE_SECONDS):
            return {
                "ok": False,
                "month_key": mk,
                "reason": "throttled",
                "queued": False,
                "retry_after_sec": int(ATTESTATION_REENTRY_THROTTLE_SECONDS - (now_unix - last_finished)),
                "attestation_in_progress": False,
                "attestation_started_at_unix": state.get("attestation_started_at_unix"),
                "attestation_finished_at_unix": state.get("attestation_finished_at_unix"),
                "attestation_last_result": state.get("attestation_last_result"),
                "attestation_last_error": state.get("attestation_last_error"),
            }
        t = threading.Thread(
            target=_attestation_worker,
            kwargs={"month_key": mk, "active_frame_time": active_frame_time},
            name=f"month-attestation-{mk}",
            daemon=True,
        )
        _attestation_thread_by_month[mk] = t
        state.update(
            {
                "attestation_in_progress": True,
                "attestation_started_at_unix": now_unix,
                "attestation_finished_at_unix": None,
                "attestation_last_result": state.get("attestation_last_result"),
                "attestation_last_error": state.get("attestation_last_error"),
            }
        )
        _attestation_state_by_month[mk] = state
        t.start()
    return {"ok": True, "month_key": mk, "queued": True, "reason": "started", **_attestation_state_snapshot(mk)}


def _active_month_freshness(month_key: str) -> Dict[str, Any]:
    return active_month_freshness_report(
        month_key=month_key,
        exact_history_months_dir=EXACT_HISTORY_MONTHS_DIR,
        repo_root=Path(__file__).resolve().parent,
        data_dir=DATA_DIR,
        frames_dir=FRAMES_DIR,
        bin_minutes=int(DEFAULT_BIN_MINUTES),
        min_trips_per_window=int(DEFAULT_MIN_TRIPS_PER_WINDOW),
    )


def _purge_month_frame_cache(month_key: str) -> int:
    mk = str(month_key or "").strip()
    if not mk:
        return 0
    removed = 0
    cache_dir = _month_frame_cache_dir(mk)
    # Purge only derived frame cache payloads. Never delete uploaded month parquet files here.
    if cache_dir.exists() and cache_dir.is_dir():
        for candidate in cache_dir.glob("frame_*.json"):
            try:
                candidate.unlink(missing_ok=True)
                removed += 1
            except Exception:
                traceback.print_exc()
    cache_dir.mkdir(parents=True, exist_ok=True)
    with _frame_cache_lock:
        stale_keys = [key for key in list(_frame_cache.keys()) if str(key[0]).strip() == mk]
        for key in stale_keys:
            _frame_cache.pop(key, None)
        if stale_keys:
            _frame_cache_order_filtered = deque([k for k in _frame_cache_order if str(k[0]).strip() != mk])
            _frame_cache_order.clear()
            _frame_cache_order.extend(_frame_cache_order_filtered)
    return int(removed)


def _to_frontend_local_iso(value: Any) -> str:
    return to_frontend_local_iso(value)


def _timeline_entries_need_iso_rewrite(timeline_payload: Any) -> bool:
    if not isinstance(timeline_payload, dict):
        return False
    timeline_items = timeline_payload.get("timeline") if isinstance(timeline_payload.get("timeline"), list) else []
    for entry in timeline_items:
        if str(entry or "").strip():
            return "T" not in str(entry)
    entries = timeline_payload.get("entries") if isinstance(timeline_payload.get("entries"), list) else []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        frame_time = str(entry.get("frame_time") or "").strip()
        if frame_time:
            return "T" not in frame_time
    return False


def _rewrite_month_timeline_from_store(month_key: str) -> None:
    store_path = _month_store_path(month_key)
    timeline_path = _month_timeline_path(month_key)
    if not (store_path.exists() and store_path.is_file() and store_path.stat().st_size > 0):
        return
    if not (timeline_path.exists() and timeline_path.is_file() and timeline_path.stat().st_size > 0):
        return

    con = duckdb.connect(database=str(store_path), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT DISTINCT
                exact_bin_local_ts,
                exact_bin_date_local,
                exact_weekday_name_local,
                exact_bin_time_label_local
            FROM exact_shadow_rows
            ORDER BY exact_bin_local_ts
            """
        ).fetchall()
    finally:
        con.close()

    timeline_entries: List[Dict[str, Any]] = []
    for row in rows:
        normalized_frame_time = _to_frontend_local_iso(row[0])
        timeline_entries.append(
            {
                "frame_time": normalized_frame_time,
                "frame_date": None if row[1] is None else str(row[1]),
                "frame_weekday_name": None if row[2] is None else str(row[2]),
                "frame_time_label": None if row[3] is None else str(row[3]),
                "bin_minutes": int(DEFAULT_BIN_MINUTES),
            }
        )
    timeline = [entry["frame_time"] for entry in timeline_entries]
    rebuilt_payload = {
        "timeline": timeline,
        "entries": timeline_entries,
        "count": len(timeline),
        "bin_minutes": int(DEFAULT_BIN_MINUTES),
        "timeline_mode": "exact_historical",
        "frame_time_model": "exact_local_20min",
        "synthetic_week_enabled": False,
    }
    temp_path = timeline_path.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(rebuilt_payload, separators=(",", ":")), encoding="utf-8")
    temp_path.replace(timeline_path)

    with _timeline_cache_lock:
        _timeline_cache_entry.pop(str(month_key).strip(), None)


def _maybe_repair_month_timeline_iso(month_key: str) -> bool:
    timeline_path = _month_timeline_path(month_key)
    store_path = _month_store_path(month_key)
    if not (
        timeline_path.exists()
        and timeline_path.is_file()
        and timeline_path.stat().st_size > 0
        and store_path.exists()
        and store_path.is_file()
        and store_path.stat().st_size > 0
    ):
        return False
    try:
        payload = _read_json(timeline_path)
    except Exception:
        return False
    if not _timeline_entries_need_iso_rewrite(payload):
        return False
    _rewrite_month_timeline_from_store(month_key)
    return True


def _dir_is_stale(path: Path, now_unix: float, min_age_sec: int) -> bool:
    try:
        if not path.exists() or not path.is_dir():
            return False
        return (now_unix - float(path.stat().st_mtime)) >= float(min_age_sec)
    except Exception:
        return False


def _count_stale_subdirs(root_dir: Path, min_age_sec: int = MONTH_BUILD_STALE_DIR_MAX_AGE_SEC) -> int:
    if not root_dir.exists() or not root_dir.is_dir():
        return 0
    now_unix = time.time()
    count = 0
    for child in root_dir.iterdir():
        if not child.is_dir():
            continue
        if _dir_is_stale(child, now_unix, int(min_age_sec)):
            count += 1
    return int(count)


def _prune_stale_month_build_dirs(min_age_sec: int = MONTH_BUILD_STALE_DIR_MAX_AGE_SEC) -> Dict[str, Any]:
    removed_paths: List[str] = []
    now_unix = time.time()
    EXACT_HISTORY_MONTHS_BUILDING_DIR.mkdir(parents=True, exist_ok=True)
    for child in EXACT_HISTORY_MONTHS_BUILDING_DIR.iterdir():
        if not child.is_dir():
            continue
        if not _dir_is_stale(child, now_unix, int(min_age_sec)):
            continue
        try:
            shutil.rmtree(child, ignore_errors=False)
            removed_paths.append(str(child))
        except Exception:
            traceback.print_exc()
    print(f"stale_month_build_cleanup_done removed_count={len(removed_paths)}")
    return {"removed_paths": removed_paths, "removed_count": len(removed_paths)}


def _prune_stale_month_backup_dirs(min_age_sec: int = MONTH_BUILD_STALE_DIR_MAX_AGE_SEC) -> Dict[str, Any]:
    removed_paths: List[str] = []
    now_unix = time.time()
    EXACT_HISTORY_MONTHS_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    for child in EXACT_HISTORY_MONTHS_BACKUP_DIR.iterdir():
        if not child.is_dir():
            continue
        if not _dir_is_stale(child, now_unix, int(min_age_sec)):
            continue
        try:
            shutil.rmtree(child, ignore_errors=False)
            removed_paths.append(str(child))
        except Exception:
            traceback.print_exc()
    print(f"stale_month_backup_cleanup_done removed_count={len(removed_paths)}")
    return {"removed_paths": removed_paths, "removed_count": len(removed_paths)}


def _legacy_frame_files() -> List[Path]:
    return sorted(
        list(FRAMES_DIR.glob("frame_*.json"))
        + [
            TIMELINE_PATH,
            ASSISTANT_OUTLOOK_PATH,
            FRAMES_DIR / "scoring_shadow_manifest.json",
        ]
    )


def _legacy_frame_file_count() -> int:
    return sum(1 for p in _legacy_frame_files() if p.exists() and p.is_file())


def _monthly_mode_ready_for_legacy_cleanup() -> bool:
    manifest = _load_month_manifest()
    available_month_keys = list(manifest.get("available_month_keys") or [])
    if not available_month_keys:
        return False
    if not (MONTH_MANIFEST_PATH.exists() and MONTH_MANIFEST_PATH.is_file() and MONTH_MANIFEST_PATH.stat().st_size > 0):
        return False
    active_month_key = resolve_active_month_key(datetime.now(timezone.utc).astimezone(NYC_TZ), available_month_keys)
    if not active_month_key:
        return False
    timeline_path = _month_timeline_path(active_month_key)
    bootstrap_state = _month_bootstrap_state(active_month_key)
    core_month_ready = bool(
        timeline_path.exists()
        and timeline_path.is_file()
        and timeline_path.stat().st_size > 0
        and bool(bootstrap_state.get("live_ready"))
        and bool(bootstrap_state.get("authoritative_fresh"))
    )
    if not core_month_ready:
        return False
    return True


def _prune_legacy_frame_files_after_monthly_ready() -> Dict[str, Any]:
    if not _monthly_mode_ready_for_legacy_cleanup():
        return {"removed_paths": [], "removed_count": 0}
    removed_paths: List[str] = []
    for target in _legacy_frame_files():
        try:
            if not target.exists() or not target.is_file():
                continue
            target.unlink(missing_ok=True)
            if not target.exists():
                removed_paths.append(str(target))
        except Exception:
            traceback.print_exc()
    print(f"legacy_frame_cleanup_done removed_count={len(removed_paths)}")
    return {"removed_paths": removed_paths, "removed_count": len(removed_paths)}


def _is_protected_source_path(path: Path) -> bool:
    name = str(path.name or "").lower()
    return name.endswith(".parquet") or name == "taxi_zones.geojson"


def _prune_obsolete_month_derived_artifacts() -> Dict[str, Any]:
    removed_paths: List[str] = []
    removed_count = 0
    manifest = _load_month_manifest()
    month_keys = [mk for mk in list(manifest.get("available_month_keys") or []) if _safe_parse_month_key(str(mk))]
    for mk in month_keys:
        month_key = str(mk)
        state = _month_bootstrap_state(month_key)
        if bool(state.get("source_of_truth") == "parquet_live" and state.get("authoritative_fresh") and state.get("store_exists")):
            retired = _retire_obsolete_exact_store(month_key, reason="parquet_live_authoritative_cleanup")
            removed_count += int(retired.get("removed_frame_cache_count") or 0)
            if retired.get("removed_store"):
                removed_count += 1
                removed_paths.append(str(_month_store_path(month_key)))
            if retired.get("removed_old_build_meta"):
                removed_count += 1
                removed_paths.append(str(_month_build_meta_path(month_key)))
        month_dir = _month_dir(month_key)
        for temp_path in [month_dir / "build_meta.json.tmp", month_dir / "exact_shadow.duckdb.tmp"]:
            if not temp_path.exists() or _is_protected_source_path(temp_path):
                continue
            _cleanup_path_quiet(temp_path)
            if not temp_path.exists():
                removed_count += 1
                removed_paths.append(str(temp_path))
    return {"removed_paths": removed_paths, "removed_count": int(removed_count)}


def _available_source_month_keys() -> List[str]:
    grouped = _group_parquets_by_month(_list_parquets())
    return sorted(grouped.keys())


def _format_month_key_label(month_key: str) -> str:
    year, month = _parse_month_key(month_key)
    month_name = datetime(year=year, month=month, day=1).strftime("%B")
    return f"{month_name} {year:04d}"


def _load_month_manifest() -> Dict[str, Any]:
    if not MONTH_MANIFEST_PATH.exists() or MONTH_MANIFEST_PATH.stat().st_size <= 0:
        return {"available_month_keys": [], "months": {}}
    try:
        payload = _read_json(MONTH_MANIFEST_PATH)
    except Exception:
        return {"available_month_keys": [], "months": {}}
    months = payload.get("months") if isinstance(payload, dict) else {}
    if not isinstance(months, dict):
        months = {}
    keys = [k for k in months.keys() if _safe_parse_month_key(k)]
    return {"available_month_keys": sorted(keys), "months": months}


def _persist_month_manifest(month_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    payload = {
        "timeline_scope": "monthly_exact_historical",
        "available_month_keys": sorted(month_map.keys()),
        "months": month_map,
        "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    MONTH_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MONTH_MANIFEST_PATH.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    return payload


def resolve_active_month_key(target_dt_nyc: datetime, available_month_keys: List[str]) -> Optional[str]:
    # Product rule: prefer same year-month, then same calendar month from any year, else latest available month.
    valid = [mk for mk in sorted(available_month_keys) if _safe_parse_month_key(mk)]
    if not valid:
        return None
    target_year = int(target_dt_nyc.year)
    target_month = int(target_dt_nyc.month)
    same_month = [mk for mk in valid if (_safe_parse_month_key(mk) or (0, 0))[1] == target_month]
    if f"{target_year:04d}-{target_month:02d}" in same_month:
        return f"{target_year:04d}-{target_month:02d}"
    if same_month:
        return sorted(same_month)[-1]
    return valid[-1]


def _resolve_target_month_key_for_request(month_key: Optional[str] = None, target_dt_nyc: Optional[datetime] = None) -> str:
    manifest = _load_month_manifest()
    built_month_keys = sorted(list(manifest.get("available_month_keys") or []))
    source_month_keys = _available_source_month_keys()

    if month_key:
        requested = str(month_key).strip()
        if requested in set(source_month_keys) or requested in set(built_month_keys):
            return requested
        raise HTTPException(status_code=404, detail=f"month_key not available: {requested}")

    target = target_dt_nyc or datetime.now(timezone.utc).astimezone(NYC_TZ)
    candidate = resolve_active_month_key(target, source_month_keys)
    if candidate:
        return candidate
    fallback = resolve_active_month_key(target, built_month_keys)
    if fallback:
        return fallback
    raise HTTPException(status_code=409, detail="No source parquet month data available. Upload parquet files first.")


def _resolve_active_month_key(month_key: Optional[str] = None, target_dt_nyc: Optional[datetime] = None) -> Tuple[str, Dict[str, Any]]:
    manifest = _load_month_manifest()
    available = list(manifest.get("available_month_keys") or [])
    if month_key:
        requested = str(month_key).strip()
        if requested not in available:
            raise HTTPException(status_code=404, detail=f"month_key not available: {requested}")
        print(f"active_month_resolved month_key={requested}")
        return requested, manifest
    target = target_dt_nyc or datetime.now(timezone.utc).astimezone(NYC_TZ)
    resolved = resolve_active_month_key(target, available)
    if not resolved:
        raise HTTPException(status_code=409, detail="No monthly exact-history partitions available. Call /generate first.")
    print(f"active_month_resolved month_key={resolved}")
    return resolved, manifest


def _source_parquets_for_month(month_key: str) -> List[Path]:
    grouped = _group_parquets_by_month(_list_parquets())
    return list(grouped.get(str(month_key).strip()) or [])


def _month_bootstrap_ready(month_key: str) -> bool:
    state = _month_bootstrap_state(month_key)
    return bool(state.get("bootstrap_ready"))


def _maybe_promote_parquet_live_authority(month_key: str) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    if not _safe_parse_month_key(mk):
        return {"promoted": False, "reason": "invalid_month_key"}
    timeline_exists = bool(_month_timeline_path(mk).exists() and _month_timeline_path(mk).is_file() and _month_timeline_path(mk).stat().st_size > 0)
    frame_cache_dir_present = bool(_month_frame_cache_dir(mk).exists() and _month_frame_cache_dir(mk).is_dir())
    source_parquet_exists = bool(_source_parquets_for_month(mk))
    store_exists = bool(_month_store_path(mk).exists() and _month_store_path(mk).is_file() and _month_store_path(mk).stat().st_size > 0)
    build_meta = load_month_build_meta(EXACT_HISTORY_MONTHS_DIR, mk)
    if source_parquet_exists and timeline_exists and frame_cache_dir_present and (not store_exists) and not isinstance(build_meta, dict):
        result = _write_parquet_live_build_meta(mk, reason="bootstrap_without_exact_store")
        return {"promoted": True, "reason": "bootstrap_without_exact_store", "build_meta_path": result.get("build_meta_path")}
    return {"promoted": False, "reason": "not_eligible"}


def _month_bootstrap_state(month_key: str) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    timeline_path = _month_timeline_path(mk)
    store_path = _month_store_path(mk)
    frame_cache_dir = _month_frame_cache_dir(mk)
    manifest = _load_month_manifest()
    month_entry = (manifest.get("months") or {}).get(mk) or {}
    _maybe_promote_parquet_live_authority(mk)
    freshness = _active_month_freshness(mk) if mk else {}
    build_meta_present = bool(freshness.get("build_meta_present"))
    signature_match = bool(freshness.get("signature_match"))
    authoritative_fresh = bool(freshness.get("authoritative_fresh"))
    authoritative_kind = freshness.get("authoritative_kind")
    source_of_truth = freshness.get("source_of_truth")
    exact_store_retired = bool((freshness.get("build_meta") or {}).get("retired_exact_store"))
    exact_store_retired_reason = (freshness.get("build_meta") or {}).get("exact_store_retired_reason")
    source_parquet_exists = bool(mk and _source_parquets_for_month(mk))
    month_manifest_present = bool(
        MONTH_MANIFEST_PATH.exists()
        and MONTH_MANIFEST_PATH.is_file()
        and MONTH_MANIFEST_PATH.stat().st_size > 0
    )
    timeline_exists = bool(timeline_path.exists() and timeline_path.is_file() and timeline_path.stat().st_size > 0)
    store_exists = bool(store_path.exists() and store_path.is_file() and store_path.stat().st_size > 0)
    frame_cache_dir_present = bool(frame_cache_dir.exists() and frame_cache_dir.is_dir())
    exact_store_fresh = bool(authoritative_kind == "exact_store" and authoritative_fresh)
    live_ready = bool(
        source_parquet_exists
        and timeline_exists
        and frame_cache_dir_present
    )
    legacy_ready_without_build_meta = bool(source_parquet_exists and timeline_exists and store_exists and not build_meta_present)
    build_meta_backfill_pending = bool(live_ready and not build_meta_present)
    authoritative_ready = bool(authoritative_fresh and authoritative_kind in {"exact_store", "parquet_live"})
    bootstrap_ready = bool(live_ready or exact_store_fresh or authoritative_ready or legacy_ready_without_build_meta)
    serving_mode = "rebuild_required"
    if exact_store_fresh:
        serving_mode = "exact_store_fresh"
    elif live_ready and authoritative_kind == "parquet_live" and authoritative_fresh:
        serving_mode = "parquet_live_authoritative"
    elif live_ready:
        serving_mode = "parquet_live_bootstrap"
    if not signature_match and not live_ready:
        _purge_month_frame_cache(mk)
    return {
        "month_manifest_present": month_manifest_present,
        "month_entry_present": bool(month_entry),
        "source_parquet_exists": source_parquet_exists,
        "timeline_exists": timeline_exists,
        "store_exists": store_exists,
        "frame_cache_dir_present": frame_cache_dir_present,
        "build_meta_present": build_meta_present,
        "signature_match": signature_match,
        "authoritative_fresh": authoritative_fresh,
        "authoritative_kind": authoritative_kind,
        "source_of_truth": source_of_truth,
        "exact_store_retired": exact_store_retired,
        "exact_store_retired_reason": exact_store_retired_reason,
        "live_ready": live_ready,
        "exact_store_fresh": exact_store_fresh,
        "active_month_live_ready": live_ready,
        "active_month_exact_store_fresh": exact_store_fresh,
        "active_month_authoritative_fresh": authoritative_fresh,
        "active_month_source_of_truth": source_of_truth,
        "active_month_exact_store_retired": exact_store_retired,
        "active_month_exact_store_retired_reason": exact_store_retired_reason,
        "serving_mode": serving_mode,
        "legacy_ready_without_build_meta": legacy_ready_without_build_meta,
        "build_meta_backfill_pending": build_meta_backfill_pending,
        "strict_ready": exact_store_fresh,
        "bootstrap_ready": bootstrap_ready,
    }


def _month_partition_ready(month_key: str) -> bool:
    return _month_bootstrap_ready(month_key)


def _build_preparing_month_payload(
    month_key: str,
    request_kind: str,
    generate_started: bool,
    retry_after_sec: int = 3,
) -> Dict[str, Any]:
    return {
        "ok": False,
        "status": "preparing_month",
        "request_kind": str(request_kind),
        "target_month_key": month_key,
        "target_month_label": _format_month_key_label(month_key),
        "message": f"Preparing {_format_month_key_label(month_key)} historical data",
        "retry_after_sec": int(retry_after_sec),
        "generate_started": bool(generate_started),
        "generate_state": _get_state(),
        "monthly_partition_mode": True,
    }


def _preparing_month_response(
    month_key: str,
    request_kind: str,
    generate_started: bool,
    retry_after_sec: int = 3,
) -> JSONResponse:
    payload = _build_preparing_month_payload(
        month_key=month_key,
        request_kind=request_kind,
        generate_started=generate_started,
        retry_after_sec=retry_after_sec,
    )
    return JSONResponse(
        status_code=202,
        content=payload,
        headers={
            "Retry-After": str(int(retry_after_sec)),
            "Cache-Control": "no-store",
        },
    )


def _recent_month_failure_backoff_remaining(month_key: str, now_unix: Optional[int] = None) -> int:
    now_val = int(now_unix if now_unix is not None else time.time())
    if str(month_key or "").strip() != str(_last_failed_month_key or "").strip():
        return 0
    failed_at = int(_last_failed_at_unix or 0)
    if failed_at <= 0:
        return 0
    elapsed = max(0, now_val - failed_at)
    remaining = int(MONTH_BUILD_FAILURE_BACKOFF_SEC) - int(elapsed)
    return max(0, int(remaining))


def _artifact_freshness_snapshot() -> Dict[str, Any]:
    repo_root = Path(__file__).resolve().parent
    try:
        report = evaluate_artifact_freshness(
            repo_root=repo_root,
            data_dir=DATA_DIR,
            frames_dir=FRAMES_DIR,
            bin_minutes=DEFAULT_BIN_MINUTES,
            min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
        )
        return {
            "fresh": bool(report.get("fresh")),
            "summary": report.get("summary") or "Artifact freshness evaluated",
            "reason_codes": report.get("reason_codes") or [],
            "artifact_signature": report.get("artifact_signature"),
            "code_dependency_hash": report.get("code_dependency_hash"),
            "source_data_hash": report.get("source_data_hash"),
        }
    except Exception:
        return {
            "fresh": False,
            "summary": "Freshness evaluation failed",
            "reason_codes": ["freshness_check_failed"],
            "artifact_signature": None,
            "code_dependency_hash": None,
            "source_data_hash": None,
        }


def _is_no_space_error(exc_or_text: Any) -> bool:
    text = str(exc_or_text or "").lower()
    return "no space left on device" in text or "errno 28" in text or "[errno 28]" in text


def _backend_identity_snapshot(freshness: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    env_build_id = (os.environ.get("BACKEND_BUILD_ID") or "").strip()
    env_release = (os.environ.get("BACKEND_RELEASE") or "").strip()
    if env_build_id or env_release:
        return {
            "backend_build_id": env_build_id or None,
            "backend_release": env_release or None,
            "source": "env",
        }
    snap = freshness or _artifact_freshness_snapshot()
    code_hash = str(snap.get("code_dependency_hash") or "").strip()
    if code_hash:
        short = code_hash[:12]
        return {
            "backend_build_id": short,
            "backend_release": f"code-{short}",
            "source": "code_dependency_hash_fallback",
        }
    return {"backend_build_id": None, "backend_release": None, "source": "missing"}


def _has_frames(month_key: Optional[str] = None) -> bool:
    try:
        resolved_key, _ = _resolve_active_month_key(month_key=month_key) if month_key else _resolve_active_month_key()
        timeline_path = _month_timeline_path(resolved_key)
        return timeline_path.exists() and timeline_path.stat().st_size > 0
    except Exception:
        return False


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _etag_for_path(path: Path, mtime: float, size: int) -> str:
    return f'W/"{path.name}-{int(mtime)}-{int(size)}"'


def _request_etag_matches(request: Request, etag: Optional[str]) -> bool:
    if not etag:
        return False
    raw = request.headers.get("if-none-match", "")
    if not raw:
        return False
    for candidate in raw.split(","):
        if candidate.strip() == etag:
            return True
    return False


def _build_json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _frame_cache_json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def _log_artifact_response(path: str, status_code: int, gzip_requested: bool, body_length: int, content_length: Optional[str]) -> None:
    print(
        f"[artifact-response] path={path} status={status_code} gzip_requested={str(gzip_requested).lower()} "
        f"body_len={body_length} content_length={content_length or '-'}"
    )


def _json_cached_response(
    request: Request,
    payload: Any,
    *,
    cache_control: str = ARTIFACT_CACHE_CONTROL,
    etag: Optional[str] = None,
) -> Response:
    headers = {"Cache-Control": cache_control}
    if etag:
        headers["ETag"] = etag

    gzip_requested = "gzip" in (request.headers.get("accept-encoding", "").lower())
    if _request_etag_matches(request, etag):
        response = Response(status_code=304, headers=headers)
        _log_artifact_response(request.url.path, response.status_code, gzip_requested, 0, response.headers.get("content-length"))
        return response

    body = _build_json_bytes(payload)
    response = Response(content=body, media_type="application/json", headers=headers)
    _log_artifact_response(request.url.path, response.status_code, gzip_requested, len(body), response.headers.get("content-length"))
    return response


def _read_timeline_cached(month_key: Optional[str] = None) -> Dict[str, Any]:
    if month_key:
        resolved_month_key = str(month_key).strip()
        if not _safe_parse_month_key(resolved_month_key):
            raise HTTPException(status_code=404, detail=f"month_key not available: {resolved_month_key}")
        manifest = _load_month_manifest()
    else:
        resolved_month_key, manifest = _resolve_active_month_key(month_key=month_key)
    _ensure_month_live_bootstrap(resolved_month_key)
    _maybe_repair_month_timeline_iso(resolved_month_key)
    timeline_path = _month_timeline_path(resolved_month_key)
    if timeline_path.exists() and timeline_path.stat().st_size > 0:
        stat_result = timeline_path.stat()
        mtime = stat_result.st_mtime
        size = int(stat_result.st_size)
        etag = _etag_for_path(timeline_path, mtime, size)
        cache_key = resolved_month_key
        with _timeline_cache_lock:
            cached = _timeline_cache_entry.get(cache_key) or {}
            if cached and cached.get("mtime") == mtime and cached.get("size") == size:
                _record_perf_metric("timeline.cache_hit")
                return dict(cached)
            _record_perf_metric("timeline.cache_miss")
            data = _read_json(timeline_path)
            data["timeline"] = [_to_frontend_local_iso(item) for item in (data.get("timeline") or [])]
            if isinstance(data.get("entries"), list):
                normalized_entries: List[Dict[str, Any]] = []
                for entry in data.get("entries") or []:
                    if not isinstance(entry, dict):
                        continue
                    entry_copy = dict(entry)
                    entry_copy["frame_time"] = _to_frontend_local_iso(entry_copy.get("frame_time"))
                    normalized_entries.append(entry_copy)
                data["entries"] = normalized_entries
            data["active_month_key"] = resolved_month_key
            data["available_month_keys"] = list(manifest.get("available_month_keys") or [])
            data["timeline_scope"] = "monthly_exact_historical"
            _timeline_cache_entry[cache_key] = {"data": data, "mtime": mtime, "size": size, "etag": etag}
            return dict(_timeline_cache_entry[cache_key])

    if resolved_month_key:
        raise FileNotFoundError(
            f"Missing monthly timeline on volume for month_key={resolved_month_key}: {timeline_path}"
        )

    artifact = load_generated_artifact("timeline")
    if artifact:
        metadata = artifact.get("metadata") or {}
        cache_token = (
            f"{metadata.get('updated_at_unix')}:"
            f"{metadata.get('content_sha256')}:"
            f"{metadata.get('payload_uncompressed_bytes')}"
        )
        etag = f"\"sha256:{metadata.get('content_sha256')}\""
        with _timeline_cache_lock:
            cached = _timeline_cache_entry
            if cached and cached.get("cache_token") == cache_token:
                _record_perf_metric("timeline.cache_hit")
                return cached
            _record_perf_metric("timeline.cache_miss")
            _timeline_cache_entry.clear()
            _timeline_cache_entry.update(
                {
                    "data": artifact.get("payload") or {},
                    "cache_token": cache_token,
                    "size": int(metadata.get("payload_uncompressed_bytes") or 0),
                    "etag": etag,
                }
            )
            return dict(_timeline_cache_entry)
    raise FileNotFoundError(f"Missing timeline artifact for month_key={resolved_month_key}: {timeline_path}")


def _read_frame_cached(idx: int, month_key: Optional[str] = None) -> Dict[str, Any]:
    timeline_cached = _read_timeline_cached(month_key=month_key)
    timeline_payload = (timeline_cached or {}).get("data") or {}
    timeline = timeline_payload.get("timeline") or []
    if idx < 0 or idx >= len(timeline):
        raise HTTPException(status_code=404, detail=f"frame not found: {idx}")
    frame_time = _to_frontend_local_iso(timeline[idx])
    active_month_key = str(timeline_payload.get("active_month_key") or month_key or "")
    timeline_etag = str((timeline_cached or {}).get("etag") or "")
    cache_idx = (active_month_key, int(idx))
    cache_file = _month_frame_cache_file(active_month_key, idx, frame_time)
    etag = f'W/"frame-{active_month_key}-{idx}-{abs(hash((frame_time, timeline_etag))) & 0xffffffff:x}"'
    with _frame_cache_lock:
        cached = _frame_cache.get(cache_idx)
        if cached is not None and cached.get("etag") == etag:
            _record_perf_metric("frame.cache_hit")
            try:
                _frame_cache_order.remove(cache_idx)
            except ValueError:
                pass
            _frame_cache_order.append(cache_idx)
            return cached

        if cache_file.exists() and cache_file.stat().st_size > 0:
            _record_perf_metric("frame.cache_hit")
            data = _read_json(cache_file)
        else:
            _record_perf_metric("frame.cache_miss")
            raise FileNotFoundError(f"Frame cache miss for month_key={active_month_key} idx={idx} frame_time={frame_time}")
        _frame_cache[cache_idx] = {"data": data, "etag": etag, "frame_time": frame_time}
        try:
            _frame_cache_order.remove(cache_idx)
        except ValueError:
            pass
        _frame_cache_order.append(cache_idx)
        while len(_frame_cache_order) > FRAME_CACHE_MAX:
            evicted_idx = _frame_cache_order.popleft()
            _frame_cache.pop(evicted_idx, None)
        return _frame_cache[cache_idx]


def _build_single_frame_for_month(month_key: str, frame_time: str) -> Dict[str, Any]:
    from build_hotspot import build_single_frame_for_month, ensure_zones_geojson

    parquets = _source_parquets_for_month(month_key)
    if not parquets:
        raise RuntimeError(f"No source parquet files for month_key={month_key}")
    zones_path = ensure_zones_geojson(DATA_DIR, force=False)
    return build_single_frame_for_month(
        parquet_files=parquets,
        zones_geojson_path=zones_path,
        frame_time=str(frame_time),
        bin_minutes=int(DEFAULT_BIN_MINUTES),
        min_trips_per_window=int(DEFAULT_MIN_TRIPS_PER_WINDOW),
    )


def _frame_build_worker(month_key: str, idx: int, frame_time: str, run_token: str) -> None:
    key = (month_key, frame_time)
    cache_file: Optional[Path] = None
    temp_file: Optional[Path] = None
    try:
        cache_dir = _month_frame_cache_dir(month_key)
        cache_dir.mkdir(parents=True, exist_ok=True)
        print(f"frame_cache_build_start month_key={month_key} idx={idx} frame_time={frame_time}")
        payload = _build_single_frame_for_month(month_key=month_key, frame_time=frame_time)
        cache_file = _month_frame_cache_file(month_key, idx, frame_time)
        temp_file = cache_file.with_suffix(f"{cache_file.suffix}.tmp")
        encoded = json.dumps(
            payload,
            separators=(",", ":"),
            default=_frame_cache_json_default,
        ).encode("utf-8")
        with temp_file.open("wb") as handle:
            handle.write(encoded)
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError:
                pass
        os.replace(temp_file, cache_file)
        with _frame_cache_lock:
            cache_idx = (month_key, int(idx))
            etag = f'W/"frame-{month_key}-{idx}-{abs(hash((frame_time, len(encoded)))) & 0xffffffff:x}"'
            _frame_cache[cache_idx] = {"data": payload, "etag": etag, "frame_time": frame_time}
            try:
                _frame_cache_order.remove(cache_idx)
            except ValueError:
                pass
            _frame_cache_order.append(cache_idx)
            while len(_frame_cache_order) > FRAME_CACHE_MAX:
                evicted_idx = _frame_cache_order.popleft()
                _frame_cache.pop(evicted_idx, None)
        print(f"frame_cache_build_done month_key={month_key} idx={idx} frame_time={frame_time} file={cache_file}")
    except Exception as exc:
        if isinstance(exc, TypeError) and "not JSON serializable" in str(exc):
            print(
                f"frame_cache_build_failed_json month_key={month_key} idx={idx} "
                f"frame_time={frame_time} error={exc}"
            )
        print(f"frame_cache_build_failed month_key={month_key} idx={idx} frame_time={frame_time} error={exc}")
        traceback.print_exc()
        if temp_file is not None:
            try:
                if temp_file.exists():
                    temp_file.unlink()
            except Exception:
                pass
    finally:
        with _frame_builds_in_progress_lock:
            marker = _frame_builds_in_progress.get(key)
            if marker and str(marker.get("run_token")) == run_token:
                _frame_builds_in_progress.pop(key, None)


def _ensure_frame_build_in_progress(month_key: str, idx: int, frame_time: str) -> bool:
    key = (str(month_key).strip(), str(frame_time).strip())
    with _frame_builds_in_progress_lock:
        if key in _frame_builds_in_progress:
            return False
        run_token = uuid.uuid4().hex
        _frame_builds_in_progress[key] = {
            "started_at_unix": int(time.time()),
            "run_token": run_token,
        }
    thread = threading.Thread(
        target=_frame_build_worker,
        args=(key[0], int(idx), key[1], run_token),
        daemon=True,
    )
    thread.start()
    return True


def _geometry_bounds_from_coordinates(coords: Any) -> Optional[Tuple[float, float, float, float]]:
    min_lat: Optional[float] = None
    min_lng: Optional[float] = None
    max_lat: Optional[float] = None
    max_lng: Optional[float] = None

    def _consume(value: Any) -> None:
        nonlocal min_lat, min_lng, max_lat, max_lng
        if isinstance(value, (list, tuple)):
            if len(value) >= 2 and all(isinstance(v, (int, float)) for v in value[:2]):
                lng = float(value[0])
                lat = float(value[1])
                if math.isnan(lat) or math.isnan(lng):
                    return
                min_lat = lat if min_lat is None else min(min_lat, lat)
                max_lat = lat if max_lat is None else max(max_lat, lat)
                min_lng = lng if min_lng is None else min(min_lng, lng)
                max_lng = lng if max_lng is None else max(max_lng, lng)
                return
            for item in value:
                _consume(item)

    _consume(coords)
    if min_lat is None or min_lng is None or max_lat is None or max_lng is None:
        return None
    return (min_lng, min_lat, max_lng, max_lat)


def _feature_geometry_bounds(feature: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
    geometry = feature.get("geometry") if isinstance(feature, dict) else None
    if not isinstance(geometry, dict):
        return None
    return _geometry_bounds_from_coordinates(geometry.get("coordinates"))


def _bbox_intersects(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> bool:
    a_min_lng, a_min_lat, a_max_lng, a_max_lat = a
    b_min_lng, b_min_lat, b_max_lng, b_max_lat = b
    return not (a_max_lng < b_min_lng or b_max_lng < a_min_lng or a_max_lat < b_min_lat or b_max_lat < a_min_lat)


def _expand_viewport_bbox(
    min_lat: float,
    min_lng: float,
    max_lat: float,
    max_lng: float,
    padding_ratio: float = 0.18,
) -> Tuple[float, float, float, float]:
    low_lat = min(min_lat, max_lat)
    high_lat = max(min_lat, max_lat)
    low_lng = min(min_lng, max_lng)
    high_lng = max(min_lng, max_lng)
    lat_span = max(0.0, high_lat - low_lat)
    lng_span = max(0.0, high_lng - low_lng)
    safe_padding = max(0.0, min(float(padding_ratio), 1.0))
    lat_pad = lat_span * safe_padding
    lng_pad = lng_span * safe_padding
    expanded_min_lat = max(-90.0, low_lat - lat_pad)
    expanded_min_lng = max(-180.0, low_lng - lng_pad)
    expanded_max_lat = min(90.0, high_lat + lat_pad)
    expanded_max_lng = min(180.0, high_lng + lng_pad)
    return (expanded_min_lng, expanded_min_lat, expanded_max_lng, expanded_max_lat)


def _frame_payload_viewport_subset(
    frame_payload: Dict[str, Any],
    min_lat: float,
    min_lng: float,
    max_lat: float,
    max_lng: float,
    padding_ratio: float = 0.18,
) -> Dict[str, Any]:
    safe_padding = max(0.0, min(float(padding_ratio), 1.0))
    expanded_bbox = _expand_viewport_bbox(
        min_lat=min_lat,
        min_lng=min_lng,
        max_lat=max_lat,
        max_lng=max_lng,
        padding_ratio=safe_padding,
    )
    source_payload = frame_payload if isinstance(frame_payload, dict) else {}
    polygons = source_payload.get("polygons")
    features = polygons.get("features") if isinstance(polygons, dict) else None
    if not isinstance(features, list):
        payload = copy.deepcopy(source_payload)
        payload["_viewport_subset"] = False
        payload["_viewport_fallback_reason"] = "missing_polygon_features"
        return payload

    subset_features: List[Dict[str, Any]] = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        bounds = _feature_geometry_bounds(feature)
        if bounds is None:
            continue
        if _bbox_intersects(bounds, expanded_bbox):
            subset_features.append(feature)

    source_count = len(features)
    if not subset_features:
        payload = copy.deepcopy(source_payload)
        payload["_viewport_subset"] = False
        payload["_viewport_bounds"] = {
            "min_lat": min(min_lat, max_lat),
            "min_lng": min(min_lng, max_lng),
            "max_lat": max(min_lat, max_lat),
            "max_lng": max(min_lng, max_lng),
        }
        payload["_viewport_padding_ratio"] = safe_padding
        payload["_source_feature_count"] = source_count
        payload["_returned_feature_count"] = source_count
        payload["_viewport_fallback_reason"] = "no_intersections"
        _record_perf_metric("frame.viewport_subset_fallback_full")
        return payload

    payload = copy.deepcopy(source_payload)
    polygons_payload = payload.get("polygons")
    polygons_payload = polygons_payload if isinstance(polygons_payload, dict) else {}
    polygons_payload["features"] = subset_features
    payload["polygons"] = polygons_payload
    payload["_viewport_subset"] = True
    payload["_viewport_bounds"] = {
        "min_lat": min(min_lat, max_lat),
        "min_lng": min(min_lng, max_lng),
        "max_lat": max(min_lat, max_lat),
        "max_lng": max(min_lng, max_lng),
    }
    payload["_viewport_padding_ratio"] = safe_padding
    payload["_source_feature_count"] = source_count
    payload["_returned_feature_count"] = len(subset_features)
    _record_perf_metric("frame.viewport_subset_hit")
    return payload


def _viewport_frame_etag(
    base_etag: Optional[str],
    min_lat: float,
    min_lng: float,
    max_lat: float,
    max_lng: float,
    padding_ratio: float,
) -> Optional[str]:
    if not base_etag:
        return None
    safe_padding = max(0.0, min(float(padding_ratio), 1.0))
    normalized = (
        round(min(min_lat, max_lat), 4),
        round(min(min_lng, max_lng), 4),
        round(max(min_lat, max_lat), 4),
        round(max(min_lng, max_lng), 4),
        round(safe_padding, 4),
    )
    suffix = hashlib.sha1(f"{normalized}".encode("utf-8")).hexdigest()[:12]
    return f'{base_etag[:-1]}-vp-{suffix}"' if base_etag.endswith('"') else f'{base_etag}-vp-{suffix}'


def _has_assistant_outlook() -> bool:
    try:
        return _has_frames()
    except Exception:
        return False


def _prune_assistant_outlook_file_if_db_ready() -> int:
    try:
        if not ASSISTANT_OUTLOOK_PATH.exists() or not ASSISTANT_OUTLOOK_PATH.is_file():
            return 0
        size = int(ASSISTANT_OUTLOOK_PATH.stat().st_size)
        ASSISTANT_OUTLOOK_PATH.unlink(missing_ok=True)
        if not ASSISTANT_OUTLOOK_PATH.exists():
            print(f"[artifact-prune] removed redundant assistant_outlook file copy: {ASSISTANT_OUTLOOK_PATH}")
            return size
    except Exception:
        traceback.print_exc()
    return 0


def _assistant_outlook_file_is_valid_json() -> bool:
    try:
        if not ASSISTANT_OUTLOOK_PATH.exists() or not ASSISTANT_OUTLOOK_PATH.is_file():
            return False
        payload = _read_json(ASSISTANT_OUTLOOK_PATH)
        return isinstance(payload, dict)
    except (json.JSONDecodeError, OSError, UnicodeDecodeError, ValueError):
        print(f"[warn] invalid assistant outlook JSON on volume: {ASSISTANT_OUTLOOK_PATH}")
        return False
    except Exception:
        traceback.print_exc()
        return False


def _prune_invalid_assistant_outlook_file_if_needed() -> int:
    try:
        if not ASSISTANT_OUTLOOK_PATH.exists() or not ASSISTANT_OUTLOOK_PATH.is_file():
            return 0
        if _assistant_outlook_file_is_valid_json():
            return 0
        if generated_artifact_present("assistant_outlook"):
            return _prune_assistant_outlook_file_if_db_ready()
        size = int(ASSISTANT_OUTLOOK_PATH.stat().st_size)
        corrupt_path = ASSISTANT_OUTLOOK_PATH.with_suffix(".json.corrupt")
        corrupt_path.unlink(missing_ok=True)
        ASSISTANT_OUTLOOK_PATH.rename(corrupt_path)
        print(f"[artifact-prune] quarantined invalid assistant outlook file: {corrupt_path}")
        return size
    except Exception:
        traceback.print_exc()
        return 0


def _assistant_outlook_bucket_cache_key(
    *,
    active_month_key: str,
    frame_time: str,
    horizon_bins: int,
    timeline_etag: str,
    bin_minutes: int,
) -> str:
    return f"{str(active_month_key).strip()}|{str(frame_time).strip()}|{int(horizon_bins)}|{int(bin_minutes)}|{timeline_etag}"


def _prune_legacy_assistant_outlook_artifact_if_present() -> bool:
    global _assistant_outlook_legacy_artifact_pruned
    try:
        if not generated_artifact_present("assistant_outlook"):
            return False
        delete_generated_artifact("assistant_outlook")
        _assistant_outlook_legacy_artifact_pruned = True
        print("[artifact-prune] deleted legacy assistant_outlook generated artifact row")
        return True
    except Exception:
        traceback.print_exc()
        return False


def _build_assistant_outlook_frame_bucket_cached(
    *,
    timeline_cached: Dict[str, Any],
    frame_time: str,
    horizon_bins: int = HORIZON_BINS_DEFAULT,
) -> Dict[str, Any]:
    timeline_payload = (timeline_cached or {}).get("data") or {}
    timeline_etag = str((timeline_cached or {}).get("etag") or "")
    frame_key = _to_frontend_local_iso(frame_time)
    if not frame_key:
        raise KeyError("frame_time is required")
    bin_minutes = int(timeline_payload.get("bin_minutes") or DEFAULT_BIN_MINUTES)
    active_month_key = str(timeline_payload.get("active_month_key") or "").strip()
    if not active_month_key:
        raise RuntimeError("timeline payload missing active_month_key for assistant outlook frame loading")
    key = _assistant_outlook_bucket_cache_key(
        active_month_key=active_month_key,
        frame_time=frame_key,
        horizon_bins=horizon_bins,
        timeline_etag=timeline_etag,
        bin_minutes=bin_minutes,
    )

    with _assistant_outlook_frame_bucket_lock:
        cached = _assistant_outlook_frame_bucket_cache.get(key)
        if cached is not None:
            _record_perf_metric("assistant_outlook.bucket_cache_hit")
            try:
                _assistant_outlook_frame_bucket_order.remove(key)
            except ValueError:
                pass
            _assistant_outlook_frame_bucket_order.append(key)
            return cached
        _record_perf_metric("assistant_outlook.bucket_cache_miss")

    def frame_loader(future_idx: int) -> List[Dict[str, Any]]:
        try:
            cached_frame = _read_frame_cached(int(future_idx), month_key=active_month_key)
        except FileNotFoundError:
            # Graceful degradation: one missing frame in the horizon must not 500 the whole outlook request.
            # Queue a background build using the helper /frame/{idx} already uses, then return []
            # so the outlook engine continues with the remaining frames in this horizon.
            try:
                _timeline_list_for_miss = list((timeline_payload or {}).get("timeline") or [])
                if 0 <= int(future_idx) < len(_timeline_list_for_miss):
                    _frame_time_for_miss = _to_frontend_local_iso(_timeline_list_for_miss[int(future_idx)])
                    _ensure_frame_build_in_progress(active_month_key, int(future_idx), _frame_time_for_miss)
                    print(
                        f"[warn] assistant outlook frame_loader miss: "
                        f"month_key={active_month_key} idx={int(future_idx)} "
                        f"frame_time={_frame_time_for_miss} queued background build"
                    )
                else:
                    print(
                        f"[warn] assistant outlook frame_loader miss (no timeline entry): "
                        f"month_key={active_month_key} idx={int(future_idx)}"
                    )
            except Exception:
                # Never let the miss-handler itself crash the outlook builder.
                traceback.print_exc()
            return []
        payload = (cached_frame or {}).get("data") or {}
        polygons = payload.get("polygons") or {}
        features = polygons.get("features") or []
        if not isinstance(features, list):
            return []
        return features

    bucket_payload = build_assistant_outlook_frame_bucket_from_loader(
        timeline_payload=timeline_payload,
        frame_time=frame_key,
        frame_loader=frame_loader,
        horizon_bins=horizon_bins,
    )
    frame_bucket = bucket_payload.get("bucket") or {}
    cached_entry = {
        "frame_time": _to_frontend_local_iso(bucket_payload.get("frame_time") or frame_key),
        "horizon_bins": int(bucket_payload.get("horizon_bins") or horizon_bins),
        "bin_minutes": int(bucket_payload.get("bin_minutes") or bin_minutes),
        "timeline_etag": timeline_etag,
        "frame_bucket": frame_bucket,
        "cache_key": key,
    }
    with _assistant_outlook_frame_bucket_lock:
        _assistant_outlook_frame_bucket_cache[key] = cached_entry
        try:
            _assistant_outlook_frame_bucket_order.remove(key)
        except ValueError:
            pass
        _assistant_outlook_frame_bucket_order.append(key)
        while len(_assistant_outlook_frame_bucket_order) > ASSISTANT_OUTLOOK_FRAME_BUCKET_CACHE_MAX:
            evicted_key = _assistant_outlook_frame_bucket_order.popleft()
            _assistant_outlook_frame_bucket_cache.pop(evicted_key, None)
    return cached_entry


def _has_day_tendency_model() -> bool:
    try:
        if generated_artifact_present("day_tendency_model"):
            return True
        return DAY_TENDENCY_MODEL_PATH.exists() and DAY_TENDENCY_MODEL_PATH.stat().st_size > 0
    except Exception:
        return False


def _read_day_tendency_model() -> Dict[str, Any]:
    artifact = load_generated_artifact("day_tendency_model")
    if artifact:
        return artifact.get("payload") or {}
    return _read_json(DAY_TENDENCY_MODEL_PATH)


def _day_tendency_model_is_current() -> bool:
    try:
        if not _has_day_tendency_model():
            return False
        model = _read_day_tendency_model()
        if str(model.get("version")) != DAY_TENDENCY_VERSION:
            return False
        if "borough_weekday_bin" not in model:
            return False
        if "borough_bin" not in model:
            return False
        if "borough_baseline" not in model:
            return False
        if "global_bin" not in model:
            return False
        if "global_baseline" not in model:
            return False
        freshness = _active_month_freshness(
            resolve_active_month_key(
                datetime.now(timezone.utc).astimezone(NYC_TZ),
                _available_source_month_keys(),
            )
            or ""
        )
        expected = freshness.get("expected") or {}
        expected_code = expected.get("code_dependency_hash")
        expected_source = expected.get("source_data_hash")
        expected_artifact = expected.get("artifact_signature")
        if expected_code and model.get("code_dependency_hash") != expected_code:
            return False
        if expected_source and model.get("source_data_hash") != expected_source:
            return False
        if expected_artifact and model.get("artifact_signature") != expected_artifact:
            return False
        return True
    except Exception:
        return False


def _prune_redundant_db_backed_artifact_files() -> Dict[str, Any]:
    removed_paths: List[str] = []
    bytes_freed_estimate = 0
    invalid_file_pruned_bytes = _prune_invalid_assistant_outlook_file_if_needed()
    if invalid_file_pruned_bytes > 0:
        removed_paths.append(str(ASSISTANT_OUTLOOK_PATH))
        bytes_freed_estimate += int(invalid_file_pruned_bytes)
    assistant_outlook_pruned_bytes = _prune_assistant_outlook_file_if_db_ready()
    if assistant_outlook_pruned_bytes > 0:
        removed_paths.append(str(ASSISTANT_OUTLOOK_PATH))
        bytes_freed_estimate += int(assistant_outlook_pruned_bytes)
    prune_targets = [
        (FRAMES_DIR / "scoring_shadow_manifest.json", "scoring_shadow_manifest"),
    ]
    for target_path, artifact_key in prune_targets:
        try:
            if not generated_artifact_present(artifact_key):
                continue
            artifact = load_generated_artifact(artifact_key)
            if not artifact:
                continue
            if not target_path.exists() or not target_path.is_file():
                continue
            size = int(target_path.stat().st_size)
            target_path.unlink(missing_ok=True)
            if not target_path.exists():
                removed_paths.append(str(target_path))
                bytes_freed_estimate += size
        except Exception:
            continue
    legacy_prune_result = _prune_legacy_frame_files_after_monthly_ready()
    for legacy_path in legacy_prune_result.get("removed_paths") or []:
        if legacy_path not in removed_paths:
            removed_paths.append(legacy_path)
    return {
        "removed_paths": removed_paths,
        "removed_count": len(removed_paths),
        "bytes_freed_estimate": int(bytes_freed_estimate),
    }


def _artifact_runtime_policy_snapshot() -> Dict[str, Any]:
    return {
        "volume_required": [
            "*.parquet",
            "taxi_zones.geojson",
            "exact_history/month_manifest.json",
            "exact_history/months/<month_key>/timeline.json",
        ],
        "db_required": [
            "scoring_shadow_manifest",
        ],
        "db_optional": [
            "day_tendency_model",
        ],
        "db_mirrored_optional": ["timeline"],
        "must_not_remain_on_volume": [
            "frames/assistant_outlook.json",
            "frames/scoring_shadow_manifest.json",
        ],
        "protected_source_inputs": [
            "*.parquet",
            "taxi_zones.geojson",
        ],
    }


def _artifact_runtime_integrity_report() -> Dict[str, Any]:
    parquet_count = len(_list_parquets())
    zones_present = DATA_DIR.joinpath("taxi_zones.geojson").exists()
    manifest = _load_month_manifest()
    available_month_keys = list(manifest.get("available_month_keys") or [])
    active_month_key = resolve_active_month_key(datetime.now(timezone.utc).astimezone(NYC_TZ), available_month_keys)
    month_manifest_present = MONTH_MANIFEST_PATH.exists() and MONTH_MANIFEST_PATH.is_file() and MONTH_MANIFEST_PATH.stat().st_size > 0
    timeline_present = bool(active_month_key and _month_timeline_path(active_month_key).exists() and _month_timeline_path(active_month_key).is_file())
    exact_store_present = bool(active_month_key and _month_store_path(active_month_key).exists() and _month_store_path(active_month_key).is_file() and _month_store_path(active_month_key).stat().st_size > 0)
    month_freshness = _active_month_freshness(active_month_key) if active_month_key else {}
    active_bootstrap_state = _month_bootstrap_state(active_month_key) if active_month_key else {}
    signature_match = bool(active_bootstrap_state.get("signature_match"))
    build_meta_present = bool(active_bootstrap_state.get("build_meta_present"))
    legacy_ready_without_build_meta = bool(active_bootstrap_state.get("legacy_ready_without_build_meta"))
    build_meta_backfill_pending = bool(active_bootstrap_state.get("build_meta_backfill_pending"))
    active_month_live_ready = bool(active_bootstrap_state.get("live_ready"))
    active_month_exact_store_fresh = bool(active_bootstrap_state.get("exact_store_fresh"))
    active_month_authoritative_fresh = bool(active_bootstrap_state.get("authoritative_fresh"))
    active_month_source_of_truth = active_bootstrap_state.get("source_of_truth")
    active_month_exact_store_retired = bool(active_bootstrap_state.get("exact_store_retired"))
    active_month_exact_store_retired_reason = active_bootstrap_state.get("exact_store_retired_reason")
    timeline_count = 0
    if timeline_present and active_month_key:
        try:
            timeline_payload = _read_json(_month_timeline_path(active_month_key))
            timeline_count = int(timeline_payload.get("count") or len(timeline_payload.get("timeline") or []))
        except Exception:
            timeline_count = 0
    required_volume_ok = bool(
        parquet_count > 0
        and timeline_count > 0
        and zones_present
        and month_manifest_present
        and timeline_present
        and active_month_live_ready
    )

    required_db_keys = ["scoring_shadow_manifest"]
    missing_required_db_artifacts = [key for key in required_db_keys if not generated_artifact_present(key)]
    required_db_ok = len(missing_required_db_artifacts) == 0
    optional_artifacts_missing: List[str] = []
    if not generated_artifact_present("day_tendency_model"):
        optional_artifacts_missing.append("day_tendency_model")

    optional_db_mirror = {"timeline": generated_artifact_present("timeline")}

    redundant_targets = [
        ASSISTANT_OUTLOOK_PATH,
        FRAMES_DIR / "scoring_shadow_manifest.json",
    ]
    redundant_file_copies_present = [str(path) for path in redundant_targets if path.exists() and path.is_file()]

    missing_required_volume: List[str] = []
    if parquet_count <= 0:
        missing_required_volume.append("*.parquet")
    if not zones_present:
        missing_required_volume.append("taxi_zones.geojson")
    if not month_manifest_present:
        missing_required_volume.append("exact_history/month_manifest.json")
    if not timeline_present:
        missing_required_volume.append("exact_history/months/<active_month_key>/timeline.json")
    if not active_month_live_ready:
        missing_required_volume.append("exact_history/months/<active_month_key>/timeline.json+frame_cache")

    core_map_ready = bool(required_volume_ok and (active_month_live_ready or active_month_authoritative_fresh))
    ok = core_map_ready and not redundant_file_copies_present
    return {
        "ok": ok,
        "core_map_ready": core_map_ready,
        "required_volume_ok": required_volume_ok,
        "required_db_ok": required_db_ok,
        "optional_db_mirror": optional_db_mirror,
        "optional_artifacts_missing": optional_artifacts_missing,
        "redundant_file_copies_present": redundant_file_copies_present,
        "missing_required_volume": missing_required_volume,
        "missing_required_db_artifacts": missing_required_db_artifacts,
        "unexpected_volume_files_present": list(redundant_file_copies_present),
        "frame_count": timeline_count,
        "parquet_count": parquet_count,
        "protected_source_parquet_count": parquet_count,
        "monthly_partition_mode": True,
        "active_month_key": active_month_key,
        "available_month_keys": available_month_keys,
        "monthly_partition_count": len(available_month_keys),
        "active_month_store_present": exact_store_present,
        "active_month_timeline_present": timeline_present,
        "active_month_frame_cache_dir_present": bool(active_month_key and _month_frame_cache_dir(active_month_key).exists() and _month_frame_cache_dir(active_month_key).is_dir()),
        "active_month_bootstrap_ready": bool(active_month_live_ready or active_month_exact_store_fresh or legacy_ready_without_build_meta),
        "active_month_live_ready": active_month_live_ready,
        "active_month_exact_store_fresh": active_month_exact_store_fresh,
        "active_month_authoritative_fresh": active_month_authoritative_fresh,
        "active_month_source_of_truth": active_month_source_of_truth,
        "active_month_exact_store_retired": active_month_exact_store_retired,
        "active_month_exact_store_retired_reason": active_month_exact_store_retired_reason,
        "active_month_serving_mode": active_bootstrap_state.get("serving_mode") or "rebuild_required",
        "active_month_build_meta_present": build_meta_present,
        "active_month_signature_matches_code": bool(month_freshness.get("code_dependency_hash_match")),
        "active_month_signature_matches_source": bool(month_freshness.get("source_data_hash_match")),
        "active_month_artifact_signature_matches": bool(month_freshness.get("artifact_signature_match")),
        "active_month_legacy_ready_without_build_meta": legacy_ready_without_build_meta,
        "active_month_build_meta_backfill_pending": build_meta_backfill_pending,
    }


def _reconcile_artifact_runtime_state() -> Dict[str, Any]:
    before_integrity = _artifact_runtime_integrity_report()
    repaired_flags = {
        "assistant_outlook_legacy_pruned": False,
        "day_tendency_rebuilt": False,
    }
    deleted_paths: List[str] = []

    if _prune_legacy_assistant_outlook_artifact_if_present():
        repaired_flags["assistant_outlook_legacy_pruned"] = True

    day_tendency_missing = not generated_artifact_present("day_tendency_model")
    day_tendency_stale = not _day_tendency_model_is_current()
    if (day_tendency_missing or day_tendency_stale) and len(_list_parquets()) > 0 and (DATA_DIR / "taxi_zones.geojson").exists():
        try:
            _build_day_tendency_only(DEFAULT_BIN_MINUTES)
            repaired_flags["day_tendency_rebuilt"] = True
        except Exception:
            traceback.print_exc()

    prune_result = _prune_redundant_db_backed_artifact_files()
    deleted_paths = list(prune_result.get("removed_paths") or [])

    after_integrity = _artifact_runtime_integrity_report()
    return {
        "repaired_flags": repaired_flags,
        "deleted_paths": deleted_paths,
        "before_integrity": before_integrity,
        "after_integrity": after_integrity,
    }


def _start_storage_cleanup_sweeper() -> None:
    def _worker() -> None:
        global _cleanup_last_periodic_removed_count, _cleanup_last_periodic_freed_bytes_estimate
        global _cleanup_last_periodic_ran_at_unix, _reconcile_last_periodic_deleted_paths, _reconcile_last_periodic_ran_at_unix
        while True:
            try:
                time.sleep(max(60, int(STORAGE_CLEANUP_INTERVAL_SECONDS)))
                cleanup_result = cleanup_artifact_storage(DATA_DIR, FRAMES_DIR)
                prune_result = _prune_redundant_db_backed_artifact_files()
                stale_build_prune = _prune_stale_month_build_dirs()
                stale_backup_prune = _prune_stale_month_backup_dirs()
                legacy_prune = _prune_legacy_frame_files_after_monthly_ready()
                obsolete_month_prune = _prune_obsolete_month_derived_artifacts()
                removed_count = int(cleanup_result.get("removed_count") or 0) + int(prune_result.get("removed_count") or 0)
                removed_count += int(stale_build_prune.get("removed_count") or 0)
                removed_count += int(stale_backup_prune.get("removed_count") or 0)
                removed_count += int(legacy_prune.get("removed_count") or 0)
                removed_count += int(obsolete_month_prune.get("removed_count") or 0)
                bytes_freed = int(cleanup_result.get("bytes_freed_estimate") or 0) + int(prune_result.get("bytes_freed_estimate") or 0)
                _cleanup_last_periodic_removed_count = removed_count
                _cleanup_last_periodic_freed_bytes_estimate = bytes_freed
                _cleanup_last_periodic_ran_at_unix = int(time.time())
                _reconcile_last_periodic_deleted_paths = list(prune_result.get("removed_paths") or [])
                _reconcile_last_periodic_ran_at_unix = int(time.time())
                print(
                    f"[storage-cleanup-periodic] removed={removed_count} freed_bytes_estimate={bytes_freed} "
                    f"reconcile_deleted={len(_reconcile_last_periodic_deleted_paths)}"
                )
            except Exception:
                traceback.print_exc()

    threading.Thread(target=_worker, daemon=True, name="storage-cleanup-sweeper").start()


def _weekday_name_from_mon0(dow: int) -> str:
    names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    idx = max(0, min(6, int(dow)))
    return names[idx]


def _band_from_score(score: int) -> str:
    s = max(0, min(100, int(score)))
    if s <= 34:
        return "low"
    if s <= 64:
        return "normal"
    return "high"


def _label_from_band(band: str) -> str:
    if band == "low":
        return "Low"
    if band == "high":
        return "High"
    return "Normal"




def _current_bin_index_from_dt(dt: datetime, bin_minutes: int = 20) -> int:
    total_min = dt.hour * 60 + dt.minute
    return int(total_min // bin_minutes)


def _bin_label(bin_index: int, bin_minutes: int = 20) -> str:
    minute_of_day = int(bin_index) * int(bin_minutes)
    hour24 = (minute_of_day // 60) % 24
    minute = minute_of_day % 60
    ampm = "AM" if hour24 < 12 else "PM"
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12
    return f"{hour12}:{minute:02d} {ampm}"


def _normalize_borough(name: str) -> Tuple[str, str]:
    raw = (name or "").strip().lower().replace("-", " ").replace("_", " ")
    if raw == "manhattan":
        return "Manhattan", "manhattan"
    if raw == "brooklyn":
        return "Brooklyn", "brooklyn"
    if raw == "queens":
        return "Queens", "queens"
    if raw == "bronx":
        return "Bronx", "bronx"
    if raw in {"staten island", "statenisland"}:
        return "Staten Island", "staten_island"
    if raw in {"newark airport", "newarkairport", "newark", "ewr"}:
        return "Newark Airport", "newark_airport"
    return "Unknown", "unknown"


def _resolve_borough_from_lat_lng(lat: float, lng: float) -> Optional[Dict[str, str]]:
    zones = _load_pickup_zone_geometries()
    if not zones:
        return None
    point = Point(float(lng), float(lat))
    for zone in zones.values():
        geom = zone.get("geometry")
        if geom is None:
            continue
        try:
            if geom.contains(point) or geom.touches(point):
                borough, borough_key = _normalize_borough(str(zone.get("borough") or ""))
                return {"borough": borough, "borough_key": borough_key}
        except Exception:
            continue
    return None


def _mode_flag_enabled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    s = str(value).strip().lower()
    return s in {"1", "true", "yes", "on"}


def _scope_label(scope: str) -> str:
    return {
        "citywide": "Citywide",
        "manhattan_mode": "Manhattan Mode",
        "staten_island_mode": "Staten Island Mode",
        "bronx_wash_heights_mode": "Bronx / Washington Heights Mode",
        "queens_mode": "Queens Mode",
        "brooklyn_mode": "Brooklyn Mode",
        "manhattan": "Manhattan",
        "staten_island": "Staten Island",
        "bronx": "Bronx",
        "bronx_wash_heights": "Bronx / Washington Heights",
        "queens": "Queens",
        "brooklyn": "Brooklyn",
    }.get(scope, "Citywide")


def _source_mode_for_scope(scope: str) -> str:
    if scope.endswith("_mode"):
        return scope
    return "real_location"


def resolve_tendency_scope(lat: Optional[float], lng: Optional[float], mode_flags: Dict[str, Any]) -> Dict[str, Any]:
    if lat is None or lng is None:
        return {"ready": False, "reason": "waiting_for_location"}
    try:
        lat_f = float(lat)
        lng_f = float(lng)
    except Exception:
        return {"ready": False, "reason": "invalid_location"}

    if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lng_f <= 180.0):
        return {"ready": False, "reason": "invalid_location"}

    borough_context = _resolve_borough_from_lat_lng(lat=lat_f, lng=lng_f)
    if not borough_context:
        return {"ready": False, "reason": "location_unresolved"}

    borough_key = str(borough_context.get("borough_key") or "")
    in_manhattan_core = borough_key == "manhattan" and lat_f < 40.82
    in_bronx_wash_heights = borough_key == "bronx" or (borough_key == "manhattan" and lat_f >= 40.82)

    if in_manhattan_core and _mode_flag_enabled(mode_flags.get("manhattan_mode")):
        scope = "manhattan_mode"
    elif borough_key == "staten_island" and _mode_flag_enabled(mode_flags.get("staten_island_mode")):
        scope = "staten_island_mode"
    elif in_bronx_wash_heights and _mode_flag_enabled(mode_flags.get("bronx_wash_heights_mode")):
        scope = "bronx_wash_heights_mode"
    elif borough_key == "queens" and _mode_flag_enabled(mode_flags.get("queens_mode")):
        scope = "queens_mode"
    elif borough_key == "brooklyn" and _mode_flag_enabled(mode_flags.get("brooklyn_mode")):
        scope = "brooklyn_mode"
    elif borough_key == "manhattan":
        scope = "manhattan"
    elif borough_key == "staten_island":
        scope = "staten_island"
    elif borough_key == "bronx":
        scope = "bronx_wash_heights"
    elif borough_key == "queens":
        scope = "queens"
    elif borough_key == "brooklyn":
        scope = "brooklyn"
    else:
        scope = "citywide"

    return {
        "ready": True,
        "scope": scope,
        "scope_label": _scope_label(scope),
        "borough": borough_context.get("borough"),
        "borough_key": borough_key,
        "source_mode": _source_mode_for_scope(scope),
    }


def _parse_frame_time_to_nyc(frame_time: str) -> datetime:
    raw = str(frame_time or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="frame_time is required")
    try:
        normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=NYC_TZ)
        return parsed.astimezone(NYC_TZ)
    except Exception:
        pass

    try:
        unix_ts = float(raw)
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).astimezone(NYC_TZ)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid frame_time '{raw}'. Expected ISO datetime or unix timestamp.",
        )


def _frame_time_iso_local(dt: datetime) -> str:
    return dt.astimezone(NYC_TZ).replace(microsecond=0).isoformat()


def _month_tendency_benchmark_path(month_key: str) -> Path:
    return _month_dir(str(month_key).strip()) / "tendency_benchmark.json"


def _resolve_month_key_for_tendency_benchmark(
    *,
    month_key: Optional[str] = None,
    frame_time: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    requested = str(month_key or "").strip()
    if requested:
        if not _safe_parse_month_key(requested):
            raise HTTPException(status_code=400, detail=f"Invalid month_key '{requested}'. Expected YYYY-MM.")
        return requested, None

    frame_time_raw = str(frame_time or "").strip()
    if frame_time_raw:
        resolved = _parse_frame_time_to_nyc(frame_time_raw).strftime("%Y-%m")
        return resolved, None

    active_month_key, _ = _resolve_active_month_key()
    return active_month_key, active_month_key


def _validate_month_tendency_benchmark_payload(payload: Any, requested_month_key: str) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=503, detail="month tendency benchmark payload malformed")
    payload_month_key = str(payload.get("month_key") or "").strip()
    if payload_month_key != str(requested_month_key or "").strip():
        raise HTTPException(
            status_code=503,
            detail=f"month tendency benchmark payload month mismatch for month_key={requested_month_key}",
        )
    families = payload.get("families")
    if not isinstance(families, dict) or not families:
        raise HTTPException(status_code=503, detail="month tendency benchmark payload malformed: families missing")
    return payload


def _write_month_tendency_benchmark_payload(month_key: str, payload: Dict[str, Any]) -> Path:
    benchmark_path = _month_tendency_benchmark_path(month_key)
    benchmark_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = benchmark_path.with_suffix(".json.tmp")
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    with temp_path.open("wb") as handle:
        handle.write(encoded)
        handle.flush()
        try:
            os.fsync(handle.fileno())
        except OSError:
            pass
    os.replace(temp_path, benchmark_path)
    return benchmark_path


def _build_and_persist_month_tendency_benchmark(month_key: str) -> Dict[str, Any]:
    resolved = str(month_key or "").strip()
    if not _safe_parse_month_key(resolved):
        raise HTTPException(status_code=400, detail=f"Invalid month_key '{resolved}'. Expected YYYY-MM.")

    exact_store_path = _month_store_path(resolved)
    if not (exact_store_path.exists() and exact_store_path.is_file() and exact_store_path.stat().st_size > 0):
        raise HTTPException(status_code=404, detail=f"month exact store not found for month_key={resolved}")

    zones_geojson_path = DATA_DIR / "taxi_zones.geojson"
    if not (zones_geojson_path.exists() and zones_geojson_path.is_file() and zones_geojson_path.stat().st_size > 0):
        raise HTTPException(status_code=503, detail="month tendency benchmark generation unavailable")

    try:
        from month_tendency_benchmark import build_month_tendency_benchmark

        generated_payload = build_month_tendency_benchmark(
            exact_store_path=exact_store_path,
            zones_geojson_path=zones_geojson_path,
            month_key=resolved,
            bin_minutes=int(DEFAULT_BIN_MINUTES),
        )
    except FileNotFoundError as exc:
        detail = str(exc)
        if "exact store" in detail.lower():
            raise HTTPException(status_code=404, detail=f"month exact store not found for month_key={resolved}")
        raise HTTPException(status_code=503, detail="month tendency benchmark generation unavailable")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=503, detail="month tendency benchmark generation unavailable")

    validated = _validate_month_tendency_benchmark_payload(generated_payload, resolved)
    _write_month_tendency_benchmark_payload(resolved, validated)
    return validated


def _load_month_tendency_benchmark_payload(month_key: str, *, active_month_key: str) -> Tuple[Dict[str, Any], str]:
    resolved = str(month_key or "").strip()
    benchmark_path = _month_tendency_benchmark_path(resolved)
    if benchmark_path.exists() and benchmark_path.is_file() and benchmark_path.stat().st_size > 0:
        try:
            payload = _read_json(benchmark_path)
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"month tendency benchmark file unreadable: {str(exc)}")
        return _validate_month_tendency_benchmark_payload(payload, resolved), "month_file"

    active_key = str(active_month_key or "").strip()
    artifact = load_generated_artifact("month_tendency_benchmark")
    artifact_payload = (artifact or {}).get("payload") if isinstance(artifact, dict) else None
    if isinstance(artifact_payload, dict):
        artifact_month_key = str(artifact_payload.get("month_key") or "").strip()
        if artifact_month_key == resolved:
            return _validate_month_tendency_benchmark_payload(artifact_payload, resolved), "active_mirror"

    payload = _build_and_persist_month_tendency_benchmark(resolved)
    if resolved == active_key:
        save_generated_artifact("month_tendency_benchmark", payload, compress=False)
    return payload, "generated_on_demand"


def _day_tendency_scope_kind(scope: Optional[str]) -> str:
    resolved = str(scope or "").strip()
    if not resolved:
        return "unknown"
    if resolved.endswith("_mode"):
        return "mode"
    if resolved == "citywide":
        return "citywide"
    return "borough"


def _build_day_tendency_context_unavailable(
    *,
    target_date: date,
    frame_dt: datetime,
    frame_time_iso: str,
    weekday: int,
    weekday_name: str,
    month: int,
    bin_index: int,
    bin_minutes: int,
    local_time_label: str,
    generated_at: str,
    status: str,
    label: str,
    explain: str,
    scope: Optional[str],
    scope_label: str,
    source_borough: Optional[str],
    source_mode: Optional[str],
    context_family: str,
    borough: Optional[str] = None,
    borough_key: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "version": DAY_TENDENCY_VERSION,
        "basis": "historical_expected_borough_timeslot",
        "tz": "America/New_York",
        "date": target_date.isoformat(),
        "frame_time": frame_time_iso,
        "borough": borough,
        "borough_key": borough_key,
        "weekday": weekday,
        "weekday_name": weekday_name,
        "month": month,
        "bin_index": bin_index,
        "bin_minutes": bin_minutes,
        "local_time_label": local_time_label,
        "status": status,
        "score": None,
        "band": None,
        "meter_pct": None,
        "label": label,
        "confidence": 0.0,
        "sample_bins": 0,
        "cohort_type": None,
        "components": None,
        "cohort_medians": None,
        "explain": explain,
        "generated_at": generated_at,
        "scope": scope,
        "scope_label": scope_label,
        "source_borough": source_borough,
        "source_mode": source_mode,
        "context_family": context_family,
    }


def _build_day_tendency_context_success(
    *,
    item: Dict[str, Any],
    target_date: date,
    frame_time_iso: str,
    weekday: int,
    weekday_name: str,
    month: int,
    bin_index: int,
    bin_minutes: int,
    local_time_label: str,
    generated_at: str,
    scope: str,
    scope_label: str,
    source_borough: Optional[str],
    source_mode: Optional[str],
    context_family: str,
    fallback_cohort_type: str,
    resolved: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    score = int(max(0, min(100, int(item.get("score", round(float(item.get("score_raw", 0.5)) * 100))))))
    band = _band_from_score(score)
    borough_name = item.get("borough")
    borough_key = item.get("borough_key")
    if resolved and not borough_name:
        borough_name = resolved.get("borough")
        borough_key = resolved.get("borough_key")
    return {
        "version": DAY_TENDENCY_VERSION,
        "basis": "historical_expected_borough_timeslot",
        "tz": "America/New_York",
        "date": target_date.isoformat(),
        "frame_time": frame_time_iso,
        "borough": borough_name,
        "borough_key": borough_key,
        "weekday": weekday,
        "weekday_name": weekday_name,
        "month": month,
        "bin_index": bin_index,
        "bin_minutes": bin_minutes,
        "local_time_label": str(item.get("bin_label") or local_time_label),
        "status": "ok",
        "score": score,
        "band": band,
        "meter_pct": round(score / 100.0, 4),
        "label": _label_from_band(band),
        "confidence": float(item.get("confidence", 0.0)),
        "sample_bins": int(item.get("sample_bins", 0)),
        "cohort_type": str(item.get("cohort_type") or fallback_cohort_type),
        "components": {
            "pickup_strength": float(item.get("pickup_strength", 0.5)),
            "pay_strength": float(item.get("pay_strength", 0.5)),
            "breadth_strength": float(item.get("breadth_strength", 0.5)),
        },
        "cohort_medians": {
            "pickups_bin": float(item.get("pickups_bin_avg", 0.0)),
            "avg_driver_pay_bin": float(item.get("avg_driver_pay_bin_avg", 0.0)),
            "active_zones_bin": float(item.get("active_zones_bin_avg", 0.0)),
        },
        "explain": str(item.get("explain", "")),
        "generated_at": generated_at,
        "scope": scope,
        "scope_label": scope_label,
        "source_borough": source_borough,
        "source_mode": source_mode,
        "context_family": context_family,
    }


def _resolve_global_day_tendency_context(
    *,
    model: Dict[str, Any],
    target_date: date,
    frame_dt: datetime,
    frame_time_iso: str,
    generated_at: str,
    weekday: int,
    weekday_name: str,
    month: int,
    bin_index: int,
    bin_minutes: int,
    local_time_label: str,
) -> Dict[str, Any]:
    global_bin = model.get("global_bin") or {}
    global_baseline = model.get("global_baseline") or {}
    key_global_bin = f"{bin_index}"
    if isinstance(global_bin, dict) and key_global_bin in global_bin:
        return _build_day_tendency_context_success(
            item=global_bin[key_global_bin],
            target_date=target_date,
            frame_time_iso=frame_time_iso,
            weekday=weekday,
            weekday_name=weekday_name,
            month=month,
            bin_index=bin_index,
            bin_minutes=bin_minutes,
            local_time_label=local_time_label,
            generated_at=generated_at,
            scope="citywide",
            scope_label=_scope_label("citywide"),
            source_borough=None,
            source_mode="citywide",
            context_family="global",
            fallback_cohort_type="global_bin",
            resolved=None,
        )
    if isinstance(global_baseline, dict) and global_baseline:
        return _build_day_tendency_context_success(
            item=global_baseline,
            target_date=target_date,
            frame_time_iso=frame_time_iso,
            weekday=weekday,
            weekday_name=weekday_name,
            month=month,
            bin_index=bin_index,
            bin_minutes=bin_minutes,
            local_time_label=local_time_label,
            generated_at=generated_at,
            scope="citywide",
            scope_label=_scope_label("citywide"),
            source_borough=None,
            source_mode="citywide",
            context_family="global",
            fallback_cohort_type="global_baseline",
            resolved=None,
        )
    return _build_day_tendency_context_unavailable(
        target_date=target_date,
        frame_dt=frame_dt,
        frame_time_iso=frame_time_iso,
        weekday=weekday,
        weekday_name=weekday_name,
        month=month,
        bin_index=bin_index,
        bin_minutes=bin_minutes,
        local_time_label=local_time_label,
        generated_at=generated_at,
        status="insufficient_data",
        label="No data",
        explain="No global day tendency cohort data available.",
        scope="citywide",
        scope_label=_scope_label("citywide"),
        source_borough=None,
        source_mode="citywide",
        context_family="global",
    )


def _resolve_local_day_tendency_context(
    *,
    model: Dict[str, Any],
    target_date: date,
    frame_dt: datetime,
    frame_time_iso: str,
    generated_at: str,
    resolved_scope: Dict[str, Any],
    weekday: int,
    weekday_name: str,
    month: int,
    bin_index: int,
    bin_minutes: int,
    local_time_label: str,
) -> Dict[str, Any]:
    if not resolved_scope.get("ready"):
        reason = str(resolved_scope.get("reason") or "waiting_for_location")
        payload = _build_day_tendency_context_unavailable(
            target_date=target_date,
            frame_dt=frame_dt,
            frame_time_iso=frame_time_iso,
            weekday=weekday,
            weekday_name=weekday_name,
            month=month,
            bin_index=bin_index,
            bin_minutes=bin_minutes,
            local_time_label=local_time_label,
            generated_at=generated_at,
            status=reason,
            label="Waiting" if reason == "waiting_for_location" else "No data",
            explain="Waiting for valid GPS location." if reason == "waiting_for_location" else "Unable to resolve local scope.",
            scope=None,
            scope_label="Waiting for location",
            source_borough=None,
            source_mode=None,
            context_family="local",
        )
        payload.update(
            {
                "source_model_layer": None,
                "source_scope_specificity": None,
                "fallback_level": None,
                "context_specificity_weight": 1.0,
                "scope_kind": None,
                "exact_scope_specific": False,
                "broad_scope_fallback": False,
            }
        )
        return payload

    scope_name = str(resolved_scope.get("scope") or "citywide")
    scope_kind = _day_tendency_scope_kind(scope_name)
    scopes = model.get("scopes") or {}
    scoped_model = scopes.get(scope_name) if isinstance(scopes, dict) else None
    borough_key = str(resolved_scope.get("borough_key") or "")
    borough_context = {
        "borough": resolved_scope.get("borough"),
        "borough_key": borough_key,
    }
    key_weekday = f"{borough_key}|{weekday}|{bin_index}"
    key_bin = f"{borough_key}|{bin_index}"
    scoped_weekday_bin = (scoped_model or {}).get("borough_weekday_bin") or {}
    scoped_bin = (scoped_model or {}).get("borough_bin") or {}
    scoped_baseline = (scoped_model or {}).get("borough_baseline") or {}
    root_weekday_bin = model.get("borough_weekday_bin") or {}
    root_bin = model.get("borough_bin") or {}
    root_baseline = model.get("borough_baseline") or {}
    if scope_kind == "mode":
        candidates = [
            ("scope", "exact_scope_weekday_bin", 0, 1.00, True, False, "borough_weekday_bin", scoped_weekday_bin, key_weekday),
            ("scope", "exact_scope_bin", 1, 0.88, True, False, "borough_bin", scoped_bin, key_bin),
            ("scope", "exact_scope_baseline", 2, 0.72, True, False, "borough_baseline", scoped_baseline, borough_key),
            ("root", "root_borough_weekday_bin_fallback", 3, 0.58, False, True, "borough_weekday_bin", root_weekday_bin, key_weekday),
            ("root", "root_borough_bin_fallback", 4, 0.44, False, True, "borough_bin", root_bin, key_bin),
            ("root", "root_borough_baseline_fallback", 5, 0.30, False, True, "borough_baseline", root_baseline, borough_key),
        ]
    else:
        candidates = [
            ("root", "borough_scope_weekday_bin", 0, 1.00, True, False, "borough_weekday_bin", root_weekday_bin, key_weekday),
            ("root", "borough_scope_bin", 1, 0.88, True, False, "borough_bin", root_bin, key_bin),
            ("root", "borough_scope_baseline", 2, 0.72, True, False, "borough_baseline", root_baseline, borough_key),
        ]

    for source_model_layer, source_scope_specificity, fallback_level, context_specificity_weight, exact_scope_specific, broad_scope_fallback, fallback_cohort_type, data_source, key in candidates:
        if not isinstance(data_source, dict) or key not in data_source:
            continue
        payload = _build_day_tendency_context_success(
            item=data_source[key],
            target_date=target_date,
            frame_time_iso=frame_time_iso,
            weekday=weekday,
            weekday_name=weekday_name,
            month=month,
            bin_index=bin_index,
            bin_minutes=bin_minutes,
            local_time_label=local_time_label,
            generated_at=generated_at,
            scope=scope_name,
            scope_label=_scope_label(scope_name),
            source_borough=resolved_scope.get("borough"),
            source_mode=resolved_scope.get("source_mode"),
            context_family="local",
            fallback_cohort_type=fallback_cohort_type,
            resolved=borough_context,
        )
        payload.update(
            {
                "source_model_layer": source_model_layer,
                "source_scope_specificity": source_scope_specificity,
                "fallback_level": fallback_level,
                "context_specificity_weight": context_specificity_weight,
                "scope_kind": scope_kind,
                "exact_scope_specific": exact_scope_specific,
                "broad_scope_fallback": broad_scope_fallback,
            }
        )
        return payload
    payload = _build_day_tendency_context_unavailable(
        target_date=target_date,
        frame_dt=frame_dt,
        frame_time_iso=frame_time_iso,
        weekday=weekday,
        weekday_name=weekday_name,
        month=month,
        bin_index=bin_index,
        bin_minutes=bin_minutes,
        local_time_label=local_time_label,
        generated_at=generated_at,
        status="insufficient_data",
        label="No data",
        explain="No local day tendency cohort data available for resolved scope.",
        scope=scope_name,
        scope_label=_scope_label(scope_name),
        source_borough=resolved_scope.get("borough"),
        source_mode=resolved_scope.get("source_mode"),
        context_family="local",
        borough=resolved_scope.get("borough"),
        borough_key=resolved_scope.get("borough_key"),
    )
    payload.update(
        {
            "source_model_layer": None,
            "source_scope_specificity": None,
            "fallback_level": None,
            "context_specificity_weight": 1.0,
            "scope_kind": scope_kind,
            "exact_scope_specific": False,
            "broad_scope_fallback": False,
        }
    )
    return payload


def _day_tendency_context_breakdown(
    context: Dict[str, Any],
    cohort_weights: Dict[str, float],
    *,
    apply_specificity_weight: bool = False,
) -> Dict[str, Any]:
    score = context.get("score")
    confidence = float(context.get("confidence") or 0.0)
    cohort_type = str(context.get("cohort_type") or "")
    base_cohort_weight = float(cohort_weights.get(cohort_type, 0.0))
    context_specificity_weight = (
        float(context.get("context_specificity_weight", 1.0)) if apply_specificity_weight else 1.0
    )
    effective_cohort_weight = base_cohort_weight * context_specificity_weight
    weakness = 0.0
    if score is not None:
        score_f = float(score)
        if score_f < DAY_TENDENCY_CONTEXT_WEAKNESS_SCORE_START:
            weakness = (DAY_TENDENCY_CONTEXT_WEAKNESS_SCORE_START - score_f) / DAY_TENDENCY_CONTEXT_WEAKNESS_SCORE_SPAN
            weakness = max(0.0, min(1.0, weakness))
    cooling_strength_pre_specificity = weakness * confidence * base_cohort_weight
    cooling_strength = weakness * confidence * effective_cohort_weight
    return {
        "status": context.get("status"),
        "score": score,
        "confidence": confidence,
        "cohort_type": cohort_type or None,
        "cohort_weight": effective_cohort_weight,
        "base_cohort_weight": base_cohort_weight,
        "context_specificity_weight": context_specificity_weight,
        "effective_cohort_weight": effective_cohort_weight,
        "weakness": round(weakness, 6),
        "cooling_strength_pre_specificity": round(cooling_strength_pre_specificity, 6),
        "cooling_strength": round(cooling_strength, 6),
        "source_scope_specificity": context.get("source_scope_specificity"),
        "source_model_layer": context.get("source_model_layer"),
        "broad_scope_fallback": bool(context.get("broad_scope_fallback")),
    }


def _build_day_tendency_advanced_context(
    *,
    frame_time_iso: str,
    frame_date: str,
    frame_weekday: int,
    frame_weekday_name: str,
    frame_bin_index: int,
    frame_bin_minutes: int,
    frame_local_time_label: str,
    global_context: Dict[str, Any],
    local_context: Dict[str, Any],
    resolved_scope: Dict[str, Any],
) -> Dict[str, Any]:
    global_breakdown = _day_tendency_context_breakdown(global_context, DAY_TENDENCY_CONTEXT_GLOBAL_COHORT_WEIGHTS)
    local_breakdown = _day_tendency_context_breakdown(
        local_context,
        DAY_TENDENCY_CONTEXT_LOCAL_COHORT_WEIGHTS,
        apply_specificity_weight=True,
    )
    global_penalty_points = int(round(DAY_TENDENCY_CONTEXT_GLOBAL_PENALTY_CAP * float(global_breakdown["cooling_strength"])))
    local_penalty_points = int(round(DAY_TENDENCY_CONTEXT_LOCAL_PENALTY_CAP * float(local_breakdown["cooling_strength"])))
    combined_penalty_points = min(
        DAY_TENDENCY_CONTEXT_TOTAL_PENALTY_CAP,
        global_penalty_points + local_penalty_points,
    )
    ready = bool(global_context.get("status") == "ok" or local_context.get("status") == "ok")
    global_context_ready = bool(global_context.get("status") == "ok")
    local_context_ready = bool(local_context.get("status") == "ok")
    has_nonzero_penalty = combined_penalty_points > 0
    local_scope = resolved_scope.get("scope") if resolved_scope.get("ready") else None
    local_scope_label = resolved_scope.get("scope_label") if resolved_scope.get("ready") else "Waiting for location"
    return {
        "status": "ok",
        "frame_time": frame_time_iso,
        "frame_date": frame_date,
        "frame_weekday": frame_weekday,
        "frame_weekday_name": frame_weekday_name,
        "frame_bin_index": frame_bin_index,
        "frame_bin_minutes": frame_bin_minutes,
        "frame_local_time_label": frame_local_time_label,
        "weakness_threshold_score": DAY_TENDENCY_CONTEXT_WEAKNESS_SCORE_START,
        "weakness_full_scale_span": DAY_TENDENCY_CONTEXT_WEAKNESS_SCORE_SPAN,
        "global_penalty_cap": DAY_TENDENCY_CONTEXT_GLOBAL_PENALTY_CAP,
        "local_penalty_cap": DAY_TENDENCY_CONTEXT_LOCAL_PENALTY_CAP,
        "total_penalty_cap": DAY_TENDENCY_CONTEXT_TOTAL_PENALTY_CAP,
        "bucket_drop_cap": DAY_TENDENCY_CONTEXT_BUCKET_DROP_CAP,
        "global_cooling_strength": global_breakdown["cooling_strength"],
        "local_cooling_strength": local_breakdown["cooling_strength"],
        "global_penalty_points": global_penalty_points,
        "local_penalty_points": local_penalty_points,
        "combined_penalty_points": combined_penalty_points,
        "apply_global_everywhere": True,
        "apply_local_only_to_matching_scope": True,
        "ready_for_frontend_adjustment": ready,
        "global_context_ready": global_context_ready,
        "local_context_ready": local_context_ready,
        "has_nonzero_penalty": has_nonzero_penalty,
        "local_scope": local_scope,
        "local_scope_label": local_scope_label,
        "local_scope_kind": _day_tendency_scope_kind(local_scope),
        "resolved_local_scope": local_scope,
        "resolved_local_scope_kind": _day_tendency_scope_kind(local_scope),
        "local_source_borough": resolved_scope.get("borough") if resolved_scope.get("ready") else None,
        "local_source_mode": resolved_scope.get("source_mode") if resolved_scope.get("ready") else None,
        "local_context_source_scope_specificity": local_context.get("source_scope_specificity"),
        "local_context_source_model_layer": local_context.get("source_model_layer"),
        "local_context_context_specificity_weight": float(local_context.get("context_specificity_weight", 1.0)),
        "local_context_exact_scope_specific": bool(local_context.get("exact_scope_specific")),
        "local_context_broad_scope_fallback": bool(local_context.get("broad_scope_fallback")),
        "global_breakdown": {
            **global_breakdown,
            "penalty_cap": DAY_TENDENCY_CONTEXT_GLOBAL_PENALTY_CAP,
            "penalty_points": global_penalty_points,
        },
        "local_breakdown": {
            **local_breakdown,
            "penalty_cap": DAY_TENDENCY_CONTEXT_LOCAL_PENALTY_CAP,
            "penalty_points": local_penalty_points,
        },
    }


def _resolve_day_tendency_payload(
    target_date: date,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    mode_flags: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    model = _read_day_tendency_model()
    _debug_log("[debug] model keys:", list(model.keys()))
    generated_at = model.get("generated_at") or datetime.now(timezone.utc).isoformat()
    bin_minutes = int(model.get("bin_minutes") or 20)
    resolved_scope = resolve_tendency_scope(lat=lat, lng=lng, mode_flags=mode_flags or {})
    borough_context = None
    if resolved_scope.get("ready"):
        borough_context = {
            "borough": resolved_scope.get("borough"),
            "borough_key": resolved_scope.get("borough_key"),
        }
    _debug_log("[debug] resolved scope:", resolved_scope)

    def insufficient() -> Dict[str, Any]:
        return {
            "version": DAY_TENDENCY_VERSION,
            "basis": "historical_expected_borough_timeslot",
            "tz": "America/New_York",
            "date": target_date.isoformat(),
            "status": "insufficient_data",
            "score": None,
            "band": None,
            "meter_pct": None,
            "label": "No data",
            "confidence": 0.0,
            "sample_bins": 0,
            "scope": resolved_scope.get("scope") if resolved_scope.get("ready") else None,
            "scope_label": resolved_scope.get("scope_label") if resolved_scope.get("ready") else "Waiting for location",
            "source_borough": resolved_scope.get("borough") if resolved_scope.get("ready") else None,
            "source_mode": resolved_scope.get("source_mode") if resolved_scope.get("ready") else None,
            "generated_at": generated_at,
        }

    if not resolved_scope.get("ready"):
        return {
            "version": DAY_TENDENCY_VERSION,
            "basis": "historical_expected_borough_timeslot",
            "tz": "America/New_York",
            "date": target_date.isoformat(),
            "status": "waiting_for_location",
            "score": None,
            "band": None,
            "meter_pct": None,
            "label": "Waiting",
            "confidence": 0.0,
            "sample_bins": 0,
            "scope": None,
            "scope_label": "Waiting for location",
            "source_borough": None,
            "source_mode": None,
            "explain": "Waiting for valid GPS location.",
            "generated_at": generated_at,
        }

    if model.get("status") == "insufficient_data":
        _debug_log("[debug] using path=insufficient_data")
        return insufficient()

    now_nyc = datetime.now(NYC_TZ)
    bin_index = _current_bin_index_from_dt(now_nyc, bin_minutes=bin_minutes)

    weekday = target_date.weekday()
    weekday_name = _weekday_name_from_mon0(weekday)
    bin_label = _bin_label(bin_index, bin_minutes=bin_minutes)
    month = int(target_date.month)

    scope_name = str(resolved_scope.get("scope") or "citywide")
    scopes = model.get("scopes") or {}
    scoped_model = scopes.get(scope_name) if isinstance(scopes, dict) else None
    borough_weekday_bin = (scoped_model or {}).get("borough_weekday_bin") or model.get("borough_weekday_bin") or {}
    borough_bin = (scoped_model or {}).get("borough_bin") or model.get("borough_bin") or {}
    borough_baseline = (scoped_model or {}).get("borough_baseline") or model.get("borough_baseline") or {}
    global_bin = (scoped_model or {}).get("global_bin") or model.get("global_bin") or {}
    global_baseline = (scoped_model or {}).get("global_baseline") or model.get("global_baseline") or {}

    _debug_log("[debug] day_tendency cohort sizes:", {
        "borough_weekday_bin": len(borough_weekday_bin) if isinstance(borough_weekday_bin, dict) else 0,
        "borough_bin": len(borough_bin) if isinstance(borough_bin, dict) else 0,
        "borough_baseline": len(borough_baseline) if isinstance(borough_baseline, dict) else 0,
        "global_bin": len(global_bin) if isinstance(global_bin, dict) else 0,
        "has_global_baseline": bool(global_baseline),
    })

    def from_item(item: Dict[str, Any], fallback_cohort_type: str | None = None, resolved: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        score = int(max(0, min(100, int(item.get("score", round(float(item.get("score_raw", 0.5)) * 100))))))
        band = _band_from_score(score)
        borough_name = item.get("borough")
        borough_key = item.get("borough_key")
        if resolved and not borough_name:
            borough_name = resolved.get("borough")
            borough_key = resolved.get("borough_key")
        return {
            "version": DAY_TENDENCY_VERSION,
            "basis": "historical_expected_borough_timeslot",
            "tz": "America/New_York",
            "date": target_date.isoformat(),
            "borough": borough_name,
            "borough_key": borough_key,
            "weekday": weekday,
            "weekday_name": weekday_name,
            "month": month,
            "bin_index": bin_index,
            "bin_minutes": bin_minutes,
            "local_time_label": str(item.get("bin_label") or bin_label),
            "score": score,
            "band": band,
            "meter_pct": round(score / 100.0, 4),
            "label": _label_from_band(band),
            "confidence": float(item.get("confidence", 0.0)),
            "sample_bins": int(item.get("sample_bins", 0)),
            "cohort_type": str(item.get("cohort_type") or fallback_cohort_type or "bin_only"),
            "components": {
                "pickup_strength": float(item.get("pickup_strength", 0.5)),
                "pay_strength": float(item.get("pay_strength", 0.5)),
                "breadth_strength": float(item.get("breadth_strength", 0.5)),
            },
            "cohort_medians": {
                "pickups_bin": float(item.get("pickups_bin_avg", 0.0)),
                "avg_driver_pay_bin": float(item.get("avg_driver_pay_bin_avg", 0.0)),
                "active_zones_bin": float(item.get("active_zones_bin_avg", 0.0)),
            },
            "explain": str(item.get("explain", "")),
            "generated_at": generated_at,
            "scope": scope_name,
            "scope_label": _scope_label(scope_name),
            "source_borough": resolved_scope.get("borough"),
            "source_mode": resolved_scope.get("source_mode"),
        }

    if borough_context:
        borough_key = str(borough_context["borough_key"])
        key_weekday = f"{borough_key}|{weekday}|{bin_index}"
        key_bin = f"{borough_key}|{bin_index}"
        key_borough = borough_key
        if isinstance(borough_weekday_bin, dict) and key_weekday in borough_weekday_bin:
            _debug_log("[debug] using path=borough_weekday_bin")
            return from_item(borough_weekday_bin[key_weekday], fallback_cohort_type="borough_weekday_bin", resolved=borough_context)
        if isinstance(borough_bin, dict) and key_bin in borough_bin:
            _debug_log("[debug] using path=borough_bin")
            return from_item(borough_bin[key_bin], fallback_cohort_type="borough_bin", resolved=borough_context)
        if isinstance(borough_baseline, dict) and key_borough in borough_baseline:
            _debug_log("[debug] using path=borough_baseline")
            return from_item(borough_baseline[key_borough], fallback_cohort_type="borough_baseline", resolved=borough_context)

    key_global_bin = f"{bin_index}"
    if isinstance(global_bin, dict) and key_global_bin in global_bin:
        _debug_log("[debug] using path=global_bin")
        return from_item(global_bin[key_global_bin], fallback_cohort_type="global_bin", resolved=borough_context)
    if isinstance(global_baseline, dict) and global_baseline:
        _debug_log("[debug] using path=global_baseline")
        return from_item(global_baseline, fallback_cohort_type="global_baseline", resolved=borough_context)

    _debug_log("[debug] using path=insufficient_data")
    return insufficient()


def _build_day_tendency_only(bin_minutes: int = DEFAULT_BIN_MINUTES) -> Dict[str, Any]:
    from build_day_tendency import build_day_tendency_model
    from build_hotspot import ensure_zones_geojson

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DAY_TENDENCY_DIR.mkdir(parents=True, exist_ok=True)

    parquets = _list_parquets()
    if not parquets:
        raise RuntimeError("No .parquet files found in /data. Cannot build day tendency model.")
    zones_path = ensure_zones_geojson(DATA_DIR, force=False)

    result = build_day_tendency_model(
        parquet_files=parquets,
        out_dir=DAY_TENDENCY_DIR,
        zones_geojson_path=zones_path,
        bin_minutes=bin_minutes,
        persist_file=True,
    )
    try:
        model_payload = ((result or {}).get("payload") if isinstance(result, dict) else None) or _read_day_tendency_model()
        freshness = active_month_freshness_report(
            month_key=resolve_active_month_key(
                datetime.now(timezone.utc).astimezone(NYC_TZ),
                _available_source_month_keys(),
            )
            or "",
            exact_history_months_dir=EXACT_HISTORY_MONTHS_DIR,
            repo_root=Path(__file__).resolve().parent,
            data_dir=DATA_DIR,
            frames_dir=FRAMES_DIR,
            bin_minutes=int(DEFAULT_BIN_MINUTES),
            min_trips_per_window=int(DEFAULT_MIN_TRIPS_PER_WINDOW),
        )
        expected = freshness.get("expected") or {}
        if isinstance(model_payload, dict):
            model_payload["code_dependency_hash"] = expected.get("code_dependency_hash")
            model_payload["source_data_hash"] = expected.get("source_data_hash")
            model_payload["artifact_signature"] = expected.get("artifact_signature")
        save_generated_artifact("day_tendency_model", model_payload, compress=False)
        _prune_redundant_db_backed_artifact_files()
    except Exception:
        print("[warn] unable to persist day tendency model into generated_artifact_store")
        print(traceback.format_exc())
    return result

#
# Maintenance note: this helper is not the live /assistant/outlook request path.
# Live requests use the monthly timeline + frame cache + loader-based bucket path.
# Do not use this helper for request-time assistant outlook serving.
def _build_assistant_outlook_only() -> Dict[str, Any]:
    timeline_artifact = load_generated_artifact("timeline")
    if not timeline_artifact and (not TIMELINE_PATH.exists() or TIMELINE_PATH.stat().st_size <= 0):
        raise RuntimeError("timeline.json missing. Cannot serve assistant outlook.")
    if not EXACT_HISTORY_DB_PATH.exists() or EXACT_HISTORY_DB_PATH.stat().st_size <= 0:
        raise RuntimeError("exact history store missing. Cannot serve assistant outlook.")
    timeline_payload = (timeline_artifact or {}).get("payload") or _read_json(TIMELINE_PATH)
    timeline_count = int(timeline_payload.get("count") or len(timeline_payload.get("timeline") or []))
    _prune_legacy_assistant_outlook_artifact_if_present()
    _prune_assistant_outlook_file_if_db_ready()
    _prune_redundant_db_backed_artifact_files()
    return {
        "ok": True,
        "mode": "on_demand_frame_bucket",
        "timeline_present": True,
        "frame_count": timeline_count,
    }


def _write_lock(run_token: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"ts": int(time.time()), "token": str(run_token)}
    LOCK_PATH.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def _clear_lock(expected_token: Optional[str] = None) -> None:
    if expected_token is not None:
        current_token = _read_lock_token()
        if current_token != str(expected_token):
            return
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink()
    except Exception:
        pass


def _lock_is_present() -> bool:
    return LOCK_PATH.exists()


def _read_lock_timestamp() -> int | None:
    try:
        raw = LOCK_PATH.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
        if isinstance(payload, dict):
            ts = payload.get("ts")
            if isinstance(ts, bool):
                return None
            if isinstance(ts, (int, float)):
                return int(ts)
            if isinstance(ts, str) and ts.strip():
                return int(ts.strip())
    except Exception:
        pass
    try:
        return int(raw)
    except Exception:
        return None


def _read_lock_token() -> Optional[str]:
    try:
        raw = LOCK_PATH.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    token = payload.get("token")
    if token is None:
        return None
    token_text = str(token).strip()
    return token_text or None


def _lock_age_seconds() -> int | None:
    ts = _read_lock_timestamp()
    if ts is None:
        return None
    try:
        return max(0, int(time.time()) - int(ts))
    except Exception:
        return None


def _clear_stale_lock(max_age_sec: int = 7200) -> bool:
    if not _lock_is_present():
        return False
    age_seconds = _lock_age_seconds()
    should_clear = age_seconds is None or age_seconds > int(max_age_sec)
    if not should_clear:
        return False
    _clear_lock()
    return True


def _clear_orphaned_generate_lock() -> bool:
    if _lock_is_present() and not _generate_thread_alive():
        _clear_lock()
        state_now = _get_state()
        if str(state_now.get("state") or "") == "running":
            _set_state(state="idle")
        print("generate_lock_orphaned_cleared")
        return True
    return False


def _clear_stale_generate_lock_if_orphaned() -> bool:
    return _clear_orphaned_generate_lock()


def _generate_thread_alive() -> bool:
    return bool(_generate_thread and _generate_thread.is_alive())


def _generate_lock_snapshot() -> Dict[str, Any]:
    state = _get_state().get("state")
    return {
        "lock_present": _lock_is_present(),
        "lock_age_seconds": _lock_age_seconds(),
        "thread_alive": _generate_thread_alive(),
        "state": state,
    }


def _bootstrap_month_partition(month_key: str, bin_minutes: int = DEFAULT_BIN_MINUTES) -> Dict[str, Any]:
    from build_hotspot import build_month_timeline_bootstrap, ensure_zones_geojson

    requested = str(month_key or "").strip()
    if not _safe_parse_month_key(requested):
        raise RuntimeError(f"Invalid month_key for bootstrap: {requested}")
    parquets = _source_parquets_for_month(requested)
    if not parquets:
        raise RuntimeError(f"No source parquet files found for month_key={requested}")
    ensure_zones_geojson(DATA_DIR, force=False)
    month_dir = _month_dir(requested)
    month_dir.mkdir(parents=True, exist_ok=True)
    timeline_payload = build_month_timeline_bootstrap(requested, bin_minutes=int(bin_minutes))
    timeline_count = int(timeline_payload.get("count") or len(timeline_payload.get("timeline") or []))
    timeline_path = _month_timeline_path(requested)
    timeline_path.write_text(json.dumps(timeline_payload, separators=(",", ":")), encoding="utf-8")
    _month_frame_cache_dir(requested).mkdir(parents=True, exist_ok=True)

    months_manifest: Dict[str, Dict[str, Any]] = dict((_load_month_manifest().get("months") or {}))
    months_manifest[requested] = _month_manifest_entry_payload(requested, existing={"source_parquet_filenames": [p.name for p in parquets]})
    _persist_month_manifest(months_manifest)
    _prune_legacy_frame_files_after_monthly_ready()
    return {
        "ok": True,
        "month_key": requested,
        "timeline_count": timeline_count,
        "frame_cache_dir": str(_month_frame_cache_dir(requested)),
    }


def _ensure_requested_month_available_or_start_generate(
    *,
    month_key: str,
    request_kind: str,
    retry_after_sec: int = 3,
) -> Optional[JSONResponse]:
    _clear_stale_generate_lock_if_orphaned()
    live_bootstrap = _ensure_month_live_bootstrap(month_key)
    state = _month_bootstrap_state(month_key)
    if bool(state.get("live_ready")) and _month_attestation_needed(month_key):
        _queue_active_month_attestation(month_key)
    if bool(state.get("live_ready") or state.get("exact_store_fresh") or state.get("legacy_ready_without_build_meta")):
        return None
    generate_started = False if live_bootstrap.get("source_parquet_exists") else False
    return _preparing_month_response(
        month_key=month_key,
        request_kind=request_kind,
        generate_started=generate_started,
        retry_after_sec=retry_after_sec,
    )


def _set_state(**kwargs):
    with _state_lock:
        _generate_state.update(kwargs)


def _get_state() -> Dict[str, Any]:
    with _state_lock:
        return dict(_generate_state)


def _generate_worker(
    bin_minutes: int,
    min_trips_per_window: int,
    run_token: str,
    include_day_tendency: bool = False,
    build_review_artifacts: bool = False,
    month_key: Optional[str] = None,
    build_all_months: bool = False,
) -> None:
    from build_hotspot import ensure_zones_geojson, build_hotspots_frames

    global _last_failed_month_key, _last_failed_at_unix, _last_failed_error
    start = time.time()
    failed_month_key_candidate = str(month_key).strip() if month_key else None
    _set_state(
        state="running",
        bin_minutes=bin_minutes,
        min_trips_per_window=min_trips_per_window,
        run_token=run_token,
        month_key=(str(month_key).strip() if month_key else None),
        build_all_months=bool(build_all_months),
        include_day_tendency=bool(include_day_tendency),
        build_review_artifacts=bool(build_review_artifacts),
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
        _prune_stale_month_build_dirs()
        _prune_stale_month_backup_dirs()

        zones_path = ensure_zones_geojson(DATA_DIR, force=False)

        parquets = _list_parquets()
        if not parquets:
            raise RuntimeError("No .parquet files found in /data. Upload via POST /upload_parquet.")
        inventory = inspect_parquet_inventory(parquets)
        _parquet_inventory_snapshot.clear()
        _parquet_inventory_snapshot.update(inventory)
        if int(inventory.get("warning_count") or 0) > 0:
            raise RuntimeError(
                "Parquet inventory validation failed due to potential duplicate/overlapping files: "
                + "; ".join(inventory.get("warnings") or [])
            )

        grouped_parquets = _group_parquets_by_month(parquets)
        if not grouped_parquets:
            raise RuntimeError("No month-keyed parquet files found for monthly exact-history build.")
        requested_month_key = str(month_key or "").strip() or None
        if requested_month_key:
            if requested_month_key not in grouped_parquets:
                raise RuntimeError(f"Requested month_key not found in parquet inventory: {requested_month_key}")
            build_month_keys = [requested_month_key]
        elif bool(build_all_months):
            build_month_keys = sorted(grouped_parquets.keys())
        else:
            resolved = resolve_active_month_key(datetime.now(timezone.utc).astimezone(NYC_TZ), sorted(grouped_parquets.keys()))
            if not resolved:
                raise RuntimeError("Unable to resolve active month_key from available parquet files.")
            build_month_keys = [resolved]
        if build_month_keys:
            failed_month_key_candidate = str(build_month_keys[0]).strip()
        months_manifest: Dict[str, Dict[str, Any]] = dict((_load_month_manifest().get("months") or {}))
        build_results: Dict[str, Any] = {}
        source_target_month_key = resolve_active_month_key(
            datetime.now(timezone.utc).astimezone(NYC_TZ),
            sorted(grouped_parquets.keys()),
        )
        for mk in build_month_keys:
            print(f"monthly_partition_build_start month_key={mk}")
            month_dir = _month_dir(mk)
            stage_dir = EXACT_HISTORY_MONTHS_BUILDING_DIR / f"{mk}__{run_token}"
            backup_dir = EXACT_HISTORY_MONTHS_BACKUP_DIR / mk
            month_result = build_hotspots_frames(
                parquet_files=grouped_parquets.get(mk) or [],
                zones_geojson_path=zones_path,
                out_dir=month_dir,
                bin_minutes=bin_minutes,
                min_trips_per_window=min_trips_per_window,
                exact_history_dir=month_dir,
                exact_history_stage_dir=stage_dir,
                exact_history_backup_dir=backup_dir,
                timeline_output_path=month_dir / "timeline.json",
                cleanup_out_dir_frames=False,
                month_key=mk,
                build_review_artifacts=bool(build_review_artifacts),
            )
            timeline_path = month_dir / "timeline.json"
            timeline_payload = _read_json(timeline_path) if timeline_path.exists() else {}
            timeline = timeline_payload.get("timeline") or []
            print(f"monthly_partition_build_done month_key={mk} frame_count={len(timeline)}")
            print(f"monthly_partition_publish_done month_key={mk}")
            store_path = month_dir / "exact_shadow.duckdb"
            store_exists = bool(store_path.exists() and store_path.is_file() and store_path.stat().st_size > 0)
            timeline_exists = bool(timeline_path.exists() and timeline_path.is_file() and timeline_path.stat().st_size > 0)
            if not store_exists or not timeline_exists:
                raise RuntimeError(f"Monthly publish verification failed for {mk}")
            build_meta = load_month_build_meta(EXACT_HISTORY_MONTHS_DIR, mk)
            if not isinstance(build_meta, dict):
                raise RuntimeError(
                    f"Monthly publish contract failed for {mk}: build_meta.json missing after successful publish verification."
                )
            freshness = _active_month_freshness(mk)
            if not bool(freshness.get("build_meta_present")):
                raise RuntimeError(
                    f"Monthly publish contract failed for {mk}: freshness gate did not detect build_meta.json."
                )
            months_manifest[mk] = _month_manifest_entry_payload(
                mk,
                existing={"source_parquet_filenames": [p.name for p in (grouped_parquets.get(mk) or [])]},
            )
            if str(mk).strip() == str(source_target_month_key or "").strip() and bool(freshness.get("signature_match")):
                stale_removed = _purge_month_frame_cache(mk)
                print(f"monthly_partition_frame_cache_purged month_key={mk} removed={stale_removed}")
            build_results[mk] = month_result
        manifest_payload = _persist_month_manifest(months_manifest)
        _prune_legacy_frame_files_after_monthly_ready()
        rebuilt_day_tendency_result: Dict[str, Any] = {
            "ok": False,
            "skipped": True,
            "warning": None,
        }
        try:
            rebuilt_day_tendency_result = _build_day_tendency_only(bin_minutes)
            if isinstance(rebuilt_day_tendency_result, dict):
                rebuilt_day_tendency_result.setdefault("skipped", False)
        except Exception as day_tendency_exc:
            print("[warn] automatic day_tendency rebuild after monthly publish failed")
            print(traceback.format_exc())
            rebuilt_day_tendency_result = {
                "ok": False,
                "skipped": False,
                "warning": f"post_publish_day_tendency_failed: {str(day_tendency_exc)}",
            }
        active_month_key = resolve_active_month_key(
            datetime.now(timezone.utc).astimezone(NYC_TZ),
            list(manifest_payload.get("available_month_keys") or []),
        )
        frames_result = {
            "ok": True,
            "mode": "monthly_exact_historical",
            "built_month_keys": build_month_keys,
            "active_month_key": active_month_key,
            "available_month_keys": list(manifest_payload.get("available_month_keys") or []),
            "month_results": build_results,
        }
        day_tendency_result: Dict[str, Any] = dict(rebuilt_day_tendency_result or {})
        day_tendency_result["trigger"] = "post_month_publish"
        if include_day_tendency:
            day_tendency_result["requested_include_day_tendency"] = True
        result = {
            "frames": frames_result,
            "day_tendency": day_tendency_result,
            "include_day_tendency": bool(include_day_tendency),
            "build_review_artifacts": bool(build_review_artifacts),
            "timeline_mode": "monthly_exact_historical",
            "frame_time_model": "exact_local_20min",
            "synthetic_week_enabled": False,
            "parquet_inventory": inventory,
            "storage_report": get_artifact_storage_report(DATA_DIR, FRAMES_DIR),
        }

        end = time.time()
        _set_state(
            state="done",
            finished_at_unix=end,
            duration_sec=round(end - start, 2),
            result=result,
        )
        if _last_failed_month_key and str(_last_failed_month_key).strip() in {str(k).strip() for k in build_month_keys}:
            _last_failed_month_key = None
            _last_failed_at_unix = None
            _last_failed_error = None
        _prune_stale_month_build_dirs()
        _prune_stale_month_backup_dirs()

    except Exception as e:
        end = time.time()
        storage_report = get_artifact_storage_report(DATA_DIR, FRAMES_DIR)
        low_space = bool(storage_report.get("low_space")) or _is_no_space_error(e)
        error_text = str(e)
        if low_space:
            error_text = (
                "Artifact rebuild blocked by low space on mounted volume. "
                f"Original error: {str(e)}"
            )
        _set_state(
            state="error",
            finished_at_unix=end,
            duration_sec=round(end - start, 2),
            error=error_text,
            trace=traceback.format_exc(),
            result={
                "likely_cause": "low_space_volume" if low_space else "unknown",
                "storage_report": storage_report,
            },
        )
        _last_failed_month_key = str(failed_month_key_candidate).strip() if failed_month_key_candidate else None
        _last_failed_at_unix = int(end)
        _last_failed_error = str(error_text)
    finally:
        _clear_lock(expected_token=run_token)
        global _generate_thread
        with _generate_control_lock:
            if _generate_thread is threading.current_thread():
                _generate_thread = None


def start_generate(
    bin_minutes: int,
    min_trips_per_window: int,
    force_clear_lock: bool = False,
    include_day_tendency: bool = False,
    build_review_artifacts: bool = False,
    month_key: Optional[str] = None,
    build_all_months: bool = False,
) -> Dict[str, Any]:
    global _generate_thread
    with _generate_control_lock:
        requested_month_key = str(month_key).strip() if month_key else None
        if not requested_month_key and not bool(build_all_months):
            try:
                requested_month_key = _resolve_target_month_key_for_request()
            except HTTPException:
                requested_month_key = None

        st = _get_state()
        state_name = str(st.get("state") or "")
        same_job = (
            str(st.get("month_key") or "").strip() == str(requested_month_key or "").strip()
            and bool(st.get("build_all_months")) == bool(build_all_months)
            and bool(st.get("include_day_tendency")) == bool(include_day_tendency)
            and bool(st.get("build_review_artifacts")) == bool(build_review_artifacts)
        )
        if state_name in {"started", "running"} and same_job:
            return {
                "ok": True,
                "state": state_name,
                "bin_minutes": st["bin_minutes"],
                "min_trips_per_window": st["min_trips_per_window"],
                "include_day_tendency": bool(st.get("include_day_tendency")),
                "build_review_artifacts": bool(st.get("build_review_artifacts")),
                "month_key": st.get("month_key"),
                "build_all_months": bool(st.get("build_all_months")),
                "run_token": st.get("run_token"),
            }

        cleanup_result = None
        lock_cleared = False
        thread_alive = _generate_thread_alive()
        lock_present = _lock_is_present()
        if not thread_alive:
            cleanup_result = cleanup_artifact_storage(DATA_DIR, FRAMES_DIR)
            _prune_stale_month_build_dirs()
            _prune_stale_month_backup_dirs()
            if force_clear_lock and lock_present:
                _clear_lock()
                lock_cleared = True
                lock_present = _lock_is_present()

        if lock_present:
            if not thread_alive:
                _clear_lock()
                lock_cleared = True
                state_now = _get_state()
                if state_now.get("state") == "running" and not _generate_thread_alive():
                    _set_state(state="idle")
            if _lock_is_present():
                _set_state(
                    state="running",
                    bin_minutes=bin_minutes,
                    min_trips_per_window=min_trips_per_window,
                    month_key=requested_month_key,
                    build_all_months=bool(build_all_months),
                    include_day_tendency=bool(include_day_tendency),
                    build_review_artifacts=bool(build_review_artifacts),
                )
                return {
                    "ok": True,
                    "state": "running",
                    "bin_minutes": bin_minutes,
                    "min_trips_per_window": min_trips_per_window,
                    "cleanup": cleanup_result,
                    "lock_cleared": lock_cleared,
                    "include_day_tendency": bool(include_day_tendency),
                    "build_review_artifacts": bool(build_review_artifacts),
                    "month_key": requested_month_key,
                    "build_all_months": bool(build_all_months),
                    "run_token": _read_lock_token(),
                }

        run_token = uuid.uuid4().hex
        _write_lock(run_token)
        _set_state(
            state="started",
            bin_minutes=bin_minutes,
            min_trips_per_window=min_trips_per_window,
            run_token=run_token,
            month_key=requested_month_key,
            build_all_months=bool(build_all_months),
            include_day_tendency=bool(include_day_tendency),
            build_review_artifacts=bool(build_review_artifacts),
        )

        t = threading.Thread(
            target=_generate_worker,
            args=(
                bin_minutes,
                min_trips_per_window,
                run_token,
                bool(include_day_tendency),
                bool(build_review_artifacts),
                requested_month_key,
                bool(build_all_months),
            ),
            daemon=True,
        )
        _generate_thread = t
        t.start()

        return {
            "ok": True,
            "state": "started",
            "bin_minutes": bin_minutes,
            "min_trips_per_window": min_trips_per_window,
            "cleanup": cleanup_result,
            "lock_cleared": lock_cleared,
            "include_day_tendency": bool(include_day_tendency),
            "build_review_artifacts": bool(build_review_artifacts),
            "month_key": requested_month_key,
            "build_all_months": bool(build_all_months),
            "run_token": run_token,
        }


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


def _ghost_visible_sql(column_name: str) -> str:
    if DB_BACKEND == "postgres":
        return f"({column_name} IS NULL OR {column_name} = FALSE)"
    return f"({column_name} IS NULL OR CAST({column_name} AS INTEGER) = 0)"


def _db_bool_value(flag: bool):
    if DB_BACKEND == "postgres":
        return bool(flag)
    return 1 if flag else 0


def _presence_cutoff_unix(max_age_sec: int) -> int:
    return int(time.time()) - max(5, min(3600, int(max_age_sec)))


def _presence_visibility_snapshot(max_age_sec: int) -> Dict[str, Any]:
    cutoff = _presence_cutoff_unix(max_age_sec)
    ghost_visible = _ghost_visible_sql("u.ghost_mode")
    online_visible = _presence_online_where_sql()
    sql_mode = "postgres_boolean" if DB_BACKEND == "postgres" else "sqlite_cast_integer"
    try:
        visible_count_row = _db_query_one(
            f"""
            SELECT COUNT(*) AS c
            FROM presence p
            LEFT JOIN users u ON u.id = p.user_id
            WHERE p.updated_at >= ?
              AND {ghost_visible}
              AND {online_visible}
            """,
            (cutoff,),
        )
        visible_count = int(visible_count_row["c"] or 0) if visible_count_row else 0
        sample_rows = _db_query_all(
            f"""
            SELECT p.user_id, u.email, u.display_name
            FROM presence p
            LEFT JOIN users u ON u.id = p.user_id
            WHERE p.updated_at >= ?
              AND {ghost_visible}
              AND {online_visible}
            ORDER BY p.updated_at DESC
            LIMIT 5
            """,
            (cutoff,),
        )
        counts = _db_query_one(
            f"""
            SELECT
              COUNT(*) AS online_count,
              SUM(CASE WHEN COALESCE(u.ghost_mode, FALSE) THEN 1 ELSE 0 END) AS ghosted_count
            FROM presence p
            LEFT JOIN users u ON u.id = p.user_id
            WHERE p.updated_at >= ?
              AND {online_visible}
            """,
            (cutoff,),
        )
    except Exception as exc:
        return {
            "db_backend": DB_BACKEND,
            "visible_count": 0,
            "online_count": 0,
            "ghosted_count": 0,
            "sample_user_ids": [],
            "sample_display_names": [],
            "sql_mode": sql_mode,
            "ok": False,
            "error": str(exc),
        }

    sample_user_ids: List[int] = []
    sample_display_names: List[str] = []
    for row in sample_rows:
        uid = row["user_id"]
        if uid is None:
            continue
        email = (row["email"] or "").strip()
        dn = (row["display_name"] or "").strip() or _clean_display_name("", email or "Driver")
        sample_user_ids.append(int(uid))
        sample_display_names.append(dn)

    online_count = int(counts["online_count"] or 0) if counts else 0
    ghosted_count = int(counts["ghosted_count"] or 0) if counts else 0
    return {
        "db_backend": DB_BACKEND,
        "visible_count": visible_count,
        "online_count": online_count,
        "ghosted_count": ghosted_count,
        "sample_user_ids": sample_user_ids,
        "sample_display_names": sample_display_names,
        "sql_mode": sql_mode,
        "ok": True,
    }


def _presence_online_summary_snapshot(max_age_sec: int) -> Dict[str, Any]:
    cutoff = _presence_cutoff_unix(max_age_sec)
    online_visible = _presence_online_where_sql()
    try:
        counts = _db_query_one(
            f"""
            SELECT
              COUNT(*) AS online_count,
              SUM(CASE WHEN COALESCE(u.ghost_mode, FALSE) THEN 1 ELSE 0 END) AS ghosted_count
            FROM presence p
            LEFT JOIN users u ON u.id = p.user_id
            WHERE p.updated_at >= ?
              AND {online_visible}
            """,
            (cutoff,),
        )
    except Exception as exc:
        return {
            "online_count": 0,
            "ghosted_count": 0,
            "ok": False,
            "error": str(exc),
        }

    return {
        "online_count": int(counts["online_count"] or 0) if counts else 0,
        "ghosted_count": int(counts["ghosted_count"] or 0) if counts else 0,
        "ok": True,
    }


def _presence_change_cursor_ms() -> int:
    global _presence_last_change_cursor_ms
    now_ms = int(time.time() * 1000)
    with _presence_cursor_lock:
        next_cursor = now_ms
        if next_cursor <= _presence_last_change_cursor_ms:
            next_cursor = _presence_last_change_cursor_ms + 1
        _presence_last_change_cursor_ms = next_cursor
        return next_cursor


def _presence_peek_cursor_ms() -> int:
    now_ms = int(time.time() * 1000)
    with _presence_cursor_lock:
        if _presence_last_change_cursor_ms > 0:
            return _presence_last_change_cursor_ms
    return now_ms


def _presence_runtime_state_upsert(user_id: int, *, is_visible: bool, reason: Optional[str] = None, changed_at_ms: Optional[int] = None) -> int:
    if changed_at_ms is None:
        cursor_value = _presence_change_cursor_ms()
    else:
        cursor_value = int(changed_at_ms)
    safe_reason = None if is_visible else (reason or "hidden")
    _db_exec(
        """
        INSERT INTO presence_runtime_state(user_id, changed_at_ms, is_visible, reason)
        VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
          changed_at_ms=excluded.changed_at_ms,
          is_visible=excluded.is_visible,
          reason=excluded.reason
        """,
        (int(user_id), cursor_value, _db_bool_value(is_visible), safe_reason),
    )
    return cursor_value


def _presence_remove_runtime_visibility(user_id: int, *, reason: str, changed_at_ms: Optional[int] = None) -> int:
    return _presence_runtime_state_upsert(int(user_id), is_visible=False, reason=reason, changed_at_ms=changed_at_ms)


def _presence_visible_where_sql() -> str:
    return " AND ".join(
        [
            _ghost_visible_sql("u.ghost_mode"),
            "COALESCE(CAST(u.is_disabled AS INTEGER), 0) = 0" if DB_BACKEND != "postgres" else "COALESCE(u.is_disabled, FALSE) = FALSE",
            "COALESCE(CAST(u.is_suspended AS INTEGER), 0) = 0" if DB_BACKEND != "postgres" else "COALESCE(u.is_suspended, FALSE) = FALSE",
        ]
    )


def _presence_online_where_sql() -> str:
    return " AND ".join(
        [
            "COALESCE(CAST(u.is_disabled AS INTEGER), 0) = 0" if DB_BACKEND != "postgres" else "COALESCE(u.is_disabled, FALSE) = FALSE",
            "COALESCE(CAST(u.is_suspended AS INTEGER), 0) = 0" if DB_BACKEND != "postgres" else "COALESCE(u.is_suspended, FALSE) = FALSE",
        ]
    )


def _presence_state_from_user_row(user: Any) -> Tuple[bool, Optional[str]]:
    block_state = _user_block_state(user)
    if block_state["is_blocked"]:
        return False, str(block_state["reason"] or "blocked")
    ghost = bool(_flag_to_int(user["ghost_mode"])) if "ghost_mode" in user.keys() and user["ghost_mode"] is not None else False
    if ghost:
        return False, "ghost_mode"
    return True, None


def _presence_row_payloads(rows: List[Any], *, include_full_fields: bool) -> List[Dict[str, Any]]:
    badge_by_user = get_best_current_badges_for_users([int(r["user_id"]) for r in rows], refresh_if_needed=False)
    items: List[Dict[str, Any]] = []
    for r in rows:
        best_badge = badge_by_user.get(int(r["user_id"]), {})
        email = (r["email"] or "").strip()
        dn = (r["display_name"] or "").strip()
        if not dn:
            dn = _clean_display_name("", email or "Driver")
        payload = {
            "user_id": int(r["user_id"]),
            "display_name": dn,
            "avatar_url": _avatar_thumb_url_for_row(r),
            "avatar_thumb_url": _avatar_thumb_url_for_row(r),
            "avatar_version": _avatar_version_for_row(r),
            "map_identity_mode": (
                str(r["map_identity_mode"]).strip().lower()
                if r["map_identity_mode"] is not None and str(r["map_identity_mode"]).strip().lower() in ALLOWED_MAP_IDENTITY_MODES
                else "name"
            ),
            "lat": float(r["lat"]),
            "lng": float(r["lng"]),
            "heading": float(r["heading"]) if r["heading"] is not None else None,
            "updated_at": int(r["updated_at"]),
            "updated_at_unix": int(r["updated_at"]),
            "leaderboard_badge_code": best_badge.get("leaderboard_badge_code"),
        }
        if include_full_fields:
            payload["email"] = email
            payload["accuracy"] = float(r["accuracy"]) if r["accuracy"] is not None else None
        items.append(payload)
    return items


def _presence_viewport_where_params(
    *,
    cutoff: int,
    bbox: Optional[Tuple[float, float, float, float]],
) -> Tuple[List[str], List[Any]]:
    where_clauses = ["p.updated_at >= ?", _presence_visible_where_sql()]
    params: List[Any] = [cutoff]
    if bbox is not None:
        lo_lat, lo_lng, hi_lat, hi_lng = bbox
        where_clauses.append("p.lat BETWEEN ? AND ?")
        where_clauses.append("p.lng BETWEEN ? AND ?")
        params.extend([lo_lat, hi_lat, lo_lng, hi_lng])
    return where_clauses, params


def _presence_visible_count_for_viewport(
    *,
    cutoff: int,
    bbox: Optional[Tuple[float, float, float, float]],
) -> int:
    where_clauses, params = _presence_viewport_where_params(cutoff=cutoff, bbox=bbox)
    row = _db_query_one(
        f"""
        SELECT COUNT(*) AS visible_count_total
        FROM presence p
        JOIN users u ON u.id = p.user_id
        WHERE {' AND '.join(where_clauses)}
        """,
        tuple(params),
    )
    return int(row["visible_count_total"] or 0) if row else 0


def _presence_rows_for_viewport(
    *,
    cutoff: int,
    bbox: Optional[Tuple[float, float, float, float]],
    limit: Optional[int],
) -> List[Any]:
    where_clauses, params = _presence_viewport_where_params(cutoff=cutoff, bbox=bbox)
    limit_sql = ""
    if limit is not None:
        limit_sql = " LIMIT ?"
        params.append(int(limit))
    return _db_query_all(
        f"""
        SELECT
          p.user_id,
          u.id,
          u.email,
          u.display_name,
          u.avatar_version,
          u.map_identity_mode,
          u.ghost_mode,
          p.lat,
          p.lng,
          p.heading,
          p.accuracy,
          p.updated_at
        FROM presence p
        JOIN users u ON u.id = p.user_id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY p.updated_at DESC, p.user_id DESC
        {limit_sql}
        """,
        tuple(params),
    )


def _presence_delta_payload(
    *,
    updated_since_ms: int,
    max_age_sec: int,
    bbox: Optional[Tuple[float, float, float, float]],
    limit: Optional[int],
    include_removed: bool,
) -> Dict[str, Any]:
    safe_limit = max(1, min(PRESENCE_DELTA_MAX_LIMIT, int(limit or PRESENCE_DELTA_MAX_LIMIT)))
    cutoff = _presence_cutoff_unix(max_age_sec)
    snapshot = _presence_online_summary_snapshot(max_age_sec)
    params: List[Any] = [int(updated_since_ms)]
    rows = _db_query_all(
        f"""
        SELECT
          prs.user_id AS runtime_user_id,
          prs.changed_at_ms,
          prs.is_visible,
          prs.reason,
          p.user_id,
          u.id,
          u.email,
          u.display_name,
          u.avatar_version,
          u.map_identity_mode,
          u.ghost_mode,
          p.lat,
          p.lng,
          p.heading,
          p.accuracy,
          p.updated_at
        FROM presence_runtime_state prs
        LEFT JOIN presence p ON p.user_id = prs.user_id
        LEFT JOIN users u ON u.id = prs.user_id
        WHERE prs.changed_at_ms > ?
        ORDER BY prs.changed_at_ms ASC, prs.user_id ASC
        LIMIT ?
        """,
        tuple(params + [safe_limit + 1]),
    )

    changed_rows: List[Any] = []
    removed: List[Dict[str, Any]] = []
    next_cursor = int(updated_since_ms)
    returned_rows = rows[:safe_limit]
    for row in returned_rows:
        next_cursor = max(next_cursor, int(row["changed_at_ms"] or updated_since_ms))
        row_is_visible = int(row["is_visible"] or 0) == 1
        has_presence = row["user_id"] is not None and row["updated_at"] is not None
        is_fresh = has_presence and int(row["updated_at"]) >= cutoff
        in_viewport = True
        if bbox is not None and has_presence:
            lo_lat, lo_lng, hi_lat, hi_lng = bbox
            lat = float(row["lat"])
            lng = float(row["lng"])
            in_viewport = lo_lat <= lat <= hi_lat and lo_lng <= lng <= hi_lng
        if row_is_visible and is_fresh and in_viewport:
            changed_rows.append(row)
            continue
        if include_removed:
            reason = row["reason"]
            if row_is_visible and not is_fresh:
                reason = "stale"
            removed.append(
                {
                    "user_id": int(row["user_id"]) if row["user_id"] is not None else int(row["runtime_user_id"]),
                    "removed_at_ms": int(row["changed_at_ms"]),
                    "reason": reason or ("outside_viewport" if row_is_visible and is_fresh and not in_viewport else "hidden"),
                }
            )

    items = _presence_row_payloads(changed_rows, include_full_fields=False)
    return {
        "ok": True,
        "mode": "delta",
        "count": len(items),
        "items": items,
        "removed": removed if include_removed else [],
        "cursor": next_cursor,
        "next_updated_since_ms": next_cursor,
        "server_time_ms": _presence_peek_cursor_ms(),
        "online_count": int(snapshot.get("online_count") or 0),
        "ghosted_count": int(snapshot.get("ghosted_count") or 0),
        "has_more": len(rows) > safe_limit,
    }


def _parse_legacy_dm_room(room: str) -> Tuple[int, int] | None:
    match = re.fullmatch(r"dm:(\d+):(\d+)", (room or "").strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _legacy_dm_created_at_param(value: Any) -> Any:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(int(value), tz=timezone.utc)
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            dt = datetime.fromtimestamp(int(float(value)), tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc) if DB_BACKEND == "postgres" else dt.astimezone(timezone.utc).isoformat()


def _postgres_column_type(table: str, column: str) -> Optional[str]:
    if DB_BACKEND != "postgres":
        return None
    row = _db_query_one(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = ? AND column_name = ?
        LIMIT 1
        """,
        (table, column),
    )
    return str(row["data_type"]).strip().lower() if row and row.get("data_type") else None


def _migrate_legacy_dm_rows_to_private() -> None:
    legacy_rows = _db_query_all(
        """
        SELECT id, room, user_id, message, created_at
        FROM chat_messages
        WHERE room LIKE ?
        ORDER BY id ASC
        """,
        ("dm:%:%",),
    )
    for legacy_row in legacy_rows:
        row = dict(legacy_row)
        pair = _parse_legacy_dm_room(str(row.get("room") or ""))
        if pair is None:
            continue
        sender_user_id = int(row["user_id"])
        low, high = pair
        if sender_user_id == low:
            recipient_user_id = high
        elif sender_user_id == high:
            recipient_user_id = low
        else:
            continue
        if _db_query_one(
            "SELECT id FROM private_chat_messages WHERE legacy_room_message_id=? LIMIT 1",
            (int(row["id"]),),
        ):
            continue
        created_at_value = _legacy_dm_created_at_param(row["created_at"])
        _db_exec(
            """
            INSERT INTO private_chat_messages(
                sender_user_id, recipient_user_id, text, created_at, read_at, message_type, legacy_room_message_id
            )
            VALUES(?, ?, ?, ?, NULL, 'text', ?)
            """,
            (sender_user_id, recipient_user_id, row["message"], created_at_value, int(row["id"])),
        )


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
            "ALTER TABLE users ADD COLUMN avatar_version TEXT;",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_version TEXT;",
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
        _db_exec("CREATE INDEX IF NOT EXISTS idx_presence_updated_at ON presence(updated_at);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_presence_updated_at_user ON presence(updated_at DESC, user_id);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_presence_updated_at_lat_lng ON presence(updated_at DESC, lat, lng);")
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS presence_runtime_state (
              user_id BIGINT PRIMARY KEY,
              changed_at_ms BIGINT NOT NULL,
              is_visible BOOLEAN NOT NULL DEFAULT TRUE,
              reason TEXT
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_presence_runtime_state_changed ON presence_runtime_state(changed_at_ms DESC, user_id);")
        _db_exec(
            """
            INSERT INTO presence_runtime_state(user_id, changed_at_ms, is_visible, reason)
            SELECT
              p.user_id,
              CASE
                WHEN p.updated_at > 1000000000000 THEN p.updated_at
                ELSE p.updated_at * 1000
              END,
              CASE
                WHEN COALESCE(u.is_disabled, FALSE) = TRUE OR COALESCE(u.is_suspended, FALSE) = TRUE OR COALESCE(u.ghost_mode, FALSE) = TRUE THEN FALSE
                ELSE TRUE
              END,
              CASE
                WHEN COALESCE(u.is_disabled, FALSE) = TRUE THEN 'disabled'
                WHEN COALESCE(u.is_suspended, FALSE) = TRUE THEN 'suspended'
                WHEN COALESCE(u.ghost_mode, FALSE) = TRUE THEN 'ghost_mode'
                ELSE NULL
              END
            FROM presence p
            JOIN users u ON u.id = p.user_id
            ON CONFLICT(user_id) DO UPDATE SET
              changed_at_ms=excluded.changed_at_ms,
              is_visible=excluded.is_visible,
              reason=excluded.reason
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
        _db_exec("CREATE INDEX IF NOT EXISTS idx_pickup_logs_user_time ON pickup_logs(user_id, created_at DESC);")

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
              long_run_historical_component DOUBLE PRECISION NOT NULL DEFAULT 0,
              recent_shape_component DOUBLE PRECISION NOT NULL DEFAULT 0,
              outcome_modifier DOUBLE PRECISION NOT NULL DEFAULT 1,
              quality_modifier DOUBLE PRECISION NOT NULL DEFAULT 1,
              saturation_modifier DOUBLE PRECISION NOT NULL DEFAULT 1,
              hotspot_limit_used INTEGER NOT NULL DEFAULT 0,
              density_penalty DOUBLE PRECISION NOT NULL,
              weighted_trip_count DOUBLE PRECISION NOT NULL,
              unique_driver_count INTEGER NOT NULL,
              recommended BOOLEAN NOT NULL DEFAULT FALSE
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_hotspot_experiment_bins_time ON hotspot_experiment_bins(bin_time DESC);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_hotspot_experiment_bins_zone_time ON hotspot_experiment_bins(zone_id, bin_time DESC);")
        _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN long_run_historical_component DOUBLE PRECISION NOT NULL DEFAULT 0;", "ALTER TABLE hotspot_experiment_bins ADD COLUMN IF NOT EXISTS long_run_historical_component DOUBLE PRECISION NOT NULL DEFAULT 0;")
        _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN recent_shape_component DOUBLE PRECISION NOT NULL DEFAULT 0;", "ALTER TABLE hotspot_experiment_bins ADD COLUMN IF NOT EXISTS recent_shape_component DOUBLE PRECISION NOT NULL DEFAULT 0;")
        _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN outcome_modifier DOUBLE PRECISION NOT NULL DEFAULT 1;", "ALTER TABLE hotspot_experiment_bins ADD COLUMN IF NOT EXISTS outcome_modifier DOUBLE PRECISION NOT NULL DEFAULT 1;")
        _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN quality_modifier DOUBLE PRECISION NOT NULL DEFAULT 1;", "ALTER TABLE hotspot_experiment_bins ADD COLUMN IF NOT EXISTS quality_modifier DOUBLE PRECISION NOT NULL DEFAULT 1;")
        _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN saturation_modifier DOUBLE PRECISION NOT NULL DEFAULT 1;", "ALTER TABLE hotspot_experiment_bins ADD COLUMN IF NOT EXISTS saturation_modifier DOUBLE PRECISION NOT NULL DEFAULT 1;")
        _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN hotspot_limit_used INTEGER NOT NULL DEFAULT 0;", "ALTER TABLE hotspot_experiment_bins ADD COLUMN IF NOT EXISTS hotspot_limit_used INTEGER NOT NULL DEFAULT 0;")

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
              center_lat DOUBLE PRECISION,
              center_lng DOUBLE PRECISION,
              radius_m DOUBLE PRECISION,
              intensity DOUBLE PRECISION,
              baseline_component DOUBLE PRECISION,
              live_component DOUBLE PRECISION,
              same_timeslot_component DOUBLE PRECISION,
              eta_alignment DOUBLE PRECISION,
              recommended BOOLEAN NOT NULL DEFAULT FALSE
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_micro_hotspot_experiment_bins_time ON micro_hotspot_experiment_bins(bin_time DESC);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_micro_hotspot_experiment_bins_zone_time ON micro_hotspot_experiment_bins(zone_id, bin_time DESC);")
        _try_alter(
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN center_lat DOUBLE PRECISION;",
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN IF NOT EXISTS center_lat DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN center_lng DOUBLE PRECISION;",
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN IF NOT EXISTS center_lng DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN radius_m DOUBLE PRECISION;",
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN IF NOT EXISTS radius_m DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN intensity DOUBLE PRECISION;",
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN IF NOT EXISTS intensity DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN baseline_component DOUBLE PRECISION;",
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN IF NOT EXISTS baseline_component DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN live_component DOUBLE PRECISION;",
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN IF NOT EXISTS live_component DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN same_timeslot_component DOUBLE PRECISION;",
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN IF NOT EXISTS same_timeslot_component DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN eta_alignment DOUBLE PRECISION;",
            "ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN IF NOT EXISTS eta_alignment DOUBLE PRECISION;",
        )

        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS recommendation_outcomes (
              id BIGSERIAL PRIMARY KEY,
              user_id BIGINT,
              recommended_at BIGINT NOT NULL,
              zone_id INTEGER NOT NULL,
              cluster_id TEXT,
              hotspot_center_lat DOUBLE PRECISION,
              hotspot_center_lng DOUBLE PRECISION,
              score DOUBLE PRECISION NOT NULL,
              confidence DOUBLE PRECISION NOT NULL,
              converted_to_trip BOOLEAN,
              minutes_to_trip DOUBLE PRECISION
            );
            """
        )
        _try_alter(
            "ALTER TABLE recommendation_outcomes ADD COLUMN hotspot_center_lat DOUBLE PRECISION;",
            "ALTER TABLE recommendation_outcomes ADD COLUMN IF NOT EXISTS hotspot_center_lat DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE recommendation_outcomes ADD COLUMN hotspot_center_lng DOUBLE PRECISION;",
            "ALTER TABLE recommendation_outcomes ADD COLUMN IF NOT EXISTS hotspot_center_lng DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE recommendation_outcomes ADD COLUMN distance_to_recommendation_miles DOUBLE PRECISION;",
            "ALTER TABLE recommendation_outcomes ADD COLUMN IF NOT EXISTS distance_to_recommendation_miles DOUBLE PRECISION;",
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_recommendation_outcomes_time ON recommendation_outcomes(recommended_at DESC);")
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_recommendation_outcomes_zone_cluster_time "
            "ON recommendation_outcomes(zone_id, cluster_id, recommended_at DESC);"
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS micro_recommendation_outcomes (
              id BIGSERIAL PRIMARY KEY,
              user_id BIGINT,
              recommended_at BIGINT NOT NULL,
              zone_id INTEGER NOT NULL,
              parent_hotspot_id TEXT,
              micro_cluster_id TEXT NOT NULL,
              micro_center_lat DOUBLE PRECISION,
              micro_center_lng DOUBLE PRECISION,
              score DOUBLE PRECISION NOT NULL,
              confidence DOUBLE PRECISION NOT NULL,
              converted_to_trip BOOLEAN,
              minutes_to_trip DOUBLE PRECISION
            );
            """
        )
        _try_alter(
            "ALTER TABLE micro_recommendation_outcomes ADD COLUMN micro_center_lat DOUBLE PRECISION;",
            "ALTER TABLE micro_recommendation_outcomes ADD COLUMN IF NOT EXISTS micro_center_lat DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE micro_recommendation_outcomes ADD COLUMN micro_center_lng DOUBLE PRECISION;",
            "ALTER TABLE micro_recommendation_outcomes ADD COLUMN IF NOT EXISTS micro_center_lng DOUBLE PRECISION;",
        )
        _try_alter(
            "ALTER TABLE micro_recommendation_outcomes ADD COLUMN distance_to_recommendation_miles DOUBLE PRECISION;",
            "ALTER TABLE micro_recommendation_outcomes ADD COLUMN IF NOT EXISTS distance_to_recommendation_miles DOUBLE PRECISION;",
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_micro_recommendation_outcomes_time ON micro_recommendation_outcomes(recommended_at DESC);")
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_micro_recommendation_outcomes_zone_cluster_time "
            "ON micro_recommendation_outcomes(zone_id, micro_cluster_id, recommended_at DESC);"
        )
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_micro_recommendation_outcomes_parent_time "
            "ON micro_recommendation_outcomes(parent_hotspot_id, recommended_at DESC);"
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS driver_guidance_state (
              user_id BIGINT PRIMARY KEY,
              last_guidance_action TEXT,
              last_guidance_generated_at BIGINT,
              last_move_guidance_at BIGINT,
              last_hold_guidance_at BIGINT,
              last_target_zone_id INTEGER,
              recent_move_attempts_without_trip INTEGER NOT NULL DEFAULT 0,
              recent_wait_dispatch_count INTEGER NOT NULL DEFAULT 0,
              updated_at BIGINT NOT NULL
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_driver_guidance_state_updated_at ON driver_guidance_state(updated_at DESC);")
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS assistant_guidance_outcomes (
              id BIGSERIAL PRIMARY KEY,
              user_id BIGINT NOT NULL,
              frame_time TEXT,
              recommended_at BIGINT NOT NULL,
              action TEXT NOT NULL,
              source_zone_id INTEGER,
              target_zone_id INTEGER,
              tripless_minutes DOUBLE PRECISION,
              stationary_minutes DOUBLE PRECISION,
              movement_minutes DOUBLE PRECISION,
              current_rating DOUBLE PRECISION,
              target_rating DOUBLE PRECISION,
              dispatch_uncertainty DOUBLE PRECISION,
              converted_to_trip BOOLEAN,
              moved_before_trip BOOLEAN,
              minutes_to_trip DOUBLE PRECISION,
              settled_at BIGINT,
              settlement_reason TEXT
            );
            """
        )
        _try_alter(
            "ALTER TABLE assistant_guidance_outcomes ADD COLUMN moved_before_trip BOOLEAN;",
            "ALTER TABLE assistant_guidance_outcomes ADD COLUMN IF NOT EXISTS moved_before_trip BOOLEAN;",
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_assistant_guidance_outcomes_user_time ON assistant_guidance_outcomes(user_id, recommended_at DESC);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_assistant_guidance_outcomes_unsettled ON assistant_guidance_outcomes(user_id, converted_to_trip, recommended_at DESC);")

        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS chat_messages (
              id BIGSERIAL PRIMARY KEY,
              room TEXT NOT NULL DEFAULT 'global',
              user_id BIGINT NOT NULL,
              display_name TEXT,
              message TEXT NOT NULL,
              created_at TIMESTAMP NOT NULL DEFAULT NOW(),
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _try_alter(
            "ALTER TABLE chat_messages ADD COLUMN room TEXT NOT NULL DEFAULT 'global';",
            "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS room TEXT NOT NULL DEFAULT 'global';",
        )
        _try_alter(
            "ALTER TABLE chat_messages ADD COLUMN message_type TEXT NOT NULL DEFAULT 'text';",
            "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS message_type TEXT NOT NULL DEFAULT 'text';",
        )
        _try_alter(
            "ALTER TABLE chat_messages ADD COLUMN audio_path TEXT;",
            "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS audio_path TEXT;",
        )
        _try_alter(
            "ALTER TABLE chat_messages ADD COLUMN audio_mime_type TEXT;",
            "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS audio_mime_type TEXT;",
        )
        _try_alter(
            "ALTER TABLE chat_messages ADD COLUMN audio_duration_ms INTEGER;",
            "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS audio_duration_ms INTEGER;",
        )
        if (_postgres_column_type("chat_messages", "created_at") or "") in {"bigint", "integer", "numeric"}:
            _db_exec("ALTER TABLE chat_messages ALTER COLUMN created_at TYPE TIMESTAMP USING to_timestamp(created_at::double precision);")
        _db_exec("ALTER TABLE chat_messages ALTER COLUMN created_at SET DEFAULT NOW();")
        _db_exec("UPDATE chat_messages SET room='global' WHERE room IS NULL OR btrim(room)='';")
        _db_exec("UPDATE chat_messages SET message_type='text' WHERE message_type IS NULL OR btrim(message_type)='';")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_id ON chat_messages(id);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_room_id ON chat_messages(room, id);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_created_at ON chat_messages(created_at);")
        _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_room_created_at ON chat_messages(room, created_at DESC, id DESC);")

        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS private_chat_messages (
              id BIGSERIAL PRIMARY KEY,
              sender_user_id BIGINT NOT NULL,
              recipient_user_id BIGINT NOT NULL,
              text TEXT NOT NULL,
              created_at TIMESTAMP NOT NULL DEFAULT NOW(),
              read_at TIMESTAMP NULL,
              FOREIGN KEY(sender_user_id) REFERENCES users(id),
              FOREIGN KEY(recipient_user_id) REFERENCES users(id)
            );
            """
        )
        _try_alter(
            "ALTER TABLE private_chat_messages ADD COLUMN message_type TEXT NOT NULL DEFAULT 'text';",
            "ALTER TABLE private_chat_messages ADD COLUMN IF NOT EXISTS message_type TEXT NOT NULL DEFAULT 'text';",
        )
        _try_alter(
            "ALTER TABLE private_chat_messages ADD COLUMN audio_path TEXT;",
            "ALTER TABLE private_chat_messages ADD COLUMN IF NOT EXISTS audio_path TEXT;",
        )
        _try_alter(
            "ALTER TABLE private_chat_messages ADD COLUMN audio_mime_type TEXT;",
            "ALTER TABLE private_chat_messages ADD COLUMN IF NOT EXISTS audio_mime_type TEXT;",
        )
        _try_alter(
            "ALTER TABLE private_chat_messages ADD COLUMN audio_duration_ms INTEGER;",
            "ALTER TABLE private_chat_messages ADD COLUMN IF NOT EXISTS audio_duration_ms INTEGER;",
        )
        _try_alter(
            "ALTER TABLE private_chat_messages ADD COLUMN legacy_room_message_id BIGINT;",
            "ALTER TABLE private_chat_messages ADD COLUMN IF NOT EXISTS legacy_room_message_id BIGINT;",
        )
        _db_exec("UPDATE private_chat_messages SET message_type='text' WHERE message_type IS NULL OR btrim(message_type)='';")
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_private_chat_pair_created ON private_chat_messages(sender_user_id, recipient_user_id, created_at, id);"
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_private_chat_messages_created_at ON private_chat_messages(created_at);")
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_private_chat_sender_created ON private_chat_messages(sender_user_id, created_at DESC, id DESC);"
        )
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_private_chat_recipient_created ON private_chat_messages(recipient_user_id, created_at DESC, id DESC);"
        )
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_private_chat_recipient_read ON private_chat_messages(recipient_user_id, sender_user_id, read_at);"
        )
        _db_exec(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_private_chat_legacy_room_message_id ON private_chat_messages(legacy_room_message_id);"
        )
        _migrate_legacy_dm_rows_to_private()
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
    _try_alter("ALTER TABLE users ADD COLUMN avatar_version TEXT;")
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
    _db_exec("CREATE INDEX IF NOT EXISTS idx_presence_updated_at ON presence(updated_at);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_presence_updated_at_user ON presence(updated_at DESC, user_id);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_presence_updated_at_lat_lng ON presence(updated_at DESC, lat, lng);")
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS presence_runtime_state (
          user_id INTEGER PRIMARY KEY,
          changed_at_ms INTEGER NOT NULL,
          is_visible INTEGER NOT NULL DEFAULT 1,
          reason TEXT
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_presence_runtime_state_changed ON presence_runtime_state(changed_at_ms DESC, user_id);")
    _db_exec(
        """
        INSERT INTO presence_runtime_state(user_id, changed_at_ms, is_visible, reason)
        SELECT
          p.user_id,
          CASE
            WHEN p.updated_at > 1000000000000 THEN p.updated_at
            ELSE p.updated_at * 1000
          END,
          CASE
            WHEN COALESCE(CAST(u.is_disabled AS INTEGER), 0) = 1 OR COALESCE(CAST(u.is_suspended AS INTEGER), 0) = 1 OR COALESCE(CAST(u.ghost_mode AS INTEGER), 0) = 1 THEN 0
            ELSE 1
          END,
          CASE
            WHEN COALESCE(CAST(u.is_disabled AS INTEGER), 0) = 1 THEN 'disabled'
            WHEN COALESCE(CAST(u.is_suspended AS INTEGER), 0) = 1 THEN 'suspended'
            WHEN COALESCE(CAST(u.ghost_mode AS INTEGER), 0) = 1 THEN 'ghost_mode'
            ELSE NULL
          END
        FROM presence p
        JOIN users u ON u.id = p.user_id
        ON CONFLICT(user_id) DO UPDATE SET
          changed_at_ms=excluded.changed_at_ms,
          is_visible=excluded.is_visible,
          reason=excluded.reason
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
    _db_exec("CREATE INDEX IF NOT EXISTS idx_pickup_logs_user_time ON pickup_logs(user_id, created_at DESC);")

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
          long_run_historical_component REAL NOT NULL DEFAULT 0,
          recent_shape_component REAL NOT NULL DEFAULT 0,
          outcome_modifier REAL NOT NULL DEFAULT 1,
          quality_modifier REAL NOT NULL DEFAULT 1,
          saturation_modifier REAL NOT NULL DEFAULT 1,
          hotspot_limit_used INTEGER NOT NULL DEFAULT 0,
          density_penalty REAL NOT NULL,
          weighted_trip_count REAL NOT NULL,
          unique_driver_count INTEGER NOT NULL,
          recommended INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_hotspot_experiment_bins_time ON hotspot_experiment_bins(bin_time DESC);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_hotspot_experiment_bins_zone_time ON hotspot_experiment_bins(zone_id, bin_time DESC);")
    _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN long_run_historical_component REAL NOT NULL DEFAULT 0;")
    _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN recent_shape_component REAL NOT NULL DEFAULT 0;")
    _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN outcome_modifier REAL NOT NULL DEFAULT 1;")
    _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN quality_modifier REAL NOT NULL DEFAULT 1;")
    _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN saturation_modifier REAL NOT NULL DEFAULT 1;")
    _try_alter("ALTER TABLE hotspot_experiment_bins ADD COLUMN hotspot_limit_used INTEGER NOT NULL DEFAULT 0;")

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
          center_lat REAL,
          center_lng REAL,
          radius_m REAL,
          intensity REAL,
          baseline_component REAL,
          live_component REAL,
          same_timeslot_component REAL,
          eta_alignment REAL,
          recommended INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_micro_hotspot_experiment_bins_time ON micro_hotspot_experiment_bins(bin_time DESC);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_micro_hotspot_experiment_bins_zone_time ON micro_hotspot_experiment_bins(zone_id, bin_time DESC);")
    _try_alter("ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN center_lat REAL;")
    _try_alter("ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN center_lng REAL;")
    _try_alter("ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN radius_m REAL;")
    _try_alter("ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN intensity REAL;")
    _try_alter("ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN baseline_component REAL;")
    _try_alter("ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN live_component REAL;")
    _try_alter("ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN same_timeslot_component REAL;")
    _try_alter("ALTER TABLE micro_hotspot_experiment_bins ADD COLUMN eta_alignment REAL;")

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS recommendation_outcomes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER,
          recommended_at INTEGER NOT NULL,
          zone_id INTEGER NOT NULL,
          cluster_id TEXT,
          hotspot_center_lat REAL,
          hotspot_center_lng REAL,
          score REAL NOT NULL,
          confidence REAL NOT NULL,
          converted_to_trip INTEGER,
          minutes_to_trip REAL
        );
        """
    )
    _try_alter("ALTER TABLE recommendation_outcomes ADD COLUMN hotspot_center_lat REAL;")
    _try_alter("ALTER TABLE recommendation_outcomes ADD COLUMN hotspot_center_lng REAL;")
    _try_alter("ALTER TABLE recommendation_outcomes ADD COLUMN distance_to_recommendation_miles REAL;")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_recommendation_outcomes_time ON recommendation_outcomes(recommended_at DESC);")
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_recommendation_outcomes_zone_cluster_time "
        "ON recommendation_outcomes(zone_id, cluster_id, recommended_at DESC);"
    )
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS micro_recommendation_outcomes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER,
          recommended_at INTEGER NOT NULL,
          zone_id INTEGER NOT NULL,
          parent_hotspot_id TEXT,
          micro_cluster_id TEXT NOT NULL,
          micro_center_lat REAL,
          micro_center_lng REAL,
          score REAL NOT NULL,
          confidence REAL NOT NULL,
          converted_to_trip INTEGER,
          minutes_to_trip REAL
        );
        """
    )
    _try_alter("ALTER TABLE micro_recommendation_outcomes ADD COLUMN micro_center_lat REAL;")
    _try_alter("ALTER TABLE micro_recommendation_outcomes ADD COLUMN micro_center_lng REAL;")
    _try_alter("ALTER TABLE micro_recommendation_outcomes ADD COLUMN distance_to_recommendation_miles REAL;")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_micro_recommendation_outcomes_time ON micro_recommendation_outcomes(recommended_at DESC);")
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_micro_recommendation_outcomes_zone_cluster_time "
        "ON micro_recommendation_outcomes(zone_id, micro_cluster_id, recommended_at DESC);"
    )
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_micro_recommendation_outcomes_parent_time "
        "ON micro_recommendation_outcomes(parent_hotspot_id, recommended_at DESC);"
    )
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS driver_guidance_state (
          user_id INTEGER PRIMARY KEY,
          last_guidance_action TEXT,
          last_guidance_generated_at INTEGER,
          last_move_guidance_at INTEGER,
          last_hold_guidance_at INTEGER,
          last_target_zone_id INTEGER,
          recent_move_attempts_without_trip INTEGER NOT NULL DEFAULT 0,
          recent_wait_dispatch_count INTEGER NOT NULL DEFAULT 0,
          updated_at INTEGER NOT NULL
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_driver_guidance_state_updated_at ON driver_guidance_state(updated_at DESC);")
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS assistant_guidance_outcomes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          frame_time TEXT,
          recommended_at INTEGER NOT NULL,
          action TEXT NOT NULL,
          source_zone_id INTEGER,
          target_zone_id INTEGER,
          tripless_minutes REAL,
          stationary_minutes REAL,
          movement_minutes REAL,
          current_rating REAL,
          target_rating REAL,
          dispatch_uncertainty REAL,
          converted_to_trip INTEGER,
          moved_before_trip INTEGER,
          minutes_to_trip REAL,
          settled_at INTEGER,
          settlement_reason TEXT
        );
        """
    )
    _try_alter("ALTER TABLE assistant_guidance_outcomes ADD COLUMN moved_before_trip INTEGER;")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_assistant_guidance_outcomes_user_time ON assistant_guidance_outcomes(user_id, recommended_at DESC);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_assistant_guidance_outcomes_unsettled ON assistant_guidance_outcomes(user_id, converted_to_trip, recommended_at DESC);")

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
    _try_alter("ALTER TABLE chat_messages ADD COLUMN message_type TEXT NOT NULL DEFAULT 'text';")
    _try_alter("ALTER TABLE chat_messages ADD COLUMN audio_path TEXT;")
    _try_alter("ALTER TABLE chat_messages ADD COLUMN audio_mime_type TEXT;")
    _try_alter("ALTER TABLE chat_messages ADD COLUMN audio_duration_ms INTEGER;")
    _db_exec("UPDATE chat_messages SET room='global' WHERE room IS NULL OR trim(room)='';")
    _db_exec("UPDATE chat_messages SET message_type='text' WHERE message_type IS NULL OR trim(message_type)='';")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_id ON chat_messages(id);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_room_id ON chat_messages(room, id);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_created_at ON chat_messages(created_at);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_messages_room_created_at ON chat_messages(room, created_at DESC, id DESC);")

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS private_chat_messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          sender_user_id INTEGER NOT NULL,
          recipient_user_id INTEGER NOT NULL,
          text TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          read_at TEXT,
          FOREIGN KEY(sender_user_id) REFERENCES users(id),
          FOREIGN KEY(recipient_user_id) REFERENCES users(id)
        );
        """
    )
    _try_alter("ALTER TABLE private_chat_messages ADD COLUMN message_type TEXT NOT NULL DEFAULT 'text';")
    _try_alter("ALTER TABLE private_chat_messages ADD COLUMN audio_path TEXT;")
    _try_alter("ALTER TABLE private_chat_messages ADD COLUMN audio_mime_type TEXT;")
    _try_alter("ALTER TABLE private_chat_messages ADD COLUMN audio_duration_ms INTEGER;")
    _try_alter("ALTER TABLE private_chat_messages ADD COLUMN legacy_room_message_id INTEGER;")
    _db_exec("UPDATE private_chat_messages SET message_type='text' WHERE message_type IS NULL OR trim(message_type)='';")
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_private_chat_pair_created ON private_chat_messages(sender_user_id, recipient_user_id, created_at, id);"
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_private_chat_messages_created_at ON private_chat_messages(created_at);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_private_chat_sender_created ON private_chat_messages(sender_user_id, created_at DESC, id DESC);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_private_chat_recipient_created ON private_chat_messages(recipient_user_id, created_at DESC, id DESC);")
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_private_chat_recipient_read ON private_chat_messages(recipient_user_id, sender_user_id, read_at);"
    )
    _db_exec(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_private_chat_legacy_room_message_id ON private_chat_messages(legacy_room_message_id);"
    )
    _migrate_legacy_dm_rows_to_private()


# =========================================================
# Auth helpers (no external deps)
# =========================================================
bearer_scheme = HTTPBearer(auto_error=False)


def get_bearer_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> str:
    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    return credentials.credentials


def require_user(req: Request, token: str = Depends(get_bearer_token)) -> sqlite3.Row:
    # Reuse existing auth/trial checks while flowing through a Swagger-visible bearer scheme.
    scope = dict(req.scope)
    scope_headers = [(k, v) for (k, v) in scope.get("headers", []) if k.lower() != b"authorization"]
    scope_headers.append((b"authorization", f"Bearer {token}".encode("utf-8")))
    scope["headers"] = scope_headers
    request_with_bearer = Request(scope, receive=req.receive)
    return core_require_user(request_with_bearer)


def require_admin(user: sqlite3.Row = Depends(require_user)) -> sqlite3.Row:
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
    try:
        return normalize_avatar_data_url(value, MAX_AVATAR_DATA_URL_LENGTH)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _avatar_thumb_headers(user_id: int, version: str) -> Dict[str, str]:
    return {
        "Cache-Control": f"public, max-age={AVATAR_THUMB_IMMUTABLE_CACHE_SECONDS}, immutable",
        "ETag": f'"avatar-{int(user_id)}-{version}"',
    }


def _row_value(row: Any, key: str) -> Any:
    if row is None:
        return None
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    if isinstance(row, dict):
        return row.get(key)
    return None


def _avatar_version_for_row(row: Any) -> Optional[str]:
    if row is None:
        return None
    stored = _row_value(row, "avatar_version")
    if stored:
        return str(stored)
    avatar_data_url = _row_value(row, "avatar_url")
    if not avatar_data_url:
        return None
    return avatar_version_for_data_url(str(avatar_data_url))


def _avatar_thumb_url_for_row(row: Any) -> Optional[str]:
    user_id = _row_value(row, "id")
    if user_id is None:
        user_id = _row_value(row, "user_id")
    if user_id is None:
        return None
    return avatar_thumb_url(int(user_id), _avatar_version_for_row(row))


def _ensure_avatar_thumb_materialized(user_id: int, avatar_data_url: Optional[str], avatar_version: Optional[str]) -> Optional[str]:
    if not avatar_data_url:
        return None
    resolved_version = avatar_version or avatar_version_for_data_url(avatar_data_url)
    if not resolved_version:
        return None
    persist_avatar_thumb(DATA_DIR, int(user_id), avatar_data_url, resolved_version)
    return resolved_version


def _backfill_avatar_assets_worker() -> None:
    while True:
        rows = _db_query_all(
            """
            SELECT id, avatar_url, avatar_version
            FROM users
            WHERE avatar_url IS NOT NULL AND trim(avatar_url) <> ''
              AND (avatar_version IS NULL OR trim(avatar_version) = '')
            ORDER BY id ASC
            LIMIT ?
            """,
            (AVATAR_BACKFILL_BATCH_SIZE,),
        )
        if not rows:
            return
        for row in rows:
            avatar_data_url = row["avatar_url"]
            if not avatar_data_url:
                continue
            try:
                version = _ensure_avatar_thumb_materialized(int(row["id"]), str(avatar_data_url), None)
                if version:
                    _db_exec("UPDATE users SET avatar_version=? WHERE id=?", (version, int(row["id"])))
            except Exception:
                _debug_log("[debug] avatar backfill failed", int(row["id"]))


def _start_avatar_asset_backfill() -> None:
    global _avatar_backfill_started
    if _avatar_backfill_started:
        return
    _avatar_backfill_started = True
    threading.Thread(target=_backfill_avatar_assets_worker, name="avatar-thumb-backfill", daemon=True).start()


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


from chat import (
    _purge_expired_chat_data,
    list_legacy_global_messages,
    router as chat_router,
    send_legacy_global_text_message,
    start_chat_retention_sweeper,
)
from leaderboard_db import init_leaderboard_schema
from leaderboard_models import LeaderboardMetric, LeaderboardPeriod
from leaderboard_routes import router as leaderboard_router
from leaderboard_service import (
    get_best_current_badge_for_user,
    get_best_current_badges_for_users,
    get_leaderboard_runtime_snapshot,
    get_progression_for_user,
    get_my_rank,
    get_overview_for_user,
)
from games_routes import router as games_router
from games_service import (
    ensure_games_schema,
    get_active_match_between_users,
    get_battle_stats_for_user,
    get_viewer_game_relationship,
)
from work_battles_routes import router as work_battles_router
from work_battles_service import ensure_work_battles_schema
from leaderboard_tracker import increment_pickup_count, record_presence_heartbeat
from pickup_recording_feature import (
    router as pickup_recording_router,
    ensure_pickup_recording_schema,
    pickup_log_not_voided_sql,
    record_pickup_presence_heartbeat,
    create_pickup_record,
    register_pickup_write_cache_invalidation_hook,
)

app.include_router(chat_router)
app.include_router(leaderboard_router)
app.include_router(pickup_recording_router)
app.include_router(games_router)
app.include_router(work_battles_router)

# =========================================================
# Startup
# =========================================================
@app.on_event("startup")
def startup():
    def _log_runtime_integrity_summary() -> None:
        try:
            integrity_report = _artifact_runtime_integrity_report()
            print(
                f"[artifact-runtime] ok={integrity_report.get('ok')} "
                f"frame_count={integrity_report.get('frame_count')} parquet_count={integrity_report.get('parquet_count')} "
                f"redundant_files={integrity_report.get('redundant_file_copies_present')}"
            )
            if not integrity_report.get("ok"):
                print(f"[warn] artifact runtime integrity issues: {integrity_report}")
        except Exception:
            traceback.print_exc()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    EXACT_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    _clear_stale_generate_lock_if_orphaned()
    _db_init()
    ensure_generated_artifact_store_schema()
    _prune_redundant_db_backed_artifact_files()
    init_leaderboard_schema()
    ensure_pickup_recording_schema()
    ensure_games_schema()
    ensure_work_battles_schema()
    _ensure_admin_seed()
    try:
        _purge_expired_chat_data(force=True)
    except Exception:
        traceback.print_exc()
    try:
        start_chat_retention_sweeper()
    except Exception:
        traceback.print_exc()
    try:
        _start_avatar_asset_backfill()
    except Exception:
        traceback.print_exc()
    global _cleanup_last_startup_removed_count, _cleanup_last_startup_freed_bytes_estimate
    try:
        # Cleanup is intentionally limited to temp/build leftovers.
        # Safety guard: never auto-delete source parquet files, frame_*.json, or taxi_zones.geojson.
        cleanup_result = cleanup_artifact_storage(DATA_DIR, FRAMES_DIR)
        prune_result = _prune_redundant_db_backed_artifact_files()
        stale_build_prune = _prune_stale_month_build_dirs()
        stale_backup_prune = _prune_stale_month_backup_dirs()
        legacy_prune = _prune_legacy_frame_files_after_monthly_ready()
        obsolete_month_prune = _prune_obsolete_month_derived_artifacts()
        removed_count = int(cleanup_result.get("removed_count") or 0) + int(prune_result.get("removed_count") or 0)
        removed_count += int(stale_build_prune.get("removed_count") or 0)
        removed_count += int(stale_backup_prune.get("removed_count") or 0)
        removed_count += int(legacy_prune.get("removed_count") or 0)
        removed_count += int(obsolete_month_prune.get("removed_count") or 0)
        bytes_freed = int(cleanup_result.get("bytes_freed_estimate") or 0) + int(prune_result.get("bytes_freed_estimate") or 0)
        _cleanup_last_startup_removed_count = removed_count
        _cleanup_last_startup_freed_bytes_estimate = bytes_freed
        print(f"[storage-cleanup] removed={removed_count} freed_bytes_estimate={bytes_freed}")
    except Exception:
        _cleanup_last_startup_removed_count = 0
        _cleanup_last_startup_freed_bytes_estimate = 0
        traceback.print_exc()
    try:
        _start_storage_cleanup_sweeper()
    except Exception:
        traceback.print_exc()
    try:
        manifest = _load_month_manifest()
        for built_month_key in list(manifest.get("available_month_keys") or []):
            _maybe_repair_month_timeline_iso(str(built_month_key))
    except Exception:
        traceback.print_exc()

    # Startup auto-prepare: single target month only, core-only build mode.
    try:
        source_month_keys = _available_source_month_keys()
        target_month_key_candidate = resolve_active_month_key(
            datetime.now(timezone.utc).astimezone(NYC_TZ),
            source_month_keys,
        )
        if target_month_key_candidate:
            _ensure_month_live_bootstrap(target_month_key_candidate)
        target_ready = bool(target_month_key_candidate and _month_bootstrap_ready(target_month_key_candidate))
        if target_month_key_candidate and target_ready and _month_attestation_needed(target_month_key_candidate):
            _queue_active_month_attestation(target_month_key_candidate)
        startup_skip_reason = None

        # Evaluate retired-state and persisted backoff ONCE per startup for clean logging.
        _bootstrap_state_snapshot = (
            _month_bootstrap_state(target_month_key_candidate) if target_month_key_candidate else {}
        )
        _exact_store_retired_now = bool(_bootstrap_state_snapshot.get("exact_store_retired"))
        _exact_store_retired_reason_now = str(_bootstrap_state_snapshot.get("exact_store_retired_reason") or "")
        _store_exists_now = bool(_bootstrap_state_snapshot.get("store_exists"))
        _build_meta_orphaned_now = bool(
            _store_exists_now
            and bool(_bootstrap_state_snapshot.get("build_meta_present"))
            and not _bootstrap_state_snapshot.get("source_of_truth")
        )
        # Fix G: detect "new active month has source parquet but no exact_store yet."
        # Triggered typically by calendar month rollover (e.g., May 1 switches active month
        # from 2025-04 to 2025-05). _maybe_promote_parquet_live_authority writes a
        # parquet_live build_meta during bootstrap; this detector then upgrades to exact_store.
        _new_month_needs_exact_store_build_now = bool(
            (not _store_exists_now)
            and bool(_bootstrap_state_snapshot.get("build_meta_present"))
            and str(_bootstrap_state_snapshot.get("source_of_truth") or "").strip() == "parquet_live"
            and not bool(_bootstrap_state_snapshot.get("exact_store_retired"))
            and bool(_bootstrap_state_snapshot.get("source_parquet_exists"))
        )

        # Read the persisted auto-rebuild attempt state so backoff survives redeploys.
        _auto_rebuild_state = (
            _read_auto_rebuild_state(target_month_key_candidate) if target_month_key_candidate else {}
        )
        _last_auto_rebuild_attempt_unix = int(_auto_rebuild_state.get("last_attempt_unix") or 0)
        _now_unix_for_auto_rebuild = int(time.time())
        _auto_rebuild_in_persisted_backoff = bool(
            _last_auto_rebuild_attempt_unix > 0
            and (_now_unix_for_auto_rebuild - _last_auto_rebuild_attempt_unix) < int(AUTO_RETIRED_REBUILD_BACKOFF_SEC)
        )

        # Also respect the existing in-process failure backoff (separate, shorter gate).
        _in_process_failure_backoff = bool(
            _last_failed_month_key
            and target_month_key_candidate
            and str(_last_failed_month_key).strip() == str(target_month_key_candidate).strip()
            and _last_failed_at_unix is not None
            and (_now_unix_for_auto_rebuild - int(_last_failed_at_unix)) < int(MONTH_BUILD_FAILURE_BACKOFF_SEC)
        )

        # Once-per-boot guard — prevents double-triggering within the same process.
        global _auto_rebuild_triggered_this_boot

        if not (DATA_DIR / "taxi_zones.geojson").exists():
            startup_skip_reason = "zones_missing"
        elif not source_month_keys:
            startup_skip_reason = "no_source_month_keys"
        elif not target_month_key_candidate:
            startup_skip_reason = "no_target_month_candidate"
        elif _exact_store_retired_now and _auto_rebuild_triggered_this_boot:
            # Defense in depth: this process already triggered an auto-rebuild. Do NOT trigger again.
            startup_skip_reason = "exact_store_retired_already_triggered_this_boot"
            print(
                f"startup_auto_prepare_month_skipped_already_triggered_this_boot "
                f"month_key={target_month_key_candidate} retired_reason={_exact_store_retired_reason_now}"
            )
        elif _exact_store_retired_now and _auto_rebuild_in_persisted_backoff:
            # Persisted backoff — another process (usually a previous Railway deploy) tried recently.
            # This is the key protection against rebuild-per-redeploy during active development.
            _remaining = int(AUTO_RETIRED_REBUILD_BACKOFF_SEC) - (
                _now_unix_for_auto_rebuild - _last_auto_rebuild_attempt_unix
            )
            startup_skip_reason = "exact_store_retired_in_persisted_backoff"
            print(
                f"startup_auto_prepare_month_skipped_persisted_backoff "
                f"month_key={target_month_key_candidate} retired_reason={_exact_store_retired_reason_now} "
                f"last_attempt_unix={_last_auto_rebuild_attempt_unix} remaining_sec={max(0, _remaining)}"
            )
        elif _exact_store_retired_now and _in_process_failure_backoff:
            # In-process failure backoff — this process already saw a rebuild fail.
            startup_skip_reason = "exact_store_retired_in_process_failure_backoff"
            print(
                f"startup_auto_prepare_month_skipped_in_process_failure_backoff "
                f"month_key={target_month_key_candidate} retired_reason={_exact_store_retired_reason_now}"
            )
        elif _exact_store_retired_now:
            # All three gates cleared — auto-rebuild IS appropriate.
            # Write the state file FIRST so the next redeploy (even if this process dies mid-build) sees the attempt.
            _write_auto_rebuild_state(target_month_key_candidate)
            _auto_rebuild_triggered_this_boot = True
            print(
                f"startup_auto_prepare_month_rebuild_triggered "
                f"month_key={target_month_key_candidate} retired_reason={_exact_store_retired_reason_now} "
                f"backoff_sec={AUTO_RETIRED_REBUILD_BACKOFF_SEC}"
            )
            # Leave startup_skip_reason as None — fall through to the existing rebuild path below.
        elif (not _exact_store_retired_now) and _build_meta_orphaned_now and _auto_rebuild_triggered_this_boot:
            # Fix F: orphaned build_meta (store exists, source_of_truth missing).
            # Defense in depth: this process already triggered an auto-rebuild. Do NOT trigger again.
            startup_skip_reason = "build_meta_orphaned_already_triggered_this_boot"
            print(
                f"startup_auto_prepare_month_skipped_already_triggered_this_boot "
                f"month_key={target_month_key_candidate} orphan_reason=build_meta_missing_source_of_truth"
            )
        elif (not _exact_store_retired_now) and _build_meta_orphaned_now and _auto_rebuild_in_persisted_backoff:
            # Fix F: orphaned build_meta — persisted backoff prevents rebuild-per-redeploy during development.
            _remaining_orphan = int(AUTO_RETIRED_REBUILD_BACKOFF_SEC) - (
                _now_unix_for_auto_rebuild - _last_auto_rebuild_attempt_unix
            )
            startup_skip_reason = "build_meta_orphaned_in_persisted_backoff"
            print(
                f"startup_auto_prepare_month_skipped_persisted_backoff "
                f"month_key={target_month_key_candidate} orphan_reason=build_meta_missing_source_of_truth "
                f"last_attempt_unix={_last_auto_rebuild_attempt_unix} remaining_sec={max(0, _remaining_orphan)}"
            )
        elif (not _exact_store_retired_now) and _build_meta_orphaned_now and _in_process_failure_backoff:
            # Fix F: orphaned build_meta — in-process failure backoff.
            startup_skip_reason = "build_meta_orphaned_in_process_failure_backoff"
            print(
                f"startup_auto_prepare_month_skipped_in_process_failure_backoff "
                f"month_key={target_month_key_candidate} orphan_reason=build_meta_missing_source_of_truth"
            )
        elif (not _exact_store_retired_now) and _build_meta_orphaned_now:
            # Fix F: orphaned build_meta — all three gates cleared, auto-rebuild IS appropriate.
            # Same mechanism as the retired path: write state FIRST so the next redeploy sees the attempt.
            _write_auto_rebuild_state(target_month_key_candidate)
            _auto_rebuild_triggered_this_boot = True
            print(
                f"startup_auto_prepare_month_rebuild_triggered "
                f"month_key={target_month_key_candidate} orphan_reason=build_meta_missing_source_of_truth "
                f"backoff_sec={AUTO_RETIRED_REBUILD_BACKOFF_SEC}"
            )
            # Leave startup_skip_reason as None — fall through to the existing rebuild path below.
        elif (not _exact_store_retired_now) and (not _build_meta_orphaned_now) and _new_month_needs_exact_store_build_now and _auto_rebuild_triggered_this_boot:
            # Fix G: new active month with source parquet but no exact_store yet.
            # Defense in depth: this process already triggered an auto-rebuild. Do NOT trigger again.
            startup_skip_reason = "new_month_exact_store_build_already_triggered_this_boot"
            print(
                f"startup_auto_prepare_month_skipped_already_triggered_this_boot "
                f"month_key={target_month_key_candidate} new_month_reason=parquet_live_needs_exact_store"
            )
        elif (not _exact_store_retired_now) and (not _build_meta_orphaned_now) and _new_month_needs_exact_store_build_now and _auto_rebuild_in_persisted_backoff:
            # Fix G: new month needs exact_store — persisted backoff prevents rebuild-per-redeploy.
            _remaining_new_month = int(AUTO_RETIRED_REBUILD_BACKOFF_SEC) - (
                _now_unix_for_auto_rebuild - _last_auto_rebuild_attempt_unix
            )
            startup_skip_reason = "new_month_exact_store_build_in_persisted_backoff"
            print(
                f"startup_auto_prepare_month_skipped_persisted_backoff "
                f"month_key={target_month_key_candidate} new_month_reason=parquet_live_needs_exact_store "
                f"last_attempt_unix={_last_auto_rebuild_attempt_unix} remaining_sec={max(0, _remaining_new_month)}"
            )
        elif (not _exact_store_retired_now) and (not _build_meta_orphaned_now) and _new_month_needs_exact_store_build_now and _in_process_failure_backoff:
            # Fix G: new month needs exact_store — in-process failure backoff.
            startup_skip_reason = "new_month_exact_store_build_in_process_failure_backoff"
            print(
                f"startup_auto_prepare_month_skipped_in_process_failure_backoff "
                f"month_key={target_month_key_candidate} new_month_reason=parquet_live_needs_exact_store"
            )
        elif (not _exact_store_retired_now) and (not _build_meta_orphaned_now) and _new_month_needs_exact_store_build_now:
            # Fix G: new month needs exact_store — all three gates cleared, auto-build IS appropriate.
            # Same mechanism as retired/orphaned: write state FIRST so the next redeploy sees the attempt.
            _write_auto_rebuild_state(target_month_key_candidate)
            _auto_rebuild_triggered_this_boot = True
            print(
                f"startup_auto_prepare_month_rebuild_triggered "
                f"month_key={target_month_key_candidate} new_month_reason=parquet_live_needs_exact_store "
                f"backoff_sec={AUTO_RETIRED_REBUILD_BACKOFF_SEC}"
            )
            # Leave startup_skip_reason as None — fall through to the existing rebuild path below.
        elif _month_bootstrap_ready(target_month_key_candidate):
            startup_skip_reason = "target_month_already_ready"

        if startup_skip_reason:
            print(f"startup_auto_prepare_month_skipped reason={startup_skip_reason}")
            _set_state(
                state="idle",
                bin_minutes=DEFAULT_BIN_MINUTES,
                min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
                result={
                    "ok": bool(target_ready),
                    "reason": startup_skip_reason,
                    "target_month_key": target_month_key_candidate,
                    "source_month_keys": source_month_keys,
                    "auto_prepare_mode": "single_target_month_core_only",
                },
            )
            _log_runtime_integrity_summary()
            return

        now_unix = int(time.time())
        startup_in_failure_backoff = bool(
            _last_failed_month_key
            and str(_last_failed_month_key).strip() == str(target_month_key_candidate).strip()
            and _last_failed_at_unix is not None
            and (now_unix - int(_last_failed_at_unix)) < int(MONTH_BUILD_FAILURE_BACKOFF_SEC)
        )
        if startup_in_failure_backoff:
            print(
                "startup_auto_prepare_month_skipped "
                f"reason=failure_backoff month_key={target_month_key_candidate} "
                f"backoff_sec={MONTH_BUILD_FAILURE_BACKOFF_SEC}"
            )
            _set_state(
                state="idle",
                bin_minutes=DEFAULT_BIN_MINUTES,
                min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
                result={
                    "ok": False,
                    "reason": "failure_backoff",
                    "target_month_key": target_month_key_candidate,
                    "source_month_keys": source_month_keys,
                    "auto_prepare_mode": "single_target_month_core_only",
                },
            )
            _log_runtime_integrity_summary()
            return

        print(f"startup_auto_prepare_month_start month_key={target_month_key_candidate}")
        start_generate(
            bin_minutes=DEFAULT_BIN_MINUTES,
            min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
            include_day_tendency=False,
            build_review_artifacts=False,
            month_key=target_month_key_candidate,
            build_all_months=False,
        )
    except Exception:
        _set_state(state="idle")
    try:
        from admin_auto_run_tests import run_startup_tests
        run_startup_tests()
    except Exception:
        # Never let auto-run tests block startup.
        traceback.print_exc()
    _log_runtime_integrity_summary()


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
            "/day_tendency/today",
            "/day_tendency/date/{ymd}",
            "/day_tendency/frame_context",
            "/day_tendency/month_benchmark",
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
            "/admin/exact_history/build_review_artifacts",
            "/admin/artifacts/trap_candidate_review",
            "/admin/artifacts/trap_candidate_review/metadata",
            "/admin/artifacts/trap_candidate_review/readiness",
            "/admin/artifacts/trap_candidate_review/readiness/citywide",
            "/admin/artifacts/summary",
        ],
    }


@app.get("/status")
def status():
    parquets = [p.name for p in _list_parquets()]
    protected_source_parquet_count = len(parquets)
    source_month_keys = _available_source_month_keys()
    zones_path = DATA_DIR / "taxi_zones.geojson"
    manifest_path = FRAMES_DIR / "scoring_shadow_manifest.json"
    timeline_artifact_in_db = generated_artifact_present("timeline")
    manifest_artifact_in_db = generated_artifact_present("scoring_shadow_manifest")
    day_tendency_artifact_in_db = generated_artifact_present("day_tendency_model")
    trap_candidate_review_artifact_in_db = generated_artifact_present("trap_candidate_review")
    assistant_outlook_artifact_in_db = generated_artifact_present("assistant_outlook")
    assistant_outlook_file_present = ASSISTANT_OUTLOOK_PATH.exists() and ASSISTANT_OUTLOOK_PATH.stat().st_size > 0 if ASSISTANT_OUTLOOK_PATH.exists() else False
    assistant_outlook_file_bytes = int(ASSISTANT_OUTLOOK_PATH.stat().st_size) if ASSISTANT_OUTLOOK_PATH.exists() and ASSISTANT_OUTLOOK_PATH.is_file() else 0
    assistant_outlook_file_valid_json = _assistant_outlook_file_is_valid_json() if assistant_outlook_file_present else False
    assistant_outlook_source_mode = "on_demand_frame_bucket" if _has_assistant_outlook() else "missing"
    day_tendency_file_present = DAY_TENDENCY_MODEL_PATH.exists() and DAY_TENDENCY_MODEL_PATH.stat().st_size > 0 if DAY_TENDENCY_MODEL_PATH.exists() else False
    manifest_file_present = manifest_path.exists() and manifest_path.stat().st_size > 0 if manifest_path.exists() else False
    month_manifest = _load_month_manifest()
    available_month_keys = list(month_manifest.get("available_month_keys") or [])
    target_month_key_candidate = resolve_active_month_key(datetime.now(timezone.utc).astimezone(NYC_TZ), source_month_keys)
    active_month_key = resolve_active_month_key(datetime.now(timezone.utc).astimezone(NYC_TZ), available_month_keys) or target_month_key_candidate
    active_timeline_path = _month_timeline_path(active_month_key) if active_month_key else None
    active_store_path = _month_store_path(active_month_key) if active_month_key else None
    active_frame_cache_dir = _month_frame_cache_dir(active_month_key) if active_month_key else None
    timeline_file_present = bool(active_timeline_path and active_timeline_path.exists() and active_timeline_path.stat().st_size > 0)
    frame_cache_dir_present = bool(active_frame_cache_dir and active_frame_cache_dir.exists() and active_frame_cache_dir.is_dir())
    frame_cache_file_count = len(list(active_frame_cache_dir.glob("frame_*.json"))) if frame_cache_dir_present and active_frame_cache_dir else 0
    exact_history_store_present = bool(active_store_path and active_store_path.exists() and active_store_path.stat().st_size > 0)
    exact_history_store_bytes = int(active_store_path.stat().st_size) if active_store_path and active_store_path.exists() and active_store_path.is_file() else 0
    timeline_present = timeline_artifact_in_db or timeline_file_present
    manifest_present = manifest_artifact_in_db
    freshness = _artifact_freshness_snapshot()
    identity = _backend_identity_snapshot(freshness)
    artifact_runtime_integrity = _artifact_runtime_integrity_report()
    leaderboard_runtime = get_leaderboard_runtime_snapshot()
    stale_lock_detected = bool(_lock_is_present() and not _generate_thread_alive())
    generate_state = _get_state()
    state_name = str(generate_state.get("state") or "")
    state_run_token = str(generate_state.get("run_token") or "").strip() or None
    lock_token = _read_lock_token()
    if state_run_token and lock_token:
        lock_token_matches_state: Optional[bool] = lock_token == state_run_token
    else:
        lock_token_matches_state = None
    pending_month_key = str(generate_state.get("month_key") or "").strip() if state_name in {"started", "running"} else ""
    pending_month_key = pending_month_key or None
    pending_month_label = _format_month_key_label(pending_month_key) if pending_month_key else None
    last_failed_month_key = str(_last_failed_month_key or "").strip() or None
    last_failed_month_label = _format_month_key_label(last_failed_month_key) if last_failed_month_key else None
    stale_month_build_dirs_count = _count_stale_subdirs(EXACT_HISTORY_MONTHS_BUILDING_DIR)
    stale_month_backup_dirs_count = _count_stale_subdirs(EXACT_HISTORY_MONTHS_BACKUP_DIR)
    legacy_frame_file_count = _legacy_frame_file_count()
    month_freshness = _active_month_freshness(active_month_key) if active_month_key else {}
    month_expected = month_freshness.get("expected") or {}
    month_build_meta = month_freshness.get("build_meta") or {}
    active_month_signature_matches_code = bool(month_freshness.get("code_dependency_hash_match"))
    active_month_signature_matches_source = bool(month_freshness.get("source_data_hash_match"))
    active_month_artifact_signature_matches = bool(month_freshness.get("artifact_signature_match"))
    active_month_build_meta_present = bool(month_freshness.get("build_meta_present"))
    active_month_signature_matches_expected = bool(month_freshness.get("signature_match"))
    active_bootstrap_state = _month_bootstrap_state(active_month_key) if active_month_key else {}
    active_month_live_ready = bool(active_bootstrap_state.get("live_ready"))
    active_month_exact_store_fresh = bool(active_bootstrap_state.get("exact_store_fresh"))
    active_month_serving_mode = str(active_bootstrap_state.get("serving_mode") or "rebuild_required")
    active_month_legacy_ready_without_build_meta = bool(active_bootstrap_state.get("legacy_ready_without_build_meta"))
    active_month_build_meta_backfill_pending = bool(active_bootstrap_state.get("build_meta_backfill_pending"))
    active_month_authoritative_fresh = bool(active_bootstrap_state.get("authoritative_fresh"))
    active_month_source_of_truth = active_bootstrap_state.get("source_of_truth")
    active_month_exact_store_retired = bool(active_bootstrap_state.get("exact_store_retired"))
    active_month_exact_store_retired_reason = active_bootstrap_state.get("exact_store_retired_reason")
    attestation_trigger_report = {}
    if active_month_key and _month_attestation_needed(active_month_key):
        attestation_trigger_report = _queue_active_month_attestation(active_month_key)
    attestation_state = _attestation_state_snapshot(active_month_key) if active_month_key else {}
    attestation_report = _last_attestation_report_by_month.get(str(active_month_key or "").strip()) if active_month_key else {}
    if bool(attestation_report and attestation_report.get("ok")):
        month_freshness = _active_month_freshness(active_month_key) if active_month_key else {}
        active_bootstrap_state = _month_bootstrap_state(active_month_key) if active_month_key else {}
        active_month_live_ready = bool(active_bootstrap_state.get("live_ready"))
        active_month_exact_store_fresh = bool(active_bootstrap_state.get("exact_store_fresh"))
        active_month_serving_mode = str(active_bootstrap_state.get("serving_mode") or "rebuild_required")
        active_month_legacy_ready_without_build_meta = bool(active_bootstrap_state.get("legacy_ready_without_build_meta"))
        active_month_build_meta_backfill_pending = bool(active_bootstrap_state.get("build_meta_backfill_pending"))
        active_month_authoritative_fresh = bool(active_bootstrap_state.get("authoritative_fresh"))
        active_month_source_of_truth = active_bootstrap_state.get("source_of_truth")
        active_month_exact_store_retired = bool(active_bootstrap_state.get("exact_store_retired"))
        active_month_exact_store_retired_reason = active_bootstrap_state.get("exact_store_retired_reason")
        active_month_build_meta_present = bool(month_freshness.get("build_meta_present"))
        active_month_signature_matches_code = bool(month_freshness.get("code_dependency_hash_match"))
        active_month_signature_matches_source = bool(month_freshness.get("source_data_hash_match"))
        active_month_artifact_signature_matches = bool(month_freshness.get("artifact_signature_match"))
        active_month_signature_matches_expected = bool(month_freshness.get("signature_match"))
    stale_frame_cache_file_count_removed = 0
    if active_month_key and not active_month_live_ready and not active_month_exact_store_fresh:
        stale_frame_cache_file_count_removed = _purge_month_frame_cache(active_month_key)
    active_month_strict_ready = bool(
        MONTH_MANIFEST_PATH.exists()
        and month_manifest.get("months", {}).get(active_month_key)
        and timeline_file_present
        and exact_history_store_present
        and active_month_build_meta_present
        and active_month_signature_matches_expected
    )
    active_month_core_ready = bool(active_month_live_ready and (active_month_authoritative_fresh or active_month_serving_mode == "parquet_live_bootstrap" or active_month_strict_ready or active_month_legacy_ready_without_build_meta))
    active_month_rebuild_required = bool(
        active_month_key and (active_month_serving_mode == "rebuild_required")
    )
    build_review_artifacts_in_progress = bool(
        state_name in {"started", "running"} and bool(generate_state.get("build_review_artifacts"))
    )
    return {
        "status": "ok",
        "timeline_mode": "monthly_exact_historical",
        "frame_time_model": "exact_local_20min",
        "synthetic_week_enabled": False,
        "data_dir": str(DATA_DIR),
        "data_dir_exists": DATA_DIR.exists(),
        "upload_streaming_enabled": True,
        "parquet_delete_enabled": True,
        "parquets": parquets,
        "protected_source_parquet_count": int(protected_source_parquet_count),
        "source_month_keys": source_month_keys,
        "target_month_key_candidate": target_month_key_candidate,
        "parquet_inventory_warning_count": int(_parquet_inventory_snapshot.get("warning_count") or 0),
        "parquet_inventory_warnings": list(_parquet_inventory_snapshot.get("warnings") or []),
        "zones_geojson": zones_path.name if zones_path.exists() else None,
        "zones_present": zones_path.exists(),
        "backend_build_id": identity.get("backend_build_id"),
        "backend_release": identity.get("backend_release"),
        "backend_identity_source": identity.get("source"),
        "frames_dir": str(FRAMES_DIR),
        "exact_history_dir": str(EXACT_HISTORY_DIR),
        "exact_history_store_path": str(active_store_path) if active_store_path else None,
        "exact_history_store_present": exact_history_store_present,
        "exact_history_store_bytes": exact_history_store_bytes,
        "auto_generate_on_startup": AUTO_GENERATE_ON_STARTUP,
        "manifest_present": manifest_present,
        "timeline_present": timeline_present,
        "has_timeline": _has_frames(),
        "assistant_outlook_present": _has_assistant_outlook(),
        "cleanup_last_startup_removed_count": _cleanup_last_startup_removed_count,
        "cleanup_last_startup_freed_bytes_estimate": _cleanup_last_startup_freed_bytes_estimate,
        "cleanup_last_periodic_removed_count": _cleanup_last_periodic_removed_count,
        "cleanup_last_periodic_freed_bytes_estimate": _cleanup_last_periodic_freed_bytes_estimate,
        "cleanup_last_periodic_ran_at_unix": _cleanup_last_periodic_ran_at_unix,
        "reconcile_last_periodic_deleted_paths": list(_reconcile_last_periodic_deleted_paths),
        "reconcile_last_periodic_ran_at_unix": _reconcile_last_periodic_ran_at_unix,
        "timeline_artifact_in_db": timeline_artifact_in_db,
        "manifest_artifact_in_db": manifest_artifact_in_db,
        "day_tendency_artifact_in_db": day_tendency_artifact_in_db,
        "trap_candidate_review_artifact_in_db": trap_candidate_review_artifact_in_db,
        "trap_candidate_review_readiness_available": trap_candidate_review_artifact_in_db,
        "citywide_trap_candidate_live_promotion_enabled": CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED,
        "citywide_visible_source_expected": (
            "citywide_v3_trap_candidate"
            if CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED
            else "citywide_v3"
        ),
        "assistant_outlook_in_db": assistant_outlook_artifact_in_db,
        "assistant_outlook_artifact_in_db": assistant_outlook_artifact_in_db,
        "assistant_outlook_file_present": assistant_outlook_file_present,
        "assistant_outlook_file_bytes": assistant_outlook_file_bytes,
        "assistant_outlook_file_valid_json": assistant_outlook_file_valid_json,
        "assistant_outlook_source_mode": assistant_outlook_source_mode,
        "assistant_outlook_mode": "on_demand_frame_bucket",
        "assistant_outlook_frame_bucket_cache_entries": len(_assistant_outlook_frame_bucket_cache),
        "assistant_outlook_legacy_artifact_present": assistant_outlook_artifact_in_db,
        "assistant_outlook_legacy_artifact_pruned": _assistant_outlook_legacy_artifact_pruned,
        "day_tendency_file_present": day_tendency_file_present,
        "manifest_file_present": manifest_file_present,
        "timeline_file_present": timeline_file_present,
        "month_manifest_path": str(MONTH_MANIFEST_PATH),
        "month_manifest_present": MONTH_MANIFEST_PATH.exists(),
        "monthly_partition_mode": True,
        "auto_month_generation_enabled": True,
        "active_month_key": active_month_key,
        "available_month_keys": available_month_keys,
        "monthly_partition_count": len(available_month_keys),
        "active_month_store_present": exact_history_store_present,
        "active_month_timeline_present": timeline_file_present,
        "active_month_timeline_ready": timeline_file_present,
        "active_month_frame_cache_dir_present": frame_cache_dir_present,
        "active_month_frame_cache_ready": frame_cache_dir_present,
        "active_month_store_cache_present": exact_history_store_present,
        "active_month_bootstrap_ready": bool(active_month_live_ready or active_month_exact_store_fresh or active_month_legacy_ready_without_build_meta),
        "active_month_live_ready": active_month_live_ready,
        "active_month_exact_store_fresh": active_month_exact_store_fresh,
        "active_month_authoritative_fresh": active_month_authoritative_fresh,
        "active_month_source_of_truth": active_month_source_of_truth,
        "active_month_exact_store_retired": active_month_exact_store_retired,
        "active_month_exact_store_retired_reason": active_month_exact_store_retired_reason,
        "active_month_serving_mode": active_month_serving_mode,
        "frame_cache_file_count": int(frame_cache_file_count),
        "active_month_build_meta_present": active_month_build_meta_present,
        "active_month_signature_matches_code": active_month_signature_matches_code,
        "active_month_signature_matches_source": active_month_signature_matches_source,
        "active_month_artifact_signature_matches": active_month_artifact_signature_matches,
        "active_month_legacy_ready_without_build_meta": active_month_legacy_ready_without_build_meta,
        "active_month_build_meta_backfill_pending": active_month_build_meta_backfill_pending,
        "active_month_rebuild_required": active_month_rebuild_required,
        "active_month_attestation_report": attestation_report,
        "active_month_attestation_trigger_report": attestation_trigger_report,
        "attestation_in_progress": bool(attestation_state.get("attestation_in_progress")),
        "attestation_started_at_unix": attestation_state.get("attestation_started_at_unix"),
        "attestation_finished_at_unix": attestation_state.get("attestation_finished_at_unix"),
        "attestation_last_result": attestation_state.get("attestation_last_result"),
        "attestation_last_error": attestation_state.get("attestation_last_error"),
        "stale_frame_cache_file_count_removed": int(stale_frame_cache_file_count_removed),
        "active_month_build_meta_artifact_signature": month_build_meta.get("artifact_signature"),
        "active_month_expected_artifact_signature": month_expected.get("artifact_signature"),
        "pending_month_key": pending_month_key,
        "pending_month_label": pending_month_label,
        "last_failed_month_key": last_failed_month_key,
        "last_failed_month_label": last_failed_month_label,
        "last_failed_at_unix": _last_failed_at_unix,
        "last_failed_error": _last_failed_error,
        "stale_month_build_dirs_count": stale_month_build_dirs_count,
        "stale_month_backup_dirs_count": stale_month_backup_dirs_count,
        "legacy_frame_file_count": legacy_frame_file_count,
        "generated_artifact_store_report": generated_artifact_report(),
        "artifact_runtime_policy": _artifact_runtime_policy_snapshot(),
        "artifact_runtime_integrity": artifact_runtime_integrity,
        "core_map_ready": bool(active_month_live_ready and (active_month_authoritative_fresh or active_month_serving_mode == "parquet_live_bootstrap" or active_month_legacy_ready_without_build_meta)),
        "active_month_core_ready": active_month_core_ready,
        "build_review_artifacts_in_progress": build_review_artifacts_in_progress,
        "optional_artifacts_missing": list(artifact_runtime_integrity.get("optional_artifacts_missing") or []),
        "generate_state": _get_state(),
        "run_token": state_run_token,
        "generate_in_progress": state_name in {"started", "running"},
        "generate_lock_token_present": lock_token is not None,
        "generate_lock_token_matches_state": lock_token_matches_state,
        "generate_lock": _generate_lock_snapshot(),
        "generate_stale_lock_detected": stale_lock_detected,
        "artifact_freshness": freshness,
        "storage_report": get_artifact_storage_report(DATA_DIR, FRAMES_DIR),
        "community_db": os.environ.get("COMMUNITY_DB", str(DATA_DIR / "community.db")),
        "trial_days": TRIAL_DAYS,
        "trial_enforced": ENFORCE_TRIAL,
        "auth_enabled": bool(JWT_SECRET and len(JWT_SECRET) >= 24),
        "performance_metrics": _perf_metric_snapshot(),
        "postgres_pool_min": POSTGRES_POOL_MIN,
        "postgres_pool_max": POSTGRES_POOL_MAX,
        "current_badges_last_refresh_ts": leaderboard_runtime.get("current_badges_last_refresh_ts"),
        "current_badges_refresh_interval_seconds": leaderboard_runtime.get("current_badges_refresh_interval_seconds"),
        "current_badges_refresh_lock_active": leaderboard_runtime.get("current_badges_refresh_lock_active"),
        "leaderboard_badges_cache_entries": leaderboard_runtime.get("leaderboard_badges_cache_entries"),
        "leaderboard_progression_cache_entries": leaderboard_runtime.get("leaderboard_progression_cache_entries"),
    }


def _safe_profile_review_dict(payload: dict, profile_name: str) -> dict | None:
    if not isinstance(payload, dict):
        return None
    profile_reviews = payload.get("profile_reviews")
    if not isinstance(profile_reviews, dict):
        return None
    profile_review = profile_reviews.get(profile_name)
    if not isinstance(profile_review, dict):
        return None
    return profile_review


def _count_recurring_rows(rows: Any) -> int:
    if not isinstance(rows, list):
        return 0
    count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        location_id = row.get("LocationID")
        if isinstance(location_id, int):
            count += 1
            continue
        if isinstance(location_id, str) and location_id.strip():
            count += 1
    return count


def _evaluate_trap_candidate_readiness(profile_name: str, review: dict) -> dict:
    config = TRAP_CANDIDATE_PROMOTION_READINESS_CONFIG.get(profile_name) or {}

    def _as_int(name: str) -> int:
        value = review.get(name, 0)
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        return 0

    def _as_float(name: str) -> float | None:
        value = review.get(name)
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        return None

    eligible_zone_observations = _as_int("eligible_zone_observations")
    promoted_observations = _as_int("promoted_observations")
    demoted_observations = _as_int("demoted_observations")
    average_delta_overall = _as_float("average_delta_overall")
    min_delta_seen = _as_float("min_delta_seen")
    max_delta_seen = _as_float("max_delta_seen")
    recurring_top_demotions_count = _count_recurring_rows(review.get("top_recurring_demotions"))
    recurring_top_promotions_count = _count_recurring_rows(review.get("top_recurring_promotions"))

    avg_delta_value = average_delta_overall if average_delta_overall is not None else 0.0
    observation_floor_ok = eligible_zone_observations >= int(config.get("min_observations", 0))
    recurring_demotions_ok = recurring_top_demotions_count >= int(config.get("min_recurring_demotions", 0))
    average_delta_in_target_band = (
        float(config.get("target_average_delta_low", float("-inf")))
        <= avg_delta_value
        <= float(config.get("target_average_delta_high", float("inf")))
    )
    average_delta_not_too_large = abs(avg_delta_value) <= float(config.get("max_abs_average_delta", float("inf")))
    positive_extreme_ok = max_delta_seen is None or max_delta_seen <= float(config.get("max_positive_extreme", float("inf")))
    negative_extreme_ok = min_delta_seen is None or min_delta_seen >= float(config.get("max_negative_extreme", float("-inf")))
    require_demotions_gt_promotions = bool(config.get("require_demotions_gt_promotions"))
    demotion_balance_ok = (demoted_observations > promoted_observations) if require_demotions_gt_promotions else True

    checks = {
        "observation_floor_ok": observation_floor_ok,
        "recurring_demotions_ok": recurring_demotions_ok,
        "average_delta_in_target_band": average_delta_in_target_band,
        "average_delta_not_too_large": average_delta_not_too_large,
        "positive_extreme_ok": positive_extreme_ok,
        "negative_extreme_ok": negative_extreme_ok,
        "demotion_balance_ok": demotion_balance_ok,
    }
    secondary_true_count = sum(
        1
        for value in [
            recurring_demotions_ok,
            average_delta_in_target_band,
            average_delta_not_too_large,
            positive_extreme_ok,
            negative_extreme_ok,
            demotion_balance_ok,
        ]
        if value
    )
    if all(checks.values()):
        status_value = "ready"
    elif observation_floor_ok and secondary_true_count >= 4:
        status_value = "borderline"
    else:
        status_value = "not_ready"

    reasons: List[str] = []
    if status_value == "ready":
        reasons.append("all readiness checks passed")
    else:
        if not observation_floor_ok:
            reasons.append("insufficient eligible observations")
        if not recurring_demotions_ok:
            reasons.append("not enough recurring demotion zones")
        if not average_delta_in_target_band:
            reasons.append("average delta outside target band")
        if not average_delta_not_too_large:
            reasons.append("average delta magnitude too large")
        if not positive_extreme_ok:
            reasons.append("positive delta spike exceeds threshold")
        if not negative_extreme_ok:
            reasons.append("negative delta spike exceeds threshold")
        if not demotion_balance_ok:
            reasons.append("demotions do not exceed promotions")
        if status_value == "borderline":
            reasons.append("meets observation floor with partial signal alignment")

    metrics = {
        "eligible_zone_observations": eligible_zone_observations,
        "promoted_observations": promoted_observations,
        "demoted_observations": demoted_observations,
        "average_delta_overall": average_delta_overall,
        "min_delta_seen": min_delta_seen,
        "max_delta_seen": max_delta_seen,
        "recurring_top_demotions_count": recurring_top_demotions_count,
        "recurring_top_promotions_count": recurring_top_promotions_count,
    }
    return {
        "profile_name": profile_name,
        "status": status_value,
        "recommended_next_phase": (
            config.get("recommended_next_phase", "keep_shadow_only") if status_value == "ready" else "keep_shadow_only"
        ),
        "checks": checks,
        "metrics": metrics,
        "reasons": reasons,
    }


@app.get("/admin/artifacts/trap_candidate_review")
def admin_trap_candidate_review_artifact(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    artifact = load_generated_artifact("trap_candidate_review")
    if not artifact:
        raise HTTPException(status_code=404, detail="trap_candidate_review not found")
    return {
        "ok": True,
        "artifact_key": "trap_candidate_review",
        "metadata": artifact.get("metadata"),
        "payload": artifact.get("payload"),
    }


@app.get("/admin/artifacts/trap_candidate_review/metadata")
def admin_trap_candidate_review_artifact_metadata(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    metadata = load_generated_artifact_metadata("trap_candidate_review")
    if not metadata:
        raise HTTPException(status_code=404, detail="trap_candidate_review not found")
    return {
        "ok": True,
        "artifact_key": "trap_candidate_review",
        "metadata": metadata,
    }


@app.get("/admin/artifacts/trap_candidate_review/readiness")
def admin_trap_candidate_review_readiness(
    profile: Optional[str] = None,
    admin: sqlite3.Row = Depends(require_admin),
):
    _ = admin
    artifact = load_generated_artifact("trap_candidate_review")
    if not artifact:
        raise HTTPException(status_code=404, detail="trap_candidate_review not found")
    payload = artifact.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    target_profiles = list(TRAP_CANDIDATE_PROMOTION_READINESS_CONFIG.keys())
    if profile is not None:
        if profile not in TRAP_CANDIDATE_PROMOTION_READINESS_CONFIG:
            raise HTTPException(status_code=400, detail="unknown trap candidate profile")
        target_profiles = [profile]

    profiles_payload: Dict[str, Any] = {}
    for profile_name in target_profiles:
        review = _safe_profile_review_dict(payload, profile_name) or {}
        profiles_payload[profile_name] = _evaluate_trap_candidate_readiness(profile_name, review)

    metadata = artifact.get("metadata") or {}
    return {
        "ok": True,
        "artifact_key": "trap_candidate_review",
        "artifact_updated_at_unix": metadata.get("updated_at_unix"),
        "profiles": profiles_payload,
    }


@app.get("/admin/artifacts/trap_candidate_review/readiness/citywide")
def admin_trap_candidate_review_readiness_citywide(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    artifact = load_generated_artifact("trap_candidate_review")
    if not artifact:
        raise HTTPException(status_code=404, detail="trap_candidate_review not found")
    payload = artifact.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    profile_name = "citywide_v3_trap_candidate"
    review = _safe_profile_review_dict(payload, profile_name) or {}
    readiness = _evaluate_trap_candidate_readiness(profile_name, review)
    return {
        "ok": True,
        "profile_name": profile_name,
        "readiness": readiness,
    }


@app.get("/admin/artifacts/summary")
def admin_artifacts_summary(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    return {
        "ok": True,
        "generated_artifact_store_report": generated_artifact_report(),
        "trap_candidate_review_present": generated_artifact_present("trap_candidate_review"),
        "scoring_shadow_manifest_present": generated_artifact_present("scoring_shadow_manifest"),
        "timeline_present": generated_artifact_present("timeline"),
        "day_tendency_model_present": generated_artifact_present("day_tendency_model"),
    }


@app.get("/admin/artifacts/runtime_integrity")
def admin_artifact_runtime_integrity(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    return _artifact_runtime_integrity_report()


@app.post("/admin/artifacts/reconcile_runtime")
def admin_reconcile_artifact_runtime(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    return _reconcile_artifact_runtime_state()


@app.get("/admin/performance/metrics")
def admin_performance_metrics(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    metrics = _perf_metric_snapshot()

    def ratio(hit_key: str, miss_key: str) -> Optional[float]:
        hits = int(metrics.get(hit_key, 0))
        misses = int(metrics.get(miss_key, 0))
        total = hits + misses
        if total <= 0:
            return None
        return round(hits / total, 4)

    return {
        "ok": True,
        "counters": metrics,
        "ratios": {
            "timeline_cache_hit_rate": ratio("timeline.cache_hit", "timeline.cache_miss"),
            "frame_cache_hit_rate": ratio("frame.cache_hit", "frame.cache_miss"),
            "presence_cache_hit_rate": ratio("presence.cache_hit", "presence.cache_miss"),
            "pickup_recent_cache_hit_rate": ratio("pickup_recent.cache_hit", "pickup_recent.cache_miss"),
            "avatar_thumb_cache_hit_rate": ratio("avatar_thumb.cache_hit", "avatar_thumb.cache_miss"),
            "pickup_hotspot_cache_hit_rate": ratio("pickup_hotspot.cache_hit", "pickup_hotspot.cache_miss"),
            "pickup_score_bundle_hit_rate": ratio("pickup_score_bundle.cache_hit", "pickup_score_bundle.cache_miss"),
        },
    }


@app.get("/system/diagnostics")
def system_diagnostics(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    required_tables = [
        "users",
        "presence",
        "presence_runtime_state",
        "chat_messages",
        "private_chat_messages",
        "work_battle_challenges",
        "game_challenges",
        "game_matches",
        "game_match_participants",
        "game_match_moves",
        "game_xp_awards",
        "leaderboard_badges_current",
        "leaderboard_badges_refresh_state",
        "recommendation_outcomes",
        "hotspot_experiment_bins",
    ]
    table_state: Dict[str, bool] = {}
    for name in required_tables:
        if DB_BACKEND == "postgres":
            row = _db_query_one("SELECT to_regclass(?) AS exists_name", (f"public.{name}",))
            table_state[name] = bool(row and row.get("exists_name"))
        else:
            row = _db_query_one(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (name,),
            )
            table_state[name] = row is not None
    return {
        "ok": True,
        "db_backend": DB_BACKEND,
        "backend_build_id": os.environ.get("BACKEND_BUILD_ID"),
        "backend_release": os.environ.get("BACKEND_RELEASE"),
        "tables": table_state,
        "games_schema_present": all(
            table_state.get(n, False)
            for n in ["game_challenges", "game_matches", "game_match_participants", "game_match_moves", "game_xp_awards"]
        ),
        "games_schema": {
            "game_challenges": bool(table_state.get("game_challenges")),
            "game_matches": bool(table_state.get("game_matches")),
            "game_match_participants": bool(table_state.get("game_match_participants")),
            "game_match_moves": bool(table_state.get("game_match_moves")),
            "game_xp_awards": bool(table_state.get("game_xp_awards")),
        },
        "work_battles_schema_present": bool(table_state.get("work_battle_challenges")),
    }


@app.get("/generate")
def generate_get(
    bin_minutes: int = DEFAULT_BIN_MINUTES,
    min_trips_per_window: int = DEFAULT_MIN_TRIPS_PER_WINDOW,
    force_clear_lock: int = 0,
    month_key: Optional[str] = None,
    build_all_months: int = 0,
    include_day_tendency: int = 0,
    build_review_artifacts: int = 0,
):
    return start_generate(
        bin_minutes,
        min_trips_per_window,
        force_clear_lock=bool(int(force_clear_lock or 0)),
        include_day_tendency=bool(int(include_day_tendency or 0)),
        build_review_artifacts=bool(int(build_review_artifacts or 0)),
        month_key=(str(month_key).strip() if month_key else None),
        build_all_months=bool(int(build_all_months or 0)),
    )


@app.post("/admin/exact_history/build_review_artifacts")
def admin_build_review_artifacts(
    month_key: str,
    admin: sqlite3.Row = Depends(require_admin),
):
    _ = admin
    requested_month_key = str(month_key or "").strip()
    if not _safe_parse_month_key(requested_month_key):
        raise HTTPException(status_code=400, detail="invalid month_key format; expected YYYY-MM")
    return start_generate(
        DEFAULT_BIN_MINUTES,
        DEFAULT_MIN_TRIPS_PER_WINDOW,
        force_clear_lock=False,
        include_day_tendency=False,
        build_review_artifacts=True,
        month_key=requested_month_key,
        build_all_months=False,
    )


@app.post("/admin/generate/clear_lock")
def admin_generate_clear_lock(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    lock_present = _lock_is_present()
    lock_cleared = False
    if lock_present:
        _clear_lock()
        lock_cleared = True
    thread_alive = _generate_thread_alive()
    state_now = _get_state()
    if not thread_alive and state_now.get("state") == "running":
        _set_state(state="idle")
        state_now = _get_state()
    return {
        "ok": True,
        "lock_cleared": lock_cleared,
        "thread_alive": thread_alive,
        "state": state_now.get("state"),
    }


@app.get("/generate_status")
def generate_status():
    return _get_state()


@app.get("/day_tendency/today")
def day_tendency_today(
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    manhattan_mode: Optional[int] = None,
    staten_island_mode: Optional[int] = None,
    bronx_wash_heights_mode: Optional[int] = None,
    queens_mode: Optional[int] = None,
    brooklyn_mode: Optional[int] = None,
):
    if not _has_day_tendency_model():
        print("[warn] day tendency model missing; call /generate or allow startup backfill")
        raise HTTPException(status_code=409, detail="day tendency not ready. Call /generate first.")
    target_date = datetime.now(timezone.utc).astimezone(NYC_TZ).date()
    mode_flags = {
        "manhattan_mode": manhattan_mode,
        "staten_island_mode": staten_island_mode,
        "bronx_wash_heights_mode": bronx_wash_heights_mode,
        "queens_mode": queens_mode,
        "brooklyn_mode": brooklyn_mode,
    }
    payload = _resolve_day_tendency_payload(target_date, lat=lat, lng=lng, mode_flags=mode_flags)
    _debug_log("[debug] day_tendency_today payload:", payload)
    return payload


@app.get("/day_tendency/date/{ymd}")
def day_tendency_for_date(
    ymd: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    manhattan_mode: Optional[int] = None,
    staten_island_mode: Optional[int] = None,
    bronx_wash_heights_mode: Optional[int] = None,
    queens_mode: Optional[int] = None,
    brooklyn_mode: Optional[int] = None,
):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", ymd or ""):
        raise HTTPException(status_code=400, detail="ymd must be YYYY-MM-DD")
    try:
        parsed_date = date.fromisoformat(ymd)
    except Exception:
        raise HTTPException(status_code=400, detail="ymd must be YYYY-MM-DD")

    if not _has_day_tendency_model():
        print("[warn] day tendency model missing; call /generate or allow startup backfill")
        raise HTTPException(status_code=409, detail="day tendency not ready. Call /generate first.")
    mode_flags = {
        "manhattan_mode": manhattan_mode,
        "staten_island_mode": staten_island_mode,
        "bronx_wash_heights_mode": bronx_wash_heights_mode,
        "queens_mode": queens_mode,
        "brooklyn_mode": brooklyn_mode,
    }
    payload = _resolve_day_tendency_payload(parsed_date, lat=lat, lng=lng, mode_flags=mode_flags)
    _debug_log("[debug] day_tendency_for_date payload:", payload)
    return payload


@app.get("/day_tendency/frame_context")
def day_tendency_frame_context(
    frame_time: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    manhattan_mode: Optional[int] = None,
    staten_island_mode: Optional[int] = None,
    bronx_wash_heights_mode: Optional[int] = None,
    queens_mode: Optional[int] = None,
    brooklyn_mode: Optional[int] = None,
):
    if not _has_day_tendency_model():
        print("[warn] day tendency model missing; call /generate or allow startup backfill")
        raise HTTPException(status_code=409, detail="day tendency not ready. Call /generate first.")

    model = _read_day_tendency_model()
    generated_at = model.get("generated_at") or datetime.now(timezone.utc).isoformat()
    frame_dt = _parse_frame_time_to_nyc(frame_time)
    frame_time_iso = _frame_time_iso_local(frame_dt)
    frame_date = frame_dt.date()
    weekday = frame_date.weekday()
    weekday_name = _weekday_name_from_mon0(weekday)
    month = int(frame_date.month)
    bin_minutes = int(model.get("bin_minutes") or 20)
    bin_index = _current_bin_index_from_dt(frame_dt, bin_minutes=bin_minutes)
    local_time_label = _bin_label(bin_index, bin_minutes=bin_minutes)
    mode_flags = {
        "manhattan_mode": manhattan_mode,
        "staten_island_mode": staten_island_mode,
        "bronx_wash_heights_mode": bronx_wash_heights_mode,
        "queens_mode": queens_mode,
        "brooklyn_mode": brooklyn_mode,
    }
    resolved_scope = resolve_tendency_scope(lat=lat, lng=lng, mode_flags=mode_flags)
    if model.get("status") == "insufficient_data":
        global_context = _build_day_tendency_context_unavailable(
            target_date=frame_date,
            frame_dt=frame_dt,
            frame_time_iso=frame_time_iso,
            weekday=weekday,
            weekday_name=weekday_name,
            month=month,
            bin_index=bin_index,
            bin_minutes=bin_minutes,
            local_time_label=local_time_label,
            generated_at=generated_at,
            status="insufficient_data",
            label="No data",
            explain="Global day tendency model has insufficient_data status.",
            scope="citywide",
            scope_label=_scope_label("citywide"),
            source_borough=None,
            source_mode="citywide",
            context_family="global",
        )
        local_context = _build_day_tendency_context_unavailable(
            target_date=frame_date,
            frame_dt=frame_dt,
            frame_time_iso=frame_time_iso,
            weekday=weekday,
            weekday_name=weekday_name,
            month=month,
            bin_index=bin_index,
            bin_minutes=bin_minutes,
            local_time_label=local_time_label,
            generated_at=generated_at,
            status="insufficient_data",
            label="No data",
            explain="Local day tendency model has insufficient_data status.",
            scope=resolved_scope.get("scope") if resolved_scope.get("ready") else None,
            scope_label=resolved_scope.get("scope_label") if resolved_scope.get("ready") else "Waiting for location",
            source_borough=resolved_scope.get("borough") if resolved_scope.get("ready") else None,
            source_mode=resolved_scope.get("source_mode") if resolved_scope.get("ready") else None,
            context_family="local",
            borough=resolved_scope.get("borough") if resolved_scope.get("ready") else None,
            borough_key=resolved_scope.get("borough_key") if resolved_scope.get("ready") else None,
        )
    else:
        global_context = _resolve_global_day_tendency_context(
            model=model,
            target_date=frame_date,
            frame_dt=frame_dt,
            frame_time_iso=frame_time_iso,
            generated_at=generated_at,
            weekday=weekday,
            weekday_name=weekday_name,
            month=month,
            bin_index=bin_index,
            bin_minutes=bin_minutes,
            local_time_label=local_time_label,
        )
        local_context = _resolve_local_day_tendency_context(
            model=model,
            target_date=frame_date,
            frame_dt=frame_dt,
            frame_time_iso=frame_time_iso,
            generated_at=generated_at,
            resolved_scope=resolved_scope,
            weekday=weekday,
            weekday_name=weekday_name,
            month=month,
            bin_index=bin_index,
            bin_minutes=bin_minutes,
            local_time_label=local_time_label,
        )

    advanced_context = _build_day_tendency_advanced_context(
        frame_time_iso=frame_time_iso,
        frame_date=frame_date.isoformat(),
        frame_weekday=weekday,
        frame_weekday_name=weekday_name,
        frame_bin_index=bin_index,
        frame_bin_minutes=bin_minutes,
        frame_local_time_label=local_time_label,
        global_context=global_context,
        local_context=local_context,
        resolved_scope=resolved_scope,
    )
    return {
        "ok": True,
        "resolved_scope": resolved_scope,
        "global_context": global_context,
        "local_context": local_context,
        "advanced_context": advanced_context,
    }


@app.get("/day_tendency/month_benchmark")
def day_tendency_month_benchmark(
    month_key: Optional[str] = None,
    frame_time: Optional[str] = None,
):
    resolved_month_key, active_month_key = _resolve_month_key_for_tendency_benchmark(
        month_key=month_key,
        frame_time=frame_time,
    )
    current_active_month_key, _ = _resolve_active_month_key()
    payload, source = _load_month_tendency_benchmark_payload(
        resolved_month_key,
        active_month_key=current_active_month_key,
    )
    payload = dict(payload)
    if active_month_key:
        payload["active_month_key"] = active_month_key
    payload["source"] = source
    payload["requested_month_key"] = resolved_month_key
    return payload


@app.get("/timeline")
def timeline(request: Request, month_key: Optional[str] = None):
    target_month_key = _resolve_target_month_key_for_request(month_key=month_key)
    preparing_response = _ensure_requested_month_available_or_start_generate(
        month_key=target_month_key,
        request_kind="timeline",
    )
    if preparing_response is not None:
        return preparing_response
    cached = _read_timeline_cached(month_key=target_month_key)
    timeline_payload = dict(cached["data"])
    timeline_payload["active_month_key"] = target_month_key
    timeline_payload["available_month_keys"] = list(_load_month_manifest().get("available_month_keys") or [])
    timeline_payload["timeline_scope"] = "monthly_exact_historical"
    _queue_active_month_attestation(target_month_key)
    return _json_cached_response(request, timeline_payload, etag=cached.get("etag"))


def _parse_assistant_location_ids(location_ids: Optional[str]) -> List[str]:
    candidates: List[str] = []
    if location_ids:
        candidates.extend(part.strip() for part in str(location_ids).split(","))

    normalized: List[str] = []
    seen: set[str] = set()
    for raw in candidates:
        if not raw:
            continue
        try:
            cleaned = str(int(raw))
        except Exception:
            cleaned = raw
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            normalized.append(cleaned)
    return normalized


@app.get("/assistant/outlook")
def assistant_outlook(
    request: Request,
    frame_time: str,
    location_ids: str,
):
    requested_location_ids = _parse_assistant_location_ids(location_ids)
    if not requested_location_ids:
        raise HTTPException(status_code=400, detail="location_ids is required and must include at least one id.")

    try:
        timeline_cached = _read_timeline_cached()
    except Exception:
        raise HTTPException(status_code=503, detail="assistant outlook unavailable: timeline not ready")

    frame_key = _to_frontend_local_iso(frame_time)

    try:
        cached_bucket = _build_assistant_outlook_frame_bucket_cached(
            timeline_cached=timeline_cached,
            frame_time=frame_key,
            horizon_bins=HORIZON_BINS_DEFAULT,
        )
        payload = get_assistant_outlook_payload_from_frame_bucket(
            frame_bucket=cached_bucket.get("frame_bucket") or {},
            frame_time=frame_key,
            location_ids=requested_location_ids,
            bin_minutes=int(cached_bucket.get("bin_minutes") or DEFAULT_BIN_MINUTES),
            horizon_bins=int(cached_bucket.get("horizon_bins") or HORIZON_BINS_DEFAULT),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown frame_time: {frame_key}")
    except (FileNotFoundError, json.JSONDecodeError, OSError, UnicodeDecodeError, ValueError) as exc:
        print(f"[warn] assistant outlook frame bucket unavailable for frame_time={frame_key}: {exc}")
        raise HTTPException(
            status_code=503,
            detail={"error": "assistant_outlook_unavailable", "message": "assistant outlook temporarily unavailable"},
        )
    except HTTPException:
        raise
    except Exception:
        print("[warn] assistant outlook frame bucket build failed")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=503,
            detail={"error": "assistant_outlook_unavailable", "message": "assistant outlook temporarily unavailable"},
        )

    response_etag = str((timeline_cached or {}).get("etag") or "")
    return _json_cached_response(request, payload, etag=response_etag)


def _safe_float_value(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_haversine_miles(lat1: Any, lng1: Any, lat2: Any, lng2: Any) -> float:
    try:
        la1 = float(lat1)
        ln1 = float(lng1)
        la2 = float(lat2)
        ln2 = float(lng2)
    except Exception:
        return 0.0
    radius_m = 6371000.0
    phi1 = math.radians(la1)
    phi2 = math.radians(la2)
    dphi = math.radians(la2 - la1)
    dlambda = math.radians(ln2 - ln1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_m * c * 0.000621371


def _assistant_track_priority_from_mode_flags(mode_flags: Dict[str, bool]) -> List[str]:
    if mode_flags.get("staten_island_mode"):
        return ["staten_island_v3_shadow", "staten_island_shadow", "citywide_v3_shadow", "citywide_shadow"]
    if mode_flags.get("bronx_wash_heights_mode"):
        return ["bronx_wash_heights_v3_shadow", "bronx_wash_heights_shadow", "citywide_v3_shadow", "citywide_shadow"]
    if mode_flags.get("queens_mode"):
        return ["queens_v3_shadow", "queens_shadow", "citywide_v3_shadow", "citywide_shadow"]
    if mode_flags.get("brooklyn_mode"):
        return ["brooklyn_v3_shadow", "brooklyn_shadow", "citywide_v3_shadow", "citywide_shadow"]
    return ["manhattan_v3_shadow", "manhattan_shadow", "citywide_v3_shadow", "citywide_shadow"]


def _extract_zone_rating_from_point(point: Dict[str, Any], mode_flags: Dict[str, bool]) -> float:
    tracks = (point or {}).get("tracks") or {}
    for key in _assistant_track_priority_from_mode_flags(mode_flags):
        entry = tracks.get(key) or {}
        rating = entry.get("rating")
        if rating is not None:
            return _safe_float_value(rating, 0.0)
    return 0.0


def _extract_zone_track_entry_from_point(point: Dict[str, Any], mode_flags: Dict[str, bool]) -> Dict[str, Any]:
    tracks = (point or {}).get("tracks") or {}
    for key in _assistant_track_priority_from_mode_flags(mode_flags):
        entry = tracks.get(key) or {}
        rating = entry.get("rating")
        if rating is not None:
            bucket = entry.get("bucket")
            bucket_value = str(bucket).strip().lower() if bucket is not None else None
            return {
                "rating": _safe_float_value(rating, 0.0),
                "bucket": bucket_value,
                "color": bucket_value,
            }
    return {"rating": 0.0, "bucket": None, "color": None}


def _build_guidance_zone_context(
    *,
    frame_bucket: Dict[str, Any],
    current_zone_id: Optional[int],
    current_lat: float,
    current_lng: float,
    mode_flags: Dict[str, bool],
    centroid_lookup: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    current_zone_payload = None
    if current_zone_id is not None:
        current_zone_payload = frame_bucket.get(str(int(current_zone_id)))
    current_points = (current_zone_payload or {}).get("points") or []
    current_now = current_points[0] if current_points else {}
    current_next = current_points[1] if len(current_points) > 1 else current_now
    current_track_now = _extract_zone_track_entry_from_point(current_now, mode_flags)
    current_track_next = _extract_zone_track_entry_from_point(current_next, mode_flags)
    current_rating = _safe_float_value(current_track_now.get("rating"), 0.0)
    current_next_rating = _safe_float_value(current_track_next.get("rating"), current_rating)

    nearby_candidates: List[Dict[str, Any]] = []
    for zone_id_raw, zone_payload in (frame_bucket or {}).items():
        points = (zone_payload or {}).get("points") or []
        if not points:
            continue
        first = points[0]
        zone_id = int(zone_payload.get("location_id") or zone_id_raw)
        centroid_row = centroid_lookup.get(zone_id) or {}
        center_lat = centroid_row.get("centroid_lat")
        center_lng = centroid_row.get("centroid_lng")
        if center_lat is None or center_lng is None:
            continue
        if current_zone_id is not None and str(zone_id_raw) == str(current_zone_id):
            continue
        distance = _safe_haversine_miles(current_lat, current_lng, center_lat, center_lng)
        if distance > 3.0:
            continue
        nearby_candidates.append(
            {
                "zone_id": int(zone_payload.get("location_id") or zone_id_raw),
                "zone_name": zone_payload.get("zone_name"),
                "borough": zone_payload.get("borough"),
                "distance_miles": round(float(distance), 3),
                "rating": round(_extract_zone_rating_from_point(first, mode_flags), 2),
            }
        )
    nearby_candidates.sort(key=lambda z: (-(z.get("rating") or 0.0), z.get("distance_miles") or 999.0))

    return {
        "current_zone": {
            "zone_id": int(current_zone_id) if current_zone_id is not None else None,
            "rating": round(float(current_rating), 2),
            "bucket": current_track_now.get("bucket"),
            "color": current_track_now.get("color"),
            "next_rating": round(float(current_next_rating), 2),
            "market_saturation_penalty": _safe_float_value(current_now.get("market_saturation_penalty"), 0.0),
            "continuation_raw": _safe_float_value(current_now.get("continuation_raw"), 0.0),
        },
        "nearby_candidates": nearby_candidates[:8],
    }


def _persist_driver_guidance_state_and_outcome(
    *,
    user_id: int,
    frame_time: str,
    now_ts: int,
    guidance: Dict[str, Any],
) -> None:
    action = str(guidance.get("action") or "hold").strip().lower()
    source_zone_id = (guidance.get("current_zone") or {}).get("zone_id")
    target_zone_id = (guidance.get("target_zone") or {}).get("zone_id")
    current_rating = (guidance.get("current_zone") or {}).get("rating")
    target_rating = (guidance.get("target_zone") or {}).get("rating") if guidance.get("target_zone") else None
    prev = _db_query_one("SELECT * FROM driver_guidance_state WHERE user_id=? LIMIT 1", (int(user_id),))
    prev_move_attempts = int((prev or {}).get("recent_move_attempts_without_trip") or 0)
    prev_wait_count = int((prev or {}).get("recent_wait_dispatch_count") or 0)

    next_move_attempts = prev_move_attempts
    if action in {"move_nearby", "micro_reposition"}:
        next_move_attempts = prev_move_attempts + 1
    elif action in {"hold", "wait_dispatch"}:
        next_move_attempts = max(0, prev_move_attempts - 1)

    next_wait_count = prev_wait_count + 1 if action == "wait_dispatch" else 0
    _db_exec(
        """
        INSERT INTO assistant_guidance_outcomes(
          user_id, frame_time, recommended_at, action,
          source_zone_id, target_zone_id,
          tripless_minutes, stationary_minutes, movement_minutes,
          current_rating, target_rating, dispatch_uncertainty,
          converted_to_trip, moved_before_trip, minutes_to_trip, settled_at, settlement_reason
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(user_id),
            str(frame_time or ""),
            int(now_ts),
            action,
            source_zone_id,
            target_zone_id,
            _safe_float_value(guidance.get("tripless_minutes"), 0.0),
            _safe_float_value(guidance.get("stationary_minutes"), 0.0),
            _safe_float_value(guidance.get("movement_minutes"), 0.0),
            _safe_float_value(current_rating, 0.0),
            _safe_float_value(target_rating, 0.0) if target_rating is not None else None,
            _safe_float_value(guidance.get("dispatch_uncertainty"), 0.0),
            None,
            None,
            None,
            None,
            None,
        ),
    )
    _db_exec(
        """
        INSERT INTO driver_guidance_state(
          user_id, last_guidance_action, last_guidance_generated_at, last_move_guidance_at,
          last_hold_guidance_at, last_target_zone_id, recent_move_attempts_without_trip,
          recent_wait_dispatch_count, updated_at
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
          last_guidance_action=excluded.last_guidance_action,
          last_guidance_generated_at=excluded.last_guidance_generated_at,
          last_move_guidance_at=CASE
            WHEN excluded.last_guidance_action IN ('move_nearby', 'micro_reposition') THEN excluded.last_guidance_generated_at
            ELSE driver_guidance_state.last_move_guidance_at
          END,
          last_hold_guidance_at=CASE
            WHEN excluded.last_guidance_action IN ('hold', 'wait_dispatch') THEN excluded.last_guidance_generated_at
            ELSE driver_guidance_state.last_hold_guidance_at
          END,
          last_target_zone_id=excluded.last_target_zone_id,
          recent_move_attempts_without_trip=excluded.recent_move_attempts_without_trip,
          recent_wait_dispatch_count=excluded.recent_wait_dispatch_count,
          updated_at=excluded.updated_at
        """,
        (
            int(user_id),
            action,
            int(now_ts),
            int(now_ts) if action in {"move_nearby", "micro_reposition"} else None,
            int(now_ts) if action in {"hold", "wait_dispatch"} else None,
            target_zone_id,
            int(next_move_attempts),
            int(next_wait_count),
            int(now_ts),
        ),
    )


@app.get("/assistant/guidance")
def assistant_guidance(
    frame_time: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    staten_island_mode: int = 0,
    bronx_wash_heights_mode: int = 0,
    queens_mode: int = 0,
    brooklyn_mode: int = 0,
    user: sqlite3.Row = Depends(require_user),
):
    now_ts = int(time.time())
    mode_flags = {
        "staten_island_mode": bool(int(staten_island_mode or 0)),
        "bronx_wash_heights_mode": bool(int(bronx_wash_heights_mode or 0)),
        "queens_mode": bool(int(queens_mode or 0)),
        "brooklyn_mode": bool(int(brooklyn_mode or 0)),
    }
    presence_row = _db_query_one("SELECT lat, lng FROM presence WHERE user_id=? LIMIT 1", (int(user["id"]),))
    current_lat = float(lat if lat is not None else (presence_row or {}).get("lat")) if (lat is not None or presence_row) else None
    current_lng = float(lng if lng is not None else (presence_row or {}).get("lng")) if (lng is not None or presence_row) else None
    if current_lat is None or current_lng is None:
        raise HTTPException(status_code=400, detail="guidance requires lat/lng or a fresh presence location")

    frame_key = _to_frontend_local_iso(frame_time)
    timeline_cached = _read_timeline_cached()

    active_month_for_precheck = str((timeline_cached or {}).get("data", {}).get("active_month_key") or "")
    timeline_list_for_precheck = list((timeline_cached or {}).get("data", {}).get("timeline") or [])
    if active_month_for_precheck and timeline_list_for_precheck:
        try:
            precheck_idx = timeline_list_for_precheck.index(frame_key)
        except ValueError:
            precheck_idx = None
        if precheck_idx is not None:
            precheck_path = _month_frame_cache_file(active_month_for_precheck, precheck_idx, frame_key)
            if not (precheck_path.exists() and precheck_path.stat().st_size > 0):
                _ensure_frame_build_in_progress(active_month_for_precheck, precheck_idx, frame_key)
                print(
                    f"[warn] assistant outlook deferred: frame cache miss "
                    f"month_key={active_month_for_precheck} idx={precheck_idx} frame_time={frame_key}"
                )
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "assistant_outlook_unavailable",
                        "message": "assistant outlook temporarily unavailable",
                    },
                )

    cached_bucket = _build_assistant_outlook_frame_bucket_cached(
        timeline_cached=timeline_cached,
        frame_time=frame_key,
        horizon_bins=HORIZON_BINS_DEFAULT,
    )
    frame_bucket = cached_bucket.get("frame_bucket") or {}
    zone_resolution = resolve_current_zone_from_position(
        zones_geojson_path=(DATA_DIR / "taxi_zones.geojson"),
        lat=float(current_lat),
        lng=float(current_lng),
    )
    centroid_lookup = load_zone_centroid_lookup(DATA_DIR / "taxi_zones.geojson")
    current_zone_id = zone_resolution.get("current_zone_id")
    current_zone_name = zone_resolution.get("current_zone_name")
    current_borough = zone_resolution.get("current_borough")

    activity_snapshot = load_driver_activity_snapshot(
        user_id=int(user["id"]),
        now_ts=now_ts,
        current_lat=float(current_lat),
        current_lng=float(current_lng),
        db_query_one=_db_query_one,
        db_query_all=_db_query_all,
    )
    zone_context = _build_guidance_zone_context(
        frame_bucket=frame_bucket,
        current_zone_id=current_zone_id,
        current_lat=float(current_lat),
        current_lng=float(current_lng),
        mode_flags=mode_flags,
        centroid_lookup=centroid_lookup,
    )
    guidance = build_driver_guidance(
        user_id=int(user["id"]),
        frame_time=frame_key,
        current_lat=float(current_lat),
        current_lng=float(current_lng),
        current_zone_id=current_zone_id,
        current_zone_name=current_zone_name,
        current_borough=current_borough,
        mode_flags=mode_flags,
        assistant_outlook_bucket=frame_bucket,
        activity_snapshot=activity_snapshot,
        zone_context=zone_context,
        now_ts=now_ts,
    )
    _persist_driver_guidance_state_and_outcome(
        user_id=int(user["id"]),
        frame_time=frame_key,
        now_ts=now_ts,
        guidance=guidance,
    )
    current_zone_debug = guidance.get("current_zone") or {}
    return {
        "ok": True,
        "frame_time": frame_key,
        "action": guidance.get("action"),
        "confidence": guidance.get("confidence"),
        "message": guidance.get("message"),
        "reason_codes": guidance.get("reason_codes") or [],
        "tripless_minutes": guidance.get("tripless_minutes"),
        "stationary_minutes": guidance.get("stationary_minutes"),
        "movement_minutes": guidance.get("movement_minutes"),
        "recent_saved_trip_count": guidance.get("recent_saved_trip_count"),
        "recent_move_attempts_without_trip": guidance.get("recent_move_attempts_without_trip"),
        "dispatch_uncertainty": guidance.get("dispatch_uncertainty"),
        "move_cooldown_until_unix": guidance.get("move_cooldown_until_unix"),
        "hold_until_unix": guidance.get("hold_until_unix"),
        "current_zone_rating": current_zone_debug.get("rating"),
        "current_zone_next_rating": current_zone_debug.get("next_rating"),
        "current_zone_saturation_penalty": current_zone_debug.get("market_saturation_penalty"),
        "current_zone_continuation_raw": current_zone_debug.get("continuation_raw"),
        "current_zone": guidance.get("current_zone"),
        "target_zone": guidance.get("target_zone"),
    }


@app.get("/frame/{idx}")
def frame(idx: int, request: Request, month_key: Optional[str] = None):
    target_month_key = _resolve_target_month_key_for_request(month_key=month_key)
    preparing_response = _ensure_requested_month_available_or_start_generate(
        month_key=target_month_key,
        request_kind="frame",
    )
    if preparing_response is not None:
        return preparing_response
    timeline_cached = _read_timeline_cached(month_key=target_month_key)
    timeline_payload = (timeline_cached or {}).get("data") or {}
    timeline = timeline_payload.get("timeline") or []
    if idx < 0 or idx >= len(timeline):
        raise HTTPException(status_code=404, detail=f"frame not found: {idx}")
    frame_time = _to_frontend_local_iso(timeline[idx])
    _queue_active_month_attestation(target_month_key, active_frame_time=frame_time)
    cache_file = _month_frame_cache_file(target_month_key, idx, frame_time)
    if not (cache_file.exists() and cache_file.stat().st_size > 0):
        started = _ensure_frame_build_in_progress(target_month_key, idx, frame_time)
        return JSONResponse(
            status_code=202,
            content={
                "ok": False,
                "status": "preparing_frame",
                "target_month_key": target_month_key,
                "frame_time": frame_time,
                "retry_after_sec": 2,
                "generate_started": bool(started),
            },
            headers={"Retry-After": "2", "Cache-Control": "no-store"},
        )
    cached = _read_frame_cached(idx, month_key=target_month_key)
    return _json_cached_response(request, cached["data"], etag=cached.get("etag"))


@app.get("/frame/{idx}/viewport")
def frame_viewport(
    idx: int,
    request: Request,
    min_lat: float,
    min_lng: float,
    max_lat: float,
    max_lng: float,
    month_key: Optional[str] = None,
    padding_ratio: float = 0.18,
):
    target_month_key = _resolve_target_month_key_for_request(month_key=month_key)
    preparing_response = _ensure_requested_month_available_or_start_generate(
        month_key=target_month_key,
        request_kind="frame_viewport",
    )
    if preparing_response is not None:
        return preparing_response
    timeline_cached = _read_timeline_cached(month_key=target_month_key)
    timeline_payload = (timeline_cached or {}).get("data") or {}
    timeline = timeline_payload.get("timeline") or []
    if idx < 0 or idx >= len(timeline):
        raise HTTPException(status_code=404, detail=f"frame not found: {idx}")
    frame_time = _to_frontend_local_iso(timeline[idx])
    _queue_active_month_attestation(target_month_key, active_frame_time=frame_time)
    cache_file = _month_frame_cache_file(target_month_key, idx, frame_time)
    if not (cache_file.exists() and cache_file.stat().st_size > 0):
        started = _ensure_frame_build_in_progress(target_month_key, idx, frame_time)
        return JSONResponse(
            status_code=202,
            content={
                "ok": False,
                "status": "preparing_frame",
                "target_month_key": target_month_key,
                "frame_time": frame_time,
                "retry_after_sec": 2,
                "generate_started": bool(started),
            },
            headers={"Retry-After": "2", "Cache-Control": "no-store"},
        )
    cached = _read_frame_cached(idx, month_key=target_month_key)
    payload = _frame_payload_viewport_subset(
        cached["data"],
        min_lat=min_lat,
        min_lng=min_lng,
        max_lat=max_lat,
        max_lng=max_lng,
        padding_ratio=padding_ratio,
    )
    etag = _viewport_frame_etag(
        cached.get("etag"),
        min_lat=min_lat,
        min_lng=min_lng,
        max_lat=max_lat,
        max_lng=max_lng,
        padding_ratio=padding_ratio,
    )
    return _json_cached_response(request, payload, etag=etag)


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


def _safe_upload_filename(raw_name: str, default_name: str) -> str:
    cleaned = (raw_name or default_name).replace("\\", "/").split("/")[-1].strip()
    if not cleaned:
        cleaned = default_name
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", cleaned)
    return cleaned


def _safe_admin_filename(raw_name: str) -> str:
    cleaned = str(raw_name or "").strip().replace("\\", "/").split("/")[-1].strip()
    if not cleaned or cleaned in {".", ".."} or "/" in cleaned:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return cleaned


def _cleanup_path_quiet(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


async def _stream_upload_to_path(upload: UploadFile, target: Path, chunk_size: int = 8 * 1024 * 1024) -> int:
    tmp_target = target.with_suffix(target.suffix + ".uploading")
    _cleanup_path_quiet(tmp_target)
    total_written = 0
    try:
        with tmp_target.open("wb") as out:
            while True:
                chunk = await upload.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)
                total_written += len(chunk)
            out.flush()
            os.fsync(out.fileno())
        if total_written <= 0:
            _cleanup_path_quiet(tmp_target)
            raise HTTPException(status_code=400, detail="Empty upload.")
        tmp_target.replace(target)
        return total_written
    except HTTPException:
        _cleanup_path_quiet(tmp_target)
        raise
    except OSError as exc:
        _cleanup_path_quiet(tmp_target)
        if getattr(exc, "errno", None) == errno.ENOSPC:
            raise HTTPException(status_code=507, detail="Upload failed: no space left on target volume")
        detail = f"Upload write failed: {exc}"
        raise HTTPException(status_code=500, detail=detail)
    except Exception as exc:
        _cleanup_path_quiet(tmp_target)
        detail = f"Upload failed: {exc}"
        raise HTTPException(status_code=500, detail=detail)


@app.post("/upload_parquet")
async def upload_parquet(file: UploadFile = File(...)):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    filename = _safe_upload_filename(file.filename or "upload.parquet", "upload.parquet")
    if not filename.lower().endswith(".parquet"):
        raise HTTPException(status_code=400, detail="File must be .parquet")

    target = DATA_DIR / filename
    bytes_written = await _stream_upload_to_path(file, target)

    return {
        "saved": str(target),
        "filename": filename,
        "size_bytes": int(bytes_written),
        "size_mb": round(bytes_written / (1024 * 1024), 2),
    }


class AdminParquetDeletePayload(BaseModel):
    filename: str


@app.get("/admin/parquet/list")
def admin_list_parquets(admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    return {
        "ok": True,
        "parquets": [p.name for p in _list_parquets()],
    }


@app.post("/admin/parquet/delete")
def admin_delete_parquet(payload: AdminParquetDeletePayload, admin: sqlite3.Row = Depends(require_admin)):
    _ = admin
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    filename = _safe_admin_filename(payload.filename)
    if not filename.lower().endswith(".parquet"):
        raise HTTPException(status_code=400, detail="Only .parquet files can be deleted")

    target = DATA_DIR / filename
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="Parquet file not found")

    target.unlink()
    return {
        "ok": True,
        "deleted": True,
        "path": str(target),
        "filename": filename,
    }


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
    _enforce_user_not_blocked(row)

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
        "avatar_thumb_url": _avatar_thumb_url_for_row(user),
        "avatar_version": _avatar_version_for_row(user),
        "map_identity_mode": map_identity_mode,
        "ghost_mode": ghost,
        "is_admin": bool(_flag_to_int(user["is_admin"])),
        "trial_expires_at": int(user["trial_expires_at"]),
        "leaderboard_badge_code": best_badge.get("leaderboard_badge_code"),
    }


@app.get("/drivers/{user_id}/profile")
def driver_profile(user_id: int, viewer: sqlite3.Row = Depends(require_user)):
    target = _db_query_one(
        "SELECT id, email, display_name, avatar_url, avatar_version, is_disabled, is_suspended FROM users WHERE id=? LIMIT 1",
        (int(user_id),),
    )
    if not target or _user_block_state(target)["is_blocked"]:
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
    progression = get_progression_for_user(target_user_id)
    battle_payload = get_battle_stats_for_user(target_user_id)
    relationship = get_viewer_game_relationship(target_user_id, int(viewer["id"]))
    active_summary = get_active_match_between_users(target_user_id, int(viewer["id"]))

    miles_rank = miles_rank_data.get("row", {}).get("rank_position") if miles_rank_data.get("row") else None
    hours_rank = hours_rank_data.get("row", {}).get("rank_position") if hours_rank_data.get("row") else None

    return {
        "ok": True,
        "user": {
            "id": target_user_id,
            "display_name": display_name,
            "avatar_url": target["avatar_url"] if target["avatar_url"] else None,
            "avatar_thumb_url": _avatar_thumb_url_for_row(target),
            "avatar_version": _avatar_version_for_row(target),
            "leaderboard_badge_code": best_badge.get("leaderboard_badge_code"),
        },
        "daily": {
            "miles": daily.get("miles", 0),
            "hours": daily.get("hours", 0),
            "pickups": daily.get("pickups", 0),
            "miles_rank": miles_rank,
            "hours_rank": hours_rank,
        },
        "weekly": {
            "miles": weekly.get("miles", 0),
            "hours": weekly.get("hours", 0),
            "pickups": weekly.get("pickups", 0),
        },
        "monthly": {
            "miles": monthly.get("miles", 0),
            "hours": monthly.get("hours", 0),
            "pickups": monthly.get("pickups", 0),
        },
        "yearly": {
            "miles": yearly.get("miles", 0),
            "hours": yearly.get("hours", 0),
            "pickups": yearly.get("pickups", 0),
        },
        "progression": progression,
        "battle_stats": battle_payload["battle_stats"],
        "battle_record": battle_payload["battle_record"],
        "recent_battles": battle_payload["recent_battles"],
        "battle_history": battle_payload["battle_history"],
        "viewer_game_relationship": relationship,
        "active_match_summary": active_summary,
    }


@app.get("/avatars/thumb/{user_id}")
def avatar_thumb_asset(user_id: int, request: Request):
    row = _db_query_one(
        "SELECT id, avatar_url, avatar_version FROM users WHERE id=? LIMIT 1",
        (int(user_id),),
    )
    if not row or not row["avatar_url"]:
        raise HTTPException(status_code=404, detail="Avatar not found")

    avatar_data_url = str(row["avatar_url"])
    version = _avatar_version_for_row(row)
    if not version:
        _record_perf_metric("avatar_thumb.cache_miss")
        version = _ensure_avatar_thumb_materialized(int(user_id), avatar_data_url, None)
        if version:
            _db_exec("UPDATE users SET avatar_version=? WHERE id=?", (version, int(user_id)))
    else:
        target = avatar_thumb_path(DATA_DIR, int(user_id), version)
        if not target.exists():
            _record_perf_metric("avatar_thumb.cache_miss")
            _ensure_avatar_thumb_materialized(int(user_id), avatar_data_url, version)
        else:
            _record_perf_metric("avatar_thumb.cache_hit")

    if not version:
        raise HTTPException(status_code=404, detail="Avatar not found")
    target = avatar_thumb_path(DATA_DIR, int(user_id), version)
    if not target.exists():
        raise HTTPException(status_code=404, detail="Avatar not found")

    headers = _avatar_thumb_headers(int(user_id), version)
    if _request_etag_matches(request, headers["ETag"]):
        return Response(status_code=304, headers=headers)
    return Response(content=target.read_bytes(), media_type=AVATAR_THUMB_MIME, headers=headers)


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
    change_cursor_ms: Optional[int] = None
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
    avatar_version: Optional[str] = None
    if update_avatar:
        updates.append("avatar_url=?")
        args.append(new_avatar)
        avatar_version = avatar_version_for_data_url(new_avatar)
        updates.append("avatar_version=?")
        args.append(avatar_version)
    if update_map_identity_mode:
        updates.append("map_identity_mode=?")
        args.append(new_map_identity_mode)

    if updates:
        args.append(int(user["id"]))
        _db_exec(f"UPDATE users SET {', '.join(updates)} WHERE id=?", tuple(args))

    if update_avatar and new_avatar:
        _ensure_avatar_thumb_materialized(int(user["id"]), new_avatar, avatar_version)
    elif update_avatar and not new_avatar:
        avatar_dir = DATA_DIR / "avatar_thumbs" / str(int(user["id"]))
        if avatar_dir.exists():
            for thumb in avatar_dir.glob("*.png"):
                thumb.unlink(missing_ok=True)

    row = _db_query_one("SELECT id, email, display_name, ghost_mode, avatar_url, avatar_version, map_identity_mode, is_admin, trial_expires_at FROM users WHERE id=? LIMIT 1", (int(user["id"]),))
    if not row:
        return {"ok": True, "updated": True}

    if payload.ghost_mode is not None:
        is_visible, reason = _presence_state_from_user_row(row)
        change_cursor_ms = _presence_runtime_state_upsert(
            int(user["id"]),
            is_visible=is_visible,
            reason=reason,
            changed_at_ms=_presence_change_cursor_ms(),
        )
        with _presence_viewport_cache_lock:
            _presence_viewport_cache.clear()

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
        "avatar_thumb_url": _avatar_thumb_url_for_row(row),
        "avatar_version": _avatar_version_for_row(row),
        "map_identity_mode": map_identity_mode,
        "ghost_mode": bool(_flag_to_int(row["ghost_mode"])) if row["ghost_mode"] is not None else False,
        "presence_cursor": change_cursor_ms,
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
    _presence_remove_runtime_visibility(int(user["id"]), reason="account_deleted")
    cleanup = delete_account_runtime_data(int(user["id"]))
    return {"ok": True, "cleanup": cleanup}


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
    changed_at_ms = _presence_change_cursor_ms()

    if (
        payload.accuracy is not None
        and float(payload.accuracy) > PRESENCE_COMMUNITY_ACCURACY_MAX_METERS
    ):
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
    is_visible, reason = _presence_state_from_user_row(user)
    _presence_runtime_state_upsert(int(user["id"]), is_visible=is_visible, reason=reason, changed_at_ms=changed_at_ms)
    record_presence_heartbeat(int(user["id"]), float(payload.lat), float(payload.lng), payload.heading)
    record_pickup_presence_heartbeat(int(user["id"]), float(payload.lat), float(payload.lng), now)
    with _presence_viewport_cache_lock:
        _presence_viewport_cache.clear()
    return {"ok": True, "cursor": changed_at_ms}


@app.get("/presence/all")
def presence_all(
    max_age_sec: int = PRESENCE_STALE_SECONDS,
    min_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lat: Optional[float] = None,
    max_lng: Optional[float] = None,
    zoom: Optional[float] = None,
    mode: str = "full",
    limit: Optional[int] = None,
    viewer: sqlite3.Row = Depends(require_user),  # REQUIRE AUTH (frontend already sends token)
):
    del viewer
    cutoff = _presence_cutoff_unix(max_age_sec)
    safe_mode = (mode or "full").strip().lower()
    if safe_mode not in {"full", "lite"}:
        raise HTTPException(status_code=400, detail="mode must be 'full' or 'lite'")
    safe_limit = None if limit is None else max(1, min(PRESENCE_SNAPSHOT_MAX_LIMIT, int(limit)))
    buffered_bbox = _presence_bbox_with_buffer(min_lat, min_lng, max_lat, max_lng, zoom)
    cache_key = _presence_cache_key(
        max_age_sec=max_age_sec,
        mode=safe_mode,
        limit=safe_limit,
        bbox=buffered_bbox,
        zoom=zoom,
    )
    now_monotonic = time.monotonic()
    with _presence_viewport_cache_lock:
        _purge_presence_viewport_cache(now_monotonic)
        cached = _presence_viewport_cache.get(cache_key)
        if cached and float(cached.get("expires_at_monotonic") or 0.0) > now_monotonic:
            cached["last_access_monotonic"] = now_monotonic
            _record_perf_metric("presence.cache_hit")
            return copy.deepcopy(cached["payload"])
    _record_perf_metric("presence.cache_miss")

    rows = _presence_rows_for_viewport(cutoff=cutoff, bbox=buffered_bbox, limit=safe_limit)
    online_count = len(rows)
    ghosted_count = 0
    items = _presence_row_payloads(rows, include_full_fields=(safe_mode == "full"))
    visible_count_total = _presence_visible_count_for_viewport(cutoff=cutoff, bbox=buffered_bbox)
    snapshot = _presence_online_summary_snapshot(max_age_sec)
    if snapshot.get("ok"):
        online_count = int(snapshot.get("online_count") or online_count)
        ghosted_count = int(snapshot.get("ghosted_count") or 0)
    visible_count = len(items)
    response = {
        "ok": True,
        "count": visible_count,
        "items": items,
        "online_count": online_count,
        "ghosted_count": ghosted_count,
        "visible_count": visible_count,
        "limit_applied": safe_limit,
        "has_more": visible_count_total > visible_count,
        "visible_count_total": visible_count_total,
    }
    with _presence_viewport_cache_lock:
        _presence_viewport_cache[cache_key] = {
            "payload": copy.deepcopy(response),
            "expires_at_monotonic": now_monotonic + PRESENCE_VIEWPORT_CACHE_TTL_SECONDS,
            "last_access_monotonic": now_monotonic,
        }
        _purge_presence_viewport_cache(now_monotonic)
    return response


@app.get("/presence/viewport")
def presence_viewport(
    max_age_sec: int = PRESENCE_STALE_SECONDS,
    min_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lat: Optional[float] = None,
    max_lng: Optional[float] = None,
    zoom: Optional[float] = None,
    padding_ratio: float = 0.0,
    updated_since_ms: Optional[int] = None,
    include_removed: bool = True,
    limit: int = PRESENCE_DELTA_MAX_LIMIT,
    viewer: sqlite3.Row = Depends(require_user),
):
    del viewer
    safe_delta_limit = max(1, min(PRESENCE_DELTA_MAX_LIMIT, int(limit)))
    safe_snapshot_limit = max(1, min(PRESENCE_SNAPSHOT_MAX_LIMIT, int(limit)))
    buffered_bbox = _presence_bbox_with_buffer(min_lat, min_lng, max_lat, max_lng, zoom)
    if buffered_bbox is not None and padding_ratio > 0:
        lo_lat, lo_lng, hi_lat, hi_lng = buffered_bbox
        lat_pad = max(0.0, (hi_lat - lo_lat) * min(1.0, float(padding_ratio)))
        lng_pad = max(0.0, (hi_lng - lo_lng) * min(1.0, float(padding_ratio)))
        buffered_bbox = (
            round(lo_lat - lat_pad, 5),
            round(lo_lng - lng_pad, 5),
            round(hi_lat + lat_pad, 5),
            round(hi_lng + lng_pad, 5),
        )
    if updated_since_ms is not None and int(updated_since_ms) > 0:
        return _presence_delta_payload(
            updated_since_ms=int(updated_since_ms),
            max_age_sec=max_age_sec,
            bbox=buffered_bbox,
            limit=safe_delta_limit,
            include_removed=bool(include_removed),
        )

    cutoff = _presence_cutoff_unix(max_age_sec)
    rows = _presence_rows_for_viewport(cutoff=cutoff, bbox=buffered_bbox, limit=safe_snapshot_limit)
    items = _presence_row_payloads(rows, include_full_fields=False)
    visible_count_total = _presence_visible_count_for_viewport(cutoff=cutoff, bbox=buffered_bbox)
    cursor_row = _db_query_one("SELECT COALESCE(MAX(changed_at_ms), 0) AS cursor FROM presence_runtime_state")
    snapshot = _presence_online_summary_snapshot(max_age_sec)
    snapshot_cursor = int(cursor_row["cursor"] or 0) if cursor_row else 0
    return {
        "ok": True,
        "mode": "snapshot",
        "count": len(items),
        "items": items,
        "removed": [],
        "cursor": snapshot_cursor,
        "next_updated_since_ms": snapshot_cursor,
        "server_time_ms": _presence_peek_cursor_ms(),
        "online_count": int(snapshot.get("online_count") or 0),
        "ghosted_count": int(snapshot.get("ghosted_count") or 0),
        "visible_count": len(items),
        "limit_applied": safe_snapshot_limit,
        "has_more": visible_count_total > len(items),
        "visible_count_total": visible_count_total,
        "viewport": {
            "min_lat": buffered_bbox[0] if buffered_bbox else None,
            "min_lng": buffered_bbox[1] if buffered_bbox else None,
            "max_lat": buffered_bbox[2] if buffered_bbox else None,
            "max_lng": buffered_bbox[3] if buffered_bbox else None,
        },
    }


@app.get("/presence/delta")
def presence_delta(
    updated_since_ms: int = 0,
    max_age_sec: int = PRESENCE_STALE_SECONDS,
    min_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lat: Optional[float] = None,
    max_lng: Optional[float] = None,
    zoom: Optional[float] = None,
    include_removed: bool = True,
    limit: int = PRESENCE_DELTA_MAX_LIMIT,
    viewer: sqlite3.Row = Depends(require_user),
):
    del viewer
    buffered_bbox = _presence_bbox_with_buffer(min_lat, min_lng, max_lat, max_lng, zoom)
    return _presence_delta_payload(
        updated_since_ms=max(0, int(updated_since_ms)),
        max_age_sec=max_age_sec,
        bbox=buffered_bbox,
        limit=limit,
        include_removed=bool(include_removed),
    )


@app.get("/presence/summary")
def presence_summary(
    max_age_sec: int = PRESENCE_STALE_SECONDS,
    viewer: sqlite3.Row = Depends(require_user),  # REQUIRE AUTH (same as /presence/all)
):
    snapshot = _presence_visibility_snapshot(max_age_sec)
    if not snapshot.get("ok"):
        raise HTTPException(status_code=500, detail="Presence visibility snapshot failed")
    online_count = int(snapshot.get("online_count") or 0)
    ghosted_count = int(snapshot.get("ghosted_count") or 0)
    visible_count = int(snapshot.get("visible_count") or 0)

    return {
        "ok": True,
        "online_count": online_count,
        "ghosted_count": ghosted_count,
        "visible_count": max(0, visible_count),
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
    return send_legacy_global_text_message(user, _clean_chat_message(payload.message))


@app.get("/chat/recent")
def chat_recent(limit: int = 50, user: sqlite3.Row = Depends(require_user)):
    _ = user
    return {"ok": True, "items": list_legacy_global_messages(limit=limit)}


@app.get("/chat/since")
def chat_since(after_id: int = 0, limit: int = 50, user: sqlite3.Row = Depends(require_user)):
    _ = user
    return {"ok": True, "items": list_legacy_global_messages(limit=limit, after_id=after_id)}


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
    return create_pickup_record(payload, user)




def _load_pickup_zone_geometries() -> Dict[int, Dict[str, Any]]:
    global _pickup_zone_geom_cache, _pickup_zone_geom_cache_mtime
    global _pickup_zone_geom_missing_warned, _pickup_zone_geom_parse_warned
    zones_path = DATA_DIR / "taxi_zones.geojson"
    try:
        mtime = zones_path.stat().st_mtime
    except Exception:
        if not _pickup_zone_geom_missing_warned:
            print("[warn] taxi_zones.geojson not available for pickup hotspots")
            _pickup_zone_geom_missing_warned = True
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
        if not _pickup_zone_geom_parse_warned:
            print("[warn] Failed to parse taxi_zones.geojson for pickup hotspots", traceback.format_exc())
            _pickup_zone_geom_parse_warned = True
        parsed = {}

    _pickup_zone_geom_missing_warned = False
    _pickup_zone_geom_parse_warned = False
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
                pl.id,
                pl.zone_id,
                pl.zone_name,
                pl.borough,
                pl.user_id,
                pl.lat,
                pl.lng,
                pl.created_at,
                ROW_NUMBER() OVER (PARTITION BY pl.zone_id ORDER BY pl.created_at DESC, pl.id DESC) AS rn
            FROM pickup_logs pl
            WHERE pl.zone_id IN ({placeholders})
              AND {pickup_log_not_voided_sql("pl")}
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


def _normalize_pickup_zone_point_entries(point_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    point_entries: List[Dict[str, Any]] = []
    n = len(point_rows)
    for idx, row in enumerate(point_rows):
        try:
            lng = float(row["lng"])
            lat = float(row["lat"])
            x, y = _to_3857.transform(lng, lat)
        except Exception:
            continue
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
    return point_entries


def _build_density_components(
    zone_proj: Any,
    point_entries: List[Dict[str, Any]],
    threshold_ratio: float = 0.48,
) -> Dict[str, Any]:
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
            if not zone_proj.covers(Point(cx, cy)):
                continue
            score = 0.0
            for pe in point_entries:
                dx = cx - pe["x"]
                dy = cy - pe["y"]
                dist_sq = (dx * dx) + (dy * dy)
                if dist_sq > radius_sq:
                    continue
                score += pe["weight"] * math.exp(-(dist_sq) / (2.0 * sigma * sigma))
            if score <= 0.0:
                continue
            key = (gx, gy)
            cell_scores[key] = score
            if score > peak_score:
                peak_score = score
                peak_key = key

    if peak_key is None or peak_score <= 0.0:
        return {
            "components": [],
            "cell_scores": cell_scores,
            "peak_score": 0.0,
            "selected": set(),
            "start_x": start_x,
            "start_y": start_y,
            "cell_size": cell_size,
        }

    threshold = peak_score * float(threshold_ratio)
    selected = {k for k, v in cell_scores.items() if v >= threshold}
    selected.add(peak_key)

    visited: set[Tuple[int, int]] = set()
    raw_components: List[set[Tuple[int, int]]] = []
    for seed in selected:
        if seed in visited:
            continue
        q = deque([seed])
        comp: set[Tuple[int, int]] = set()
        while q:
            cur = q.popleft()
            if cur in visited or cur not in selected:
                continue
            visited.add(cur)
            comp.add(cur)
            cx, cy = cur
            for nx in (cx - 1, cx, cx + 1):
                for ny in (cy - 1, cy, cy + 1):
                    nkey = (nx, ny)
                    if nkey != cur and nkey in selected and nkey not in visited:
                        q.append(nkey)
        if comp:
            raw_components.append(comp)

    components: List[Dict[str, Any]] = []
    half = cell_size / 2.0
    for comp in raw_components:
        comp_score = sum(cell_scores.get(k, 0.0) for k in comp)
        comp_peak = max((cell_scores.get(k, 0.0) for k in comp), default=0.0)
        cell_polys: List[Polygon] = []
        for gx, gy in comp:
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
        if not cell_polys:
            continue
        comp_geom = unary_union(cell_polys)
        support_points = 0
        for pe in point_entries:
            pt = Point(pe["x"], pe["y"])
            if comp_geom.buffer(max(35.0, cell_size * 0.5)).covers(pt):
                support_points += 1
        components.append(
            {
                "cells": comp,
                "component_score": comp_score,
                "peak_score": comp_peak,
                "point_count": support_points,
                "geometry": comp_geom,
            }
        )

    components.sort(
        key=lambda c: (float(c.get("component_score") or 0.0), float(c.get("peak_score") or 0.0), int(c.get("point_count") or 0)),
        reverse=True,
    )
    return {
        "components": components,
        "cell_scores": cell_scores,
        "peak_score": peak_score,
        "threshold": threshold,
        "selected": selected,
        "start_x": start_x,
        "start_y": start_y,
        "cell_size": cell_size,
    }


def _shape_hotspot_component(component: Dict[str, Any], zone_proj: Any) -> Optional[Any]:
    base_geom = component.get("geometry")
    if base_geom is None or base_geom.is_empty:
        return None
    point_count = max(1, int(component.get("point_count") or 1))
    comp_score = max(0.1, float(component.get("component_score") or 0.1))
    comp_peak = max(0.1, float(component.get("peak_score") or 0.1))
    area = max(1.0, float(base_geom.area))

    intensity_factor = max(0.6, min(1.6, comp_peak / max(0.25, math.sqrt(point_count))))
    spread_factor = max(0.8, min(1.8, math.sqrt(area) / 120.0))
    point_factor = max(0.8, min(1.7, 0.75 + (point_count / 9.0)))
    score_factor = max(0.75, min(1.4, math.sqrt(comp_score) / 2.2))

    expand = 38.0 * point_factor * spread_factor
    smooth = 17.0 * intensity_factor * score_factor
    contract = min(expand * 0.55, smooth * 0.65)

    shaped = base_geom.buffer(expand).buffer(smooth).buffer(-contract)
    if shaped.is_empty:
        return None
    clipped = shaped.intersection(zone_proj)
    if clipped.is_empty:
        return None

    simplified = clipped.simplify(PICKUP_ZONE_HOTSPOT_SIMPLIFY_M, preserve_topology=True)
    if not simplified.is_empty:
        clipped2 = simplified.intersection(zone_proj)
        if not clipped2.is_empty:
            clipped = clipped2
    return clipped


def _hotspot_merge_decision(candidate_components: List[Dict[str, Any]], selected_cells: set[Tuple[int, int]]) -> Tuple[bool, str]:
    if len(candidate_components) < 2:
        return False, "single_candidate"

    a = candidate_components[0]
    b = candidate_components[1]
    ga = a.get("polygon")
    gb = b.get("polygon")
    if ga is None or gb is None:
        return False, "missing_polygon"

    area_scale = max(40.0, math.sqrt(max(ga.area, 1.0)), math.sqrt(max(gb.area, 1.0)))
    merge_buffer = max(24.0, min(130.0, area_scale * 0.32))
    if ga.buffer(merge_buffer).intersects(gb.buffer(merge_buffer)):
        return True, "buffer_intersection"

    ca = ga.centroid
    cb = gb.centroid
    centroid_distance = ca.distance(cb)
    size_threshold = max(120.0, min(420.0, area_scale * 1.75))
    if centroid_distance <= size_threshold:
        return True, "centroid_proximity"

    cells_a = a.get("cells") or set()
    cells_b = b.get("cells") or set()
    for gx, gy in cells_a:
        for nx in (gx - 1, gx, gx + 1):
            for ny in (gy - 1, gy, gy + 1):
                k = (nx, ny)
                if k in cells_b:
                    return True, "cell_corridor"
                if k in selected_cells:
                    for bx in (nx - 1, nx, nx + 1):
                        for by in (ny - 1, ny, ny + 1):
                            if (bx, by) in cells_b:
                                return True, "density_bridge"
    return False, "separate"


def _determine_live_zone_hotspot_limit(
    zone_id: int,
    zone_data: Dict[str, Any],
    now_ts: int,
) -> int:
    try:
        zone_geom = (zone_data or {}).get("geometry")
        if zone_geom is None or getattr(zone_geom, "is_empty", True):
            return 2

        long_run_rows = _pickup_zone_long_run_points(zone_id, limit=2400)
        historical_anchor_points = build_zone_historical_anchor_points(
            pickup_rows=long_run_rows,
            frame_time=now_ts,
        )
        historical_components = build_zone_historical_anchor_components(
            zone_id=zone_id,
            zone_geom=zone_geom,
            weighted_points=historical_anchor_points,
        )
        limit = determine_zone_hotspot_limit(zone_geom, historical_components)
        return 3 if int(limit) == 3 else 2
    except Exception:
        print(f"[warn] Failed to determine hotspot limit for zone {zone_id}", traceback.format_exc())
        return 2


def _build_zone_hotspot_components(
    zone_id: int,
    zone_meta: Dict[str, Any],
    point_rows: List[Dict[str, Any]],
    fallback: bool = False,
    hotspot_limit: int = 2,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    debug: Dict[str, Any] = {
        "candidate_component_count": 0,
        "emitted_hotspot_count": 0,
        "merged": False,
        "merge_reason": "none",
        "hotspot_ids": [],
        "component_point_counts": [],
        "second_hotspot_qualified": False,
        "second_hotspot_rejected_reason": "",
        "hotspot_limit_used": 2,
        "third_hotspot_qualified": False,
        "third_hotspot_rejected_reason": "",
    }
    hotspot_limit = 3 if int(hotspot_limit) >= 3 else 2
    debug["hotspot_limit_used"] = hotspot_limit

    if len(point_rows) < PICKUP_ZONE_HOTSPOT_MIN_POINTS:
        debug["second_hotspot_rejected_reason"] = "zone_below_min_points"
        return [], debug

    zone_geom = zone_meta.get("geometry")
    if zone_geom is None or zone_geom.is_empty:
        debug["second_hotspot_rejected_reason"] = "zone_geometry_missing"
        return [], debug

    try:
        zone_proj = transform(_to_3857.transform, zone_geom)
    except Exception:
        debug["second_hotspot_rejected_reason"] = "zone_projection_failed"
        return [], debug
    if zone_proj.is_empty:
        debug["second_hotspot_rejected_reason"] = "zone_projection_empty"
        return [], debug

    point_entries = _normalize_pickup_zone_point_entries(point_rows)
    if len(point_entries) < PICKUP_ZONE_HOTSPOT_MIN_POINTS:
        debug["second_hotspot_rejected_reason"] = "not_enough_valid_points"
        return [], debug

    build = _build_density_components(zone_proj, point_entries, threshold_ratio=0.48)
    components = build.get("components") or []
    selected_cells = build.get("selected") or set()
    if fallback or not components:
        fallback_groups: List[Dict[str, Any]] = []
        by_cell: Dict[Tuple[int, int], List[Dict[str, Any]]] = defaultdict(list)
        cell_size = max(90.0, float(PICKUP_ZONE_HOTSPOT_CELL_SIZE_M))
        minx, miny, _, _ = zone_proj.bounds
        for pe in point_entries:
            gx = int(math.floor((pe["x"] - minx) / cell_size))
            gy = int(math.floor((pe["y"] - miny) / cell_size))
            by_cell[(gx, gy)].append(pe)
        if by_cell:
            seeds = sorted(by_cell.items(), key=lambda kv: len(kv[1]), reverse=True)[:2]
            for _, seed_points in seeds:
                sx = sum(p["x"] for p in seed_points) / len(seed_points)
                sy = sum(p["y"] for p in seed_points) / len(seed_points)
                members: List[Dict[str, Any]] = []
                for pe in point_entries:
                    if Point(pe["x"], pe["y"]).distance(Point(sx, sy)) <= 320.0:
                        members.append(pe)
                if not members:
                    continue
                geom = unary_union([Point(p["x"], p["y"]).buffer(max(115.0, 75.0 + 12.0 * len(members))) for p in members]).intersection(zone_proj)
                if geom.is_empty:
                    continue
                fallback_groups.append(
                    {
                        "cells": set(),
                        "component_score": float(sum(p["weight"] for p in members)),
                        "peak_score": float(max((p["weight"] for p in members), default=0.0)),
                        "point_count": len(members),
                        "geometry": geom,
                    }
                )
        components = sorted(
            fallback_groups,
            key=lambda c: (float(c.get("component_score") or 0.0), int(c.get("point_count") or 0)),
            reverse=True,
        )
        selected_cells = set()

    if not components:
        debug["second_hotspot_rejected_reason"] = "no_components"
        return [], debug

    debug["candidate_component_count"] = len(components)
    debug["component_point_counts"] = [int(c.get("point_count") or 0) for c in components[:hotspot_limit]]

    strongest = components[0]
    top_components: List[Dict[str, Any]] = [strongest]
    if len(components) > 1:
        second = components[1]
        second_ok = True
        second_reason = ""
        if len(point_entries) < PICKUP_ZONE_SECOND_HOTSPOT_MIN_POINTS:
            second_ok = False
            second_reason = "zone_points_below_second_threshold"
        elif int(second.get("point_count") or 0) < PICKUP_ZONE_SECOND_COMPONENT_MIN_POINTS:
            second_ok = False
            second_reason = "second_component_low_point_count"
        else:
            s0 = max(0.0001, float(strongest.get("component_score") or 0.0001))
            s1 = float(second.get("component_score") or 0.0)
            if (s1 / s0) < PICKUP_ZONE_SECOND_COMPONENT_MIN_SCORE_RATIO:
                second_ok = False
                second_reason = "second_component_low_strength"
        if second_ok:
            debug["second_hotspot_qualified"] = True
            top_components.append(second)
        else:
            debug["second_hotspot_rejected_reason"] = second_reason

    if hotspot_limit >= 3 and len(components) > 2:
        third = components[2]
        third_ok = True
        third_reason = ""
        if len(point_entries) < 12:
            third_ok = False
            third_reason = "zone_points_below_third_threshold"
        elif int(third.get("point_count") or 0) < 4:
            third_ok = False
            third_reason = "third_component_low_point_count"
        else:
            s0 = max(0.0001, float(strongest.get("component_score") or 0.0001))
            s2 = float(third.get("component_score") or 0.0)
            if (s2 / s0) < 0.35:
                third_ok = False
                third_reason = "third_component_low_strength"
        if third_ok:
            debug["third_hotspot_qualified"] = True
            top_components.append(third)
        else:
            debug["third_hotspot_rejected_reason"] = third_reason
    elif hotspot_limit < 3:
        debug["third_hotspot_rejected_reason"] = "hotspot_limit_below_3"
    elif len(components) <= 2:
        debug["third_hotspot_rejected_reason"] = "no_third_component"

    shaped_candidates: List[Dict[str, Any]] = []
    for rank, comp in enumerate(top_components):
        shaped = _shape_hotspot_component(comp, zone_proj)
        if shaped is None or shaped.is_empty:
            continue
        shaped_candidates.append({**comp, "polygon": shaped, "component_rank": rank})

    if not shaped_candidates:
        debug["second_hotspot_rejected_reason"] = "component_shaping_failed"
        return [], debug

    merge_candidates = shaped_candidates[:2]
    merged, merge_reason = _hotspot_merge_decision(merge_candidates, selected_cells)
    debug["merged"] = merged
    debug["merge_reason"] = merge_reason

    final_components: List[Dict[str, Any]] = []
    if merged and len(merge_candidates) > 1:
        merged_geom = unary_union([c["polygon"] for c in merge_candidates]).intersection(zone_proj)
        if not merged_geom.is_empty:
            merged_component = {
                "polygon": merged_geom,
                "component_score": sum(float(c.get("component_score") or 0.0) for c in merge_candidates),
                "peak_score": max(float(c.get("peak_score") or 0.0) for c in merge_candidates),
                "point_count": sum(int(c.get("point_count") or 0) for c in merge_candidates),
                "component_rank": 0,
                "merged_from_count": len(merge_candidates),
                "cells": set().union(*[c.get("cells") or set() for c in merge_candidates]),
            }
            final_components = [merged_component]
            if hotspot_limit >= 3 and len(shaped_candidates) > 2:
                final_components.append({**shaped_candidates[2], "merged_from_count": 1})
    if not final_components:
        for c in shaped_candidates[:hotspot_limit]:
            final_components.append({**c, "merged_from_count": 1})

    latest_created_at = max((int(p.get("created_at") or 0) for p in point_entries), default=0)
    sample_size = len(point_entries)
    zone_name = zone_meta.get("zone_name") or ((point_rows[0].get("zone_name") if point_rows else "") or "")
    borough = zone_meta.get("borough") or ((point_rows[0].get("borough") if point_rows else "") or "")
    signature = _pickup_zone_signature(point_rows)

    emitted: List[Dict[str, Any]] = []
    for hotspot_index, comp in enumerate(final_components[:hotspot_limit]):
        polygon = comp.get("polygon")
        if polygon is None or polygon.is_empty:
            continue
        hotspot_ll = transform(_to_4326.transform, polygon)
        if hotspot_ll.is_empty:
            continue
        component_score = float(comp.get("component_score") or 0.0)
        peak_score = max(0.0001, float(comp.get("peak_score") or 0.0001))
        intensity = min(1.0, max(0.22, peak_score / max(0.75, 0.45 * sample_size)))
        hotspot_id = f"{zone_id}:{signature[:12]}:{hotspot_index}"
        emitted.append(
            {
                "type": "Feature",
                "geometry": mapping(hotspot_ll),
                "properties": {
                    "zone_id": zone_id,
                    "zone_name": zone_name,
                    "borough": borough,
                    "hotspot_index": hotspot_index,
                    "hotspot_id": hotspot_id,
                    "hotspot_method": "fallback_multi_cluster" if fallback else "recency_weighted_density_components",
                    "sample_size": sample_size,
                    "component_point_count": int(comp.get("point_count") or 0),
                    "latest_created_at": latest_created_at,
                    "intensity": intensity,
                    "signature": signature,
                    "component_rank": int(comp.get("component_rank") or 0),
                    "merged_from_count": int(comp.get("merged_from_count") or 1),
                    "peak_score": peak_score,
                    "component_score": component_score,
                    "max_points_per_zone": PICKUP_ZONE_HOTSPOT_MAX_POINTS,
                    "min_points": PICKUP_ZONE_HOTSPOT_MIN_POINTS,
                    "micro_hotspots": [],
                },
                "_hotspot_proj": polygon,
                "_component_cells": comp.get("cells") or set(),
            }
        )
        debug["hotspot_ids"].append(hotspot_id)

    debug["emitted_hotspot_count"] = len(emitted)
    return emitted, debug


def _build_pickup_zone_hotspot_feature(
    zone_id: int, zone_meta: Dict[str, Any], point_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    components, _ = _build_zone_hotspot_components(zone_id, zone_meta, point_rows, fallback=False)
    return components


def _build_fallback_pickup_zone_hotspot_feature(
    zone_id: int,
    zone_meta: Dict[str, Any],
    point_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    components, _ = _build_zone_hotspot_components(zone_id, zone_meta, point_rows, fallback=True)
    return components


def _build_historical_fallback_zone_hotspot_features(
    zone_id: int,
    zone_data: Dict[str, Any],
    recent_pts: List[Dict[str, Any]],
    hotspot_limit: int,
    now_ts: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    debug: Dict[str, Any] = {
        "historical_fallback_attempted": True,
        "historical_fallback_ok": False,
        "historical_fallback_component_count": 0,
        "historical_fallback_anchor_point_count": 0,
        "historical_fallback_strongest_support": 0.0,
        "historical_fallback_reason": "not_attempted",
    }

    zone_geom = (zone_data or {}).get("geometry")
    if zone_geom is None or getattr(zone_geom, "is_empty", True):
        debug["historical_fallback_reason"] = "zone_geometry_missing"
        return [], debug

    if len(recent_pts) < 2:
        debug["historical_fallback_reason"] = "recent_signal_too_low"
        return [], debug

    long_run_rows = _pickup_zone_long_run_points(zone_id, limit=2400)
    historical_anchor_points = build_zone_historical_anchor_points(
        pickup_rows=long_run_rows,
        frame_time=now_ts,
    )
    historical_components = build_zone_historical_anchor_components(
        zone_id=zone_id,
        zone_geom=zone_geom,
        weighted_points=historical_anchor_points,
    )
    debug["historical_fallback_component_count"] = len(historical_components)
    debug["historical_fallback_anchor_point_count"] = len(historical_anchor_points)
    strongest_support = max(
        [float(c.get("weighted_point_count") or 0.0) for c in historical_components] or [0.0]
    )
    debug["historical_fallback_strongest_support"] = strongest_support

    strong_history = (len(historical_anchor_points) >= 10) or (strongest_support >= 6.0)
    if not strong_history:
        debug["historical_fallback_reason"] = "historical_support_too_low"
        return [], debug

    shaped = convert_historical_components_to_emittable_shapes(
        historical_components=historical_components,
        zone_geom=zone_geom,
    )
    if not shaped:
        debug["historical_fallback_reason"] = "no_historical_shapes"
        return [], debug

    safe_limit = 3 if int(hotspot_limit) >= 3 else 2
    recent_signature = _pickup_zone_signature(recent_pts)
    zone_name = str((zone_data or {}).get("zone_name") or "")
    borough = str((zone_data or {}).get("borough") or "")
    latest_created_at = max((int(p.get("created_at") or 0) for p in recent_pts), default=0)
    historical_sample_size = len(long_run_rows)

    emitted: List[Dict[str, Any]] = []
    for hotspot_index, comp in enumerate(shaped[:safe_limit]):
        polygon = comp.get("polygon_proj")
        geom_ll = comp.get("geometry")
        if polygon is None or getattr(polygon, "is_empty", True) or geom_ll is None or getattr(geom_ll, "is_empty", True):
            continue
        component_point_count = int(comp.get("point_count") or 0)
        strongest_component = float(comp.get("weighted_point_count") or 0.0)
        signature_seed = (
            f"hist|{zone_id}|{recent_signature}|{round(strongest_component, 3)}|"
            f"{component_point_count}|{latest_created_at}|{hotspot_index}"
        )
        signature = hashlib.sha1(signature_seed.encode("utf-8")).hexdigest()
        hotspot_id = f"hist:{zone_id}:{signature[:12]}:{hotspot_index}"
        emitted.append(
            {
                "type": "Feature",
                "geometry": mapping(geom_ll),
                "properties": {
                    "zone_id": zone_id,
                    "zone_name": zone_name,
                    "borough": borough,
                    "hotspot_index": hotspot_index,
                    "hotspot_id": hotspot_id,
                    "hotspot_method": "historical_anchor_fallback",
                    "sample_size": len(recent_pts),
                    "historical_sample_size": historical_sample_size,
                    "component_point_count": component_point_count,
                    "latest_created_at": latest_created_at,
                    "intensity": float(comp.get("intensity") or 0.3),
                    "confidence": float(comp.get("confidence") or 0.4),
                    "signature": signature,
                    "merged_from_count": 1,
                    "max_points_per_zone": PICKUP_ZONE_HOTSPOT_MAX_POINTS,
                    "min_points": PICKUP_ZONE_HOTSPOT_MIN_POINTS,
                    "micro_hotspots": [],
                },
                "_hotspot_proj": polygon,
                "_component_cells": set(comp.get("cells") or []),
            }
        )

    debug["historical_fallback_ok"] = bool(emitted)
    debug["historical_fallback_reason"] = "emitted" if emitted else "shape_emit_empty"
    return emitted, debug


def _build_zone_micro_hotspots_payload(
    zone_id: int,
    zone_meta: Dict[str, Any],
    point_rows: List[Dict[str, Any]],
    hotspot_feature: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if len(point_rows) < PICKUP_ZONE_HOTSPOT_MIN_POINTS or not isinstance(hotspot_feature, dict):
        return []

    props = hotspot_feature.get("properties") or {}
    hotspot_id = props.get("hotspot_id")
    hotspot_index = props.get("hotspot_index")
    if hotspot_id is None or hotspot_index is None:
        return []

    zone_geom = zone_meta.get("geometry")
    if zone_geom is None or zone_geom.is_empty:
        return []
    try:
        zone_proj = transform(_to_3857.transform, zone_geom)
    except Exception:
        return []
    if zone_proj.is_empty:
        return []

    hotspot_proj = hotspot_feature.get("_hotspot_proj")
    if hotspot_proj is None:
        try:
            hotspot_proj = transform(_to_3857.transform, shape(hotspot_feature.get("geometry"))).intersection(zone_proj)
        except Exception:
            return []
    if hotspot_proj is None or hotspot_proj.is_empty:
        return []

    cell_size_m = 70.0
    minx, miny, _, _ = zone_proj.bounds
    weighted_buckets: Dict[Tuple[int, int], float] = defaultdict(float)
    raw_counts: Dict[Tuple[int, int], int] = defaultdict(int)

    for idx, row in enumerate(point_rows):
        try:
            lng = float(row["lng"])
            lat = float(row["lat"])
            x, y = _to_3857.transform(lng, lat)
        except Exception:
            continue
        pt = Point(x, y)
        if not hotspot_proj.covers(pt):
            continue
        n = len(point_rows)
        weight = 1.0 if n <= 1 else (1.0 - 0.65 * (idx / (n - 1)))
        gx = int(math.floor((x - minx) / cell_size_m))
        gy = int(math.floor((y - miny) / cell_size_m))
        weighted_buckets[(gx, gy)] += weight
        raw_counts[(gx, gy)] += 1

    if not weighted_buckets:
        return []

    best_cell = max(weighted_buckets.items(), key=lambda kv: kv[1])[0]
    gx, gy = best_cell
    center_x = minx + ((gx + 0.5) * cell_size_m)
    center_y = miny + ((gy + 0.5) * cell_size_m)
    center_lng, center_lat = _to_4326.transform(center_x, center_y)
    event_count = int(raw_counts.get(best_cell, 0))
    intensity = round(min(1.0, max(0.2, weighted_buckets[best_cell] / 7.5)), 3)
    confidence = round(min(0.98, 0.5 + (weighted_buckets[best_cell] / 12.0)), 3)

    zone_name = zone_meta.get("zone_name") or ((point_rows[0].get("zone_name") if point_rows else "") or "")
    borough = zone_meta.get("borough") or ((point_rows[0].get("borough") if point_rows else "") or "")

    micro = {
        "cluster_id": f"{hotspot_id}:{gx}:{gy}",
        "zone_id": zone_id,
        "hotspot_id": hotspot_id,
        "hotspot_index": int(hotspot_index),
        "center_lat": center_lat,
        "center_lng": center_lng,
        "radius_m": 42,
        "intensity": intensity,
        "confidence": confidence,
        "event_count": event_count,
        "recommended": False,
        "zone_name": zone_name,
        "borough": borough,
        "micro_method": "densest_cell_inside_hotspot",
    }
    return [micro]


def _current_timeslot_bin(now_ts: int, bin_minutes: int = HOTSPOT_TIMESLOT_BIN_MINUTES) -> int:
    dt = time.gmtime(now_ts)
    return int((dt.tm_hour * 60 + dt.tm_min) // max(1, int(bin_minutes)))


def _active_visible_driver_count() -> int:
    cutoff = int(time.time()) - max(30, PRESENCE_STALE_SECONDS)
    ghost_visible = _ghost_visible_sql("u.ghost_mode")
    row = _db_query_one(
        f"""
        SELECT COUNT(*) AS c
        FROM presence p
        LEFT JOIN users u ON u.id = p.user_id
        WHERE p.updated_at >= ?
          AND {ghost_visible}
        """,
        (cutoff,),
    )
    return int(row["c"] or 0) if row else 0


def _rounded_bbox_key(
    min_lat: Optional[float],
    min_lng: Optional[float],
    max_lat: Optional[float],
    max_lng: Optional[float],
) -> Optional[Tuple[float, float, float, float]]:
    bbox = [min_lat, min_lng, max_lat, max_lng]
    if not all(v is not None for v in bbox):
        return None
    lo_lat = min(float(min_lat), float(max_lat))
    hi_lat = max(float(min_lat), float(max_lat))
    lo_lng = min(float(min_lng), float(max_lng))
    hi_lng = max(float(min_lng), float(max_lng))
    return (
        round(lo_lat, 4),
        round(lo_lng, 4),
        round(hi_lat, 4),
        round(hi_lng, 4),
    )


def _presence_bbox_with_buffer(
    min_lat: Optional[float],
    min_lng: Optional[float],
    max_lat: Optional[float],
    max_lng: Optional[float],
    zoom: Optional[float],
) -> Optional[Tuple[float, float, float, float]]:
    bbox_key = _rounded_bbox_key(min_lat, min_lng, max_lat, max_lng)
    if bbox_key is None:
        return None
    lo_lat, lo_lng, hi_lat, hi_lng = bbox_key
    lat_span = max(0.002, hi_lat - lo_lat)
    lng_span = max(0.002, hi_lng - lo_lng)
    zoom_value = int(zoom or 0)
    if zoom_value >= 14:
        factor = 0.12
    elif zoom_value >= 11:
        factor = 0.22
    else:
        factor = 0.35
    buffer_lat = max(0.0025, lat_span * factor)
    buffer_lng = max(0.0025, lng_span * factor)
    return (
        round(lo_lat - buffer_lat, 5),
        round(lo_lng - buffer_lng, 5),
        round(hi_lat + buffer_lat, 5),
        round(hi_lng + buffer_lng, 5),
    )


def _zoom_bucket(zoom: Optional[float]) -> int:
    value = int(zoom or 0)
    if value <= 0:
        return 0
    if value <= 10:
        return 10
    if value <= 13:
        return 13
    return 16


def _presence_cache_key(
    *,
    max_age_sec: int,
    mode: str,
    limit: Optional[int],
    bbox: Optional[Tuple[float, float, float, float]],
    zoom: Optional[float],
) -> str:
    return "|".join(
        [
            f"max_age={int(max_age_sec)}",
            f"mode={mode}",
            f"limit={int(limit) if limit is not None else 'all'}",
            f"bbox={bbox!r}",
            f"zoom_bucket={_zoom_bucket(zoom)}",
        ]
    )


def _purge_presence_viewport_cache(now_monotonic: Optional[float] = None) -> None:
    now_value = now_monotonic if now_monotonic is not None else time.monotonic()
    expired = [
        key
        for key, value in _presence_viewport_cache.items()
        if float(value.get("expires_at_monotonic") or 0.0) <= now_value
    ]
    for key in expired:
        _presence_viewport_cache.pop(key, None)
    while len(_presence_viewport_cache) > PRESENCE_VIEWPORT_CACHE_MAX:
        oldest_key = min(
            _presence_viewport_cache.items(),
            key=lambda item: float(item[1].get("last_access_monotonic") or 0.0),
        )[0]
        _presence_viewport_cache.pop(oldest_key, None)


def _pickup_recent_cache_key(
    *,
    limit: int,
    zone_sample_limit: int,
    zone_id: Optional[int],
    bbox_key: Optional[Tuple[float, float, float, float]],
    include_debug: bool,
    viewer_is_admin: bool,
) -> str:
    parts = [
        f"limit={int(limit)}",
        f"zone_sample_limit={int(zone_sample_limit)}",
        f"zone_id={int(zone_id) if zone_id is not None else 'all'}",
        f"bbox={bbox_key!r}",
        f"debug={1 if include_debug else 0}",
        f"admin={1 if viewer_is_admin else 0}",
    ]
    return "|".join(parts)


def _purge_pickup_recent_cache(now_monotonic: Optional[float] = None) -> None:
    now_value = now_monotonic if now_monotonic is not None else time.monotonic()
    expired = [
        key for key, value in _pickup_recent_cache.items()
        if float(value.get("expires_at_monotonic") or 0.0) <= now_value
    ]
    for key in expired:
        _pickup_recent_cache.pop(key, None)
    while len(_pickup_recent_cache) > PICKUP_RECENT_CACHE_MAX:
        oldest_key = min(
            _pickup_recent_cache.items(),
            key=lambda item: float(item[1].get("last_access_monotonic") or 0.0),
        )[0]
        _pickup_recent_cache.pop(oldest_key, None)


def _get_pickup_last_good_overlay(
    cache_key: str,
    now_monotonic: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    now_value = now_monotonic if now_monotonic is not None else time.monotonic()
    with _pickup_recent_last_good_overlay_lock:
        entry = _pickup_recent_last_good_overlay_cache.get(cache_key)
        if not entry:
            return None
        saved_at_monotonic = float(entry.get("saved_at_monotonic") or 0.0)
        if (now_value - saved_at_monotonic) > PICKUP_LAST_GOOD_OVERLAY_TTL_SECONDS:
            _pickup_recent_last_good_overlay_cache.pop(cache_key, None)
            return None
        return copy.deepcopy(entry)


def _set_pickup_last_good_overlay(
    cache_key: str,
    payload: Dict[str, Any],
    now_monotonic: Optional[float] = None,
) -> None:
    now_value = now_monotonic if now_monotonic is not None else time.monotonic()
    saved_payload = copy.deepcopy(payload)
    saved_payload["saved_at_monotonic"] = now_value
    saved_payload["saved_at_unix"] = int(time.time())
    with _pickup_recent_last_good_overlay_lock:
        _pickup_recent_last_good_overlay_cache[cache_key] = saved_payload


def _purge_pickup_last_good_overlay_cache(now_monotonic: Optional[float] = None) -> None:
    now_value = now_monotonic if now_monotonic is not None else time.monotonic()
    with _pickup_recent_last_good_overlay_lock:
        expired = [
            key
            for key, value in _pickup_recent_last_good_overlay_cache.items()
            if (now_value - float(value.get("saved_at_monotonic") or 0.0)) > PICKUP_LAST_GOOD_OVERLAY_TTL_SECONDS
        ]
        for key in expired:
            _pickup_recent_last_good_overlay_cache.pop(key, None)


def _cleanup_pickup_zone_caches(now_monotonic: Optional[float] = None) -> None:
    now_value = now_monotonic if now_monotonic is not None else time.monotonic()
    with _pickup_zone_hotspot_cache_lock:
        stale_zone_ids = [
            zone_id
            for zone_id, cached in _pickup_zone_hotspot_feature_cache.items()
            if (now_value - float(cached.get("last_access_monotonic") or 0.0)) > PICKUP_HOTSPOT_CACHE_STALE_SECONDS
        ]
        for zone_id in stale_zone_ids:
            _pickup_zone_hotspot_feature_cache.pop(zone_id, None)
            _pickup_zone_score_cache.pop(zone_id, None)
    with _pickup_zone_score_bundle_lock:
        stale_bundle_keys = [
            key
            for key, cached in _pickup_zone_score_bundle_cache.items()
            if float(cached.get("expires_at_monotonic") or 0.0) <= now_value
        ]
        for key in stale_bundle_keys:
            _pickup_zone_score_bundle_cache.pop(key, None)
    _purge_pickup_last_good_overlay_cache(now_value)


def _maybe_prune_pickup_experiment_tables(now_ts: int) -> None:
    global _pickup_last_experiment_prune_monotonic
    now_monotonic = time.monotonic()
    if (now_monotonic - _pickup_last_experiment_prune_monotonic) < PICKUP_EXPERIMENT_PRUNE_INTERVAL_SECONDS:
        return
    with _pickup_zone_maintenance_lock:
        now_monotonic = time.monotonic()
        if (now_monotonic - _pickup_last_experiment_prune_monotonic) < PICKUP_EXPERIMENT_PRUNE_INTERVAL_SECONDS:
            return
        prune_experiment_tables(_db_exec, now_ts=now_ts)
        _pickup_last_experiment_prune_monotonic = now_monotonic




def _invalidate_pickup_overlay_caches() -> None:
    with _pickup_recent_cache_lock:
        _pickup_recent_cache.clear()
    with _pickup_recent_last_good_overlay_lock:
        _pickup_recent_last_good_overlay_cache.clear()
    with _pickup_zone_hotspot_cache_lock:
        _pickup_zone_hotspot_feature_cache.clear()
        _pickup_zone_score_cache.clear()
    with _pickup_zone_score_bundle_lock:
        _pickup_zone_score_bundle_cache.clear()


register_pickup_write_cache_invalidation_hook(_invalidate_pickup_overlay_caches)


def _pickup_zone_same_timeslot_support(zone_ids: List[int], now_ts: int) -> Dict[int, float]:
    if not zone_ids:
        return {}
    slot = _current_timeslot_bin(now_ts)
    weekday = int(time.gmtime(now_ts).tm_wday)
    placeholders = ",".join(["?"] * len(zone_ids))

    if DB_BACKEND == "postgres":
        dow_expr = "CAST(EXTRACT(DOW FROM to_timestamp(pl.created_at)) AS INTEGER)"
        bin_expr = "CAST((MOD(pl.created_at, 86400) / 60) / ? AS INTEGER)"
    else:
        dow_expr = "CAST(strftime('%w', pl.created_at, 'unixepoch') AS INTEGER)"
        bin_expr = "CAST(((pl.created_at % 86400) / 60) / ? AS INTEGER)"

    rows = _db_query_all(
        f"""
        SELECT pl.zone_id, {dow_expr} AS dow_v, {bin_expr} AS bin_v, COUNT(*) AS c
        FROM pickup_logs pl
        WHERE pl.zone_id IN ({placeholders})
          AND {pickup_log_not_voided_sql('pl')}
        GROUP BY pl.zone_id, dow_v, bin_v
        """,
        tuple([HOTSPOT_TIMESLOT_BIN_MINUTES] + list(zone_ids)),
    )

    out: Dict[int, float] = {int(z): 0.0 for z in zone_ids}
    for r in rows:
        zid = int(r["zone_id"])
        dow_v = int(r["dow_v"])
        bin_v = int(r["bin_v"])
        c = float(r["c"] or 0.0)
        delta = abs(bin_v - slot)
        if dow_v == weekday and delta == 0:
            w = 1.00
        elif dow_v == weekday and delta == 1:
            w = 0.80
        elif dow_v == weekday and delta == 2:
            w = 0.65
        elif delta == 0:
            w = 0.55
        else:
            w = 0.35
        out[zid] += c * w
    return out


def _pickup_zone_historical_support(zone_ids: List[int], now_ts: int) -> Dict[int, float]:
    if not zone_ids:
        return {}
    placeholders = ",".join(["?"] * len(zone_ids))
    rows = _db_query_all(
        f"""
        SELECT pl.zone_id, COUNT(*) AS c
        FROM pickup_logs pl
        WHERE pl.zone_id IN ({placeholders})
          AND {pickup_log_not_voided_sql("pl")}
        GROUP BY pl.zone_id
        """,
        tuple(zone_ids),
    )
    out: Dict[int, float] = {int(z): 0.0 for z in zone_ids}
    for r in rows:
        out[int(r["zone_id"])] = float(r["c"])
    return out


def _pickup_zone_long_run_points(zone_id: int, limit: int = 2400) -> List[Dict[str, Any]]:
    cap = max(120, min(5000, int(limit)))
    rows = _db_query_all(
        f"""
        SELECT pl.id, pl.zone_id, pl.zone_name, pl.borough, pl.user_id, pl.lat, pl.lng, pl.created_at
        FROM pickup_logs pl
        WHERE pl.zone_id = ?
          AND {pickup_log_not_voided_sql('pl')}
        ORDER BY pl.created_at DESC, pl.id DESC
        LIMIT ?
        """,
        (int(zone_id), cap),
    )
    return [dict(r) for r in rows]


def _choose_primary_recommended_hotspot_feature(zone_features: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not zone_features:
        return None
    ranked = sorted(
        [f for f in zone_features if isinstance(f, dict)],
        key=lambda feature: (
            -float((feature.get("properties") or {}).get("intensity") or 0.0),
            -float((feature.get("properties") or {}).get("confidence") or 0.0),
            int((feature.get("properties") or {}).get("hotspot_index") or 10_000),
        ),
    )
    return ranked[0] if ranked else None


def _settle_stale_hotspot_recommendation_outcomes(now_ts: int) -> int:
    cutoff = int(now_ts) - RECOMMENDATION_OUTCOME_MATURITY_SECONDS
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute(
                _sql(
                    """
                    UPDATE recommendation_outcomes
                    SET converted_to_trip = ?, minutes_to_trip = ?
                    WHERE converted_to_trip IS NULL
                      AND recommended_at <= ?
                    """
                ),
                (
                    False if DB_BACKEND == "postgres" else 0,
                    float(RECOMMENDATION_OUTCOME_MATURITY_SECONDS / 60.0),
                    int(cutoff),
                ),
            )
            updated_rows = int(cur.rowcount or 0)
            conn.commit()
            return updated_rows
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _settle_stale_micro_recommendation_outcomes(now_ts: int) -> int:
    cutoff = int(now_ts) - RECOMMENDATION_OUTCOME_MATURITY_SECONDS
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute(
                _sql(
                    """
                    UPDATE micro_recommendation_outcomes
                    SET converted_to_trip = ?, minutes_to_trip = ?
                    WHERE converted_to_trip IS NULL
                      AND recommended_at <= ?
                    """
                ),
                (
                    False if DB_BACKEND == "postgres" else 0,
                    float(RECOMMENDATION_OUTCOME_MATURITY_SECONDS / 60.0),
                    int(cutoff),
                ),
            )
            updated_rows = int(cur.rowcount or 0)
            conn.commit()
            return updated_rows
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _maybe_settle_stale_recommendation_outcomes(now_ts: int) -> Dict[str, int]:
    global _last_outcome_settlement_monotonic
    now_monotonic = time.monotonic()
    if (now_monotonic - float(_last_outcome_settlement_monotonic or 0.0)) < OUTCOME_SETTLEMENT_SWEEP_INTERVAL_SECONDS:
        return {
            "settled_stale_hotspot_outcomes": 0,
            "settled_stale_micro_outcomes": 0,
        }

    with _outcome_settlement_lock:
        refreshed_monotonic = time.monotonic()
        if (refreshed_monotonic - float(_last_outcome_settlement_monotonic or 0.0)) < OUTCOME_SETTLEMENT_SWEEP_INTERVAL_SECONDS:
            return {
                "settled_stale_hotspot_outcomes": 0,
                "settled_stale_micro_outcomes": 0,
            }
        settled_hotspot = _settle_stale_hotspot_recommendation_outcomes(now_ts)
        settled_micro = _settle_stale_micro_recommendation_outcomes(now_ts)
        _last_outcome_settlement_monotonic = refreshed_monotonic
    return {
        "settled_stale_hotspot_outcomes": int(settled_hotspot),
        "settled_stale_micro_outcomes": int(settled_micro),
    }


def _recent_recommendation_outcomes_with_scope(
    zone_id: int,
    hotspot_id: Optional[str] = None,
    max_rows: int = 80,
) -> Tuple[List[Dict[str, Any]], str, int, int]:
    cutoff = int(time.time()) - (30 * 24 * 3600)
    zone_fallback_rows: List[Dict[str, Any]] = []
    if hotspot_id:
        hotspot_rows = _db_query_all(
            """
            SELECT converted_to_trip, minutes_to_trip, recommended_at, distance_to_recommendation_miles
            FROM recommendation_outcomes
            WHERE zone_id = ?
              AND cluster_id = ?
              AND converted_to_trip IS NOT NULL
              AND recommended_at >= ?
            ORDER BY recommended_at DESC, id DESC
            LIMIT ?
            """,
            (int(zone_id), str(hotspot_id), int(cutoff), int(max_rows)),
        )
        hotspot_count = len(hotspot_rows)
        if hotspot_count >= 6:
            return [dict(r) for r in hotspot_rows], "hotspot_specific", hotspot_count, 0
        zone_fallback_rows = _db_query_all(
            """
            SELECT converted_to_trip, minutes_to_trip, recommended_at, distance_to_recommendation_miles
            FROM recommendation_outcomes
            WHERE zone_id = ?
              AND converted_to_trip IS NOT NULL
              AND recommended_at >= ?
            ORDER BY recommended_at DESC, id DESC
            LIMIT ?
            """,
            (int(zone_id), int(cutoff), int(max_rows)),
        )
        return [dict(r) for r in zone_fallback_rows], "zone_fallback", hotspot_count, len(zone_fallback_rows)

    zone_fallback_rows = _db_query_all(
        """
        SELECT converted_to_trip, minutes_to_trip, recommended_at, distance_to_recommendation_miles
        FROM recommendation_outcomes
        WHERE zone_id = ?
          AND converted_to_trip IS NOT NULL
          AND recommended_at >= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT ?
        """,
        (int(zone_id), int(cutoff), int(max_rows)),
    )
    return [dict(r) for r in zone_fallback_rows], "zone_fallback", 0, len(zone_fallback_rows)


def _recent_recommendation_outcomes(zone_id: int, hotspot_id: Optional[str] = None, max_rows: int = 80) -> List[Dict[str, Any]]:
    rows, _scope, _hotspot_count, _zone_count = _recent_recommendation_outcomes_with_scope(
        zone_id=zone_id,
        hotspot_id=hotspot_id,
        max_rows=max_rows,
    )
    return rows


def _recent_recommendation_outcomes_for_merged_hotspot(
    covered_zone_ids: Any,
    hotspot_id: Optional[str],
    max_rows: int = 80,
) -> Tuple[List[Dict[str, Any]], str, int]:
    normalized_zone_ids: List[int] = []
    seen_zone_ids: set[int] = set()
    for raw_zone_id in (covered_zone_ids or []):
        try:
            zone_id = int(raw_zone_id)
        except Exception:
            continue
        if zone_id in seen_zone_ids:
            continue
        seen_zone_ids.add(zone_id)
        normalized_zone_ids.append(zone_id)

    hotspot_key = str(hotspot_id or "").strip()
    if not normalized_zone_ids or not hotspot_key:
        return [], "merged_hotspot_specific", 0

    cutoff = int(time.time()) - (30 * 24 * 3600)
    placeholders = ",".join(["?"] * len(normalized_zone_ids))
    rows = _db_query_all(
        f"""
        SELECT converted_to_trip, minutes_to_trip, recommended_at, distance_to_recommendation_miles
        FROM recommendation_outcomes
        WHERE cluster_id = ?
          AND zone_id IN ({placeholders})
          AND converted_to_trip IS NOT NULL
          AND recommended_at >= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT ?
        """,
        tuple([hotspot_key] + normalized_zone_ids + [int(cutoff), int(max_rows)]),
    )
    normalized_rows = [dict(r) for r in rows]
    return normalized_rows, "merged_hotspot_specific", len(normalized_rows)


def _compute_merged_hotspot_learning_payload(
    merged_feature: Dict[str, Any],
    feature_a: Dict[str, Any],
    feature_b: Dict[str, Any],
) -> Dict[str, Any]:
    props = (merged_feature or {}).get("properties") or {}
    merged_hotspot_id = str(props.get("hotspot_id") or "").strip()
    covered_zone_ids = props.get("covered_zone_ids") or props.get("merged_zone_ids") or []
    merged_rows, merged_scope_used, merged_sample_count = _recent_recommendation_outcomes_for_merged_hotspot(
        covered_zone_ids=covered_zone_ids,
        hotspot_id=merged_hotspot_id if merged_hotspot_id else None,
    )
    if merged_sample_count >= 6:
        outcome = get_zone_or_hotspot_outcome_modifier(
            merged_rows,
            precision_target_miles=0.12,
            precision_span_miles=0.40,
            precision_profile="merged_hotspot_v1",
        )
        outcome_modifier = float(outcome.get("modifier") or 1.0)
        quality_modifier = (
            _metric_from_feature_props(feature_a, "quality_modifier", default=1.0)
            + _metric_from_feature_props(feature_b, "quality_modifier", default=1.0)
        ) / 2.0
        return {
            "outcome_modifier": outcome_modifier,
            "quality_modifier": quality_modifier,
            "live_combined_modifier": max(0.20, outcome_modifier * quality_modifier),
            "outcome_sample_count": int(float(outcome.get("sample_count") or 0.0)),
            "outcome_effective_sample_count": float(outcome.get("effective_sample_count") or 0.0),
            "outcome_support_strength": float(outcome.get("support_strength") or 0.0),
            "outcome_scope_used": merged_scope_used,
            "outcome_conversion_rate": float(outcome.get("conversion_rate") or 0.0),
            "outcome_raw_conversion_rate": float(outcome.get("raw_conversion_rate") or 0.0),
            "outcome_median_minutes_to_trip": float(outcome.get("median_minutes_to_trip") or 0.0),
            "outcome_representative_minutes_to_trip": float(outcome.get("representative_minutes_to_trip") or 0.0),
            "outcome_representative_distance_to_recommendation_miles": float(
                outcome.get("representative_distance_to_recommendation_miles") or 0.0
            ),
            "outcome_distance_sample_count": int(float(outcome.get("distance_sample_count") or 0.0)),
            "outcome_precision_boost_component": float(outcome.get("precision_boost_component") or 0.0),
            "outcome_precision_target_miles": float(outcome.get("precision_target_miles") or 0.12),
            "outcome_precision_span_miles": float(outcome.get("precision_span_miles") or 0.40),
            "outcome_precision_profile": str(outcome.get("precision_profile") or "merged_hotspot_v1"),
            "outcome_raw_modifier_before_support_damping": float(outcome.get("raw_modifier_before_support_damping") or 1.0),
            "outcome_recency_weight_version": str(outcome.get("recency_weight_version") or ""),
            "merged_outcome_scope_used": merged_scope_used,
            "merged_outcome_sample_count": int(float(outcome.get("sample_count") or 0.0)),
            "merged_outcome_effective_sample_count": float(outcome.get("effective_sample_count") or 0.0),
            "merged_outcome_support_strength": float(outcome.get("support_strength") or 0.0),
            "merged_outcome_conversion_rate": float(outcome.get("conversion_rate") or 0.0),
            "merged_outcome_raw_conversion_rate": float(outcome.get("raw_conversion_rate") or 0.0),
            "merged_outcome_median_minutes_to_trip": float(outcome.get("median_minutes_to_trip") or 0.0),
            "merged_outcome_representative_minutes_to_trip": float(outcome.get("representative_minutes_to_trip") or 0.0),
            "merged_outcome_representative_distance_to_recommendation_miles": float(
                outcome.get("representative_distance_to_recommendation_miles") or 0.0
            ),
            "merged_outcome_distance_sample_count": int(float(outcome.get("distance_sample_count") or 0.0)),
            "merged_outcome_precision_boost_component": float(outcome.get("precision_boost_component") or 0.0),
            "merged_outcome_precision_target_miles": float(outcome.get("precision_target_miles") or 0.12),
            "merged_outcome_precision_span_miles": float(outcome.get("precision_span_miles") or 0.40),
            "merged_outcome_precision_profile": str(outcome.get("precision_profile") or "merged_hotspot_v1"),
            "merged_outcome_raw_modifier_before_support_damping": float(outcome.get("raw_modifier_before_support_damping") or 1.0),
            "merged_outcome_recency_weight_version": str(outcome.get("recency_weight_version") or ""),
            "used_merged_hotspot_specific": True,
        }

    fallback_outcome_modifier = max(
        _metric_from_feature_props(feature_a, "outcome_modifier", default=1.0),
        _metric_from_feature_props(feature_b, "outcome_modifier", default=1.0),
    )
    fallback_quality_modifier = (
        _metric_from_feature_props(feature_a, "quality_modifier", default=1.0)
        + _metric_from_feature_props(feature_b, "quality_modifier", default=1.0)
    ) / 2.0
    fallback_live_combined_modifier = max(
        _metric_from_feature_props(feature_a, "live_combined_modifier", default=1.0),
        _metric_from_feature_props(feature_b, "live_combined_modifier", default=1.0),
    )
    return {
        "outcome_modifier": fallback_outcome_modifier,
        "quality_modifier": fallback_quality_modifier,
        "live_combined_modifier": fallback_live_combined_modifier,
        "outcome_sample_count": int(merged_sample_count),
        "outcome_effective_sample_count": 0.0,
        "outcome_support_strength": 0.0,
        "outcome_scope_used": "merged_fallback_from_children",
        "outcome_conversion_rate": 0.0,
        "outcome_raw_conversion_rate": 0.0,
        "outcome_median_minutes_to_trip": 0.0,
        "outcome_representative_minutes_to_trip": 0.0,
        "outcome_representative_distance_to_recommendation_miles": 0.0,
        "outcome_distance_sample_count": 0,
        "outcome_precision_boost_component": 0.0,
        "outcome_precision_target_miles": 0.12,
        "outcome_precision_span_miles": 0.40,
        "outcome_precision_profile": "merged_hotspot_v1",
        "outcome_raw_modifier_before_support_damping": float(fallback_outcome_modifier),
        "outcome_recency_weight_version": "resolved_recency_v1",
        "merged_outcome_scope_used": "merged_fallback_from_children",
        "merged_outcome_sample_count": int(merged_sample_count),
        "merged_outcome_effective_sample_count": 0.0,
        "merged_outcome_support_strength": 0.0,
        "merged_outcome_conversion_rate": 0.0,
        "merged_outcome_raw_conversion_rate": 0.0,
        "merged_outcome_median_minutes_to_trip": 0.0,
        "merged_outcome_representative_minutes_to_trip": 0.0,
        "merged_outcome_representative_distance_to_recommendation_miles": 0.0,
        "merged_outcome_distance_sample_count": 0,
        "merged_outcome_precision_boost_component": 0.0,
        "merged_outcome_precision_target_miles": 0.12,
        "merged_outcome_precision_span_miles": 0.40,
        "merged_outcome_precision_profile": "merged_hotspot_v1",
        "merged_outcome_raw_modifier_before_support_damping": float(fallback_outcome_modifier),
        "merged_outcome_recency_weight_version": "resolved_recency_v1",
        "used_merged_hotspot_specific": False,
    }


def _pickup_zone_density_penalty(zone_ids: List[int]) -> Dict[int, float]:
    if not zone_ids:
        return {}
    cutoff = int(time.time()) - max(30, PRESENCE_STALE_SECONDS)
    placeholders = ",".join(["?"] * len(zone_ids))
    rows = _db_query_all(
        f"""
        SELECT pl.zone_id, COUNT(DISTINCT p.user_id) AS c
        FROM presence p
        LEFT JOIN pickup_logs pl ON pl.user_id = p.user_id AND {pickup_log_not_voided_sql('pl')}
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


def _enrich_emitted_zone_hotspot_features(
    zone_id: int,
    zone_data: Dict[str, Any],
    zone_features: List[Dict[str, Any]],
    pts: List[Dict[str, Any]],
    score: Any,
    now_ts: int,
) -> Dict[str, Any]:
    if not zone_features:
        return {}

    zone_geom = (zone_data or {}).get("geometry")
    long_run_rows: List[Dict[str, Any]] = []
    historical_anchor_points: List[Dict[str, Any]] = []
    historical_components: List[Dict[str, Any]] = []
    try:
        long_run_rows = _pickup_zone_long_run_points(zone_id, limit=2400)
        historical_anchor_points = build_zone_historical_anchor_points(
            pickup_rows=long_run_rows,
            frame_time=now_ts,
        )
        historical_components = build_zone_historical_anchor_components(
            zone_id=zone_id,
            zone_geom=zone_geom,
            weighted_points=historical_anchor_points,
        )
    except Exception:
        print(f"[warn] Failed hotspot historical enrichment for zone {zone_id}", traceback.format_exc())

    historical_weighted_support = max(
        [float(c.get("weighted_point_count") or 0.0) for c in historical_components] or [0.0]
    )
    historical_component_score = max(
        [float(c.get("component_score") or 0.0) for c in historical_components] or [0.0]
    )
    historical_component_count = len(historical_components)
    historical_anchor_point_count = len(historical_anchor_points)
    historical_strength = max(0.0, min(1.0, historical_weighted_support / 12.0))

    score_short_trip_share = 0.0
    score_continuation = 0.0
    score_saturation = 0.0
    if score is not None:
        score_short_trip_share = float(getattr(score, "short_trip_share", 0.0) or 0.0)
        score_continuation = float(getattr(score, "continuation_score", getattr(score, "same_timeslot_component", 0.0)) or 0.0)
        score_saturation = float(getattr(score, "saturation", getattr(score, "density_penalty", 0.0)) or 0.0)
    borough = str((zone_data or {}).get("borough") or "")

    for feature in zone_features:
        props = feature.setdefault("properties", {})
        hotspot_id = str(props.get("hotspot_id") or "")
        outcome_rows, outcome_scope_used, hotspot_specific_count, zone_fallback_count = _recent_recommendation_outcomes_with_scope(
            zone_id,
            hotspot_id if hotspot_id else None,
        )
        outcome = get_zone_or_hotspot_outcome_modifier(
            outcome_rows,
            precision_target_miles=0.12,
            precision_span_miles=0.40,
            precision_profile="hotspot_v1",
        )
        outcome_modifier = float(outcome.get("modifier") or 1.0)

        quality = build_hotspot_quality_modifier(
            short_trip_share=score_short_trip_share,
            continuation_score=score_continuation,
            saturation=score_saturation,
            borough=borough,
        )
        quality_modifier = float(quality.get("quality_modifier") or 1.0)

        base_intensity = float(props.get("intensity") or 0.55)
        base_confidence = float(props.get("confidence") or getattr(score, "confidence", 0.55) or 0.55)
        hist_boost = 0.85 + (0.25 * historical_strength)
        live_combined_modifier = hist_boost * outcome_modifier * quality_modifier
        final_intensity = max(0.20, min(1.00, base_intensity * live_combined_modifier))
        final_confidence = max(0.20, min(0.98, base_confidence * live_combined_modifier))

        props["historical_anchor_point_count"] = historical_anchor_point_count
        props["historical_component_count"] = historical_component_count
        props["historical_weighted_support"] = round(historical_weighted_support, 4)
        props["historical_component_score"] = round(historical_component_score, 4)
        props["historical_strength"] = round(historical_strength, 4)

        props["outcome_modifier"] = round(outcome_modifier, 4)
        props["outcome_sample_count"] = int(float(outcome.get("sample_count") or 0.0))
        props["outcome_scope_used"] = outcome_scope_used
        props["hotspot_specific_outcome_sample_count"] = hotspot_specific_count
        props["zone_fallback_outcome_sample_count"] = zone_fallback_count
        props["outcome_conversion_rate"] = round(float(outcome.get("conversion_rate") or 0.0), 4)
        props["outcome_median_minutes_to_trip"] = round(float(outcome.get("median_minutes_to_trip") or 0.0), 4)
        props["outcome_effective_sample_count"] = round(float(outcome.get("effective_sample_count") or 0.0), 4)
        props["outcome_raw_conversion_rate"] = round(float(outcome.get("raw_conversion_rate") or 0.0), 4)
        props["outcome_representative_minutes_to_trip"] = round(float(outcome.get("representative_minutes_to_trip") or 0.0), 4)
        props["outcome_representative_distance_to_recommendation_miles"] = round(
            float(outcome.get("representative_distance_to_recommendation_miles") or 0.0),
            4,
        )
        props["outcome_distance_sample_count"] = int(float(outcome.get("distance_sample_count") or 0.0))
        props["outcome_precision_boost_component"] = round(float(outcome.get("precision_boost_component") or 0.0), 4)
        props["outcome_precision_target_miles"] = round(float(outcome.get("precision_target_miles") or 0.12), 4)
        props["outcome_precision_span_miles"] = round(float(outcome.get("precision_span_miles") or 0.40), 4)
        props["outcome_precision_profile"] = str(outcome.get("precision_profile") or "hotspot_v1")
        props["outcome_support_strength"] = round(float(outcome.get("support_strength") or 0.0), 4)
        props["outcome_raw_modifier_before_support_damping"] = round(float(outcome.get("raw_modifier_before_support_damping") or 1.0), 4)
        props["outcome_recency_weight_version"] = str(outcome.get("recency_weight_version") or "")

        props["quality_modifier"] = round(quality_modifier, 4)
        props["short_trip_trap_penalty"] = round(float(quality.get("short_trip_trap_penalty") or 0.0), 4)
        props["trap_penalty"] = props["short_trip_trap_penalty"]
        props["continuation_bonus"] = round(float(quality.get("continuation_bonus") or 0.0), 4)
        props["saturation_penalty"] = round(float(quality.get("saturation_penalty") or 0.0), 4)

        props["live_modifier_version"] = "hybrid_v1"
        props["live_combined_modifier"] = round(live_combined_modifier, 4)
        props["intensity"] = round(final_intensity, 4)
        props["confidence"] = round(final_confidence, 4)

    return {
        "historical_anchor_point_count": historical_anchor_point_count,
        "historical_component_count": historical_component_count,
        "historical_strength": round(historical_strength, 4),
        "outcome_scope_used": str(((zone_features[0].get("properties") or {}).get("outcome_scope_used")) if zone_features else "zone_fallback"),
        "outcome_effective_sample_count": float(((zone_features[0].get("properties") or {}).get("outcome_effective_sample_count")) if zone_features else 0.0),
        "outcome_raw_conversion_rate": float(((zone_features[0].get("properties") or {}).get("outcome_raw_conversion_rate")) if zone_features else 0.0),
        "outcome_representative_minutes_to_trip": float(((zone_features[0].get("properties") or {}).get("outcome_representative_minutes_to_trip")) if zone_features else 0.0),
        "outcome_representative_distance_to_recommendation_miles": float(
            ((zone_features[0].get("properties") or {}).get("outcome_representative_distance_to_recommendation_miles"))
            if zone_features
            else 0.0
        ),
        "outcome_distance_sample_count": int(((zone_features[0].get("properties") or {}).get("outcome_distance_sample_count")) if zone_features else 0),
        "outcome_precision_boost_component": float(
            ((zone_features[0].get("properties") or {}).get("outcome_precision_boost_component")) if zone_features else 0.0
        ),
        "outcome_precision_target_miles": float(
            ((zone_features[0].get("properties") or {}).get("outcome_precision_target_miles")) if zone_features else 0.12
        ),
        "outcome_precision_span_miles": float(
            ((zone_features[0].get("properties") or {}).get("outcome_precision_span_miles")) if zone_features else 0.40
        ),
        "outcome_precision_profile": str(
            ((zone_features[0].get("properties") or {}).get("outcome_precision_profile")) if zone_features else "hotspot_v1"
        ),
        "outcome_support_strength": float(((zone_features[0].get("properties") or {}).get("outcome_support_strength")) if zone_features else 0.0),
        "outcome_raw_modifier_before_support_damping": float(
            ((zone_features[0].get("properties") or {}).get("outcome_raw_modifier_before_support_damping")) if zone_features else 1.0
        ),
        "outcome_recency_weight_version": str(
            ((zone_features[0].get("properties") or {}).get("outcome_recency_weight_version")) if zone_features else "resolved_recency_v1"
        ),
        "hotspot_specific_outcome_sample_count": int(((zone_features[0].get("properties") or {}).get("hotspot_specific_outcome_sample_count")) if zone_features else 0),
        "zone_fallback_outcome_sample_count": int(((zone_features[0].get("properties") or {}).get("zone_fallback_outcome_sample_count")) if zone_features else 0),
    }


def _refine_emitted_zone_hotspot_geometries(
    zone_id: int,
    zone_data: Dict[str, Any],
    zone_features: List[Dict[str, Any]],
    pts: List[Dict[str, Any]],
    now_ts: int,
) -> Dict[str, Any]:
    reason_counts: Dict[str, int] = defaultdict(int)
    debug_out: Dict[str, Any] = {
        "geometry_refinement_attempted": False,
        "geometry_refinement_applied_count": 0,
        "geometry_refinement_rejected_count": 0,
        "refinement_rejected_reason_counts": {},
        "historical_component_count_for_refinement": 0,
        "refined_hotspot_ids": [],
        "rejected_refinement_hotspot_ids": [],
    }
    if not zone_features:
        return debug_out

    for feature in zone_features:
        props = feature.setdefault("properties", {})
        props["geometry_refinement_version"] = "recent_shape_v1"
        props["geometry_refined"] = False
        props["geometry_refinement_method"] = "recent_shape_sculpt"
        props["recent_shape_component"] = 0.0
        props["geometry_refinement_overlap_ratio"] = 0.0
        props["geometry_refinement_centroid_shift_miles"] = 0.0
        props["geometry_refinement_area_ratio"] = 0.0

    debug_out["geometry_refinement_attempted"] = True
    zone_geom = (zone_data or {}).get("geometry")
    if zone_geom is None or getattr(zone_geom, "is_empty", True):
        for feature in zone_features:
            hotspot_id = str((feature.get("properties") or {}).get("hotspot_id") or "")
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
        reason_counts["zone_geometry_missing"] += len(zone_features)
        debug_out["geometry_refinement_rejected_count"] = len(zone_features)
        debug_out["refinement_rejected_reason_counts"] = dict(reason_counts)
        return debug_out

    try:
        zone_proj = transform(_to_3857.transform, zone_geom)
    except Exception:
        zone_proj = None
    if zone_proj is None or getattr(zone_proj, "is_empty", True):
        for feature in zone_features:
            hotspot_id = str((feature.get("properties") or {}).get("hotspot_id") or "")
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
        reason_counts["zone_projection_failed"] += len(zone_features)
        debug_out["geometry_refinement_rejected_count"] = len(zone_features)
        debug_out["refinement_rejected_reason_counts"] = dict(reason_counts)
        return debug_out

    sculpted_candidates: List[Dict[str, Any]] = []
    try:
        long_run_rows = _pickup_zone_long_run_points(zone_id, limit=2400)
        historical_anchor_points = build_zone_historical_anchor_points(
            pickup_rows=long_run_rows,
            frame_time=now_ts,
        )
        historical_components = build_zone_historical_anchor_components(
            zone_id=zone_id,
            zone_geom=zone_geom,
            weighted_points=historical_anchor_points,
        )
        debug_out["historical_component_count_for_refinement"] = len(historical_components)
        sculpted_candidates = sculpt_hotspot_shapes_from_recent_points(
            historical_components=historical_components,
            recent_points=pts,
            zone_geom=zone_geom,
            frame_time=now_ts,
        )
    except Exception:
        reason_counts["sculpt_generation_failed"] += len(zone_features)
        for feature in zone_features:
            hotspot_id = str((feature.get("properties") or {}).get("hotspot_id") or "")
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
        debug_out["geometry_refinement_rejected_count"] = len(zone_features)
        debug_out["refinement_rejected_reason_counts"] = dict(reason_counts)
        print(f"[warn] Failed hotspot geometry refinement for zone {zone_id}", traceback.format_exc())
        return debug_out

    applied_count = 0
    rejected_count = 0
    for fallback_idx, feature in enumerate(zone_features):
        props = feature.setdefault("properties", {})
        hotspot_id = str(props.get("hotspot_id") or "")
        hotspot_index = props.get("hotspot_index")
        try:
            idx = int(hotspot_index)
        except Exception:
            idx = int(fallback_idx)

        if idx < 0 or idx >= len(sculpted_candidates):
            rejected_count += 1
            reason_counts["missing_sculpted_candidate"] += 1
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
            continue

        candidate = sculpted_candidates[idx]
        sculpted_proj = candidate.get("polygon_proj")
        if sculpted_proj is None or getattr(sculpted_proj, "is_empty", True):
            rejected_count += 1
            reason_counts["sculpted_geometry_empty"] += 1
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
            continue

        try:
            original_geom = shape(feature.get("geometry"))
            original_proj = transform(_to_3857.transform, original_geom).intersection(zone_proj)
        except Exception:
            original_proj = None
        if original_proj is None or getattr(original_proj, "is_empty", True):
            rejected_count += 1
            reason_counts["original_geometry_empty"] += 1
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
            continue

        sculpted_proj = sculpted_proj.intersection(zone_proj)
        if sculpted_proj.is_empty:
            rejected_count += 1
            reason_counts["sculpted_outside_zone"] += 1
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
            continue
        if not zone_proj.buffer(0.75).covers(sculpted_proj):
            rejected_count += 1
            reason_counts["sculpted_not_inside_zone"] += 1
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
            continue

        centroid_shift_miles = float(original_proj.centroid.distance(sculpted_proj.centroid)) / 1609.344
        if centroid_shift_miles > 0.28:
            rejected_count += 1
            reason_counts["centroid_shift_too_far"] += 1
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
            continue

        original_area = max(1.0, float(original_proj.area))
        sculpted_area = max(1.0, float(sculpted_proj.area))
        area_ratio = sculpted_area / original_area
        if area_ratio < 0.40 or area_ratio > 2.50:
            rejected_count += 1
            reason_counts["area_ratio_out_of_range"] += 1
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
            continue

        inter = original_proj.intersection(sculpted_proj)
        union = original_proj.union(sculpted_proj)
        overlap_ratio = 0.0
        if not union.is_empty and union.area > 0:
            overlap_ratio = float(inter.area) / float(union.area)
        if overlap_ratio < 0.18:
            rejected_count += 1
            reason_counts["overlap_ratio_too_low"] += 1
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
            continue

        refined_ll = transform(_to_4326.transform, sculpted_proj)
        if refined_ll.is_empty:
            rejected_count += 1
            reason_counts["refined_projection_empty"] += 1
            if hotspot_id:
                debug_out["rejected_refinement_hotspot_ids"].append(hotspot_id)
            continue

        feature["geometry"] = mapping(refined_ll)
        props["geometry_refined"] = True
        props["recent_shape_component"] = round(float(candidate.get("recent_shape_component") or 0.0), 4)
        props["geometry_refinement_overlap_ratio"] = round(overlap_ratio, 4)
        props["geometry_refinement_centroid_shift_miles"] = round(centroid_shift_miles, 4)
        props["geometry_refinement_area_ratio"] = round(area_ratio, 4)
        applied_count += 1
        if hotspot_id:
            debug_out["refined_hotspot_ids"].append(hotspot_id)

    debug_out["geometry_refinement_applied_count"] = applied_count
    debug_out["geometry_refinement_rejected_count"] = rejected_count
    debug_out["refinement_rejected_reason_counts"] = dict(reason_counts)
    return debug_out


def _metric_from_feature_props(feature: Dict[str, Any], *keys: str, default: float = 0.0) -> float:
    props = (feature or {}).get("properties") or {}
    for key in keys:
        if key not in props:
            continue
        try:
            return float(props.get(key))
        except Exception:
            continue
    return float(default)


def _build_cross_zone_merged_hotspot_feature(
    merge_result: Dict[str, Any],
    feature_a: Dict[str, Any],
    feature_b: Dict[str, Any],
) -> Dict[str, Any]:
    props_a = (feature_a or {}).get("properties") or {}
    props_b = (feature_b or {}).get("properties") or {}
    merged_zone_ids = sorted(int(z) for z in (merge_result.get("merged_zone_ids") or []) if z is not None)
    merged_zone_names = [str(name or "") for name in (merge_result.get("merged_zone_names") or [])]
    if len(merged_zone_names) != len(merged_zone_ids):
        merged_zone_names = [str((zone_name or "")) for zone_name in (props_a.get("covered_zone_names") or [])][: len(merged_zone_ids)]
    primary_zone_id = int(merge_result.get("primary_zone_id") or (merged_zone_ids[0] if merged_zone_ids else 0))
    if len(merged_zone_ids) >= 2:
        small_zone_id, large_zone_id = sorted(merged_zone_ids[:2])
    else:
        small_zone_id = primary_zone_id
        large_zone_id = primary_zone_id
    hotspot_id_a = str(props_a.get("hotspot_id") or "a")
    hotspot_id_b = str(props_b.get("hotspot_id") or "b")
    merged_hotspot_id = f"merged:{small_zone_id}:{large_zone_id}:{hotspot_id_a}:{hotspot_id_b}"

    sample_size = int(_metric_from_feature_props(feature_a, "sample_size", default=0.0) + _metric_from_feature_props(feature_b, "sample_size", default=0.0))
    component_point_count = int(
        _metric_from_feature_props(feature_a, "component_point_count", default=0.0)
        + _metric_from_feature_props(feature_b, "component_point_count", default=0.0)
    )
    latest_created_at = int(
        max(
            _metric_from_feature_props(feature_a, "latest_created_at", default=0.0),
            _metric_from_feature_props(feature_b, "latest_created_at", default=0.0),
        )
    )

    signature_a = str(props_a.get("signature") or "")
    signature_b = str(props_b.get("signature") or "")
    combined_signature = f"merge:{small_zone_id}:{large_zone_id}:{signature_a}:{signature_b}"

    merged_micro: List[Dict[str, Any]] = []
    for source in (props_a.get("micro_hotspots") or [], props_b.get("micro_hotspots") or []):
        if not isinstance(source, list):
            continue
        for item in source:
            if not isinstance(item, dict):
                continue
            micro = copy.deepcopy(item)
            micro["hotspot_id"] = merged_hotspot_id
            micro["hotspot_index"] = 0
            merged_micro.append(micro)
            if len(merged_micro) >= 2:
                break
        if len(merged_micro) >= 2:
            break

    zone_label = merged_zone_names[0] if merged_zone_names else f"Merged {small_zone_id}/{large_zone_id}"
    merged_geometry = merge_result.get("geometry")
    if isinstance(merged_geometry, dict):
        merged_geometry_payload = merged_geometry
    else:
        merged_geometry_payload = mapping(merged_geometry)

    merged_feature = {
        "type": "Feature",
        "geometry": merged_geometry_payload,
        "properties": {
            "zone_id": primary_zone_id,
            "zone_name": zone_label,
            "borough": str(merge_result.get("borough") or props_a.get("borough") or props_b.get("borough") or ""),
            "hotspot_index": 0,
            "hotspot_id": merged_hotspot_id,
            "hotspot_method": "cross_zone_merge",
            "merged": True,
            "merged_zone_count": len(merged_zone_ids) or 2,
            "merged_zone_ids": merged_zone_ids,
            "merged_zone_names": merged_zone_names,
            "covered_zone_ids": merged_zone_ids,
            "covered_zone_names": merged_zone_names,
            "sample_size": sample_size,
            "component_point_count": component_point_count,
            "latest_created_at": latest_created_at,
            "signature": combined_signature,
            "merged_from_count": 2,
            "intensity": max(_metric_from_feature_props(feature_a, "intensity"), _metric_from_feature_props(feature_b, "intensity")),
            "confidence": max(_metric_from_feature_props(feature_a, "confidence"), _metric_from_feature_props(feature_b, "confidence")),
            "historical_strength": max(
                _metric_from_feature_props(feature_a, "historical_strength"),
                _metric_from_feature_props(feature_b, "historical_strength"),
            ),
            "outcome_modifier": max(
                _metric_from_feature_props(feature_a, "outcome_modifier", default=1.0),
                _metric_from_feature_props(feature_b, "outcome_modifier", default=1.0),
            ),
            "quality_modifier": (
                _metric_from_feature_props(feature_a, "quality_modifier", default=1.0)
                + _metric_from_feature_props(feature_b, "quality_modifier", default=1.0)
            )
            / 2.0,
            "live_combined_modifier": max(
                _metric_from_feature_props(feature_a, "live_combined_modifier", default=1.0),
                _metric_from_feature_props(feature_b, "live_combined_modifier", default=1.0),
            ),
            "recommended": bool(props_a.get("recommended")) or bool(props_b.get("recommended")),
            "micro_hotspots": merged_micro,
            "merged_learning_version": "merged_hotspot_v1",
        },
    }
    merged_props = merged_feature.setdefault("properties", {})
    merged_base_intensity = float(merged_props.get("intensity") or 0.55)
    merged_base_confidence = float(merged_props.get("confidence") or 0.55)
    merged_learning_payload = _compute_merged_hotspot_learning_payload(merged_feature, feature_a, feature_b)

    merged_props["outcome_modifier"] = round(float(merged_learning_payload.get("outcome_modifier") or 1.0), 4)
    merged_props["outcome_sample_count"] = int(merged_learning_payload.get("outcome_sample_count") or 0)
    merged_props["outcome_scope_used"] = str(merged_learning_payload.get("outcome_scope_used") or "merged_fallback_from_children")
    merged_props["outcome_conversion_rate"] = round(float(merged_learning_payload.get("outcome_conversion_rate") or 0.0), 4)
    merged_props["outcome_median_minutes_to_trip"] = round(float(merged_learning_payload.get("outcome_median_minutes_to_trip") or 0.0), 4)
    merged_props["outcome_effective_sample_count"] = round(float(merged_learning_payload.get("outcome_effective_sample_count") or 0.0), 4)
    merged_props["outcome_raw_conversion_rate"] = round(float(merged_learning_payload.get("outcome_raw_conversion_rate") or 0.0), 4)
    merged_props["outcome_representative_minutes_to_trip"] = round(float(merged_learning_payload.get("outcome_representative_minutes_to_trip") or 0.0), 4)
    merged_props["outcome_representative_distance_to_recommendation_miles"] = round(
        float(merged_learning_payload.get("outcome_representative_distance_to_recommendation_miles") or 0.0),
        4,
    )
    merged_props["outcome_distance_sample_count"] = int(merged_learning_payload.get("outcome_distance_sample_count") or 0)
    merged_props["outcome_precision_boost_component"] = round(float(merged_learning_payload.get("outcome_precision_boost_component") or 0.0), 4)
    merged_props["outcome_precision_target_miles"] = round(float(merged_learning_payload.get("outcome_precision_target_miles") or 0.12), 4)
    merged_props["outcome_precision_span_miles"] = round(float(merged_learning_payload.get("outcome_precision_span_miles") or 0.40), 4)
    merged_props["outcome_precision_profile"] = str(merged_learning_payload.get("outcome_precision_profile") or "merged_hotspot_v1")
    merged_props["outcome_support_strength"] = round(float(merged_learning_payload.get("outcome_support_strength") or 0.0), 4)
    merged_props["outcome_raw_modifier_before_support_damping"] = round(
        float(merged_learning_payload.get("outcome_raw_modifier_before_support_damping") or 1.0),
        4,
    )
    merged_props["outcome_recency_weight_version"] = str(merged_learning_payload.get("outcome_recency_weight_version") or "")
    merged_props["live_combined_modifier"] = round(float(merged_learning_payload.get("live_combined_modifier") or 1.0), 4)
    merged_props["quality_modifier"] = round(float(merged_learning_payload.get("quality_modifier") or 1.0), 4)

    merged_props["merged_outcome_scope_used"] = str(merged_learning_payload.get("merged_outcome_scope_used") or "merged_fallback_from_children")
    merged_props["merged_outcome_sample_count"] = int(merged_learning_payload.get("merged_outcome_sample_count") or 0)
    merged_props["merged_outcome_conversion_rate"] = round(float(merged_learning_payload.get("merged_outcome_conversion_rate") or 0.0), 4)
    merged_props["merged_outcome_median_minutes_to_trip"] = round(float(merged_learning_payload.get("merged_outcome_median_minutes_to_trip") or 0.0), 4)
    merged_props["merged_outcome_effective_sample_count"] = round(float(merged_learning_payload.get("merged_outcome_effective_sample_count") or 0.0), 4)
    merged_props["merged_outcome_raw_conversion_rate"] = round(float(merged_learning_payload.get("merged_outcome_raw_conversion_rate") or 0.0), 4)
    merged_props["merged_outcome_representative_minutes_to_trip"] = round(float(merged_learning_payload.get("merged_outcome_representative_minutes_to_trip") or 0.0), 4)
    merged_props["merged_outcome_representative_distance_to_recommendation_miles"] = round(
        float(merged_learning_payload.get("merged_outcome_representative_distance_to_recommendation_miles") or 0.0),
        4,
    )
    merged_props["merged_outcome_distance_sample_count"] = int(merged_learning_payload.get("merged_outcome_distance_sample_count") or 0)
    merged_props["merged_outcome_precision_boost_component"] = round(
        float(merged_learning_payload.get("merged_outcome_precision_boost_component") or 0.0),
        4,
    )
    merged_props["merged_outcome_precision_target_miles"] = round(
        float(merged_learning_payload.get("merged_outcome_precision_target_miles") or 0.12),
        4,
    )
    merged_props["merged_outcome_precision_span_miles"] = round(
        float(merged_learning_payload.get("merged_outcome_precision_span_miles") or 0.40),
        4,
    )
    merged_props["merged_outcome_precision_profile"] = str(
        merged_learning_payload.get("merged_outcome_precision_profile") or "merged_hotspot_v1"
    )
    merged_props["merged_outcome_support_strength"] = round(float(merged_learning_payload.get("merged_outcome_support_strength") or 0.0), 4)
    merged_props["merged_outcome_raw_modifier_before_support_damping"] = round(
        float(merged_learning_payload.get("merged_outcome_raw_modifier_before_support_damping") or 1.0),
        4,
    )
    merged_props["merged_outcome_recency_weight_version"] = str(merged_learning_payload.get("merged_outcome_recency_weight_version") or "")

    if bool(merged_learning_payload.get("used_merged_hotspot_specific")):
        merged_outcome_modifier = float(merged_learning_payload.get("outcome_modifier") or 1.0)
        merged_props["intensity"] = round(max(0.20, min(1.00, merged_base_intensity * merged_outcome_modifier)), 4)
        merged_props["confidence"] = round(max(0.20, min(0.98, merged_base_confidence * merged_outcome_modifier)), 4)

    for key in ("final_score", "hotspot_score"):
        if key in props_a or key in props_b:
            merged_feature["properties"][key] = max(
                _metric_from_feature_props(feature_a, key),
                _metric_from_feature_props(feature_b, key),
            )
    return merged_feature


def _apply_final_recommended_hotspot_ownership(
    clean_zone_ids: List[int],
    zone_scores: Dict[int, Any],
    zone_feature_map: Dict[int, List[Dict[str, Any]]],
    merged_features: List[Dict[str, Any]],
    consumed_zones: set[int],
    score_bundle_fresh: bool,
    now_ts: int,
    zone_debug_map: Dict[int, Dict[str, Any]],
) -> Dict[str, int]:
    merged_feature_by_zone_id: Dict[int, Dict[str, Any]] = {}
    for merged_feature in merged_features:
        props = merged_feature.setdefault("properties", {})
        merged_zone_ids = props.get("merged_zone_ids") or props.get("covered_zone_ids") or []
        for zone_id_raw in merged_zone_ids:
            try:
                merged_feature_by_zone_id[int(zone_id_raw)] = merged_feature
            except Exception:
                continue

    final_zone_feature_map: Dict[int, Optional[Dict[str, Any]]] = {}
    for zone_id in clean_zone_ids:
        if zone_id in consumed_zones:
            final_zone_feature_map[zone_id] = merged_feature_by_zone_id.get(zone_id)
            continue
        zone_features = zone_feature_map.get(zone_id) or []
        final_zone_feature_map[zone_id] = _choose_primary_recommended_hotspot_feature(zone_features)

    assignment_count = 0
    for zone_id in clean_zone_ids:
        zone_features = zone_feature_map.get(zone_id) or []
        for feature in zone_features:
            props = feature.setdefault("properties", {})
            props["recommended_hotspot"] = False
            if not str(props.get("recommendation_scope") or "").strip():
                props["recommendation_scope"] = "zone_secondary"
        zone_debug = zone_debug_map.get(zone_id)
        if zone_debug is not None:
            zone_debug["final_recommended_hotspot_id"] = None
            zone_debug["final_recommendation_scope"] = None
            zone_debug["recommendation_logged_post_merge"] = False
            zone_debug["recommendation_logged_cluster_id"] = None

    for zone_id in clean_zone_ids:
        score = zone_scores.get(zone_id)
        if score is None or not bool(getattr(score, "recommended", False)):
            continue

        consumed = zone_id in consumed_zones
        target_feature: Optional[Dict[str, Any]] = None
        if consumed:
            target_feature = merged_feature_by_zone_id.get(zone_id)
        else:
            target_feature = final_zone_feature_map.get(zone_id)
        if target_feature is None:
            continue

        target_props = target_feature.setdefault("properties", {})
        target_props["recommended_hotspot"] = True
        target_scope = "merged_hotspot" if consumed else "hotspot"
        target_props["recommendation_scope"] = target_scope

        recommended_zone_ids = target_props.get("recommended_zone_ids") or []
        normalized_zone_ids = {
            int(z)
            for z in recommended_zone_ids
            if isinstance(z, (int, float)) or (isinstance(z, str) and z.strip().isdigit())
        }
        normalized_zone_ids.add(int(zone_id))
        target_props["recommended_zone_ids"] = sorted(normalized_zone_ids)

        recommended_zone_names = [str(name or "").strip() for name in (target_props.get("recommended_zone_names") or []) if str(name or "").strip()]
        zone_name = ""
        if zone_features := (zone_feature_map.get(zone_id) or []):
            zone_name = str(((zone_features[0].get("properties") or {}).get("zone_name")) or "").strip()
        if zone_name and zone_name not in recommended_zone_names:
            recommended_zone_names.append(zone_name)
        target_props["recommended_zone_names"] = sorted(dict.fromkeys(recommended_zone_names))

        if consumed:
            target_props["final_recommendation_owner"] = "post_merge"
            if "recommended" in target_props:
                target_props["recommended"] = True

        assignment_count += 1
        final_hotspot_id = str(target_props.get("hotspot_id") or "") or None
        zone_debug = zone_debug_map.get(zone_id)
        if zone_debug is not None:
            zone_debug["primary_recommended_hotspot_id"] = final_hotspot_id
            zone_debug["final_recommended_hotspot_id"] = final_hotspot_id
            zone_debug["final_recommendation_scope"] = target_scope

    return {
        "post_merge_recommendation_assignment_count": assignment_count,
        "post_merge_logged_outcome_count": 0,
    }


def _apply_micro_hotspot_parent_recommendation_state(features: List[Dict[str, Any]]) -> Dict[str, int]:
    recommended_micro_hotspot_count = 0
    nonrecommended_micro_hotspot_count = 0

    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties") or {}
        hotspot_id = props.get("hotspot_id")
        hotspot_index = props.get("hotspot_index")
        hotspot_method = props.get("hotspot_method")
        parent_recommended = bool(props.get("recommended_hotspot"))
        parent_recommendation_scope = props.get("recommendation_scope")
        parent_merged = bool(props.get("merged"))
        parent_covered_zone_ids = copy.deepcopy(props.get("covered_zone_ids") or [])
        parent_covered_zone_names = copy.deepcopy(props.get("covered_zone_names") or [])

        micro_hotspots = props.get("micro_hotspots")
        if not isinstance(micro_hotspots, list):
            continue

        for micro_rank, micro in enumerate(micro_hotspots):
            if not isinstance(micro, dict):
                continue
            micro["hotspot_id"] = hotspot_id
            micro["hotspot_index"] = hotspot_index
            micro["recommended"] = parent_recommended
            micro["recommendation_scope"] = "micro_hotspot" if parent_recommended else "micro_secondary"
            micro["parent_hotspot_id"] = hotspot_id
            micro["parent_hotspot_method"] = hotspot_method
            micro["parent_recommendation_scope"] = parent_recommendation_scope
            micro["parent_merged"] = parent_merged
            micro["parent_covered_zone_ids"] = copy.deepcopy(parent_covered_zone_ids)
            micro["parent_covered_zone_names"] = copy.deepcopy(parent_covered_zone_names)
            micro["micro_rank"] = int(micro_rank)
            micro["micro_parent_feature_kind"] = "hotspot"
            micro["micro_owner_version"] = "parent_recommendation_v1"
            if parent_recommended:
                recommended_micro_hotspot_count += 1
            else:
                nonrecommended_micro_hotspot_count += 1

    return {
        "recommended_micro_hotspot_count": recommended_micro_hotspot_count,
        "nonrecommended_micro_hotspot_count": nonrecommended_micro_hotspot_count,
    }


def _choose_primary_recommended_micro_hotspot(micro_hotspots: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(micro_hotspots, list) or not micro_hotspots:
        return None
    ranked = sorted(
        [micro for micro in micro_hotspots if isinstance(micro, dict)],
        key=lambda micro: (
            -float(micro.get("intensity") or 0.0),
            -float(micro.get("confidence") or 0.0),
            -int(micro.get("event_count") or 0),
            int(micro.get("micro_rank") or 10_000),
        ),
    )
    return ranked[0] if ranked else None


def _feature_center_lat_lng(feature: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    try:
        geom_data = (feature or {}).get("geometry")
        if not geom_data:
            return (None, None)
        geom = shape(geom_data)
        if geom.is_empty:
            return (None, None)
        centroid = geom.centroid
        return (float(centroid.y), float(centroid.x))
    except Exception:
        return (None, None)


def _log_hotspot_recommendation_outcome(
    *,
    user_id: Optional[int],
    zone_id: int,
    cluster_id: Optional[str],
    score: float,
    confidence: float,
    recommended_at: int,
    hotspot_center_lat: Optional[float] = None,
    hotspot_center_lng: Optional[float] = None,
) -> None:
    cluster_key = str(cluster_id or "").strip() or None
    _db_exec(
        """
        INSERT INTO recommendation_outcomes(
          user_id, recommended_at, zone_id, cluster_id, hotspot_center_lat, hotspot_center_lng, score, confidence, converted_to_trip, minutes_to_trip
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
        """,
        (
            int(user_id) if user_id is not None else None,
            int(recommended_at),
            int(zone_id),
            cluster_key,
            float(hotspot_center_lat) if hotspot_center_lat is not None else None,
            float(hotspot_center_lng) if hotspot_center_lng is not None else None,
            float(score),
            float(confidence),
        ),
    )


def _log_micro_recommendation_outcome(
    *,
    user_id: Optional[int],
    zone_id: int,
    parent_hotspot_id: Optional[str],
    micro_cluster_id: Optional[str],
    score: float,
    confidence: float,
    recommended_at: int,
    micro_center_lat: Optional[float] = None,
    micro_center_lng: Optional[float] = None,
) -> None:
    cluster_key = str(micro_cluster_id or "").strip()
    if not cluster_key:
        return
    _db_exec(
        """
        INSERT INTO micro_recommendation_outcomes(
          user_id, recommended_at, zone_id, parent_hotspot_id, micro_cluster_id, micro_center_lat, micro_center_lng, score, confidence, converted_to_trip, minutes_to_trip
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
        """,
        (
            int(user_id) if user_id is not None else None,
            int(recommended_at),
            int(zone_id),
            str(parent_hotspot_id or "").strip() or None,
            cluster_key,
            float(micro_center_lat) if micro_center_lat is not None else None,
            float(micro_center_lng) if micro_center_lng is not None else None,
            float(score),
            float(confidence),
        ),
    )


def _apply_final_recommended_micro_hotspot_ownership(
    features: List[Dict[str, Any]],
    score_bundle_fresh: bool,
    now_ts: int,
    zone_debug_map: Dict[int, Dict[str, Any]],
) -> Dict[str, int]:
    recommended_primary_micro_hotspot_count = 0
    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.setdefault("properties", {})
        parent_recommended = bool(props.get("recommended_hotspot"))
        parent_hotspot_id = str(props.get("hotspot_id") or "").strip() or None
        micro_hotspots = props.get("micro_hotspots")
        if not isinstance(micro_hotspots, list):
            continue

        for micro in micro_hotspots:
            if not isinstance(micro, dict):
                continue
            micro["recommended"] = False
            micro["recommended_micro_hotspot"] = False
            micro["recommendation_scope"] = "micro_secondary"

        if not parent_recommended:
            continue

        primary_micro = _choose_primary_recommended_micro_hotspot(micro_hotspots)
        if not isinstance(primary_micro, dict):
            continue
        primary_micro["recommended"] = True
        primary_micro["recommended_micro_hotspot"] = True
        primary_micro["recommendation_scope"] = "micro_hotspot_primary"
        recommended_primary_micro_hotspot_count += 1

        covered_zone_ids = props.get("covered_zone_ids") or [props.get("zone_id")]
        for raw_zone_id in covered_zone_ids:
            try:
                zone_id = int(raw_zone_id)
            except Exception:
                continue
            zone_debug = zone_debug_map.get(zone_id)
            if zone_debug is None:
                continue
            zone_debug["recommended_primary_micro_hotspot_count"] = int(zone_debug.get("recommended_primary_micro_hotspot_count") or 0) + 1

    return {
        "recommended_primary_micro_hotspot_count": recommended_primary_micro_hotspot_count,
        "logged_primary_micro_outcome_count": 0,
    }


def _recent_micro_recommendation_outcomes(zone_id: int, micro_cluster_id: str, max_rows: int = 80) -> List[Dict[str, Any]]:
    cutoff = int(time.time()) - (30 * 24 * 3600)
    rows = _db_query_all(
        """
        SELECT converted_to_trip, minutes_to_trip, recommended_at, distance_to_recommendation_miles
        FROM micro_recommendation_outcomes
        WHERE zone_id = ?
          AND micro_cluster_id = ?
          AND converted_to_trip IS NOT NULL
          AND recommended_at >= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT ?
        """,
        (int(zone_id), str(micro_cluster_id), int(cutoff), int(max_rows)),
    )
    return [dict(r) for r in rows]


def _viewer_recent_hotspot_impression_exists(
    user_id: int,
    zone_id: int,
    hotspot_id: Optional[str],
    now_ts: int,
    cooldown_sec: int = 480,
) -> bool:
    cluster_key = str(hotspot_id or "").strip() or None
    if not cluster_key:
        return False
    cutoff = int(now_ts) - max(1, int(cooldown_sec))
    row = _db_query_one(
        """
        SELECT id
        FROM recommendation_outcomes
        WHERE user_id = ?
          AND zone_id = ?
          AND cluster_id = ?
          AND recommended_at >= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT 1
        """,
        (int(user_id), int(zone_id), cluster_key, cutoff),
    )
    return row is not None


def _viewer_recent_micro_impression_exists(
    user_id: int,
    zone_id: int,
    micro_cluster_id: Optional[str],
    now_ts: int,
    cooldown_sec: int = 480,
) -> bool:
    cluster_key = str(micro_cluster_id or "").strip() or None
    if not cluster_key:
        return False
    cutoff = int(now_ts) - max(1, int(cooldown_sec))
    row = _db_query_one(
        """
        SELECT id
        FROM micro_recommendation_outcomes
        WHERE user_id = ?
          AND zone_id = ?
          AND micro_cluster_id = ?
          AND recommended_at >= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT 1
        """,
        (int(user_id), int(zone_id), cluster_key, cutoff),
    )
    return row is not None


def _log_viewer_recommendation_impressions_from_pickup_payload(
    payload: Dict[str, Any],
    viewer_user_id: int,
    now_ts: int,
) -> Dict[str, int]:
    counts = {
        "viewer_logged_hotspot_impressions": 0,
        "viewer_logged_micro_impressions": 0,
        "viewer_deduped_hotspot_impressions": 0,
        "viewer_deduped_micro_impressions": 0,
        "viewer_logged_hotspot_impressions_with_centers": 0,
        "viewer_logged_micro_impressions_with_centers": 0,
    }
    features = (((payload or {}).get("zone_hotspots") or {}).get("features") or [])
    if not isinstance(features, list):
        return counts

    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties") or {}
        if not bool(props.get("recommended_hotspot")):
            continue
        hotspot_id = str(props.get("hotspot_id") or "").strip() or None
        if not hotspot_id:
            continue

        zone_candidates = props.get("recommended_zone_ids")
        if not isinstance(zone_candidates, list) or not zone_candidates:
            zone_candidates = [props.get("zone_id")]
        zone_ids: List[int] = []
        for raw_zone_id in zone_candidates:
            try:
                zone_id = int(raw_zone_id)
            except Exception:
                continue
            if zone_id not in zone_ids:
                zone_ids.append(zone_id)
        if not zone_ids:
            continue

        score_val = float(props.get("final_score") or props.get("hotspot_score") or 0.0)
        confidence_val = float(props.get("confidence") or 0.0)
        hotspot_center_lat, hotspot_center_lng = _feature_center_lat_lng(feature)
        micro_hotspots = props.get("micro_hotspots") or []
        for zone_id in zone_ids:
            try:
                if _viewer_recent_hotspot_impression_exists(
                    user_id=viewer_user_id,
                    zone_id=zone_id,
                    hotspot_id=hotspot_id,
                    now_ts=now_ts,
                ):
                    counts["viewer_deduped_hotspot_impressions"] += 1
                else:
                    _log_hotspot_recommendation_outcome(
                        recommended_at=now_ts,
                        user_id=viewer_user_id,
                        zone_id=zone_id,
                        cluster_id=hotspot_id,
                        score=score_val,
                        confidence=confidence_val,
                        hotspot_center_lat=hotspot_center_lat,
                        hotspot_center_lng=hotspot_center_lng,
                    )
                    counts["viewer_logged_hotspot_impressions"] += 1
                    if hotspot_center_lat is not None and hotspot_center_lng is not None:
                        counts["viewer_logged_hotspot_impressions_with_centers"] += 1
            except Exception:
                print(f"[warn] Failed viewer hotspot impression logging for zone={zone_id}", traceback.format_exc())

            for micro in micro_hotspots:
                if not isinstance(micro, dict) or not bool(micro.get("recommended_micro_hotspot")):
                    continue
                micro_cluster_id = str(micro.get("cluster_id") or "").strip() or None
                if not micro_cluster_id:
                    continue
                center_lat: Optional[float] = None
                center_lng: Optional[float] = None
                try:
                    if micro.get("center_lat") is not None and micro.get("center_lng") is not None:
                        center_lat = float(micro.get("center_lat"))
                        center_lng = float(micro.get("center_lng"))
                    elif isinstance(micro.get("center"), dict):
                        center = micro.get("center") or {}
                        center_lat = float(center.get("lat")) if center.get("lat") is not None else None
                        center_lng = float(center.get("lng")) if center.get("lng") is not None else None
                except Exception:
                    center_lat = None
                    center_lng = None
                try:
                    if _viewer_recent_micro_impression_exists(
                        user_id=viewer_user_id,
                        zone_id=zone_id,
                        micro_cluster_id=micro_cluster_id,
                        now_ts=now_ts,
                    ):
                        counts["viewer_deduped_micro_impressions"] += 1
                        continue
                    _log_micro_recommendation_outcome(
                        user_id=viewer_user_id,
                        zone_id=zone_id,
                        parent_hotspot_id=hotspot_id,
                        micro_cluster_id=micro_cluster_id,
                        score=score_val,
                        confidence=float(micro.get("confidence") or confidence_val),
                        recommended_at=now_ts,
                        micro_center_lat=center_lat,
                        micro_center_lng=center_lng,
                    )
                    counts["viewer_logged_micro_impressions"] += 1
                    if center_lat is not None and center_lng is not None:
                        counts["viewer_logged_micro_impressions_with_centers"] += 1
                except Exception:
                    print(f"[warn] Failed viewer micro impression logging for zone={zone_id}", traceback.format_exc())
    return counts


def _apply_micro_hotspot_learning(
    features: List[Dict[str, Any]],
    zone_debug_map: Dict[int, Dict[str, Any]],
) -> Dict[str, int]:
    micro_hotspot_specific_learning_count = 0
    micro_hotspot_parent_fallback_learning_count = 0
    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.setdefault("properties", {})
        parent_outcome_modifier = float(props.get("outcome_modifier") or 1.0)
        parent_zone_id = int(props.get("zone_id") or 0)
        micro_hotspots = props.get("micro_hotspots")
        if not isinstance(micro_hotspots, list):
            continue

        for micro in micro_hotspots:
            if not isinstance(micro, dict):
                continue
            base_intensity = float(micro.get("intensity") or 0.55)
            base_confidence = float(micro.get("confidence") or 0.55)
            micro["micro_learning_version"] = "micro_hotspot_v1"
            micro["micro_outcome_conversion_rate"] = 0.0
            micro["micro_outcome_median_minutes_to_trip"] = 0.0
            micro["micro_outcome_effective_sample_count"] = 0.0
            micro["micro_outcome_raw_conversion_rate"] = 0.0
            micro["micro_outcome_representative_minutes_to_trip"] = 0.0
            micro["micro_outcome_representative_distance_to_recommendation_miles"] = 0.0
            micro["micro_outcome_distance_sample_count"] = 0
            micro["micro_outcome_precision_boost_component"] = 0.0
            micro["micro_outcome_precision_target_miles"] = 0.05
            micro["micro_outcome_precision_span_miles"] = 0.20
            micro["micro_outcome_precision_profile"] = "micro_hotspot_v1"
            micro["micro_outcome_support_strength"] = 0.0
            micro["micro_outcome_raw_modifier_before_support_damping"] = round(parent_outcome_modifier, 4)
            micro["micro_outcome_recency_weight_version"] = "resolved_recency_v1"
            if not bool(micro.get("recommended_micro_hotspot")):
                micro["micro_outcome_modifier"] = round(parent_outcome_modifier, 4)
                micro["micro_outcome_sample_count"] = 0
                micro["micro_outcome_scope_used"] = "micro_not_recommended"
                continue

            zone_id = int(micro.get("zone_id") or parent_zone_id)
            micro_cluster_id = str(micro.get("cluster_id") or "").strip()
            rows = _recent_micro_recommendation_outcomes(zone_id, micro_cluster_id) if micro_cluster_id else []
            if len(rows) >= 6:
                outcome = get_zone_or_hotspot_outcome_modifier(
                    rows,
                    precision_target_miles=0.05,
                    precision_span_miles=0.20,
                    precision_profile="micro_hotspot_v1",
                )
                micro_modifier = float(outcome.get("modifier") or 1.0)
                micro["micro_outcome_modifier"] = round(micro_modifier, 4)
                micro["micro_outcome_sample_count"] = int(float(outcome.get("sample_count") or 0.0))
                micro["micro_outcome_conversion_rate"] = round(float(outcome.get("conversion_rate") or 0.0), 4)
                micro["micro_outcome_median_minutes_to_trip"] = round(float(outcome.get("median_minutes_to_trip") or 0.0), 4)
                micro["micro_outcome_effective_sample_count"] = round(float(outcome.get("effective_sample_count") or 0.0), 4)
                micro["micro_outcome_raw_conversion_rate"] = round(float(outcome.get("raw_conversion_rate") or 0.0), 4)
                micro["micro_outcome_representative_minutes_to_trip"] = round(float(outcome.get("representative_minutes_to_trip") or 0.0), 4)
                micro["micro_outcome_representative_distance_to_recommendation_miles"] = round(
                    float(outcome.get("representative_distance_to_recommendation_miles") or 0.0),
                    4,
                )
                micro["micro_outcome_distance_sample_count"] = int(float(outcome.get("distance_sample_count") or 0.0))
                micro["micro_outcome_precision_boost_component"] = round(float(outcome.get("precision_boost_component") or 0.0), 4)
                micro["micro_outcome_precision_target_miles"] = round(float(outcome.get("precision_target_miles") or 0.05), 4)
                micro["micro_outcome_precision_span_miles"] = round(float(outcome.get("precision_span_miles") or 0.20), 4)
                micro["micro_outcome_precision_profile"] = str(outcome.get("precision_profile") or "micro_hotspot_v1")
                micro["micro_outcome_support_strength"] = round(float(outcome.get("support_strength") or 0.0), 4)
                micro["micro_outcome_raw_modifier_before_support_damping"] = round(
                    float(outcome.get("raw_modifier_before_support_damping") or 1.0),
                    4,
                )
                micro["micro_outcome_recency_weight_version"] = str(outcome.get("recency_weight_version") or "")
                micro["micro_outcome_scope_used"] = "micro_hotspot_specific"
                micro["intensity"] = round(max(0.20, min(1.00, base_intensity * micro_modifier)), 4)
                micro["confidence"] = round(max(0.20, min(0.98, base_confidence * micro_modifier)), 4)
                micro_hotspot_specific_learning_count += 1
            else:
                micro["micro_outcome_scope_used"] = "micro_parent_fallback"
                micro["micro_outcome_modifier"] = round(parent_outcome_modifier, 4)
                micro["micro_outcome_sample_count"] = len(rows)
                micro["micro_outcome_effective_sample_count"] = 0.0
                micro["micro_outcome_raw_conversion_rate"] = 0.0
                micro["micro_outcome_representative_minutes_to_trip"] = 0.0
                micro["micro_outcome_representative_distance_to_recommendation_miles"] = 0.0
                micro["micro_outcome_distance_sample_count"] = 0
                micro["micro_outcome_precision_boost_component"] = 0.0
                micro["micro_outcome_precision_target_miles"] = 0.05
                micro["micro_outcome_precision_span_miles"] = 0.20
                micro["micro_outcome_precision_profile"] = "micro_hotspot_v1"
                micro["micro_outcome_support_strength"] = 0.0
                micro["micro_outcome_raw_modifier_before_support_damping"] = round(parent_outcome_modifier, 4)
                micro["micro_outcome_recency_weight_version"] = "resolved_recency_v1"
                micro["intensity"] = round(max(0.20, min(1.00, base_intensity * parent_outcome_modifier)), 4)
                micro["confidence"] = round(max(0.20, min(0.98, base_confidence * parent_outcome_modifier)), 4)
                micro_hotspot_parent_fallback_learning_count += 1

                zone_debug = zone_debug_map.get(zone_id)
                if zone_debug is not None:
                    zone_debug["micro_hotspot_parent_fallback_learning_count"] = int(
                        zone_debug.get("micro_hotspot_parent_fallback_learning_count") or 0
                    ) + 1

    return {
        "micro_hotspot_specific_learning_count": micro_hotspot_specific_learning_count,
        "micro_hotspot_parent_fallback_learning_count": micro_hotspot_parent_fallback_learning_count,
    }


def _build_micro_experiment_rows_from_features(features: List[Dict[str, Any]]) -> List[MicroHotspotScoreResult]:
    deduped_rows: Dict[str, MicroHotspotScoreResult] = {}
    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties") or {}
        parent_zone_id_raw = props.get("zone_id")
        baseline_component = float(props.get("final_score") or props.get("hotspot_score") or 0.0)
        same_timeslot_component = float(props.get("outcome_conversion_rate") or 0.0)
        micro_hotspots = props.get("micro_hotspots")
        if not isinstance(micro_hotspots, list):
            continue
        for micro in micro_hotspots:
            if not isinstance(micro, dict):
                continue
            cluster_id = str(micro.get("cluster_id") or "").strip()
            if not cluster_id:
                continue
            try:
                zone_id = int(micro.get("zone_id") if micro.get("zone_id") is not None else parent_zone_id_raw)
            except Exception:
                continue
            try:
                center_lat = float(micro.get("center_lat"))
                center_lng = float(micro.get("center_lng"))
            except Exception:
                continue
            if (
                not math.isfinite(center_lat)
                or not math.isfinite(center_lng)
                or center_lat < -90.0
                or center_lat > 90.0
                or center_lng < -180.0
                or center_lng > 180.0
            ):
                continue
            radius_m = float(micro.get("radius_m") or 42.0)
            intensity = float(micro.get("intensity") or 0.0)
            confidence = float(micro.get("confidence") or 0.0)
            weighted_trip_count = float(micro.get("event_count") or 0.0)
            unique_driver_count = int(micro.get("event_count") or 0)
            live_component = intensity
            final_score = max(0.0, min(1.0, (0.55 * live_component) + (0.35 * confidence) + (0.10 * baseline_component)))
            row = MicroHotspotScoreResult(
                cluster_id=cluster_id,
                zone_id=zone_id,
                center_lat=center_lat,
                center_lng=center_lng,
                radius_m=radius_m,
                intensity=intensity,
                confidence=confidence,
                weighted_trip_count=weighted_trip_count,
                unique_driver_count=unique_driver_count,
                crowding_penalty=0.0,
                baseline_component=baseline_component,
                live_component=live_component,
                same_timeslot_component=same_timeslot_component,
                final_score=float(final_score),
                recommended=bool(micro.get("recommended_micro_hotspot", micro.get("recommended"))),
            )
            existing = deduped_rows.get(cluster_id)
            if existing is None or (row.final_score, row.confidence) > (existing.final_score, existing.confidence):
                deduped_rows[cluster_id] = row
    return list(deduped_rows.values())


def _pickup_zone_hotspots_with_debug(
    zone_ids: List[int],
    include_debug: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    empty = {"type": "FeatureCollection", "features": []}
    clean_zone_ids: List[int] = []
    for z in zone_ids:
        try:
            clean_zone_ids.append(int(z))
        except Exception:
            continue

    debug: Dict[str, Any] = {
        "min_points_threshold": PICKUP_ZONE_HOTSPOT_MIN_POINTS,
        "qualification_rule": "point_count>=min_points_threshold",
        "requested_zone_ids": clean_zone_ids,
        "zone_hotspot_count": 0,
        "orphan_micro_hotspot_count": 0,
        "top_level_micro_hotspot_count": 0,
        "cross_zone_merge_candidates": 0,
        "cross_zone_merge_applied_count": 0,
        "merged_zone_pairs": [],
        "post_merge_recommendation_assignment_count": 0,
        "post_merge_logged_outcome_count": 0,
        "merged_hotspot_specific_learning_count": 0,
        "merged_hotspot_fallback_learning_count": 0,
        "distance_aware_hotspot_learning_count": 0,
        "distance_aware_merged_hotspot_learning_count": 0,
        "distance_aware_micro_hotspot_learning_count": 0,
        "recommended_micro_hotspot_count": 0,
        "nonrecommended_micro_hotspot_count": 0,
        "recommended_primary_micro_hotspot_count": 0,
        "logged_primary_micro_outcome_count": 0,
        "micro_hotspot_specific_learning_count": 0,
        "micro_hotspot_parent_fallback_learning_count": 0,
        "micro_experiment_rows_built": 0,
        "micro_experiment_rows_logged": 0,
        "micro_experiment_logging_failed": False,
        "settled_stale_hotspot_outcomes": 0,
        "settled_stale_micro_outcomes": 0,
        "outcome_learning_resolved_only": True,
        "outcome_learning_recency_weighted": True,
        "outcome_recency_weight_version": "resolved_recency_v1",
    }
    if include_debug:
        debug.update(
            {
                "qualified_zone_ids": [],
                "rendered_zone_ids": [],
                "global_errors": [],
                "zones": [],
            }
        )
    if not clean_zone_ids:
        return empty, debug

    now_ts = int(time.time())
    now_monotonic = time.monotonic()
    settlement_counts = _maybe_settle_stale_recommendation_outcomes(now_ts)
    debug.update(settlement_counts)
    zone_geoms: Dict[int, Dict[str, Any]] = {}
    try:
        zone_geoms = _load_pickup_zone_geometries()
    except Exception:
        if include_debug:
            debug["global_errors"].append("zone_geometry_load_failed")
    if not zone_geoms:
        if include_debug and "zone_geometry_load_failed" not in debug["global_errors"]:
            debug["global_errors"].append("zone_geometry_missing")
        return empty, debug

    zone_points: Dict[int, List[Dict[str, Any]]] = {}
    try:
        zone_points = _pickup_zone_recent_points(clean_zone_ids, PICKUP_ZONE_HOTSPOT_MAX_POINTS)
    except Exception:
        if include_debug:
            debug["global_errors"].append("recent_points_load_failed")

    zone_signatures = {zone_id: _pickup_zone_signature(zone_points.get(zone_id, [])) for zone_id in clean_zone_ids}
    score_cache_key = "|".join(
        [f"slot={_current_timeslot_bin(now_ts)}"]
        + [f"{zone_id}:{zone_signatures[zone_id]}" for zone_id in clean_zone_ids]
    )
    with _pickup_zone_score_bundle_lock:
        score_bundle = _pickup_zone_score_bundle_cache.get(score_cache_key)
    score_bundle_fresh = bool(
        score_bundle and float(score_bundle.get("expires_at_monotonic") or 0.0) > now_monotonic
    )

    zone_scores: Dict[int, Any] = {}
    if score_bundle_fresh:
        _record_perf_metric("pickup_score_bundle.cache_hit")
        zone_scores = score_bundle.get("zone_scores") or {}
    else:
        _record_perf_metric("pickup_score_bundle.cache_miss")
        try:
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
            with _pickup_zone_score_bundle_lock:
                _pickup_zone_score_bundle_cache[score_cache_key] = {
                    "zone_scores": zone_scores,
                    "expires_at_monotonic": now_monotonic + PICKUP_SCORE_CACHE_TTL_SECONDS,
                }
            try:
                log_zone_bins(_db_exec, bin_time=now_ts, rows=zone_scores.values())
            except Exception:
                if include_debug:
                    debug["global_errors"].append("log_zone_bins_failed")
                print("[warn] Failed to log pickup zone bins", traceback.format_exc())
        except Exception:
            if include_debug:
                debug["global_errors"].append("score_zones_failed")
            print("[warn] Failed to score pickup zones", traceback.format_exc())
            zone_scores = {}

    try:
        _maybe_prune_pickup_experiment_tables(now_ts)
    except Exception:
        if include_debug:
            debug["global_errors"].append("prune_experiment_tables_failed")
        print("[warn] Failed to prune pickup hotspot experiment tables", traceback.format_exc())

    zone_feature_map: Dict[int, List[Dict[str, Any]]] = {}
    zone_debug_map: Dict[int, Dict[str, Any]] = {}
    zone_recent_points_map: Dict[int, List[Dict[str, Any]]] = {}
    zone_meta_map: Dict[int, Dict[str, Any]] = {}
    for zone_id in clean_zone_ids:
        pts = zone_points.get(zone_id, [])
        zone_data = zone_geoms.get(zone_id)
        signature = zone_signatures.get(zone_id) or _pickup_zone_signature([])
        # Hotspot polygon emission is cluster-driven (legacy builder behavior).
        # Recommendation scoring metadata is attached later, but must not gate
        # whether a zone can emit hotspot polygons.
        qualified = len(pts) >= PICKUP_ZONE_HOTSPOT_MIN_POINTS
        zone_debug: Optional[Dict[str, Any]] = None
        if include_debug:
            zone_debug = {
                "zone_id": zone_id,
                "zone_name": (zone_data or {}).get("zone_name") or "",
                "borough": (zone_data or {}).get("borough") or "",
                "point_count": len(pts),
                "recent_point_count": len(pts),
                "qualified": qualified,
                "geometry_found": bool(zone_data),
                "cached_hit": False,
                "primary_attempted": False,
                "primary_ok": False,
                "fallback_attempted": False,
                "fallback_ok": False,
                "historical_fallback_attempted": False,
                "historical_fallback_ok": False,
                "historical_fallback_component_count": 0,
                "historical_fallback_anchor_point_count": 0,
                "historical_fallback_strongest_support": 0.0,
                "historical_fallback_reason": "not_attempted",
                "feature_emitted": False,
                "micro_hotspot_count": 0,
                "hotspot_method": "none",
                "signature": signature,
                "candidate_component_count": 0,
                "emitted_hotspot_count": 0,
                "merged": False,
                "merge_reason": "none",
                "hotspot_ids": [],
                "component_point_counts": [],
                "second_hotspot_qualified": False,
                "second_hotspot_rejected_reason": "",
                "hotspot_limit_used": 2,
                "third_hotspot_qualified": False,
                "third_hotspot_rejected_reason": "",
                "geometry_refinement_attempted": False,
                "geometry_refinement_applied_count": 0,
                "geometry_refinement_rejected_count": 0,
                "refinement_rejected_reason_counts": {},
                "historical_component_count_for_refinement": 0,
                "refined_hotspot_ids": [],
                "rejected_refinement_hotspot_ids": [],
                "primary_recommended_hotspot_id": None,
                "final_recommended_hotspot_id": None,
                "final_recommendation_scope": None,
                "recommendation_logged_post_merge": False,
                "recommendation_logged_cluster_id": None,
                "recommended_micro_hotspot_count": 0,
                "recommended_primary_micro_hotspot_count": 0,
                "micro_hotspot_parent_sync_ok": True,
                "micro_hotspot_parent_fallback_learning_count": 0,
                "outcome_scope_used": "zone_fallback",
                "hotspot_specific_outcome_sample_count": 0,
                "zone_fallback_outcome_sample_count": 0,
                "errors": [],
            }
            if qualified:
                debug["qualified_zone_ids"].append(zone_id)

        if not zone_data:
            if zone_debug is not None:
                zone_debug["errors"].append("geometry_missing")
                zone_debug_map[zone_id] = zone_debug
            continue

        hotspot_limit = _determine_live_zone_hotspot_limit(zone_id, zone_data, now_ts)
        if zone_debug is not None:
            zone_debug["hotspot_limit_used"] = hotspot_limit

        zone_recent_points_map[zone_id] = pts
        zone_meta_map[zone_id] = zone_data

        zone_features: List[Dict[str, Any]] = []
        cached_hit = False
        with _pickup_zone_hotspot_cache_lock:
            cached = _pickup_zone_hotspot_feature_cache.get(zone_id)
            if (
                cached
                and cached.get("signature") == signature
                and cached.get("features")
                and int(cached.get("hotspot_limit_used") or 2) == hotspot_limit
                and (now_monotonic - float(cached.get("created_at_monotonic") or 0.0)) <= PICKUP_HOTSPOT_CACHE_TTL_SECONDS
            ):
                cached["last_access_monotonic"] = now_monotonic
                zone_features = copy.deepcopy(cached["features"])
                cached_hit = True
                _record_perf_metric("pickup_hotspot.cache_hit")
        if not cached_hit:
            _record_perf_metric("pickup_hotspot.cache_miss")
        if zone_debug is not None and cached_hit:
            zone_debug["cached_hit"] = True
            zone_debug["hotspot_method"] = "cache"

        zone_component_debug: Dict[str, Any] = {}
        if not zone_features and qualified:
            if zone_debug is not None:
                zone_debug["primary_attempted"] = True
            try:
                zone_features, zone_component_debug = _build_zone_hotspot_components(zone_id, zone_data, pts, fallback=False, hotspot_limit=hotspot_limit)
                if zone_debug is not None:
                    zone_debug["primary_ok"] = bool(zone_features)
                    if zone_features:
                        zone_debug["hotspot_method"] = "primary"
            except Exception:
                if zone_debug is not None:
                    zone_debug["errors"].append("primary_hotspot_build_failed")
                print(f"[warn] Failed to generate pickup zone hotspot for zone {zone_id}", traceback.format_exc())

        if not zone_features and qualified:
            if zone_debug is not None:
                zone_debug["fallback_attempted"] = True
            try:
                zone_features, zone_component_debug = _build_zone_hotspot_components(zone_id, zone_data, pts, fallback=True, hotspot_limit=hotspot_limit)
                if zone_debug is not None:
                    zone_debug["fallback_ok"] = bool(zone_features)
                    if zone_features:
                        zone_debug["hotspot_method"] = "fallback"
            except Exception:
                if zone_debug is not None:
                    zone_debug["errors"].append("fallback_hotspot_build_failed")
                print(f"[warn] Failed to generate fallback pickup zone hotspot for zone {zone_id}", traceback.format_exc())

        if not zone_features:
            historical_debug: Dict[str, Any] = {}
            try:
                zone_features, historical_debug = _build_historical_fallback_zone_hotspot_features(
                    zone_id=zone_id,
                    zone_data=zone_data,
                    recent_pts=pts,
                    hotspot_limit=hotspot_limit,
                    now_ts=now_ts,
                )
                if zone_debug is not None:
                    zone_debug.update(historical_debug)
                    if zone_features:
                        zone_debug["hotspot_method"] = "historical_fallback"
                        zone_debug["historical_fallback_reason"] = "normal_build_empty_then_historical_fallback"
            except Exception:
                if zone_debug is not None:
                    zone_debug["historical_fallback_attempted"] = True
                    zone_debug["historical_fallback_reason"] = "historical_fallback_build_failed"
                    zone_debug["errors"].append("historical_fallback_hotspot_build_failed")
                print(f"[warn] Failed to generate historical fallback hotspot for zone {zone_id}", traceback.format_exc())

        if zone_debug is not None and zone_component_debug:
            zone_debug.update(zone_component_debug)

        if not zone_features:
            with _pickup_zone_hotspot_cache_lock:
                _pickup_zone_hotspot_feature_cache.pop(zone_id, None)
            if zone_debug is not None:
                zone_debug_map[zone_id] = zone_debug
            continue

        if not cached_hit:
            zone_micro_total = 0
            for feature in zone_features:
                props = feature.setdefault("properties", {})
                props["signature"] = signature
                try:
                    micro_payload = _build_zone_micro_hotspots_payload(zone_id, zone_data, pts, feature)
                except Exception:
                    micro_payload = []
                    if zone_debug is not None:
                        zone_debug["errors"].append("micro_hotspot_build_failed")
                    print(f"[warn] Failed to build pickup micro-hotspots for zone {zone_id}", traceback.format_exc())
                props["micro_hotspots"] = [item for item in micro_payload if isinstance(item, dict)][:1]
                zone_micro_total += len(props["micro_hotspots"])
                feature.pop("_hotspot_proj", None)
                feature.pop("_component_cells", None)
            with _pickup_zone_hotspot_cache_lock:
                _pickup_zone_hotspot_feature_cache[zone_id] = {
                    "signature": signature,
                    "features": copy.deepcopy(zone_features),
                    "hotspot_limit_used": hotspot_limit,
                    "created_at_monotonic": now_monotonic,
                    "last_access_monotonic": now_monotonic,
                }
            if zone_debug is not None:
                zone_debug["micro_hotspot_count"] = zone_micro_total

        score = zone_scores.get(zone_id)
        zone_micro_total = 0
        for feature in zone_features:
            props = feature.setdefault("properties", {})
            props["signature"] = signature
            zone_micro_total += len(props.get("micro_hotspots") or [])
            props["recommended_hotspot"] = False
            props["recommendation_scope"] = "zone_secondary"
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
        refinement_debug: Dict[str, Any] = {}
        try:
            refinement_debug = _refine_emitted_zone_hotspot_geometries(
                zone_id=zone_id,
                zone_data=zone_data,
                zone_features=zone_features,
                pts=pts,
                now_ts=now_ts,
            )
        except Exception:
            if zone_debug is not None:
                zone_debug["errors"].append("hotspot_geometry_refinement_failed")
            print(f"[warn] Failed to refine pickup zone hotspot geometries for zone {zone_id}", traceback.format_exc())
        enrichment_debug: Dict[str, Any] = {}
        try:
            enrichment_debug = _enrich_emitted_zone_hotspot_features(
                zone_id=zone_id,
                zone_data=zone_data,
                zone_features=zone_features,
                pts=pts,
                score=score,
                now_ts=now_ts,
            )
        except Exception:
            if zone_debug is not None:
                zone_debug["errors"].append("hotspot_enrichment_failed")
            print(f"[warn] Failed to enrich pickup zone hotspots for zone {zone_id}", traceback.format_exc())
        zone_feature_map[zone_id] = zone_features

        if zone_debug is not None:
            zone_debug["feature_emitted"] = bool(zone_features)
            zone_debug["micro_hotspot_count"] = zone_micro_total
            for key, default in (
                ("geometry_refinement_attempted", False),
                ("geometry_refinement_applied_count", 0),
                ("geometry_refinement_rejected_count", 0),
                ("refinement_rejected_reason_counts", {}),
                ("historical_component_count_for_refinement", 0),
                ("refined_hotspot_ids", []),
                ("rejected_refinement_hotspot_ids", []),
            ):
                zone_debug[key] = refinement_debug.get(key, default)
            for key, default in (
                ("historical_anchor_point_count", 0),
                ("historical_component_count", 0),
                ("historical_strength", 0.0),
                ("outcome_modifier", 1.0),
                ("outcome_sample_count", 0),
                ("outcome_scope_used", "zone_fallback"),
                ("hotspot_specific_outcome_sample_count", 0),
                ("zone_fallback_outcome_sample_count", 0),
                ("outcome_effective_sample_count", 0.0),
                ("outcome_raw_conversion_rate", 0.0),
                ("outcome_representative_minutes_to_trip", 0.0),
                ("outcome_precision_target_miles", 0.12),
                ("outcome_precision_span_miles", 0.40),
                ("outcome_precision_profile", "hotspot_v1"),
                ("outcome_support_strength", 0.0),
                ("outcome_raw_modifier_before_support_damping", 1.0),
                ("outcome_recency_weight_version", "resolved_recency_v1"),
                ("quality_modifier", 1.0),
                ("short_trip_trap_penalty", 0.0),
                ("continuation_bonus", 0.0),
                ("saturation_penalty", 0.0),
                ("live_combined_modifier", 1.0),
                ("live_modifier_version", "hybrid_v1"),
            ):
                zone_debug[key] = enrichment_debug.get(key, default)
            if zone_features:
                top_props = zone_features[0].get("properties") or {}
                for key in (
                    "outcome_modifier",
                    "outcome_sample_count",
                    "outcome_scope_used",
                    "hotspot_specific_outcome_sample_count",
                    "zone_fallback_outcome_sample_count",
                    "outcome_effective_sample_count",
                    "outcome_raw_conversion_rate",
                    "outcome_representative_minutes_to_trip",
                    "outcome_precision_target_miles",
                    "outcome_precision_span_miles",
                    "outcome_precision_profile",
                    "outcome_support_strength",
                    "outcome_raw_modifier_before_support_damping",
                    "outcome_recency_weight_version",
                    "quality_modifier",
                    "short_trip_trap_penalty",
                    "continuation_bonus",
                    "saturation_penalty",
                    "live_combined_modifier",
                    "live_modifier_version",
                ):
                    if key in top_props:
                        zone_debug[key] = top_props.get(key)
            zone_debug_map[zone_id] = zone_debug

    merge_candidate_map: Dict[int, List[Dict[str, Any]]] = {}
    for zone_id, zone_features in zone_feature_map.items():
        zone_meta = zone_meta_map.get(zone_id) or {}
        if len(zone_features) != 1:
            continue
        if zone_meta.get("geometry") is None or getattr(zone_meta.get("geometry"), "is_empty", True):
            continue
        borough = str(zone_meta.get("borough") or "").strip()
        if not borough:
            continue
        merge_candidate_map[zone_id] = [zone_features[0]]

    merge_results: List[Dict[str, Any]] = []
    if len(merge_candidate_map) >= 2:
        try:
            merge_results = build_cross_zone_merged_hotspots(
                zone_feature_map=merge_candidate_map,
                zone_meta_map=zone_meta_map,
                zone_recent_points=zone_recent_points_map,
            )
        except Exception:
            if include_debug:
                debug["global_errors"].append("cross_zone_merge_build_failed")
            print("[warn] Failed to build cross-zone merged hotspots", traceback.format_exc())
            merge_results = []

    consumed_zones: set[int] = set()
    merged_features: List[Dict[str, Any]] = []
    merged_zone_pairs: List[List[int]] = []
    for merge_result in sorted(
        merge_results,
        key=lambda m: tuple(int(z) for z in (m.get("zone_pair") or ())),
    ):
        zone_pair = tuple(int(z) for z in (merge_result.get("zone_pair") or ()))
        if len(zone_pair) != 2:
            continue
        zone_a, zone_b = zone_pair
        if zone_a in consumed_zones or zone_b in consumed_zones:
            continue
        features_a = zone_feature_map.get(zone_a) or []
        features_b = zone_feature_map.get(zone_b) or []
        if len(features_a) != 1 or len(features_b) != 1:
            continue
        meta_a = zone_meta_map.get(zone_a) or {}
        meta_b = zone_meta_map.get(zone_b) or {}
        borough_a = str(meta_a.get("borough") or "").strip().lower()
        borough_b = str(meta_b.get("borough") or "").strip().lower()
        if not borough_a or borough_a != borough_b:
            continue
        merged_feature = _build_cross_zone_merged_hotspot_feature(merge_result, features_a[0], features_b[0])
        merged_features.append(merged_feature)
        consumed_zones.add(zone_a)
        consumed_zones.add(zone_b)
        merged_zone_pairs.append([zone_a, zone_b])
        merged_hotspot_id = str((merged_feature.get("properties") or {}).get("hotspot_id") or "")
        for zid, partner in ((zone_a, zone_b), (zone_b, zone_a)):
            zone_debug = zone_debug_map.get(zid)
            if zone_debug is None:
                continue
            zone_debug["merged"] = True
            zone_debug["merge_reason"] = "cross_zone_merge"
            zone_debug["merged_into_cross_zone_hotspot"] = True
            zone_debug["merged_partner_zone_id"] = partner
            zone_debug["merged_hotspot_id"] = merged_hotspot_id

    features: List[Dict[str, Any]] = []
    features.extend(merged_features)
    for zone_id in clean_zone_ids:
        if zone_id in consumed_zones:
            continue
        features.extend(zone_feature_map.get(zone_id) or [])

    post_merge_recommendation_debug = _apply_final_recommended_hotspot_ownership(
        clean_zone_ids=clean_zone_ids,
        zone_scores=zone_scores,
        zone_feature_map=zone_feature_map,
        merged_features=merged_features,
        consumed_zones=consumed_zones,
        score_bundle_fresh=score_bundle_fresh,
        now_ts=now_ts,
        zone_debug_map=zone_debug_map,
    )
    debug.update(post_merge_recommendation_debug)
    debug.update(_apply_micro_hotspot_parent_recommendation_state(features))
    debug.update(
        _apply_final_recommended_micro_hotspot_ownership(
            features=features,
            score_bundle_fresh=score_bundle_fresh,
            now_ts=now_ts,
            zone_debug_map=zone_debug_map,
        )
    )
    debug.update(_apply_micro_hotspot_learning(features, zone_debug_map))
    debug["distance_aware_hotspot_learning_count"] = sum(
        1
        for feature in features
        if int(float(((feature.get("properties") or {}).get("outcome_distance_sample_count") or 0.0)) > 0)
    )
    debug["distance_aware_merged_hotspot_learning_count"] = sum(
        1
        for feature in merged_features
        if int(float(((feature.get("properties") or {}).get("merged_outcome_distance_sample_count") or 0.0)) > 0)
    )
    distance_aware_micro_hotspot_learning_count = 0
    for feature in features:
        props = (feature or {}).get("properties") or {}
        for micro in (props.get("micro_hotspots") or []):
            if not isinstance(micro, dict):
                continue
            if int(float(micro.get("micro_outcome_distance_sample_count") or 0.0)) > 0:
                distance_aware_micro_hotspot_learning_count += 1
    debug["distance_aware_micro_hotspot_learning_count"] = distance_aware_micro_hotspot_learning_count
    debug["hotspot_precision_profile_count"] = sum(
        1
        for feature in features
        if str(((feature.get("properties") or {}).get("outcome_precision_profile") or "")).strip()
    )
    debug["merged_hotspot_precision_profile_count"] = sum(
        1
        for feature in merged_features
        if str(((feature.get("properties") or {}).get("merged_outcome_precision_profile") or "")).strip()
    )
    debug["micro_hotspot_precision_profile_count"] = sum(
        1
        for feature in features
        for micro in (((feature.get("properties") or {}).get("micro_hotspots")) or [])
        if isinstance(micro, dict) and str((micro.get("micro_outcome_precision_profile") or "")).strip()
    )
    if not score_bundle_fresh:
        micro_rows = _build_micro_experiment_rows_from_features(features)
        debug["micro_experiment_rows_built"] = len(micro_rows)
        if micro_rows:
            try:
                log_micro_bins(_db_exec, bin_time=now_ts, rows=micro_rows)
                debug["micro_experiment_rows_logged"] = len(micro_rows)
            except Exception:
                debug["micro_experiment_logging_failed"] = True
                if include_debug:
                    debug["global_errors"].append("log_micro_bins_failed")
                print("[warn] Failed to log pickup micro bins", traceback.format_exc())

    for zone_id, zdebug in zone_debug_map.items():
        zone_recommended_micro_count = 0
        zone_nonrecommended_micro_count = 0
        zone_sync_ok = True
        for feature in features:
            props = (feature or {}).get("properties") or {}
            covered_zone_ids = props.get("covered_zone_ids") or [props.get("zone_id")]
            normalized_zone_ids: set[int] = set()
            for raw_zone_id in covered_zone_ids:
                try:
                    normalized_zone_ids.add(int(raw_zone_id))
                except Exception:
                    continue
            if zone_id not in normalized_zone_ids:
                continue
            for micro in (props.get("micro_hotspots") or []):
                if not isinstance(micro, dict):
                    continue
                if bool(micro.get("recommended")):
                    zone_recommended_micro_count += 1
                else:
                    zone_nonrecommended_micro_count += 1
                parent_hotspot_id = micro.get("parent_hotspot_id")
                zone_sync_ok = zone_sync_ok and (str(parent_hotspot_id or "") == str(props.get("hotspot_id") or ""))
        zdebug["recommended_micro_hotspot_count"] = zone_recommended_micro_count
        zdebug["nonrecommended_micro_hotspot_count"] = zone_nonrecommended_micro_count
        zdebug["micro_hotspot_parent_sync_ok"] = bool(zone_sync_ok)

    if include_debug:
        debug["cross_zone_merge_candidates"] = len(merge_results)
        debug["cross_zone_merge_applied_count"] = len(merged_features)
        debug["merged_zone_pairs"] = merged_zone_pairs
        debug["merged_hotspot_specific_learning_count"] = sum(
            1
            for feature in merged_features
            if str(((feature.get("properties") or {}).get("merged_outcome_scope_used") or "")).strip()
            == "merged_hotspot_specific"
        )
        debug["merged_hotspot_fallback_learning_count"] = sum(
            1
            for feature in merged_features
            if str(((feature.get("properties") or {}).get("merged_outcome_scope_used") or "")).strip()
            == "merged_fallback_from_children"
        )
        rendered_zone_ids: List[int] = []
        for zone_id in clean_zone_ids:
            zdebug = zone_debug_map.get(zone_id)
            if zdebug is None:
                continue
            if zdebug.get("feature_emitted"):
                rendered_zone_ids.append(zone_id)
            debug["zones"].append(zdebug)
        debug["rendered_zone_ids"] = rendered_zone_ids

    _cleanup_pickup_zone_caches(now_monotonic)
    payload = {"type": "FeatureCollection", "features": features}
    debug["low_support_damped_hotspot_count"] = sum(
        1
        for feature in features
        if float((feature.get("properties") or {}).get("outcome_support_strength") or 0.0) < 0.999
        and abs(float((feature.get("properties") or {}).get("outcome_raw_modifier_before_support_damping") or 1.0) - 1.0) > 0.01
    )
    debug["low_support_damped_merged_hotspot_count"] = sum(
        1
        for feature in merged_features
        if float((feature.get("properties") or {}).get("merged_outcome_support_strength") or 0.0) < 0.999
        and abs(float((feature.get("properties") or {}).get("merged_outcome_raw_modifier_before_support_damping") or 1.0) - 1.0) > 0.01
    )
    debug["low_support_damped_micro_hotspot_count"] = sum(
        1
        for feature in features
        for micro in ((feature.get("properties") or {}).get("micro_hotspots") or [])
        if isinstance(micro, dict)
        and float(micro.get("micro_outcome_support_strength") or 0.0) < 0.999
        and abs(float(micro.get("micro_outcome_raw_modifier_before_support_damping") or 1.0) - 1.0) > 0.01
    )
    debug["zone_hotspot_count"] = len(features)
    debug["orphan_micro_hotspot_count"] = 0
    debug["top_level_micro_hotspot_count"] = len(_flatten_zone_micro_hotspots(payload))
    return payload, debug

def _pickup_zone_hotspots(zone_ids: List[int]) -> Dict[str, Any]:
    try:
        payload, _ = _pickup_zone_hotspots_with_debug(zone_ids)
        return payload
    except Exception:
        print("[warn] Failed to generate pickup zone hotspots", traceback.format_exc())
        return {"type": "FeatureCollection", "features": []}


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
    clean_zone_ids = list(dict.fromkeys(clean_zone_ids))[:256]
    zone_rows = _pickup_zone_recent_points(
        clean_zone_ids,
        max_points_per_zone=safe_sample_limit,
    )
    try:
        zone_geoms = _load_pickup_zone_geometries()
    except Exception:
        print("[warn] pickup zone geometry metadata load failed", traceback.format_exc())
        zone_geoms = {}
    stats: List[Dict[str, Any]] = []
    for zid in clean_zone_ids:
        rows = zone_rows.get(zid) or []
        if not rows:
            continue

        lat_values: List[float] = []
        lng_values: List[float] = []
        latest_created_at: Optional[int] = None
        first_row = rows[0] if rows else {}

        for row in rows:
            try:
                lat_values.append(float(row.get("lat")))
            except Exception:
                pass
            try:
                lng_values.append(float(row.get("lng")))
            except Exception:
                pass
            try:
                created_at = int(row.get("created_at"))
                latest_created_at = created_at if latest_created_at is None else max(latest_created_at, created_at)
            except Exception:
                continue

        zone_meta = zone_geoms.get(zid) if isinstance(zone_geoms, dict) else None
        zone_name = ""
        borough = ""
        if isinstance(zone_meta, dict):
            zone_name = str(zone_meta.get("zone_name") or "").strip()
            borough = str(zone_meta.get("borough") or "").strip()
        if not zone_name:
            zone_name = str(first_row.get("zone_name") or "").strip()
        if not borough:
            borough = str(first_row.get("borough") or "").strip()

        stats.append(
            {
                "zone_id": zid,
                "zone_name": zone_name,
                "borough": borough,
                "sample_size": len(rows),
                "avg_lat": (sum(lat_values) / len(lat_values)) if lat_values else None,
                "avg_lng": (sum(lng_values) / len(lng_values)) if lng_values else None,
                "latest_created_at": latest_created_at,
                "sample_limit": safe_sample_limit,
            }
        )
    return stats


def _flatten_zone_micro_hotspots(zone_hotspots: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Flatten only micro-hotspots attached to emitted hotspot features.
    flattened: List[Dict[str, Any]] = []

    def _normalize_micro_hotspot(
        item: Dict[str, Any],
        fallback_zone_id: Optional[int] = None,
        fallback_hotspot_id: Optional[str] = None,
        fallback_hotspot_index: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None
        normalized = dict(item)
        zone_val = normalized.get("zone_id")
        if zone_val is None and fallback_zone_id is not None:
            zone_val = fallback_zone_id
        try:
            normalized["zone_id"] = int(zone_val) if zone_val is not None else None
        except Exception:
            return None
        if normalized.get("zone_id") is None:
            return None

        if normalized.get("hotspot_id") is None:
            normalized["hotspot_id"] = fallback_hotspot_id
        if normalized.get("hotspot_index") is None and fallback_hotspot_index is not None:
            normalized["hotspot_index"] = fallback_hotspot_index
        if normalized.get("hotspot_id") is None or normalized.get("hotspot_index") is None:
            return None
        try:
            normalized["hotspot_index"] = int(normalized.get("hotspot_index"))
        except Exception:
            return None
        return normalized

    features = zone_hotspots.get("features") if isinstance(zone_hotspots, dict) else None
    if isinstance(features, list):
        for feature in features:
            if not isinstance(feature, dict):
                continue
            props = feature.get("properties")
            if not isinstance(props, dict):
                continue
            fallback_zone_id: Optional[int] = None
            try:
                if props.get("zone_id") is not None:
                    fallback_zone_id = int(props.get("zone_id"))
            except Exception:
                fallback_zone_id = None
            fallback_hotspot_id = props.get("hotspot_id") if props.get("hotspot_id") is not None else None
            fallback_hotspot_index: Optional[int] = None
            try:
                if props.get("hotspot_index") is not None:
                    fallback_hotspot_index = int(props.get("hotspot_index"))
            except Exception:
                fallback_hotspot_index = None
            micro_hotspots = props.get("micro_hotspots")
            if not isinstance(micro_hotspots, list):
                continue
            for item in micro_hotspots:
                normalized = _normalize_micro_hotspot(
                    item,
                    fallback_zone_id,
                    fallback_hotspot_id,
                    fallback_hotspot_index,
                )
                if normalized is not None:
                    flattened.append(normalized)
    return flattened


def _recent_pickups_payload(
    limit: int = 30,
    zone_sample_limit: int = 100,
    debug: int = 0,
    zone_id: Optional[int] = None,
    min_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lat: Optional[float] = None,
    max_lng: Optional[float] = None,
    viewer: Optional[sqlite3.Row] = None,
) -> Dict[str, Any]:
    viewer_is_admin = bool(viewer is not None and _flag_to_int(viewer["is_admin"]) == 1)
    viewer_user_id: Optional[int] = None
    if viewer is not None:
        try:
            viewer_user_id = int(viewer["id"])
        except Exception:
            viewer_user_id = None
    safe_limit = max(1, min(200, int(limit)))
    safe_zone_sample_limit = max(1, min(100, int(zone_sample_limit)))
    include_debug = int(debug) == 1 and viewer_is_admin
    bbox_key = _rounded_bbox_key(min_lat, min_lng, max_lat, max_lng)
    lo_lat = hi_lat = lo_lng = hi_lng = None
    if bbox_key is not None:
        lo_lat = min(float(min_lat), float(max_lat))
        hi_lat = max(float(min_lat), float(max_lat))
        lo_lng = min(float(min_lng), float(max_lng))
        hi_lng = max(float(min_lng), float(max_lng))
    now_monotonic = time.monotonic()
    cache_key = _pickup_recent_cache_key(
        limit=safe_limit,
        zone_sample_limit=safe_zone_sample_limit,
        zone_id=zone_id,
        bbox_key=bbox_key,
        include_debug=include_debug,
        viewer_is_admin=viewer_is_admin,
    )
    if not include_debug:
        with _pickup_recent_cache_lock:
            _purge_pickup_recent_cache(now_monotonic)
            cached = _pickup_recent_cache.get(cache_key)
            if cached and float(cached.get("expires_at_monotonic") or 0.0) > now_monotonic:
                cached["last_access_monotonic"] = now_monotonic
                _record_perf_metric("pickup_recent.cache_hit")
                response = copy.deepcopy(cached["payload"])
                if viewer_user_id is not None:
                    impression_counts = _log_viewer_recommendation_impressions_from_pickup_payload(
                        payload=response,
                        viewer_user_id=viewer_user_id,
                        now_ts=int(time.time()),
                    )
                    if include_debug:
                        response.setdefault("pickup_hotspot_debug", {}).update(impression_counts)
                return response
    _record_perf_metric("pickup_recent.cache_miss")

    sql = f"""
        SELECT id, lat, lng, zone_id, zone_name, borough, frame_time, created_at
        FROM pickup_logs pl
        WHERE 1=1
          AND {pickup_log_not_voided_sql("pl")}
    """
    params: List[Any] = []

    if zone_id is not None:
        sql += " AND pl.zone_id = ?"
        params.append(int(zone_id))

    if bbox_key is not None:
        sql += " AND pl.lat BETWEEN ? AND ? AND pl.lng BETWEEN ? AND ?"
        params.extend([lo_lat, hi_lat, lo_lng, hi_lng])

    sql += " ORDER BY pl.created_at DESC LIMIT ?"
    params.append(safe_limit)

    rows = _db_query_all(sql, tuple(params))
    items = [dict(r) for r in rows]

    zone_ids_for_stats: List[int] = []
    if zone_id is not None:
        zone_ids_for_stats = [int(zone_id)]
    else:
        stats_sql = f"SELECT DISTINCT pl.zone_id FROM pickup_logs pl WHERE pl.zone_id IS NOT NULL AND {pickup_log_not_voided_sql('pl')}"
        stats_params: List[Any] = []
        if bbox_key is not None:
            stats_sql += " AND pl.lat BETWEEN ? AND ? AND pl.lng BETWEEN ? AND ?"
            stats_params.extend([lo_lat, hi_lat, lo_lng, hi_lng])
        stats_rows = _db_query_all(stats_sql, tuple(stats_params))
        zone_ids_for_stats = [int(dict(r)["zone_id"]) for r in stats_rows if dict(r).get("zone_id") is not None]

    zone_stats: List[Dict[str, Any]] = []
    hotspot_zone_ids: List[int] = []
    zone_hotspots: Dict[str, Any] = {"type": "FeatureCollection", "features": []}
    micro_hotspots: List[Dict[str, Any]] = []
    hotspot_aux_errors: List[str] = []
    pickup_hotspot_debug: Dict[str, Any] = {
        "min_points_threshold": PICKUP_ZONE_HOTSPOT_MIN_POINTS,
        "requested_zone_ids": [],
        "zone_hotspot_count": 0,
        "orphan_micro_hotspot_count": 0,
        "top_level_micro_hotspot_count": 0,
        "qualified_zone_ids": [],
        "rendered_zone_ids": [],
        "global_errors": [],
        "zones": [],
    }
    try:
        zone_stats = _pickup_zone_stats(zone_ids_for_stats, sample_limit=safe_zone_sample_limit)
        hotspot_zone_ids = [int(z.get("zone_id")) for z in zone_stats if z.get("zone_id") is not None]
        pickup_hotspot_debug["requested_zone_ids"] = hotspot_zone_ids
    except Exception:
        print("[warn] pickup zone stats helper failed", traceback.format_exc())
        zone_stats = []
        hotspot_zone_ids = list(dict.fromkeys(zone_ids_for_stats))
        pickup_hotspot_debug["requested_zone_ids"] = hotspot_zone_ids
        hotspot_aux_errors.append("pickup_zone_stats_failed")
    try:
        zone_hotspots, pickup_hotspot_debug = _pickup_zone_hotspots_with_debug(
            hotspot_zone_ids,
            include_debug=include_debug,
        )
        micro_hotspots = _flatten_zone_micro_hotspots(zone_hotspots)
    except Exception:
        print("[warn] pickup hotspot geometry helper failed", traceback.format_exc())
        zone_hotspots = {"type": "FeatureCollection", "features": []}
        micro_hotspots = []
        hotspot_aux_errors.append("pickup_hotspot_geometry_failed")
        pickup_hotspot_debug = {
            "min_points_threshold": PICKUP_ZONE_HOTSPOT_MIN_POINTS,
            "requested_zone_ids": hotspot_zone_ids,
            "zone_hotspot_count": 0,
            "orphan_micro_hotspot_count": 0,
            "top_level_micro_hotspot_count": 0,
            "qualified_zone_ids": [],
            "rendered_zone_ids": [],
            "global_errors": hotspot_aux_errors[:] if include_debug else [],
            "zones": [],
        }
    if include_debug and hotspot_aux_errors:
        existing_errors = pickup_hotspot_debug.get("global_errors")
        if not isinstance(existing_errors, list):
            existing_errors = []
        for err in hotspot_aux_errors:
            if err not in existing_errors:
                existing_errors.append(err)
        pickup_hotspot_debug["global_errors"] = existing_errors

    if not isinstance(zone_hotspots, dict):
        zone_hotspots = {"type": "FeatureCollection", "features": []}
    if not isinstance(micro_hotspots, list):
        micro_hotspots = []
    zone_features = zone_hotspots.get("features") if isinstance(zone_hotspots, dict) else []
    zone_hotspot_count = len(zone_features) if isinstance(zone_features, list) else 0
    has_recent_items = len(items) > 0
    has_zone_stats = len(zone_stats) > 0
    zone_hotspots_empty = zone_hotspot_count <= 0
    micro_hotspots_empty = len(micro_hotspots) <= 0
    hotspot_helpers_failed = bool(hotspot_aux_errors)
    good_overlay = (not zone_hotspots_empty) or (not micro_hotspots_empty)
    helper_error_empty_overlay = hotspot_helpers_failed and zone_hotspots_empty and micro_hotspots_empty
    needs_last_good_fallback = (
        zone_hotspots_empty
        and micro_hotspots_empty
        and (hotspot_helpers_failed or has_recent_items or has_zone_stats)
    )
    used_last_good_hotspot_overlay_fallback = False
    last_good_hotspot_overlay_age_seconds = 0.0
    last_good_hotspot_overlay_reason = ""
    suppressed_failed_empty_hotspot_cache_write = False
    if needs_last_good_fallback:
        last_good_overlay = _get_pickup_last_good_overlay(cache_key, now_monotonic=now_monotonic)
        if last_good_overlay:
            fallback_zone_hotspots = last_good_overlay.get("zone_hotspots")
            fallback_micro_hotspots = last_good_overlay.get("micro_hotspots")
            fallback_zone_stats = last_good_overlay.get("zone_stats")
            if isinstance(fallback_zone_hotspots, dict):
                zone_hotspots = fallback_zone_hotspots
            if isinstance(fallback_micro_hotspots, list):
                micro_hotspots = fallback_micro_hotspots
            if (not has_zone_stats) and isinstance(fallback_zone_stats, list):
                zone_stats = fallback_zone_stats
                has_zone_stats = len(zone_stats) > 0
            if include_debug and isinstance(last_good_overlay.get("pickup_hotspot_debug"), dict):
                pickup_hotspot_debug = last_good_overlay.get("pickup_hotspot_debug") or pickup_hotspot_debug
            zone_features = zone_hotspots.get("features") if isinstance(zone_hotspots, dict) else []
            zone_hotspot_count = len(zone_features) if isinstance(zone_features, list) else 0
            zone_hotspots_empty = zone_hotspot_count <= 0
            micro_hotspots_empty = len(micro_hotspots) <= 0
            good_overlay = (not zone_hotspots_empty) or (not micro_hotspots_empty)
            used_last_good_hotspot_overlay_fallback = good_overlay
            saved_at_monotonic = float(last_good_overlay.get("saved_at_monotonic") or now_monotonic)
            last_good_hotspot_overlay_age_seconds = max(0.0, now_monotonic - saved_at_monotonic)
            if hotspot_helpers_failed:
                last_good_hotspot_overlay_reason = "helper_failed_with_empty_overlay"
            elif has_recent_items:
                last_good_hotspot_overlay_reason = "recent_items_with_empty_overlay"
            elif has_zone_stats:
                last_good_hotspot_overlay_reason = "zone_stats_with_empty_overlay"
            else:
                last_good_hotspot_overlay_reason = "transient_empty_overlay"
    if good_overlay and not helper_error_empty_overlay:
        overlay_payload: Dict[str, Any] = {
            "zone_hotspots": zone_hotspots,
            "micro_hotspots": micro_hotspots,
            "zone_stats": zone_stats,
        }
        if include_debug and isinstance(pickup_hotspot_debug, dict):
            overlay_payload["pickup_hotspot_debug"] = pickup_hotspot_debug
        _set_pickup_last_good_overlay(
            cache_key,
            overlay_payload,
            now_monotonic=now_monotonic,
        )

    response = {
        "ok": True,
        "count": len(items),
        "items": items,
        "zone_stats": zone_stats,
        "zone_hotspots": zone_hotspots,
        "micro_hotspots": micro_hotspots,
        "micro_hotspot_debug": {
            "zone_hotspot_count": zone_hotspot_count,
            "orphan_micro_hotspot_count": 0,
            "top_level_micro_hotspot_count": len(micro_hotspots),
        },
    }
    if include_debug:
        response["pickup_hotspot_debug"] = pickup_hotspot_debug
        response["pickup_hotspot_debug"].setdefault("viewer_logged_hotspot_impressions", 0)
        response["pickup_hotspot_debug"].setdefault("viewer_logged_micro_impressions", 0)
        response["pickup_hotspot_debug"].setdefault("viewer_deduped_hotspot_impressions", 0)
        response["pickup_hotspot_debug"].setdefault("viewer_deduped_micro_impressions", 0)
        response["pickup_hotspot_debug"].setdefault("viewer_logged_hotspot_impressions_with_centers", 0)
        response["pickup_hotspot_debug"].setdefault("viewer_logged_micro_impressions_with_centers", 0)
        response["used_last_good_hotspot_overlay_fallback"] = used_last_good_hotspot_overlay_fallback
        response["last_good_hotspot_overlay_age_seconds"] = last_good_hotspot_overlay_age_seconds
        response["last_good_hotspot_overlay_reason"] = last_good_hotspot_overlay_reason
        response["suppressed_failed_empty_hotspot_cache_write"] = suppressed_failed_empty_hotspot_cache_write
    if viewer_user_id is not None:
        impression_counts = _log_viewer_recommendation_impressions_from_pickup_payload(
            payload=response,
            viewer_user_id=viewer_user_id,
            now_ts=int(time.time()),
        )
        if include_debug:
            response.setdefault("pickup_hotspot_debug", {}).update(impression_counts)
    if not include_debug:
        should_cache_response = True
        if helper_error_empty_overlay and not used_last_good_hotspot_overlay_fallback:
            should_cache_response = False
            suppressed_failed_empty_hotspot_cache_write = True
        with _pickup_recent_cache_lock:
            if should_cache_response:
                _pickup_recent_cache[cache_key] = {
                    "payload": copy.deepcopy(response),
                    "expires_at_monotonic": now_monotonic + PICKUP_RECENT_CACHE_TTL_SECONDS,
                    "last_access_monotonic": now_monotonic,
                }
            _purge_pickup_recent_cache(now_monotonic)
    elif suppressed_failed_empty_hotspot_cache_write:
        response["suppressed_failed_empty_hotspot_cache_write"] = True
    return response


@app.get("/events/pickups/recent")
def get_recent_pickups(
    limit: int = 30,
    zone_sample_limit: int = 100,
    debug: int = 0,
    zone_id: Optional[int] = None,
    min_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lat: Optional[float] = None,
    max_lng: Optional[float] = None,
    viewer: sqlite3.Row = Depends(require_user),
):
    return _recent_pickups_payload(
        limit=limit,
        zone_sample_limit=zone_sample_limit,
        debug=debug,
        zone_id=zone_id,
        min_lat=min_lat,
        min_lng=min_lng,
        max_lat=max_lat,
        max_lng=max_lng,
        viewer=viewer,
    )


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
    if bool(payload.disabled):
        _db_exec("DELETE FROM presence WHERE user_id=?", (int(payload.user_id),))
        _presence_remove_runtime_visibility(int(payload.user_id), reason="disabled")
    else:
        row = _db_query_one(
            "SELECT id, ghost_mode, is_disabled, is_suspended FROM users WHERE id=? LIMIT 1",
            (int(payload.user_id),),
        )
        if row:
            is_visible, reason = _presence_state_from_user_row(row)
            _presence_runtime_state_upsert(int(payload.user_id), is_visible=is_visible, reason=reason)
    with _presence_viewport_cache_lock:
        _presence_viewport_cache.clear()
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
