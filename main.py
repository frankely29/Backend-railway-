from __future__ import annotations

import hmac
import hashlib
import gzip
import json
import math
import os
import copy
import re
import sqlite3
import threading
import time
import traceback
from collections import defaultdict, deque
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Depends, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pyproj import Transformer
from shapely.geometry import MultiPolygon, Point, Polygon, mapping, shape
from shapely.ops import transform, unary_union
from starlette.middleware.base import BaseHTTPMiddleware

from hotspot_experiments import (
    log_recommendation_outcome,
    log_zone_bins,
    prune_experiment_tables,
)
from hotspot_scoring import score_zones
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
    _auth_user_from_request,
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
    _make_token,
    _user_block_state,
    _require_jwt_secret,
    ENFORCE_TRIAL,
    require_user,
)

# =========================================================
# Paths (Railway volume)
# =========================================================
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
FRAMES_DIR = Path(os.environ.get("FRAMES_DIR", str(DATA_DIR / "frames")))
TIMELINE_PATH = FRAMES_DIR / "timeline.json"
DAY_TENDENCY_DIR = DATA_DIR / "day_tendency"
DAY_TENDENCY_MODEL_PATH = DAY_TENDENCY_DIR / "model.json"
NYC_TZ = ZoneInfo("America/New_York")
DAY_TENDENCY_VERSION = "borough_tendency_v2"

DEFAULT_BIN_MINUTES = int(os.environ.get("DEFAULT_BIN_MINUTES", "20"))
DEFAULT_MIN_TRIPS_PER_WINDOW = int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25"))

LOCK_PATH = DATA_DIR / ".generate.lock"


# Auth / Admin config
JWT_SECRET = os.environ.get("JWT_SECRET", "")  # REQUIRED (set in Railway)
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "").strip().lower()
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
ADMIN_BOOTSTRAP_TOKEN = os.environ.get("ADMIN_BOOTSTRAP_TOKEN", "").strip()
DEBUG_VERBOSE_LOGS = str(os.environ.get("DEBUG_VERBOSE_LOGS", "0")).strip().lower() in ("1", "true", "yes", "on")

TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "7"))
TOKEN_TTL_SECONDS = int(os.environ.get("TOKEN_TTL_SECONDS", str(30 * 24 * 3600)))  # 30 days
PRESENCE_STALE_SECONDS = int(os.environ.get("PRESENCE_STALE_SECONDS", "300"))  # 5 min
EVENT_DEFAULT_WINDOW_SECONDS = int(os.environ.get("EVENT_DEFAULT_WINDOW_SECONDS", str(24 * 3600)))  # 24h
MAX_AVATAR_DATA_URL_LENGTH = int(os.environ.get("MAX_AVATAR_DATA_URL_LENGTH", "20000"))
ALLOWED_MAP_IDENTITY_MODES = {"name", "avatar"}
RESPONSE_GZIP_MIN_BYTES = int(os.environ.get("RESPONSE_GZIP_MIN_BYTES", "1024"))
PRESENCE_VIEWPORT_CACHE_TTL_SECONDS = float(os.environ.get("PRESENCE_VIEWPORT_CACHE_TTL_SECONDS", "3"))
PRESENCE_VIEWPORT_CACHE_MAX = int(os.environ.get("PRESENCE_VIEWPORT_CACHE_MAX", "128"))
PRESENCE_DELTA_MAX_LIMIT = int(os.environ.get("PRESENCE_DELTA_MAX_LIMIT", "500"))
AVATAR_THUMB_IMMUTABLE_CACHE_SECONDS = int(os.environ.get("AVATAR_THUMB_IMMUTABLE_CACHE_SECONDS", str(30 * 24 * 3600)))
AVATAR_BACKFILL_BATCH_SIZE = int(os.environ.get("AVATAR_BACKFILL_BATCH_SIZE", "25"))

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
_timeline_cache_entry: Dict[str, Any] = {}
_timeline_cache_lock = threading.Lock()
_frame_cache: Dict[int, Dict[str, Any]] = {}
_frame_cache_order: deque[int] = deque()
_frame_cache_lock = threading.Lock()
FRAME_CACHE_MAX = 8
ARTIFACT_CACHE_CONTROL = "public, max-age=60"
PICKUP_RECENT_CACHE_TTL_SECONDS = float(os.environ.get("PICKUP_RECENT_CACHE_TTL_SECONDS", "10"))
PICKUP_RECENT_CACHE_MAX = int(os.environ.get("PICKUP_RECENT_CACHE_MAX", "64"))
PICKUP_HOTSPOT_CACHE_TTL_SECONDS = float(os.environ.get("PICKUP_HOTSPOT_CACHE_TTL_SECONDS", "180"))
PICKUP_HOTSPOT_CACHE_STALE_SECONDS = float(os.environ.get("PICKUP_HOTSPOT_CACHE_STALE_SECONDS", "900"))
PICKUP_SCORE_CACHE_TTL_SECONDS = float(os.environ.get("PICKUP_SCORE_CACHE_TTL_SECONDS", "15"))
PICKUP_EXPERIMENT_PRUNE_INTERVAL_SECONDS = float(os.environ.get("PICKUP_EXPERIMENT_PRUNE_INTERVAL_SECONDS", "300"))
_pickup_recent_cache: Dict[str, Dict[str, Any]] = {}
_pickup_recent_cache_lock = threading.Lock()
_pickup_zone_score_bundle_cache: Dict[str, Dict[str, Any]] = {}
_pickup_zone_score_bundle_lock = threading.Lock()
_pickup_zone_maintenance_lock = threading.Lock()
_pickup_last_experiment_prune_monotonic = 0.0
_presence_viewport_cache: Dict[str, Dict[str, Any]] = {}
_presence_viewport_cache_lock = threading.Lock()
_avatar_backfill_started = False
_perf_metrics_lock = threading.Lock()
_perf_metrics: Dict[str, int] = defaultdict(int)
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


def _has_frames() -> bool:
    try:
        return TIMELINE_PATH.exists() and TIMELINE_PATH.stat().st_size > 0
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


def _read_timeline_cached() -> Dict[str, Any]:
    stat_result = TIMELINE_PATH.stat()
    mtime = stat_result.st_mtime
    size = int(stat_result.st_size)
    etag = _etag_for_path(TIMELINE_PATH, mtime, size)
    with _timeline_cache_lock:
        cached = _timeline_cache_entry
        if cached and cached.get("mtime") == mtime and cached.get("size") == size:
            _record_perf_metric("timeline.cache_hit")
            return cached
        _record_perf_metric("timeline.cache_miss")
        data = _read_json(TIMELINE_PATH)
        _timeline_cache_entry.clear()
        _timeline_cache_entry.update({"data": data, "mtime": mtime, "size": size, "etag": etag})
        return dict(_timeline_cache_entry)


def _read_frame_cached(idx: int) -> Dict[str, Any]:
    frame_path = FRAMES_DIR / f"frame_{idx:06d}.json"
    if not frame_path.exists():
        raise HTTPException(status_code=404, detail=f"frame not found: {idx}")

    stat_result = frame_path.stat()
    mtime = stat_result.st_mtime
    size = int(stat_result.st_size)
    etag = _etag_for_path(frame_path, mtime, size)
    with _frame_cache_lock:
        cached = _frame_cache.get(idx)
        if cached is not None and cached.get("mtime") == mtime and cached.get("size") == size:
            _record_perf_metric("frame.cache_hit")
            try:
                _frame_cache_order.remove(idx)
            except ValueError:
                pass
            _frame_cache_order.append(idx)
            return cached

        _record_perf_metric("frame.cache_miss")
        data = _read_json(frame_path)
        _frame_cache[idx] = {"data": data, "mtime": mtime, "size": size, "etag": etag}
        try:
            _frame_cache_order.remove(idx)
        except ValueError:
            pass
        _frame_cache_order.append(idx)
        while len(_frame_cache_order) > FRAME_CACHE_MAX:
            evicted_idx = _frame_cache_order.popleft()
            _frame_cache.pop(evicted_idx, None)
        return _frame_cache[idx]


def _has_day_tendency_model() -> bool:
    try:
        return DAY_TENDENCY_MODEL_PATH.exists() and DAY_TENDENCY_MODEL_PATH.stat().st_size > 0
    except Exception:
        return False


def _read_day_tendency_model() -> Dict[str, Any]:
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
        return True
    except Exception:
        return False


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

    return build_day_tendency_model(
        parquet_files=parquets,
        out_dir=DAY_TENDENCY_DIR,
        zones_geojson_path=zones_path,
        bin_minutes=bin_minutes,
    )


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
    from build_day_tendency import build_day_tendency_model
    from build_hotspot import ensure_zones_geojson, build_hotspots_frames

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

        frames_result = build_hotspots_frames(
            parquet_files=parquets,
            zones_geojson_path=zones_path,
            out_dir=FRAMES_DIR,
            bin_minutes=bin_minutes,
            min_trips_per_window=min_trips_per_window,
        )
        day_tendency_result = build_day_tendency_model(
            parquet_files=parquets,
            out_dir=DAY_TENDENCY_DIR,
            zones_geojson_path=zones_path,
            bin_minutes=bin_minutes,
        )
        result = {
            "frames": frames_result,
            "day_tendency": day_tendency_result,
        }

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


def _ghost_visible_sql(column_name: str) -> str:
    if DB_BACKEND == "postgres":
        return f"({column_name} IS NULL OR {column_name} = FALSE)"
    return f"({column_name} IS NULL OR CAST({column_name} AS INTEGER) = 0)"


def _presence_visibility_snapshot(max_age_sec: int) -> Dict[str, Any]:
    cutoff = int(time.time()) - max(5, min(3600, int(max_age_sec)))
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


def _presence_change_cursor_ms() -> int:
    return int(time.time() * 1000)


def _presence_runtime_state_upsert(user_id: int, *, is_visible: bool, reason: Optional[str] = None, changed_at_ms: Optional[int] = None) -> int:
    cursor_value = int(changed_at_ms if changed_at_ms is not None else _presence_change_cursor_ms())
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
        (int(user_id), cursor_value, 1 if is_visible else 0, safe_reason),
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
    badge_by_user = get_best_current_badges_for_users([int(r["user_id"]) for r in rows])
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


def _presence_rows_for_viewport(
    *,
    cutoff: int,
    bbox: Optional[Tuple[float, float, float, float]],
    limit: Optional[int],
) -> List[Any]:
    where_clauses = ["p.updated_at >= ?", _presence_visible_where_sql()]
    params: List[Any] = [cutoff]
    if bbox is not None:
        lo_lat, lo_lng, hi_lat, hi_lng = bbox
        where_clauses.append("p.lat BETWEEN ? AND ?")
        where_clauses.append("p.lng BETWEEN ? AND ?")
        params.extend([lo_lat, hi_lat, lo_lng, hi_lng])
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
    cutoff = int(time.time()) - max(5, min(3600, int(max_age_sec)))
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
        tuple(params + [safe_limit]),
    )

    changed_rows: List[Any] = []
    removed: List[Dict[str, Any]] = []
    next_cursor = int(updated_since_ms)
    for row in rows:
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
        "server_time_ms": _presence_change_cursor_ms(),
        "has_more": len(rows) >= safe_limit,
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
    get_progression_for_user,
    get_my_rank,
    get_overview_for_user,
)
from leaderboard_tracker import increment_pickup_count, record_presence_heartbeat
from games_routes import router as games_router
from games_service import ensure_games_schema, get_game_battle_stats_for_users, get_recent_battles_for_user
from pickup_recording_feature import (
    router as pickup_recording_router,
    ensure_pickup_recording_schema,
    pickup_log_not_voided_sql,
    record_pickup_presence_heartbeat,
    create_pickup_record,
)

app.include_router(chat_router)
app.include_router(leaderboard_router)
app.include_router(games_router)
app.include_router(pickup_recording_router)

# =========================================================
# Startup
# =========================================================
@app.on_event("startup")
def startup():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    _db_init()
    init_leaderboard_schema()
    ensure_games_schema()
    ensure_pickup_recording_schema()
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

    # Auto-fill generate state if frames/day tendency already exist
    try:
        frames_ready = _has_frames()
        day_tendency_ready = _day_tendency_model_is_current()
        zones_ok = (DATA_DIR / "taxi_zones.geojson").exists()
        parquets_ok = len(_list_parquets()) > 0

        if frames_ready and day_tendency_ready:
            try:
                tl = _read_json(TIMELINE_PATH)
                _set_state(
                    state="done",
                    bin_minutes=DEFAULT_BIN_MINUTES,
                    min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
                    result={
                        "ok": True,
                        "count": tl.get("count"),
                        "day_tendency": {"ok": True, "built_at_startup": False},
                    },
                )
            except Exception:
                _set_state(
                    state="done",
                    bin_minutes=DEFAULT_BIN_MINUTES,
                    min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
                    result={
                        "ok": True,
                        "day_tendency": {"ok": True, "built_at_startup": False},
                    },
                )
            return

        if frames_ready and not day_tendency_ready:
            print("[warn] day tendency model missing or stale; rebuilding at startup")
            if parquets_ok:
                try:
                    _build_day_tendency_only(DEFAULT_BIN_MINUTES)
                except Exception:
                    print("[warn] startup day tendency backfill failed")
                    print(traceback.format_exc())
            try:
                tl = _read_json(TIMELINE_PATH)
                _set_state(
                    state="done",
                    bin_minutes=DEFAULT_BIN_MINUTES,
                    min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
                    result={
                        "ok": True,
                        "count": tl.get("count"),
                        "day_tendency": {"ok": _has_day_tendency_model(), "built_at_startup": True},
                    },
                )
            except Exception:
                _set_state(
                    state="done",
                    bin_minutes=DEFAULT_BIN_MINUTES,
                    min_trips_per_window=DEFAULT_MIN_TRIPS_PER_WINDOW,
                    result={
                        "ok": True,
                        "day_tendency": {"ok": _has_day_tendency_model(), "built_at_startup": True},
                    },
                )
            return

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
            "/day_tendency/today",
            "/day_tendency/date/{ymd}",
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
        "trial_enforced": ENFORCE_TRIAL,
        "auth_enabled": bool(JWT_SECRET and len(JWT_SECRET) >= 24),
        "performance_metrics": _perf_metric_snapshot(),
    }


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


@app.get("/generate")
def generate_get(bin_minutes: int = DEFAULT_BIN_MINUTES, min_trips_per_window: int = DEFAULT_MIN_TRIPS_PER_WINDOW):
    return start_generate(bin_minutes, min_trips_per_window)


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


@app.get("/timeline")
def timeline(request: Request):
    if not _has_frames():
        raise HTTPException(status_code=409, detail="timeline not ready. Call /generate first.")
    cached = _read_timeline_cached()
    return _json_cached_response(request, cached["data"], etag=cached.get("etag"))


@app.get("/frame/{idx}")
def frame(idx: int, request: Request):
    if not _has_frames():
        raise HTTPException(status_code=409, detail="timeline not ready. Call /generate first.")
    cached = _read_frame_cached(idx)
    return _json_cached_response(request, cached["data"], etag=cached.get("etag"))


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
    battle_stats = get_game_battle_stats_for_users([int(user["id"])]).get(int(user["id"]), {})

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
        "battle_stats": battle_stats,
    }


@app.get("/drivers/{user_id}/profile")
def driver_profile(user_id: int, viewer: sqlite3.Row = Depends(require_user)):
    _ = viewer
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
    battle_stats = get_game_battle_stats_for_users([target_user_id]).get(target_user_id, {})
    recent_battles = get_recent_battles_for_user(target_user_id)

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
        "battle_stats": battle_stats,
        "recent_battles": recent_battles,
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
    zoom: Optional[int] = None,
    mode: str = "full",
    limit: Optional[int] = None,
    viewer: sqlite3.Row = Depends(require_user),  # REQUIRE AUTH (frontend already sends token)
):
    del viewer
    cutoff = int(time.time()) - max(5, min(3600, int(max_age_sec)))
    safe_mode = (mode or "full").strip().lower()
    if safe_mode not in {"full", "lite"}:
        raise HTTPException(status_code=400, detail="mode must be 'full' or 'lite'")
    safe_limit = None if limit is None else max(1, min(500, int(limit)))
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
    snapshot = _presence_visibility_snapshot(max_age_sec)
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
    zoom: Optional[int] = None,
    padding_ratio: float = 0.0,
    updated_since_ms: Optional[int] = None,
    include_removed: bool = True,
    limit: int = PRESENCE_DELTA_MAX_LIMIT,
    viewer: sqlite3.Row = Depends(require_user),
):
    del viewer
    safe_limit = max(1, min(PRESENCE_DELTA_MAX_LIMIT, int(limit)))
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
            limit=safe_limit,
            include_removed=bool(include_removed),
        )

    cutoff = int(time.time()) - max(5, min(3600, int(max_age_sec)))
    rows = _presence_rows_for_viewport(cutoff=cutoff, bbox=buffered_bbox, limit=safe_limit)
    items = _presence_row_payloads(rows, include_full_fields=False)
    cursor_row = _db_query_one("SELECT COALESCE(MAX(changed_at_ms), 0) AS cursor FROM presence_runtime_state")
    snapshot = _presence_visibility_snapshot(max_age_sec)
    return {
        "ok": True,
        "mode": "snapshot",
        "count": len(items),
        "items": items,
        "removed": [],
        "cursor": int(cursor_row["cursor"] or 0) if cursor_row else 0,
        "server_time_ms": _presence_change_cursor_ms(),
        "online_count": int(snapshot.get("online_count") or 0),
        "ghosted_count": int(snapshot.get("ghosted_count") or 0),
        "visible_count": len(items),
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
    zoom: Optional[int] = None,
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


def _build_zone_hotspot_components(
    zone_id: int,
    zone_meta: Dict[str, Any],
    point_rows: List[Dict[str, Any]],
    fallback: bool = False,
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
    }
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
    debug["component_point_counts"] = [int(c.get("point_count") or 0) for c in components[:2]]

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

    shaped_candidates: List[Dict[str, Any]] = []
    for rank, comp in enumerate(top_components):
        shaped = _shape_hotspot_component(comp, zone_proj)
        if shaped is None or shaped.is_empty:
            continue
        shaped_candidates.append({**comp, "polygon": shaped, "component_rank": rank})

    if not shaped_candidates:
        debug["second_hotspot_rejected_reason"] = "component_shaping_failed"
        return [], debug

    merged, merge_reason = _hotspot_merge_decision(shaped_candidates, selected_cells)
    debug["merged"] = merged
    debug["merge_reason"] = merge_reason

    final_components: List[Dict[str, Any]] = []
    if merged and len(shaped_candidates) > 1:
        merged_geom = unary_union([c["polygon"] for c in shaped_candidates]).intersection(zone_proj)
        if not merged_geom.is_empty:
            merged_component = {
                "polygon": merged_geom,
                "component_score": sum(float(c.get("component_score") or 0.0) for c in shaped_candidates),
                "peak_score": max(float(c.get("peak_score") or 0.0) for c in shaped_candidates),
                "point_count": sum(int(c.get("point_count") or 0) for c in shaped_candidates),
                "component_rank": 0,
                "merged_from_count": len(shaped_candidates),
                "cells": set().union(*[c.get("cells") or set() for c in shaped_candidates]),
            }
            final_components = [merged_component]
    if not final_components:
        for c in shaped_candidates[:2]:
            final_components.append({**c, "merged_from_count": 1})

    latest_created_at = max((int(p.get("created_at") or 0) for p in point_entries), default=0)
    sample_size = len(point_entries)
    zone_name = zone_meta.get("zone_name") or ((point_rows[0].get("zone_name") if point_rows else "") or "")
    borough = zone_meta.get("borough") or ((point_rows[0].get("borough") if point_rows else "") or "")
    signature = _pickup_zone_signature(point_rows)

    emitted: List[Dict[str, Any]] = []
    for hotspot_index, comp in enumerate(final_components[:2]):
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
        "recommended": True,
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
    zoom: Optional[int],
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


def _zoom_bucket(zoom: Optional[int]) -> int:
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
    zoom: Optional[int],
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


def _pickup_zone_same_timeslot_support(zone_ids: List[int], now_ts: int) -> Dict[int, float]:
    if not zone_ids:
        return {}
    slot = _current_timeslot_bin(now_ts)
    lookback = now_ts - (14 * 24 * 3600)
    placeholders = ",".join(["?"] * len(zone_ids))
    if DB_BACKEND == "postgres":
        timeslot_expr = "CAST((MOD(pl.created_at, 86400) / 60) / ? AS INTEGER)"
    else:
        timeslot_expr = "CAST(((pl.created_at % 86400) / 60) / ? AS INTEGER)"
    sql = f"""
        SELECT pl.zone_id, COUNT(*) AS c
        FROM pickup_logs pl
        WHERE pl.zone_id IN ({placeholders})
          AND {pickup_log_not_voided_sql('pl')}
          AND pl.created_at >= ?
          AND {timeslot_expr} = ?
        GROUP BY pl.zone_id
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
        SELECT pl.zone_id, COUNT(*) AS c
        FROM pickup_logs pl
        WHERE pl.zone_id IN ({placeholders})
          AND {pickup_log_not_voided_sql("pl")}
          AND pl.created_at >= ?
        GROUP BY pl.zone_id
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
        "requested_zone_ids": clean_zone_ids,
        "zone_hotspot_count": 0,
        "orphan_micro_hotspot_count": 0,
        "top_level_micro_hotspot_count": 0,
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

    features: List[Dict[str, Any]] = []
    for zone_id in clean_zone_ids:
        pts = zone_points.get(zone_id, [])
        zone_data = zone_geoms.get(zone_id)
        signature = zone_signatures.get(zone_id) or _pickup_zone_signature([])
        qualified = len(pts) >= PICKUP_ZONE_HOTSPOT_MIN_POINTS
        zone_debug: Optional[Dict[str, Any]] = None
        if include_debug:
            zone_debug = {
                "zone_id": zone_id,
                "zone_name": (zone_data or {}).get("zone_name") or "",
                "borough": (zone_data or {}).get("borough") or "",
                "point_count": len(pts),
                "qualified": qualified,
                "geometry_found": bool(zone_data),
                "cached_hit": False,
                "primary_attempted": False,
                "primary_ok": False,
                "fallback_attempted": False,
                "fallback_ok": False,
                "feature_emitted": False,
                "micro_hotspot_count": 0,
                "hotspot_method": "none",
                "signature": signature,
                "errors": [],
            }
            if qualified:
                debug["qualified_zone_ids"].append(zone_id)

        if not zone_data:
            if zone_debug is not None:
                zone_debug["errors"].append("geometry_missing")
                debug["zones"].append(zone_debug)
            continue

        zone_features: List[Dict[str, Any]] = []
        cached_hit = False
        with _pickup_zone_hotspot_cache_lock:
            cached = _pickup_zone_hotspot_feature_cache.get(zone_id)
            if (
                cached
                and cached.get("signature") == signature
                and cached.get("features")
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
                zone_features, zone_component_debug = _build_zone_hotspot_components(zone_id, zone_data, pts, fallback=False)
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
                zone_features, zone_component_debug = _build_zone_hotspot_components(zone_id, zone_data, pts, fallback=True)
                if zone_debug is not None:
                    zone_debug["fallback_ok"] = bool(zone_features)
                    if zone_features:
                        zone_debug["hotspot_method"] = "fallback"
            except Exception:
                if zone_debug is not None:
                    zone_debug["errors"].append("fallback_hotspot_build_failed")
                print(f"[warn] Failed to generate fallback pickup zone hotspot for zone {zone_id}", traceback.format_exc())

        if zone_debug is not None and zone_component_debug:
            zone_debug.update(zone_component_debug)

        if not zone_features:
            with _pickup_zone_hotspot_cache_lock:
                _pickup_zone_hotspot_feature_cache.pop(zone_id, None)
            if zone_debug is not None:
                debug["zones"].append(zone_debug)
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
                if score.recommended and not score_bundle_fresh:
                    try:
                        log_recommendation_outcome(
                            _db_exec,
                            recommended_at=now_ts,
                            zone_id=zone_id,
                            score=score.final_score,
                            confidence=score.confidence,
                            cluster_id=None,
                        )
                    except Exception:
                        if zone_debug is not None:
                            zone_debug["errors"].append("log_recommendation_outcome_failed")
                        print(f"[warn] Failed to log recommendation outcome for zone {zone_id}", traceback.format_exc())
            features.append(feature)

        if zone_debug is not None:
            zone_debug["feature_emitted"] = bool(zone_features)
            zone_debug["micro_hotspot_count"] = zone_micro_total
            debug["rendered_zone_ids"].append(zone_id)
            debug["zones"].append(zone_debug)

    _cleanup_pickup_zone_caches(now_monotonic)
    payload = {"type": "FeatureCollection", "features": features}
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
    clean_zone_ids = clean_zone_ids[:256]
    placeholders = ",".join(["?"] * len(clean_zone_ids))

    sql = f"""
        WITH ranked AS (
            SELECT
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
                return copy.deepcopy(cached["payload"])
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

    zone_stats = _pickup_zone_stats(zone_ids_for_stats, sample_limit=safe_zone_sample_limit)
    hotspot_zone_ids = [int(z.get("zone_id")) for z in zone_stats if z.get("zone_id") is not None]
    pickup_hotspot_debug: Dict[str, Any] = {}
    try:
        zone_hotspots, pickup_hotspot_debug = _pickup_zone_hotspots_with_debug(
            hotspot_zone_ids,
            include_debug=include_debug,
        )
    except Exception:
        print("[warn] Failed to attach pickup zone hotspots", traceback.format_exc())
        zone_hotspots = {"type": "FeatureCollection", "features": []}
        pickup_hotspot_debug = {
            "min_points_threshold": PICKUP_ZONE_HOTSPOT_MIN_POINTS,
            "requested_zone_ids": hotspot_zone_ids,
            "zone_hotspot_count": 0,
            "orphan_micro_hotspot_count": 0,
            "top_level_micro_hotspot_count": 0,
            "qualified_zone_ids": [],
            "rendered_zone_ids": [],
            "global_errors": ["pickup_zone_hotspots_with_debug_failed"],
            "zones": [],
        }
    # Return top-level micro-hotspots so frontend can render compact clusters directly.
    micro_hotspots = _flatten_zone_micro_hotspots(zone_hotspots)
    zone_features = zone_hotspots.get("features") if isinstance(zone_hotspots, dict) else []
    zone_hotspot_count = len(zone_features) if isinstance(zone_features, list) else 0
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
    if not include_debug:
        with _pickup_recent_cache_lock:
            _pickup_recent_cache[cache_key] = {
                "payload": copy.deepcopy(response),
                "expires_at_monotonic": now_monotonic + PICKUP_RECENT_CACHE_TTL_SECONDS,
                "last_access_monotonic": now_monotonic,
            }
            _purge_pickup_recent_cache(now_monotonic)
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
