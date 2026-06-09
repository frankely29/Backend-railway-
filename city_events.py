"""
City Events — big NYC events (concerts, sports, conventions) for the map.

Fetches today's major events from the Ticketmaster Discovery API on a
background thread, caches them in the `city_events` table, and serves them
via GET /city_events. The frontend draws a pin per event and derives the
upcoming / in-progress / "letting out" (best-pickup surge) state from each
event's start time + category — so the heavy let-out logic lives client-side
and the backend just supplies the facts.

Mirrors the codebase conventions:
  - external HTTP + secrets like paddle_client.py (httpx.Client + os.environ
    + an `*_is_configured()` guard),
  - a per-feature APIRouter + `ensure_*_schema()` like pickup_recording_feature.py,
  - a daemon refresh thread like main.py's `_start_avatar_asset_backfill`.

No API key configured => the feature is dormant (no fetch, empty endpoint),
so it is safe to deploy before TICKETMASTER_API_KEY is set.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException

from core import DB_BACKEND, _db_exec, _db_query_all, _db_run_in_transaction, _sql, require_user

logger = logging.getLogger(__name__)
router = APIRouter()

NYC_TZ = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Config (Railway env vars; all optional except the key)
# ---------------------------------------------------------------------------
TICKETMASTER_API_KEY = os.environ.get("TICKETMASTER_API_KEY", "").strip()
TM_EVENTS_URL = "https://app.ticketmaster.com/discovery/v2/events.json"
# New York DMA — covers the five boroughs plus the close NJ/LI venues
# (MetLife, Prudential Center, UBS Arena). Adjustable to latlong+radius.
TM_DMA_NEW_YORK = os.environ.get("CITY_EVENTS_TM_DMA", "345").strip()

# Ticketmaster segment ids -> our category. Music=concert, Sports=sports,
# Miscellaneous=convention (expos / comic-cons / fan events).
TM_SEGMENTS: Dict[str, str] = {
    "KZFzniwnSyZfZ7v7nJ": "concert",
    "KZFzniwnSyZfZ7v7nE": "sports",
    "KZFzniwnSyZfZ7v7n1": "convention",
}

CITY_EVENTS_REFRESH_SECONDS = int(os.environ.get("CITY_EVENTS_REFRESH_SECONDS", "1800"))
# Keep + serve events for this long after their start so ones that are still
# letting out from earlier today stay on the map (a long show + let-out tail).
MAX_EVENT_SPAN_SECONDS = int(os.environ.get("CITY_EVENTS_MAX_SPAN_SECONDS", str(6 * 3600)))
_MAX_PAGES = 3
_HTTP_TIMEOUT = 10.0

# Coordinate fallback for the rare event whose venue has no lat/lng in the
# API response. Top NYC-area venues only — Ticketmaster supplies coordinates
# for essentially every real event, so this is belt-and-suspenders.
_VENUE_FALLBACK: Dict[str, Tuple[float, float]] = {
    "madison square garden": (40.7505, -73.9934),
    "barclays center": (40.6826, -73.9754),
    "yankee stadium": (40.8296, -73.9262),
    "citi field": (40.7571, -73.8458),
    "metlife stadium": (40.8135, -74.0745),
    "ubs arena": (40.7106, -73.7227),
    "prudential center": (40.7336, -74.1711),
    "radio city music hall": (40.7600, -73.9800),
    "carnegie hall": (40.7651, -73.9799),
    "lincoln center": (40.7725, -73.9835),
    "javits center": (40.7577, -74.0024),
    "forest hills stadium": (40.7197, -73.8455),
    "barclays": (40.6826, -73.9754),
}


def events_api_is_configured() -> bool:
    return bool(TICKETMASTER_API_KEY)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def ensure_city_events_schema() -> None:
    id_col = (
        "id BIGSERIAL PRIMARY KEY"
        if DB_BACKEND == "postgres"
        else "id INTEGER PRIMARY KEY AUTOINCREMENT"
    )
    _db_exec(
        f"""
        CREATE TABLE IF NOT EXISTS city_events (
          {id_col},
          source TEXT NOT NULL,
          source_id TEXT NOT NULL,
          name TEXT NOT NULL,
          category TEXT NOT NULL,
          venue TEXT,
          lat DOUBLE PRECISION NOT NULL,
          lng DOUBLE PRECISION NOT NULL,
          start_at BIGINT NOT NULL,
          end_at BIGINT,
          url TEXT,
          fetched_at BIGINT NOT NULL,
          UNIQUE(source, source_id)
        )
        """
    )
    try:
        _db_exec("CREATE INDEX IF NOT EXISTS idx_city_events_start ON city_events(start_at)")
    except Exception:
        logger.exception("[city_events] index create failed")


_UPSERT_SQL = """
INSERT INTO city_events
  (source, source_id, name, category, venue, lat, lng, start_at, end_at, url, fetched_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source, source_id) DO UPDATE SET
  name = excluded.name,
  category = excluded.category,
  venue = excluded.venue,
  lat = excluded.lat,
  lng = excluded.lng,
  start_at = excluded.start_at,
  end_at = excluded.end_at,
  url = excluded.url,
  fetched_at = excluded.fetched_at
"""


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------
def _end_of_today_nyc_unix(now_utc: Optional[datetime] = None) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
    nyc_now = now_utc.astimezone(NYC_TZ)
    end = nyc_now.replace(hour=23, minute=59, second=59, microsecond=0)
    return int(end.timestamp())


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_start_unix(start: Dict[str, Any]) -> Optional[int]:
    """Ticketmaster `dates.start` -> unix seconds. Prefers the absolute UTC
    `dateTime`; falls back to localDate (+localTime, NYC) and finally an 8pm
    NYC default for TBA-time entries."""
    dt_str = start.get("dateTime")
    if dt_str:
        try:
            dt = datetime.strptime(str(dt_str), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            pass
    local_date = start.get("localDate")
    if local_date:
        local_time = str(start.get("localTime") or "20:00:00")
        if len(local_time) == 5:  # "HH:MM"
            local_time += ":00"
        try:
            naive = datetime.strptime(f"{local_date} {local_time[:8]}", "%Y-%m-%d %H:%M:%S")
            return int(naive.replace(tzinfo=NYC_TZ).timestamp())
        except Exception:
            pass
    return None


def _venue_coords_fallback(venue_name: str) -> Optional[Tuple[float, float]]:
    key = (venue_name or "").strip().lower()
    if not key:
        return None
    if key in _VENUE_FALLBACK:
        return _VENUE_FALLBACK[key]
    for known, coords in _VENUE_FALLBACK.items():
        if known in key:
            return coords
    return None


# ---------------------------------------------------------------------------
# Fetch + normalize (pure-ish: no DB writes)
# ---------------------------------------------------------------------------
def normalize_event(ev: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """One Ticketmaster event -> our row dict, or None to skip (no id/name,
    unknown category, or no coordinates). Defensive: never raises."""
    try:
        source_id = str(ev.get("id") or "").strip()
        name = str(ev.get("name") or "").strip()
        if not source_id or not name:
            return None

        seg_id = ""
        classifications = ev.get("classifications") or []
        if classifications:
            seg = (classifications[0] or {}).get("segment") or {}
            seg_id = str(seg.get("id") or "")
        category = TM_SEGMENTS.get(seg_id)
        if not category:
            return None  # not one of the requested segments

        venue_name = ""
        lat: Optional[float] = None
        lng: Optional[float] = None
        venues = ((ev.get("_embedded") or {}).get("venues")) or []
        if venues:
            v0 = venues[0] or {}
            venue_name = str(v0.get("name") or "").strip()
            loc = v0.get("location") or {}
            try:
                lat = float(loc.get("latitude"))
                lng = float(loc.get("longitude"))
            except (TypeError, ValueError):
                lat = lng = None
        if lat is None or lng is None:
            coords = _venue_coords_fallback(venue_name)
            if coords:
                lat, lng = coords
        if lat is None or lng is None:
            return None

        start_at = _parse_start_unix((ev.get("dates") or {}).get("start") or {})
        if start_at is None:
            return None

        return {
            "source": "ticketmaster",
            "source_id": source_id,
            "name": name,
            "category": category,
            "venue": venue_name,
            "lat": lat,
            "lng": lng,
            "start_at": start_at,
            "end_at": None,  # Ticketmaster rarely provides an end; estimated client-side
            "url": str(ev.get("url") or ""),
        }
    except Exception:
        logger.exception("[city_events] normalize failed")
        return None


def fetch_nyc_events_today() -> List[Dict[str, Any]]:
    """Fetch today's NYC concerts/sports/conventions from Ticketmaster.
    Returns normalized, de-duplicated rows. Empty if unconfigured or on error."""
    if not events_api_is_configured():
        return []

    import httpx  # lazy: only needed for the live fetch (keeps the rest importable without it)

    now_utc = datetime.now(timezone.utc)
    base_params: List[Tuple[str, str]] = [
        ("apikey", TICKETMASTER_API_KEY),
        ("dmaId", TM_DMA_NEW_YORK),
        ("startDateTime", _iso_z(now_utc)),
        ("endDateTime", _iso_z(datetime.fromtimestamp(_end_of_today_nyc_unix(now_utc), timezone.utc))),
        ("size", "200"),
        ("sort", "date,asc"),
    ]
    for seg_id in TM_SEGMENTS:  # repeated segmentId = OR filter
        base_params.append(("segmentId", seg_id))

    raw: List[Dict[str, Any]] = []
    try:
        with httpx.Client(timeout=_HTTP_TIMEOUT) as client:
            for page in range(_MAX_PAGES):
                resp = client.get(TM_EVENTS_URL, params=base_params + [("page", str(page))])
                if resp.status_code >= 400:
                    logger.warning("[city_events] Ticketmaster %s: %s", resp.status_code, resp.text[:300])
                    break
                data = resp.json()
                raw.extend(((data.get("_embedded") or {}).get("events")) or [])
                total_pages = int(((data.get("page") or {}).get("totalPages")) or 1)
                if page + 1 >= min(total_pages, _MAX_PAGES):
                    break
    except Exception:
        logger.exception("[city_events] Ticketmaster fetch failed")
        return []

    out: List[Dict[str, Any]] = []
    seen = set()
    for ev in raw:
        n = normalize_event(ev)
        if not n:
            continue
        key = (n["source"], n["source_id"])
        if key in seen:
            continue
        seen.add(key)
        out.append(n)
    return out


# ---------------------------------------------------------------------------
# DB write / read
# ---------------------------------------------------------------------------
def upsert_events(events: List[Dict[str, Any]]) -> int:
    if not events:
        return 0
    now = int(time.time())

    def _tx(conn, cur):
        for e in events:
            cur.execute(
                _sql(_UPSERT_SQL),
                (
                    e["source"], e["source_id"], e["name"], e["category"], e["venue"],
                    float(e["lat"]), float(e["lng"]), int(e["start_at"]),
                    e.get("end_at"), e.get("url") or "", now,
                ),
            )
        return len(events)

    return _db_run_in_transaction(_tx)


def prune_old_events() -> None:
    threshold = int(time.time()) - MAX_EVENT_SPAN_SECONDS
    _db_exec("DELETE FROM city_events WHERE start_at < ?", (threshold,))


def refresh_city_events_once() -> Dict[str, int]:
    """Fetch -> upsert -> prune. Used by the worker and the admin endpoint."""
    events = fetch_nyc_events_today()
    stored = upsert_events(events)
    prune_old_events()
    return {"fetched": len(events), "stored": stored}


def select_events_for_today() -> Dict[str, Any]:
    now = int(time.time())
    lower = now - MAX_EVENT_SPAN_SECONDS
    upper = _end_of_today_nyc_unix()
    rows = _db_query_all(
        "SELECT id, name, category, venue, lat, lng, start_at, end_at, url "
        "FROM city_events WHERE start_at >= ? AND start_at <= ? "
        "ORDER BY start_at ASC LIMIT 500",
        (lower, upper),
    )
    events: List[Dict[str, Any]] = []
    for r in rows:
        end_at = r["end_at"]
        events.append({
            "id": int(r["id"]),
            "name": str(r["name"] or ""),
            "category": str(r["category"] or "event"),
            "venue": str(r["venue"] or ""),
            "lat": float(r["lat"]),
            "lng": float(r["lng"]),
            "startAt": int(r["start_at"]),
            "endAt": int(end_at) if end_at is not None else None,
            "url": str(r["url"] or ""),
        })
    return {"events": events, "asOf": now}


# ---------------------------------------------------------------------------
# Background refresh thread (mirrors main.py's _start_avatar_asset_backfill)
# ---------------------------------------------------------------------------
_city_events_started = False


def _city_events_worker() -> None:
    while True:
        try:
            result = refresh_city_events_once()
            logger.info("[city_events] refreshed %s", result)
        except Exception:
            logger.exception("[city_events] refresh cycle failed")
        time.sleep(max(60, CITY_EVENTS_REFRESH_SECONDS))


def start_city_events_refresh() -> None:
    global _city_events_started
    if _city_events_started:
        return
    if not events_api_is_configured():
        logger.info("[city_events] TICKETMASTER_API_KEY not set — events refresh disabled")
        return
    _city_events_started = True
    threading.Thread(target=_city_events_worker, name="city-events-refresh", daemon=True).start()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
def _require_admin(user: Any = Depends(require_user)):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    raw = user["is_admin"] if "is_admin" in user.keys() else user.get("is_admin")
    is_admin = bool(raw) if isinstance(raw, bool) else int(raw or 0) == 1
    if not is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


@router.get("/city_events")
def city_events_list(user: Any = Depends(require_user)):
    """Today's big NYC events (+ ones still letting out from earlier today)."""
    _ = user
    return select_events_for_today()


@router.post("/admin/city_events/refresh")
def city_events_refresh(user: Any = Depends(_require_admin)):
    """Force an immediate Ticketmaster refresh (ops/testing)."""
    _ = user
    if not events_api_is_configured():
        raise HTTPException(status_code=503, detail="TICKETMASTER_API_KEY is not configured")
    result = refresh_city_events_once()
    return {"ok": True, **result}
