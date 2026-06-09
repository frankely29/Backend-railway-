"""
City Events — big NYC events for the map (sports games + optional Ticketmaster).

Fetches today's games from the free, keyless league schedule APIs (MLB / NHL /
NBA — Yankees, Mets, Knicks, Nets, Rangers, Islanders, Devils home games) on a
background thread, caches them in the `city_events` table, and serves them via
GET /city_events. Ticketmaster is an optional extra source (dormant without a
key). The frontend draws a pin per event and derives the upcoming / in-progress
/ "letting out" (best-pickup surge) state from each event's start time +
category — so the heavy let-out logic lives client-side and the backend just
supplies the facts.

Mirrors the codebase conventions:
  - external HTTP + secrets like paddle_client.py (httpx.Client + os.environ
    + an `*_is_configured()` guard),
  - a per-feature APIRouter + `ensure_*_schema()` like pickup_recording_feature.py,
  - a daemon refresh thread like main.py's `_start_avatar_asset_backfill`.

No API key configured => Ticketmaster is dormant, but the keyless sports
sources still run with no account, so the map has events out of the box.
Safe to deploy with no keys at all.
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

# ---------------------------------------------------------------------------
# Keyless sports sources (no API key / no account — official league schedule
# feeds). We keep only HOME games of the NYC-metro teams, so every kept game
# lands at a venue already present in _VENUE_FALLBACK below.
# ---------------------------------------------------------------------------
MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
NHL_SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule"   # + /<YYYY-MM-DD>
NBA_SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"

MLB_TEAM_IDS = {147, 121}                    # Yankees, Mets
NHL_TEAM_ABBREVS = {"NYR", "NYI", "NJD"}     # Rangers, Islanders, Devils
NBA_TEAM_TRICODES = {"NYK", "BKN"}           # Knicks, Nets

# League CDNs 403 non-browser agents, so every request carries a browser UA.
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Lowercased status markers (across leagues) that mean "not happening" -> skip.
_SKIP_STATUS_MARKERS = ("postpon", "cancel", "suspend", "ppd", "cncl", "susp")

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


def _parse_iso_utc(value: Any) -> Optional[int]:
    """ISO-8601 timestamp ('...Z' or with a UTC offset) -> unix seconds.
    Used by the sports feeds (MLB gameDate, NHL startTimeUTC, NBA
    gameDateTimeUTC). Returns None on anything unparseable; never raises."""
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:  # fast path: the common "....Z" form, no fractional seconds
        return int(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")
                   .replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        pass
    try:  # general path: offsets and/or fractional seconds
        iso = text[:-1] + "+00:00" if text.endswith("Z") else text
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
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
# Keyless sports feeds (MLB / NHL / NBA) — normalize + fetch
#
# Each normalizer mirrors normalize_event: .get()-guarded, never raises, and
# returns None to skip (away game, postponed/cancelled, unknown venue, or a
# missing field). Every kept game maps to the same row shape with
# category="sports", so the existing upsert/select/frontend path serves it
# unchanged. We keep only HOME games of the in-scope teams; a venue miss in
# _venue_coords_fallback safely skips the game.
# ---------------------------------------------------------------------------
def _status_is_skippable(status: Any) -> bool:
    s = str(status or "").strip().lower()
    return any(marker in s for marker in _SKIP_STATUS_MARKERS)


def _join_team_name(*parts: Any) -> str:
    return " ".join(p for p in (str(x or "").strip() for x in parts) if p)


def _nhl_team_name(team: Dict[str, Any]) -> str:
    """Readable name across NHL api-web shapes: placeName + commonName
    ('New York' + 'Rangers'), else a flat name field, else the abbrev."""
    def _d(v: Any) -> str:
        return str((v.get("default") if isinstance(v, dict) else v) or "").strip()
    name = _join_team_name(_d(team.get("placeName")), _d(team.get("commonName")))
    if name:
        return name
    for key in ("name", "fullName"):
        flat = _d(team.get(key))
        if flat:
            return flat
    return str(team.get("abbrev") or "").strip()


def normalize_mlb_game(game: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """One MLB statsapi game -> our sports row, or None to skip."""
    try:
        source_id = str(game.get("gamePk") or "").strip()
        if not source_id:
            return None
        if _status_is_skippable((game.get("status") or {}).get("detailedState")):
            return None
        teams = game.get("teams") or {}
        home = (teams.get("home") or {}).get("team") or {}
        away = (teams.get("away") or {}).get("team") or {}
        try:
            home_id = int(home.get("id"))
        except (TypeError, ValueError):
            return None
        if home_id not in MLB_TEAM_IDS:            # keep only our home games
            return None
        home_name = str(home.get("name") or "").strip()
        away_name = str(away.get("name") or "").strip()
        if not home_name or not away_name:
            return None
        venue_name = str((game.get("venue") or {}).get("name") or "").strip()
        coords = _venue_coords_fallback(venue_name)
        if not coords:
            return None
        start_at = _parse_iso_utc(game.get("gameDate"))
        if start_at is None:
            return None
        return {
            "source": "mlb",
            "source_id": source_id,
            "name": f"{away_name} at {home_name}",
            "category": "sports",
            "venue": venue_name,
            "lat": coords[0],
            "lng": coords[1],
            "start_at": start_at,
            "end_at": None,
            "url": "",
        }
    except Exception:
        logger.exception("[city_events] normalize_mlb_game failed")
        return None


def normalize_nhl_game(game: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """One NHL api-web game -> our sports row, or None to skip."""
    try:
        source_id = str(game.get("id") or "").strip()
        if not source_id:
            return None
        if _status_is_skippable(game.get("gameScheduleState")):
            return None
        home = game.get("homeTeam") or {}
        away = game.get("awayTeam") or {}
        if str(home.get("abbrev") or "").strip().upper() not in NHL_TEAM_ABBREVS:
            return None
        venue_name = str((game.get("venue") or {}).get("default") or "").strip()
        coords = _venue_coords_fallback(venue_name)
        if not coords:
            return None
        start_at = _parse_iso_utc(game.get("startTimeUTC"))
        if start_at is None:
            return None
        home_name = _nhl_team_name(home)
        away_name = _nhl_team_name(away)
        if not home_name or not away_name:
            return None
        return {
            "source": "nhl",
            "source_id": source_id,
            "name": f"{away_name} at {home_name}",
            "category": "sports",
            "venue": venue_name,
            "lat": coords[0],
            "lng": coords[1],
            "start_at": start_at,
            "end_at": None,
            "url": "",
        }
    except Exception:
        logger.exception("[city_events] normalize_nhl_game failed")
        return None


def normalize_nba_game(game: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """One NBA scheduleLeagueV2 game -> our sports row, or None to skip."""
    try:
        source_id = str(game.get("gameId") or "").strip()
        if not source_id:
            return None
        if _status_is_skippable(game.get("gameStatusText")):
            return None
        home = game.get("homeTeam") or {}
        away = game.get("awayTeam") or {}
        if str(home.get("teamTricode") or "").strip().upper() not in NBA_TEAM_TRICODES:
            return None
        venue_name = str(game.get("arenaName") or "").strip()
        coords = _venue_coords_fallback(venue_name)
        if not coords:
            return None
        start_at = _parse_iso_utc(game.get("gameDateTimeUTC"))
        if start_at is None:
            return None
        home_name = _join_team_name(home.get("teamCity"), home.get("teamName"))
        away_name = _join_team_name(away.get("teamCity"), away.get("teamName"))
        if not home_name or not away_name:
            return None
        return {
            "source": "nba",
            "source_id": source_id,
            "name": f"{away_name} at {home_name}",
            "category": "sports",
            "venue": venue_name,
            "lat": coords[0],
            "lng": coords[1],
            "start_at": start_at,
            "end_at": None,
            "url": "",
        }
    except Exception:
        logger.exception("[city_events] normalize_nba_game failed")
        return None


def _within_today_window(start_at: int, now_utc: datetime) -> bool:
    """True if a game falls in the same [now-MAX_SPAN, end_of_today_NYC] window
    select_events_for_today serves — drops games already long over."""
    now = int(now_utc.timestamp())
    return (now - MAX_EVENT_SPAN_SECONDS) <= start_at <= _end_of_today_nyc_unix(now_utc)


def _collect_sports(raw_games, normalizer, now_utc: datetime) -> List[Dict[str, Any]]:
    """Normalize a league's games; drop skips, out-of-window games, and dupes."""
    out: List[Dict[str, Any]] = []
    seen = set()
    for g in raw_games or []:
        n = normalizer(g if isinstance(g, dict) else {})
        if not n or not _within_today_window(n["start_at"], now_utc):
            continue
        key = (n["source"], n["source_id"])
        if key in seen:
            continue
        seen.add(key)
        out.append(n)
    return out


def _get_json(url: str, params: Optional[Dict[str, str]] = None) -> Optional[Any]:
    """GET JSON with a browser UA + timeout. None on any error or 4xx/5xx."""
    import httpx  # lazy: keeps the module importable without httpx installed
    try:
        with httpx.Client(timeout=_HTTP_TIMEOUT, headers={"User-Agent": _BROWSER_UA}) as client:
            resp = client.get(url, params=params)
            if resp.status_code >= 400:
                logger.warning("[city_events] GET %s -> %s: %s", url, resp.status_code, resp.text[:200])
                return None
            return resp.json()
    except Exception:
        logger.exception("[city_events] GET %s failed", url)
        return None


def fetch_mlb_today() -> List[Dict[str, Any]]:
    """Today's Yankees/Mets home games from MLB statsapi (keyless). [] on error."""
    now_utc = datetime.now(timezone.utc)
    today = now_utc.astimezone(NYC_TZ).strftime("%Y-%m-%d")
    data = _get_json(MLB_SCHEDULE_URL, {
        "sportId": "1",
        "teamId": ",".join(str(t) for t in sorted(MLB_TEAM_IDS)),
        "startDate": today,
        "endDate": today,
    })
    if not data:
        return []
    games: List[Dict[str, Any]] = []
    for date_block in (data.get("dates") or []):
        games.extend((date_block or {}).get("games") or [])
    return _collect_sports(games, normalize_mlb_game, now_utc)


def fetch_nhl_today() -> List[Dict[str, Any]]:
    """Today's Rangers/Islanders/Devils home games from NHL api-web (keyless)."""
    now_utc = datetime.now(timezone.utc)
    today = now_utc.astimezone(NYC_TZ).strftime("%Y-%m-%d")
    data = _get_json(f"{NHL_SCHEDULE_URL}/{today}")
    if not data:
        return []
    games: List[Dict[str, Any]] = []
    for block in (data.get("gameWeek") or []):       # a week of days; keep today
        block_date = str((block or {}).get("date") or "")
        if block_date and block_date != today:
            continue
        games.extend((block or {}).get("games") or [])
    return _collect_sports(games, normalize_nhl_game, now_utc)


def fetch_nba_today() -> List[Dict[str, Any]]:
    """Today's Knicks/Nets home games from the NBA static schedule JSON."""
    now_utc = datetime.now(timezone.utc)
    bucket = now_utc.astimezone(NYC_TZ).strftime("%m/%d/%Y")  # NBA gameDate prefix
    data = _get_json(NBA_SCHEDULE_URL)
    if not data:
        return []
    games: List[Dict[str, Any]] = []
    for date_block in ((data.get("leagueSchedule") or {}).get("gameDates") or []):
        if bucket not in str((date_block or {}).get("gameDate") or ""):  # pre-filter by day
            continue
        games.extend((date_block or {}).get("games") or [])
    return _collect_sports(games, normalize_nba_game, now_utc)


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
    """Fetch every source -> merge -> upsert -> prune. Used by the worker and
    the admin endpoint. Each source runs in its own try/except so one failing
    feed can't sink the batch. Ticketmaster stays dormant (returns [] without a
    key); the MLB/NHL/NBA feeds are keyless. Returns per-source counts."""
    sources = (
        ("ticketmaster", fetch_nyc_events_today),
        ("mlb", fetch_mlb_today),
        ("nhl", fetch_nhl_today),
        ("nba", fetch_nba_today),
    )
    counts: Dict[str, int] = {}
    merged: List[Dict[str, Any]] = []
    seen = set()
    for name, fetch in sources:
        try:
            rows = fetch()
        except Exception:
            logger.exception("[city_events] %s fetch raised", name)
            rows = []
        counts[f"src_{name}"] = len(rows)
        for r in rows:
            key = (r["source"], r["source_id"])
            if key in seen:
                continue
            seen.add(key)
            merged.append(r)
    stored = upsert_events(merged)
    prune_old_events()
    counts["fetched"] = len(merged)
    counts["stored"] = stored
    return counts


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
    _city_events_started = True
    if not events_api_is_configured():
        logger.info("[city_events] no TICKETMASTER_API_KEY — running keyless sports sources only")
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
    """Force an immediate refresh of all sources (ops/testing). Keyless sports
    always run; Ticketmaster contributes only when a key is set."""
    _ = user
    result = refresh_city_events_once()
    return {"ok": True, **result}
