from __future__ import annotations

import os
import json
import time
import threading
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

import httpx

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
# Context overlay cache (places/events) — separate from TLC data
# ----------------------------
_CONTEXT_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}  # key -> (expires_unix, payload)
_CONTEXT_TTL_SEC = int(os.environ.get("CONTEXT_TTL_SEC", "600"))  # 10 minutes default


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    hit = _CONTEXT_CACHE.get(key)
    if not hit:
        return None
    exp, payload = hit
    if now > exp:
        _CONTEXT_CACHE.pop(key, None)
        return None
    return payload


def _cache_set(key: str, payload: Dict[str, Any], ttl_sec: int = _CONTEXT_TTL_SEC) -> None:
    _CONTEXT_CACHE[key] = (time.time() + ttl_sec, payload)


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
    - Else -> start generation in background using defaults
    """
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        FRAMES_DIR.mkdir(parents=True, exist_ok=True)

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


# ----------------------------
# Context overlay helpers
# ----------------------------
def _parse_bbox(bbox: str) -> Tuple[float, float, float, float]:
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("Expected bbox=minLng,minLat,maxLng,maxLat")
    min_lng, min_lat, max_lng, max_lat = [float(x) for x in parts]
    return (min_lng, min_lat, max_lng, max_lat)


def _bbox_cache_key(min_lng: float, min_lat: float, max_lng: float, max_lat: float, types: str) -> str:
    return f"{round(min_lng,3)},{round(min_lat,3)},{round(max_lng,3)},{round(max_lat,3)}|{types}"


async def fetch_ticketmaster_concerts(bbox_t: Tuple[float, float, float, float], limit: int = 80) -> List[Dict[str, Any]]:
    api_key = os.getenv("TICKETMASTER_API_KEY", "").strip()
    if not api_key:
        return []

    min_lng, min_lat, max_lng, max_lat = bbox_t
    lat = (min_lat + max_lat) / 2.0
    lng = (min_lng + max_lng) / 2.0

    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {
        "apikey": api_key,
        "latlong": f"{lat},{lng}",
        "radius": "10",
        "unit": "miles",
        "size": str(limit),
        "sort": "date,asc",
        "classificationName": "music",
    }

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            return []
        data = r.json()

    events = ((data.get("_embedded") or {}).get("events") or [])
    items: List[Dict[str, Any]] = []

    for ev in events:
        name = ev.get("name") or ""
        dates = ev.get("dates") or {}
        start_dt = ((dates.get("start") or {}).get("dateTime"))

        venues = (((ev.get("_embedded") or {}).get("venues")) or [])
        venue = venues[0] if venues else {}
        vname = venue.get("name") or ""
        loc = (venue.get("location") or {})

        try:
            vlat = float(loc.get("latitude"))
            vlng = float(loc.get("longitude"))
        except Exception:
            continue

        items.append({
            "type": "concert",
            "name": name,
            "venue": vname,
            "lat": vlat,
            "lng": vlng,
            "start": start_dt,
            "source": "ticketmaster",
        })

    return items


async def fetch_foursquare_places(
    bbox_t: Tuple[float, float, float, float],
    kind: str,
    limit: int = 60
) -> List[Dict[str, Any]]:
    """
    Uses ll + radius (more reliable than bbox for many accounts).
    kind: "nightlife" or "restaurants"
    """
    api_key = os.getenv("FOURSQUARE_API_KEY", "").strip()
    if not api_key:
        return []

    min_lng, min_lat, max_lng, max_lat = bbox_t
    lat = (min_lat + max_lat) / 2.0
    lng = (min_lng + max_lng) / 2.0

    url = "https://api.foursquare.com/v3/places/search"
    headers = {"Authorization": api_key, "Accept": "application/json"}

    q = "nightclub" if kind == "nightlife" else "restaurant"
    params = {
        "query": q,
        "ll": f"{lat},{lng}",
        "radius": "6000",       # meters
        "limit": str(limit),
        "sort": "POPULARITY",
    }

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params, headers=headers)
        if r.status_code != 200:
            # return empty rather than crashing the whole map
            return []
        data = r.json()

    items: List[Dict[str, Any]] = []
    for p in (data.get("results") or []):
        name = p.get("name") or ""
        main = ((p.get("geocodes") or {}).get("main") or {})
        plat = main.get("latitude")
        plng = main.get("longitude")
        if plat is None or plng is None:
            continue
        items.append({
            "type": kind,
            "name": name,
            "lat": float(plat),
            "lng": float(plng),
            "source": "foursquare",
        })
    return items


# ----------------------------
# Routes
# ----------------------------
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "NYC TLC Hotspot Backend",
        "endpoints": ["/status", "/timeline", "/frame/{idx}", "/generate", "/generate_status", "/context", "/context_debug"],
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


@app.get("/context_debug")
def context_debug():
    fsq = os.getenv("FOURSQUARE_API_KEY", "").strip()
    tm = os.getenv("TICKETMASTER_API_KEY", "").strip()
    return {
        "foursquare_key_present": bool(fsq),
        "foursquare_key_len": len(fsq),
        "ticketmaster_key_present": bool(tm),
        "ticketmaster_key_len": len(tm),
        "cache_size": len(_CONTEXT_CACHE),
        "cached_ttl_sec": _CONTEXT_TTL_SEC,
    }


@app.get("/context")
async def context(
    bbox: str = Query(..., description="minLng,minLat,maxLng,maxLat"),
    types: str = Query("concerts,nightlife,restaurants", description="comma-separated: concerts,nightlife,restaurants"),
):
    """
    Separate overlay layer (NOT used in TLC rating).
    """
    try:
        min_lng, min_lat, max_lng, max_lat = _parse_bbox(bbox)
        bbox_t = (min_lng, min_lat, max_lng, max_lat)
    except Exception as e:
        return JSONResponse({"error": f"Invalid bbox: {e}"}, status_code=400)

    requested = [t.strip().lower() for t in types.split(",") if t.strip()]
    allowed = {"concerts", "nightlife", "restaurants"}
    req = [t for t in requested if t in allowed]
    if not req:
        return {"items": [], "generated_at_unix": int(time.time()), "types": [], "cached_ttl_sec": _CONTEXT_TTL_SEC}

    cache_key = _bbox_cache_key(min_lng, min_lat, max_lng, max_lat, ",".join(sorted(req)))
    cached = _cache_get(cache_key)
    if cached:
        return cached

    items: List[Dict[str, Any]] = []

    if "concerts" in req:
        items.extend(await fetch_ticketmaster_concerts(bbox_t))

    if "nightlife" in req:
        items.extend(await fetch_foursquare_places(bbox_t, kind="nightlife"))

    if "restaurants" in req:
        items.extend(await fetch_foursquare_places(bbox_t, kind="restaurants"))

    payload = {
        "items": items,
        "generated_at_unix": int(time.time()),
        "types": req,
        "cached_ttl_sec": _CONTEXT_TTL_SEC,
    }
    _cache_set(cache_key, payload)
    return payload


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