from __future__ import annotations

import os
import json
import time
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional

import duckdb
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

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

# Best-effort file lock to prevent multi-start concurrency
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
# App
# ----------------------------
app = FastAPI(title="NYC TLC Hotspot Backend", version="1.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock down later if desired
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


def _duckdb_schema_for_parquets(parquets: List[Path]) -> Dict[str, Any]:
    if not parquets:
        return {"ok": False, "error": "No parquet files found."}

    tmp_dir = DATA_DIR / "duckdb_tmp_debug"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA enable_progress_bar=false")
    con.execute(f"PRAGMA temp_directory='{tmp_dir.as_posix()}'")

    parquet_list = [str(p) for p in parquets]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{parquet_sql}])").fetchall()
    cols = [{"name": r[0], "type": r[1]} for r in rows]
    lower = [c["name"].lower() for c in cols]

    # what we WANT
    want_candidates = {
        "PULocationID": ["PULocationID", "pulocationid"],
        "pickup_datetime": ["pickup_datetime", "Pickup_datetime", "PICKUP_DATETIME"],
        "driver_pay": ["driver_pay", "Driver_pay"],
        "trip_miles": ["trip_miles", "Trip_miles"],
        "trip_time": ["trip_time", "Trip_time"],
        "dropoff_datetime": ["dropoff_datetime", "Dropoff_datetime"],
    }

    selected: Dict[str, Optional[str]] = {}
    for key, cands in want_candidates.items():
        chosen = None
        for c in cands:
            if c.lower() in lower:
                # return the actual cased version from cols
                for cc in cols:
                    if cc["name"].lower() == c.lower():
                        chosen = cc["name"]
                        break
                if chosen:
                    break
        selected[key] = chosen

    # Detect trip_time unit (seconds vs minutes) via quick median heuristic
    trip_time_unit = None
    trip_time_col = selected.get("trip_time")
    if trip_time_col:
        try:
            med = con.execute(
                f"""
                SELECT approx_quantile(CAST({trip_time_col} AS DOUBLE), 0.5)
                FROM read_parquet([{parquet_sql}])
                WHERE {trip_time_col} IS NOT NULL
                LIMIT 100000
                """
            ).fetchone()[0]
            if med is not None:
                # TLC FHV trip_time is typically seconds (often hundreds to thousands).
                # If median > ~200, it's almost certainly seconds.
                trip_time_unit = "seconds" if float(med) > 200 else "minutes"
        except Exception:
            trip_time_unit = None

    con.close()

    return {
        "ok": True,
        "files": [p.name for p in parquets],
        "columns": cols,
        "columns_lower": lower,
        "selected_columns": selected,
        "trip_time_unit_guess": trip_time_unit,
    }


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

        # build frames (this function now auto-detects columns + falls back safely)
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
    If /data/frames/timeline.json exists -> do nothing
    Else -> start generation in background using defaults (if inputs exist)
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
            "/debug/schema",
            "/debug/sample",
            "/debug/frame_stats/{idx}",
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


# ----------------------------
# DEBUG endpoints (Railway verification)
# ----------------------------
@app.get("/debug/schema")
def debug_schema():
    parquets = _list_parquets()
    return _duckdb_schema_for_parquets(parquets)


@app.get("/debug/sample")
def debug_sample(limit: int = 5):
    """
    Quick sanity check:
      - shows selected columns + a few rows
      - proves trip_time looks like seconds in your dataset
    """
    parquets = _list_parquets()
    if not parquets:
        return {"ok": False, "error": "No parquet files found."}

    schema = _duckdb_schema_for_parquets(parquets)
    if not schema.get("ok"):
        return schema

    sel = schema["selected_columns"]
    need = ["PULocationID", "pickup_datetime"]
    for k in need:
        if not sel.get(k):
            return {"ok": False, "error": f"Missing required column: {k}.", "selected_columns": sel}

    tmp_dir = DATA_DIR / "duckdb_tmp_debug"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA enable_progress_bar=false")
    con.execute(f"PRAGMA temp_directory='{tmp_dir.as_posix()}'")

    parquet_list = [str(p) for p in parquets]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    pu = sel["PULocationID"]
    pd = sel["pickup_datetime"]
    pay = sel.get("driver_pay")
    miles = sel.get("trip_miles")
    ttime = sel.get("trip_time")

    # Build a safe select that works even if optional cols missing
    cols_sql = [f"CAST({pu} AS INTEGER) AS PULocationID", f"CAST({pd} AS TIMESTAMP) AS pickup_datetime"]
    if pay:
        cols_sql.append(f"TRY_CAST({pay} AS DOUBLE) AS driver_pay")
    else:
        cols_sql.append("NULL::DOUBLE AS driver_pay")
    if miles:
        cols_sql.append(f"TRY_CAST({miles} AS DOUBLE) AS trip_miles")
    else:
        cols_sql.append("NULL::DOUBLE AS trip_miles")
    if ttime:
        cols_sql.append(f"TRY_CAST({ttime} AS DOUBLE) AS trip_time")
    else:
        cols_sql.append("NULL::DOUBLE AS trip_time")

    q = f"""
    SELECT {", ".join(cols_sql)}
    FROM read_parquet([{parquet_sql}])
    WHERE {pu} IS NOT NULL AND {pd} IS NOT NULL
    LIMIT {int(limit)}
    """
    rows = con.execute(q).fetchall()
    con.close()

    return {
        "ok": True,
        "files": [p.name for p in parquets],
        "selected_columns": ["PULocationID", "pickup_datetime", "driver_pay", "trip_miles", "trip_time"],
        "rows": [
            [r[0], r[1].isoformat() if r[1] else None, r[2], r[3], r[4]]
            for r in rows
        ],
        "trip_time_unit_guess": schema.get("trip_time_unit_guess"),
        "note": "Use /debug/schema to see exact column names; this endpoint is a quick value sanity-check.",
    }


@app.get("/debug/frame_stats/{idx}")
def debug_frame_stats(idx: int):
    """
    Verifies per-frame metrics are present:
      avg_trip_miles, avg_trip_minutes, pay_per_hour_zone
    """
    if not _has_frames():
        raise HTTPException(status_code=409, detail="timeline not ready. Call /generate first.")

    p = FRAMES_DIR / f"frame_{idx:06d}.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"frame not found: {idx}")

    frame = _read_json(p)
    feats = (frame.get("polygons") or {}).get("features") or []

    def num(x):
        try:
            if x is None:
                return None
            v = float(x)
            return v if v == v else None
        except Exception:
            return None

    miles_vals = []
    mins_vals = []
    pph_vals = []

    has_miles = 0
    has_mins = 0
    has_pph = 0

    for f in feats:
        props = (f or {}).get("properties") or {}
        m = num(props.get("avg_trip_miles"))
        t = num(props.get("avg_trip_minutes"))
        pph = num(props.get("pay_per_hour_zone"))

        if m is not None:
            has_miles += 1
            miles_vals.append(m)
        if t is not None:
            has_mins += 1
            mins_vals.append(t)
        if pph is not None:
            has_pph += 1
            pph_vals.append(pph)

    def summary(vals: List[float]):
        if not vals:
            return None
        vals2 = sorted(vals)
        n = len(vals2)
        return {
            "n": n,
            "min": vals2[0],
            "median": vals2[n // 2],
            "max": vals2[-1],
        }

    return {
        "ok": True,
        "idx": idx,
        "time": frame.get("time"),
        "features": len(feats),
        "present": {
            "avg_trip_miles": f"{has_miles}/{len(feats)}",
            "avg_trip_minutes": f"{has_mins}/{len(feats)}",
            "pay_per_hour_zone": f"{has_pph}/{len(feats)}",
        },
        "stats": {
            "avg_trip_miles": summary(miles_vals),
            "avg_trip_minutes": summary(mins_vals),
            "pay_per_hour_zone": summary(pph_vals),
        },
    }