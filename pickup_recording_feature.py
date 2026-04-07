from __future__ import annotations

import math
import inspect
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core import DB_BACKEND, _db, _db_exec, _db_lock, _db_query_all, _db_query_one, _sql, require_user
from leaderboard_service import (
    build_progression_from_daily_stats_rows,
)

router = APIRouter()
NYC_TZ = ZoneInfo("America/New_York")

PICKUP_SAVE_COOLDOWN_SECONDS = 600
PICKUP_SAVE_MIN_DRIVING_SECONDS = 360
PICKUP_SAVE_SESSION_BREAK_SECONDS = 480
PICKUP_SAVE_MOTION_STALE_SECONDS = 180
PICKUP_SAVE_RELOCATION_MIN_MILES = 0.25
PICKUP_SAVE_SAME_POSITION_MAX_MILES = 0.08
PICKUP_HOTSPOT_RECOMMENDATION_ATTRIBUTION_MAX_MILES = 0.35
PICKUP_MICRO_RECOMMENDATION_ATTRIBUTION_MAX_MILES = 0.20


def _noop_pickup_cache_invalidator() -> None:
    return


_pickup_cache_invalidator: Callable[[], None] = _noop_pickup_cache_invalidator


def set_pickup_cache_invalidator(fn: Optional[Callable[[], None]]) -> None:
    global _pickup_cache_invalidator
    _pickup_cache_invalidator = fn if callable(fn) else _noop_pickup_cache_invalidator


def register_pickup_write_cache_invalidation_hook(hook: Optional[Callable[[], None]]) -> None:
    set_pickup_cache_invalidator(hook)


def _invalidate_pickup_write_caches() -> None:
    try:
        _pickup_cache_invalidator()
    except Exception:
        pass


class PickupRecordingPayload(BaseModel):
    lat: float
    lng: float
    zone_id: Optional[int] = None
    zone_name: Optional[str] = None
    borough: Optional[str] = None
    frame_time: Optional[str] = None


class AdminVoidPickupPayload(BaseModel):
    reason: str


class AdminGuardEvaluatePayload(BaseModel):
    user_id: int
    lat: float
    lng: float
    now_ts: Optional[int] = None


class AdminSimulateSavePayload(BaseModel):
    user_id: int
    lat: float
    lng: float
    zone_id: Optional[int] = None
    zone_name: Optional[str] = None
    borough: Optional[str] = None
    frame_time: Optional[str] = None


def _is_admin(user: Any = Depends(require_user)):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    raw = user["is_admin"] if "is_admin" in user.keys() else user.get("is_admin")
    is_admin = bool(raw) if isinstance(raw, bool) else int(raw or 0) == 1
    if not is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _bool_db_value(flag: bool):
    if DB_BACKEND == "postgres":
        return bool(flag)
    return 1 if flag else 0


def ensure_pickup_recording_schema() -> None:
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS pickup_guard_state (
          user_id BIGINT PRIMARY KEY,
          last_seen_at BIGINT,
          last_lat DOUBLE PRECISION,
          last_lng DOUBLE PRECISION,
          previous_session_end_at BIGINT,
          previous_session_end_lat DOUBLE PRECISION,
          previous_session_end_lng DOUBLE PRECISION,
          movement_streak_started_at BIGINT,
          last_meaningful_motion_at BIGINT
        )
        """
    )

    alter_specs = [
        ("is_voided", "BOOLEAN NOT NULL DEFAULT FALSE" if DB_BACKEND == "postgres" else "INTEGER NOT NULL DEFAULT 0"),
        ("voided_at", "BIGINT"),
        ("voided_by_admin_user_id", "BIGINT"),
        ("void_reason", "TEXT"),
        (
            "counted_for_pickup_stats",
            "BOOLEAN NOT NULL DEFAULT TRUE" if DB_BACKEND == "postgres" else "INTEGER NOT NULL DEFAULT 1",
        ),
        ("guard_reason", "TEXT"),
    ]
    for col_name, col_type in alter_specs:
        try:
            _db_exec(f"ALTER TABLE pickup_logs ADD COLUMN {col_name} {col_type}")
        except Exception:
            pass


def pickup_log_not_voided_sql(alias: str) -> str:
    a = alias.strip() or "pickup_logs"
    if DB_BACKEND == "postgres":
        return f"COALESCE({a}.is_voided, FALSE) = FALSE"
    return f"COALESCE(CAST({a}.is_voided AS INTEGER), 0) = 0"


def _query_one_cur(cur, sql: str, params: tuple = ()) -> Optional[dict]:
    cur.execute(_sql(sql), params)
    row = cur.fetchone()
    return dict(row) if row else None


def _query_all_cur(cur, sql: str, params: tuple = ()) -> List[dict]:
    cur.execute(_sql(sql), params)
    return [dict(row) for row in cur.fetchall()]


def _exec_cur(cur, sql: str, params: tuple = ()) -> None:
    cur.execute(_sql(sql), params)


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


def _format_wait_short(wait_seconds: int) -> str:
    wait = max(1, int(wait_seconds))
    minutes = wait // 60
    seconds = wait % 60
    if minutes > 0 and seconds > 0:
        return f"{minutes}m {seconds}s"
    if minutes > 0:
        return f"{minutes}m"
    return f"{seconds}s"


def _settle_latest_hotspot_recommendation_outcome_tx(
    cur,
    user_id: int,
    zone_id: int,
    pickup_lat: float,
    pickup_lng: float,
    now_ts: int,
) -> Dict[str, Any]:
    rows = _query_all_cur(
        cur,
        """
        SELECT id, recommended_at, hotspot_center_lat, hotspot_center_lng
        FROM recommendation_outcomes
        WHERE user_id = ?
          AND zone_id = ?
          AND converted_to_trip IS NULL
          AND recommended_at <= ?
          AND recommended_at >= ?
          AND hotspot_center_lat IS NOT NULL
          AND hotspot_center_lng IS NOT NULL
        """,
        (int(user_id), int(zone_id), int(now_ts), int(now_ts) - 5400),
    )
    best: Optional[Dict[str, Any]] = None
    for row in rows:
        distance_miles = _safe_haversine_miles(
            float(pickup_lat),
            float(pickup_lng),
            float(row.get("hotspot_center_lat")),
            float(row.get("hotspot_center_lng")),
        )
        if distance_miles > PICKUP_HOTSPOT_RECOMMENDATION_ATTRIBUTION_MAX_MILES:
            continue
        candidate = dict(row)
        candidate["_distance_miles"] = float(distance_miles)
        if best is None:
            best = candidate
            continue
        if candidate["_distance_miles"] < best["_distance_miles"]:
            best = candidate
            continue
        if (
            math.isclose(candidate["_distance_miles"], best["_distance_miles"], rel_tol=0.0, abs_tol=1e-9)
            and int(candidate.get("recommended_at") or 0) > int(best.get("recommended_at") or 0)
        ):
            best = candidate
    if best is not None:
        recommended_at = int(best.get("recommended_at") or now_ts)
        minutes_to_trip = max(0.0, (float(now_ts) - float(recommended_at)) / 60.0)
        _exec_cur(
            cur,
            "UPDATE recommendation_outcomes SET converted_to_trip=?, minutes_to_trip=?, distance_to_recommendation_miles=? WHERE id=?",
            (_bool_db_value(True), float(minutes_to_trip), float(best["_distance_miles"]), int(best["id"])),
        )
        return {
            "settled": True,
            "spatial": True,
            "legacy_fallback": False,
            "used_spatial_match": True,
            "matched_distance_miles": float(best["_distance_miles"]),
        }

    fallback_row = _query_one_cur(
        cur,
        """
        SELECT id, recommended_at
        FROM recommendation_outcomes
        WHERE user_id = ?
          AND zone_id = ?
          AND converted_to_trip IS NULL
          AND recommended_at <= ?
          AND recommended_at >= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT 1
        """,
        (int(user_id), int(zone_id), int(now_ts), int(now_ts) - 5400),
    )
    if not fallback_row:
        return {"settled": False, "spatial": False, "legacy_fallback": False, "used_spatial_match": False, "matched_distance_miles": None}
    recommended_at = int(fallback_row.get("recommended_at") or now_ts)
    minutes_to_trip = max(0.0, (float(now_ts) - float(recommended_at)) / 60.0)
    _exec_cur(
        cur,
        "UPDATE recommendation_outcomes SET converted_to_trip=?, minutes_to_trip=?, distance_to_recommendation_miles=? WHERE id=?",
        (_bool_db_value(True), float(minutes_to_trip), None, int(fallback_row["id"])),
    )
    return {
        "settled": True,
        "spatial": False,
        "legacy_fallback": True,
        "used_spatial_match": False,
        "matched_distance_miles": None,
    }


def _settle_latest_micro_recommendation_outcome_tx(
    cur,
    user_id: int,
    zone_id: int,
    pickup_lat: float,
    pickup_lng: float,
    now_ts: int,
) -> Dict[str, Any]:
    rows = _query_all_cur(
        cur,
        """
        SELECT id, recommended_at, micro_center_lat, micro_center_lng
        FROM micro_recommendation_outcomes
        WHERE user_id = ?
          AND zone_id = ?
          AND converted_to_trip IS NULL
          AND recommended_at <= ?
          AND recommended_at >= ?
          AND micro_center_lat IS NOT NULL
          AND micro_center_lng IS NOT NULL
        """,
        (int(user_id), int(zone_id), int(now_ts), int(now_ts) - 5400),
    )
    best: Optional[Dict[str, Any]] = None
    for row in rows:
        distance_miles = _safe_haversine_miles(
            float(pickup_lat),
            float(pickup_lng),
            float(row.get("micro_center_lat")),
            float(row.get("micro_center_lng")),
        )
        if distance_miles > PICKUP_MICRO_RECOMMENDATION_ATTRIBUTION_MAX_MILES:
            continue
        candidate = dict(row)
        candidate["_distance_miles"] = float(distance_miles)
        if best is None:
            best = candidate
            continue
        if candidate["_distance_miles"] < best["_distance_miles"]:
            best = candidate
            continue
        if (
            math.isclose(candidate["_distance_miles"], best["_distance_miles"], rel_tol=0.0, abs_tol=1e-9)
            and int(candidate.get("recommended_at") or 0) > int(best.get("recommended_at") or 0)
        ):
            best = candidate
    if best is None:
        return {"settled": False, "used_spatial_match": False, "matched_distance_miles": None}
    recommended_at = int(best.get("recommended_at") or now_ts)
    minutes_to_trip = max(0.0, (float(now_ts) - float(recommended_at)) / 60.0)
    _exec_cur(
        cur,
        "UPDATE micro_recommendation_outcomes SET converted_to_trip=?, minutes_to_trip=?, distance_to_recommendation_miles=? WHERE id=?",
        (_bool_db_value(True), float(minutes_to_trip), float(best["_distance_miles"]), int(best["id"])),
    )
    return {"settled": True, "used_spatial_match": True, "matched_distance_miles": float(best["_distance_miles"])}


def _settle_latest_assistant_guidance_outcome_tx(
    cur,
    user_id: int,
    pickup_zone_id: Optional[int],
    pickup_lat: float,
    pickup_lng: float,
    now_ts: int,
) -> Dict[str, Any]:
    row = _query_one_cur(
        cur,
        """
        SELECT id, recommended_at, action, source_zone_id, target_zone_id
        FROM assistant_guidance_outcomes
        WHERE user_id = ?
          AND converted_to_trip IS NULL
          AND recommended_at <= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT 1
        """,
        (int(user_id), int(now_ts)),
    )
    if not row:
        return {"settled": False, "settlement_reason": None}

    minutes_to_trip = max(0.0, (float(now_ts) - float(row.get("recommended_at") or now_ts)) / 60.0)
    action = str(row.get("action") or "").strip().lower()
    source_zone_id = row.get("source_zone_id")
    target_zone_id = row.get("target_zone_id")
    settlement_reason = "trip_after_guidance"
    moved_before_trip = False
    if action in {"move_nearby", "micro_reposition"}:
        if pickup_zone_id is not None and target_zone_id is not None and int(pickup_zone_id) == int(target_zone_id):
            settlement_reason = "trip_after_move_target_zone_match"
            moved_before_trip = True
        elif pickup_zone_id is not None and source_zone_id is not None and int(pickup_zone_id) != int(source_zone_id):
            settlement_reason = "trip_after_move_detected"
            moved_before_trip = True
        else:
            settlement_reason = "trip_without_material_move"
    elif action in {"hold", "wait_dispatch"}:
        if pickup_zone_id is not None and source_zone_id is not None and int(pickup_zone_id) == int(source_zone_id):
            settlement_reason = "trip_while_holding_zone"
        else:
            settlement_reason = "trip_after_hold_with_relocation"
            moved_before_trip = True

    _exec_cur(
        cur,
        """
        UPDATE assistant_guidance_outcomes
        SET converted_to_trip=?, moved_before_trip=?, minutes_to_trip=?, settled_at=?, settlement_reason=?
        WHERE id=?
        """,
        (_bool_db_value(True), _bool_db_value(moved_before_trip), float(minutes_to_trip), int(now_ts), settlement_reason, int(row["id"])),
    )
    _exec_cur(
        cur,
        """
        UPDATE driver_guidance_state
        SET recent_move_attempts_without_trip=0, recent_wait_dispatch_count=0, updated_at=?
        WHERE user_id=?
        """,
        (int(now_ts), int(user_id)),
    )
    return {"settled": True, "settlement_reason": settlement_reason}


def record_pickup_presence_heartbeat(user_id: int, lat: float, lng: float, now_ts: int) -> None:
    row = _db_query_one("SELECT * FROM pickup_guard_state WHERE user_id=? LIMIT 1", (int(user_id),))
    prev_last_seen = int(row["last_seen_at"]) if row and row["last_seen_at"] is not None else None
    prev_last_lat = float(row["last_lat"]) if row and row["last_lat"] is not None else None
    prev_last_lng = float(row["last_lng"]) if row and row["last_lng"] is not None else None
    prev_session_end_at = int(row["previous_session_end_at"]) if row and row["previous_session_end_at"] is not None else None
    prev_session_end_lat = float(row["previous_session_end_lat"]) if row and row["previous_session_end_lat"] is not None else None
    prev_session_end_lng = float(row["previous_session_end_lng"]) if row and row["previous_session_end_lng"] is not None else None
    move_start = int(row["movement_streak_started_at"]) if row and row["movement_streak_started_at"] is not None else None
    last_motion = int(row["last_meaningful_motion_at"]) if row and row["last_meaningful_motion_at"] is not None else None

    if prev_last_seen is None:
        pass
    elif int(now_ts) - prev_last_seen > PICKUP_SAVE_SESSION_BREAK_SECONDS:
        prev_session_end_at = prev_last_seen
        prev_session_end_lat = prev_last_lat
        prev_session_end_lng = prev_last_lng
        move_start = None
        last_motion = None

    meaningful = False
    if prev_last_lat is not None and prev_last_lng is not None:
        meaningful = _safe_haversine_miles(prev_last_lat, prev_last_lng, lat, lng) >= 0.05

    if meaningful:
        if move_start is None:
            move_start = prev_last_seen if prev_last_seen is not None else int(now_ts)
        last_motion = int(now_ts)
    elif last_motion is not None and int(now_ts) - int(last_motion) > PICKUP_SAVE_MOTION_STALE_SECONDS:
        move_start = None
        last_motion = None

    _db_exec(
        """
        INSERT INTO pickup_guard_state(
          user_id, last_seen_at, last_lat, last_lng,
          previous_session_end_at, previous_session_end_lat, previous_session_end_lng,
          movement_streak_started_at, last_meaningful_motion_at
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
          last_seen_at=excluded.last_seen_at,
          last_lat=excluded.last_lat,
          last_lng=excluded.last_lng,
          previous_session_end_at=excluded.previous_session_end_at,
          previous_session_end_lat=excluded.previous_session_end_lat,
          previous_session_end_lng=excluded.previous_session_end_lng,
          movement_streak_started_at=excluded.movement_streak_started_at,
          last_meaningful_motion_at=excluded.last_meaningful_motion_at
        """,
        (
            int(user_id),
            int(now_ts),
            float(lat),
            float(lng),
            prev_session_end_at,
            prev_session_end_lat,
            prev_session_end_lng,
            move_start,
            last_motion,
        ),
    )


def _latest_active_pickup_log_for_user(user_id: int, cur=None) -> Optional[Dict[str, Any]]:
    sql = f"""
        SELECT id, user_id, lat, lng, created_at, zone_id, zone_name, borough, frame_time, guard_reason,
               counted_for_pickup_stats, is_voided, voided_at, void_reason
        FROM pickup_logs pl
        WHERE pl.user_id=?
          AND {pickup_log_not_voided_sql('pl')}
        ORDER BY pl.created_at DESC, pl.id DESC
        LIMIT 1
    """
    if cur is not None:
        return _query_one_cur(cur, sql, (int(user_id),))
    row = _db_query_one(sql, (int(user_id),))
    return dict(row) if row else None


def evaluate_pickup_guard(user_id: int, lat: float, lng: float, now_ts: int, cur=None) -> Dict[str, Any]:
    if cur is not None:
        guard = _query_one_cur(cur, "SELECT * FROM pickup_guard_state WHERE user_id=? LIMIT 1", (int(user_id),))
    else:
        row = _db_query_one("SELECT * FROM pickup_guard_state WHERE user_id=? LIMIT 1", (int(user_id),))
        guard = dict(row) if row else None
    latest = _latest_active_pickup_log_for_user(int(user_id), cur=cur)

    if latest is not None:
        cooldown_until = int(latest.get("created_at") or 0) + PICKUP_SAVE_COOLDOWN_SECONDS
        if int(now_ts) < cooldown_until:
            wait = _format_wait_short(cooldown_until - int(now_ts))
            return {
                "ok": False,
                "code": "pickup_cooldown_active",
                "title": "Save button cooling off",
                "detail": f"Wait {wait} before saving another trip.",
                "cooldown_until_unix": cooldown_until,
            }

    driving_ok = False
    relocation_prev_session_ok = False
    relocation_last_pickup_ok = False

    if guard:
        move_start = guard["movement_streak_started_at"]
        last_motion = guard["last_meaningful_motion_at"]
        if move_start is not None and last_motion is not None:
            driving_ok = (
                int(now_ts) - int(move_start) >= PICKUP_SAVE_MIN_DRIVING_SECONDS
                and int(now_ts) - int(last_motion) <= PICKUP_SAVE_MOTION_STALE_SECONDS
            )

        p_lat = guard["previous_session_end_lat"]
        p_lng = guard["previous_session_end_lng"]
        if p_lat is not None and p_lng is not None:
            relocation_prev_session_ok = (
                _safe_haversine_miles(float(p_lat), float(p_lng), float(lat), float(lng))
                >= PICKUP_SAVE_RELOCATION_MIN_MILES
            )

    if latest is not None:
        relocation_last_pickup_ok = (
            _safe_haversine_miles(float(latest.get("lat") or 0.0), float(latest.get("lng") or 0.0), float(lat), float(lng))
            >= PICKUP_SAVE_RELOCATION_MIN_MILES
        )

    if driving_ok:
        return {
            "ok": True,
            "accepted_guard_reason": "driving_evidence",
            "cooldown_until_unix": int(now_ts) + PICKUP_SAVE_COOLDOWN_SECONDS,
        }
    if relocation_prev_session_ok:
        return {
            "ok": True,
            "accepted_guard_reason": "relocated_from_previous_session",
            "cooldown_until_unix": int(now_ts) + PICKUP_SAVE_COOLDOWN_SECONDS,
        }
    if relocation_last_pickup_ok:
        return {
            "ok": True,
            "accepted_guard_reason": "relocated_from_last_accepted_trip",
            "cooldown_until_unix": int(now_ts) + PICKUP_SAVE_COOLDOWN_SECONDS,
        }

    if latest is not None:
        same_pos_dist = _safe_haversine_miles(float(latest.get("lat") or 0.0), float(latest.get("lng") or 0.0), float(lat), float(lng))
        if same_pos_dist <= PICKUP_SAVE_SAME_POSITION_MAX_MILES:
            return {
                "ok": False,
                "code": "pickup_same_position",
                "title": "Trip not saved",
                "detail": "Same position detected. Move to a new location or keep driving before saving another trip.",
            }

    return {
        "ok": False,
        "code": "pickup_needs_recent_driving",
        "title": "Trip not saved",
        "detail": "Drive at least 6 minutes or move to a new location before saving this trip.",
    }


def _nyc_business_date_from_unix(ts_unix: int) -> str:
    local = datetime.fromtimestamp(int(ts_unix), tz=timezone.utc).astimezone(NYC_TZ) - timedelta(hours=4)
    return local.date().isoformat()


def _pickup_progression_rows_for_user(user_id: int, cur=None) -> List[Dict[str, Any]]:
    sql = """
        SELECT nyc_date, miles_worked, hours_worked, pickups_recorded
        FROM driver_daily_stats
        WHERE user_id=?
        ORDER BY nyc_date ASC
    """
    if cur is not None:
        return _query_all_cur(cur, sql, (int(user_id),))
    rows = _db_query_all(sql, (int(user_id),))
    return [dict(r) for r in rows]


def get_pickup_progression_for_user(user_id: int, cur=None) -> Dict[str, Any]:
    rows = _pickup_progression_rows_for_user(int(user_id), cur=cur)
    game_xp_total = 0
    try:
        if cur is not None:
            game_row = _query_one_cur(
                cur,
                "SELECT COALESCE(SUM(xp_awarded), 0) AS xp_total FROM game_xp_awards WHERE user_id=?",
                (int(user_id),),
            )
        else:
            game_row = _db_query_one(
                "SELECT COALESCE(SUM(xp_awarded), 0) AS xp_total FROM game_xp_awards WHERE user_id=?",
                (int(user_id),),
            )
        game_xp_total = int((game_row or {}).get("xp_total") or 0)
    except Exception:
        game_xp_total = 0
    return build_progression_from_daily_stats_rows(rows, game_xp=game_xp_total)


def _increment_pickup_count_tx(cur, user_id: int, now_ts: int, amount: int = 1) -> None:
    nyc_date = _nyc_business_date_from_unix(int(now_ts))
    cur.execute(
        _sql(
            """
            INSERT INTO driver_daily_stats(user_id, nyc_date, pickups_recorded, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id, nyc_date) DO UPDATE SET
              pickups_recorded=driver_daily_stats.pickups_recorded + excluded.pickups_recorded,
              updated_at=excluded.updated_at
            """
        ),
        (int(user_id), nyc_date, int(amount), int(now_ts)),
    )


def _decrement_pickup_count_tx(cur, user_id: int, created_at_unix: int, amount: int = 1) -> None:
    nyc_date = _nyc_business_date_from_unix(int(created_at_unix))
    now = int(time.time())
    _exec_cur(
        cur,
        """
        UPDATE driver_daily_stats
        SET pickups_recorded = CASE
          WHEN pickups_recorded IS NULL THEN 0
          WHEN pickups_recorded <= ? THEN 0
          ELSE pickups_recorded - ?
        END,
        updated_at=?
        WHERE user_id=? AND nyc_date=?
        """,
        (int(amount), int(amount), now, int(user_id), nyc_date),
    )


def _soft_void_pickup_trip_tx(cur, trip_id: int, admin_user_id: int, reason: str) -> Dict[str, Any]:
    now = int(time.time())
    trip = _query_one_cur(cur, "SELECT * FROM pickup_logs WHERE id=? LIMIT 1", (int(trip_id),))
    if not trip:
        raise HTTPException(status_code=404, detail="Pickup trip not found")

    already_voided = bool(trip["is_voided"]) if isinstance(trip["is_voided"], bool) else int(trip["is_voided"] or 0) == 1
    if already_voided:
        return {
            "ok": True,
            "trip_id": int(trip_id),
            "voided": True,
            "stats_reversed": False,
            "preserved_in_audit": True,
            "already_voided": True,
        }

    _exec_cur(
        cur,
        """
        UPDATE pickup_logs
        SET is_voided=?, voided_at=?, voided_by_admin_user_id=?, void_reason=?
        WHERE id=?
        """,
        (_bool_db_value(True), now, int(admin_user_id), reason, int(trip_id)),
    )

    stats_reversed = False
    counted = (
        bool(trip["counted_for_pickup_stats"])
        if isinstance(trip["counted_for_pickup_stats"], bool)
        else int(trip["counted_for_pickup_stats"] or 0) == 1
    )
    if counted:
        _decrement_pickup_count_tx(cur, int(trip["user_id"]), int(trip.get("created_at") or now), 1)
        _exec_cur(
            cur,
            "UPDATE pickup_logs SET counted_for_pickup_stats=? WHERE id=?",
            (_bool_db_value(False), int(trip_id)),
        )
        stats_reversed = True

    return {
        "ok": True,
        "trip_id": int(trip_id),
        "voided": True,
        "stats_reversed": stats_reversed,
        "preserved_in_audit": True,
        "already_voided": False,
    }


def soft_void_pickup_trip(trip_id: int, admin_user_id: int, reason: str) -> Dict[str, Any]:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            result = _soft_void_pickup_trip_tx(cur, int(trip_id), int(admin_user_id), str(reason))
            conn.commit()
            if not bool(result.get("already_voided")):
                _invalidate_pickup_write_caches()
            return result
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def create_pickup_record(payload: PickupRecordingPayload, user: Any) -> Dict[str, Any]:
    zone_name = (payload.zone_name or "").strip() or None
    borough = (payload.borough or "").strip() or None
    frame_time = (payload.frame_time or "").strip() or None
    now = int(time.time())
    expires = now + 24 * 3600
    settled_hotspot_recommendation = False
    settled_micro_recommendation = False
    settled_hotspot_recommendation_spatial = False
    settled_micro_recommendation_spatial = False
    settled_hotspot_recommendation_legacy_fallback = False
    settled_assistant_guidance = False
    settled_assistant_guidance_reason: Optional[str] = None

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            progression_before = get_pickup_progression_for_user(int(user["id"]), cur=cur)
            guard = evaluate_pickup_guard(int(user["id"]), float(payload.lat), float(payload.lng), now, cur=cur)
            if not guard.get("ok"):
                status_code = 429 if guard.get("code") == "pickup_cooldown_active" else 409
                raise HTTPException(status_code=status_code, detail=guard)

            _exec_cur(
                cur,
                """
                INSERT INTO pickup_logs(
                  user_id, lat, lng, zone_id, zone_name, borough, frame_time, created_at,
                  is_voided, counted_for_pickup_stats, guard_reason
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(user["id"]),
                    float(payload.lat),
                    float(payload.lng),
                    payload.zone_id,
                    zone_name,
                    borough,
                    frame_time,
                    now,
                    _bool_db_value(False),
                    _bool_db_value(True),
                    str(guard.get("accepted_guard_reason") or ""),
                ),
            )
            _exec_cur(
                cur,
                """
                INSERT INTO events(type, user_id, lat, lng, text, zone_id, created_at, expires_at)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                ("pickup", int(user["id"]), float(payload.lat), float(payload.lng), "", payload.zone_id, now, expires),
            )
            _increment_pickup_count_tx(cur, int(user["id"]), now, 1)
            if payload.zone_id is not None:
                try:
                    hotspot_settlement = _settle_latest_hotspot_recommendation_outcome_tx(
                        cur,
                        user_id=int(user["id"]),
                        zone_id=int(payload.zone_id),
                        pickup_lat=float(payload.lat),
                        pickup_lng=float(payload.lng),
                        now_ts=now,
                    )
                    settled_hotspot_recommendation = bool(hotspot_settlement.get("settled"))
                    settled_hotspot_recommendation_spatial = bool(hotspot_settlement.get("spatial"))
                    settled_hotspot_recommendation_legacy_fallback = bool(hotspot_settlement.get("legacy_fallback"))
                except Exception:
                    settled_hotspot_recommendation = False
                    settled_hotspot_recommendation_spatial = False
                    settled_hotspot_recommendation_legacy_fallback = False
                try:
                    micro_settlement = _settle_latest_micro_recommendation_outcome_tx(
                        cur,
                        user_id=int(user["id"]),
                        zone_id=int(payload.zone_id),
                        pickup_lat=float(payload.lat),
                        pickup_lng=float(payload.lng),
                        now_ts=now,
                    )
                    settled_micro_recommendation = bool(micro_settlement.get("settled"))
                    settled_micro_recommendation_spatial = bool(micro_settlement.get("used_spatial_match"))
                except Exception:
                    settled_micro_recommendation = False
                    settled_micro_recommendation_spatial = False
            try:
                guidance_settlement = _settle_latest_assistant_guidance_outcome_tx(
                    cur,
                    user_id=int(user["id"]),
                    pickup_zone_id=payload.zone_id,
                    pickup_lat=float(payload.lat),
                    pickup_lng=float(payload.lng),
                    now_ts=now,
                )
                settled_assistant_guidance = bool(guidance_settlement.get("settled"))
                settled_assistant_guidance_reason = (
                    str(guidance_settlement.get("settlement_reason"))
                    if guidance_settlement.get("settlement_reason") is not None
                    else None
                )
            except Exception:
                settled_assistant_guidance = False
                settled_assistant_guidance_reason = None
            progression_after = get_pickup_progression_for_user(int(user["id"]), cur=cur)
            conn.commit()
            _invalidate_pickup_write_caches()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    return {
        "ok": True,
        "xp_awarded": int(progression_after.get("total_xp", 0)) - int(progression_before.get("total_xp", 0)),
        "leveled_up": int(progression_after.get("level", 1)) > int(progression_before.get("level", 1)),
        "previous_level": int(progression_before.get("level", 1)),
        "new_level": int(progression_after.get("level", 1)),
        "progression": progression_after,
        "cooldown_until_unix": int(guard.get("cooldown_until_unix") or (now + PICKUP_SAVE_COOLDOWN_SECONDS)),
        "accepted_guard_reason": str(guard.get("accepted_guard_reason") or ""),
        "settled_hotspot_recommendation": bool(settled_hotspot_recommendation),
        "settled_micro_recommendation": bool(settled_micro_recommendation),
        "settled_hotspot_recommendation_spatial": bool(settled_hotspot_recommendation_spatial),
        "settled_micro_recommendation_spatial": bool(settled_micro_recommendation_spatial),
        "settled_hotspot_recommendation_legacy_fallback": bool(settled_hotspot_recommendation_legacy_fallback),
        "settled_assistant_guidance": bool(settled_assistant_guidance),
        "settled_assistant_guidance_reason": settled_assistant_guidance_reason,
    }


@router.get("/admin/pickup-recording/trips/recent")
def admin_recent_pickup_trips(
    limit: int = 50,
    include_voided: int = 0,
    admin: Any = Depends(_is_admin),
):
    safe_limit = max(1, min(300, int(limit)))
    sql = f"""
        SELECT
          pl.id,
          pl.user_id,
          COALESCE(u.display_name, '') AS display_name,
          pl.lat,
          pl.lng,
          pl.zone_id,
          pl.zone_name,
          pl.borough,
          pl.frame_time,
          pl.created_at,
          pl.is_voided,
          pl.voided_at,
          pl.void_reason,
          pl.guard_reason,
          pl.counted_for_pickup_stats
        FROM pickup_logs pl
        LEFT JOIN users u ON u.id = pl.user_id
        WHERE {pickup_log_not_voided_sql('pl') if int(include_voided) != 1 else '1=1'}
        ORDER BY pl.created_at DESC, pl.id DESC
        LIMIT ?
    """
    rows = _db_query_all(sql, (safe_limit,))
    items = [dict(r) for r in rows]
    return {"ok": True, "items": items}


@router.post("/admin/pickup-recording/trips/{trip_id}/void")
def admin_void_pickup_trip(trip_id: int, payload: AdminVoidPickupPayload, admin: Any = Depends(_is_admin)):
    reason = (payload.reason or "").strip()
    if len(reason) < 5:
        raise HTTPException(status_code=400, detail="Reason must be at least 5 characters")
    return soft_void_pickup_trip(int(trip_id), int(admin["id"]), reason)


@router.get("/admin/pickup-recording/tests/health")
def admin_pickup_tests_health(admin: Any = Depends(_is_admin)):
    started = time.time()
    checks: Dict[str, Any] = {}

    try:
        ensure_pickup_recording_schema()
        checks["pickup_schema_ready"] = True
    except Exception:
        checks["pickup_schema_ready"] = False

    try:
        _db_query_one("SELECT user_id FROM pickup_guard_state LIMIT 1")
        checks["pickup_guard_state_exists"] = True
    except Exception:
        checks["pickup_guard_state_exists"] = False

    try:
        _db_query_one(
            """
            SELECT is_voided, voided_at, voided_by_admin_user_id, void_reason, counted_for_pickup_stats, guard_reason
            FROM pickup_logs
            LIMIT 1
            """
        )
        checks["pickup_logs_columns_ready"] = True
    except Exception:
        checks["pickup_logs_columns_ready"] = False

    try:
        _db_query_all(f"SELECT pl.id FROM pickup_logs pl WHERE {pickup_log_not_voided_sql('pl')} ORDER BY pl.id DESC LIMIT 1")
        checks["active_recent_query_safe"] = True
    except Exception:
        checks["active_recent_query_safe"] = False

    try:
        import admin_mutation_service

        clear_source = inspect.getsource(admin_mutation_service.clear_pickup_report)
        checks["legacy_clear_route_safe"] = "DELETE FROM pickup_logs" not in clear_source
    except Exception:
        checks["legacy_clear_route_safe"] = False

    try:
        if DB_BACKEND == "postgres":
            timeslot_expr = "CAST((MOD(pl.created_at, 86400) / 60) / ? AS INTEGER)"
        else:
            timeslot_expr = "CAST(((pl.created_at % 86400) / 60) / ? AS INTEGER)"
        _db_query_all(
            f"SELECT pl.zone_id, COUNT(*) AS c FROM pickup_logs pl WHERE {timeslot_expr} = ? GROUP BY pl.zone_id LIMIT 1",
            (20, 1),
        )
        checks["same_timeslot_query_safe"] = True
    except Exception:
        checks["same_timeslot_query_safe"] = False

    return {
        "ok": all(bool(v) for v in checks.values()),
        "checks": checks,
        "timings": {"elapsed_ms": int((time.time() - started) * 1000)},
    }


@router.post("/admin/pickup-recording/tests/guard-evaluate")
def admin_pickup_tests_guard_evaluate(payload: AdminGuardEvaluatePayload, admin: Any = Depends(_is_admin)):
    now_ts = int(payload.now_ts or time.time())
    return {
        "ok": True,
        "decision": evaluate_pickup_guard(int(payload.user_id), float(payload.lat), float(payload.lng), now_ts),
    }


@router.post("/admin/pickup-recording/tests/simulate-save")
def admin_pickup_tests_simulate_save(payload: AdminSimulateSavePayload, admin: Any = Depends(_is_admin)):
    now_ts = int(time.time())
    guard = evaluate_pickup_guard(int(payload.user_id), float(payload.lat), float(payload.lng), now_ts)
    if not guard.get("ok"):
        return {
            "ok": True,
            "would_save": False,
            "status_code": 429 if guard.get("code") == "pickup_cooldown_active" else 409,
            "error": {
                "code": str(guard.get("code") or ""),
                "title": str(guard.get("title") or ""),
                "detail": str(guard.get("detail") or ""),
            },
        }

    progression = get_pickup_progression_for_user(int(payload.user_id))
    return {
        "ok": True,
        "would_save": True,
        "cooldown_until_unix": int(guard.get("cooldown_until_unix") or (now_ts + PICKUP_SAVE_COOLDOWN_SECONDS)),
        "accepted_guard_reason": str(guard.get("accepted_guard_reason") or ""),
        "reward_contract": {
            "level": int(progression.get("level") or 1),
            "rank_name": str(progression.get("rank_name") or "Recruit"),
            "rank_icon_key": str(progression.get("rank_icon_key") or "recruit"),
            "total_xp": int(progression.get("total_xp") or 0),
            "current_level_xp": int(progression.get("current_level_xp") or 0),
            "next_level_xp": progression.get("next_level_xp"),
            "xp_to_next_level": int(progression.get("xp_to_next_level") or 0),
        },
    }


@router.get("/admin/pickup-recording/tests/filter-smoke")
def admin_pickup_tests_filter_smoke(admin: Any = Depends(_is_admin)):
    active_rows = _db_query_all(
        f"SELECT id FROM pickup_logs pl WHERE {pickup_log_not_voided_sql('pl')} ORDER BY id DESC LIMIT 200"
    )
    include_rows = _db_query_all("SELECT id FROM pickup_logs ORDER BY id DESC LIMIT 200")
    active_ids = {int(r["id"]) for r in active_rows}
    include_ids = {int(r["id"]) for r in include_rows}
    return {
        "ok": True,
        "active_recent_count": len(active_ids),
        "include_voided_count": len(include_ids),
        "active_is_subset": active_ids.issubset(include_ids),
    }
