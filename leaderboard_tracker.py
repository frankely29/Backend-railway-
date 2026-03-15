from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from core import _db, _db_lock, _sql
from leaderboard_models import LeaderboardPeriod

NYC_TZ = ZoneInfo("America/New_York")
MAX_COUNTED_GAP_SECONDS = 300
MAX_SPEED_MPH = 100.0
MIN_MEANINGFUL_MOVEMENT_MILES = 0.01
MIN_MEANINGFUL_SPEED_MPH = 1.0
MILES_PER_METER = 0.000621371
PICKUP_SAVE_COOLDOWN_SECONDS = 600
PICKUP_SAVE_MIN_DRIVING_SECONDS = 360
PICKUP_SAVE_SESSION_BREAK_SECONDS = 480
PICKUP_SAVE_MOTION_STALE_SECONDS = 180
PICKUP_SAVE_RELOCATION_MIN_MILES = 0.25
PICKUP_SAVE_SAME_POSITION_MAX_MILES = 0.08


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius_m = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_m * c * MILES_PER_METER


def nyc_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(NYC_TZ)


def nyc_date_from_unix(ts: int) -> str:
    """
    Convert a UNIX timestamp to a date string (YYYY-MM-DD) in America/New_York,
    where the “day” begins at 4 AM local time. Subtract 4 hours before
    taking the date so that data recorded between midnight and 3:59:59 AM
    counts toward the previous day.
    """
    local = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(NYC_TZ) - timedelta(hours=4)
    return local.date().isoformat()


def period_key_for_date(period: LeaderboardPeriod, d: date) -> str:
    if period == LeaderboardPeriod.daily:
        return d.isoformat()
    if period == LeaderboardPeriod.weekly:
        start = d - timedelta(days=d.weekday())
        end = start + timedelta(days=6)
        return f"{start.isoformat()}_{end.isoformat()}"
    if period == LeaderboardPeriod.monthly:
        return d.strftime("%Y-%m")
    return str(d.year)


def _split_seconds_by_nyc_date(start_ts: int, end_ts: int) -> list[tuple[str, float]]:
    if end_ts <= start_ts:
        return []

    cursor = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    out: list[tuple[str, float]] = []
    while cursor < end_dt:
        local = cursor.astimezone(NYC_TZ)
        shifted_local = local - timedelta(hours=4)
        next_boundary_shifted = datetime.combine(
            shifted_local.date() + timedelta(days=1),
            datetime.min.time(),
            tzinfo=NYC_TZ,
        )
        next_boundary_local = next_boundary_shifted + timedelta(hours=4)
        chunk_end = min(end_dt, next_boundary_local.astimezone(timezone.utc))
        sec = max(0.0, (chunk_end - cursor).total_seconds())
        out.append((shifted_local.date().isoformat(), sec))
        cursor = chunk_end
    return out


def _upsert_daily_stat(cur, user_id: int, nyc_date: str, miles: float, hours: float, heartbeat_inc: int = 0) -> None:
    now = int(time.time())
    cur.execute(
        _sql(
            """
            INSERT INTO driver_daily_stats(user_id, nyc_date, miles_worked, hours_worked, heartbeat_count, updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(user_id, nyc_date) DO UPDATE SET
              miles_worked=driver_daily_stats.miles_worked + excluded.miles_worked,
              hours_worked=driver_daily_stats.hours_worked + excluded.hours_worked,
              heartbeat_count=driver_daily_stats.heartbeat_count + excluded.heartbeat_count,
              updated_at=excluded.updated_at
            """
        ),
        (int(user_id), nyc_date, float(miles), float(hours), int(heartbeat_inc), now),
    )


def record_presence_heartbeat(user_id: int, lat: float, lng: float, heading: float | None = None) -> None:
    now = int(time.time())
    nyc_date = nyc_date_from_unix(now)

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute(_sql("SELECT * FROM driver_work_state WHERE user_id=? LIMIT 1"), (int(user_id),))
            prev = cur.fetchone()

            counted_seconds = 0
            distance_miles = 0.0
            meaningful_movement = False
            if prev and prev["last_seen_at"]:
                prev_seen = int(prev["last_seen_at"])
                if now > prev_seen:
                    raw_gap = now - prev_seen
                    prev_lat = prev["last_lat"]
                    prev_lng = prev["last_lng"]
                    if prev_lat is not None and prev_lng is not None:
                        d = _haversine_miles(float(prev_lat), float(prev_lng), float(lat), float(lng))
                        speed_mph = d / (raw_gap / 3600.0) if raw_gap > 0 else 0.0
                        if (
                            MIN_MEANINGFUL_SPEED_MPH <= speed_mph <= MAX_SPEED_MPH
                            and d >= MIN_MEANINGFUL_MOVEMENT_MILES
                        ):
                            counted_seconds = min(raw_gap, MAX_COUNTED_GAP_SECONDS)
                            distance_miles = d
                            meaningful_movement = counted_seconds > 0

            if counted_seconds > 0:
                splits = _split_seconds_by_nyc_date(now - counted_seconds, now)
                total = sum(sec for _, sec in splits) or 1.0
                for dt, sec in splits:
                    miles_chunk = distance_miles * (sec / total)
                    _upsert_daily_stat(cur, user_id, dt, miles_chunk, sec / 3600.0, 1 if dt == nyc_date else 0)
            else:
                _upsert_daily_stat(cur, user_id, nyc_date, 0.0, 0.0, 1)

            previous_session_end_at = int(prev["previous_session_end_at"]) if prev and prev["previous_session_end_at"] is not None else None
            previous_session_end_lat = float(prev["previous_session_end_lat"]) if prev and prev["previous_session_end_lat"] is not None else None
            previous_session_end_lng = float(prev["previous_session_end_lng"]) if prev and prev["previous_session_end_lng"] is not None else None
            movement_streak_started_at = int(prev["movement_streak_started_at"]) if prev and prev["movement_streak_started_at"] is not None else None
            last_meaningful_motion_at = int(prev["last_meaningful_motion_at"]) if prev and prev["last_meaningful_motion_at"] is not None else None

            is_new_session = False
            prev_seen_at = int(prev["last_seen_at"]) if prev and prev["last_seen_at"] is not None else None
            prev_last_lat = float(prev["last_lat"]) if prev and prev["last_lat"] is not None else None
            prev_last_lng = float(prev["last_lng"]) if prev and prev["last_lng"] is not None else None
            if prev_seen_at is not None and now - prev_seen_at > PICKUP_SAVE_SESSION_BREAK_SECONDS:
                is_new_session = True
                previous_session_end_at = prev_seen_at
                previous_session_end_lat = prev_last_lat
                previous_session_end_lng = prev_last_lng
                movement_streak_started_at = None
                last_meaningful_motion_at = None

            if meaningful_movement:
                if movement_streak_started_at is None:
                    start_candidate = now - counted_seconds
                    if prev_seen_at is not None:
                        movement_streak_started_at = max(prev_seen_at, start_candidate)
                    else:
                        movement_streak_started_at = start_candidate
                last_meaningful_motion_at = now
            elif (
                not is_new_session
                and last_meaningful_motion_at is not None
                and now - last_meaningful_motion_at > PICKUP_SAVE_MOTION_STALE_SECONDS
            ):
                movement_streak_started_at = None
                last_meaningful_motion_at = None

            cur.execute(
                _sql(
                    """
                    INSERT INTO driver_work_state(
                        user_id,
                        last_seen_at,
                        last_lat,
                        last_lng,
                        last_heading,
                        previous_session_end_at,
                        previous_session_end_lat,
                        previous_session_end_lng,
                        movement_streak_started_at,
                        last_meaningful_motion_at,
                        updated_at
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(user_id) DO UPDATE SET
                      last_seen_at=excluded.last_seen_at,
                      last_lat=excluded.last_lat,
                      last_lng=excluded.last_lng,
                      last_heading=excluded.last_heading,
                      previous_session_end_at=excluded.previous_session_end_at,
                      previous_session_end_lat=excluded.previous_session_end_lat,
                      previous_session_end_lng=excluded.previous_session_end_lng,
                      movement_streak_started_at=excluded.movement_streak_started_at,
                      last_meaningful_motion_at=excluded.last_meaningful_motion_at,
                      updated_at=excluded.updated_at
                    """
                ),
                (
                    int(user_id),
                    now,
                    float(lat),
                    float(lng),
                    heading,
                    previous_session_end_at,
                    previous_session_end_lat,
                    previous_session_end_lng,
                    movement_streak_started_at,
                    last_meaningful_motion_at,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()


def increment_trip_count(user_id: int, amount: int = 1) -> None:
    _increment_daily_counter(user_id, "trips_recorded", amount)


def increment_pickup_count(user_id: int, amount: int = 1) -> None:
    _increment_daily_counter(user_id, "pickups_recorded", amount)


def decrement_pickup_count_for_timestamp(user_id: int, created_at_unix: int, amount: int = 1) -> None:
    nyc_date = nyc_date_from_unix(int(created_at_unix))
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            now = int(time.time())
            cur.execute(
                _sql(
                    """
                    UPDATE driver_daily_stats
                    SET pickups_recorded = CASE
                        WHEN pickups_recorded - ? < 0 THEN 0
                        ELSE pickups_recorded - ?
                    END,
                    updated_at = ?
                    WHERE user_id = ? AND nyc_date = ?
                    """
                ),
                (int(amount), int(amount), now, int(user_id), nyc_date),
            )
            conn.commit()
        finally:
            conn.close()


def _increment_daily_counter(user_id: int, field_name: str, amount: int) -> None:
    nyc_date = nyc_date_from_unix(int(time.time()))
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            now = int(time.time())
            cur.execute(
                _sql(
                    f"""
                    INSERT INTO driver_daily_stats(user_id, nyc_date, {field_name}, updated_at)
                    VALUES(?,?,?,?)
                    ON CONFLICT(user_id, nyc_date) DO UPDATE SET
                      {field_name}=driver_daily_stats.{field_name} + excluded.{field_name},
                      updated_at=excluded.updated_at
                    """
                ),
                (int(user_id), nyc_date, int(amount), now),
            )
            conn.commit()
        finally:
            conn.close()
