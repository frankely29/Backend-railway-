from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from core import _db, _db_lock, _sql

NYC_TZ = ZoneInfo("America/New_York")
MAX_COUNTED_GAP_SECONDS = 300
MAX_SPEED_MPH = 100.0
MILES_PER_METER = 0.000621371


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius_m = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)

    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_m * c * MILES_PER_METER


def _nyc_date_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(NYC_TZ).date().isoformat()


def _split_seconds_by_nyc_date(start_ts: int, end_ts: int) -> list[tuple[str, float]]:
    if end_ts <= start_ts:
        return []
    cursor = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    out: list[tuple[str, float]] = []
    while cursor < end_dt:
        local = cursor.astimezone(NYC_TZ)
        next_midnight_local = datetime.combine(local.date() + timedelta(days=1), datetime.min.time(), tzinfo=NYC_TZ)
        chunk_end = min(end_dt, next_midnight_local.astimezone(timezone.utc))
        sec = max(0.0, (chunk_end - cursor).total_seconds())
        out.append((local.date().isoformat(), sec))
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
    nyc_date = _nyc_date_from_unix(now)

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute(_sql("SELECT * FROM driver_work_state WHERE user_id=? LIMIT 1"), (int(user_id),))
            prev = cur.fetchone()

            counted_seconds = 0
            distance_miles = 0.0
            if prev and prev["last_seen_at"]:
                prev_seen = int(prev["last_seen_at"])
                if now > prev_seen:
                    raw_gap = now - prev_seen
                    counted_seconds = min(raw_gap, MAX_COUNTED_GAP_SECONDS)

                    prev_lat = prev["last_lat"]
                    prev_lng = prev["last_lng"]
                    if prev_lat is not None and prev_lng is not None:
                        d = _haversine_miles(float(prev_lat), float(prev_lng), float(lat), float(lng))
                        speed_mph = d / (raw_gap / 3600.0) if raw_gap > 0 else 0.0
                        if speed_mph <= MAX_SPEED_MPH:
                            distance_miles = d

            if counted_seconds > 0:
                splits = _split_seconds_by_nyc_date(now - counted_seconds, now)
                total = sum(sec for _, sec in splits) or 1.0
                for dt, sec in splits:
                    miles_chunk = distance_miles * (sec / total)
                    _upsert_daily_stat(cur, user_id, dt, miles_chunk, sec / 3600.0, 1 if dt == nyc_date else 0)
            else:
                _upsert_daily_stat(cur, user_id, nyc_date, 0.0, 0.0, 1)

            cur.execute(
                _sql(
                    """
                    INSERT INTO driver_work_state(user_id, last_seen_at, last_lat, last_lng, last_heading, last_nyc_date, updated_at)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(user_id) DO UPDATE SET
                      last_seen_at=excluded.last_seen_at,
                      last_lat=excluded.last_lat,
                      last_lng=excluded.last_lng,
                      last_heading=excluded.last_heading,
                      last_nyc_date=excluded.last_nyc_date,
                      updated_at=excluded.updated_at
                    """
                ),
                (int(user_id), now, float(lat), float(lng), heading, nyc_date, now),
            )
            conn.commit()
        finally:
            conn.close()


def increment_pickup_count(user_id: int, amount: int = 1) -> None:
    nyc_date = _nyc_date_from_unix(int(time.time()))
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            now = int(time.time())
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
                (int(user_id), nyc_date, int(amount), now),
            )
            conn.commit()
        finally:
            conn.close()
