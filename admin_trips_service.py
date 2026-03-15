from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from core import DB_BACKEND, _db, _db_lock, _db_query_all, _db_query_one, _sql
from leaderboard_tracker import decrement_pickup_count_for_timestamp


def _to_iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))
    except Exception:
        return str(ts)


def _not_voided_sql(alias: str) -> str:
    if DB_BACKEND == "postgres":
        return f"COALESCE({alias}.is_voided, FALSE) = FALSE"
    return f"COALESCE(CAST({alias}.is_voided AS INTEGER), 0) = 0"


def get_admin_trips_summary(include_voided: bool = False) -> Dict[str, Any]:
    cutoff_24h = int(time.time()) - 86400
    cutoff_7d = int(time.time()) - (7 * 86400)
    active_filter = "" if include_voided else f"WHERE {_not_voided_sql('pl')}"

    row = _db_query_one(
        f"""
        SELECT
            COUNT(*) AS total_recorded_trips,
            SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS trips_last_24h,
            SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS trips_last_7d,
            MAX(created_at) AS latest_trip_at,
            COUNT(DISTINCT user_id) AS distinct_users_count,
            COUNT(DISTINCT zone_id) AS distinct_zones_count,
            SUM(CASE WHEN NOT {_not_voided_sql('pl')} THEN 1 ELSE 0 END) AS voided_trips_count
        FROM pickup_logs pl
        {active_filter}
        """,
        (cutoff_24h, cutoff_7d),
    )

    if not row:
        return {
            "total_recorded_trips": 0,
            "trips_last_24h": 0,
            "trips_last_7d": 0,
            "latest_trip_at": None,
            "distinct_users_count": 0,
            "distinct_zones_count": 0,
            "voided_trips_count": 0,
        }

    data = dict(row)
    return {
        "total_recorded_trips": int(data.get("total_recorded_trips") or 0),
        "trips_last_24h": int(data.get("trips_last_24h") or 0),
        "trips_last_7d": int(data.get("trips_last_7d") or 0),
        "latest_trip_at": _to_iso(data.get("latest_trip_at")),
        "distinct_users_count": int(data.get("distinct_users_count") or 0),
        "distinct_zones_count": int(data.get("distinct_zones_count") or 0),
        "voided_trips_count": int(data.get("voided_trips_count") or 0),
    }


def get_admin_recent_trips(limit: int = 20, include_voided: bool = False) -> List[Dict[str, Any]]:
    where_sql = "" if include_voided else f"WHERE {_not_voided_sql('pl')}"
    rows = _db_query_all(
        f"""
        SELECT
            pl.id,
            pl.user_id,
            u.display_name,
            pl.zone_id,
            NULL AS location_id,
            pl.zone_name,
            pl.borough,
            pl.frame_time,
            pl.created_at,
            pl.lat,
            pl.lng,
            pl.is_voided,
            pl.voided_at,
            pl.void_reason
        FROM pickup_logs pl
        LEFT JOIN users u ON u.id = pl.user_id
        {where_sql}
        ORDER BY pl.created_at DESC
        LIMIT ?
        """,
        (int(limit),),
    )

    items: List[Dict[str, Any]] = []
    for r in rows:
        row = dict(r)
        items.append(
            {
                "id": int(row["id"]) if row.get("id") is not None else None,
                "user_id": int(row["user_id"]) if row.get("user_id") is not None else None,
                "display_name": row.get("display_name"),
                "zone_id": int(row["zone_id"]) if row.get("zone_id") is not None else None,
                "location_id": int(row["location_id"]) if row.get("location_id") is not None else None,
                "zone_name": row.get("zone_name"),
                "borough": row.get("borough"),
                "frame_time": row.get("frame_time"),
                "created_at": _to_iso(row.get("created_at")),
                "lat": float(row["lat"]) if row.get("lat") is not None else None,
                "lng": float(row["lng"]) if row.get("lng") is not None else None,
                "is_voided": bool(row.get("is_voided")),
                "voided_at": _to_iso(row.get("voided_at")),
                "void_reason": row.get("void_reason"),
            }
        )
    return items


def void_admin_recorded_trip(trip_id: int, admin_user_id: int, reason: str) -> Dict[str, Any]:
    clean_reason = (reason or "").strip()
    if len(clean_reason) < 5:
        raise HTTPException(status_code=422, detail="reason must be at least 5 characters")

    now_ts = int(time.time())
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute(_sql("SELECT * FROM pickup_logs WHERE id=? LIMIT 1"), (int(trip_id),))
            trip = cur.fetchone()
            if not trip:
                raise HTTPException(status_code=404, detail="trip not found")
            trip_data = dict(trip)

            already_voided = bool(trip_data.get("is_voided"))
            if already_voided:
                return {
                    "ok": False,
                    "trip_id": int(trip_id),
                    "voided": True,
                    "stats_reversed": False,
                    "preserved_in_audit": True,
                }

            cur.execute(
                _sql(
                    """
                    UPDATE pickup_logs
                    SET is_voided=?,
                        voided_at=?,
                        voided_by_admin_user_id=?,
                        void_reason=?
                    WHERE id=?
                    """
                ),
                (
                    True if DB_BACKEND == "postgres" else 1,
                    now_ts,
                    int(admin_user_id),
                    clean_reason,
                    int(trip_id),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    stats_reversed = bool(trip_data.get("counted_for_pickup_stats"))
    if stats_reversed:
        decrement_pickup_count_for_timestamp(int(trip_data["user_id"]), int(trip_data["created_at"]), 1)

    return {
        "ok": True,
        "trip_id": int(trip_id),
        "voided": True,
        "stats_reversed": stats_reversed,
        "preserved_in_audit": True,
    }
