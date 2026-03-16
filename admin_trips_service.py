from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from core import _db_query_all, _db_query_one
from pickup_recording_feature import pickup_log_not_voided_sql


def _flag_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return int(value) == 1


def _to_iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))
    except Exception:
        return str(ts)


def get_admin_trips_summary() -> Dict[str, Any]:
    cutoff_24h = int(time.time()) - 86400
    cutoff_7d = int(time.time()) - (7 * 86400)

    row = _db_query_one(
        f"""
        SELECT
            SUM(CASE WHEN {pickup_log_not_voided_sql('pl')} THEN 1 ELSE 0 END) AS total_recorded_trips,
            SUM(CASE WHEN {pickup_log_not_voided_sql('pl')} AND pl.created_at >= ? THEN 1 ELSE 0 END) AS trips_last_24h,
            SUM(CASE WHEN {pickup_log_not_voided_sql('pl')} AND pl.created_at >= ? THEN 1 ELSE 0 END) AS trips_last_7d,
            MAX(CASE WHEN {pickup_log_not_voided_sql('pl')} THEN pl.created_at ELSE NULL END) AS latest_trip_at,
            COUNT(DISTINCT CASE WHEN {pickup_log_not_voided_sql('pl')} THEN pl.user_id ELSE NULL END) AS distinct_users_count,
            COUNT(DISTINCT CASE WHEN {pickup_log_not_voided_sql('pl')} THEN pl.zone_id ELSE NULL END) AS distinct_zones_count,
            SUM(CASE WHEN NOT ({pickup_log_not_voided_sql('pl')}) THEN 1 ELSE 0 END) AS voided_trips_count
        FROM pickup_logs pl
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
            "active_recorded_trips": 0,
            "voided_trips_count": 0,
        }

    data = dict(row)
    active_recorded_trips = int(data.get("total_recorded_trips") or 0)
    return {
        "total_recorded_trips": active_recorded_trips,
        "trips_last_24h": int(data.get("trips_last_24h") or 0),
        "trips_last_7d": int(data.get("trips_last_7d") or 0),
        "latest_trip_at": _to_iso(data.get("latest_trip_at")),
        "distinct_users_count": int(data.get("distinct_users_count") or 0),
        "distinct_zones_count": int(data.get("distinct_zones_count") or 0),
        "active_recorded_trips": active_recorded_trips,
        "voided_trips_count": int(data.get("voided_trips_count") or 0),
    }


def get_admin_recent_trips(limit: int = 20, include_voided: bool = True) -> List[Dict[str, Any]]:
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
            pl.void_reason,
            pl.guard_reason,
            pl.counted_for_pickup_stats
        FROM pickup_logs pl
        LEFT JOIN users u ON u.id = pl.user_id
        WHERE {'1=1' if include_voided else pickup_log_not_voided_sql('pl')}
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
                "is_voided": _flag_to_bool(row.get("is_voided")),
                "voided_at": _to_iso(row.get("voided_at")),
                "void_reason": row.get("void_reason"),
                "guard_reason": row.get("guard_reason"),
                "counted_for_pickup_stats": _flag_to_bool(row.get("counted_for_pickup_stats")),
            }
        )
    return items
