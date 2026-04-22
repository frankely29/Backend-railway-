from __future__ import annotations

import time
from typing import Any, Dict, Optional

from fastapi import HTTPException

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one, is_account_owner
from pickup_recording_feature import pickup_log_not_voided_sql, soft_void_pickup_trip
from subscription_state import days_until_comp_ends, is_comp_forever


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


def _bool_db_value(value: bool) -> Any:
    if DB_BACKEND == "postgres":
        return bool(value)
    return 1 if value else 0


def set_user_admin(actor_user_id: int, user_id: int, is_admin: bool) -> Dict[str, Any]:
    target = _db_query_one("SELECT id, email, is_admin FROM users WHERE id=? LIMIT 1", (int(user_id),))
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    if not is_admin and is_account_owner(target):
        raise HTTPException(status_code=403, detail="Cannot modify the account owner")

    if int(actor_user_id) == int(user_id) and not is_admin:
        raise HTTPException(status_code=400, detail="Admins cannot demote themselves")

    if not is_admin and _flag_to_bool(target["is_admin"]):
        admin_count_row = _db_query_one("SELECT COUNT(*) AS c FROM users WHERE is_admin = ?", (_bool_db_value(True),))
        admin_count = int(admin_count_row["c"]) if admin_count_row else 0
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="Cannot demote the last remaining admin")

    _db_exec("UPDATE users SET is_admin=? WHERE id=?", (_bool_db_value(is_admin), int(user_id)))

    return {"ok": True, "user_id": int(user_id), "is_admin": bool(is_admin)}


def set_user_suspended(actor_user_id: int, user_id: int, is_suspended: bool) -> Dict[str, Any]:
    target = _db_query_one(
        "SELECT id, email, ghost_mode, is_disabled, is_suspended FROM users WHERE id=? LIMIT 1",
        (int(user_id),),
    )
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    if bool(is_suspended) and is_account_owner(target):
        raise HTTPException(status_code=403, detail="Cannot modify the account owner")

    if int(actor_user_id) == int(user_id) and bool(is_suspended):
        raise HTTPException(status_code=400, detail="Admins cannot suspend themselves")

    _db_exec("UPDATE users SET is_suspended=? WHERE id=?", (_bool_db_value(is_suspended), int(user_id)))
    changed_at_ms = int(time.time() * 1000)
    if bool(is_suspended):
        _db_exec("DELETE FROM presence WHERE user_id=?", (int(user_id),))
        _db_exec(
            """
            INSERT INTO presence_runtime_state(user_id, changed_at_ms, is_visible, reason)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
              changed_at_ms=excluded.changed_at_ms,
              is_visible=excluded.is_visible,
              reason=excluded.reason
            """,
            (int(user_id), changed_at_ms, _bool_db_value(False), "suspended"),
        )
    else:
        is_visible = not _flag_to_bool(target.get("ghost_mode")) and not _flag_to_bool(target.get("is_disabled"))
        _db_exec(
            """
            INSERT INTO presence_runtime_state(user_id, changed_at_ms, is_visible, reason)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
              changed_at_ms=excluded.changed_at_ms,
              is_visible=excluded.is_visible,
              reason=excluded.reason
            """,
            (int(user_id), changed_at_ms, _bool_db_value(is_visible), None if is_visible else "hidden"),
        )

    return {"ok": True, "user_id": int(user_id), "is_suspended": bool(is_suspended)}


def get_admin_user_detail(user_id: int) -> Dict[str, Any]:
    user = _db_query_one(
        """
        SELECT id, email, display_name, is_admin, is_disabled, is_suspended, ghost_mode, avatar_url, created_at,
               trial_expires_at,
               subscription_status, subscription_provider,
               subscription_customer_id, subscription_id,
               subscription_current_period_end,
               subscription_comp_reason, subscription_comp_granted_by, subscription_comp_granted_at,
               subscription_comp_expires_at,
               subscription_updated_at
        FROM users
        WHERE id=?
        LIMIT 1
        """,
        (int(user_id),),
    )
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    presence = _db_query_one(
        """
        SELECT lat, lng, heading, accuracy, updated_at
        FROM presence
        WHERE user_id=?
        LIMIT 1
        """,
        (int(user_id),),
    )

    active_pickup_count_row = _db_query_one(
        f"""
        SELECT COUNT(*) AS c
        FROM pickup_logs pl
        WHERE pl.user_id=? AND {pickup_log_not_voided_sql('pl')}
        """,
        (int(user_id),),
    )
    voided_pickup_count_row = _db_query_one(
        f"""
        SELECT COUNT(*) AS c
        FROM pickup_logs pl
        WHERE pl.user_id=? AND NOT ({pickup_log_not_voided_sql('pl')})
        """,
        (int(user_id),),
    )
    police_count_row = _db_query_one(
        "SELECT COUNT(*) AS c FROM events WHERE user_id=? AND lower(type)='police'",
        (int(user_id),),
    )

    presence_payload = None
    if presence:
        presence_payload = {
            "lat": float(presence["lat"]),
            "lng": float(presence["lng"]),
            "heading": float(presence["heading"]) if presence.get("heading") is not None else None,
            "accuracy": float(presence["accuracy"]) if presence.get("accuracy") is not None else None,
            "updated_at": _to_iso(presence.get("updated_at")),
        }

    return {
        "id": int(user["id"]),
        "email": user.get("email"),
        "display_name": user.get("display_name"),
        "is_admin": _flag_to_bool(user.get("is_admin")),
        "is_disabled": _flag_to_bool(user.get("is_disabled")),
        "is_suspended": _flag_to_bool(user.get("is_suspended")),
        "ghost_mode": _flag_to_bool(user.get("ghost_mode")),
        "avatar_url": user.get("avatar_url"),
        "created_at": _to_iso(user.get("created_at")),
        "trial_expires_at": user.get("trial_expires_at"),
        "subscription_status": user.get("subscription_status"),
        "subscription_provider": user.get("subscription_provider"),
        "subscription_customer_id": user.get("subscription_customer_id"),
        "subscription_id": user.get("subscription_id"),
        "subscription_current_period_end": user.get("subscription_current_period_end"),
        "subscription_comp_reason": user.get("subscription_comp_reason"),
        "subscription_comp_granted_by": user.get("subscription_comp_granted_by"),
        "subscription_comp_granted_at": user.get("subscription_comp_granted_at"),
        "subscription_comp_expires_at": user.get("subscription_comp_expires_at"),
        "subscription_updated_at": user.get("subscription_updated_at"),
        "presence": presence_payload,
        "pickup_count": int(active_pickup_count_row["c"]) if active_pickup_count_row else 0,
        "voided_pickup_count": int(voided_pickup_count_row["c"]) if voided_pickup_count_row else 0,
        "police_report_count": int(police_count_row["c"]) if police_count_row else 0,
    }


def clear_police_report(report_id: int) -> Dict[str, Any]:
    existing = _db_query_one(
        "SELECT id FROM events WHERE id=? AND lower(type)='police' LIMIT 1",
        (int(report_id),),
    )
    if not existing:
        raise HTTPException(status_code=404, detail="Police report not found")

    _db_exec("DELETE FROM events WHERE id=? AND lower(type)='police'", (int(report_id),))

    return {"ok": True, "report_id": int(report_id), "cleared": True}


def clear_pickup_report(report_id: int, admin_user_id: int) -> Dict[str, Any]:
    result = soft_void_pickup_trip(
        trip_id=int(report_id),
        admin_user_id=int(admin_user_id),
        reason="Legacy admin clear pickup log",
    )
    response = {
        "ok": True,
        "report_id": int(report_id),
        "cleared": True,
        "soft_deleted": True,
    }
    response.update(result)
    return response


def _duration_to_seconds(unit: str, value: int) -> Optional[int]:
    if unit == "forever":
        return None
    multipliers = {"hours": 3600, "days": 86400, "weeks": 604800}
    return int(value) * multipliers[unit]


def grant_comp(actor_user_id: int, user_id: int, duration_unit: str, duration_value: int, reason: str) -> Dict[str, Any]:
    target = _db_query_one("SELECT id FROM users WHERE id=? LIMIT 1", (int(user_id),))
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    now = int(time.time())
    seconds = _duration_to_seconds(duration_unit, int(duration_value))
    comp_expires_at = None if seconds is None else (now + seconds)

    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_comp_reason=?,
            subscription_comp_granted_by=?,
            subscription_comp_granted_at=?,
            subscription_comp_expires_at=?,
            subscription_updated_at=?
        WHERE id=?
        """,
        ("comp", reason, int(actor_user_id), now, comp_expires_at, now, int(user_id)),
    )

    updated = _db_query_one("SELECT * FROM users WHERE id=?", (int(user_id),))
    days_remaining = days_until_comp_ends(updated)
    is_forever = is_comp_forever(updated)

    return {
        "ok": True,
        "user_id": int(user_id),
        "status": "comp",
        "comp_expires_at": comp_expires_at,
        "comp_reason": reason,
        "is_comp_forever": is_forever,
        "days_remaining": days_remaining,
    }


def extend_comp(actor_user_id: int, user_id: int, duration_unit: str, duration_value: int) -> Dict[str, Any]:
    target = _db_query_one(
        "SELECT id, subscription_status, subscription_comp_expires_at, subscription_comp_reason FROM users WHERE id=? LIMIT 1",
        (int(user_id),),
    )
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    now = int(time.time())
    seconds = _duration_to_seconds(duration_unit, int(duration_value))
    if seconds is None:
        raise HTTPException(status_code=400, detail="Cannot extend with 'forever' unit; use grant instead")

    current_status = target["subscription_status"]
    current_expires_raw = target["subscription_comp_expires_at"]

    try:
        current_expires = int(current_expires_raw) if current_expires_raw is not None else None
    except Exception:
        current_expires = None

    if current_status == "comp" and current_expires is not None and current_expires > now:
        new_expires = current_expires + seconds
    else:
        new_expires = now + seconds

    reason = target["subscription_comp_reason"] or "Extended"

    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_comp_reason=?,
            subscription_comp_granted_by=?,
            subscription_comp_granted_at=?,
            subscription_comp_expires_at=?,
            subscription_updated_at=?
        WHERE id=?
        """,
        ("comp", reason, int(actor_user_id), now, new_expires, now, int(user_id)),
    )

    updated = _db_query_one("SELECT * FROM users WHERE id=?", (int(user_id),))
    return {
        "ok": True,
        "user_id": int(user_id),
        "status": "comp",
        "comp_expires_at": new_expires,
        "comp_reason": reason,
        "is_comp_forever": False,
        "days_remaining": days_until_comp_ends(updated),
    }


def revoke_comp(actor_user_id: int, user_id: int) -> Dict[str, Any]:
    _ = actor_user_id
    target = _db_query_one(
        "SELECT id, subscription_status, subscription_id, subscription_current_period_end FROM users WHERE id=? LIMIT 1",
        (int(user_id),),
    )
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    now = int(time.time())
    new_status = "none"
    period_end_raw = target["subscription_current_period_end"]
    try:
        period_end = int(period_end_raw) if period_end_raw is not None else None
    except Exception:
        period_end = None

    if target["subscription_id"] and period_end and period_end > now:
        new_status = "active"

    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_comp_reason=NULL,
            subscription_comp_granted_by=NULL,
            subscription_comp_granted_at=NULL,
            subscription_comp_expires_at=NULL,
            subscription_updated_at=?
        WHERE id=?
        """,
        (new_status, now, int(user_id)),
    )

    return {
        "ok": True,
        "user_id": int(user_id),
        "status": new_status,
    }


def list_active_comps(limit: int = 100, offset: int = 0, search: Optional[str] = None) -> Dict[str, Any]:
    now = int(time.time())

    search_clause = ""
    search_params: list = []
    if search:
        search_clause = " AND (email LIKE ? OR display_name LIKE ? OR subscription_comp_reason LIKE ?)"
        pattern = f"%{search}%"
        search_params = [pattern, pattern, pattern]

    base_where = f"""
        subscription_status = 'comp'
        AND (subscription_comp_expires_at IS NULL OR subscription_comp_expires_at > ?)
        {search_clause}
    """

    count_row = _db_query_one(
        f"SELECT COUNT(*) AS c FROM users WHERE {base_where}",
        (now, *search_params),
    )
    total = int(count_row["c"]) if count_row else 0

    rows = _db_query_all(
        f"""
        SELECT id, email, display_name,
               subscription_comp_reason, subscription_comp_granted_by,
               subscription_comp_granted_at, subscription_comp_expires_at
        FROM users
        WHERE {base_where}
        ORDER BY subscription_comp_granted_at DESC
        LIMIT ? OFFSET ?
        """,
        (now, *search_params, int(limit), int(offset)),
    )

    items = []
    for row in rows:
        comp_expires_raw = row["subscription_comp_expires_at"]
        is_forever = comp_expires_raw is None
        try:
            comp_expires = int(comp_expires_raw) if comp_expires_raw is not None else None
        except Exception:
            comp_expires = None
        days_remaining = None if comp_expires is None else max(0, (comp_expires - now) // 86400)

        items.append(
            {
                "user_id": int(row["id"]),
                "email": row["email"],
                "display_name": row["display_name"],
                "reason": row["subscription_comp_reason"],
                "granted_by": int(row["subscription_comp_granted_by"]) if row["subscription_comp_granted_by"] is not None else None,
                "granted_at": int(row["subscription_comp_granted_at"]) if row["subscription_comp_granted_at"] is not None else None,
                "expires_at": comp_expires,
                "days_remaining": days_remaining,
                "is_forever": is_forever,
            }
        )

    return {"items": items, "total": total}
