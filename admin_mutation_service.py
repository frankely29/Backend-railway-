from __future__ import annotations

import time
from typing import Any, Dict, Optional

from fastapi import HTTPException

from core import DB_BACKEND, _db_exec, _db_query_one


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
    target = _db_query_one("SELECT id, is_admin FROM users WHERE id=? LIMIT 1", (int(user_id),))
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    if not is_admin and _flag_to_bool(target["is_admin"]):
        admin_count_row = _db_query_one("SELECT COUNT(*) AS c FROM users WHERE is_admin = ?", (_bool_db_value(True),))
        admin_count = int(admin_count_row["c"]) if admin_count_row else 0
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="Cannot demote the last remaining admin")

    _db_exec("UPDATE users SET is_admin=? WHERE id=?", (_bool_db_value(is_admin), int(user_id)))

    return {"ok": True, "user_id": int(user_id), "is_admin": bool(is_admin)}


def set_user_suspended(actor_user_id: int, user_id: int, is_suspended: bool) -> Dict[str, Any]:
    target = _db_query_one("SELECT id FROM users WHERE id=? LIMIT 1", (int(user_id),))
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    if int(actor_user_id) == int(user_id) and bool(is_suspended):
        raise HTTPException(status_code=400, detail="Admins cannot suspend themselves")

    _db_exec("UPDATE users SET is_suspended=? WHERE id=?", (_bool_db_value(is_suspended), int(user_id)))

    return {"ok": True, "user_id": int(user_id), "is_suspended": bool(is_suspended)}


def get_admin_user_detail(user_id: int) -> Dict[str, Any]:
    user = _db_query_one(
        """
        SELECT id, email, display_name, is_admin, is_suspended, ghost_mode, avatar_url, created_at
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

    pickup_count_row = _db_query_one("SELECT COUNT(*) AS c FROM pickup_logs WHERE user_id=?", (int(user_id),))
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
        "is_suspended": _flag_to_bool(user.get("is_suspended")),
        "ghost_mode": _flag_to_bool(user.get("ghost_mode")),
        "avatar_url": user.get("avatar_url"),
        "created_at": _to_iso(user.get("created_at")),
        "presence": presence_payload,
        "pickup_count": int(pickup_count_row["c"]) if pickup_count_row else 0,
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


def clear_pickup_report(report_id: int) -> Dict[str, Any]:
    existing = _db_query_one("SELECT id FROM pickup_logs WHERE id=? LIMIT 1", (int(report_id),))
    if not existing:
        raise HTTPException(status_code=404, detail="Pickup report not found")

    _db_exec("DELETE FROM pickup_logs WHERE id=?", (int(report_id),))

    return {"ok": True, "report_id": int(report_id), "cleared": True}
