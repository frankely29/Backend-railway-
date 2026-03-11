from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core import DB_BACKEND, _clean_display_name, _db, _db_lock, _db_query_all, _sql, require_user

router = APIRouter(prefix="/chat", tags=["chat"])

_RATE_LIMIT_SECONDS = 2.0
_rate_limit_lock = threading.Lock()
_last_message_by_user: dict[int, float] = {}


class ChatMessagePayload(BaseModel):
    text: str


def _to_iso(unix_ts: int) -> str:
    return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).isoformat()


def _parse_after(after: str | None) -> tuple[str | None, int | None]:
    if after is None:
        return None, None

    value = after.strip()
    if not value:
        return None, None

    if value.isdigit():
        return "id", int(value)

    iso_value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_value)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid after. Use numeric id or ISO timestamp.")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return "created_at", int(dt.timestamp())


def _normalize_room(room: str) -> str:
    cleaned = (room or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="Room is required")
    return cleaned


def _dm_room_for_users(a: int, b: int) -> str:
    low, high = sorted((int(a), int(b)))
    return f"dm:{low}:{high}"


def _validate_text(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="text cannot be empty")
    if len(cleaned) > 600:
        raise HTTPException(status_code=400, detail="text too long (max 600)")
    return cleaned


def _enforce_rate_limit(user_id: int) -> None:
    now = time.monotonic()
    with _rate_limit_lock:
        prev = _last_message_by_user.get(user_id)
        if prev is not None and (now - prev) < _RATE_LIMIT_SECONDS:
            raise HTTPException(status_code=429, detail="Too many messages. Slow down.")
        _last_message_by_user[user_id] = now


def _serialize_message(row: dict) -> dict:
    # created_at is an int in SQLite or a datetime in Postgres. Normalize it.
    created = row["created_at"]
    if isinstance(created, (int, float)):
        ts = int(created)
    else:
        ts = int(created.timestamp())

    return {
        "id": int(row["id"]),
        "room": row["room"],
        "user_id": int(row["user_id"]),
        "display_name": row["display_name"],
        "text": row["message"],
        "created_at": _to_iso(ts),
    }


def _ensure_dm_target_exists(other_user_id: int):
    row = _db_query_all(
        "SELECT id, is_disabled FROM users WHERE id=? LIMIT 1",
        (int(other_user_id),),
    )
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    target = dict(row[0])
    is_disabled = bool(target["is_disabled"]) if DB_BACKEND == "postgres" else int(target["is_disabled"] or 0) == 1
    if is_disabled:
        raise HTTPException(status_code=404, detail="User not found")


def _list_messages_for_room(room: str, after: str | None, limit: int) -> dict:
    safe_room = _normalize_room(room)
    safe_limit = max(1, min(200, int(limit)))
    after_field, after_value = _parse_after(after)

    where = ["room = ?"]
    params: list[int | str] = [safe_room]

    if after_field == "id":
        where.append("id > ?")
        params.append(int(after_value))
    elif after_field == "created_at":
        if DB_BACKEND == "postgres":
            where.append("created_at > to_timestamp(?)")
            params.append(int(after_value))
        else:
            where.append("created_at > ?")
            params.append(int(after_value))

    if after_field is None:
        rows = _db_query_all(
            """
            SELECT id, room, user_id, display_name, message, created_at
            FROM (
                SELECT id, room, user_id, display_name, message, created_at
                FROM chat_messages
                WHERE room = ?
                ORDER BY id DESC
                LIMIT ?
            ) recent
            ORDER BY id ASC
            """,
            (safe_room, safe_limit),
        )
    else:
        rows = _db_query_all(
            f"""
            SELECT id, room, user_id, display_name, message, created_at
            FROM chat_messages
            WHERE {' AND '.join(where)}
            ORDER BY id ASC
            LIMIT ?
            """,
            tuple(params + [safe_limit]),
        )

    return {"room": safe_room, "messages": [_serialize_message(dict(r)) for r in rows]}


def _create_message_for_room(room: str, payload: ChatMessagePayload, user) -> dict:
    safe_room = _normalize_room(room)
    message = _validate_text(payload.text)
    user_id = int(user["id"])
    _enforce_rate_limit(user_id)

    now = int(time.time())
    display_name = _clean_display_name(user["display_name"] or "", user["email"])

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            if DB_BACKEND == "postgres":
                insert_sql = """
                    INSERT INTO chat_messages(room, user_id, display_name, message, created_at)
                    VALUES (?, ?, ?, ?, to_timestamp(?))
                """
                cur.execute(_sql(insert_sql + " RETURNING id"), (safe_room, user_id, display_name, message, now))
                row = cur.fetchone()
                new_id = int(row["id"])
            else:
                insert_sql = """
                    INSERT INTO chat_messages(room, user_id, display_name, message, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """
                cur.execute(_sql(insert_sql), (safe_room, user_id, display_name, message, now))
                new_id = int(cur.lastrowid)
            conn.commit()
        finally:
            conn.close()

    return {
        "id": new_id,
        "room": safe_room,
        "user_id": user_id,
        "display_name": display_name,
        "text": message,
        "created_at": _to_iso(now),
    }


@router.get("/rooms/{room}")
def list_room_messages(
    room: str,
    after: str | None = None,
    limit: int = 50,
    user=Depends(require_user),
):
    _ = user
    return _list_messages_for_room(room, after, limit)


@router.post("/rooms/{room}")
def create_room_message(room: str, payload: ChatMessagePayload, user=Depends(require_user)):
    return _create_message_for_room(room, payload, user)


@router.get("/dm/{other_user_id}")
def list_dm_messages(
    other_user_id: int,
    after: str | None = None,
    limit: int = 50,
    user=Depends(require_user),
):
    my_user_id = int(user["id"])
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    room = _dm_room_for_users(my_user_id, int(other_user_id))
    return _list_messages_for_room(room, after, limit)


@router.post("/dm/{other_user_id}")
def create_dm_message(other_user_id: int, payload: ChatMessagePayload, user=Depends(require_user)):
    my_user_id = int(user["id"])
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    room = _dm_room_for_users(my_user_id, int(other_user_id))
    return _create_message_for_room(room, payload, user)
