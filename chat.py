from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from main import _clean_display_name, _db, _db_lock, _db_query_all, require_user

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


def _validate_text(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="text cannot be empty")
    if len(cleaned) > 280:
        raise HTTPException(status_code=400, detail="text too long (max 280)")
    return cleaned


def _enforce_rate_limit(user_id: int) -> None:
    now = time.monotonic()
    with _rate_limit_lock:
        prev = _last_message_by_user.get(user_id)
        if prev is not None and (now - prev) < _RATE_LIMIT_SECONDS:
            raise HTTPException(status_code=429, detail="Too many messages. Slow down.")
        _last_message_by_user[user_id] = now


def _serialize_message(row: dict) -> dict:
    return {
        "id": int(row["id"]),
        "room": row["room"],
        "user_id": int(row["user_id"]),
        "display_name": row["display_name"],
        "text": row["message"],
        "created_at": _to_iso(int(row["created_at"])),
    }


@router.get("/rooms/{room}")
def list_room_messages(
    room: str,
    after: str | None = None,
    limit: int = 50,
    user=Depends(require_user),
):
    _ = user
    safe_room = _normalize_room(room)
    safe_limit = max(1, min(200, int(limit)))
    after_field, after_value = _parse_after(after)

    where = ["room = ?"]
    params: list[int | str] = [safe_room]

    if after_field == "id":
        where.append("id > ?")
        params.append(int(after_value))
    elif after_field == "created_at":
        where.append("created_at > ?")
        params.append(int(after_value))

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


@router.post("/rooms/{room}")
def create_room_message(room: str, payload: ChatMessagePayload, user=Depends(require_user)):
    safe_room = _normalize_room(room)
    message = _validate_text(payload.text)
    user_id = int(user["id"])
    _enforce_rate_limit(user_id)

    now = int(time.time())
    display_name = _clean_display_name(user["display_name"] or "", user["email"])

    with _db_lock:
        conn = _db()
        try:
            cur = conn.execute(
                """
                INSERT INTO chat_messages(room, user_id, display_name, message, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (safe_room, user_id, display_name, message, now),
            )
            conn.commit()
            new_id = int(cur.lastrowid)
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
