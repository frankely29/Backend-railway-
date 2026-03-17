from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core import DB_BACKEND, _clean_display_name, _db, _db_exec, _db_lock, _db_query_all, _sql, require_user

router = APIRouter(prefix="/chat", tags=["chat"])

_RATE_LIMIT_SECONDS = 2.0
_rate_limit_lock = threading.Lock()
_last_message_by_user: dict[int, float] = {}


class ChatMessagePayload(BaseModel):
    text: str


class PrivateChatSendIn(BaseModel):
    text: str


class PrivateChatMessageOut(BaseModel):
    id: int
    sender_user_id: int
    recipient_user_id: int
    text: str
    created_at: str


class PrivateChatMessagesResponse(BaseModel):
    other_user_id: int
    messages: list[PrivateChatMessageOut]


class PrivateChatThreadOut(BaseModel):
    other_user_id: int
    other_display_name: str
    other_avatar_url: str | None = None
    last_message_text: str
    last_message_at: str
    last_message_sender_user_id: int
    unread_count: int


class PrivateChatThreadsResponse(BaseModel):
    threads: list[PrivateChatThreadOut]


class DMUserRow(BaseModel):
    id: int
    display_name: str
    avatar_url: str | None = None
    is_online: bool | None = None
    last_seen_at: str | None = None
    leaderboard_badge_code: str | None = None


class DMUsersResponse(BaseModel):
    users: list[DMUserRow]


class DMThreadRow(BaseModel):
    other_user_id: int
    display_name: str
    avatar_url: str | None = None
    last_message_text: str
    last_message_created_at: str
    last_message_user_id: int
    room: str


class DMThreadsResponse(BaseModel):
    threads: list[DMThreadRow]


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


def _timestamp_to_iso(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(int(value), tz=timezone.utc)
    else:
        text = str(value).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            dt = datetime.fromtimestamp(int(float(value)), tz=timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


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

    sender_name = _resolve_sender_display_name(row)
    sender_avatar_url = row.get("sender_avatar_url")

    return {
        "id": int(row["id"]),
        "room": row["room"],
        "user_id": int(row["user_id"]),
        "display_name": sender_name,
        "sender_display_name": sender_name,
        "sender_avatar_url": sender_avatar_url,
        "text": row["message"],
        "created_at": _to_iso(ts),
    }


def _resolve_sender_display_name(row: dict) -> str:
    user_id = int(row["user_id"])
    user_display_name = (row.get("user_display_name") or "").strip()
    user_email = (row.get("user_email") or "").strip()
    message_display_name = (row.get("message_display_name") or row.get("display_name") or "").strip()

    if user_display_name:
        return user_display_name
    if user_email and "@" in user_email:
        return user_email.split("@", 1)[0].strip() or f"User {user_id}"
    if message_display_name:
        return message_display_name
    return f"User {user_id}"


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


def _serialize_private_message(row: dict) -> dict:
    return {
        "id": int(row["id"]),
        "sender_user_id": int(row["sender_user_id"]),
        "recipient_user_id": int(row["recipient_user_id"]),
        "text": row["text"],
        "created_at": _timestamp_to_iso(row["created_at"]),
    }


def _fetch_private_conversation(me: int, other: int, since_id: int | None) -> list[dict]:
    where = [
        "((sender_user_id=? AND recipient_user_id=?) OR (sender_user_id=? AND recipient_user_id=?))"
    ]
    params: list[int] = [int(me), int(other), int(other), int(me)]
    if since_id is not None:
        where.append("id > ?")
        params.append(int(since_id))

    rows = _db_query_all(
        f"""
        SELECT id, sender_user_id, recipient_user_id, text, created_at
        FROM private_chat_messages
        WHERE {' AND '.join(where)}
        ORDER BY created_at ASC, id ASC
        """,
        tuple(params),
    )
    return [_serialize_private_message(dict(row)) for row in rows]


def _mark_private_read(me: int, other: int) -> None:
    if DB_BACKEND == "postgres":
        _db_exec(
            """
            UPDATE private_chat_messages
            SET read_at = NOW()
            WHERE sender_user_id=? AND recipient_user_id=? AND read_at IS NULL
            """,
            (int(other), int(me)),
        )
    else:
        _db_exec(
            """
            UPDATE private_chat_messages
            SET read_at = CURRENT_TIMESTAMP
            WHERE sender_user_id=? AND recipient_user_id=? AND read_at IS NULL
            """,
            (int(other), int(me)),
        )


def _list_private_threads(me: int) -> list[dict]:
    thread_rows = _db_query_all(
        """
        SELECT
            CASE
                WHEN sender_user_id=? THEN recipient_user_id
                ELSE sender_user_id
            END AS other_user_id,
            MAX(created_at) AS last_message_at
        FROM private_chat_messages
        WHERE sender_user_id=? OR recipient_user_id=?
        GROUP BY other_user_id
        ORDER BY last_message_at DESC
        """,
        (int(me), int(me), int(me)),
    )
    if not thread_rows:
        return []

    other_ids = [int(row["other_user_id"]) for row in thread_rows]
    placeholders = ", ".join(["?"] * len(other_ids))

    user_rows = _db_query_all(
        f"SELECT id, display_name, avatar_url, email FROM users WHERE id IN ({placeholders})",
        tuple(other_ids),
    )
    users = {
        int(row["id"]): {
            "display_name": _clean_display_name(row["display_name"] or "", row["email"]),
            "avatar_url": row["avatar_url"],
        }
        for row in user_rows
    }

    unread_rows = _db_query_all(
        f"""
        SELECT sender_user_id AS other_user_id, COUNT(*) AS unread_count
        FROM private_chat_messages
        WHERE recipient_user_id=?
          AND read_at IS NULL
          AND sender_user_id IN ({placeholders})
        GROUP BY sender_user_id
        """,
        tuple([int(me)] + other_ids),
    )
    unread_by_other = {int(row["other_user_id"]): int(row["unread_count"]) for row in unread_rows}

    threads: list[dict] = []
    for row in thread_rows:
        other_user_id = int(row["other_user_id"])
        latest = _db_query_all(
            """
            SELECT id, sender_user_id, recipient_user_id, text, created_at
            FROM private_chat_messages
            WHERE (sender_user_id=? AND recipient_user_id=?) OR (sender_user_id=? AND recipient_user_id=?)
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (int(me), other_user_id, other_user_id, int(me)),
        )
        if not latest:
            continue
        last_message = dict(latest[0])
        user_payload = users.get(other_user_id)
        if user_payload is None:
            continue
        threads.append(
            {
                "other_user_id": other_user_id,
                "other_display_name": user_payload["display_name"],
                "other_avatar_url": user_payload["avatar_url"],
                "last_message_text": last_message["text"],
                "last_message_at": _timestamp_to_iso(last_message["created_at"]),
                "last_message_sender_user_id": int(last_message["sender_user_id"]),
                "unread_count": int(unread_by_other.get(other_user_id, 0)),
            }
        )
    return threads


def _create_private_message(sender_user_id: int, recipient_user_id: int, text: str) -> dict:
    clean_text = _validate_text(text)
    _enforce_rate_limit(int(sender_user_id))

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            if DB_BACKEND == "postgres":
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(sender_user_id, recipient_user_id, text)
                        VALUES (?, ?, ?)
                        RETURNING id, sender_user_id, recipient_user_id, text, created_at
                        """
                    ),
                    (int(sender_user_id), int(recipient_user_id), clean_text),
                )
                row = dict(cur.fetchone())
            else:
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(sender_user_id, recipient_user_id, text)
                        VALUES (?, ?, ?)
                        """
                    ),
                    (int(sender_user_id), int(recipient_user_id), clean_text),
                )
                new_id = int(cur.lastrowid)
                cur.execute(
                    _sql(
                        """
                        SELECT id, sender_user_id, recipient_user_id, text, created_at
                        FROM private_chat_messages
                        WHERE id=?
                        LIMIT 1
                        """
                    ),
                    (new_id,),
                )
                row = dict(cur.fetchone())
            conn.commit()
        finally:
            conn.close()

    return _serialize_private_message(row)


def _other_user_id_from_dm_room(room: str, me: int) -> int | None:
    parts = (room or "").split(":")
    if len(parts) != 3 or parts[0] != "dm":
        return None
    try:
        left = int(parts[1])
        right = int(parts[2])
    except ValueError:
        return None

    if left == int(me):
        return right
    if right == int(me):
        return left
    return None


def _list_dm_users(me: int, q: str | None) -> list[dict]:
    search = (q or "").strip().lower()
    where = ["id <> ?"]
    params: list[Any] = [int(me)]

    if DB_BACKEND == "postgres":
        where.append("COALESCE(is_disabled, FALSE) = FALSE")
        where.append("COALESCE(is_suspended, FALSE) = FALSE")
    else:
        where.append("COALESCE(is_disabled, 0) = 0")
        where.append("COALESCE(is_suspended, 0) = 0")

    if search:
        where.append("(LOWER(COALESCE(display_name, '')) LIKE ? OR LOWER(COALESCE(email, '')) LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like])

    user_rows = _db_query_all(
        f"""
        SELECT id, display_name, avatar_url, email
        FROM users
        WHERE {' AND '.join(where)}
        ORDER BY LOWER(COALESCE(display_name, email, '')) ASC, id ASC
        """,
        tuple(params),
    )
    if not user_rows:
        return []

    users: list[dict[str, Any]] = []
    user_ids: list[int] = []
    for row in user_rows:
        rid = int(row["id"])
        user_ids.append(rid)
        users.append(
            {
                "id": rid,
                "display_name": _clean_display_name(row["display_name"] or "", row["email"]),
                "avatar_url": row["avatar_url"],
                "is_online": None,
                "last_seen_at": None,
                "leaderboard_badge_code": None,
            }
        )

    placeholders = ", ".join(["?"] * len(user_ids))
    presence_rows = _db_query_all(
        f"""
        SELECT user_id, MAX(updated_at) AS updated_at
        FROM presence
        WHERE user_id IN ({placeholders})
        GROUP BY user_id
        """,
        tuple(user_ids),
    )
    now_ts = int(time.time())
    online_cutoff_seconds = 300
    presence_by_user: dict[int, tuple[bool, str]] = {}
    for row in presence_rows:
        last_seen_iso = _timestamp_to_iso(row["updated_at"])
        last_seen_ts = int(datetime.fromisoformat(last_seen_iso.replace("Z", "+00:00")).timestamp())
        presence_by_user[int(row["user_id"])] = (last_seen_ts >= now_ts - online_cutoff_seconds, last_seen_iso)

    for item in users:
        presence = presence_by_user.get(int(item["id"]))
        if presence is None:
            continue
        item["is_online"] = bool(presence[0])
        item["last_seen_at"] = presence[1]

    users.sort(key=lambda item: (0 if item["is_online"] else 1, item["display_name"].lower(), int(item["id"])))
    return users


def _list_dm_threads(me: int) -> list[dict]:
    message_rows = _db_query_all(
        """
        SELECT id, room, user_id, message, created_at
        FROM chat_messages
        WHERE room LIKE 'dm:%'
          AND (room LIKE ? OR room LIKE ?)
        ORDER BY created_at DESC, id DESC
        """,
        (f"dm:{int(me)}:%", f"dm:%:{int(me)}"),
    )
    if not message_rows:
        return []

    latest_by_other: dict[int, dict[str, Any]] = {}
    for row in message_rows:
        room = str(row["room"])
        other_user_id = _other_user_id_from_dm_room(room, int(me))
        if other_user_id is None or other_user_id == int(me):
            continue
        if other_user_id in latest_by_other:
            continue
        latest_by_other[other_user_id] = {
            "other_user_id": int(other_user_id),
            "last_message_text": row["message"],
            "last_message_created_at": _timestamp_to_iso(row["created_at"]),
            "last_message_user_id": int(row["user_id"]),
            "room": room,
        }

    if not latest_by_other:
        return []

    other_ids = list(latest_by_other.keys())
    placeholders = ", ".join(["?"] * len(other_ids))
    user_rows = _db_query_all(
        f"""
        SELECT id, display_name, avatar_url, email
        FROM users
        WHERE id IN ({placeholders})
        """,
        tuple(other_ids),
    )
    users: dict[int, dict[str, Any]] = {
        int(row["id"]): {
            "display_name": _clean_display_name(row["display_name"] or "", row["email"]),
            "avatar_url": row["avatar_url"],
        }
        for row in user_rows
    }

    threads: list[dict] = []
    for other_user_id, payload in latest_by_other.items():
        user_payload = users.get(int(other_user_id))
        if user_payload is None:
            continue
        threads.append(
            {
                "other_user_id": int(other_user_id),
                "display_name": user_payload["display_name"],
                "avatar_url": user_payload["avatar_url"],
                "last_message_text": payload["last_message_text"],
                "last_message_created_at": payload["last_message_created_at"],
                "last_message_user_id": payload["last_message_user_id"],
                "room": payload["room"],
            }
        )

    threads.sort(key=lambda item: item["last_message_created_at"], reverse=True)
    return threads


def _list_messages_for_room(room: str, after: str | None, limit: int) -> dict:
    safe_room = _normalize_room(room)
    safe_limit = max(1, min(200, int(limit)))
    after_field, after_value = _parse_after(after)

    where = ["cm.room = ?"]
    params: list[int | str] = [safe_room]

    if after_field == "id":
        where.append("cm.id > ?")
        params.append(int(after_value))
    elif after_field == "created_at":
        if DB_BACKEND == "postgres":
            where.append("cm.created_at > to_timestamp(?)")
            params.append(int(after_value))
        else:
            where.append("cm.created_at > ?")
            params.append(int(after_value))

    if after_field is None:
        rows = _db_query_all(
            """
            SELECT cm.id, cm.room, cm.user_id,
                   cm.display_name AS message_display_name,
                   u.display_name AS user_display_name,
                   u.email AS user_email,
                   u.avatar_url AS sender_avatar_url,
                   cm.message, cm.created_at
            FROM (
                SELECT id, room, user_id, display_name, message, created_at
                FROM chat_messages
                WHERE room = ?
                ORDER BY id DESC
                LIMIT ?
            ) cm
            LEFT JOIN users u ON u.id = cm.user_id
            ORDER BY cm.id ASC
            """,
            (safe_room, safe_limit),
        )
    else:
        rows = _db_query_all(
            f"""
            SELECT cm.id, cm.room, cm.user_id,
                   cm.display_name AS message_display_name,
                   u.display_name AS user_display_name,
                   u.email AS user_email,
                   u.avatar_url AS sender_avatar_url,
                   cm.message, cm.created_at
            FROM chat_messages cm
            LEFT JOIN users u ON u.id = cm.user_id
            WHERE {' AND '.join(where)}
            ORDER BY cm.id ASC
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
        "sender_display_name": display_name,
        "sender_avatar_url": user["avatar_url"] if "avatar_url" in user.keys() else None,
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


@router.get("/dm/users", response_model=DMUsersResponse)
def list_dm_users(q: str | None = None, user=Depends(require_user)):
    return {"users": _list_dm_users(int(user["id"]), q)}


@router.get("/dm/threads", response_model=DMThreadsResponse)
def list_dm_threads(user=Depends(require_user)):
    return {"threads": _list_dm_threads(int(user["id"]))}


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


@router.get("/private/threads", response_model=PrivateChatThreadsResponse)
def list_private_threads(user=Depends(require_user)):
    return {"threads": _list_private_threads(int(user["id"]))}


@router.get("/private/{other_user_id}", response_model=PrivateChatMessagesResponse)
def list_private_messages(other_user_id: int, since_id: int | None = None, user=Depends(require_user)):
    my_user_id = int(user["id"])
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))

    messages = _fetch_private_conversation(my_user_id, int(other_user_id), since_id)
    _mark_private_read(my_user_id, int(other_user_id))

    return {"other_user_id": int(other_user_id), "messages": messages}


@router.post("/private/{other_user_id}", response_model=PrivateChatMessageOut)
def create_private_message(other_user_id: int, payload: PrivateChatSendIn, user=Depends(require_user)):
    my_user_id = int(user["id"])
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    return _create_private_message(my_user_id, int(other_user_id), payload.text)
