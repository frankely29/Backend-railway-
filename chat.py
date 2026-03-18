from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from core import (
    DB_BACKEND,
    _chat_expired_where_clause,
    _chat_not_expired_where_clause,
    _db,
    _db_exec,
    _db_lock,
    _db_query_all,
    _retention_deadline_value,
    _sql,
    require_user,
)

router = APIRouter(prefix="/chat", tags=["chat"])

_RATE_LIMIT_SECONDS = 2.0
_rate_limit_lock = threading.Lock()
_last_message_by_user: dict[int, float] = {}

_VOICE_MAX_DURATION_SEC = 10
_VOICE_MAX_FILE_BYTES = 2 * 1024 * 1024
_ALLOWED_AUDIO_MIME_TYPES = {
    "audio/aac",
    "audio/m4a",
    "audio/mp4",
    "audio/mpeg",
    "audio/ogg",
    "audio/wav",
    "audio/webm",
    "audio/x-m4a",
    "audio/x-wav",
}
_VOICE_ROOT = Path("/data/chat_voice")
_PURGE_DEBOUNCE_SECONDS = 600
_purge_lock = threading.Lock()
_last_purge_at = 0.0

class ChatMessagePayload(BaseModel):
    text: str


class PrivateChatSendIn(BaseModel):
    text: str


class PrivateChatMessageOut(BaseModel):
    id: int
    user_id: int
    sender_user_id: int
    recipient_user_id: int
    text: str
    created_at: str
    display_name: str
    sender_display_name: str
    sender_avatar_url: str | None = None
    message_kind: str = "text"
    voice_url: str | None = None
    voice_duration_sec: int | None = None
    expires_at: str | None = None


class PrivateChatMessagesResponse(BaseModel):
    other_user_id: int
    messages: list[PrivateChatMessageOut]


class PrivateChatThreadOut(BaseModel):
    other_user_id: int
    other_display_name: str
    display_name: str
    other_avatar_url: str | None = None
    avatar_url: str | None = None
    last_message_text: str
    last_message_at: str
    last_message_created_at: str
    last_message_sender_user_id: int
    last_message_user_id: int
    unread_count: int
    message_kind: str = "text"
    voice_duration_sec: int | None = None
    room: str | None = None


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


def _to_iso(unix_ts: int) -> str:
    return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).isoformat()


def _timestamp_to_iso(value: Any) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(int(value), tz=timezone.utc)
        else:
            text = str(value).strip().replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(text)
            except ValueError:
                dt = datetime.fromtimestamp(int(float(text)), tz=timezone.utc)
    except Exception:
        dt = datetime.fromtimestamp(int(time.time()), tz=timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _unix_from_timestamp(value: Any) -> int:
    if value is None:
        return int(time.time())
    try:
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return int(float(text))
    except Exception:
        return int(time.time())


def _normalize_room(room: str) -> str:
    cleaned = (room or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="Room is required")
    if cleaned.lower().startswith("dm:"):
        raise HTTPException(status_code=400, detail="Legacy DM rooms are not supported on public chat routes")
    return cleaned


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


def _is_placeholder_sender_name(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"", "driver"}


def _resolve_sender_display_name(row: dict, prefix: str = "") -> str:
    sender_raw = row.get(f"{prefix}sender_user_id") if f"{prefix}sender_user_id" in row else row.get("user_id")
    try:
        sender_id = int(sender_raw)
    except Exception:
        sender_id = 0

    user_display_name = (row.get(f"{prefix}sender_display_name") or row.get("user_display_name") or "").strip()
    user_email = (row.get(f"{prefix}sender_email") or row.get("user_email") or "").strip()
    message_display_name = (row.get("message_display_name") or row.get("display_name") or "").strip()

    if not _is_placeholder_sender_name(user_display_name):
        return user_display_name
    if user_email and "@" in user_email:
        return user_email.split("@", 1)[0].strip() or f"User {sender_id or '?'}"
    if not _is_placeholder_sender_name(message_display_name):
        return message_display_name
    return f"User {sender_id}" if sender_id > 0 else "User"


def _expires_iso(row: dict) -> str | None:
    expires_value = row.get("expires_at")
    if expires_value is None:
        return None
    return _to_iso(_unix_from_timestamp(expires_value))


def _voice_url_if_available(scope: str, row: dict) -> str | None:
    if (row.get("message_kind") or "text").strip().lower() != "voice":
        return None
    voice_path = row.get("voice_path")
    if not voice_path:
        return None
    path = Path(str(voice_path))
    if not path.exists() or not path.is_file():
        return None
    return f"/chat/voice/{scope}/{int(row['id'])}"


def _serialize_public_message(row: dict) -> dict:
    sender_name = _resolve_sender_display_name(row)
    message_kind = (row.get("message_kind") or "text").strip().lower()
    voice_url = _voice_url_if_available("public", row)
    return {
        "id": int(row["id"]),
        "room": row["room"],
        "user_id": int(row["user_id"]),
        "display_name": sender_name,
        "sender_display_name": sender_name,
        "sender_avatar_url": row.get("sender_avatar_url"),
        "text": row.get("message") or "",
        "created_at": _to_iso(_unix_from_timestamp(row["created_at"])),
        "message_kind": message_kind,
        "voice_url": voice_url,
        "voice_duration_sec": int(row["voice_duration_sec"]) if row.get("voice_duration_sec") is not None else None,
        "expires_at": _expires_iso(row),
    }


def _serialize_private_message(row: dict) -> dict:
    sender_name = _resolve_sender_display_name(row, prefix="private_")
    message_kind = (row.get("message_kind") or "text").strip().lower()
    voice_url = _voice_url_if_available("private", row)
    return {
        "id": int(row["id"]),
        "user_id": int(row["sender_user_id"]),
        "sender_user_id": int(row["sender_user_id"]),
        "recipient_user_id": int(row["recipient_user_id"]),
        "text": row.get("text") or "",
        "created_at": _timestamp_to_iso(row["created_at"]),
        "display_name": sender_name,
        "sender_display_name": sender_name,
        "sender_avatar_url": row.get("private_sender_avatar_url"),
        "message_kind": message_kind,
        "voice_url": voice_url,
        "voice_duration_sec": int(row["voice_duration_sec"]) if row.get("voice_duration_sec") is not None else None,
        "expires_at": _expires_iso(row),
    }


def _ensure_dm_target_exists(other_user_id: int):
    row = _db_query_all("SELECT id, is_disabled, is_suspended FROM users WHERE id=? LIMIT 1", (int(other_user_id),))
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    target = dict(row[0])
    is_disabled = bool(target["is_disabled"]) if DB_BACKEND == "postgres" else int(target["is_disabled"] or 0) == 1
    is_suspended = bool(target.get("is_suspended")) if DB_BACKEND == "postgres" else int(target.get("is_suspended") or 0) == 1
    if is_disabled or is_suspended:
        raise HTTPException(status_code=404, detail="User not found")


def _voice_extension(mime: str) -> str:
    mapping = {
        "audio/mpeg": ".mp3",
        "audio/mp4": ".m4a",
        "audio/m4a": ".m4a",
        "audio/x-m4a": ".m4a",
        "audio/aac": ".aac",
        "audio/ogg": ".ogg",
        "audio/wav": ".wav",
        "audio/x-wav": ".wav",
        "audio/webm": ".webm",
    }
    return mapping.get(mime.lower(), ".audio")


def _save_voice_file(scope: str, data: bytes, mime: str) -> str:
    folder = _VOICE_ROOT / scope
    folder.mkdir(parents=True, exist_ok=True)
    ext = _voice_extension(mime)
    filename = f"{int(time.time())}_{uuid.uuid4().hex}{ext}"
    path = folder / filename
    path.write_bytes(data)
    return str(path)


def _validate_voice_upload(upload: UploadFile, duration_sec: int) -> bytes:
    if duration_sec < 1 or duration_sec > _VOICE_MAX_DURATION_SEC:
        raise HTTPException(status_code=400, detail="duration_sec must be between 1 and 10")
    mime = (upload.content_type or "").lower().strip()
    if mime not in _ALLOWED_AUDIO_MIME_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported audio type")
    data = upload.file.read(_VOICE_MAX_FILE_BYTES + 1)
    if not data:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(data) > _VOICE_MAX_FILE_BYTES:
        raise HTTPException(status_code=400, detail="Voice file too large")
    return data


def _touch_chat_maintenance() -> None:
    purge_expired_chat_rows_if_due(force=False)


def purge_expired_chat_rows_if_due(force: bool = False) -> None:
    global _last_purge_at
    now_mono = time.monotonic()
    with _purge_lock:
        if not force and (now_mono - _last_purge_at) < _PURGE_DEBOUNCE_SECONDS:
            return
        _last_purge_at = now_mono

    public_clause, public_params = _chat_expired_where_clause("chat_messages")
    private_clause, private_params = _chat_expired_where_clause("private_chat_messages")

    expired_public = _db_query_all(
        f"SELECT id, voice_path FROM chat_messages WHERE {public_clause}",
        public_params,
    )
    expired_private = _db_query_all(
        f"SELECT id, voice_path FROM private_chat_messages WHERE {private_clause}",
        private_params,
    )

    _db_exec(f"DELETE FROM chat_messages WHERE {public_clause}", public_params)
    _db_exec(f"DELETE FROM private_chat_messages WHERE {private_clause}", private_params)

    for row in [*expired_public, *expired_private]:
        payload = dict(row)
        voice_path = payload.get("voice_path")
        if not voice_path:
            continue
        try:
            p = Path(str(voice_path))
            if p.exists() and p.is_file():
                p.unlink()
        except Exception:
            continue


def _list_messages_for_room(room: str, limit: int, after: str | None = None) -> dict:
    _touch_chat_maintenance()
    safe_room = _normalize_room(room)
    safe_limit = max(1, min(200, int(limit)))

    expires_clause, expires_params = _chat_not_expired_where_clause("chat_messages")
    where = ["room = ?", expires_clause]
    params: list[Any] = [safe_room, *expires_params]
    after_value = (after or "").strip()
    if after_value.isdigit():
        where.append("id > ?")
        params.append(int(after_value))

    rows = _db_query_all(
        f"""
        SELECT cm.id, cm.room, cm.user_id,
               cm.display_name AS message_display_name,
               u.display_name AS user_display_name,
               u.email AS user_email,
               u.avatar_url AS sender_avatar_url,
               cm.message, cm.created_at, cm.message_kind,
               cm.voice_path, cm.voice_duration_sec, cm.expires_at
        FROM (
            SELECT id, room, user_id, display_name, message, created_at, message_kind, voice_path, voice_duration_sec, expires_at
            FROM chat_messages
            WHERE {' AND '.join(where)}
            ORDER BY id DESC
            LIMIT ?
        ) cm
        LEFT JOIN users u ON u.id = cm.user_id
        ORDER BY cm.created_at ASC, cm.id ASC
        """,
        tuple(params + [safe_limit]),
    )

    return {"room": safe_room, "messages": [_serialize_public_message(dict(r)) for r in rows]}


def _create_public_text_message(room: str, payload: ChatMessagePayload, user) -> dict:
    _touch_chat_maintenance()
    safe_room = _normalize_room(room)
    message = _validate_text(payload.text)
    user_id = int(user["id"])
    _enforce_rate_limit(user_id)

    now = int(time.time())
    expires_at = _retention_deadline_value("chat_messages", "text")
    display_name = _resolve_sender_display_name({"user_id": user_id, "user_display_name": user.get("display_name"), "user_email": user.get("email")})

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            if DB_BACKEND == "postgres":
                cur.execute(
                    _sql(
                        """
                        INSERT INTO chat_messages(room, user_id, display_name, message, created_at, message_kind, expires_at)
                        VALUES (?, ?, ?, ?, ?, 'text', ?)
                        RETURNING id, room, user_id, display_name AS message_display_name, message, created_at,
                                  message_kind, voice_path, voice_duration_sec, expires_at
                        """
                    ),
                    (safe_room, user_id, display_name, message, now, expires_at),
                )
                row = dict(cur.fetchone())
            else:
                cur.execute(
                    _sql(
                        """
                        INSERT INTO chat_messages(room, user_id, display_name, message, created_at, message_kind, expires_at)
                        VALUES (?, ?, ?, ?, ?, 'text', ?)
                        """
                    ),
                    (safe_room, user_id, display_name, message, now, expires_at),
                )
                new_id = int(cur.lastrowid)
                cur.execute(
                    _sql(
                        """
                        SELECT id, room, user_id, display_name AS message_display_name, message, created_at,
                               message_kind, voice_path, voice_duration_sec, expires_at
                        FROM chat_messages
                        WHERE id=?
                        """
                    ),
                    (new_id,),
                )
                row = dict(cur.fetchone())
            conn.commit()
        finally:
            conn.close()

    row["user_display_name"] = user.get("display_name")
    row["user_email"] = user.get("email")
    row["sender_avatar_url"] = user.get("avatar_url")
    return _serialize_public_message(row)


def _create_public_voice_message(room: str, user, upload: UploadFile, duration_sec: int) -> dict:
    _touch_chat_maintenance()
    safe_room = _normalize_room(room)
    user_id = int(user["id"])
    _enforce_rate_limit(user_id)

    data = _validate_voice_upload(upload, duration_sec)
    voice_path = _save_voice_file("public", data, upload.content_type or "audio/webm")

    now = int(time.time())
    expires_at = _retention_deadline_value("chat_messages", "voice")
    display_name = _resolve_sender_display_name({"user_id": user_id, "user_display_name": user.get("display_name"), "user_email": user.get("email")})

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            if DB_BACKEND == "postgres":
                cur.execute(
                    _sql(
                        """
                        INSERT INTO chat_messages(
                            room, user_id, display_name, message, created_at, message_kind,
                            voice_path, voice_mime, voice_duration_sec, expires_at
                        )
                        VALUES (?, ?, ?, '', ?, 'voice', ?, ?, ?, ?)
                        RETURNING id, room, user_id, display_name AS message_display_name, message, created_at,
                                  message_kind, voice_path, voice_duration_sec, expires_at
                        """
                    ),
                    (safe_room, user_id, display_name, now, voice_path, upload.content_type, int(duration_sec), expires_at),
                )
                row = dict(cur.fetchone())
            else:
                cur.execute(
                    _sql(
                        """
                        INSERT INTO chat_messages(
                            room, user_id, display_name, message, created_at, message_kind,
                            voice_path, voice_mime, voice_duration_sec, expires_at
                        )
                        VALUES (?, ?, ?, '', ?, 'voice', ?, ?, ?, ?)
                        """
                    ),
                    (safe_room, user_id, display_name, now, voice_path, upload.content_type, int(duration_sec), expires_at),
                )
                new_id = int(cur.lastrowid)
                cur.execute(
                    _sql(
                        """
                        SELECT id, room, user_id, display_name AS message_display_name, message, created_at,
                               message_kind, voice_path, voice_duration_sec, expires_at
                        FROM chat_messages
                        WHERE id=?
                        """
                    ),
                    (new_id,),
                )
                row = dict(cur.fetchone())
            conn.commit()
        except Exception:
            try:
                Path(voice_path).unlink(missing_ok=True)
            except Exception:
                pass
            raise
        finally:
            conn.close()

    row["user_display_name"] = user.get("display_name")
    row["user_email"] = user.get("email")
    row["sender_avatar_url"] = user.get("avatar_url")
    return _serialize_public_message(row)


def _fetch_private_conversation(me: int, other: int, since_id: int | None) -> list[dict]:
    _touch_chat_maintenance()
    where = [
        "((pcm.sender_user_id=? AND pcm.recipient_user_id=?) OR (pcm.sender_user_id=? AND pcm.recipient_user_id=?))",
    ]
    params: list[Any] = [int(me), int(other), int(other), int(me)]
    expires_clause, expires_params = _chat_not_expired_where_clause("private_chat_messages", "pcm.expires_at")
    where.append(expires_clause)
    params.extend(expires_params)
    if since_id is not None:
        where.append("pcm.id > ?")
        params.append(int(since_id))

    rows = _db_query_all(
        f"""
        SELECT pcm.id, pcm.sender_user_id, pcm.recipient_user_id, pcm.text, pcm.created_at,
               pcm.message_kind, pcm.voice_path, pcm.voice_mime, pcm.voice_duration_sec, pcm.expires_at,
               u.display_name AS private_sender_display_name,
               u.email AS private_sender_email,
               u.avatar_url AS private_sender_avatar_url
        FROM private_chat_messages pcm
        LEFT JOIN users u ON u.id = pcm.sender_user_id
        WHERE {' AND '.join(where)}
        ORDER BY pcm.created_at ASC, pcm.id ASC
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
    _touch_chat_maintenance()
    expires_clause, expires_params = _chat_not_expired_where_clause("private_chat_messages")
    message_rows = _db_query_all(
        f"""
        SELECT id, sender_user_id, recipient_user_id, text, created_at, message_kind, voice_duration_sec
        FROM private_chat_messages
        WHERE (sender_user_id=? OR recipient_user_id=?)
          AND {expires_clause}
        ORDER BY created_at DESC, id DESC
        """,
        tuple([int(me), int(me), *expires_params]),
    )
    if not message_rows:
        return []

    latest_by_other: dict[int, dict[str, Any]] = {}
    for row in message_rows:
        sender_user_id = int(row["sender_user_id"])
        recipient_user_id = int(row["recipient_user_id"])
        other_user_id = recipient_user_id if sender_user_id == int(me) else sender_user_id
        if other_user_id == int(me) or other_user_id in latest_by_other:
            continue
        latest_by_other[other_user_id] = dict(row)

    if not latest_by_other:
        return []

    other_ids = list(latest_by_other.keys())
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
          AND {expires_clause}
          AND sender_user_id IN ({placeholders})
        GROUP BY sender_user_id
        """,
        tuple([int(me), *expires_params] + other_ids),
    )
    unread_by_other = {int(row["other_user_id"]): int(row["unread_count"]) for row in unread_rows}

    threads: list[dict] = []
    for other_user_id, last_message in latest_by_other.items():
        user_payload = users.get(other_user_id)
        if user_payload is None:
            continue
        message_kind = (last_message.get("message_kind") or "text").strip().lower()
        last_text = (last_message.get("text") or "").strip()
        if message_kind == "voice" and not last_text:
            last_text = "[Voice message]"
        threads.append(
            {
                "other_user_id": other_user_id,
                "other_display_name": user_payload["display_name"],
                "display_name": user_payload["display_name"],
                "other_avatar_url": user_payload["avatar_url"],
                "avatar_url": user_payload["avatar_url"],
                "last_message_text": last_text,
                "last_message_at": _timestamp_to_iso(last_message["created_at"]),
                "last_message_created_at": _timestamp_to_iso(last_message["created_at"]),
                "last_message_sender_user_id": int(last_message["sender_user_id"]),
                "last_message_user_id": int(last_message["sender_user_id"]),
                "unread_count": int(unread_by_other.get(other_user_id, 0)),
                "message_kind": message_kind,
                "voice_duration_sec": int(last_message["voice_duration_sec"]) if last_message.get("voice_duration_sec") is not None else None,
                "room": f"dm:{min(int(me), other_user_id)}:{max(int(me), other_user_id)}",
            }
        )

    threads.sort(key=lambda item: item["last_message_at"], reverse=True)
    return threads


def _create_private_message(sender_user_id: int, recipient_user_id: int, text: str) -> dict:
    _touch_chat_maintenance()
    clean_text = _validate_text(text)
    _enforce_rate_limit(int(sender_user_id))
    now = int(time.time())
    expires_at = _retention_deadline_value("private_chat_messages", "text")

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            if DB_BACKEND == "postgres":
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(
                            sender_user_id, recipient_user_id, text, created_at,
                            message_kind, expires_at
                        )
                        VALUES (?, ?, ?, NOW(), 'text', ?)
                        RETURNING id, sender_user_id, recipient_user_id, text, created_at,
                                  message_kind, voice_path, voice_mime, voice_duration_sec, expires_at
                        """
                    ),
                    (int(sender_user_id), int(recipient_user_id), clean_text, expires_at),
                )
                row = dict(cur.fetchone())
            else:
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(
                            sender_user_id, recipient_user_id, text, created_at,
                            message_kind, expires_at
                        )
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'text', ?)
                        """
                    ),
                    (int(sender_user_id), int(recipient_user_id), clean_text, expires_at),
                )
                new_id = int(cur.lastrowid)
                cur.execute(
                    _sql(
                        """
                        SELECT id, sender_user_id, recipient_user_id, text, created_at,
                               message_kind, voice_path, voice_mime, voice_duration_sec, expires_at
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

    sender_row = _db_query_all("SELECT display_name, email, avatar_url FROM users WHERE id=? LIMIT 1", (int(sender_user_id),))
    if sender_row:
        row["private_sender_display_name"] = sender_row[0]["display_name"]
        row["private_sender_email"] = sender_row[0]["email"]
        row["private_sender_avatar_url"] = sender_row[0]["avatar_url"]
    return _serialize_private_message(row)


def _create_private_voice_message(sender_user_id: int, recipient_user_id: int, upload: UploadFile, duration_sec: int) -> dict:
    _touch_chat_maintenance()
    _enforce_rate_limit(int(sender_user_id))
    data = _validate_voice_upload(upload, duration_sec)
    voice_path = _save_voice_file("private", data, upload.content_type or "audio/webm")
    now = int(time.time())
    expires_at = _retention_deadline_value("private_chat_messages", "voice")

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            if DB_BACKEND == "postgres":
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(
                            sender_user_id, recipient_user_id, text, created_at,
                            message_kind, voice_path, voice_mime, voice_duration_sec, expires_at
                        )
                        VALUES (?, ?, '', NOW(), 'voice', ?, ?, ?, ?)
                        RETURNING id, sender_user_id, recipient_user_id, text, created_at,
                                  message_kind, voice_path, voice_mime, voice_duration_sec, expires_at
                        """
                    ),
                    (int(sender_user_id), int(recipient_user_id), voice_path, upload.content_type, int(duration_sec), expires_at),
                )
                row = dict(cur.fetchone())
            else:
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(
                            sender_user_id, recipient_user_id, text, created_at,
                            message_kind, voice_path, voice_mime, voice_duration_sec, expires_at
                        )
                        VALUES (?, ?, '', CURRENT_TIMESTAMP, 'voice', ?, ?, ?, ?)
                        """
                    ),
                    (int(sender_user_id), int(recipient_user_id), voice_path, upload.content_type, int(duration_sec), expires_at),
                )
                new_id = int(cur.lastrowid)
                cur.execute(
                    _sql(
                        """
                        SELECT id, sender_user_id, recipient_user_id, text, created_at,
                               message_kind, voice_path, voice_mime, voice_duration_sec, expires_at
                        FROM private_chat_messages
                        WHERE id=?
                        LIMIT 1
                        """
                    ),
                    (new_id,),
                )
                row = dict(cur.fetchone())
            conn.commit()
        except Exception:
            try:
                Path(voice_path).unlink(missing_ok=True)
            except Exception:
                pass
            raise
        finally:
            conn.close()

    sender_row = _db_query_all("SELECT display_name, email, avatar_url FROM users WHERE id=? LIMIT 1", (int(sender_user_id),))
    if sender_row:
        row["private_sender_display_name"] = sender_row[0]["display_name"]
        row["private_sender_email"] = sender_row[0]["email"]
        row["private_sender_avatar_url"] = sender_row[0]["avatar_url"]
    return _serialize_private_message(row)




def _resolve_since_id(since_id: int | None, after: str | None) -> int | None:
    if since_id is not None:
        return int(since_id)
    value = (after or '').strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)
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
        user_id = int(row["id"])
        user_ids.append(user_id)
        users.append(
            {
                "id": user_id,
                "display_name": _clean_display_name(row["display_name"] or "", row["email"]),
                "avatar_url": row["avatar_url"],
                "is_online": False,
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


@router.get("/rooms/{room}")
def list_room_messages(room: str, after: str | None = None, limit: int = 50, user=Depends(require_user)):
    _ = user
    return _list_messages_for_room(room, limit, after=after)


@router.post("/rooms/{room}")
def create_room_message(room: str, payload: ChatMessagePayload, user=Depends(require_user)):
    return _create_public_text_message(room, payload, user)


@router.post("/rooms/{room}/voice")
def create_room_voice_message(
    room: str,
    duration_sec: int = Form(...),
    file: UploadFile = File(...),
    user=Depends(require_user),
):
    return _create_public_voice_message(room, user, file, int(duration_sec))


@router.get("/dm/users", response_model=DMUsersResponse)
def list_dm_users(q: str | None = None, user=Depends(require_user)):
    _touch_chat_maintenance()
    return {"users": _list_dm_users(int(user["id"]), q)}


@router.get("/private/users", response_model=DMUsersResponse)
def list_private_users(q: str | None = None, user=Depends(require_user)):
    _touch_chat_maintenance()
    return {"users": _list_dm_users(int(user["id"]), q)}


@router.get("/private/threads", response_model=PrivateChatThreadsResponse)
def list_private_threads(user=Depends(require_user)):
    return {"threads": _list_private_threads(int(user["id"]))}


@router.get("/dm/threads", response_model=PrivateChatThreadsResponse)
def list_dm_threads(user=Depends(require_user)):
    return {"threads": _list_private_threads(int(user["id"]))}


@router.get("/private/{other_user_id}", response_model=PrivateChatMessagesResponse)
def list_private_messages(other_user_id: int, since_id: int | None = None, after: str | None = None, user=Depends(require_user)):
    my_user_id = int(user["id"])
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))

    messages = _fetch_private_conversation(my_user_id, int(other_user_id), _resolve_since_id(since_id, after))
    _mark_private_read(my_user_id, int(other_user_id))

    return {"other_user_id": int(other_user_id), "messages": messages}


@router.get("/dm/{other_user_id}", response_model=PrivateChatMessagesResponse)
def list_dm_messages(other_user_id: int, since_id: int | None = None, after: str | None = None, user=Depends(require_user)):
    return list_private_messages(other_user_id=other_user_id, since_id=since_id, after=after, user=user)


@router.post("/private/{other_user_id}", response_model=PrivateChatMessageOut)
def create_private_message(other_user_id: int, payload: PrivateChatSendIn, user=Depends(require_user)):
    my_user_id = int(user["id"])
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    return _create_private_message(my_user_id, int(other_user_id), payload.text)


@router.post("/dm/{other_user_id}", response_model=PrivateChatMessageOut)
def create_dm_message(other_user_id: int, payload: PrivateChatSendIn, user=Depends(require_user)):
    return create_private_message(other_user_id=other_user_id, payload=payload, user=user)


@router.post("/private/{other_user_id}/voice", response_model=PrivateChatMessageOut)
def create_private_voice_message(
    other_user_id: int,
    duration_sec: int = Form(...),
    file: UploadFile = File(...),
    user=Depends(require_user),
):
    my_user_id = int(user["id"])
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    return _create_private_voice_message(my_user_id, int(other_user_id), file, int(duration_sec))


@router.post("/dm/{other_user_id}/voice", response_model=PrivateChatMessageOut)
def create_dm_voice_message(
    other_user_id: int,
    duration_sec: int = Form(...),
    file: UploadFile = File(...),
    user=Depends(require_user),
):
    return create_private_voice_message(other_user_id=other_user_id, duration_sec=duration_sec, file=file, user=user)


@router.get("/voice/public/{message_id}")
def get_public_voice_file(message_id: int, user=Depends(require_user)):
    _ = user
    _touch_chat_maintenance()
    expires_clause, expires_params = _chat_not_expired_where_clause("chat_messages")
    row = _db_query_all(
        f"""
        SELECT voice_path, voice_mime
        FROM chat_messages
        WHERE id=?
          AND room NOT LIKE 'dm:%'
          AND message_kind='voice'
          AND {expires_clause}
        LIMIT 1
        """,
        tuple([int(message_id), *expires_params]),
    )
    if not row or not row[0]["voice_path"]:
        raise HTTPException(status_code=404, detail="Voice message not found")
    path = Path(str(row[0]["voice_path"]))
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Voice message file missing")
    return FileResponse(path, media_type=row[0]["voice_mime"] or "application/octet-stream")


@router.get("/voice/private/{message_id}")
def get_private_voice_file(message_id: int, user=Depends(require_user)):
    my_user_id = int(user["id"])
    _touch_chat_maintenance()
    expires_clause, expires_params = _chat_not_expired_where_clause("private_chat_messages")
    row = _db_query_all(
        f"""
        SELECT sender_user_id, recipient_user_id, voice_path, voice_mime
        FROM private_chat_messages
        WHERE id=?
          AND message_kind='voice'
          AND {expires_clause}
        LIMIT 1
        """,
        tuple([int(message_id), *expires_params]),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Voice message not found")
    payload = row[0]
    if my_user_id not in {int(payload["sender_user_id"]), int(payload["recipient_user_id"])}:
        raise HTTPException(status_code=403, detail="Not allowed")
    path = Path(str(payload["voice_path"] or ""))
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Voice message file missing")
    return FileResponse(path, media_type=payload["voice_mime"] or "application/octet-stream")
