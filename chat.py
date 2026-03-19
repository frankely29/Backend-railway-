from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from email.utils import formatdate
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, Response, UploadFile
from pydantic import BaseModel

from core import (
    DATA_DIR,
    DB_BACKEND,
    _clean_display_name,
    _db,
    _db_exec,
    _db_lock,
    _db_query_all,
    _sql,
    _user_block_state,
    require_user,
)

router = APIRouter(prefix="/chat", tags=["chat"])

_RATE_LIMIT_SECONDS = 2.0
_rate_limit_lock = threading.Lock()
_last_message_by_user: dict[int, float] = {}

_CHAT_AUDIO_DIR = DATA_DIR / "chat_audio"
_MAX_AUDIO_BYTES = int(os.environ.get("CHAT_AUDIO_MAX_BYTES", str(6 * 1024 * 1024)))
_MAX_PRIVATE_PAGE_SIZE = 200
CHAT_VOICE_MAX_MS = 60_000
CHAT_RETENTION_SECONDS = 24 * 60 * 60
CHAT_RETENTION_SWEEP_SECONDS = 15 * 60
_ALLOWED_AUDIO_MIME_TYPES = {
    "audio/mpeg": ".mp3",
    "audio/mp4": ".mp4",
    "audio/ogg": ".ogg",
    "audio/webm": ".webm",
}

_LOGGER = logging.getLogger(__name__)
_VOICE_NOTE_FALLBACK_TEXT = "Voice note"
_retention_state_lock = threading.Lock()
_retention_purge_lock = threading.Lock()
_retention_last_run_monotonic = 0.0
_retention_sweeper_started = False


class ChatMessagePayload(BaseModel):
    text: str


class PrivateChatSendIn(BaseModel):
    text: str


class PrivateChatMessageOut(BaseModel):
    id: int
    sender_user_id: int
    recipient_user_id: int
    text: str
    message_type: str
    created_at: str
    audio_url: str | None = None
    audio_duration_ms: int | None = None
    audio_mime_type: str | None = None


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
    preview_text: str
    last_message_at: str
    last_created_at: str
    last_message_sender_user_id: int
    unread_count: int


class PrivateChatThreadsResponse(BaseModel):
    threads: list[PrivateChatThreadOut]


class _AfterFilter(BaseModel):
    field: str | None
    value: int | None


def _to_iso(unix_ts: int) -> str:
    return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).isoformat()


def _parse_after(after: str | None) -> _AfterFilter:
    if after is None:
        return _AfterFilter(field=None, value=None)

    value = after.strip()
    if not value:
        return _AfterFilter(field=None, value=None)

    if value.isdigit():
        return _AfterFilter(field="id", value=int(value))

    iso_value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid after. Use numeric id or ISO timestamp.") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return _AfterFilter(field="created_at", value=int(dt.timestamp()))


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


def _room_slug(room: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", _normalize_room(room)).strip("._-") or "room"


def _dm_room_for_users(a: int, b: int) -> str:
    low, high = sorted((int(a), int(b)))
    return f"dm:{low}:{high}"


def _private_audio_subdir(a: int, b: int) -> str:
    low, high = sorted((int(a), int(b)))
    return f"private/{low}_{high}"


def _validate_text(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="text cannot be empty")
    if len(cleaned) > 600:
        raise HTTPException(status_code=400, detail="text too long (max 600)")
    return cleaned


def _voice_note_text_fallback(text: str | None) -> str:
    cleaned = (text or "").strip()
    if len(cleaned) > 600:
        raise HTTPException(status_code=400, detail="text too long (max 600)")
    return cleaned or _VOICE_NOTE_FALLBACK_TEXT


def _validate_duration_ms(duration_ms: int | None) -> int | None:
    if duration_ms is None:
        return None
    value = int(duration_ms)
    if value < 0:
        return 0
    return min(value, CHAT_VOICE_MAX_MS)


def _retention_cutoff_unix() -> int:
    return int(time.time()) - CHAT_RETENTION_SECONDS


def _public_expired_created_at_clause() -> str:
    if DB_BACKEND == "postgres":
        return "created_at < to_timestamp(?)"
    return "CAST(created_at AS INTEGER) < ?"


def _private_expired_created_at_clause() -> str:
    if DB_BACKEND == "postgres":
        return "created_at < to_timestamp(?)"
    return "CAST(strftime('%s', created_at) AS INTEGER) < ?"


def _safe_unlink_chat_audio(relative_path: str | None) -> None:
    if not relative_path:
        return
    try:
        target = _resolve_audio_path(str(relative_path))
    except HTTPException:
        _LOGGER.warning("Skipping unsafe chat audio purge path", extra={"audio_path": relative_path})
        return

    try:
        target.unlink(missing_ok=True)
    except OSError:
        _LOGGER.warning("Failed to unlink expired chat audio", exc_info=True, extra={"audio_path": relative_path})
        return

    allowed_cleanup_roots = {
        (_CHAT_AUDIO_DIR / "public").resolve(),
        (_CHAT_AUDIO_DIR / "private").resolve(),
    }
    for parent in target.parents:
        resolved_parent = parent.resolve()
        if resolved_parent == _CHAT_AUDIO_DIR.resolve():
            break
        if resolved_parent not in allowed_cleanup_roots and not any(
            root == resolved_parent or root in resolved_parent.parents for root in allowed_cleanup_roots
        ):
            break
        try:
            parent.rmdir()
        except OSError:
            break


def _collect_expired_chat_audio_rows(cutoff_unix: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    public_rows = _db_query_all(
        f"""
        SELECT id, audio_path
        FROM chat_messages
        WHERE message_type='voice'
          AND audio_path IS NOT NULL
          AND {_public_expired_created_at_clause()}
        """,
        (int(cutoff_unix),),
    )
    private_rows = _db_query_all(
        f"""
        SELECT id, audio_path
        FROM private_chat_messages
        WHERE message_type='voice'
          AND audio_path IS NOT NULL
          AND {_private_expired_created_at_clause()}
        """,
        (int(cutoff_unix),),
    )
    rows.extend({"scope": "public", "id": int(row["id"]), "audio_path": row["audio_path"]} for row in public_rows)
    rows.extend({"scope": "private", "id": int(row["id"]), "audio_path": row["audio_path"]} for row in private_rows)
    return rows


def _delete_expired_public_messages(cutoff_unix: int) -> int:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute(
                _sql(
                    f"""
                    DELETE FROM chat_messages
                    WHERE {_public_expired_created_at_clause()}
                    """
                ),
                (int(cutoff_unix),),
            )
            deleted = int(cur.rowcount or 0)
            conn.commit()
            return deleted
        finally:
            conn.close()


def _delete_expired_private_messages(cutoff_unix: int) -> int:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute(
                _sql(
                    f"""
                    DELETE FROM private_chat_messages
                    WHERE {_private_expired_created_at_clause()}
                    """
                ),
                (int(cutoff_unix),),
            )
            deleted = int(cur.rowcount or 0)
            conn.commit()
            return deleted
        finally:
            conn.close()


def _purge_expired_chat_data(force: bool = False) -> bool:
    global _retention_last_run_monotonic

    now_monotonic = time.monotonic()
    with _retention_state_lock:
        if not force and (now_monotonic - _retention_last_run_monotonic) < CHAT_RETENTION_SWEEP_SECONDS:
            return False

    if not _retention_purge_lock.acquire(blocking=False):
        return False

    try:
        cutoff_unix = _retention_cutoff_unix()
        expired_audio_rows = _collect_expired_chat_audio_rows(cutoff_unix)
        seen_paths: set[str] = set()
        for row in expired_audio_rows:
            audio_path = str(row.get("audio_path") or "").strip()
            if not audio_path or audio_path in seen_paths:
                continue
            seen_paths.add(audio_path)
            _safe_unlink_chat_audio(audio_path)

        public_deleted = _delete_expired_public_messages(cutoff_unix)
        private_deleted = _delete_expired_private_messages(cutoff_unix)
        with _retention_state_lock:
            _retention_last_run_monotonic = time.monotonic()
        if expired_audio_rows or public_deleted or private_deleted:
            _LOGGER.info(
                "Purged expired chat data",
                extra={
                    "cutoff_unix": cutoff_unix,
                    "expired_audio_files": len(seen_paths),
                    "deleted_public_messages": public_deleted,
                    "deleted_private_messages": private_deleted,
                },
            )
        return True
    except Exception:
        _LOGGER.exception("Failed to purge expired chat data")
        raise
    finally:
        _retention_purge_lock.release()


def maybe_purge_expired_chat_data() -> bool:
    try:
        return _purge_expired_chat_data(force=False)
    except Exception:
        return False


def start_chat_retention_sweeper() -> bool:
    global _retention_sweeper_started

    with _retention_state_lock:
        if _retention_sweeper_started:
            return False
        _retention_sweeper_started = True

    def _worker() -> None:
        while True:
            try:
                _purge_expired_chat_data(force=True)
            except Exception:
                _LOGGER.exception("Chat retention sweeper iteration failed")
            time.sleep(CHAT_RETENTION_SWEEP_SECONDS)

    thread = threading.Thread(
        target=_worker,
        name="chat-retention-sweeper",
        daemon=True,
    )
    thread.start()
    return True


def _enforce_rate_limit(user_id: int) -> None:
    now = time.monotonic()
    with _rate_limit_lock:
        prev = _last_message_by_user.get(user_id)
        if prev is not None and (now - prev) < _RATE_LIMIT_SECONDS:
            raise HTTPException(status_code=429, detail="Too many messages. Slow down.")
        _last_message_by_user[user_id] = now


def _preview_text(text: str | None, message_type: str) -> str:
    cleaned = (text or "").strip()
    if cleaned:
        return cleaned
    if message_type == "voice":
        return "[Voice note]"
    return ""


def _public_audio_url(message_id: int) -> str:
    return f"/chat/audio/public/{int(message_id)}"


def _private_audio_url(message_id: int) -> str:
    return f"/chat/audio/private/{int(message_id)}"


def _voice_fields(audio_url: str, row: dict) -> dict[str, Any]:
    return {
        "audio_url": audio_url,
        "audio_duration_ms": int(row["audio_duration_ms"]) if row.get("audio_duration_ms") is not None else None,
        "audio_mime_type": row.get("audio_mime_type"),
    }


def _serialize_public_message(row: dict) -> dict:
    payload = {
        "id": int(row["id"]),
        "room": row["room"],
        "user_id": int(row["user_id"]),
        "display_name": row["display_name"],
        "text": row["message"] or "",
        "message_type": row.get("message_type") or "text",
        "created_at": _timestamp_to_iso(row["created_at"]),
        "audio_url": None,
        "audio_duration_ms": None,
        "audio_mime_type": None,
    }
    if payload["message_type"] == "voice" and row.get("audio_path"):
        payload.update(_voice_fields(_public_audio_url(payload["id"]), row))
    return payload


def _serialize_private_message(row: dict, include_legacy_aliases: bool = False) -> dict:
    payload = {
        "id": int(row["id"]),
        "sender_user_id": int(row["sender_user_id"]),
        "recipient_user_id": int(row["recipient_user_id"]),
        "text": row.get("text") or "",
        "message_type": row.get("message_type") or "text",
        "created_at": _timestamp_to_iso(row["created_at"]),
        "audio_url": None,
        "audio_duration_ms": None,
        "audio_mime_type": None,
    }
    if payload["message_type"] == "voice" and row.get("audio_path"):
        payload.update(_voice_fields(_private_audio_url(payload["id"]), row))
    if include_legacy_aliases:
        payload["user_id"] = payload["sender_user_id"]
        payload["room"] = _dm_room_for_users(payload["sender_user_id"], payload["recipient_user_id"])
        payload["display_name"] = row.get("sender_display_name")
    return payload


def _ensure_dm_target_exists(other_user_id: int) -> None:
    row = _db_query_all(
        "SELECT id, is_disabled, is_suspended FROM users WHERE id=? LIMIT 1",
        (int(other_user_id),),
    )
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    target = dict(row[0])
    if _user_block_state(target)["is_blocked"]:
        raise HTTPException(status_code=404, detail="User not found")


def _user_directory_payloads(user_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not user_ids:
        return {}
    placeholders = ", ".join(["?"] * len(user_ids))
    rows = _db_query_all(
        f"SELECT id, display_name, avatar_url, email FROM users WHERE id IN ({placeholders})",
        tuple(user_ids),
    )
    return {
        int(row["id"]): {
            "display_name": _clean_display_name(row["display_name"] or "", row["email"]),
            "avatar_url": row["avatar_url"],
        }
        for row in rows
    }


def _fetch_private_conversation(
    me: int,
    other: int,
    since_id: int | None,
    limit: int,
    after_filter: _AfterFilter | None = None,
) -> list[dict]:
    safe_limit = max(1, min(_MAX_PRIVATE_PAGE_SIZE, int(limit)))
    where = [
        "((sender_user_id=? AND recipient_user_id=?) OR (sender_user_id=? AND recipient_user_id=?))"
    ]
    params: list[int] = [int(me), int(other), int(other), int(me)]

    if since_id is not None:
        where.append("id > ?")
        params.append(int(since_id))
    elif after_filter and after_filter.field == "id" and after_filter.value is not None:
        where.append("id > ?")
        params.append(int(after_filter.value))
    elif after_filter and after_filter.field == "created_at" and after_filter.value is not None:
        if DB_BACKEND == "postgres":
            where.append("created_at > to_timestamp(?)")
        else:
            where.append("strftime('%s', created_at) > ?")
        params.append(int(after_filter.value))

    select_sql = """
        SELECT id, sender_user_id, recipient_user_id, text, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
        FROM private_chat_messages
        WHERE {where_clause}
    """
    if since_id is None and not (after_filter and after_filter.field):
        rows = _db_query_all(
            f"""
            SELECT id, sender_user_id, recipient_user_id, text, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
            FROM (
                {select_sql.format(where_clause=' AND '.join(where))}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
            ) recent
            ORDER BY created_at ASC, id ASC
            """,
            tuple(params + [safe_limit]),
        )
    else:
        rows = _db_query_all(
            f"""
            {select_sql.format(where_clause=' AND '.join(where))}
            ORDER BY created_at ASC, id ASC
            LIMIT ?
            """,
            tuple(params + [safe_limit]),
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
    users = _user_directory_payloads(other_ids)
    placeholders = ", ".join(["?"] * len(other_ids))
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
            SELECT id, sender_user_id, recipient_user_id, text, created_at, message_type
            FROM private_chat_messages
            WHERE (sender_user_id=? AND recipient_user_id=?) OR (sender_user_id=? AND recipient_user_id=?)
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (int(me), other_user_id, other_user_id, int(me)),
        )
        if not latest:
            continue
        user_payload = users.get(other_user_id)
        if user_payload is None:
            continue
        last_message = dict(latest[0])
        preview = _preview_text(last_message.get("text"), last_message.get("message_type") or "text")
        created_at = _timestamp_to_iso(last_message["created_at"])
        threads.append(
            {
                "other_user_id": other_user_id,
                "other_display_name": user_payload["display_name"],
                "display_name": user_payload["display_name"],
                "other_avatar_url": user_payload["avatar_url"],
                "avatar_url": user_payload["avatar_url"],
                "last_message_text": preview,
                "preview_text": preview,
                "last_message_at": created_at,
                "last_created_at": created_at,
                "last_message_sender_user_id": int(last_message["sender_user_id"]),
                "unread_count": int(unread_by_other.get(other_user_id, 0)),
            }
        )

    threads.sort(key=lambda item: item["last_message_at"], reverse=True)
    return threads


def _insert_private_message(
    sender_user_id: int,
    recipient_user_id: int,
    text: str,
    message_type: str = "text",
    audio_path: str | None = None,
    audio_mime_type: str | None = None,
    audio_duration_ms: int | None = None,
) -> dict:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            if DB_BACKEND == "postgres":
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(
                            sender_user_id, recipient_user_id, text, message_type, audio_path, audio_mime_type, audio_duration_ms
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        RETURNING id, sender_user_id, recipient_user_id, text, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        """
                    ),
                    (
                        int(sender_user_id),
                        int(recipient_user_id),
                        text,
                        message_type,
                        audio_path,
                        audio_mime_type,
                        audio_duration_ms,
                    ),
                )
                row = dict(cur.fetchone())
            else:
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(
                            sender_user_id, recipient_user_id, text, message_type, audio_path, audio_mime_type, audio_duration_ms
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                    ),
                    (
                        int(sender_user_id),
                        int(recipient_user_id),
                        text,
                        message_type,
                        audio_path,
                        audio_mime_type,
                        audio_duration_ms,
                    ),
                )
                new_id = int(cur.lastrowid)
                cur.execute(
                    _sql(
                        """
                        SELECT id, sender_user_id, recipient_user_id, text, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
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
    return row


def _insert_public_message(
    room: str,
    user,
    text: str,
    message_type: str = "text",
    audio_path: str | None = None,
    audio_mime_type: str | None = None,
    audio_duration_ms: int | None = None,
) -> dict:
    safe_room = _normalize_room(room)
    display_name = _clean_display_name(user["display_name"] or "", user["email"])
    user_id = int(user["id"])
    now = int(time.time())

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            if DB_BACKEND == "postgres":
                cur.execute(
                    _sql(
                        """
                        INSERT INTO chat_messages(
                            room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        )
                        VALUES (?, ?, ?, ?, to_timestamp(?), ?, ?, ?, ?)
                        RETURNING id, room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        """
                    ),
                    (
                        safe_room,
                        user_id,
                        display_name,
                        text,
                        now,
                        message_type,
                        audio_path,
                        audio_mime_type,
                        audio_duration_ms,
                    ),
                )
                row = dict(cur.fetchone())
            else:
                cur.execute(
                    _sql(
                        """
                        INSERT INTO chat_messages(
                            room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                    ),
                    (
                        safe_room,
                        user_id,
                        display_name,
                        text,
                        now,
                        message_type,
                        audio_path,
                        audio_mime_type,
                        audio_duration_ms,
                    ),
                )
                new_id = int(cur.lastrowid)
                cur.execute(
                    _sql(
                        """
                        SELECT id, room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        FROM chat_messages
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
    return row


def _log_voice_upload_failure(request: Request, file: UploadFile | None, audio: UploadFile | None) -> None:
    _LOGGER.warning(
        "Voice upload missing multipart file",
        extra={
            "route": request.url.path,
            "content_type": request.headers.get("content-type"),
            "file_present": file is not None,
            "audio_present": audio is not None,
        },
    )


def _log_voice_file_issue(
    *,
    route: str,
    message_id: int | None,
    content_type: str | None,
    file_exists: bool | None,
    detail: str,
) -> None:
    _LOGGER.warning(
        "Voice note issue: %s",
        detail,
        extra={
            "route": route,
            "message_id": message_id,
            "content_type": content_type,
            "file_exists": file_exists,
        },
    )


def _resolve_voice_upload(request: Request, file: UploadFile | None, audio: UploadFile | None) -> UploadFile:
    upload = file or audio
    if upload is None:
        _log_voice_upload_failure(request, file, audio)
        raise HTTPException(status_code=422, detail="file or audio required")
    return upload


def _read_upload_audio(upload: UploadFile) -> tuple[bytes, str, str]:
    mime_type = (upload.content_type or "").strip().lower()
    if mime_type not in _ALLOWED_AUDIO_MIME_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported audio format")
    data = upload.file.read(_MAX_AUDIO_BYTES + 1)
    if not data:
        raise HTTPException(status_code=400, detail="Audio upload cannot be empty")
    if len(data) > _MAX_AUDIO_BYTES:
        raise HTTPException(status_code=400, detail="Audio upload too large")
    return data, mime_type, _ALLOWED_AUDIO_MIME_TYPES[mime_type]


def _resolve_audio_path(relative_path: str) -> Path:
    target = (_CHAT_AUDIO_DIR / relative_path).resolve()
    base = _CHAT_AUDIO_DIR.resolve()
    if base != target and base not in target.parents:
        raise HTTPException(status_code=400, detail="Invalid audio path")
    return target


def _store_audio_file(relative_dir: str, message_id: int, user_id: int, extension: str, payload: bytes) -> str:
    relative_path = f"{relative_dir}/user-{int(user_id)}-message-{int(message_id)}{extension}"
    target = _resolve_audio_path(relative_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    with tmp.open("wb") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(target)
    try:
        dir_fd = os.open(str(target.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass
    if not target.exists() or not target.is_file():
        raise RuntimeError("Audio file was not persisted")
    return relative_path


def _persist_public_voice_message(room: str, user, upload: UploadFile, duration_ms: int | None, text: str | None) -> dict:
    _enforce_rate_limit(int(user["id"]))
    clean_text = _voice_note_text_fallback(text)
    safe_duration_ms = _validate_duration_ms(duration_ms)
    payload, mime_type, extension = _read_upload_audio(upload)
    relative_dir = f"public/{_room_slug(room)}"
    target: Path | None = None
    conn = None
    row: dict | None = None
    try:
        with _db_lock:
            conn = _db()
            cur = conn.cursor()
            safe_room = _normalize_room(room)
            display_name = _clean_display_name(user["display_name"] or "", user["email"])
            user_id = int(user["id"])
            now = int(time.time())
            if DB_BACKEND == "postgres":
                cur.execute(
                    _sql(
                        """
                        INSERT INTO chat_messages(
                            room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        )
                        VALUES (?, ?, ?, ?, to_timestamp(?), ?, ?, ?, ?)
                        RETURNING id, room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        """
                    ),
                    (safe_room, user_id, display_name, clean_text, now, "voice", None, None, None),
                )
                row = dict(cur.fetchone())
            else:
                cur.execute(
                    _sql(
                        """
                        INSERT INTO chat_messages(
                            room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                    ),
                    (safe_room, user_id, display_name, clean_text, now, "voice", None, None, None),
                )
                new_id = int(cur.lastrowid)
                cur.execute(
                    _sql(
                        """
                        SELECT id, room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        FROM chat_messages
                        WHERE id=?
                        LIMIT 1
                        """
                    ),
                    (new_id,),
                )
                row = dict(cur.fetchone())
            relative_path = _store_audio_file(relative_dir, int(row["id"]), user_id, extension, payload)
            target = _resolve_audio_path(relative_path)
            cur.execute(
                _sql(
                    """
                    UPDATE chat_messages
                    SET audio_path=?, audio_mime_type=?, audio_duration_ms=?
                    WHERE id=?
                    """
                ),
                (relative_path, mime_type, safe_duration_ms, int(row["id"])),
            )
            cur.execute(
                _sql(
                    """
                    SELECT id, room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                    FROM chat_messages
                    WHERE id=?
                    LIMIT 1
                    """
                ),
                (int(row["id"]),),
            )
            row = dict(cur.fetchone())
            conn.commit()
        return _serialize_public_message(row)
    except Exception:
        if conn is not None:
            conn.rollback()
            conn.close()
            conn = None
        file_exists = bool(target and target.exists() and target.is_file())
        _LOGGER.exception(
            "Failed to persist public voice note",
            extra={"room": room, "message_id": int(row["id"]) if row else None, "user_id": int(user["id"])},
        )
        _log_voice_file_issue(
            route=f"/chat/rooms/{room}/voice",
            message_id=int(row["id"]) if row else None,
            content_type=mime_type,
            file_exists=file_exists,
            detail="public voice persistence failed",
        )
        if target and target.exists():
            target.unlink(missing_ok=True)
        raise
    finally:
        if conn is not None:
            conn.close()


def _persist_private_voice_message(
    sender_user_id: int,
    recipient_user_id: int,
    upload: UploadFile,
    duration_ms: int | None,
    text: str | None,
) -> dict:
    _enforce_rate_limit(int(sender_user_id))
    clean_text = _voice_note_text_fallback(text)
    safe_duration_ms = _validate_duration_ms(duration_ms)
    payload, mime_type, extension = _read_upload_audio(upload)
    relative_dir = _private_audio_subdir(sender_user_id, recipient_user_id)
    target: Path | None = None
    conn = None
    row: dict | None = None
    try:
        with _db_lock:
            conn = _db()
            cur = conn.cursor()
            if DB_BACKEND == "postgres":
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(
                            sender_user_id, recipient_user_id, text, message_type, audio_path, audio_mime_type, audio_duration_ms
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        RETURNING id, sender_user_id, recipient_user_id, text, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        """
                    ),
                    (int(sender_user_id), int(recipient_user_id), clean_text, "voice", None, None, None),
                )
                row = dict(cur.fetchone())
            else:
                cur.execute(
                    _sql(
                        """
                        INSERT INTO private_chat_messages(
                            sender_user_id, recipient_user_id, text, message_type, audio_path, audio_mime_type, audio_duration_ms
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                    ),
                    (int(sender_user_id), int(recipient_user_id), clean_text, "voice", None, None, None),
                )
                new_id = int(cur.lastrowid)
                cur.execute(
                    _sql(
                        """
                        SELECT id, sender_user_id, recipient_user_id, text, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                        FROM private_chat_messages
                        WHERE id=?
                        LIMIT 1
                        """
                    ),
                    (new_id,),
                )
                row = dict(cur.fetchone())
            relative_path = _store_audio_file(relative_dir, int(row["id"]), int(sender_user_id), extension, payload)
            target = _resolve_audio_path(relative_path)
            cur.execute(
                _sql(
                    """
                    UPDATE private_chat_messages
                    SET audio_path=?, audio_mime_type=?, audio_duration_ms=?
                    WHERE id=?
                    """
                ),
                (relative_path, mime_type, safe_duration_ms, int(row["id"])),
            )
            cur.execute(
                _sql(
                    """
                    SELECT id, sender_user_id, recipient_user_id, text, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
                    FROM private_chat_messages
                    WHERE id=?
                    LIMIT 1
                    """
                ),
                (int(row["id"]),),
            )
            row = dict(cur.fetchone())
            conn.commit()
        return _serialize_private_message(row)
    except Exception:
        if conn is not None:
            conn.rollback()
            conn.close()
            conn = None
        file_exists = bool(target and target.exists() and target.is_file())
        _LOGGER.exception(
            "Failed to persist private voice note",
            extra={
                "message_id": int(row["id"]) if row else None,
                "sender_user_id": int(sender_user_id),
                "recipient_user_id": int(recipient_user_id),
            },
        )
        _log_voice_file_issue(
            route=f"/chat/private/{recipient_user_id}/voice",
            message_id=int(row["id"]) if row else None,
            content_type=mime_type,
            file_exists=file_exists,
            detail="private voice persistence failed",
        )
        if target and target.exists():
            target.unlink(missing_ok=True)
        raise
    finally:
        if conn is not None:
            conn.close()


def _parse_range_header(range_header: str | None, file_size: int) -> tuple[int, int] | None:
    if not range_header:
        return None
    text = range_header.strip().lower()
    if not text.startswith("bytes="):
        raise HTTPException(status_code=416, detail="Invalid range")
    try:
        start_text, _, end_text = text[6:].partition("-")
        if not start_text and not end_text:
            raise HTTPException(status_code=416, detail="Invalid range")
        if not start_text:
            length = int(end_text)
            if length <= 0:
                raise HTTPException(status_code=416, detail="Invalid range")
            start = max(0, file_size - length)
            end = file_size - 1
            return start, end
        start = int(start_text)
        end = file_size - 1 if not end_text else int(end_text)
    except ValueError as exc:
        raise HTTPException(status_code=416, detail="Invalid range") from exc
    if start < 0 or end < start or start >= file_size:
        raise HTTPException(status_code=416, detail="Invalid range")
    return start, min(end, file_size - 1)


def _audio_response(target: Path, mime_type: str, range_header: str | None, head_only: bool) -> Response:
    stat_result = target.stat()
    file_size = int(stat_result.st_size)
    byte_range = _parse_range_header(range_header, file_size)
    etag = f'W/"chat-audio-{target.name}-{int(stat_result.st_mtime)}-{file_size}"'
    common_headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime_type,
        "Last-Modified": formatdate(stat_result.st_mtime, usegmt=True),
        "Cache-Control": "private, max-age=300",
        "ETag": etag,
    }
    if byte_range is None:
        common_headers["Content-Length"] = str(file_size)
        return Response(
            content=b"" if head_only else target.read_bytes(),
            media_type=mime_type,
            headers=common_headers,
            status_code=200,
        )

    start, end = byte_range
    content_length = (end - start) + 1
    common_headers["Content-Length"] = str(content_length)
    common_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
    if head_only:
        return Response(status_code=206, headers=common_headers)
    with target.open("rb") as fh:
        fh.seek(start)
        content = fh.read(content_length)
    return Response(content=content, media_type=mime_type, headers=common_headers, status_code=206)


def _fetch_public_audio_row(message_id: int) -> dict:
    rows = _db_query_all(
        """
        SELECT id, audio_path, audio_mime_type
        FROM chat_messages
        WHERE id=? AND message_type='voice'
        LIMIT 1
        """,
        (int(message_id),),
    )
    if not rows:
        _log_voice_file_issue(
            route="/chat/audio/public/{message_id}",
            message_id=int(message_id),
            content_type=None,
            file_exists=None,
            detail="public audio metadata missing",
        )
        raise HTTPException(status_code=404, detail="Audio not found")
    return dict(rows[0])


def _fetch_private_audio_row(message_id: int) -> dict:
    rows = _db_query_all(
        """
        SELECT id, sender_user_id, recipient_user_id, audio_path, audio_mime_type
        FROM private_chat_messages
        WHERE id=? AND message_type='voice'
        LIMIT 1
        """,
        (int(message_id),),
    )
    if not rows:
        _log_voice_file_issue(
            route="/chat/audio/private/{message_id}",
            message_id=int(message_id),
            content_type=None,
            file_exists=None,
            detail="private audio metadata missing",
        )
        raise HTTPException(status_code=404, detail="Audio not found")
    return dict(rows[0])


def _serve_audio(route: str, message_id: int, row: dict, request: Request) -> Response:
    if not row.get("audio_path"):
        _log_voice_file_issue(
            route=route,
            message_id=message_id,
            content_type=row.get("audio_mime_type"),
            file_exists=False,
            detail="audio path missing",
        )
        raise HTTPException(status_code=404, detail="Audio not found")
    target = _resolve_audio_path(str(row["audio_path"]))
    exists = target.exists() and target.is_file()
    if not exists:
        _log_voice_file_issue(
            route=route,
            message_id=message_id,
            content_type=row.get("audio_mime_type"),
            file_exists=False,
            detail="audio file missing",
        )
        raise HTTPException(status_code=404, detail="Audio file missing")
    return _audio_response(
        target=target,
        mime_type=row.get("audio_mime_type") or "application/octet-stream",
        range_header=request.headers.get("range"),
        head_only=request.method.upper() == "HEAD",
    )


def _create_private_text_message(sender_user_id: int, recipient_user_id: int, text: str) -> dict:
    clean_text = _validate_text(text)
    _enforce_rate_limit(int(sender_user_id))
    row = _insert_private_message(sender_user_id, recipient_user_id, clean_text, message_type="text")
    return _serialize_private_message(row)


def _with_sender_legacy_fields(message: dict, sender_user_id: int, other_user_id: int) -> dict:
    sender_payload = _user_directory_payloads([sender_user_id]).get(sender_user_id, {})
    enriched = dict(message)
    enriched["user_id"] = sender_user_id
    enriched["display_name"] = sender_payload.get("display_name")
    enriched["sender_display_name"] = sender_payload.get("display_name")
    enriched["room"] = _dm_room_for_users(sender_user_id, int(other_user_id))
    return enriched


def _list_messages_for_room(room: str, after: str | None, limit: int) -> dict:
    safe_room = _normalize_room(room)
    safe_limit = max(1, min(200, int(limit)))
    after_filter = _parse_after(after)

    where = ["room = ?"]
    params: list[int | str] = [safe_room]

    if after_filter.field == "id" and after_filter.value is not None:
        where.append("id > ?")
        params.append(int(after_filter.value))
    elif after_filter.field == "created_at" and after_filter.value is not None:
        if DB_BACKEND == "postgres":
            where.append("created_at > to_timestamp(?)")
        else:
            where.append("created_at > ?")
        params.append(int(after_filter.value))

    select_sql = """
        SELECT id, room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
        FROM chat_messages
        WHERE {where_clause}
    """
    if after_filter.field is None:
        rows = _db_query_all(
            f"""
            SELECT id, room, user_id, display_name, message, created_at, message_type, audio_path, audio_mime_type, audio_duration_ms
            FROM (
                {select_sql.format(where_clause='room = ?')}
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
            {select_sql.format(where_clause=' AND '.join(where))}
            ORDER BY id ASC
            LIMIT ?
            """,
            tuple(params + [safe_limit]),
        )

    return {"room": safe_room, "messages": [_serialize_public_message(dict(r)) for r in rows]}


def _create_message_for_room(room: str, payload: ChatMessagePayload, user) -> dict:
    _enforce_rate_limit(int(user["id"]))
    row = _insert_public_message(room, user, _validate_text(payload.text), message_type="text")
    return _serialize_public_message(row)


def send_legacy_global_text_message(user, text: str) -> dict:
    _enforce_rate_limit(int(user["id"]))
    row = _insert_public_message("global", user, _validate_text(text), message_type="text")
    created_at = row.get("created_at")
    if hasattr(created_at, "timestamp"):
        created_at = int(created_at.timestamp())
    else:
        try:
            created_at = int(created_at)
        except Exception:
            created_at = int(time.time())
    return {
        "ok": True,
        "id": int(row["id"]),
        "created_at": created_at,
        "display_name": row.get("display_name"),
    }


def list_legacy_global_messages(limit: int = 50, after_id: int | None = None) -> list[dict[str, Any]]:
    safe_limit = max(1, min(200, int(limit)))
    if after_id is None:
        rows = _db_query_all(
            """
            SELECT id, user_id, display_name, message, created_at
            FROM chat_messages
            WHERE room = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            ("global", safe_limit),
        )
        iterable = reversed(rows)
    else:
        rows = _db_query_all(
            """
            SELECT id, user_id, display_name, message, created_at
            FROM chat_messages
            WHERE room = ? AND id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            ("global", max(0, int(after_id)), safe_limit),
        )
        iterable = rows
    return [dict(row) for row in iterable]


def _list_dm_messages_payload(
    my_user_id: int,
    other_user_id: int,
    after: str | None,
    limit: int,
    mark_read: bool,
    since_id: int | None = None,
) -> dict:
    after_filter = _parse_after(after) if after is not None else _AfterFilter(field=None, value=None)
    messages = _fetch_private_conversation(
        my_user_id,
        other_user_id,
        since_id=since_id,
        limit=limit,
        after_filter=after_filter,
    )
    sender_ids = sorted({int(msg["sender_user_id"]) for msg in messages})
    user_payloads = _user_directory_payloads(sender_ids)
    legacy_messages: list[dict] = []
    for msg in messages:
        item = dict(msg)
        sender_payload = user_payloads.get(int(msg["sender_user_id"]), {})
        item["user_id"] = int(msg["sender_user_id"])
        item["display_name"] = sender_payload.get("display_name")
        item["sender_display_name"] = sender_payload.get("display_name")
        item["room"] = _dm_room_for_users(my_user_id, other_user_id)
        legacy_messages.append(item)
    if mark_read:
        _mark_private_read(my_user_id, other_user_id)
    return {"room": _dm_room_for_users(my_user_id, other_user_id), "messages": legacy_messages}


@router.get("/rooms/{room}")
def list_room_messages(
    room: str,
    after: str | None = None,
    limit: int = 50,
    user=Depends(require_user),
):
    _ = user
    maybe_purge_expired_chat_data()
    return _list_messages_for_room(room, after, limit)


@router.post("/rooms/{room}")
def create_room_message(room: str, payload: ChatMessagePayload, user=Depends(require_user)):
    maybe_purge_expired_chat_data()
    return _create_message_for_room(room, payload, user)


@router.post("/rooms/{room}/voice")
def create_room_voice_message(
    room: str,
    request: Request,
    file: UploadFile | None = File(None),
    audio: UploadFile | None = File(None),
    duration_ms: int | None = Form(None),
    text: str | None = Form(default=None),
    user=Depends(require_user),
):
    maybe_purge_expired_chat_data()
    upload = _resolve_voice_upload(request, file, audio)
    return _persist_public_voice_message(room, user, upload, duration_ms, text)


@router.get("/dm/{other_user_id}")
def list_dm_messages(
    other_user_id: int,
    after: str | None = None,
    since_id: int | None = None,
    limit: int = 50,
    mark_read: bool = True,
    user=Depends(require_user),
):
    my_user_id = int(user["id"])
    maybe_purge_expired_chat_data()
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    return _list_dm_messages_payload(my_user_id, int(other_user_id), after, limit, mark_read, since_id=since_id)


@router.post("/dm/{other_user_id}")
def create_dm_message(other_user_id: int, payload: ChatMessagePayload, user=Depends(require_user)):
    my_user_id = int(user["id"])
    maybe_purge_expired_chat_data()
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    message = _create_private_text_message(my_user_id, int(other_user_id), payload.text)
    return _with_sender_legacy_fields(message, my_user_id, int(other_user_id))


@router.post("/dm/{other_user_id}/voice")
def create_dm_voice_message(
    other_user_id: int,
    request: Request,
    file: UploadFile | None = File(None),
    audio: UploadFile | None = File(None),
    duration_ms: int | None = Form(None),
    text: str | None = Form(default=None),
    user=Depends(require_user),
):
    my_user_id = int(user["id"])
    maybe_purge_expired_chat_data()
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    upload = _resolve_voice_upload(request, file, audio)
    message = _persist_private_voice_message(my_user_id, int(other_user_id), upload, duration_ms, text)
    return _with_sender_legacy_fields(message, my_user_id, int(other_user_id))


@router.get("/private/threads", response_model=PrivateChatThreadsResponse)
def list_private_threads(user=Depends(require_user)):
    maybe_purge_expired_chat_data()
    return {"threads": _list_private_threads(int(user["id"]))}


@router.get("/private/{other_user_id}", response_model=PrivateChatMessagesResponse)
def list_private_messages(
    other_user_id: int,
    since_id: int | None = None,
    limit: int = Query(default=50, ge=1, le=_MAX_PRIVATE_PAGE_SIZE),
    mark_read: bool = True,
    user=Depends(require_user),
):
    my_user_id = int(user["id"])
    maybe_purge_expired_chat_data()
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))

    messages = _fetch_private_conversation(my_user_id, int(other_user_id), since_id=since_id, limit=limit)
    if mark_read:
        _mark_private_read(my_user_id, int(other_user_id))

    return {"other_user_id": int(other_user_id), "messages": messages}


@router.post("/private/{other_user_id}", response_model=PrivateChatMessageOut)
def create_private_message(other_user_id: int, payload: PrivateChatSendIn, user=Depends(require_user)):
    my_user_id = int(user["id"])
    maybe_purge_expired_chat_data()
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    return _create_private_text_message(my_user_id, int(other_user_id), payload.text)


@router.post("/private/{other_user_id}/voice", response_model=PrivateChatMessageOut)
def create_private_voice_message(
    other_user_id: int,
    request: Request,
    file: UploadFile | None = File(None),
    audio: UploadFile | None = File(None),
    duration_ms: int | None = Form(None),
    text: str | None = Form(default=None),
    user=Depends(require_user),
):
    my_user_id = int(user["id"])
    maybe_purge_expired_chat_data()
    if int(other_user_id) == my_user_id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")
    _ensure_dm_target_exists(int(other_user_id))
    upload = _resolve_voice_upload(request, file, audio)
    return _persist_private_voice_message(my_user_id, int(other_user_id), upload, duration_ms, text)


@router.api_route("/audio/public/{message_id}", methods=["GET", "HEAD"])
def get_public_audio(message_id: int, request: Request, user=Depends(require_user)):
    _ = user
    row = _fetch_public_audio_row(message_id)
    return _serve_audio("/chat/audio/public/{message_id}", int(message_id), row, request)


@router.api_route("/audio/private/{message_id}", methods=["GET", "HEAD"])
def get_private_audio(message_id: int, request: Request, user=Depends(require_user)):
    my_user_id = int(user["id"])
    row = _fetch_private_audio_row(message_id)
    if my_user_id not in {int(row["sender_user_id"]), int(row["recipient_user_id"])}:
        raise HTTPException(status_code=403, detail="Not allowed")
    return _serve_audio("/chat/audio/private/{message_id}", int(message_id), row, request)
