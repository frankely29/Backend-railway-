from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, List

from core import DATA_DIR, DB_BACKEND, _db, _db_lock, _sql


def _table_exists(cur, table_name: str) -> bool:
    if DB_BACKEND == "postgres":
        cur.execute(_sql("SELECT to_regclass(?) AS regclass"), (str(table_name),))
        row = cur.fetchone()
        return bool(row and row.get("regclass"))
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (str(table_name),))
    return cur.fetchone() is not None


def _collect_audio_paths(cur, table_name: str, where_sql: str, params: tuple[Any, ...]) -> List[str]:
    if not _table_exists(cur, table_name):
        return []
    cur.execute(
        _sql(f"SELECT audio_path FROM {table_name} WHERE audio_path IS NOT NULL AND {where_sql}"),
        params,
    )
    rows = cur.fetchall() or []
    seen: set[str] = set()
    ordered: List[str] = []
    for row in rows:
        audio_path = str(row.get("audio_path") or "").strip()
        if audio_path and audio_path not in seen:
            seen.add(audio_path)
            ordered.append(audio_path)
    return ordered


def _safe_unlink_audio(relative_path: str) -> None:
    if not relative_path:
        return
    base = (DATA_DIR / "chat_audio").resolve()
    target = (base / relative_path).resolve()
    if base != target and base not in target.parents:
        return
    target.unlink(missing_ok=True)
    for parent in target.parents:
        if parent == base:
            break
        try:
            parent.rmdir()
        except OSError:
            break


def _safe_delete_avatar_assets(user_id: int) -> None:
    avatar_dir = DATA_DIR / "avatar_thumbs" / str(int(user_id))
    if avatar_dir.exists():
        shutil.rmtree(avatar_dir, ignore_errors=True)


def delete_account_runtime_data(user_id: int) -> Dict[str, Any]:
    uid = int(user_id)
    deleted_counts: Dict[str, int] = {}
    anonymized_counts: Dict[str, int] = {}
    audio_paths: List[str] = []

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            audio_paths.extend(_collect_audio_paths(cur, "chat_messages", "user_id=?", (uid,)))
            audio_paths.extend(
                _collect_audio_paths(
                    cur,
                    "private_chat_messages",
                    "sender_user_id=? OR recipient_user_id=?",
                    (uid, uid),
                )
            )

            delete_specs = [
                ("presence", "DELETE FROM presence WHERE user_id=?"),
                ("chat_messages", "DELETE FROM chat_messages WHERE user_id=?"),
                ("private_chat_messages", "DELETE FROM private_chat_messages WHERE sender_user_id=? OR recipient_user_id=?"),
                ("events", "DELETE FROM events WHERE user_id=?"),
                ("pickup_logs", "DELETE FROM pickup_logs WHERE user_id=?"),
                ("pickup_guard_state", "DELETE FROM pickup_guard_state WHERE user_id=?"),
                ("driver_work_state", "DELETE FROM driver_work_state WHERE user_id=?"),
                ("driver_daily_stats", "DELETE FROM driver_daily_stats WHERE user_id=?"),
                ("leaderboard_badges_current", "DELETE FROM leaderboard_badges_current WHERE user_id=?"),
            ]
            for table_name, sql in delete_specs:
                if not _table_exists(cur, table_name):
                    continue
                params = (uid, uid) if table_name == "private_chat_messages" else (uid,)
                cur.execute(_sql(sql), params)
                deleted_counts[table_name] = max(0, int(cur.rowcount or 0))

            if _table_exists(cur, "recommendation_outcomes"):
                cur.execute(_sql("UPDATE recommendation_outcomes SET user_id=NULL WHERE user_id=?"), (uid,))
                anonymized_counts["recommendation_outcomes"] = max(0, int(cur.rowcount or 0))

            if _table_exists(cur, "users"):
                cur.execute(_sql("DELETE FROM users WHERE id=?"), (uid,))
                deleted_counts["users"] = max(0, int(cur.rowcount or 0))

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    for relative_path in audio_paths:
        _safe_unlink_audio(relative_path)
    _safe_delete_avatar_assets(uid)

    return {
        "ok": True,
        "user_id": uid,
        "deleted": deleted_counts,
        "anonymized": anonymized_counts,
        "avatar_assets_deleted": True,
        "chat_audio_deleted": len(audio_paths),
    }
