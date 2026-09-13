from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List

from core import DATA_DIR, DB_BACKEND, _db, _db_lock, _sql

_LOGGER = logging.getLogger(__name__)


def _table_exists(cur, table_name: str) -> bool:
    if DB_BACKEND == "postgres":
        cur.execute(_sql("SELECT to_regclass(?) AS regclass"), (str(table_name),))
        row = cur.fetchone()
        return bool(row and row.get("regclass"))
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (str(table_name),))
    return cur.fetchone() is not None


_AUDIO_TABLE_WHERE_WHITELIST = frozenset({
    ("chat_messages", "user_id=?"),
    ("private_chat_messages", "sender_user_id=? OR recipient_user_id=?"),
})


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    # Postgres rows are dict-like (RealDictCursor); SQLite rows are sqlite3.Row,
    # which indexes by name but has no .get. Reading one with .get raises
    # AttributeError on SQLite the first time a row actually comes back.
    if row is None:
        return default
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if value is None else value


def _collect_audio_paths(cur, table_name: str, where_sql: str, params: tuple[Any, ...]) -> List[str]:
    # Both table_name and where_sql are interpolated into the f-string because
    # SQL cannot bind identifiers. Gate the call behind a hardcoded whitelist
    # of (table, where-clause) pairs so a future refactor that sources either
    # from config or user input cannot turn this into SQL injection.
    if (table_name, where_sql) not in _AUDIO_TABLE_WHERE_WHITELIST:
        raise ValueError(f"Refusing audio-path query for ({table_name!r}, {where_sql!r})")
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
        audio_path = str(_row_value(row, "audio_path") or "").strip()
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


def _collect_post_image_paths(cur, user_id: int) -> List[str]:
    # A deleted account's photos have to leave the disk, the same way deleting a
    # single post unlinks its bytes. The rows are about to be hard-deleted, so
    # this has to run first or the paths are gone.
    if not _table_exists(cur, "posts"):
        return []
    cur.execute(
        _sql("SELECT image_path FROM posts WHERE image_path IS NOT NULL AND user_id=?"),
        (int(user_id),),
    )
    seen: set[str] = set()
    ordered: List[str] = []
    for row in cur.fetchall() or []:
        image_path = str(_row_value(row, "image_path") or "").strip()
        if image_path and image_path not in seen:
            seen.add(image_path)
            ordered.append(image_path)
    return ordered


def _safe_unlink_post_image(relative_path: str) -> None:
    # Imported here rather than at module scope: chat.py is a large router
    # module and account deletion is the only thing in here that needs it.
    from chat import _resolve_image_path, _unlink_image_and_thumb

    _unlink_image_and_thumb(_resolve_image_path(str(relative_path)))


# Every table that declares FOREIGN KEY(...) REFERENCES users(id) has to be
# emptied of this driver before the users row goes, or Postgres refuses the
# delete outright. SQLite does not enforce foreign keys unless
# PRAGMA foreign_keys=ON, which this backend never sets -- so the unit suite
# passes whether or not this list is complete, and it wasn't: on production,
# deleting an account that had posted once returned 500 and left both the
# account and the post standing. tests/test_account_deletion_covers_fks.py
# reads the schema and fails if a table with such a key is missing from here.
#
# Order matters: rows that point at other rows go before the rows they point at.
_DELETE_SPECS: List[tuple[str, str]] = [
    # --- the driver's social content, and everything hanging off it -------
    # Deleting someone's post has to take other drivers' likes and comments on
    # it too; those rows point at posts(id).
    ("post_likes",
     "DELETE FROM post_likes WHERE user_id=? "
     "OR post_id IN (SELECT id FROM posts WHERE user_id=?)"),
    ("post_comments",
     "DELETE FROM post_comments WHERE user_id=? "
     "OR post_id IN (SELECT id FROM posts WHERE user_id=?)"),
    ("posts", "DELETE FROM posts WHERE user_id=?"),
    ("follows", "DELETE FROM follows WHERE follower_id=? OR followee_id=?"),
    ("user_blocks", "DELETE FROM user_blocks WHERE blocker_id=? OR blocked_id=?"),
    ("user_mutes", "DELETE FROM user_mutes WHERE muter_id=? OR muted_id=?"),
    ("content_reports",
     "DELETE FROM content_reports WHERE reporter_id=? OR target_user_id=?"),
    ("work_battle_challenges",
     "DELETE FROM work_battle_challenges WHERE challenger_user_id=? "
     "OR challenged_user_id=? OR winner_user_id=? OR loser_user_id=? "
     "OR canceled_by_user_id=? OR declined_by_user_id=?"),
    # --- games: no foreign keys here, so these never blocked the delete.
    # They are in the list because a match whose player no longer exists is not
    # history, it is a row that renders as a blank opponent, and the xp ledger
    # is this driver's own record.
    ("game_match_moves",
     "DELETE FROM game_match_moves WHERE actor_user_id=? OR match_id IN "
     "(SELECT id FROM game_matches WHERE player_one_user_id=? OR player_two_user_id=?)"),
    ("game_match_participants",
     "DELETE FROM game_match_participants WHERE user_id=? OR match_id IN "
     "(SELECT id FROM game_matches WHERE player_one_user_id=? OR player_two_user_id=?)"),
    ("game_xp_awards",
     "DELETE FROM game_xp_awards WHERE user_id=? OR match_id IN "
     "(SELECT id FROM game_matches WHERE player_one_user_id=? OR player_two_user_id=?)"),
    ("game_matches",
     "DELETE FROM game_matches WHERE player_one_user_id=? OR player_two_user_id=? "
     "OR challenger_user_id=? OR challenged_user_id=?"),
    ("game_challenges",
     "DELETE FROM game_challenges WHERE challenger_user_id=? OR challenged_user_id=?"),
    # --- the rest of this driver's own rows -------------------------------
    ("access_token_redemptions", "DELETE FROM access_token_redemptions WHERE user_id=?"),
    ("driver_guidance_state", "DELETE FROM driver_guidance_state WHERE user_id=?"),
    ("assistant_guidance_outcomes", "DELETE FROM assistant_guidance_outcomes WHERE user_id=?"),
    ("presence", "DELETE FROM presence WHERE user_id=?"),
    ("presence_runtime_state", "DELETE FROM presence_runtime_state WHERE user_id=?"),
    ("chat_messages", "DELETE FROM chat_messages WHERE user_id=?"),
    ("private_chat_messages",
     "DELETE FROM private_chat_messages WHERE sender_user_id=? OR recipient_user_id=?"),
    ("events", "DELETE FROM events WHERE user_id=?"),
    ("pickup_logs", "DELETE FROM pickup_logs WHERE user_id=?"),
    ("pickup_guard_state", "DELETE FROM pickup_guard_state WHERE user_id=?"),
    ("driver_work_state", "DELETE FROM driver_work_state WHERE user_id=?"),
    ("driver_daily_stats", "DELETE FROM driver_daily_stats WHERE user_id=?"),
    ("leaderboard_badges_current", "DELETE FROM leaderboard_badges_current WHERE user_id=?"),
]

# Rows that are not this driver's personal data but are keyed by them: the
# scoring history the recommendation engine learns from. Detaching the user id
# keeps the signal and drops the person. Both columns are nullable; the
# guidance-outcome table's is NOT NULL, so that one is deleted above instead.
_ANONYMIZE_SPECS: List[tuple[str, str]] = [
    ("recommendation_outcomes",
     "UPDATE recommendation_outcomes SET user_id=NULL WHERE user_id=?"),
    ("micro_recommendation_outcomes",
     "UPDATE micro_recommendation_outcomes SET user_id=NULL WHERE user_id=?"),
]

# Deliberately untouched: paddle_webhook_events. It is the billing record --
# what was charged, refunded and when -- and it has no foreign key, so it does
# not block the delete. Payment history outlives the account on purpose.


def delete_account_runtime_data(user_id: int) -> Dict[str, Any]:
    uid = int(user_id)
    deleted_counts: Dict[str, int] = {}
    anonymized_counts: Dict[str, int] = {}
    audio_paths: List[str] = []
    image_paths: List[str] = []

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

            image_paths.extend(_collect_post_image_paths(cur, uid))

            for table_name, sql in _DELETE_SPECS:
                if not _table_exists(cur, table_name):
                    continue
                # One bind per placeholder, so a clause can name the same driver
                # in six columns without the caller counting them out by hand.
                cur.execute(_sql(sql), (uid,) * sql.count("?"))
                deleted_counts[table_name] = max(0, int(cur.rowcount or 0))

            for table_name, sql in _ANONYMIZE_SPECS:
                if not _table_exists(cur, table_name):
                    continue
                cur.execute(_sql(sql), (uid,) * sql.count("?"))
                anonymized_counts[table_name] = max(0, int(cur.rowcount or 0))

            if _table_exists(cur, "users"):
                cur.execute(_sql("DELETE FROM users WHERE id=?"), (uid,))
                deleted_counts["users"] = max(0, int(cur.rowcount or 0))

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    for relative_path in dict.fromkeys(audio_paths):
        _safe_unlink_audio(relative_path)
    unlinked_images = 0
    for relative_path in dict.fromkeys(image_paths):
        # The rows are already gone and committed. A file that will not unlink
        # is worth a log line, not a failed deletion the driver has to retry.
        try:
            _safe_unlink_post_image(relative_path)
            unlinked_images += 1
        except Exception:
            _LOGGER.warning("Could not unlink post media for deleted account", exc_info=True)
    _safe_delete_avatar_assets(uid)

    return {
        "ok": True,
        "user_id": uid,
        "deleted": deleted_counts,
        "anonymized": anonymized_counts,
        "avatar_assets_deleted": True,
        "chat_audio_deleted": len(dict.fromkeys(audio_paths)),
        "post_images_deleted": unlinked_images,
    }
