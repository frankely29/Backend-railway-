from __future__ import annotations

import copy
import json
import math
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from fastapi import HTTPException

from avatar_assets import avatar_thumb_path, avatar_thumb_url, avatar_version_for_data_url, persist_avatar_thumb
from chat import publish_public_battle_chat_message, publish_public_battle_notification
from core import DATA_DIR, DB_BACKEND, _clean_display_name, _db, _db_lock, _db_query_all, _db_query_one, _sql
from games_dominoes_engine import apply_move as apply_dominoes_move
from games_dominoes_engine import create_initial_state as create_dominoes_state
from leaderboard_service import (
    build_reward_contract,
    get_best_current_badges_for_users,
    get_progression_for_user,
    get_progression_for_users,
    get_progression_snapshot_for_total_xp,
)

ALLOWED_GAME_TYPES = {"dominoes", "billiards"}
CHALLENGE_EXPIRATION_SECONDS = 15 * 60
WINNER_XP_AWARD = 60
LOSER_XP_AWARD = 20
MAX_RECENT_BATTLES = 5
MAX_CHALLENGEABLE_USERS = 100
BILLIARDS_GROUPS = {"solids": [1, 2, 3, 4, 5, 6, 7], "stripes": [9, 10, 11, 12, 13, 14, 15]}
FINAL_EIGHT_BALL = 8


def _iso(ts: Any | None) -> Optional[str]:
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def _now_ts() -> int:
    return int(time.time())


def _load_json(value: Any, fallback: Any) -> Any:
    if not value:
        return copy.deepcopy(fallback)
    if isinstance(value, (dict, list)):
        return copy.deepcopy(value)
    try:
        return json.loads(value)
    except Exception:
        return copy.deepcopy(fallback)


def _dump_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _query_one_cur(cur, sql: str, params: tuple = ()) -> Optional[dict]:
    cur.execute(_sql(sql), params)
    row = cur.fetchone()
    return dict(row) if row else None


def _query_all_cur(cur, sql: str, params: tuple = ()) -> list[dict]:
    cur.execute(_sql(sql), params)
    return [dict(row) for row in cur.fetchall()]


def _exec_cur(cur, sql: str, params: tuple = ()) -> None:
    cur.execute(_sql(sql), params)


def _insert_and_get_id(cur, sql: str, params: tuple = ()) -> int:
    if DB_BACKEND == "postgres":
        cur.execute(_sql(sql.rstrip().rstrip(";") + " RETURNING id"), params)
        row = cur.fetchone()
        return int(row[0] if not isinstance(row, dict) else row["id"])
    cur.execute(_sql(sql), params)
    return int(cur.lastrowid)


def _normalize_challenge_status(status: Any) -> str:
    raw = str(status or "pending").strip().lower()
    if raw == "cancelled":
        return "canceled"
    return raw


def _normalize_match_status(status: Any) -> str:
    raw = str(status or "active").strip().lower()
    if raw == "void":
        return "abandoned"
    return raw


def _user_row(user_id: int, cur=None) -> Optional[dict]:
    sql = "SELECT id, email, display_name, avatar_url, avatar_version, is_disabled, is_suspended, map_identity_mode FROM users WHERE id=? LIMIT 1"
    row = _query_one_cur(cur, sql, (int(user_id),)) if cur is not None else _db_query_one(sql, (int(user_id),))
    return dict(row) if row else None


def _display_name_for_user(row: dict[str, Any]) -> str:
    return _clean_display_name((row.get("display_name") or "").strip(), row.get("email") or "Driver")


def _avatar_thumb_for_user_row(row: dict[str, Any] | None, *, persist_version: bool = False, cur=None) -> tuple[Optional[str], Optional[str]]:
    if not row:
        return None, None
    user_id = int(row["id"])
    avatar_url_value = row.get("avatar_url")
    avatar_version = row.get("avatar_version")
    if not avatar_url_value:
        return None, None
    resolved_version = str(avatar_version).strip() if avatar_version else None
    if not resolved_version:
        resolved_version = avatar_version_for_data_url(str(avatar_url_value))
        if resolved_version:
            if not avatar_thumb_path(DATA_DIR, user_id, resolved_version).exists():
                persist_avatar_thumb(DATA_DIR, user_id, str(avatar_url_value), resolved_version)
            if persist_version:
                if cur is not None:
                    _exec_cur(cur, "UPDATE users SET avatar_version=? WHERE id=? AND (avatar_version IS NULL OR trim(avatar_version)='')", (resolved_version, user_id))
                else:
                    from core import _db_exec

                    _db_exec("UPDATE users SET avatar_version=? WHERE id=? AND (avatar_version IS NULL OR trim(avatar_version)='')", (resolved_version, user_id))
        return avatar_thumb_url(user_id, resolved_version), resolved_version
    if not avatar_thumb_path(DATA_DIR, user_id, resolved_version).exists():
        persist_avatar_thumb(DATA_DIR, user_id, str(avatar_url_value), resolved_version)
    return avatar_thumb_url(user_id, resolved_version), resolved_version


def _ensure_sqlite_column(cur, table: str, column: str, ddl: str) -> None:
    cur.execute(f"PRAGMA table_info({table})")
    existing = {str(row[1]) for row in cur.fetchall()}
    if column not in existing:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _ensure_postgres_column(cur, table: str, column: str, ddl: str) -> None:
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s LIMIT 1",
        (table, column),
    )
    if not cur.fetchone():
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def ensure_games_schema() -> None:
    conn = _db()
    try:
        cur = conn.cursor()
        if DB_BACKEND == "postgres":
            statements = [
                """
                CREATE TABLE IF NOT EXISTS game_challenges (
                  id BIGSERIAL PRIMARY KEY,
                  challenger_user_id BIGINT NOT NULL,
                  challenged_user_id BIGINT NOT NULL,
                  game_type TEXT NOT NULL,
                  status TEXT NOT NULL,
                  created_at BIGINT NOT NULL,
                  updated_at BIGINT NOT NULL,
                  expires_at BIGINT NOT NULL,
                  accepted_at BIGINT,
                  declined_at BIGINT,
                  canceled_at BIGINT,
                  cancelled_at BIGINT,
                  responded_at BIGINT,
                  completed_match_id BIGINT,
                  FOREIGN KEY(challenger_user_id) REFERENCES users(id),
                  FOREIGN KEY(challenged_user_id) REFERENCES users(id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS game_matches (
                  id BIGSERIAL PRIMARY KEY,
                  source_challenge_id BIGINT,
                  challenge_id BIGINT,
                  game_type TEXT NOT NULL,
                  challenger_user_id BIGINT,
                  challenged_user_id BIGINT,
                  player_one_user_id BIGINT NOT NULL,
                  player_two_user_id BIGINT NOT NULL,
                  current_turn_user_id BIGINT,
                  status TEXT NOT NULL,
                  winner_user_id BIGINT,
                  loser_user_id BIGINT,
                  winner_xp_awarded INTEGER NOT NULL DEFAULT 0,
                  loser_xp_awarded INTEGER NOT NULL DEFAULT 0,
                  match_state_json TEXT NOT NULL,
                  result_summary TEXT,
                  created_at BIGINT NOT NULL,
                  updated_at BIGINT NOT NULL,
                  completed_at BIGINT,
                  reward_announced_at BIGINT,
                  FOREIGN KEY(source_challenge_id) REFERENCES game_challenges(id),
                  FOREIGN KEY(challenge_id) REFERENCES game_challenges(id),
                  FOREIGN KEY(player_one_user_id) REFERENCES users(id),
                  FOREIGN KEY(player_two_user_id) REFERENCES users(id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS game_match_moves (
                  id BIGSERIAL PRIMARY KEY,
                  match_id BIGINT NOT NULL,
                  move_number INTEGER NOT NULL,
                  actor_user_id BIGINT NOT NULL,
                  move_type TEXT NOT NULL,
                  move_payload_json TEXT NOT NULL,
                  created_at BIGINT NOT NULL,
                  FOREIGN KEY(match_id) REFERENCES game_matches(id),
                  FOREIGN KEY(actor_user_id) REFERENCES users(id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS user_game_stats (
                  user_id BIGINT PRIMARY KEY,
                  total_matches INTEGER NOT NULL DEFAULT 0,
                  total_wins INTEGER NOT NULL DEFAULT 0,
                  total_losses INTEGER NOT NULL DEFAULT 0,
                  dominoes_wins INTEGER NOT NULL DEFAULT 0,
                  dominoes_losses INTEGER NOT NULL DEFAULT 0,
                  billiards_wins INTEGER NOT NULL DEFAULT 0,
                  billiards_losses INTEGER NOT NULL DEFAULT 0,
                  game_xp_earned INTEGER NOT NULL DEFAULT 0,
                  updated_at BIGINT NOT NULL DEFAULT 0,
                  FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """,
            ]
            for statement in statements:
                cur.execute(statement)
            for table, column, ddl in [
                ("game_challenges", "accepted_at", "accepted_at BIGINT"),
                ("game_challenges", "declined_at", "declined_at BIGINT"),
                ("game_challenges", "canceled_at", "canceled_at BIGINT"),
                ("game_challenges", "cancelled_at", "cancelled_at BIGINT"),
                ("game_challenges", "completed_match_id", "completed_match_id BIGINT"),
                ("game_matches", "source_challenge_id", "source_challenge_id BIGINT"),
                ("game_matches", "challenge_id", "challenge_id BIGINT"),
                ("game_matches", "challenger_user_id", "challenger_user_id BIGINT"),
                ("game_matches", "challenged_user_id", "challenged_user_id BIGINT"),
                ("game_matches", "result_summary", "result_summary TEXT"),
                ("game_matches", "reward_announced_at", "reward_announced_at BIGINT"),
            ]:
                _ensure_postgres_column(cur, table, column, ddl)
        else:
            statements = [
                """
                CREATE TABLE IF NOT EXISTS game_challenges (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  challenger_user_id INTEGER NOT NULL,
                  challenged_user_id INTEGER NOT NULL,
                  game_type TEXT NOT NULL,
                  status TEXT NOT NULL,
                  created_at INTEGER NOT NULL,
                  updated_at INTEGER NOT NULL,
                  expires_at INTEGER NOT NULL,
                  accepted_at INTEGER,
                  declined_at INTEGER,
                  canceled_at INTEGER,
                  cancelled_at INTEGER,
                  responded_at INTEGER,
                  completed_match_id INTEGER,
                  FOREIGN KEY(challenger_user_id) REFERENCES users(id),
                  FOREIGN KEY(challenged_user_id) REFERENCES users(id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS game_matches (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  source_challenge_id INTEGER,
                  challenge_id INTEGER,
                  game_type TEXT NOT NULL,
                  challenger_user_id INTEGER,
                  challenged_user_id INTEGER,
                  player_one_user_id INTEGER NOT NULL,
                  player_two_user_id INTEGER NOT NULL,
                  current_turn_user_id INTEGER,
                  status TEXT NOT NULL,
                  winner_user_id INTEGER,
                  loser_user_id INTEGER,
                  winner_xp_awarded INTEGER NOT NULL DEFAULT 0,
                  loser_xp_awarded INTEGER NOT NULL DEFAULT 0,
                  match_state_json TEXT NOT NULL,
                  result_summary TEXT,
                  created_at INTEGER NOT NULL,
                  updated_at INTEGER NOT NULL,
                  completed_at INTEGER,
                  reward_announced_at INTEGER,
                  FOREIGN KEY(source_challenge_id) REFERENCES game_challenges(id),
                  FOREIGN KEY(challenge_id) REFERENCES game_challenges(id),
                  FOREIGN KEY(player_one_user_id) REFERENCES users(id),
                  FOREIGN KEY(player_two_user_id) REFERENCES users(id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS game_match_moves (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  match_id INTEGER NOT NULL,
                  move_number INTEGER NOT NULL,
                  actor_user_id INTEGER NOT NULL,
                  move_type TEXT NOT NULL,
                  move_payload_json TEXT NOT NULL,
                  created_at INTEGER NOT NULL,
                  FOREIGN KEY(match_id) REFERENCES game_matches(id),
                  FOREIGN KEY(actor_user_id) REFERENCES users(id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS user_game_stats (
                  user_id INTEGER PRIMARY KEY,
                  total_matches INTEGER NOT NULL DEFAULT 0,
                  total_wins INTEGER NOT NULL DEFAULT 0,
                  total_losses INTEGER NOT NULL DEFAULT 0,
                  dominoes_wins INTEGER NOT NULL DEFAULT 0,
                  dominoes_losses INTEGER NOT NULL DEFAULT 0,
                  billiards_wins INTEGER NOT NULL DEFAULT 0,
                  billiards_losses INTEGER NOT NULL DEFAULT 0,
                  game_xp_earned INTEGER NOT NULL DEFAULT 0,
                  updated_at INTEGER NOT NULL DEFAULT 0,
                  FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """,
            ]
            for statement in statements:
                cur.execute(statement)
            for table, column, ddl in [
                ("game_challenges", "accepted_at", "accepted_at INTEGER"),
                ("game_challenges", "declined_at", "declined_at INTEGER"),
                ("game_challenges", "canceled_at", "canceled_at INTEGER"),
                ("game_challenges", "cancelled_at", "cancelled_at INTEGER"),
                ("game_challenges", "completed_match_id", "completed_match_id INTEGER"),
                ("game_matches", "source_challenge_id", "source_challenge_id INTEGER"),
                ("game_matches", "challenge_id", "challenge_id INTEGER"),
                ("game_matches", "challenger_user_id", "challenger_user_id INTEGER"),
                ("game_matches", "challenged_user_id", "challenged_user_id INTEGER"),
                ("game_matches", "result_summary", "result_summary TEXT"),
                ("game_matches", "reward_announced_at", "reward_announced_at INTEGER"),
            ]:
                _ensure_sqlite_column(cur, table, column, ddl)
        for statement in [
            "CREATE INDEX IF NOT EXISTS idx_game_challenges_incoming ON game_challenges(challenged_user_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_challenges_outgoing ON game_challenges(challenger_user_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_challenges_pending_pairs ON game_challenges(game_type, challenger_user_id, challenged_user_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_game_matches_p1_status ON game_matches(player_one_user_id, status, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_matches_p2_status ON game_matches(player_two_user_id, status, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_matches_pair_status ON game_matches(challenger_user_id, challenged_user_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_game_match_moves_lookup ON game_match_moves(match_id, move_number)",
            "CREATE INDEX IF NOT EXISTS idx_game_matches_completed_at ON game_matches(completed_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_matches_game_type ON game_matches(game_type, status)",
        ]:
            cur.execute(statement)
        conn.commit()
    finally:
        conn.close()


def expire_stale_challenges(cur=None, *, now_ts: int | None = None) -> int:
    now = int(now_ts or _now_ts())
    sql = """
        UPDATE game_challenges
        SET status='expired', updated_at=?, responded_at=?
        WHERE status='pending' AND expires_at < ?
    """
    if cur is not None:
        _exec_cur(cur, sql, (now, now, now))
        return int(getattr(cur, "rowcount", 0) or 0)
    with _db_lock:
        conn = _db()
        try:
            cur2 = conn.cursor()
            _exec_cur(cur2, sql, (now, now, now))
            conn.commit()
            return int(getattr(cur2, "rowcount", 0) or 0)
        finally:
            conn.close()


def _validate_target_user(target_user_id: int, *, cur=None) -> dict[str, Any]:
    target = _user_row(int(target_user_id), cur=cur)
    if not target:
        raise HTTPException(status_code=404, detail="Target user not found")
    if int(target.get("is_disabled") or 0) == 1 or int(target.get("is_suspended") or 0) == 1:
        raise HTTPException(status_code=404, detail="Target user not found")
    return target


def _challenge_row_to_payload(row: dict[str, Any]) -> dict[str, Any]:
    challenger_user_id = int(row["challenger_user_id"])
    challenged_user_id = int(row["challenged_user_id"])
    viewer_user_id = row.get("viewer_user_id")
    viewer_is_challenger = viewer_user_id is not None and int(viewer_user_id) == challenger_user_id
    other_user_id = challenged_user_id if viewer_is_challenger else challenger_user_id
    other_user_display_name = row["challenged_display_name"] if viewer_is_challenger else row["challenger_display_name"]
    canceled_at = row.get("canceled_at") if row.get("canceled_at") is not None else row.get("cancelled_at")
    return {
        "id": int(row["id"]),
        "game_type": row["game_type"],
        "game_key": row["game_type"],
        "status": _normalize_challenge_status(row.get("status")),
        "challenger_user_id": challenger_user_id,
        "challenger_display_name": row["challenger_display_name"],
        "challenged_user_id": challenged_user_id,
        "challenged_display_name": row["challenged_display_name"],
        "other_user_id": other_user_id,
        "other_user_display_name": other_user_display_name,
        "opponent_user_id": other_user_id,
        "opponent_display_name": other_user_display_name,
        "created_at": _iso(row.get("created_at")),
        "expires_at": _iso(row.get("expires_at")),
        "accepted_at": _iso(row.get("accepted_at")),
        "declined_at": _iso(row.get("declined_at")),
        "canceled_at": _iso(canceled_at),
        "completed_match_id": int(row["completed_match_id"]) if row.get("completed_match_id") is not None else None,
    }


def _serialize_match_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "game_type": row["game_type"],
        "game_key": row["game_type"],
        "status": _normalize_match_status(row["status"]),
        "challenger_user_id": int(row.get("challenger_user_id") or row.get("player_one_user_id")),
        "challenged_user_id": int(row.get("challenged_user_id") or row.get("player_two_user_id")),
        "current_turn_user_id": int(row["current_turn_user_id"]) if row.get("current_turn_user_id") is not None else None,
        "player_one_user_id": int(row["player_one_user_id"]),
        "player_two_user_id": int(row["player_two_user_id"]),
        "winner_user_id": int(row["winner_user_id"]) if row.get("winner_user_id") is not None else None,
        "loser_user_id": int(row["loser_user_id"]) if row.get("loser_user_id") is not None else None,
        "winner_xp_awarded": int(row.get("winner_xp_awarded") or 0),
        "loser_xp_awarded": int(row.get("loser_xp_awarded") or 0),
        "created_at": _iso(row.get("created_at")),
        "updated_at": _iso(row.get("updated_at")),
        "completed_at": _iso(row.get("completed_at")),
    }


def _upsert_user_game_stats(cur, *, user_id: int, is_win: bool, game_type: str, xp_earned: int) -> None:
    now = _now_ts()
    normalized_game_type = str(game_type or "").strip().lower()
    dominoes_win = 1 if is_win and normalized_game_type == "dominoes" else 0
    dominoes_loss = 1 if (not is_win) and normalized_game_type == "dominoes" else 0
    billiards_win = 1 if is_win and normalized_game_type == "billiards" else 0
    billiards_loss = 1 if (not is_win) and normalized_game_type == "billiards" else 0
    _exec_cur(
        cur,
        """
        INSERT INTO user_game_stats(
          user_id, total_matches, total_wins, total_losses,
          dominoes_wins, dominoes_losses, billiards_wins, billiards_losses,
          game_xp_earned, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
          total_matches=user_game_stats.total_matches + excluded.total_matches,
          total_wins=user_game_stats.total_wins + excluded.total_wins,
          total_losses=user_game_stats.total_losses + excluded.total_losses,
          dominoes_wins=user_game_stats.dominoes_wins + excluded.dominoes_wins,
          dominoes_losses=user_game_stats.dominoes_losses + excluded.dominoes_losses,
          billiards_wins=user_game_stats.billiards_wins + excluded.billiards_wins,
          billiards_losses=user_game_stats.billiards_losses + excluded.billiards_losses,
          game_xp_earned=user_game_stats.game_xp_earned + excluded.game_xp_earned,
          updated_at=excluded.updated_at
        """,
        (
            int(user_id),
            1,
            1 if is_win else 0,
            0 if is_win else 1,
            dominoes_win,
            dominoes_loss,
            billiards_win,
            billiards_loss,
            max(0, int(xp_earned or 0)),
            now,
        ),
    )


def _pair_active_match(cur, user_a: int, user_b: int) -> Optional[dict[str, Any]]:
    low_id, high_id = sorted((int(user_a), int(user_b)))
    return _query_one_cur(
        cur,
        """
        SELECT *
        FROM game_matches
        WHERE status='active'
          AND ((player_one_user_id=? AND player_two_user_id=?) OR (player_one_user_id=? AND player_two_user_id=?))
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (low_id, high_id, high_id, low_id),
    )


def list_challengeable_users(user_id: int, *, q: str = "", limit: int = 25, stale_after_sec: int = 300) -> list[dict[str, Any]]:
    safe_limit = max(1, min(MAX_CHALLENGEABLE_USERS, int(limit or 25)))
    cutoff = _now_ts() - max(5, int(stale_after_sec or 300))
    normalized_q = (q or "").strip().lower()
    params: list[Any] = [int(user_id)]
    where = [
        "u.id <> ?",
        "COALESCE(CAST(u.is_disabled AS INTEGER), 0) = 0" if DB_BACKEND != "postgres" else "COALESCE(u.is_disabled, FALSE) = FALSE",
        "COALESCE(CAST(u.is_suspended AS INTEGER), 0) = 0" if DB_BACKEND != "postgres" else "COALESCE(u.is_suspended, FALSE) = FALSE",
    ]
    if normalized_q:
        like = f"%{normalized_q}%"
        where.append("(LOWER(COALESCE(u.display_name, '')) LIKE ? OR LOWER(COALESCE(u.email, '')) LIKE ?)")
        params.extend([like, like])
    params.extend([cutoff, safe_limit])
    rows = _db_query_all(
        f"""
        SELECT
          u.id,
          u.email,
          u.display_name,
          u.avatar_url,
          u.avatar_version,
          MAX(p.updated_at) AS presence_updated_at
        FROM users u
        LEFT JOIN presence p ON p.user_id = u.id
        WHERE {' AND '.join(where)}
        GROUP BY u.id, u.email, u.display_name, u.avatar_url, u.avatar_version
        ORDER BY
          CASE WHEN MAX(p.updated_at) >= ? THEN 0 ELSE 1 END ASC,
          LOWER(COALESCE(u.display_name, '')) ASC,
          u.id ASC
        LIMIT ?
        """,
        tuple(params),
    )
    clean_user_ids = [int(row["id"]) for row in rows]
    badges = get_best_current_badges_for_users(clean_user_ids)
    progressions = get_progression_for_users(clean_user_ids)
    items: list[dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        uid = int(row["id"])
        progression = progressions.get(uid, {})
        presence_updated_at = row.get("presence_updated_at")
        display_name = _display_name_for_user(row)
        thumb_url, avatar_version = _avatar_thumb_for_user_row(row, persist_version=True)
        items.append(
            {
                "user_id": uid,
                "display_name": display_name,
                "avatar_thumb_url": thumb_url,
                "avatar_url": row.get("avatar_url"),
                "avatar_version": avatar_version,
                "level": int(progression.get("level") or 1),
                "rank_icon_key": str(progression.get("rank_icon_key") or "band_001"),
                "leaderboard_badge_code": (badges.get(uid) or {}).get("leaderboard_badge_code"),
                "online": bool(presence_updated_at is not None and int(presence_updated_at) >= cutoff),
            }
        )
    return items


def _challenge_query_base(where_sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    rows = _db_query_all(
        f"""
        SELECT c.*, cu.email AS challenger_email, cu.display_name AS challenger_display_name_raw,
               tu.email AS challenged_email, tu.display_name AS challenged_display_name_raw
        FROM game_challenges c
        JOIN users cu ON cu.id = c.challenger_user_id
        JOIN users tu ON tu.id = c.challenged_user_id
        WHERE {where_sql}
        ORDER BY c.created_at DESC, c.id DESC
        """,
        params,
    )
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["challenger_display_name"] = _clean_display_name(item.get("challenger_display_name_raw") or "", item.get("challenger_email") or "Driver")
        item["challenged_display_name"] = _clean_display_name(item.get("challenged_display_name_raw") or "", item.get("challenged_email") or "Driver")
        items.append(item)
    return items


def list_challenges_for_user(user_id: int) -> dict[str, Any]:
    expire_stale_challenges()
    incoming_rows = _challenge_query_base("c.challenged_user_id=? AND c.status='pending'", (int(user_id),))
    outgoing_rows = _challenge_query_base("c.challenger_user_id=? AND c.status='pending'", (int(user_id),))
    active_match_row = _db_query_one(
        """
        SELECT * FROM game_matches
        WHERE status='active' AND (player_one_user_id=? OR player_two_user_id=?)
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (int(user_id), int(user_id)),
    )

    def _with_names(row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        item["viewer_user_id"] = int(user_id)
        return _challenge_row_to_payload(item)

    active_match = _serialize_match_summary(dict(active_match_row)) if active_match_row else None
    return {
        "incoming": [_with_names(row) for row in incoming_rows],
        "outgoing": [_with_names(row) for row in outgoing_rows],
        "active_match": active_match,
        "activeMatch": active_match,
    }


def list_incoming_challenges_for_user(user_id: int) -> list[dict[str, Any]]:
    return list_challenges_for_user(int(user_id)).get("incoming", [])


def list_outgoing_challenges_for_user(user_id: int) -> list[dict[str, Any]]:
    return list_challenges_for_user(int(user_id)).get("outgoing", [])


def create_challenge(challenger_user_id: int, target_user_id: int, game_type: str) -> dict[str, Any]:
    normalized_game_type = str(game_type or "").strip().lower()
    if normalized_game_type not in ALLOWED_GAME_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported game type")
    if int(challenger_user_id) == int(target_user_id):
        raise HTTPException(status_code=400, detail="You cannot challenge yourself")

    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            expire_stale_challenges(cur)
            challenger = _validate_target_user(int(challenger_user_id), cur=cur)
            target = _validate_target_user(int(target_user_id), cur=cur)
            low_id, high_id = sorted((int(challenger_user_id), int(target_user_id)))
            duplicate = _query_one_cur(
                cur,
                """
                SELECT id FROM game_challenges
                WHERE status='pending' AND game_type=?
                  AND ((challenger_user_id=? AND challenged_user_id=?) OR (challenger_user_id=? AND challenged_user_id=?))
                LIMIT 1
                """,
                (normalized_game_type, low_id, high_id, high_id, low_id),
            )
            if duplicate:
                raise HTTPException(status_code=409, detail="A pending challenge already exists for these players")
            active_match = _pair_active_match(cur, int(challenger_user_id), int(target_user_id))
            if active_match:
                raise HTTPException(status_code=409, detail="These players already have an active match")
            now = _now_ts()
            challenge_id = _insert_and_get_id(
                cur,
                """
                INSERT INTO game_challenges(
                  challenger_user_id, challenged_user_id, game_type, status,
                  created_at, updated_at, expires_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    int(challenger_user_id),
                    int(target_user_id),
                    normalized_game_type,
                    "pending",
                    now,
                    now,
                    now + CHALLENGE_EXPIRATION_SECONDS,
                ),
            )
            conn.commit()
            return {
                "id": challenge_id,
                "game_type": normalized_game_type,
                "game_key": normalized_game_type,
                "status": "pending",
                "challenger_user_id": int(challenger_user_id),
                "challenger_display_name": _display_name_for_user(challenger),
                "challenged_user_id": int(target_user_id),
                "challenged_display_name": _display_name_for_user(target),
                "other_user_id": int(target_user_id),
                "other_user_display_name": _display_name_for_user(target),
                "opponent_user_id": int(target_user_id),
                "opponent_display_name": _display_name_for_user(target),
                "created_at": _iso(now),
                "expires_at": _iso(now + CHALLENGE_EXPIRATION_SECONDS),
                "accepted_at": None,
                "declined_at": None,
                "canceled_at": None,
                "completed_match_id": None,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _default_billiards_state(player_one_user_id: int, player_two_user_id: int) -> dict[str, Any]:
    first_turn = min(int(player_one_user_id), int(player_two_user_id))
    return {
        "game_type": "billiards",
        "rules": "Server-authoritative 8-ball challenge flow. The table opens until a group is claimed, pocketed balls stay down, and the 8-ball wins only after clearing your group without fouling.",
        "turn_user_id": first_turn,
        "turn_count": 1,
        "table_open": True,
        "assignments": {str(int(player_one_user_id)): None, str(int(player_two_user_id)): None},
        "remaining_balls": {
            "solids": BILLIARDS_GROUPS["solids"][:],
            "stripes": BILLIARDS_GROUPS["stripes"][:],
            "eight": [FINAL_EIGHT_BALL],
        },
        "pocketed_balls": [],
        "foul_flags": [],
        "players": {
            str(int(player_one_user_id)): {"group": None, "targets_remaining": 7, "targets_cleared": 0, "black_unlocked": False},
            str(int(player_two_user_id)): {"group": None, "targets_remaining": 7, "targets_cleared": 0, "black_unlocked": False},
        },
        "last_shot": None,
        "winner_user_id": None,
        "result_summary": None,
    }


def _normalize_billiards_state(match_row: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    if state.get("remaining_balls"):
        normalized = copy.deepcopy(state)
    else:
        normalized = _default_billiards_state(int(match_row["player_one_user_id"]), int(match_row["player_two_user_id"]))
        normalized["turn_user_id"] = int(state.get("turn_user_id") or normalized["turn_user_id"])
        normalized["last_shot"] = copy.deepcopy(state.get("last_shot"))
        normalized["result_summary"] = copy.deepcopy(state.get("result_summary"))
        legacy_players = state.get("players") or {}
        for user_id in [int(match_row["player_one_user_id"]), int(match_row["player_two_user_id"])]:
            legacy = legacy_players.get(str(user_id)) or {}
            normalized["players"][str(user_id)]["targets_remaining"] = int(legacy.get("targets_remaining") or 7)
            normalized["players"][str(user_id)]["targets_cleared"] = int(legacy.get("targets_cleared") or 0)
            normalized["players"][str(user_id)]["black_unlocked"] = bool(legacy.get("black_unlocked"))
    normalized.setdefault("turn_count", 1)
    normalized.setdefault("table_open", not any(normalized.get("assignments", {}).values()))
    normalized.setdefault("pocketed_balls", [])
    normalized.setdefault("foul_flags", [])
    normalized.setdefault("last_shot", None)
    normalized.setdefault("result_summary", None)
    normalized.setdefault("players", {})
    for user_id in [int(match_row["player_one_user_id"]), int(match_row["player_two_user_id"])]:
        normalized["players"].setdefault(str(user_id), {"group": None, "targets_remaining": 7, "targets_cleared": 0, "black_unlocked": False})
    return normalized


def _other_player_id(match_row: dict[str, Any], actor_user_id: int) -> int:
    return int(match_row["player_one_user_id"]) if int(match_row["player_two_user_id"]) == int(actor_user_id) else int(match_row["player_two_user_id"])


def _group_for_ball(ball: int) -> Optional[str]:
    if ball in BILLIARDS_GROUPS["solids"]:
        return "solids"
    if ball in BILLIARDS_GROUPS["stripes"]:
        return "stripes"
    return None


def _sync_billiards_player_state(state: dict[str, Any], match_row: dict[str, Any]) -> None:
    assignments = state.get("assignments") or {}
    remaining = state.get("remaining_balls") or {}
    for user_id in [int(match_row["player_one_user_id"]), int(match_row["player_two_user_id"])]:
        player = state["players"].setdefault(str(user_id), {})
        group = assignments.get(str(user_id))
        player["group"] = group
        if group in {"solids", "stripes"}:
            targets_remaining = len(remaining.get(group) or [])
            player["targets_remaining"] = targets_remaining
            player["targets_cleared"] = 7 - targets_remaining
            player["black_unlocked"] = targets_remaining == 0
        else:
            player.setdefault("targets_remaining", 7)
            player.setdefault("targets_cleared", 0)
            player["black_unlocked"] = False


def _validate_billiards_transition(previous_state: dict[str, Any], next_state: dict[str, Any], *, actor_user_id: int) -> None:
    previous_pocketed = set(int(ball) for ball in previous_state.get("pocketed_balls") or [])
    next_pocketed = set(int(ball) for ball in next_state.get("pocketed_balls") or [])
    if not previous_pocketed.issubset(next_pocketed):
        raise HTTPException(status_code=409, detail="Pocketed balls cannot return to the table")
    assignments = next_state.get("assignments") or {}
    actor_group = assignments.get(str(int(actor_user_id)))
    if actor_group not in {None, "solids", "stripes"}:
        raise HTTPException(status_code=400, detail="Invalid billiards group assignment")


def _apply_billiards_result_state(match_row: dict[str, Any], state: dict[str, Any], actor_user_id: int, move: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    shot_input = copy.deepcopy(move.get("shot_input") or {})
    result_state = copy.deepcopy(move.get("result_state") or {})
    if not isinstance(result_state, dict):
        raise HTTPException(status_code=400, detail="Billiards result_state must be an object")
    next_state = _normalize_billiards_state(match_row, state)
    actor_key = str(int(actor_user_id))
    opponent_user_id = _other_player_id(match_row, actor_user_id)
    opponent_key = str(opponent_user_id)

    raw_pocketed = result_state.get("pocketed_balls", [])
    if not isinstance(raw_pocketed, list):
        raise HTTPException(status_code=400, detail="Billiards result_state.pocketed_balls must be an array")
    pocketed_balls = [int(ball) for ball in raw_pocketed]
    if len(set(pocketed_balls)) != len(pocketed_balls):
        raise HTTPException(status_code=400, detail="Duplicate pocketed balls are not allowed")
    invalid = [ball for ball in pocketed_balls if ball < 1 or ball > 15]
    if invalid:
        raise HTTPException(status_code=400, detail="Pocketed balls must be between 1 and 15")
    already_pocketed = set(int(ball) for ball in next_state.get("pocketed_balls") or [])
    if already_pocketed.intersection(pocketed_balls):
        raise HTTPException(status_code=409, detail="Pocketed balls cannot be pocketed again")

    foul = bool(result_state.get("foul") or result_state.get("scratch") or result_state.get("cue_ball_pocketed"))
    next_state["turn_count"] = int(next_state.get("turn_count") or 0) + 1
    next_state["foul_flags"] = list(next_state.get("foul_flags") or [])
    if foul:
        next_state["foul_flags"].append({"turn": next_state["turn_count"], "actor_user_id": int(actor_user_id), "code": "foul"})

    remaining = copy.deepcopy(next_state.get("remaining_balls") or {})
    for ball in pocketed_balls:
        group = _group_for_ball(ball)
        if group:
            remaining[group] = [value for value in list(remaining.get(group) or []) if int(value) != ball]
        elif ball == FINAL_EIGHT_BALL:
            remaining["eight"] = []
    next_state["remaining_balls"] = remaining
    next_state["pocketed_balls"] = sorted(already_pocketed.union(pocketed_balls))

    assignments = dict(next_state.get("assignments") or {actor_key: None, opponent_key: None})
    actor_group = assignments.get(actor_key)
    opponent_group = assignments.get(opponent_key)
    non_eight_groups = {_group_for_ball(ball) for ball in pocketed_balls if ball != FINAL_EIGHT_BALL}
    non_eight_groups.discard(None)
    if actor_group is None and opponent_group is None and not foul and len(non_eight_groups) == 1:
        actor_group = next(iter(non_eight_groups))
        opponent_group = "stripes" if actor_group == "solids" else "solids"
        assignments[actor_key] = actor_group
        assignments[opponent_key] = opponent_group
        next_state["table_open"] = False
    else:
        next_state["table_open"] = not any(assignments.values())
    next_state["assignments"] = assignments
    _sync_billiards_player_state(next_state, match_row)
    actor_group = assignments.get(actor_key)

    actor_scored = False
    if not foul:
        if actor_group in {"solids", "stripes"}:
            actor_scored = any(_group_for_ball(ball) == actor_group for ball in pocketed_balls)
        else:
            actor_scored = any(ball != FINAL_EIGHT_BALL for ball in pocketed_balls)

    outcome = {"completed": False}
    if FINAL_EIGHT_BALL in pocketed_balls:
        black_unlocked = bool(next_state["players"][actor_key].get("black_unlocked"))
        legal_eight_ball = (not foul) and actor_group in {"solids", "stripes"} and black_unlocked
        winner_user_id = int(actor_user_id) if legal_eight_ball else int(opponent_user_id)
        loser_user_id = int(opponent_user_id) if legal_eight_ball else int(actor_user_id)
        next_state["winner_user_id"] = winner_user_id
        next_state["result_summary"] = {
            "reason": "eight_ball_pocketed" if legal_eight_ball else "illegal_eight_ball",
            "actor_user_id": int(actor_user_id),
            "foul": foul,
            "pocketed_balls": pocketed_balls,
        }
        outcome = {"completed": True, "winner_user_id": winner_user_id, "loser_user_id": loser_user_id}
    else:
        next_turn_user_id = int(actor_user_id) if actor_scored and not foul else int(opponent_user_id)
        next_state["turn_user_id"] = next_turn_user_id
        next_state["last_shot"] = {
            "actor_user_id": int(actor_user_id),
            "shot_input": shot_input,
            "reported_result": result_state,
            "pocketed_balls": pocketed_balls,
            "foul": foul,
            "turn_retained": next_turn_user_id == int(actor_user_id),
        }
        reported_next_turn = result_state.get("next_turn_user_id")
        if reported_next_turn is None:
            reported_next_turn = result_state.get("current_turn_user_id")
        if reported_next_turn is not None and int(reported_next_turn) != next_turn_user_id:
            raise HTTPException(status_code=409, detail="Reported next turn does not match authoritative match state")
    if result_state.get("winner_user_id") is not None:
        expected_winner = int(outcome["winner_user_id"]) if outcome.get("completed") else None
        if expected_winner is None or int(result_state.get("winner_user_id")) != expected_winner:
            raise HTTPException(status_code=409, detail="Reported winner does not match authoritative match state")
    _validate_billiards_transition(_normalize_billiards_state(match_row, state), next_state, actor_user_id=int(actor_user_id))
    return next_state, outcome


def _apply_billiards_legacy_move(match_row: dict[str, Any], state: dict[str, Any], actor_user_id: int, move: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    move_type = str(move.get("move_type") or "").strip().lower()
    if move_type != "shot":
        raise HTTPException(status_code=400, detail="Billiards matches accept move_type=shot")
    try:
        angle = float(move.get("angle"))
        power = float(move.get("power"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Billiards shot requires numeric angle and power") from exc
    if not math.isfinite(angle) or not math.isfinite(power):
        raise HTTPException(status_code=400, detail="Billiards shot requires finite angle and power")
    power = max(0.0, min(1.0, power))

    legacy_state = copy.deepcopy(state)
    legacy_state.setdefault("players", {})
    actor_state = legacy_state["players"].setdefault(str(int(actor_user_id)), {"targets_remaining": 3, "targets_cleared": 0, "black_unlocked": False})
    shot_quality = (abs(math.sin(angle)) * 0.45) + (abs(math.cos(angle * 0.5)) * 0.25) + (power * 0.55)
    control_bonus = max(0.0, 0.25 - abs(power - 0.72))
    total_score = shot_quality + control_bonus
    pocketed_targets = 0
    pocketed_final = False

    if int(actor_state.get("targets_remaining") or 0) > 0:
        if total_score >= 0.86:
            pocketed_targets = 1
            actor_state["targets_remaining"] = max(0, int(actor_state.get("targets_remaining") or 0) - 1)
            actor_state["targets_cleared"] = int(actor_state.get("targets_cleared") or 0) + 1
            actor_state["black_unlocked"] = int(actor_state.get("targets_remaining") or 0) == 0
    else:
        if total_score >= 0.93:
            pocketed_final = True
            legacy_state.setdefault("table", {}).setdefault("final_ball", {})["pocketed"] = True

    legacy_state["last_shot"] = {
        "actor_user_id": int(actor_user_id),
        "angle": angle,
        "power": power,
        "pocketed_targets": pocketed_targets,
        "pocketed_final": pocketed_final,
        "score_metric": round(total_score, 4),
    }
    if pocketed_final:
        legacy_state["result_summary"] = {"reason": "final_ball_pocketed", "score_metric": round(total_score, 4)}
        loser = _other_player_id(match_row, actor_user_id)
        return legacy_state, {"completed": True, "winner_user_id": int(actor_user_id), "loser_user_id": loser}
    legacy_state["turn_user_id"] = int(actor_user_id) if pocketed_targets > 0 else _other_player_id(match_row, actor_user_id)
    return legacy_state, {"completed": False}


def _apply_billiards_move(match_row: dict[str, Any], state: dict[str, Any], actor_user_id: int, move: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if move.get("result_state") is not None or move.get("shot_input") is not None:
        return _apply_billiards_result_state(match_row, state, actor_user_id, move)
    return _apply_billiards_legacy_move(match_row, state, actor_user_id, move)


def _create_initial_match_state(game_type: str, player_one_user_id: int, player_two_user_id: int, match_id: int) -> dict[str, Any]:
    if game_type == "dominoes":
        return create_dominoes_state(player_one_user_id, player_two_user_id, match_id)
    if game_type == "billiards":
        return _default_billiards_state(player_one_user_id, player_two_user_id)
    raise HTTPException(status_code=400, detail="Unsupported game type")


def _build_public_notification_payload(match_row: dict[str, Any]) -> dict[str, Any]:
    winner_id = int(match_row["winner_user_id"])
    loser_id = int(match_row["loser_user_id"])
    winner = _user_row(winner_id) or {"email": "winner@example.com"}
    loser = _user_row(loser_id) or {"email": "loser@example.com"}
    winner_progression = get_progression_for_user(winner_id)
    return {
        "type": "battle_result",
        "match_id": int(match_row["id"]),
        "game_type": match_row["game_type"],
        "winner_user_id": winner_id,
        "winner_display_name": _display_name_for_user(winner),
        "loser_user_id": loser_id,
        "loser_display_name": _display_name_for_user(loser),
        "winner_xp_awarded": int(match_row.get("winner_xp_awarded") or 0),
        "winner_new_level": int(winner_progression.get("level") or 1),
        "completed_at": _iso(match_row.get("completed_at")),
    }


def _finalize_match(cur, match_row: dict[str, Any], state: dict[str, Any], winner_user_id: int, loser_user_id: int, *, status: str) -> tuple[dict[str, Any], dict[int, dict[str, Any]], dict[str, Any]]:
    completed_at = _now_ts()
    winner_before = get_progression_for_user(int(winner_user_id))
    loser_before = get_progression_for_user(int(loser_user_id))
    result_summary = copy.deepcopy(state.get("result_summary") or {})
    _exec_cur(
        cur,
        """
        UPDATE game_matches
        SET status=?, current_turn_user_id=NULL, winner_user_id=?, loser_user_id=?,
            winner_xp_awarded=?, loser_xp_awarded=?, match_state_json=?, result_summary=?,
            updated_at=?, completed_at=?
        WHERE id=? AND status='active'
        """,
        (
            status,
            int(winner_user_id),
            int(loser_user_id),
            WINNER_XP_AWARD,
            LOSER_XP_AWARD,
            _dump_json(state),
            _dump_json(result_summary),
            completed_at,
            completed_at,
            int(match_row["id"]),
        ),
    )
    updated_match = _query_one_cur(cur, "SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_row["id"]),))
    if not updated_match:
        raise HTTPException(status_code=500, detail="Failed to finalize match")
    _upsert_user_game_stats(cur, user_id=int(winner_user_id), is_win=True, game_type=str(updated_match.get("game_type") or ""), xp_earned=WINNER_XP_AWARD)
    _upsert_user_game_stats(cur, user_id=int(loser_user_id), is_win=False, game_type=str(updated_match.get("game_type") or ""), xp_earned=LOSER_XP_AWARD)
    winner_after = dict(winner_before)
    winner_after.update(get_progression_snapshot_for_total_xp(int(winner_before.get("total_xp") or 0) + WINNER_XP_AWARD))
    winner_after["xp_breakdown"] = dict(winner_before.get("xp_breakdown") or {})
    winner_after["xp_breakdown"]["game_xp"] = int((winner_after["xp_breakdown"].get("game_xp") or 0) + WINNER_XP_AWARD)
    loser_after = dict(loser_before)
    loser_after.update(get_progression_snapshot_for_total_xp(int(loser_before.get("total_xp") or 0) + LOSER_XP_AWARD))
    loser_after["xp_breakdown"] = dict(loser_before.get("xp_breakdown") or {})
    loser_after["xp_breakdown"]["game_xp"] = int((loser_after["xp_breakdown"].get("game_xp") or 0) + LOSER_XP_AWARD)
    reward_contracts = {
        int(winner_user_id): build_reward_contract(winner_after, WINNER_XP_AWARD),
        int(loser_user_id): build_reward_contract(loser_after, LOSER_XP_AWARD),
    }
    return updated_match, reward_contracts, _build_public_notification_payload(updated_match)


def _maybe_publish_match_result(match_row: dict[str, Any]) -> dict[str, Any]:
    payload = _build_public_notification_payload(match_row)
    if match_row.get("reward_announced_at") is not None:
        return payload
    publish_public_battle_notification(payload)
    publish_public_battle_chat_message(
        author_user_id=int(match_row["winner_user_id"]),
        winner_display_name=payload["winner_display_name"],
        loser_display_name=payload["loser_display_name"],
        game_type=str(match_row.get("game_type") or ""),
        winner_xp_awarded=int(match_row.get("winner_xp_awarded") or 0),
    )
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            _exec_cur(cur, "UPDATE game_matches SET reward_announced_at=? WHERE id=? AND reward_announced_at IS NULL", (_now_ts(), int(match_row["id"])))
            conn.commit()
        finally:
            conn.close()
    return payload


def _accept_challenge(challenge_id: int, user_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            expire_stale_challenges(cur)
            challenge = _query_one_cur(cur, "SELECT * FROM game_challenges WHERE id=? LIMIT 1", (int(challenge_id),))
            if not challenge:
                raise HTTPException(status_code=404, detail="Challenge not found")
            if challenge["status"] == "accepted" and challenge.get("completed_match_id") is not None:
                existing_match = _query_one_cur(cur, "SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(challenge["completed_match_id"]),))
                if existing_match:
                    conn.commit()
                    return challenge, existing_match
            if challenge["status"] != "pending":
                raise HTTPException(status_code=409, detail="Challenge is no longer pending")
            if int(challenge["challenged_user_id"]) != int(user_id):
                raise HTTPException(status_code=403, detail="Only the challenged user can accept this challenge")
            now = _now_ts()
            player_one, player_two = sorted((int(challenge["challenger_user_id"]), int(challenge["challenged_user_id"])))
            existing_match = _query_one_cur(
                cur,
                """
                SELECT * FROM game_matches
                WHERE source_challenge_id=? OR challenge_id=? OR (
                  status='active' AND
                  ((player_one_user_id=? AND player_two_user_id=?) OR (player_one_user_id=? AND player_two_user_id=?))
                )
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(challenge_id), int(challenge_id), player_one, player_two, player_two, player_one),
            )
            if existing_match:
                _exec_cur(
                    cur,
                    """
                    UPDATE game_challenges
                    SET status='accepted', updated_at=?, responded_at=COALESCE(responded_at, ?), accepted_at=COALESCE(accepted_at, ?), completed_match_id=COALESCE(completed_match_id, ?)
                    WHERE id=?
                    """,
                    (now, now, now, int(existing_match["id"]), int(challenge_id)),
                )
                conn.commit()
                return challenge, existing_match
            match_id = _insert_and_get_id(
                cur,
                """
                INSERT INTO game_matches(
                  source_challenge_id, challenge_id, game_type, challenger_user_id, challenged_user_id,
                  player_one_user_id, player_two_user_id, current_turn_user_id, status,
                  winner_user_id, loser_user_id, match_state_json, result_summary, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(challenge_id),
                    int(challenge_id),
                    challenge["game_type"],
                    int(challenge["challenger_user_id"]),
                    int(challenge["challenged_user_id"]),
                    player_one,
                    player_two,
                    min(player_one, player_two),
                    "active",
                    None,
                    None,
                    "{}",
                    None,
                    now,
                    now,
                ),
            )
            state = _create_initial_match_state(challenge["game_type"], player_one, player_two, match_id)
            _exec_cur(
                cur,
                "UPDATE game_matches SET match_state_json=?, current_turn_user_id=? WHERE id=?",
                (_dump_json(state), int(state["turn_user_id"]), int(match_id)),
            )
            _exec_cur(
                cur,
                """
                UPDATE game_challenges
                SET status='accepted', updated_at=?, responded_at=?, accepted_at=?, completed_match_id=?
                WHERE id=?
                """,
                (now, now, now, int(match_id), int(challenge_id)),
            )
            match_row = _query_one_cur(cur, "SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_id),))
            conn.commit()
            return challenge, match_row
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def accept_challenge(challenge_id: int, user_id: int) -> dict[str, Any]:
    _challenge, match_row = _accept_challenge(challenge_id, user_id)
    return get_match_detail(int(match_row["id"]), int(user_id))


def _transition_challenge(challenge_id: int, user_id: int, *, action: str) -> dict[str, Any]:
    if action not in {"decline", "cancel"}:
        raise ValueError(action)
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            expire_stale_challenges(cur)
            challenge = _query_one_cur(cur, "SELECT * FROM game_challenges WHERE id=? LIMIT 1", (int(challenge_id),))
            if not challenge:
                raise HTTPException(status_code=404, detail="Challenge not found")
            if challenge["status"] != "pending":
                raise HTTPException(status_code=409, detail="Challenge is no longer pending")
            if action == "decline" and int(challenge["challenged_user_id"]) != int(user_id):
                raise HTTPException(status_code=403, detail="Only the challenged user can decline this challenge")
            if action == "cancel" and int(challenge["challenger_user_id"]) != int(user_id):
                raise HTTPException(status_code=403, detail="Only the challenger can cancel this challenge")
            now = _now_ts()
            status = "declined" if action == "decline" else "canceled"
            column = "declined_at" if action == "decline" else "canceled_at"
            _exec_cur(cur, f"UPDATE game_challenges SET status=?, updated_at=?, responded_at=?, {column}=? WHERE id=?", (status, now, now, now, int(challenge_id)))
            if action == "cancel":
                try:
                    _exec_cur(cur, "UPDATE game_challenges SET cancelled_at=? WHERE id=?", (now, int(challenge_id)))
                except Exception:
                    pass
            challenge = _query_one_cur(cur, "SELECT * FROM game_challenges WHERE id=? LIMIT 1", (int(challenge_id),))
            conn.commit()
            challenger = _user_row(int(challenge["challenger_user_id"])) or {"email": "Driver"}
            challenged = _user_row(int(challenge["challenged_user_id"])) or {"email": "Driver"}
            challenge["challenger_display_name"] = _display_name_for_user(challenger)
            challenge["challenged_display_name"] = _display_name_for_user(challenged)
            challenge["viewer_user_id"] = int(user_id)
            return _challenge_row_to_payload(challenge)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def decline_challenge(challenge_id: int, user_id: int) -> dict[str, Any]:
    return _transition_challenge(challenge_id, user_id, action="decline")


def cancel_challenge(challenge_id: int, user_id: int) -> dict[str, Any]:
    return _transition_challenge(challenge_id, user_id, action="cancel")


def get_active_match_for_user(user_id: int) -> Optional[dict[str, Any]]:
    row = _db_query_one(
        """
        SELECT * FROM game_matches
        WHERE status='active' AND (player_one_user_id=? OR player_two_user_id=?)
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (int(user_id), int(user_id)),
    )
    if not row:
        return None
    return get_match_detail(int(row["id"]), int(user_id))


def _assert_match_participant(match_row: dict[str, Any], user_id: int) -> None:
    if int(user_id) not in {int(match_row["player_one_user_id"]), int(match_row["player_two_user_id"])}:
        raise HTTPException(status_code=403, detail="Only match participants can access this match")


def get_match_detail(match_id: int, user_id: int) -> dict[str, Any]:
    row = _db_query_one("SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    match_row = dict(row)
    _assert_match_participant(match_row, int(user_id))
    moves = _db_query_all(
        """
        SELECT move_number, actor_user_id, move_type, move_payload_json, created_at
        FROM game_match_moves
        WHERE match_id=?
        ORDER BY move_number ASC
        """,
        (int(match_id),),
    )
    reward_contract = None
    public_notification = None
    opponent_user_id = int(match_row["player_one_user_id"]) if int(match_row["player_two_user_id"]) == int(user_id) else int(match_row["player_two_user_id"])
    opponent = _user_row(opponent_user_id) or {"email": f"user{opponent_user_id}@example.com"}
    match_state = _load_json(match_row.get("match_state_json"), {})
    result_summary = _load_json(match_row.get("result_summary"), match_state.get("result_summary"))
    if _normalize_match_status(match_row["status"]) in {"completed", "forfeited"}:
        if int(user_id) == int(match_row.get("winner_user_id") or 0):
            reward_contract = build_reward_contract(get_progression_for_user(int(user_id)), int(match_row.get("winner_xp_awarded") or 0))
        elif int(user_id) == int(match_row.get("loser_user_id") or 0):
            reward_contract = build_reward_contract(get_progression_for_user(int(user_id)), int(match_row.get("loser_xp_awarded") or 0))
        public_notification = _maybe_publish_match_result(match_row)
    return {
        "ok": True,
        "match": {
            "id": int(match_row["id"]),
            "challenge_id": int(match_row["challenge_id"]) if match_row.get("challenge_id") is not None else None,
            "source_challenge_id": int(match_row["source_challenge_id"]) if match_row.get("source_challenge_id") is not None else (int(match_row["challenge_id"]) if match_row.get("challenge_id") is not None else None),
            "game_type": match_row["game_type"],
            "game_key": match_row["game_type"],
            "status": _normalize_match_status(match_row["status"]),
            "challenger_user_id": int(match_row.get("challenger_user_id") or match_row.get("player_one_user_id")),
            "challenged_user_id": int(match_row.get("challenged_user_id") or match_row.get("player_two_user_id")),
            "player_one_user_id": int(match_row["player_one_user_id"]),
            "player_two_user_id": int(match_row["player_two_user_id"]),
            "current_turn_user_id": int(match_row["current_turn_user_id"]) if match_row.get("current_turn_user_id") is not None else None,
            "opponent_user_id": opponent_user_id,
            "opponent_display_name": _display_name_for_user(opponent),
            "winner_user_id": int(match_row["winner_user_id"]) if match_row.get("winner_user_id") is not None else None,
            "loser_user_id": int(match_row["loser_user_id"]) if match_row.get("loser_user_id") is not None else None,
            "winner_xp_awarded": int(match_row.get("winner_xp_awarded") or 0),
            "loser_xp_awarded": int(match_row.get("loser_xp_awarded") or 0),
            "created_at": _iso(match_row.get("created_at")),
            "updated_at": _iso(match_row.get("updated_at")),
            "completed_at": _iso(match_row.get("completed_at")),
            "match_state": match_state,
            "result_summary": result_summary,
            "moves": [
                {
                    "move_number": int(dict(move)["move_number"]),
                    "actor_user_id": int(dict(move)["actor_user_id"]),
                    "move_type": dict(move)["move_type"],
                    "move_payload": _load_json(dict(move).get("move_payload_json"), {}),
                    "created_at": _iso(dict(move).get("created_at")),
                }
                for move in moves
            ],
        },
        "reward_contract": reward_contract,
        "public_notification": public_notification,
    }


def submit_move(match_id: int, actor_user_id: int, move: dict[str, Any]) -> dict[str, Any]:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            match_row = _query_one_cur(cur, "SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_id),))
            if not match_row:
                raise HTTPException(status_code=404, detail="Match not found")
            _assert_match_participant(match_row, int(actor_user_id))
            if match_row["status"] != "active":
                raise HTTPException(status_code=409, detail="Match is no longer active")
            if int(match_row["current_turn_user_id"] or 0) != int(actor_user_id):
                raise HTTPException(status_code=409, detail="It is not your turn")
            state = _load_json(match_row.get("match_state_json"), {})
            if match_row["game_type"] == "dominoes":
                state, outcome = apply_dominoes_move(
                    state,
                    player_one_user_id=int(match_row["player_one_user_id"]),
                    player_two_user_id=int(match_row["player_two_user_id"]),
                    actor_user_id=int(actor_user_id),
                    move=move,
                )
            elif match_row["game_type"] == "billiards":
                state, outcome = _apply_billiards_move(match_row, state, int(actor_user_id), move)
            else:
                raise HTTPException(status_code=400, detail="Unsupported game type")
            next_move_number_row = _query_one_cur(cur, "SELECT COALESCE(MAX(move_number), 0) AS max_move FROM game_match_moves WHERE match_id=?", (int(match_id),))
            next_move_number = int(next_move_number_row["max_move"] or 0) + 1
            move_payload = dict(move)
            move_payload["resolved_state_turn_user_id"] = state.get("turn_user_id")
            _insert_and_get_id(
                cur,
                """
                INSERT INTO game_match_moves(match_id, move_number, actor_user_id, move_type, move_payload_json, created_at)
                VALUES(?,?,?,?,?,?)
                """,
                (int(match_id), next_move_number, int(actor_user_id), str(move.get("move_type") or ""), _dump_json(move_payload), _now_ts()),
            )
            reward_contracts: dict[int, dict[str, Any]] = {}
            public_notification = None
            if outcome.get("completed"):
                state["completed"] = True
                match_row, reward_contracts, public_notification = _finalize_match(
                    cur,
                    match_row,
                    state,
                    int(outcome["winner_user_id"]),
                    int(outcome["loser_user_id"]),
                    status="completed",
                )
            else:
                _exec_cur(
                    cur,
                    "UPDATE game_matches SET current_turn_user_id=?, match_state_json=?, result_summary=?, updated_at=? WHERE id=?",
                    (state.get("turn_user_id"), _dump_json(state), _dump_json(state.get("result_summary")), _now_ts(), int(match_id)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    response = get_match_detail(int(match_id), int(actor_user_id))
    if reward_contracts:
        response["reward_contract"] = reward_contracts.get(int(actor_user_id))
    if public_notification:
        response["public_notification"] = public_notification
    return response


def forfeit_match(match_id: int, actor_user_id: int) -> dict[str, Any]:
    public_notification = None
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            match_row = _query_one_cur(cur, "SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_id),))
            if not match_row:
                raise HTTPException(status_code=404, detail="Match not found")
            _assert_match_participant(match_row, int(actor_user_id))
            if match_row["status"] != "active":
                raise HTTPException(status_code=409, detail="Match is no longer active")
            loser = int(actor_user_id)
            winner = _other_player_id(match_row, actor_user_id)
            state = _load_json(match_row.get("match_state_json"), {})
            state["result_summary"] = {"reason": "forfeit", "forfeiting_user_id": loser}
            _updated_match, _reward_contracts, public_notification = _finalize_match(cur, match_row, state, winner, loser, status="forfeited")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    response = get_match_detail(int(match_id), int(actor_user_id))
    if public_notification:
        response["public_notification"] = public_notification
    return response


def get_recent_battles_for_user(user_id: int, limit: int = MAX_RECENT_BATTLES) -> list[dict[str, Any]]:
    safe_limit = max(1, min(MAX_RECENT_BATTLES, int(limit)))
    rows = _db_query_all(
        """
        SELECT *
        FROM game_matches
        WHERE status IN ('completed', 'forfeited') AND (winner_user_id=? OR loser_user_id=?)
        ORDER BY completed_at DESC, id DESC
        LIMIT ?
        """,
        (int(user_id), int(user_id), safe_limit),
    )
    user_ids = set()
    for row in rows:
        user_ids.add(int(row["player_one_user_id"]))
        user_ids.add(int(row["player_two_user_id"]))
    names = {uid: _display_name_for_user(_user_row(uid) or {"email": f"user{uid}@example.com"}) for uid in user_ids}
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if int(item.get("winner_user_id") or 0) == int(user_id):
            result = "win"
            opponent_id = int(item["loser_user_id"])
            xp_awarded = int(item.get("winner_xp_awarded") or 0)
        else:
            result = "loss"
            opponent_id = int(item["winner_user_id"])
            xp_awarded = int(item.get("loser_xp_awarded") or 0)
        items.append(
            {
                "match_id": int(item["id"]),
                "game_type": item["game_type"],
                "game_key": item["game_type"],
                "result": result,
                "opponent_user_id": opponent_id,
                "opponent_display_name": names.get(opponent_id, f"Driver {opponent_id}"),
                "xp_awarded": xp_awarded,
                "xp_delta": xp_awarded,
                "completed_at": _iso(item.get("completed_at")),
            }
        )
    return items


def get_game_battle_stats_for_users(user_ids: list[int]) -> dict[int, dict[str, Any]]:
    clean_user_ids = [int(uid) for uid in user_ids]
    if not clean_user_ids:
        return {}
    placeholders = ",".join(["?" for _ in clean_user_ids])
    rows = _db_query_all(
        f"""
        SELECT
          user_id,
          total_matches,
          total_wins,
          total_losses,
          dominoes_wins,
          dominoes_losses,
          billiards_wins,
          billiards_losses,
          game_xp_earned
        FROM user_game_stats
        WHERE user_id IN ({placeholders})
        """,
        tuple(clean_user_ids),
    )
    result = {
        uid: {
            "total_matches": 0,
            "wins": 0,
            "losses": 0,
            "matches_played": 0,
            "win_rate": 0.0,
            "dominoes_wins": 0,
            "dominoes_losses": 0,
            "billiards_wins": 0,
            "billiards_losses": 0,
            "game_xp_earned": 0,
        }
        for uid in clean_user_ids
    }
    seen_user_ids: set[int] = set()
    for row in rows:
        item = dict(row)
        uid = int(item["user_id"])
        seen_user_ids.add(uid)
        matches_played = int(item.get("total_matches") or 0)
        wins = int(item.get("total_wins") or 0)
        losses = int(item.get("total_losses") or 0)
        result[uid] = {
            "total_matches": matches_played,
            "wins": wins,
            "losses": losses,
            "matches_played": matches_played,
            "win_rate": round((wins / matches_played) if matches_played else 0.0, 4),
            "dominoes_wins": int(item.get("dominoes_wins") or 0),
            "dominoes_losses": int(item.get("dominoes_losses") or 0),
            "billiards_wins": int(item.get("billiards_wins") or 0),
            "billiards_losses": int(item.get("billiards_losses") or 0),
            "game_xp_earned": int(item.get("game_xp_earned") or 0),
        }
    missing_user_ids = [uid for uid in clean_user_ids if uid not in seen_user_ids]
    if missing_user_ids:
        fallback_placeholders = ",".join(["?" for _ in missing_user_ids])
        fallback_rows = _db_query_all(
            f"""
            SELECT user_id,
                   COUNT(*) AS matches_played,
                   SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) AS losses,
                   SUM(CASE WHEN result='win' AND game_type='dominoes' THEN 1 ELSE 0 END) AS dominoes_wins,
                   SUM(CASE WHEN result='loss' AND game_type='dominoes' THEN 1 ELSE 0 END) AS dominoes_losses,
                   SUM(CASE WHEN result='win' AND game_type='billiards' THEN 1 ELSE 0 END) AS billiards_wins,
                   SUM(CASE WHEN result='loss' AND game_type='billiards' THEN 1 ELSE 0 END) AS billiards_losses,
                   COALESCE(SUM(xp_awarded), 0) AS game_xp_earned
            FROM (
              SELECT winner_user_id AS user_id, 'win' AS result, game_type, winner_xp_awarded AS xp_awarded
              FROM game_matches WHERE status IN ('completed', 'forfeited') AND winner_user_id IS NOT NULL
              UNION ALL
              SELECT loser_user_id AS user_id, 'loss' AS result, game_type, loser_xp_awarded AS xp_awarded
              FROM game_matches WHERE status IN ('completed', 'forfeited') AND loser_user_id IS NOT NULL
            ) battle_rows
            WHERE user_id IN ({fallback_placeholders})
            GROUP BY user_id
            """,
            tuple(missing_user_ids),
        )
        for row in fallback_rows:
            item = dict(row)
            uid = int(item["user_id"])
            matches_played = int(item.get("matches_played") or 0)
            wins = int(item.get("wins") or 0)
            losses = int(item.get("losses") or 0)
            result[uid] = {
                "total_matches": matches_played,
                "wins": wins,
                "losses": losses,
                "matches_played": matches_played,
                "win_rate": round((wins / matches_played) if matches_played else 0.0, 4),
                "dominoes_wins": int(item.get("dominoes_wins") or 0),
                "dominoes_losses": int(item.get("dominoes_losses") or 0),
                "billiards_wins": int(item.get("billiards_wins") or 0),
                "billiards_losses": int(item.get("billiards_losses") or 0),
                "game_xp_earned": int(item.get("game_xp_earned") or 0),
            }
    return result


def get_profile_game_context(viewer_user_id: int, target_user_id: int) -> dict[str, Any]:
    viewer_user_id = int(viewer_user_id)
    target_user_id = int(target_user_id)
    if viewer_user_id == target_user_id:
        active_match = get_active_match_for_user(viewer_user_id)
        relationship = {"status": "active_match" if active_match else "none", "game_type": active_match["match"]["game_type"] if active_match else None, "challenge_id": active_match["match"].get("challenge_id") if active_match else None, "match_id": active_match["match"]["id"] if active_match else None}
        return {
            "active_match_summary": active_match["match"] if active_match else None,
            "challenge_state_with_viewer": "active" if active_match else "none",
            "viewer_game_relationship": relationship,
        }
    active_match_row = _db_query_one(
        """
        SELECT * FROM game_matches
        WHERE status='active'
          AND ((player_one_user_id=? AND player_two_user_id=?) OR (player_one_user_id=? AND player_two_user_id=?))
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (viewer_user_id, target_user_id, target_user_id, viewer_user_id),
    )
    if active_match_row:
        active_match_summary = get_match_detail(int(active_match_row["id"]), viewer_user_id)["match"]
        return {
            "active_match_summary": active_match_summary,
            "challenge_state_with_viewer": "active",
            "viewer_game_relationship": {
                "status": "active_match",
                "game_type": active_match_summary["game_type"],
                "challenge_id": active_match_summary.get("challenge_id"),
                "match_id": active_match_summary["id"],
            },
        }
    pending = _db_query_one(
        """
        SELECT * FROM game_challenges
        WHERE status='pending'
          AND ((challenger_user_id=? AND challenged_user_id=?) OR (challenger_user_id=? AND challenged_user_id=?))
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (viewer_user_id, target_user_id, target_user_id, viewer_user_id),
    )
    if not pending:
        return {
            "active_match_summary": None,
            "challenge_state_with_viewer": "none",
            "viewer_game_relationship": {"status": "none", "game_type": None, "challenge_id": None, "match_id": None},
        }
    incoming = int(pending["challenged_user_id"]) == viewer_user_id
    status = "incoming_challenge" if incoming else "outgoing_challenge"
    return {
        "active_match_summary": None,
        "challenge_state_with_viewer": "incoming" if incoming else "outgoing",
        "viewer_game_relationship": {
            "status": status,
            "game_type": pending["game_type"],
            "challenge_id": int(pending["id"]),
            "match_id": None,
        },
    }


def get_history_for_user(user_id: int) -> dict[str, Any]:
    return {"ok": True, "items": get_recent_battles_for_user(int(user_id), limit=MAX_RECENT_BATTLES)}
