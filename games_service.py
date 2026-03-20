from __future__ import annotations

import json
import math
import random
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import HTTPException

from avatar_assets import avatar_thumb_url, avatar_version_for_data_url
from chat import publish_public_battle_chat_message, publish_public_battle_notification
from core import DB_BACKEND, _clean_display_name, _db, _db_lock, _db_query_all, _db_query_one, _sql
from leaderboard_service import (
    build_reward_contract,
    get_best_current_badges_for_users,
    get_progression_for_user,
    get_progression_snapshot_for_total_xp,
)

ALLOWED_GAME_TYPES = {"dominoes", "billiards"}
CHALLENGE_EXPIRATION_SECONDS = 15 * 60
WINNER_XP_AWARD = 60
LOSER_XP_AWARD = 20
MAX_RECENT_BATTLES = 5
MAX_CHALLENGEABLE_USERS = 100
DOMINO_SET: list[tuple[int, int]] = [(a, b) for a in range(7) for b in range(a, 7)]
BILLIARDS_TARGETS_TO_CLEAR = 3


def _bool_db_value(flag: bool):
    if DB_BACKEND == "postgres":
        return bool(flag)
    return 1 if flag else 0


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
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _dump_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _query_one_cur(cur, sql: str, params: tuple = ()) -> Optional[dict]:
    cur.execute(_sql(sql), params)
    row = cur.fetchone()
    return dict(row) if row else None


def _query_all_cur(cur, sql: str, params: tuple = ()) -> List[dict]:
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


def _user_row(user_id: int, cur=None) -> Optional[dict]:
    sql = "SELECT id, email, display_name, is_disabled, is_suspended FROM users WHERE id=? LIMIT 1"
    row = _query_one_cur(cur, sql, (int(user_id),)) if cur is not None else _db_query_one(sql, (int(user_id),))
    return dict(row) if row else None


def _display_name_for_user(row: dict[str, Any]) -> str:
    return _clean_display_name((row.get("display_name") or "").strip(), row.get("email") or "Driver")


def ensure_games_schema() -> None:
    if DB_BACKEND == "postgres":
        conn = _db()
        try:
            cur = conn.cursor()
            statements = [
                """
                CREATE TABLE IF NOT EXISTS game_challenges (
                  id BIGSERIAL PRIMARY KEY,
                  game_type TEXT NOT NULL,
                  challenger_user_id BIGINT NOT NULL,
                  challenged_user_id BIGINT NOT NULL,
                  status TEXT NOT NULL,
                  created_at BIGINT NOT NULL,
                  updated_at BIGINT NOT NULL,
                  expires_at BIGINT NOT NULL,
                  responded_at BIGINT,
                  accepted_at BIGINT,
                  cancelled_at BIGINT,
                  declined_at BIGINT,
                  completed_match_id BIGINT,
                  FOREIGN KEY(challenger_user_id) REFERENCES users(id),
                  FOREIGN KEY(challenged_user_id) REFERENCES users(id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS game_matches (
                  id BIGSERIAL PRIMARY KEY,
                  challenge_id BIGINT,
                  game_type TEXT NOT NULL,
                  player_one_user_id BIGINT NOT NULL,
                  player_two_user_id BIGINT NOT NULL,
                  current_turn_user_id BIGINT,
                  status TEXT NOT NULL,
                  winner_user_id BIGINT,
                  loser_user_id BIGINT,
                  winner_xp_awarded INTEGER NOT NULL DEFAULT 0,
                  loser_xp_awarded INTEGER NOT NULL DEFAULT 0,
                  match_state_json TEXT NOT NULL,
                  created_at BIGINT NOT NULL,
                  updated_at BIGINT NOT NULL,
                  completed_at BIGINT,
                  reward_announced_at BIGINT,
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
                "CREATE INDEX IF NOT EXISTS idx_game_challenges_incoming ON game_challenges(challenged_user_id, status, created_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_game_challenges_outgoing ON game_challenges(challenger_user_id, status, created_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_game_challenges_pending_pairs ON game_challenges(game_type, challenger_user_id, challenged_user_id, status)",
                "CREATE INDEX IF NOT EXISTS idx_game_matches_p1_status ON game_matches(player_one_user_id, status, updated_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_game_matches_p2_status ON game_matches(player_two_user_id, status, updated_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_game_match_moves_lookup ON game_match_moves(match_id, move_number)",
                "CREATE INDEX IF NOT EXISTS idx_game_matches_completed_at ON game_matches(completed_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_game_matches_game_type ON game_matches(game_type, status)",
            ]
            for statement in statements:
                cur.execute(statement)
            conn.commit()
        finally:
            conn.close()
        return

    conn = _db()
    try:
        cur = conn.cursor()
        statements = [
            """
            CREATE TABLE IF NOT EXISTS game_challenges (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              game_type TEXT NOT NULL,
              challenger_user_id INTEGER NOT NULL,
              challenged_user_id INTEGER NOT NULL,
              status TEXT NOT NULL,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              expires_at INTEGER NOT NULL,
              responded_at INTEGER,
              accepted_at INTEGER,
              cancelled_at INTEGER,
              declined_at INTEGER,
              completed_match_id INTEGER,
              FOREIGN KEY(challenger_user_id) REFERENCES users(id),
              FOREIGN KEY(challenged_user_id) REFERENCES users(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS game_matches (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              challenge_id INTEGER,
              game_type TEXT NOT NULL,
              player_one_user_id INTEGER NOT NULL,
              player_two_user_id INTEGER NOT NULL,
              current_turn_user_id INTEGER,
              status TEXT NOT NULL,
              winner_user_id INTEGER,
              loser_user_id INTEGER,
              winner_xp_awarded INTEGER NOT NULL DEFAULT 0,
              loser_xp_awarded INTEGER NOT NULL DEFAULT 0,
              match_state_json TEXT NOT NULL,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              completed_at INTEGER,
              reward_announced_at INTEGER,
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
            "CREATE INDEX IF NOT EXISTS idx_game_challenges_incoming ON game_challenges(challenged_user_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_challenges_outgoing ON game_challenges(challenger_user_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_challenges_pending_pairs ON game_challenges(game_type, challenger_user_id, challenged_user_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_game_matches_p1_status ON game_matches(player_one_user_id, status, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_matches_p2_status ON game_matches(player_two_user_id, status, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_match_moves_lookup ON game_match_moves(match_id, move_number)",
            "CREATE INDEX IF NOT EXISTS idx_game_matches_completed_at ON game_matches(completed_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_game_matches_game_type ON game_matches(game_type, status)",
        ]
        for statement in statements:
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
    return {
        "id": int(row["id"]),
        "game_type": row["game_type"],
        "game_key": row["game_type"],
        "status": row["status"],
        "challenger_user_id": challenger_user_id,
        "challenger_display_name": row["challenger_display_name"],
        "challenged_user_id": challenged_user_id,
        "challenged_display_name": row["challenged_display_name"],
        "other_user_id": other_user_id,
        "other_user_display_name": other_user_display_name,
        "opponent_user_id": other_user_id,
        "opponent_display_name": other_user_display_name,
        "created_at": _iso(row.get("created_at")),
        "updated_at": _iso(row.get("updated_at")),
        "expires_at": _iso(row.get("expires_at")),
        "responded_at": _iso(row.get("responded_at")),
        "accepted_at": _iso(row.get("accepted_at")),
        "cancelled_at": _iso(row.get("cancelled_at")),
        "declined_at": _iso(row.get("declined_at")),
        "completed_match_id": int(row["completed_match_id"]) if row.get("completed_match_id") is not None else None,
    }


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
            active_match = _query_one_cur(
                cur,
                """
                SELECT id FROM game_matches
                WHERE status='active'
                  AND ((player_one_user_id=? AND player_two_user_id=?) OR (player_one_user_id=? AND player_two_user_id=?))
                LIMIT 1
                """,
                (low_id, high_id, high_id, low_id),
            )
            if active_match:
                raise HTTPException(status_code=409, detail="These players already have an active match")
            now = _now_ts()
            challenge_id = _insert_and_get_id(
                cur,
                """
                INSERT INTO game_challenges(
                  game_type, challenger_user_id, challenged_user_id, status,
                  created_at, updated_at, expires_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    normalized_game_type,
                    int(challenger_user_id),
                    int(target_user_id),
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
                "updated_at": _iso(now),
                "expires_at": _iso(now + CHALLENGE_EXPIRATION_SECONDS),
                "responded_at": None,
                "accepted_at": None,
                "cancelled_at": None,
                "declined_at": None,
                "completed_match_id": None,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _serialize_match_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "game_type": row["game_type"],
        "game_key": row["game_type"],
        "status": row["status"],
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


def _upsert_user_game_stats(
    cur,
    *,
    user_id: int,
    is_win: bool,
    game_type: str,
    xp_earned: int,
) -> None:
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
    progressions = {uid: get_progression_for_user(uid) for uid in clean_user_ids}
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        uid = int(item["id"])
        progression = progressions.get(uid, {})
        presence_updated_at = item.get("presence_updated_at")
        display_name = _display_name_for_user(item)
        avatar_thumb = None
        if item.get("avatar_url"):
            avatar_thumb = avatar_thumb_url(uid, item.get("avatar_version") or avatar_version_for_data_url(str(item.get("avatar_url") or "")))
        items.append(
            {
                "user_id": uid,
                "display_name": display_name,
                "avatar_thumb_url": avatar_thumb,
                "avatar_url": avatar_thumb,
                "avatar_version": item.get("avatar_version") or avatar_version_for_data_url(str(item.get("avatar_url") or "")),
                "level": int(progression.get("level") or 1),
                "rank_icon_key": str(progression.get("rank_icon_key") or "band_001"),
                "leaderboard_badge_code": (badges.get(uid) or {}).get("leaderboard_badge_code"),
                "online": bool(presence_updated_at is not None and int(presence_updated_at) >= cutoff),
                "presence_updated_at": int(presence_updated_at) if presence_updated_at is not None else None,
            }
        )
    return items


def list_challenges_for_user(user_id: int) -> dict[str, Any]:
    expire_stale_challenges()
    incoming_rows = _db_query_all(
        """
        SELECT c.*, cu.email AS challenger_email, cu.display_name AS challenger_display_name_raw,
               tu.email AS challenged_email, tu.display_name AS challenged_display_name_raw
        FROM game_challenges c
        JOIN users cu ON cu.id = c.challenger_user_id
        JOIN users tu ON tu.id = c.challenged_user_id
        WHERE c.challenged_user_id=? AND c.status='pending'
        ORDER BY c.created_at DESC, c.id DESC
        """,
        (int(user_id),),
    )
    outgoing_rows = _db_query_all(
        """
        SELECT c.*, cu.email AS challenger_email, cu.display_name AS challenger_display_name_raw,
               tu.email AS challenged_email, tu.display_name AS challenged_display_name_raw
        FROM game_challenges c
        JOIN users cu ON cu.id = c.challenger_user_id
        JOIN users tu ON tu.id = c.challenged_user_id
        WHERE c.challenger_user_id=? AND c.status='pending'
        ORDER BY c.created_at DESC, c.id DESC
        """,
        (int(user_id),),
    )
    active_match_row = _db_query_one(
        """
        SELECT * FROM game_matches
        WHERE status='active' AND (player_one_user_id=? OR player_two_user_id=?)
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (int(user_id), int(user_id)),
    )

    def _with_names(row: Any) -> dict[str, Any]:
        item = dict(row)
        item["challenger_display_name"] = _clean_display_name(item.get("challenger_display_name_raw") or "", item.get("challenger_email") or "Driver")
        item["challenged_display_name"] = _clean_display_name(item.get("challenged_display_name_raw") or "", item.get("challenged_email") or "Driver")
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


def _seed_for_match(match_id: int) -> int:
    return 7000 + int(match_id) * 17


def _normalize_tile(tile: Iterable[int]) -> tuple[int, int]:
    values = list(tile)
    if len(values) != 2:
        raise HTTPException(status_code=400, detail="Domino tile must contain two pips")
    a = int(values[0])
    b = int(values[1])
    if not (0 <= a <= 6 and 0 <= b <= 6):
        raise HTTPException(status_code=400, detail="Domino tile pips must be between 0 and 6")
    return tuple(sorted((a, b)))


def _dominoes_state(player_one_user_id: int, player_two_user_id: int, match_id: int) -> dict[str, Any]:
    deck = DOMINO_SET.copy()
    random.Random(_seed_for_match(match_id)).shuffle(deck)
    player_one_hand = [list(tile) for tile in deck[:7]]
    player_two_hand = [list(tile) for tile in deck[7:14]]
    stock = [list(tile) for tile in deck[14:]]
    first_turn = min(int(player_one_user_id), int(player_two_user_id))
    return {
        "game_type": "dominoes",
        "rules": "Double-six draw dominoes. Draw when blocked, pass only when no legal move and the boneyard is empty. Lower hand pip total wins blocked rounds.",
        "board": [],
        "left_end": None,
        "right_end": None,
        "hands": {
            str(int(player_one_user_id)): player_one_hand,
            str(int(player_two_user_id)): player_two_hand,
        },
        "stock": stock,
        "passes_in_row": 0,
        "last_action": None,
        "turn_user_id": first_turn,
        "result_summary": None,
    }


def _tile_in_hand(hand: list[list[int]], tile: tuple[int, int]) -> bool:
    return list(tile) in hand


def _domino_legal_play_sides(state: dict[str, Any], tile: tuple[int, int]) -> set[str]:
    board = state.get("board") or []
    if not board:
        return {"left", "right"}
    left_end = int(state["left_end"])
    right_end = int(state["right_end"])
    sides: set[str] = set()
    a, b = tile
    if a == left_end or b == left_end:
        sides.add("left")
    if a == right_end or b == right_end:
        sides.add("right")
    return sides


def _orient_tile_for_side(tile: tuple[int, int], side: str, state: dict[str, Any]) -> list[int]:
    a, b = tile
    if not state.get("board"):
        return [a, b]
    if side == "left":
        left_end = int(state["left_end"])
        return [b, a] if a == left_end else [a, b]
    right_end = int(state["right_end"])
    return [a, b] if a == right_end else [b, a]


def _domino_hand_total(hand: list[list[int]]) -> int:
    return sum(int(tile[0]) + int(tile[1]) for tile in hand)


def _dominoes_player_has_legal_play(state: dict[str, Any], user_id: int) -> bool:
    hand = state.get("hands", {}).get(str(int(user_id)), [])
    return any(_domino_legal_play_sides(state, tuple(tile)) for tile in hand)


def _dominoes_finalize_blocked(match_row: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    player_one = int(match_row["player_one_user_id"])
    player_two = int(match_row["player_two_user_id"])
    hand_one = state["hands"].get(str(player_one), [])
    hand_two = state["hands"].get(str(player_two), [])
    total_one = _domino_hand_total(hand_one)
    total_two = _domino_hand_total(hand_two)
    if total_one < total_two:
        winner, loser = player_one, player_two
    elif total_two < total_one:
        winner, loser = player_two, player_one
    else:
        winner, loser = min(player_one, player_two), max(player_one, player_two)
    state["result_summary"] = {
        "reason": "blocked",
        "player_one_pips": total_one,
        "player_two_pips": total_two,
    }
    return {"winner_user_id": winner, "loser_user_id": loser, "state": state}


def _apply_dominoes_move(match_row: dict[str, Any], state: dict[str, Any], actor_user_id: int, move: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    move_type = str(move.get("move_type") or "").strip().lower()
    actor_key = str(int(actor_user_id))
    hand = list(state.get("hands", {}).get(actor_key, []))
    if move_type == "play_tile":
        tile = _normalize_tile(move.get("tile") or [])
        side = str(move.get("side") or "").strip().lower()
        if side not in {"left", "right"}:
            raise HTTPException(status_code=400, detail="Dominoes play_tile requires side=left or side=right")
        if not _tile_in_hand(hand, tile):
            raise HTTPException(status_code=409, detail="Tile is not in your hand")
        legal_sides = _domino_legal_play_sides(state, tile)
        if side not in legal_sides:
            raise HTTPException(status_code=409, detail="That tile cannot be played on the requested side")
        oriented = _orient_tile_for_side(tile, side, state)
        hand.remove(list(tile))
        board = list(state.get("board") or [])
        if not board:
            board = [oriented]
        elif side == "left":
            board.insert(0, oriented)
        else:
            board.append(oriented)
        state["board"] = board
        state["left_end"] = int(board[0][0])
        state["right_end"] = int(board[-1][1])
        state["hands"][actor_key] = hand
        state["passes_in_row"] = 0
        state["last_action"] = {"type": "play_tile", "tile": oriented, "side": side, "actor_user_id": int(actor_user_id)}
        if not hand:
            state["result_summary"] = {"reason": "emptied_hand"}
            other = int(match_row["player_one_user_id"]) if int(match_row["player_two_user_id"]) == int(actor_user_id) else int(match_row["player_two_user_id"])
            return state, {"completed": True, "winner_user_id": int(actor_user_id), "loser_user_id": other}
        next_turn = int(match_row["player_one_user_id"]) if int(match_row["player_two_user_id"]) == int(actor_user_id) else int(match_row["player_two_user_id"])
        state["turn_user_id"] = next_turn
        return state, {"completed": False}

    if move_type == "draw_tile":
        if _dominoes_player_has_legal_play(state, actor_user_id):
            raise HTTPException(status_code=409, detail="You already have a legal dominoes play")
        stock = list(state.get("stock") or [])
        if not stock:
            raise HTTPException(status_code=409, detail="Boneyard is empty")
        drawn_tile = stock.pop(0)
        hand.append(drawn_tile)
        state["stock"] = stock
        state["hands"][actor_key] = hand
        state["last_action"] = {"type": "draw_tile", "tile": drawn_tile, "actor_user_id": int(actor_user_id)}
        return state, {"completed": False}

    if move_type == "pass":
        if state.get("stock"):
            raise HTTPException(status_code=409, detail="You cannot pass while the boneyard still has tiles")
        if _dominoes_player_has_legal_play(state, actor_user_id):
            raise HTTPException(status_code=409, detail="You still have a legal dominoes play")
        state["passes_in_row"] = int(state.get("passes_in_row") or 0) + 1
        state["last_action"] = {"type": "pass", "actor_user_id": int(actor_user_id)}
        next_turn = int(match_row["player_one_user_id"]) if int(match_row["player_two_user_id"]) == int(actor_user_id) else int(match_row["player_two_user_id"])
        state["turn_user_id"] = next_turn
        if int(state["passes_in_row"]) >= 2:
            blocked = _dominoes_finalize_blocked(match_row, state)
            return blocked["state"], {"completed": True, "winner_user_id": blocked["winner_user_id"], "loser_user_id": blocked["loser_user_id"]}
        return state, {"completed": False}

    raise HTTPException(status_code=400, detail="Unsupported dominoes move")


def _billiards_state(player_one_user_id: int, player_two_user_id: int) -> dict[str, Any]:
    first_turn = min(int(player_one_user_id), int(player_two_user_id))
    return {
        "game_type": "billiards",
        "rules": "Quick Battle rules: each player must pocket 3 target balls, then pocket the final black ball. Scoring shots keep the turn; misses hand the turn over.",
        "turn_user_id": first_turn,
        "players": {
            str(int(player_one_user_id)): {"targets_remaining": BILLIARDS_TARGETS_TO_CLEAR, "targets_cleared": 0, "black_unlocked": False},
            str(int(player_two_user_id)): {"targets_remaining": BILLIARDS_TARGETS_TO_CLEAR, "targets_cleared": 0, "black_unlocked": False},
        },
        "table": {
            "width": 100,
            "height": 50,
            "cue_ball": {"x": 18, "y": 25},
            "final_ball": {"x": 82, "y": 25, "pocketed": False},
        },
        "last_shot": None,
        "result_summary": None,
    }


def _billiards_switch_turn(match_row: dict[str, Any], actor_user_id: int) -> int:
    return int(match_row["player_one_user_id"]) if int(match_row["player_two_user_id"]) == int(actor_user_id) else int(match_row["player_two_user_id"])


def _apply_billiards_move(match_row: dict[str, Any], state: dict[str, Any], actor_user_id: int, move: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
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

    actor_state = state["players"][str(int(actor_user_id))]
    shot_quality = (abs(math.sin(angle)) * 0.45) + (abs(math.cos(angle * 0.5)) * 0.25) + (power * 0.55)
    control_bonus = max(0.0, 0.25 - abs(power - 0.72))
    total_score = shot_quality + control_bonus
    pocketed_targets = 0
    pocketed_final = False

    if int(actor_state["targets_remaining"]) > 0:
        if total_score >= 0.86:
            pocketed_targets = 1
            actor_state["targets_remaining"] = max(0, int(actor_state["targets_remaining"]) - 1)
            actor_state["targets_cleared"] = int(actor_state["targets_cleared"]) + 1
            actor_state["black_unlocked"] = int(actor_state["targets_remaining"]) == 0
    else:
        if total_score >= 0.93:
            pocketed_final = True
            state["table"]["final_ball"]["pocketed"] = True

    state["last_shot"] = {
        "actor_user_id": int(actor_user_id),
        "angle": angle,
        "power": power,
        "pocketed_targets": pocketed_targets,
        "pocketed_final": pocketed_final,
        "score_metric": round(total_score, 4),
    }

    if pocketed_final:
        state["result_summary"] = {"reason": "final_ball_pocketed", "score_metric": round(total_score, 4)}
        loser = _billiards_switch_turn(match_row, actor_user_id)
        return state, {"completed": True, "winner_user_id": int(actor_user_id), "loser_user_id": loser}

    if pocketed_targets > 0:
        state["turn_user_id"] = int(actor_user_id)
    else:
        state["turn_user_id"] = _billiards_switch_turn(match_row, actor_user_id)
    return state, {"completed": False}


def _create_initial_match_state(game_type: str, player_one_user_id: int, player_two_user_id: int, match_id: int) -> dict[str, Any]:
    if game_type == "dominoes":
        return _dominoes_state(player_one_user_id, player_two_user_id, match_id)
    if game_type == "billiards":
        return _billiards_state(player_one_user_id, player_two_user_id)
    raise HTTPException(status_code=400, detail="Unsupported game type")


def _finalize_match(cur, match_row: dict[str, Any], state: dict[str, Any], winner_user_id: int, loser_user_id: int, *, status: str) -> tuple[dict[str, Any], dict[int, dict[str, Any]]]:
    completed_at = _now_ts()
    winner_before = get_progression_for_user(int(winner_user_id))
    loser_before = get_progression_for_user(int(loser_user_id))
    _exec_cur(
        cur,
        """
        UPDATE game_matches
        SET status=?, current_turn_user_id=NULL, winner_user_id=?, loser_user_id=?,
            winner_xp_awarded=?, loser_xp_awarded=?, match_state_json=?,
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
            completed_at,
            completed_at,
            int(match_row["id"]),
        ),
    )
    updated_match = _query_one_cur(cur, "SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_row["id"]),))
    if not updated_match:
        raise HTTPException(status_code=500, detail="Failed to finalize match")
    _upsert_user_game_stats(
        cur,
        user_id=int(winner_user_id),
        is_win=True,
        game_type=str(updated_match.get("game_type") or match_row.get("game_type") or ""),
        xp_earned=WINNER_XP_AWARD,
    )
    _upsert_user_game_stats(
        cur,
        user_id=int(loser_user_id),
        is_win=False,
        game_type=str(updated_match.get("game_type") or match_row.get("game_type") or ""),
        xp_earned=LOSER_XP_AWARD,
    )
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
    return updated_match, reward_contracts


def _maybe_publish_match_result(match_row: dict[str, Any]) -> None:
    if match_row.get("reward_announced_at") is not None:
        return
    winner_id = int(match_row["winner_user_id"])
    loser_id = int(match_row["loser_user_id"])
    winner = _user_row(winner_id)
    loser = _user_row(loser_id)
    winner_progression = get_progression_for_user(winner_id)
    publish_public_battle_notification(
        {
            "match_id": int(match_row["id"]),
            "game_type": match_row["game_type"],
            "winner_user_id": winner_id,
            "winner_display_name": _display_name_for_user(winner or {"email": "winner"}),
            "loser_user_id": loser_id,
            "loser_display_name": _display_name_for_user(loser or {"email": "loser"}),
            "winner_xp_awarded": int(match_row.get("winner_xp_awarded") or 0),
            "winner_new_level": int(winner_progression.get("level") or 1),
            "completed_at": _iso(match_row.get("completed_at")),
        }
    )
    publish_public_battle_chat_message(
        author_user_id=winner_id,
        winner_display_name=_display_name_for_user(winner or {"email": "winner"}),
        loser_display_name=_display_name_for_user(loser or {"email": "loser"}),
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
                WHERE challenge_id=? OR (
                  status='active' AND
                  ((player_one_user_id=? AND player_two_user_id=?) OR (player_one_user_id=? AND player_two_user_id=?))
                )
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(challenge_id), player_one, player_two, player_two, player_one),
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
                  challenge_id, game_type, player_one_user_id, player_two_user_id,
                  current_turn_user_id, status, winner_user_id, loser_user_id,
                  match_state_json, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(challenge_id),
                    challenge["game_type"],
                    player_one,
                    player_two,
                    min(player_one, player_two),
                    "active",
                    None,
                    None,
                    "{}",
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
            status = "declined" if action == "decline" else "cancelled"
            column = "declined_at" if action == "decline" else "cancelled_at"
            _exec_cur(
                cur,
                f"UPDATE game_challenges SET status=?, updated_at=?, responded_at=?, {column}=? WHERE id=?",
                (status, now, now, now, int(challenge_id)),
            )
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
    opponent_user_id = int(match_row["player_one_user_id"]) if int(match_row["player_two_user_id"]) == int(user_id) else int(match_row["player_two_user_id"])
    opponent = _user_row(opponent_user_id) or {"email": f"user{opponent_user_id}@example.com"}
    match_state = _load_json(match_row.get("match_state_json"), {})
    if match_row["status"] in {"completed", "forfeited"}:
        if int(user_id) == int(match_row.get("winner_user_id") or 0):
            reward_contract = build_reward_contract(get_progression_for_user(int(user_id)), int(match_row.get("winner_xp_awarded") or 0))
        elif int(user_id) == int(match_row.get("loser_user_id") or 0):
            reward_contract = build_reward_contract(get_progression_for_user(int(user_id)), int(match_row.get("loser_xp_awarded") or 0))
        _maybe_publish_match_result(match_row)
    return {
        "ok": True,
        "match": {
            "id": int(match_row["id"]),
            "challenge_id": int(match_row["challenge_id"]) if match_row.get("challenge_id") is not None else None,
            "game_type": match_row["game_type"],
            "game_key": match_row["game_type"],
            "status": match_row["status"],
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
            "result_summary": match_state.get("result_summary"),
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
                state, outcome = _apply_dominoes_move(match_row, state, int(actor_user_id), move)
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
            if outcome.get("completed"):
                state["completed"] = True
                match_row, reward_contracts = _finalize_match(
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
                    "UPDATE game_matches SET current_turn_user_id=?, match_state_json=?, updated_at=? WHERE id=?",
                    (state.get("turn_user_id"), _dump_json(state), _now_ts(), int(match_id)),
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
    return response


def forfeit_match(match_id: int, actor_user_id: int) -> dict[str, Any]:
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
            winner = int(match_row["player_one_user_id"]) if int(match_row["player_two_user_id"]) == loser else int(match_row["player_two_user_id"])
            state = _load_json(match_row.get("match_state_json"), {})
            state["result_summary"] = {"reason": "forfeit", "forfeiting_user_id": loser}
            _finalize_match(cur, match_row, state, winner, loser, status="forfeited")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    return get_match_detail(int(match_id), int(actor_user_id))


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


def get_history_for_user(user_id: int) -> dict[str, Any]:
    return {"ok": True, "items": get_recent_battles_for_user(int(user_id), limit=MAX_RECENT_BATTLES)}
