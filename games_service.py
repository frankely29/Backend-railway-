from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one
from leaderboard_service import get_best_current_badges_for_users, get_progression_for_users

MATCH_TTL_SECONDS = 24 * 3600
WINNER_XP = 60
LOSER_XP = 0
_ALLOWED_GAME_TYPES = {"dominoes", "billiards"}
_GAMES_SCHEMA_LOCK = threading.Lock()
_GAMES_SCHEMA_READY = False


def _safe_iso(unix_ts: Optional[int]) -> Optional[str]:
    if unix_ts is None:
        return None
    return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).isoformat()


def _public_user_map(user_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    clean = sorted({int(uid) for uid in user_ids if uid is not None})
    if not clean:
        return {}
    placeholders = ",".join(["?" for _ in clean])
    rows = _db_query_all(
        f"""
        SELECT id, email, display_name, avatar_url, avatar_version
        FROM users
        WHERE id IN ({placeholders})
        """,
        tuple(clean),
    )
    progression = get_progression_for_users(clean)
    badges = get_best_current_badges_for_users(clean)
    out: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        raw = dict(row)
        uid = int(raw["id"])
        fallback = (raw["email"] or "Driver").split("@")[0]
        out[uid] = {
            "id": uid,
            "display_name": ((raw.get("display_name") or "").strip() or fallback)[:28],
            "avatar_url": raw.get("avatar_url"),
            "avatar_thumb_url": f"/avatars/thumb/{uid}",
            "rank_icon_key": (progression.get(uid) or {}).get("rank_icon_key", "band_001"),
            "leaderboard_badge_code": (badges.get(uid) or {}).get("leaderboard_badge_code"),
        }
    return out


def _normalize_game_type(raw: Optional[str]) -> str:
    value = str(raw or "").strip().lower()
    if value not in _ALLOWED_GAME_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported game_type")
    return value


def _challenge_row_to_payload(row: Dict[str, Any], viewer_user_id: int, user_map: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    row = dict(row)
    challenger_id = int(row["challenger_user_id"])
    challenged_id = int(row["challenged_user_id"])
    opponent_user_id = challenged_id if challenger_id == int(viewer_user_id) else challenger_id
    challenger = user_map.get(challenger_id, {"display_name": "Driver"})
    challenged = user_map.get(challenged_id, {"display_name": "Driver"})
    opponent = user_map.get(opponent_user_id, {"display_name": "Driver"})
    return {
        "id": int(row["id"]),
        "challenge_id": int(row["id"]),
        "game_type": row["game_type"],
        "game_key": row["game_type"],
        "status": row["status"],
        "challenger_user_id": challenger_id,
        "challenged_user_id": challenged_id,
        "challenger_display_name": challenger["display_name"],
        "challenged_display_name": challenged["display_name"],
        "opponent_user_id": opponent_user_id,
        "opponent_display_name": opponent["display_name"],
        "other_user_display_name": opponent["display_name"],
        "created_at": _safe_iso(int(row["created_at"])) if row.get("created_at") is not None else None,
        "expires_at": _safe_iso(int(row["expires_at"])) if row.get("expires_at") is not None else None,
    }


def _match_row_to_payload(row: Dict[str, Any], viewer_user_id: int, user_map: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    row = dict(row)
    challenger_id = int(row["challenger_user_id"])
    challenged_id = int(row["challenged_user_id"])
    opponent_id = challenged_id if challenger_id == int(viewer_user_id) else challenger_id
    opponent = user_map.get(opponent_id, {"display_name": "Driver"})
    state: Dict[str, Any] = {}
    if row.get("match_state_json"):
        try:
            state = json.loads(row["match_state_json"])
        except Exception:
            state = {}
    result_summary = row.get("result_summary")
    if isinstance(result_summary, str) and result_summary:
        try:
            result_summary = json.loads(result_summary)
        except Exception:
            result_summary = {"summary": result_summary}
    return {
        "id": int(row["id"]),
        "challenge_id": int(row["challenge_id"]) if row.get("challenge_id") is not None else None,
        "game_type": row["game_type"],
        "game_key": row["game_type"],
        "status": row["status"],
        "challenger_user_id": challenger_id,
        "challenged_user_id": challenged_id,
        "player_one_user_id": int(row["player_one_user_id"]),
        "player_two_user_id": int(row["player_two_user_id"]),
        "current_turn_user_id": int(row["current_turn_user_id"]) if row.get("current_turn_user_id") is not None else None,
        "winner_user_id": int(row["winner_user_id"]) if row.get("winner_user_id") is not None else None,
        "loser_user_id": int(row["loser_user_id"]) if row.get("loser_user_id") is not None else None,
        "winner_xp_awarded": int(row.get("winner_xp_awarded") or 0),
        "loser_xp_awarded": int(row.get("loser_xp_awarded") or 0),
        "opponent_user_id": opponent_id,
        "opponent_display_name": opponent["display_name"],
        "result_summary": result_summary,
        "state": state,
    }


def _table_columns_sqlite(table_name: str) -> set[str]:
    rows = _db_query_all(f"PRAGMA table_info({table_name})")
    return {str(r["name"] if isinstance(r, dict) else r[1]) for r in rows}


def _postgres_columns_by_table(table_names: List[str]) -> Dict[str, set[str]]:
    if not table_names:
        return {}
    placeholders = ",".join(["?" for _ in table_names])
    rows = _db_query_all(
        f"""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name IN ({placeholders})
        """,
        tuple(table_names),
    )
    out: Dict[str, set[str]] = {name: set() for name in table_names}
    for row in rows:
        raw = dict(row)
        out.setdefault(str(raw["table_name"]), set()).add(str(raw["column_name"]))
    return out


def _ensure_column_if_missing(table_name: str, existing_columns: set[str], column_name: str, ddl: str) -> None:
    if column_name in existing_columns:
        return
    _db_exec(f"ALTER TABLE {table_name} ADD COLUMN {ddl}")
    existing_columns.add(column_name)


def ensure_games_schema() -> None:
    global _GAMES_SCHEMA_READY
    if _GAMES_SCHEMA_READY:
        return
    with _GAMES_SCHEMA_LOCK:
        if _GAMES_SCHEMA_READY:
            return
        _ensure_games_schema_impl()
        _GAMES_SCHEMA_READY = True


def _ensure_games_schema_impl() -> None:
    if DB_BACKEND == "postgres":
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_challenges (
              id BIGSERIAL PRIMARY KEY,
              game_type TEXT NOT NULL,
              challenger_user_id BIGINT NOT NULL,
              challenged_user_id BIGINT NOT NULL,
              status TEXT NOT NULL,
              created_at BIGINT NOT NULL,
              updated_at BIGINT NOT NULL,
              responded_at BIGINT,
              expires_at BIGINT NOT NULL,
              accepted_match_id BIGINT
            )
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_matches (
              id BIGSERIAL PRIMARY KEY,
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
              accepted_at BIGINT,
              completed_at BIGINT,
              expires_at BIGINT
            )
            """
        )
    else:
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_challenges (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              game_type TEXT NOT NULL,
              challenger_user_id INTEGER NOT NULL,
              challenged_user_id INTEGER NOT NULL,
              status TEXT NOT NULL,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              responded_at INTEGER,
              expires_at INTEGER NOT NULL,
              accepted_match_id INTEGER
            )
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_matches (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
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
              accepted_at INTEGER,
              completed_at INTEGER,
              expires_at INTEGER
            )
            """
        )
    if DB_BACKEND == "postgres":
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_match_participants (
              id BIGSERIAL PRIMARY KEY,
              match_id BIGINT NOT NULL,
              user_id BIGINT NOT NULL,
              team_no INTEGER,
              seat_role TEXT NOT NULL DEFAULT 'solo',
              result TEXT NOT NULL DEFAULT 'pending',
              xp_awarded INTEGER NOT NULL DEFAULT 0,
              joined_at BIGINT NOT NULL
            )
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_match_moves (
              id BIGSERIAL PRIMARY KEY,
              match_id BIGINT NOT NULL,
              move_number INTEGER NOT NULL,
              actor_user_id BIGINT NOT NULL,
              move_type TEXT NOT NULL,
              move_payload_json TEXT NOT NULL,
              created_at BIGINT NOT NULL
            )
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_xp_awards (
              id BIGSERIAL PRIMARY KEY,
              match_id BIGINT NOT NULL,
              user_id BIGINT NOT NULL,
              xp_awarded INTEGER NOT NULL,
              created_at BIGINT NOT NULL
            )
            """
        )
    else:
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_match_participants (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              match_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              team_no INTEGER,
              seat_role TEXT NOT NULL DEFAULT 'solo',
              result TEXT NOT NULL DEFAULT 'pending',
              xp_awarded INTEGER NOT NULL DEFAULT 0,
              joined_at INTEGER NOT NULL
            )
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_match_moves (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              match_id INTEGER NOT NULL,
              move_number INTEGER NOT NULL,
              actor_user_id INTEGER NOT NULL,
              move_type TEXT NOT NULL,
              move_payload_json TEXT NOT NULL,
              created_at INTEGER NOT NULL
            )
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS game_xp_awards (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              match_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              xp_awarded INTEGER NOT NULL,
              created_at INTEGER NOT NULL
            )
            """
        )
    _db_exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_game_xp_awards_match_user ON game_xp_awards(match_id, user_id)")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_game_challenges_challenged_status ON game_challenges(challenged_user_id, status)")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_game_challenges_challenger_status ON game_challenges(challenger_user_id, status)")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_game_matches_status ON game_matches(status)")

    if DB_BACKEND == "postgres":
        pg_cols = _postgres_columns_by_table(
            ["game_challenges", "game_matches", "game_match_participants", "game_match_moves", "game_xp_awards"]
        )
        challenge_cols = pg_cols.setdefault("game_challenges", set())
        _ensure_column_if_missing("game_challenges", challenge_cols, "responded_at", "responded_at BIGINT")
        _ensure_column_if_missing("game_challenges", challenge_cols, "expires_at", "expires_at BIGINT")
        _ensure_column_if_missing("game_challenges", challenge_cols, "accepted_match_id", "accepted_match_id BIGINT")

        match_cols = pg_cols.setdefault("game_matches", set())
        _ensure_column_if_missing("game_matches", match_cols, "challenge_id", "challenge_id BIGINT")
        _ensure_column_if_missing("game_matches", match_cols, "challenger_user_id", "challenger_user_id BIGINT")
        _ensure_column_if_missing("game_matches", match_cols, "challenged_user_id", "challenged_user_id BIGINT")
        _ensure_column_if_missing("game_matches", match_cols, "winner_user_id", "winner_user_id BIGINT")
        _ensure_column_if_missing("game_matches", match_cols, "loser_user_id", "loser_user_id BIGINT")
        _ensure_column_if_missing(
            "game_matches", match_cols, "winner_xp_awarded", "winner_xp_awarded INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column_if_missing(
            "game_matches", match_cols, "loser_xp_awarded", "loser_xp_awarded INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column_if_missing("game_matches", match_cols, "result_summary", "result_summary TEXT")
        _ensure_column_if_missing("game_matches", match_cols, "accepted_at", "accepted_at BIGINT")
        _ensure_column_if_missing("game_matches", match_cols, "completed_at", "completed_at BIGINT")
        _ensure_column_if_missing("game_matches", match_cols, "expires_at", "expires_at BIGINT")
    else:
        challenge_cols = _table_columns_sqlite("game_challenges")
        _ensure_column_if_missing("game_challenges", challenge_cols, "responded_at", "responded_at INTEGER")
        _ensure_column_if_missing("game_challenges", challenge_cols, "expires_at", "expires_at INTEGER")
        _ensure_column_if_missing("game_challenges", challenge_cols, "accepted_match_id", "accepted_match_id INTEGER")

        match_cols = _table_columns_sqlite("game_matches")
        _ensure_column_if_missing("game_matches", match_cols, "challenge_id", "challenge_id INTEGER")
        _ensure_column_if_missing("game_matches", match_cols, "challenger_user_id", "challenger_user_id INTEGER")
        _ensure_column_if_missing("game_matches", match_cols, "challenged_user_id", "challenged_user_id INTEGER")
        _ensure_column_if_missing("game_matches", match_cols, "winner_user_id", "winner_user_id INTEGER")
        _ensure_column_if_missing("game_matches", match_cols, "loser_user_id", "loser_user_id INTEGER")
        _ensure_column_if_missing(
            "game_matches", match_cols, "winner_xp_awarded", "winner_xp_awarded INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column_if_missing(
            "game_matches", match_cols, "loser_xp_awarded", "loser_xp_awarded INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column_if_missing("game_matches", match_cols, "result_summary", "result_summary TEXT")
        _ensure_column_if_missing("game_matches", match_cols, "accepted_at", "accepted_at INTEGER")
        _ensure_column_if_missing("game_matches", match_cols, "completed_at", "completed_at INTEGER")
        _ensure_column_if_missing("game_matches", match_cols, "expires_at", "expires_at INTEGER")

    _db_exec("UPDATE game_matches SET accepted_at = created_at WHERE accepted_at IS NULL")
    _db_exec(
        "UPDATE game_matches SET expires_at = COALESCE(accepted_at, created_at) + ? WHERE expires_at IS NULL",
        (MATCH_TTL_SECONDS,),
    )


def create_challenge(challenger_user_id: int, challenged_user_id: int, game_type: str) -> Dict[str, Any]:
    ensure_games_schema()
    game = _normalize_game_type(game_type)
    if int(challenger_user_id) == int(challenged_user_id):
        raise HTTPException(status_code=400, detail="Cannot challenge yourself")
    exists = _db_query_one("SELECT id FROM users WHERE id=? LIMIT 1", (int(challenged_user_id),))
    if not exists:
        raise HTTPException(status_code=404, detail="User not found")
    duplicate = _db_query_one(
        """
        SELECT id FROM game_challenges
        WHERE status='pending' AND game_type=?
          AND ((challenger_user_id=? AND challenged_user_id=?) OR (challenger_user_id=? AND challenged_user_id=?))
        LIMIT 1
        """,
        (game, int(challenger_user_id), int(challenged_user_id), int(challenged_user_id), int(challenger_user_id)),
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="Challenge already exists")

    now = int(time.time())
    expires_at = now + MATCH_TTL_SECONDS
    _db_exec(
        """
        INSERT INTO game_challenges(game_type, challenger_user_id, challenged_user_id, status, created_at, updated_at, expires_at)
        VALUES(?,?,?,?,?,?,?)
        """,
        (game, int(challenger_user_id), int(challenged_user_id), "pending", now, now, expires_at),
    )
    row = _db_query_one(
        "SELECT * FROM game_challenges WHERE challenger_user_id=? AND challenged_user_id=? AND created_at=? ORDER BY id DESC LIMIT 1",
        (int(challenger_user_id), int(challenged_user_id), now),
    )
    assert row
    users = _public_user_map([int(challenger_user_id), int(challenged_user_id)])
    return _challenge_row_to_payload(dict(row), int(challenger_user_id), users)


def get_incoming_challenges(user_id: int) -> List[Dict[str, Any]]:
    rows = _db_query_all("SELECT * FROM game_challenges WHERE challenged_user_id=? AND status='pending' ORDER BY id DESC", (int(user_id),))
    user_ids = [int(user_id)]
    for r in rows:
        user_ids.extend([int(r["challenger_user_id"]), int(r["challenged_user_id"])])
    users = _public_user_map(user_ids)
    return [_challenge_row_to_payload(dict(r), int(user_id), users) for r in rows]


def get_outgoing_challenges(user_id: int) -> List[Dict[str, Any]]:
    rows = _db_query_all("SELECT * FROM game_challenges WHERE challenger_user_id=? AND status='pending' ORDER BY id DESC", (int(user_id),))
    user_ids = [int(user_id)]
    for r in rows:
        user_ids.extend([int(r["challenger_user_id"]), int(r["challenged_user_id"])])
    users = _public_user_map(user_ids)
    return [_challenge_row_to_payload(dict(r), int(user_id), users) for r in rows]


def _default_state(game_type: str, player_one_user_id: int, player_two_user_id: int) -> Dict[str, Any]:
    return {
        "game_type": game_type,
        "turn_user_id": int(player_one_user_id),
        "players": {
            str(int(player_one_user_id)): {"targets_remaining": 7, "targets_cleared": 0, "black_unlocked": False},
            str(int(player_two_user_id)): {"targets_remaining": 7, "targets_cleared": 0, "black_unlocked": False},
        },
        "result_summary": None,
    }


def accept_challenge(challenge_id: int, acting_user_id: int) -> Dict[str, Any]:
    ensure_games_schema()
    row = _db_query_one("SELECT * FROM game_challenges WHERE id=? LIMIT 1", (int(challenge_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Challenge not found")
    challenge = dict(row)
    if int(challenge["challenged_user_id"]) != int(acting_user_id):
        raise HTTPException(status_code=403, detail="Forbidden")
    if challenge["status"] == "accepted" and challenge.get("accepted_match_id"):
        return get_match_bundle(int(challenge["accepted_match_id"]), int(acting_user_id))
    if challenge["status"] != "pending":
        raise HTTPException(status_code=409, detail="Challenge is not pending")

    now = int(time.time())
    player_one = int(challenge["challenger_user_id"])
    player_two = int(challenge["challenged_user_id"])
    state = _default_state(str(challenge["game_type"]), player_one, player_two)
    _db_exec(
        """
        INSERT INTO game_matches(
          challenge_id, game_type, challenger_user_id, challenged_user_id,
          player_one_user_id, player_two_user_id, current_turn_user_id,
          status, match_state_json, result_summary, created_at, updated_at, accepted_at, expires_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(challenge_id),
            challenge["game_type"],
            int(challenge["challenger_user_id"]),
            int(challenge["challenged_user_id"]),
            player_one,
            player_two,
            player_one,
            "active",
            json.dumps(state, separators=(",", ":")),
            None,
            now,
            now,
            now,
            now + MATCH_TTL_SECONDS,
        ),
    )
    match_row = _db_query_one("SELECT * FROM game_matches WHERE challenge_id=? ORDER BY id DESC LIMIT 1", (int(challenge_id),))
    assert match_row
    match_id = int(match_row["id"])
    _db_exec("UPDATE game_challenges SET status='accepted', updated_at=?, responded_at=?, accepted_match_id=? WHERE id=?", (now, now, match_id, int(challenge_id)))
    _db_exec(
        "INSERT INTO game_match_participants(match_id, user_id, team_no, seat_role, result, xp_awarded, joined_at) VALUES(?,?,?,?,?,?,?)",
        (match_id, player_one, 1, "solo", "pending", 0, now),
    )
    _db_exec(
        "INSERT INTO game_match_participants(match_id, user_id, team_no, seat_role, result, xp_awarded, joined_at) VALUES(?,?,?,?,?,?,?)",
        (match_id, player_two, 2, "solo", "pending", 0, now),
    )
    return get_match_bundle(match_id, int(acting_user_id), challenge_id=int(challenge_id))


def decline_challenge(challenge_id: int, acting_user_id: int) -> Dict[str, Any]:
    row = _db_query_one("SELECT * FROM game_challenges WHERE id=? LIMIT 1", (int(challenge_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Challenge not found")
    if int(row["challenged_user_id"]) != int(acting_user_id):
        raise HTTPException(status_code=403, detail="Forbidden")
    if row["status"] != "pending":
        raise HTTPException(status_code=409, detail="Challenge is not pending")
    now = int(time.time())
    _db_exec("UPDATE game_challenges SET status='declined', updated_at=?, responded_at=? WHERE id=?", (now, now, int(challenge_id)))
    out = dict(row)
    out["status"] = "declined"
    users = _public_user_map([int(out["challenger_user_id"]), int(out["challenged_user_id"])])
    return _challenge_row_to_payload(out, int(acting_user_id), users)


def cancel_challenge(challenge_id: int, acting_user_id: int) -> Dict[str, Any]:
    row = _db_query_one("SELECT * FROM game_challenges WHERE id=? LIMIT 1", (int(challenge_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Challenge not found")
    if int(row["challenger_user_id"]) != int(acting_user_id):
        raise HTTPException(status_code=403, detail="Forbidden")
    if row["status"] != "pending":
        raise HTTPException(status_code=409, detail="Challenge is not pending")
    now = int(time.time())
    _db_exec("UPDATE game_challenges SET status='cancelled', updated_at=?, responded_at=? WHERE id=?", (now, now, int(challenge_id)))
    out = dict(row)
    out["status"] = "cancelled"
    users = _public_user_map([int(out["challenger_user_id"]), int(out["challenged_user_id"])])
    return _challenge_row_to_payload(out, int(acting_user_id), users)


def _complete_match(match_id: int, *, winner_user_id: int, loser_user_id: int, reason: str) -> Dict[str, Any]:
    match = _db_query_one("SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_id),))
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")
    existing = dict(match)
    if existing["status"] in {"completed", "forfeited"} and existing.get("winner_user_id"):
        return existing
    now = int(time.time())
    summary = {"reason": reason, "winner_user_id": int(winner_user_id), "loser_user_id": int(loser_user_id)}
    status = "forfeited" if reason == "forfeit" else "completed"
    _db_exec(
        """
        UPDATE game_matches
        SET status=?, winner_user_id=?, loser_user_id=?, winner_xp_awarded=?, loser_xp_awarded=?,
            result_summary=?, completed_at=?, updated_at=?
        WHERE id=?
        """,
        (status, int(winner_user_id), int(loser_user_id), WINNER_XP, LOSER_XP, json.dumps(summary, separators=(",", ":")), now, now, int(match_id)),
    )
    _db_exec("UPDATE game_match_participants SET result='winner', xp_awarded=? WHERE match_id=? AND user_id=?", (WINNER_XP, int(match_id), int(winner_user_id)))
    _db_exec("UPDATE game_match_participants SET result='loser', xp_awarded=? WHERE match_id=? AND user_id=?", (LOSER_XP, int(match_id), int(loser_user_id)))
    return dict(_db_query_one("SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_id),)))


def _apply_xp_idempotent(match_id: int) -> None:
    match = _db_query_one("SELECT winner_user_id, loser_user_id, winner_xp_awarded, loser_xp_awarded, status FROM game_matches WHERE id=? LIMIT 1", (int(match_id),))
    if not match:
        return
    match = dict(match)
    if match["status"] not in {"completed", "forfeited"}:
        return
    winner_id = match.get("winner_user_id")
    loser_id = match.get("loser_user_id")
    now = int(time.time())
    if winner_id is not None:
        if DB_BACKEND == "postgres":
            _db_exec(
                "INSERT INTO game_xp_awards(match_id, user_id, xp_awarded, created_at) VALUES(?,?,?,?) ON CONFLICT(match_id, user_id) DO NOTHING",
                (int(match_id), int(winner_id), int(match.get("winner_xp_awarded") or 0), now),
            )
        else:
            _db_exec(
                "INSERT OR IGNORE INTO game_xp_awards(match_id, user_id, xp_awarded, created_at) VALUES(?,?,?,?)",
                (int(match_id), int(winner_id), int(match.get("winner_xp_awarded") or 0), now),
            )
    if loser_id is not None:
        if DB_BACKEND == "postgres":
            _db_exec(
                "INSERT INTO game_xp_awards(match_id, user_id, xp_awarded, created_at) VALUES(?,?,?,?) ON CONFLICT(match_id, user_id) DO NOTHING",
                (int(match_id), int(loser_id), int(match.get("loser_xp_awarded") or 0), now),
            )
        else:
            _db_exec(
                "INSERT OR IGNORE INTO game_xp_awards(match_id, user_id, xp_awarded, created_at) VALUES(?,?,?,?)",
                (int(match_id), int(loser_id), int(match.get("loser_xp_awarded") or 0), now),
            )


def get_match_bundle(match_id: int, viewer_user_id: int, *, challenge_id: Optional[int] = None) -> Dict[str, Any]:
    ensure_games_schema()
    row = _db_query_one("SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    match = dict(row)
    allowed = {int(match["challenger_user_id"]), int(match["challenged_user_id"])}
    if int(viewer_user_id) not in allowed:
        raise HTTPException(status_code=403, detail="Forbidden")
    _apply_xp_idempotent(int(match_id))
    users = _public_user_map([int(match["challenger_user_id"]), int(match["challenged_user_id"])])
    payload = _match_row_to_payload(match, int(viewer_user_id), users)
    winner_xp = int(match.get("winner_xp_awarded") or 0)
    loser_xp = int(match.get("loser_xp_awarded") or 0)
    viewer_xp = winner_xp if int(match.get("winner_user_id") or 0) == int(viewer_user_id) else loser_xp
    reward_contract = {"xp_awarded": int(viewer_xp)}
    out = {"match": payload, "reward_contract": reward_contract}
    if challenge_id is not None:
        out["match"]["challenge_id"] = int(challenge_id)
    return out


def get_match_detail(match_id: int, viewer_user_id: int) -> Dict[str, Any]:
    return get_match_bundle(int(match_id), int(viewer_user_id))


def get_active_match_for_user(user_id: int) -> Optional[Dict[str, Any]]:
    ensure_games_schema()
    try:
        row = _db_query_one(
            """
            SELECT * FROM game_matches
            WHERE status='active' AND (challenger_user_id=? OR challenged_user_id=?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(user_id), int(user_id)),
        )
    except Exception:
        return None
    if not row:
        return None
    raw = dict(row)
    if raw.get("challenge_id") is None:
        return None
    users = _public_user_map([int(raw["challenger_user_id"]), int(raw["challenged_user_id"])])
    return _match_row_to_payload(raw, int(user_id), users)


def move_match(match_id: int, actor_user_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    row = _db_query_one("SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    match = dict(row)
    if int(actor_user_id) not in {int(match["challenger_user_id"]), int(match["challenged_user_id"])}:
        raise HTTPException(status_code=403, detail="Forbidden")
    if match["status"] != "active":
        return get_match_bundle(int(match_id), int(actor_user_id))

    current_turn = match.get("current_turn_user_id")
    if current_turn is not None and int(current_turn) != int(actor_user_id):
        raise HTTPException(status_code=409, detail="Not your turn")

    state = json.loads(match.get("match_state_json") or "{}")
    move_number_row = _db_query_one("SELECT COALESCE(MAX(move_number), 0) AS n FROM game_match_moves WHERE match_id=?", (int(match_id),))
    move_number = int(move_number_row["n"] or 0) + 1
    now = int(time.time())
    _db_exec(
        "INSERT INTO game_match_moves(match_id, move_number, actor_user_id, move_type, move_payload_json, created_at) VALUES(?,?,?,?,?,?)",
        (int(match_id), move_number, int(actor_user_id), str(payload.get("move_type") or "move"), json.dumps(payload, separators=(",", ":")), now),
    )

    completed = False
    public_notification = None
    if match["game_type"] == "billiards":
        result_state = payload.get("result_state") or {}
        winner_user_id = result_state.get("winner_user_id")
        if winner_user_id is None:
            players = state.get("players") or {}
            player_state = players.get(str(actor_user_id)) or {}
            if int(player_state.get("targets_remaining") or 0) <= 0:
                winner_user_id = int(actor_user_id)
        if winner_user_id is not None:
            loser_user_id = int(match["challenged_user_id"]) if int(winner_user_id) == int(match["challenger_user_id"]) else int(match["challenger_user_id"])
            reason = "eight_ball_pocketed" if (result_state.get("pocketed_balls") or [None])[-1] == 8 else "win"
            completed_row = _complete_match(int(match_id), winner_user_id=int(winner_user_id), loser_user_id=int(loser_user_id), reason=reason)
            match = completed_row
            completed = True
            from chat import publish_public_battle_notification

            users = _public_user_map([int(winner_user_id), int(loser_user_id)])
            public_notification = publish_public_battle_notification(
                {
                    "match_id": int(match_id),
                    "game_type": match["game_type"],
                    "winner_user_id": int(winner_user_id),
                    "winner_display_name": users.get(int(winner_user_id), {}).get("display_name", "Winner"),
                    "loser_user_id": int(loser_user_id),
                    "loser_display_name": users.get(int(loser_user_id), {}).get("display_name", "Loser"),
                    "winner_xp_awarded": WINNER_XP,
                    "winner_new_level": get_progression_for_users([int(winner_user_id)]).get(int(winner_user_id), {}).get("level", 1),
                    "completed_at": _safe_iso(now),
                }
            )
    elif match["game_type"] == "dominoes":
        next_turn = int(match["challenged_user_id"]) if int(actor_user_id) == int(match["challenger_user_id"]) else int(match["challenger_user_id"])
        state["last_action"] = payload.get("move_type")
        state["turn_user_id"] = next_turn
        _db_exec(
            "UPDATE game_matches SET current_turn_user_id=?, match_state_json=?, updated_at=? WHERE id=?",
            (next_turn, json.dumps(state, separators=(",", ":")), now, int(match_id)),
        )

    if not completed and match["game_type"] == "billiards":
        next_turn = int(match["challenged_user_id"]) if int(actor_user_id) == int(match["challenger_user_id"]) else int(match["challenger_user_id"])
        state["turn_user_id"] = next_turn
        _db_exec(
            "UPDATE game_matches SET current_turn_user_id=?, match_state_json=?, updated_at=? WHERE id=?",
            (next_turn, json.dumps(state, separators=(",", ":")), now, int(match_id)),
        )
    bundle = get_match_bundle(int(match_id), int(actor_user_id))
    if public_notification is not None:
        bundle["public_notification"] = public_notification
    return bundle


def forfeit_match(match_id: int, actor_user_id: int) -> Dict[str, Any]:
    row = _db_query_one("SELECT * FROM game_matches WHERE id=? LIMIT 1", (int(match_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    if int(actor_user_id) not in {int(row["challenger_user_id"]), int(row["challenged_user_id"])}:
        raise HTTPException(status_code=403, detail="Forbidden")
    if row["status"] not in {"active"}:
        return get_match_bundle(int(match_id), int(actor_user_id))
    winner = int(row["challenged_user_id"]) if int(actor_user_id) == int(row["challenger_user_id"]) else int(row["challenger_user_id"])
    _complete_match(int(match_id), winner_user_id=winner, loser_user_id=int(actor_user_id), reason="forfeit")
    return get_match_bundle(int(match_id), int(actor_user_id))


def get_history_for_user(user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    rows = _db_query_all(
        """
        SELECT * FROM game_matches
        WHERE status IN ('completed', 'forfeited')
          AND (challenger_user_id=? OR challenged_user_id=?)
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(user_id), int(user_id), max(1, min(100, int(limit)))),
    )
    ids = [int(user_id)]
    for r in rows:
        ids.extend([int(r["challenger_user_id"]), int(r["challenged_user_id"])])
    users = _public_user_map(ids)
    return [_match_row_to_payload(dict(r), int(user_id), users) for r in rows]


def get_games_users(viewer_user_id: int, q: str, limit: int = 20) -> List[Dict[str, Any]]:
    needle = f"%{(q or '').strip().lower()}%"
    rows = _db_query_all(
        """
        SELECT id, email, display_name, avatar_url, avatar_version
        FROM users
        WHERE id != ?
          AND (lower(display_name) LIKE ? OR lower(email) LIKE ?)
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(viewer_user_id), needle, needle, max(1, min(100, int(limit)))),
    )
    ids = [int(r["id"]) for r in rows]
    prog = get_progression_for_users(ids)
    badges = get_best_current_badges_for_users(ids)
    items: List[Dict[str, Any]] = []
    for r in rows:
        row = dict(r)
        uid = int(row["id"])
        p = prog.get(uid) or {}
        items.append(
            {
                "user_id": uid,
                "display_name": (row.get("display_name") or row.get("email") or "Driver").split("@")[0][:28],
                "avatar_url": row.get("avatar_url"),
                "avatar_thumb_url": f"/avatars/thumb/{uid}",
                "rank_icon_key": p.get("rank_icon_key", "band_001"),
                "leaderboard_badge_code": (badges.get(uid) or {}).get("leaderboard_badge_code"),
            }
        )
    return items


def get_challenge_dashboard(user_id: int) -> Dict[str, Any]:
    incoming = get_incoming_challenges(int(user_id))
    outgoing = get_outgoing_challenges(int(user_id))
    active_match = get_active_match_for_user(int(user_id))
    return {
        "items": incoming + outgoing,
        "incoming": incoming,
        "outgoing": outgoing,
        "active_match": active_match,
        "activeMatch": active_match,
        "match": active_match,
    }


def get_viewer_game_relationship(target_user_id: int, viewer_user_id: int) -> Dict[str, Any]:
    if int(target_user_id) == int(viewer_user_id):
        return {"status": "none"}
    row = _db_query_one(
        """
        SELECT * FROM game_matches
        WHERE status='active'
          AND ((challenger_user_id=? AND challenged_user_id=?) OR (challenger_user_id=? AND challenged_user_id=?))
        ORDER BY id DESC LIMIT 1
        """,
        (int(target_user_id), int(viewer_user_id), int(viewer_user_id), int(target_user_id)),
    )
    if row:
        return {"status": "active_match", "match_id": int(row["id"])}

    pending = _db_query_one(
        """
        SELECT * FROM game_challenges
        WHERE status='pending'
          AND ((challenger_user_id=? AND challenged_user_id=?) OR (challenger_user_id=? AND challenged_user_id=?))
        ORDER BY id DESC LIMIT 1
        """,
        (int(target_user_id), int(viewer_user_id), int(viewer_user_id), int(target_user_id)),
    )
    if pending:
        if int(pending["challenged_user_id"]) == int(viewer_user_id):
            return {"status": "incoming_challenge", "challenge_id": int(pending["id"])}
        return {"status": "outgoing_challenge", "challenge_id": int(pending["id"])}
    return {"status": "none"}


def get_active_match_between_users(a: int, b: int) -> Optional[Dict[str, Any]]:
    row = _db_query_one(
        """
        SELECT * FROM game_matches
        WHERE status='active'
          AND ((challenger_user_id=? AND challenged_user_id=?) OR (challenger_user_id=? AND challenged_user_id=?))
        ORDER BY id DESC LIMIT 1
        """,
        (int(a), int(b), int(b), int(a)),
    )
    if not row:
        return None
    users = _public_user_map([int(a), int(b)])
    return _match_row_to_payload(dict(row), int(a), users)


def get_battle_stats_for_user(user_id: int, *, limit: int = 10) -> Dict[str, Any]:
    rows = _db_query_all(
        """
        SELECT * FROM game_matches
        WHERE status IN ('completed', 'forfeited')
          AND (challenger_user_id=? OR challenged_user_id=?)
        ORDER BY id DESC
        """,
        (int(user_id), int(user_id)),
    )
    wins = 0
    losses = 0
    recent: List[Dict[str, Any]] = []
    ids = [int(user_id)]
    for r in rows[: max(1, min(50, int(limit)) )]:
        ids.extend([int(r["challenger_user_id"]), int(r["challenged_user_id"])])
    users = _public_user_map(ids)
    for r in rows:
        row = dict(r)
        if int(row.get("winner_user_id") or 0) == int(user_id):
            wins += 1
        elif row.get("winner_user_id") is not None:
            losses += 1
    for r in rows[: max(1, min(20, int(limit))) ]:
        row = dict(r)
        recent.append({
            "match_id": int(row["id"]),
            "game_type": row["game_type"],
            "status": row["status"],
            "winner_user_id": int(row["winner_user_id"]) if row.get("winner_user_id") is not None else None,
            "opponent_display_name": users.get(int(row["challenged_user_id"]) if int(row["challenger_user_id"]) == int(user_id) else int(row["challenger_user_id"]), {}).get("display_name", "Driver"),
        })
    total = wins + losses
    return {
        "battle_stats": {"wins": wins, "losses": losses, "matches_played": total},
        "battle_record": {"wins": wins, "losses": losses, "total_matches": total},
        "recent_battles": recent,
        "battle_history": recent,
    }
