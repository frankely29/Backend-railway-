from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from avatar_assets import avatar_thumb_url, avatar_version_for_data_url
from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one
from leaderboard_service import get_best_current_badges_for_users, get_progression_for_users
from work_battles_db import ensure_work_battles_schema as _ensure_work_battles_schema

NYC_TZ = ZoneInfo("America/New_York")
PENDING_EXPIRY_MS = 24 * 60 * 60 * 1000
PRESENCE_STALE_SECONDS = 300
CATALOG: dict[str, dict[str, str]] = {
    "daily_miles": {"metric_key": "miles", "period_key": "daily"},
    "daily_hours": {"metric_key": "hours", "period_key": "daily"},
    "weekly_miles": {"metric_key": "miles", "period_key": "weekly"},
    "weekly_hours": {"metric_key": "hours", "period_key": "weekly"},
}


def ensure_work_battles_schema() -> None:
    _ensure_work_battles_schema()


def _now_ms(now_ms: Optional[int] = None) -> int:
    return int(now_ms if now_ms is not None else time.time() * 1000)


def _catalog_item(battle_type: str) -> Dict[str, str]:
    item = CATALOG.get(str(battle_type))
    if not item:
        raise HTTPException(status_code=400, detail="Unsupported battle_type")
    return item


def _today_nyc_from_ms(now_ms: int) -> date:
    current = datetime.fromtimestamp(int(now_ms) / 1000.0, tz=timezone.utc).astimezone(NYC_TZ)
    return (current - timedelta(hours=4)).date()


def _period_bounds(period_key: str, now_ms: int) -> tuple[date, date]:
    today = _today_nyc_from_ms(now_ms)
    if period_key == "daily":
        return today, today
    start = today - timedelta(days=today.weekday())
    return start, start + timedelta(days=6)


def _period_end_ms(period_end_date: date) -> int:
    boundary_local = datetime.combine(period_end_date + timedelta(days=1), datetime.min.time(), tzinfo=NYC_TZ) + timedelta(hours=4)
    return int(boundary_local.astimezone(timezone.utc).timestamp() * 1000)


def _metric_column(metric_key: str) -> str:
    if metric_key == "miles":
        return "miles_worked"
    if metric_key == "hours":
        return "hours_worked"
    raise HTTPException(status_code=400, detail="Unsupported metric")


def _round_metric(value: Any) -> float:
    return round(float(value or 0.0), 4)


def _require_user_exists(user_id: int) -> Dict[str, Any]:
    row = _db_query_one(
        "SELECT id, email, display_name, avatar_url, avatar_version, is_disabled, is_suspended FROM users WHERE id=? LIMIT 1",
        (int(user_id),),
    )
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    if int(row["is_disabled"] or 0) == 1 or int(row["is_suspended"] or 0) == 1:
        raise HTTPException(status_code=403, detail="User is not challengeable")
    return dict(row)


def _period_total_for_user(user_id: int, metric_key: str, start_date: date, end_date: date) -> float:
    metric_column = _metric_column(metric_key)
    row = _db_query_one(
        f"""
        SELECT COALESCE(SUM({metric_column}), 0) AS total_value
        FROM driver_daily_stats
        WHERE user_id=? AND nyc_date >= ? AND nyc_date <= ?
        """,
        (int(user_id), start_date.isoformat(), end_date.isoformat()),
    )
    return _round_metric(row["total_value"] if row else 0.0)


def _row_to_battle_type(row: Dict[str, Any]) -> str:
    return f"{row['period_key']}_{row['metric_key']}"


def _is_boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return int(value or 0) == 1


def _inactive_user_flag_sql(column_name: str) -> str:
    if DB_BACKEND == "postgres":
        return f"COALESCE({column_name}, FALSE) = FALSE"
    return f"COALESCE(CAST({column_name} AS INTEGER), 0) = 0"


def _public_user_map(user_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not user_ids:
        return {}
    unique_ids = sorted({int(uid) for uid in user_ids})
    placeholders = ",".join(["?" for _ in unique_ids])
    cutoff = int(time.time()) - PRESENCE_STALE_SECONDS
    rows = _db_query_all(
        f"""
        SELECT
          u.id AS user_id,
          u.email,
          u.display_name,
          u.avatar_url,
          u.avatar_version,
          u.ghost_mode,
          u.is_disabled,
          u.is_suspended,
          p.updated_at AS presence_updated_at
        FROM users u
        LEFT JOIN presence p ON p.user_id = u.id
        WHERE u.id IN ({placeholders})
        """,
        tuple(unique_ids),
    )
    progression = get_progression_for_users(unique_ids)
    badges = get_best_current_badges_for_users(unique_ids)
    result: Dict[int, Dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        uid = int(row["user_id"])
        version = row.get("avatar_version") or avatar_version_for_data_url(row.get("avatar_url"))
        display_name = (row.get("display_name") or "").strip() or ((row.get("email") or "Driver").split("@")[0])
        progress = progression.get(uid) or {}
        badge = badges.get(uid) or {}
        online = bool(
            row.get("presence_updated_at") is not None
            and int(row.get("presence_updated_at") or 0) >= cutoff
            and not _is_boolish(row.get("ghost_mode"))
            and not _is_boolish(row.get("is_disabled"))
            and not _is_boolish(row.get("is_suspended"))
        )
        result[uid] = {
            "user_id": uid,
            "display_name": display_name[:28],
            "avatar_url": row.get("avatar_url"),
            "avatar_thumb_url": avatar_thumb_url(uid, version),
            "online": online,
            "level": progress.get("level"),
            "rank_icon_key": progress.get("rank_icon_key"),
            "leaderboard_badge_code": badge.get("leaderboard_badge_code"),
        }
    return result


def expire_due_pending_challenges(*, now_ms: Optional[int] = None) -> int:
    ensure_work_battles_schema()
    current_ms = _now_ms(now_ms)
    _db_exec(
        """
        UPDATE work_battle_challenges
        SET status='expired', last_action_at_ms=?
        WHERE status='pending' AND expires_at_ms <= ?
        """,
        (current_ms, current_ms),
    )
    row = _db_query_one(
        "SELECT changes() AS count" if DB_BACKEND == "sqlite" else "SELECT 0 AS count",
    )
    return int(row["count"] or 0) if row else 0


def _complete_active_row(row: Dict[str, Any], current_ms: int) -> Dict[str, Any]:
    start_date = date.fromisoformat(str(row["period_start_date"]))
    end_date = date.fromisoformat(str(row["period_end_date"]))
    challenger_final = max(0.0, _round_metric(_period_total_for_user(int(row["challenger_user_id"]), str(row["metric_key"]), start_date, end_date) - float(row["challenger_start_value"] or 0.0)))
    challenged_final = max(0.0, _round_metric(_period_total_for_user(int(row["challenged_user_id"]), str(row["metric_key"]), start_date, end_date) - float(row["challenged_start_value"] or 0.0)))
    winner_user_id: Optional[int]
    loser_user_id: Optional[int]
    result_code: str
    if challenger_final > challenged_final:
        winner_user_id = int(row["challenger_user_id"])
        loser_user_id = int(row["challenged_user_id"])
        result_code = "challenger_win"
    elif challenged_final > challenger_final:
        winner_user_id = int(row["challenged_user_id"])
        loser_user_id = int(row["challenger_user_id"])
        result_code = "challenged_win"
    else:
        winner_user_id = None
        loser_user_id = None
        result_code = "tie"
    _db_exec(
        """
        UPDATE work_battle_challenges
        SET status='completed',
            winner_user_id=?,
            loser_user_id=?,
            challenger_final_value=?,
            challenged_final_value=?,
            completed_at_ms=?,
            result_code=?,
            last_action_at_ms=?
        WHERE id=? AND status='active'
        """,
        (
            winner_user_id,
            loser_user_id,
            challenger_final,
            challenged_final,
            current_ms,
            result_code,
            current_ms,
            int(row["id"]),
        ),
    )
    refreshed = _db_query_one("SELECT * FROM work_battle_challenges WHERE id=? LIMIT 1", (int(row["id"]),))
    return dict(refreshed) if refreshed else dict(row)


def finalize_due_active_challenges(*, now_ms: Optional[int] = None) -> int:
    ensure_work_battles_schema()
    current_ms = _now_ms(now_ms)
    rows = _db_query_all(
        "SELECT * FROM work_battle_challenges WHERE status='active' AND ends_at_ms IS NOT NULL AND ends_at_ms <= ?",
        (current_ms,),
    )
    for row in rows:
        _complete_active_row(dict(row), current_ms)
    return len(rows)


def _refresh_challenge_row(challenge_id: int, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    current_ms = _now_ms(now_ms)
    expire_due_pending_challenges(now_ms=current_ms)
    finalize_due_active_challenges(now_ms=current_ms)
    row = _db_query_one("SELECT * FROM work_battle_challenges WHERE id=? LIMIT 1", (int(challenge_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Challenge not found")
    return dict(row)


def _assert_no_duplicate_or_overlap(challenger_user_id: int, challenged_user_id: int, metric_key: str, period_key: str) -> None:
    duplicate = _db_query_one(
        """
        SELECT id, status
        FROM work_battle_challenges
        WHERE metric_key=?
          AND period_key=?
          AND status IN ('pending', 'active')
          AND (
            (challenger_user_id=? AND challenged_user_id=?)
            OR
            (challenger_user_id=? AND challenged_user_id=?)
          )
        ORDER BY id DESC
        LIMIT 1
        """,
        (metric_key, period_key, int(challenger_user_id), int(challenged_user_id), int(challenged_user_id), int(challenger_user_id)),
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="A pending or active work battle already exists between these users")

    overlap = _db_query_one(
        """
        SELECT id
        FROM work_battle_challenges
        WHERE metric_key=?
          AND period_key=?
          AND status='active'
          AND (
            challenger_user_id IN (?, ?)
            OR challenged_user_id IN (?, ?)
          )
        LIMIT 1
        """,
        (metric_key, period_key, int(challenger_user_id), int(challenged_user_id), int(challenger_user_id), int(challenged_user_id)),
    )
    if overlap:
        raise HTTPException(status_code=409, detail="One of these users already has an active work battle for this metric and period")


def create_challenge(challenger_user_id: int, target_user_id: int, battle_type: str, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    ensure_work_battles_schema()
    current_ms = _now_ms(now_ms)
    expire_due_pending_challenges(now_ms=current_ms)
    finalize_due_active_challenges(now_ms=current_ms)
    if int(challenger_user_id) == int(target_user_id):
        raise HTTPException(status_code=400, detail="You cannot challenge yourself")
    battle = _catalog_item(battle_type)
    _require_user_exists(int(challenger_user_id))
    _require_user_exists(int(target_user_id))
    _assert_no_duplicate_or_overlap(int(challenger_user_id), int(target_user_id), battle["metric_key"], battle["period_key"])
    expires_at_ms = current_ms + PENDING_EXPIRY_MS
    _db_exec(
        """
        INSERT INTO work_battle_challenges(
          metric_key, period_key, challenger_user_id, challenged_user_id, status,
          created_at_ms, expires_at_ms, last_action_at_ms
        )
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            battle["metric_key"],
            battle["period_key"],
            int(challenger_user_id),
            int(target_user_id),
            "pending",
            current_ms,
            expires_at_ms,
            current_ms,
        ),
    )
    row = _db_query_one(
        """
        SELECT * FROM work_battle_challenges
        WHERE challenger_user_id=? AND challenged_user_id=? AND created_at_ms=?
        ORDER BY id DESC LIMIT 1
        """,
        (int(challenger_user_id), int(target_user_id), current_ms),
    )
    return get_challenge_detail(int(row["id"]), int(challenger_user_id), now_ms=current_ms)


def accept_challenge(challenge_id: int, acting_user_id: int, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    ensure_work_battles_schema()
    current_ms = _now_ms(now_ms)
    row = _refresh_challenge_row(int(challenge_id), now_ms=current_ms)
    if int(row["challenged_user_id"]) != int(acting_user_id):
        raise HTTPException(status_code=403, detail="Only the challenged user can accept this challenge")
    if row["status"] != "pending":
        raise HTTPException(status_code=409, detail="Challenge is not pending")
    start_date, end_date = _period_bounds(str(row["period_key"]), current_ms)
    ends_at_ms = _period_end_ms(end_date)
    challenger_start = _period_total_for_user(int(row["challenger_user_id"]), str(row["metric_key"]), start_date, end_date)
    challenged_start = _period_total_for_user(int(row["challenged_user_id"]), str(row["metric_key"]), start_date, end_date)
    _db_exec(
        """
        UPDATE work_battle_challenges
        SET status='active',
            accepted_at_ms=?,
            last_action_at_ms=?,
            period_start_date=?,
            period_end_date=?,
            ends_at_ms=?,
            challenger_start_value=?,
            challenged_start_value=?
        WHERE id=? AND status='pending'
        """,
        (
            current_ms,
            current_ms,
            start_date.isoformat(),
            end_date.isoformat(),
            ends_at_ms,
            challenger_start,
            challenged_start,
            int(challenge_id),
        ),
    )
    return get_challenge_detail(int(challenge_id), int(acting_user_id), now_ms=current_ms)


def decline_challenge(challenge_id: int, acting_user_id: int, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    current_ms = _now_ms(now_ms)
    row = _refresh_challenge_row(int(challenge_id), now_ms=current_ms)
    if int(row["challenged_user_id"]) != int(acting_user_id):
        raise HTTPException(status_code=403, detail="Only the challenged user can decline this challenge")
    if row["status"] != "pending":
        raise HTTPException(status_code=409, detail="Challenge is not pending")
    _db_exec(
        "UPDATE work_battle_challenges SET status='declined', declined_by_user_id=?, last_action_at_ms=? WHERE id=?",
        (int(acting_user_id), current_ms, int(challenge_id)),
    )
    return get_challenge_detail(int(challenge_id), int(acting_user_id), now_ms=current_ms)


def cancel_challenge(challenge_id: int, acting_user_id: int, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    current_ms = _now_ms(now_ms)
    row = _refresh_challenge_row(int(challenge_id), now_ms=current_ms)
    if int(row["challenger_user_id"]) != int(acting_user_id):
        raise HTTPException(status_code=403, detail="Only the challenger can cancel this challenge")
    if row["status"] != "pending":
        raise HTTPException(status_code=409, detail="Only pending challenges can be canceled")
    _db_exec(
        "UPDATE work_battle_challenges SET status='canceled', canceled_by_user_id=?, last_action_at_ms=? WHERE id=?",
        (int(acting_user_id), current_ms, int(challenge_id)),
    )
    return get_challenge_detail(int(challenge_id), int(acting_user_id), now_ms=current_ms)


def list_challengeable_users(user_id: int, q: str = "", limit: int = 25) -> List[Dict[str, Any]]:
    ensure_work_battles_schema()
    search = f"%{(q or '').strip().lower()}%"
    safe_limit = max(1, min(50, int(limit)))
    rows = _db_query_all(
        f"""
        SELECT id AS user_id
        FROM users
        WHERE id != ?
          AND {_inactive_user_flag_sql("is_disabled")}
          AND {_inactive_user_flag_sql("is_suspended")}
          AND (
            ? = '%%'
            OR lower(COALESCE(display_name, '')) LIKE ?
            OR lower(COALESCE(email, '')) LIKE ?
          )
        ORDER BY lower(COALESCE(display_name, email)) ASC, id ASC
        LIMIT ?
        """,
        (int(user_id), search, search, search, safe_limit),
    )
    user_map = _public_user_map([int(row["user_id"]) for row in rows])
    return [user_map[int(row["user_id"])] for row in rows if int(row["user_id"]) in user_map]


def _challenge_summary(row: Dict[str, Any], viewer_user_id: int, user_map: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    challenger = user_map.get(int(row["challenger_user_id"]), {"display_name": "Driver"})
    challenged = user_map.get(int(row["challenged_user_id"]), {"display_name": "Driver"})
    opponent_user_id = int(row["challenged_user_id"] if int(row["challenger_user_id"]) == int(viewer_user_id) else row["challenger_user_id"])
    opponent = user_map.get(opponent_user_id, {"display_name": "Driver"})
    return {
        "id": int(row["id"]),
        "battle_type": _row_to_battle_type(row),
        "metric_key": str(row["metric_key"]),
        "period_key": str(row["period_key"]),
        "status": str(row["status"]),
        "challenger_user_id": int(row["challenger_user_id"]),
        "challenger_display_name": challenger["display_name"],
        "challenged_user_id": int(row["challenged_user_id"]),
        "challenged_display_name": challenged["display_name"],
        "created_at_ms": int(row["created_at_ms"]),
        "expires_at_ms": int(row["expires_at_ms"]),
        "accepted_at_ms": int(row["accepted_at_ms"]) if row.get("accepted_at_ms") is not None else None,
        "ends_at_ms": int(row["ends_at_ms"]) if row.get("ends_at_ms") is not None else None,
        "result_code": row.get("result_code"),
        "opponent_user_id": opponent_user_id,
        "opponent_display_name": opponent.get("display_name") or "Driver",
    }


def _history_row(row: Dict[str, Any], viewer_user_id: int, user_map: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    challenger_is_viewer = int(row["challenger_user_id"]) == int(viewer_user_id)
    opponent_user_id = int(row["challenged_user_id"] if challenger_is_viewer else row["challenger_user_id"])
    opponent = user_map.get(opponent_user_id, {"display_name": "Driver"})
    my_final = row["challenger_final_value"] if challenger_is_viewer else row["challenged_final_value"]
    other_final = row["challenged_final_value"] if challenger_is_viewer else row["challenger_final_value"]
    return {
        "id": int(row["id"]),
        "battle_type": _row_to_battle_type(row),
        "opponent_user_id": opponent_user_id,
        "opponent_display_name": opponent.get("display_name") or "Driver",
        "result_code": row.get("result_code") or "tie",
        "my_final_value": _round_metric(my_final),
        "other_final_value": _round_metric(other_final),
        "completed_at_ms": int(row["completed_at_ms"] or 0),
    }


def list_incoming_challenges_for_user(user_id: int, *, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
    current_ms = _now_ms(now_ms)
    expire_due_pending_challenges(now_ms=current_ms)
    rows = _db_query_all(
        "SELECT * FROM work_battle_challenges WHERE challenged_user_id=? AND status='pending' ORDER BY created_at_ms DESC, id DESC",
        (int(user_id),),
    )
    user_map = _public_user_map([int(user_id)] + [int(row["challenger_user_id"]) for row in rows] + [int(row["challenged_user_id"]) for row in rows])
    return [_challenge_summary(dict(row), int(user_id), user_map) for row in rows]


def list_outgoing_challenges_for_user(user_id: int, *, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
    current_ms = _now_ms(now_ms)
    expire_due_pending_challenges(now_ms=current_ms)
    rows = _db_query_all(
        "SELECT * FROM work_battle_challenges WHERE challenger_user_id=? AND status='pending' ORDER BY created_at_ms DESC, id DESC",
        (int(user_id),),
    )
    user_map = _public_user_map([int(user_id)] + [int(row["challenger_user_id"]) for row in rows] + [int(row["challenged_user_id"]) for row in rows])
    return [_challenge_summary(dict(row), int(user_id), user_map) for row in rows]


def get_active_challenge_for_user(user_id: int, *, now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
    current_ms = _now_ms(now_ms)
    finalize_due_active_challenges(now_ms=current_ms)
    row = _db_query_one(
        """
        SELECT *
        FROM work_battle_challenges
        WHERE status='active' AND (challenger_user_id=? OR challenged_user_id=?)
        ORDER BY accepted_at_ms DESC, id DESC
        LIMIT 1
        """,
        (int(user_id), int(user_id)),
    )
    if not row:
        return None
    return get_challenge_detail(int(row["id"]), int(user_id), now_ms=current_ms)


def get_history_for_user(user_id: int, *, limit: int = 25, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
    current_ms = _now_ms(now_ms)
    finalize_due_active_challenges(now_ms=current_ms)
    safe_limit = max(1, min(100, int(limit)))
    rows = _db_query_all(
        """
        SELECT *
        FROM work_battle_challenges
        WHERE status='completed' AND (challenger_user_id=? OR challenged_user_id=?)
        ORDER BY completed_at_ms DESC, id DESC
        LIMIT ?
        """,
        (int(user_id), int(user_id), safe_limit),
    )
    ids: List[int] = []
    for row in rows:
        ids.extend([int(row["challenger_user_id"]), int(row["challenged_user_id"])])
    user_map = _public_user_map(ids)
    return [_history_row(dict(row), int(user_id), user_map) for row in rows]


def list_challenges_for_user(user_id: int, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    current_ms = _now_ms(now_ms)
    return {
        "incoming": list_incoming_challenges_for_user(int(user_id), now_ms=current_ms),
        "outgoing": list_outgoing_challenges_for_user(int(user_id), now_ms=current_ms),
        "active": get_active_challenge_for_user(int(user_id), now_ms=current_ms),
        "history_preview": get_history_for_user(int(user_id), limit=5, now_ms=current_ms),
    }


def get_challenge_detail(challenge_id: int, viewer_user_id: int, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    current_ms = _now_ms(now_ms)
    row = _refresh_challenge_row(int(challenge_id), now_ms=current_ms)
    if int(viewer_user_id) not in {int(row["challenger_user_id"]), int(row["challenged_user_id"])}:
        raise HTTPException(status_code=403, detail="Not allowed to view this challenge")
    user_map = _public_user_map([int(row["challenger_user_id"]), int(row["challenged_user_id"])])
    challenger = user_map.get(int(row["challenger_user_id"]), {"display_name": "Driver"})
    challenged = user_map.get(int(row["challenged_user_id"]), {"display_name": "Driver"})
    viewer_is_challenger = int(viewer_user_id) == int(row["challenger_user_id"])
    challenger_value = 0.0
    challenged_value = 0.0
    if row["status"] == "completed":
        challenger_value = _round_metric(row.get("challenger_final_value"))
        challenged_value = _round_metric(row.get("challenged_final_value"))
    elif row["status"] == "active" and row.get("period_start_date") and row.get("period_end_date"):
        today = _today_nyc_from_ms(min(current_ms, int(row.get("ends_at_ms") or current_ms)))
        start_date = date.fromisoformat(str(row["period_start_date"]))
        end_date = date.fromisoformat(str(row["period_end_date"]))
        clamped_end = min(today, end_date)
        if clamped_end >= start_date:
            challenger_value = max(0.0, _round_metric(_period_total_for_user(int(row["challenger_user_id"]), str(row["metric_key"]), start_date, clamped_end) - float(row.get("challenger_start_value") or 0.0)))
            challenged_value = max(0.0, _round_metric(_period_total_for_user(int(row["challenged_user_id"]), str(row["metric_key"]), start_date, clamped_end) - float(row.get("challenged_start_value") or 0.0)))
    my_value = challenger_value if viewer_is_challenger else challenged_value
    other_value = challenged_value if viewer_is_challenger else challenger_value
    my_label = challenger.get("display_name") if viewer_is_challenger else challenged.get("display_name")
    other_label = challenged.get("display_name") if viewer_is_challenger else challenger.get("display_name")
    leader: Optional[str] = None
    if my_value > other_value:
        leader = "me"
    elif other_value > my_value:
        leader = "other"
    return {
        "id": int(row["id"]),
        "challenger_user_id": int(row["challenger_user_id"]),
        "challenged_user_id": int(row["challenged_user_id"]),
        "challenger_display_name": challenger.get("display_name") or "Driver",
        "challenged_display_name": challenged.get("display_name") or "Driver",
        "battle_type": _row_to_battle_type(row),
        "metric_key": str(row["metric_key"]),
        "period_key": str(row["period_key"]),
        "status": str(row["status"]),
        "created_at_ms": int(row["created_at_ms"]),
        "accepted_at_ms": int(row["accepted_at_ms"]) if row.get("accepted_at_ms") is not None else None,
        "ends_at_ms": int(row["ends_at_ms"]) if row.get("ends_at_ms") is not None else None,
        "my_current_value": _round_metric(my_value),
        "other_current_value": _round_metric(other_value),
        "my_label": my_label or "Driver",
        "other_label": other_label or "Driver",
        "leader": leader,
        "result_code": row.get("result_code"),
    }
