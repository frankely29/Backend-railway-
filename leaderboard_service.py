from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one
from leaderboard_models import LeaderboardMetric, LeaderboardPeriod

NYC_TZ = ZoneInfo("America/New_York")

PROGRESSION_XP_PER_MILE = 8
PROGRESSION_XP_PER_HOUR = 30
PROGRESSION_XP_PER_REPORTED_PICKUP = 20
PROGRESSION_MAX_PICKUP_REPORTS_PER_DAY_FOR_XP = 25


def _build_level_xp_thresholds() -> List[int]:
    thresholds = [0]
    total_xp = 0
    for level_index in range(2, 101):
        step_xp = round(120 + ((level_index - 1) * 26) + (((level_index - 1) ** 1.28) * 7))
        total_xp += int(step_xp)
        thresholds.append(total_xp)
    return thresholds


LEVEL_XP_THRESHOLDS = _build_level_xp_thresholds()

RANK_LADDER = [
    (1, 4, "Recruit", "recruit"),
    (5, 8, "Private", "private"),
    (9, 12, "Corporal", "corporal"),
    (13, 16, "Sergeant", "sergeant"),
    (17, 20, "Staff Sergeant", "staff_sergeant"),
    (21, 24, "Sergeant First Class", "sergeant_first_class"),
    (25, 28, "Master Sergeant", "master_sergeant"),
    (29, 32, "Lieutenant", "lieutenant"),
    (33, 36, "Captain", "captain"),
    (37, 40, "Major", "major"),
    (41, 44, "Colonel", "colonel"),
    (45, 52, "Brigadier", "brigadier"),
    (53, 60, "Major General", "major_general"),
    (61, 70, "Lieutenant General", "lieutenant_general"),
    (71, 82, "General", "general"),
    (83, 92, "Commander", "commander"),
    (93, 100, "Road Legend", "road_legend"),
]


def get_rank_ladder() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for start, end, rank_name, rank_icon_key in RANK_LADDER:
        rows.append(
            {
                "start_level": start,
                "end_level": end,
                "rank_name": rank_name,
                "rank_icon_key": rank_icon_key,
            }
        )
    return rows


def _bool_db_value(flag: bool):
    if DB_BACKEND == "postgres":
        return bool(flag)
    return 1 if flag else 0



@dataclass
class PeriodBounds:
    start_date: date
    end_date: date
    period_key: str


def _today_nyc() -> date:
    # Match leaderboard_tracker.py business-day logic:
    # the leaderboard “day” runs from 4 AM NYC to 3:59:59 AM NYC next day.
    return (datetime.now(timezone.utc).astimezone(NYC_TZ) - timedelta(hours=4)).date()


def current_period_bounds(period: LeaderboardPeriod) -> PeriodBounds:
    today = _today_nyc()
    if period == LeaderboardPeriod.daily:
        return PeriodBounds(today, today, today.isoformat())
    if period == LeaderboardPeriod.weekly:
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
        return PeriodBounds(start, end, f"{start.isoformat()}_{end.isoformat()}")
    if period == LeaderboardPeriod.monthly:
        start = today.replace(day=1)
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1)
        else:
            next_month = start.replace(month=start.month + 1)
        end = next_month - timedelta(days=1)
        return PeriodBounds(start, end, start.strftime("%Y-%m"))
    start = today.replace(month=1, day=1)
    end = today.replace(month=12, day=31)
    return PeriodBounds(start, end, str(today.year))


def _metric_column(metric: LeaderboardMetric) -> str:
    return "miles_worked" if metric == LeaderboardMetric.miles else "hours_worked"


def _badge_for_rank(rank: int) -> Optional[str]:
    if rank == 1:
        return "crown"
    if rank == 2:
        return "silver"
    if rank == 3:
        return "bronze"
    return None


def _normalized_badge_code(rank_position: int, badge_code: Optional[str] = None) -> Optional[str]:
    # Always derive the podium badge from current rank position so stale legacy
    # values can never leak crown to rank #2/#3 and non-podium ranks are null.
    return _badge_for_rank(int(rank_position))


def _display_name(row: Dict) -> str:
    email = (row.get("email") or "Driver").strip()
    fallback = email.split("@")[0] if "@" in email else "Driver"
    return ((row.get("display_name") or "").strip() or fallback)[:28]


def _rank_for_level(level: int) -> Dict[str, str]:
    clamped = max(1, min(100, int(level)))
    for start, end, rank_name, rank_icon_key in RANK_LADDER:
        if start <= clamped <= end:
            return {"rank_name": rank_name, "rank_icon_key": rank_icon_key}
    return {"rank_name": "Recruit", "rank_icon_key": "recruit"}


def get_level_from_lifetime_xp(total_xp: int) -> int:
    xp = max(0, int(total_xp or 0))
    if xp >= LEVEL_XP_THRESHOLDS[-1]:
        return 100
    level = 1
    for idx, threshold in enumerate(LEVEL_XP_THRESHOLDS):
        if xp >= threshold:
            level = idx + 1
        else:
            break
    return min(100, max(1, level))


def get_next_level_xp(level: int) -> Optional[int]:
    clamped = max(1, min(100, int(level)))
    if clamped >= 100:
        return None
    return int(LEVEL_XP_THRESHOLDS[clamped])


def get_level_progress_from_lifetime_xp(total_xp: int) -> Dict[str, Any]:
    normalized_xp = max(0, int(total_xp or 0))
    level = get_level_from_lifetime_xp(normalized_xp)
    rank = _rank_for_level(level)
    current_level_xp = int(LEVEL_XP_THRESHOLDS[level - 1])
    next_level_xp = get_next_level_xp(level)
    xp_to_next_level = 0 if next_level_xp is None else max(0, int(next_level_xp) - normalized_xp)
    return {
        "level": level,
        "rank_name": rank["rank_name"],
        "rank_icon_key": rank["rank_icon_key"],
        "title": rank["rank_name"],
        "total_xp": normalized_xp,
        "current_level_xp": current_level_xp,
        "next_level_xp": next_level_xp,
        "xp_to_next_level": xp_to_next_level,
        "max_level_reached": level == 100,
    }


def _build_progression_from_daily_stats_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    lifetime_miles = 0.0
    lifetime_hours = 0.0
    lifetime_pickups_recorded = 0
    miles_xp = 0
    hours_xp = 0
    report_xp = 0

    for raw_row in rows:
        row = dict(raw_row)
        miles = float(row.get("miles_worked") or 0.0)
        hours = float(row.get("hours_worked") or 0.0)
        pickups_recorded = int(row.get("pickups_recorded") or 0)
        pickup_count_for_xp = min(max(0, pickups_recorded), PROGRESSION_MAX_PICKUP_REPORTS_PER_DAY_FOR_XP)

        lifetime_miles += miles
        lifetime_hours += hours
        lifetime_pickups_recorded += max(0, pickups_recorded)

        miles_xp += round(miles * PROGRESSION_XP_PER_MILE)
        hours_xp += round(hours * PROGRESSION_XP_PER_HOUR)
        report_xp += pickup_count_for_xp * PROGRESSION_XP_PER_REPORTED_PICKUP

    total_xp = int(miles_xp + hours_xp + report_xp)
    progression = get_level_progress_from_lifetime_xp(total_xp)
    progression["lifetime_miles"] = round(lifetime_miles, 4)
    progression["lifetime_hours"] = round(lifetime_hours, 4)
    progression["lifetime_pickups_recorded"] = int(lifetime_pickups_recorded)
    progression["xp_breakdown"] = {
        "miles_xp": int(miles_xp),
        "hours_xp": int(hours_xp),
        "report_xp": int(report_xp),
    }
    return progression


def get_lifetime_totals_for_user(user_id: int) -> Dict[str, float]:
    row = _db_query_one(
        """
        SELECT
          COALESCE(SUM(miles_worked), 0) AS miles_worked,
          COALESCE(SUM(hours_worked), 0) AS hours_worked,
          COALESCE(SUM(pickups_recorded), 0) AS pickups_recorded
        FROM driver_daily_stats
        WHERE user_id=?
        """,
        (int(user_id),),
    )
    return {
        "miles": round(float(row["miles_worked"] or 0.0), 4),
        "hours": round(float(row["hours_worked"] or 0.0), 4),
        "pickups": int(row["pickups_recorded"] or 0),
    }


def get_progression_for_user(user_id: int) -> Dict[str, Any]:
    by_user = get_progression_for_users([int(user_id)])
    return by_user.get(int(user_id), _build_progression_from_daily_stats_rows([]))


def get_progression_for_users(user_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    clean_user_ids = [int(uid) for uid in user_ids]
    if not clean_user_ids:
        return {}

    placeholders = ",".join(["?" for _ in clean_user_ids])
    rows = _db_query_all(
        f"""
        SELECT user_id,
               COALESCE(miles_worked, 0) AS miles_worked,
               COALESCE(hours_worked, 0) AS hours_worked,
               COALESCE(pickups_recorded, 0) AS pickups_recorded
        FROM driver_daily_stats
        WHERE user_id IN ({placeholders})
        """,
        tuple(clean_user_ids),
    )

    rows_by_user: Dict[int, List[Dict[str, Any]]] = {uid: [] for uid in clean_user_ids}
    for row in rows:
        rows_by_user.setdefault(int(row["user_id"]), []).append(dict(row))
    return {uid: _build_progression_from_daily_stats_rows(rows_by_user.get(uid, [])) for uid in clean_user_ids}


def _enrich_rows_with_progression(rows: List[Dict]) -> None:
    if not rows:
        return
    progression_by_user = get_progression_for_users([int(row["user_id"]) for row in rows])
    for row in rows:
        progression = progression_by_user.get(int(row["user_id"])) or {}
        row["level"] = progression.get("level")
        row["rank_name"] = progression.get("rank_name")
        row["rank_icon_key"] = progression.get("rank_icon_key")
        row["title"] = progression.get("title")


def _aggregate_rows(metric: LeaderboardMetric, period: LeaderboardPeriod) -> Dict:
    bounds = current_period_bounds(period)
    metric_col = _metric_column(metric)
    rows = _db_query_all(
        f"""
        SELECT s.user_id,
               u.display_name,
               u.email,
               COALESCE(SUM(s.{metric_col}), 0) AS metric_value
        FROM driver_daily_stats s
        JOIN users u ON u.id = s.user_id
        WHERE s.nyc_date >= ? AND s.nyc_date <= ?
        GROUP BY s.user_id, u.display_name, u.email
        ORDER BY metric_value DESC, s.user_id ASC
        """,
        (bounds.start_date.isoformat(), bounds.end_date.isoformat()),
    )
    ranked: List[Dict] = []
    for idx, row in enumerate(rows, start=1):
        badge_code = _badge_for_rank(idx)
        ranked.append(
            {
                "user_id": int(row["user_id"]),
                "display_name": _display_name(dict(row)),
                "metric_value": round(float(row["metric_value"] or 0.0), 4),
                "rank_position": idx,
                "badge_code": badge_code,
            }
        )
    return {"metric": metric, "period": period, "period_key": bounds.period_key, "rows": ranked}


def get_leaderboard(metric: LeaderboardMetric, period: LeaderboardPeriod, limit: int = 10) -> Dict:
    board = _aggregate_rows(metric, period)
    board["rows"] = board["rows"][: max(1, min(100, int(limit)))]
    _enrich_rows_with_progression(board["rows"])
    return board


def get_my_rank(user_id: int, metric: LeaderboardMetric, period: LeaderboardPeriod) -> Dict:
    bounds = current_period_bounds(period)
    metric_col = _metric_column(metric)

    my_totals = _db_query_one(
        f"""
        SELECT s.user_id,
               u.display_name,
               u.email,
               COALESCE(SUM(s.{metric_col}), 0) AS metric_value
        FROM driver_daily_stats s
        JOIN users u ON u.id = s.user_id
        WHERE s.user_id = ? AND s.nyc_date >= ? AND s.nyc_date <= ?
        GROUP BY s.user_id, u.display_name, u.email
        LIMIT 1
        """,
        (int(user_id), bounds.start_date.isoformat(), bounds.end_date.isoformat()),
    )

    if not my_totals:
        return {"metric": metric, "period": period, "period_key": bounds.period_key, "row": None}

    metric_value = float(my_totals["metric_value"] or 0.0)
    better_count_row = _db_query_one(
        f"""
        SELECT COUNT(*) AS better_count
        FROM (
          SELECT s.user_id, COALESCE(SUM(s.{metric_col}), 0) AS metric_value
          FROM driver_daily_stats s
          WHERE s.nyc_date >= ? AND s.nyc_date <= ?
          GROUP BY s.user_id
        ) ranked
        WHERE ranked.metric_value > ?
           OR (ranked.metric_value = ? AND ranked.user_id < ?)
        """,
        (bounds.start_date.isoformat(), bounds.end_date.isoformat(), metric_value, metric_value, int(user_id)),
    )
    rank_position = int((better_count_row["better_count"] or 0) + 1)

    row = {
        "user_id": int(my_totals["user_id"]),
        "display_name": _display_name(dict(my_totals)),
        "metric_value": round(metric_value, 4),
        "rank_position": rank_position,
        "badge_code": _badge_for_rank(rank_position),
    }
    progression = get_progression_for_users([int(my_totals["user_id"])])
    row_progression = progression.get(int(my_totals["user_id"])) or {}
    row["level"] = row_progression.get("level")
    row["rank_name"] = row_progression.get("rank_name")
    row["rank_icon_key"] = row_progression.get("rank_icon_key")
    row["title"] = row_progression.get("title")
    return {"metric": metric, "period": period, "period_key": bounds.period_key, "row": row}


def refresh_current_badges() -> None:
    now = int(time.time())

    # Current badges must be DAILY miles only.
    _db_exec(
        "DELETE FROM leaderboard_badges_current WHERE metric<>? OR period<>?",
        (LeaderboardMetric.miles.value, LeaderboardPeriod.daily.value),
    )

    board = _aggregate_rows(LeaderboardMetric.miles, LeaderboardPeriod.daily)

    _db_exec(
        "DELETE FROM leaderboard_badges_current WHERE metric=? AND period=?",
        (LeaderboardMetric.miles.value, LeaderboardPeriod.daily.value),
    )

    for row in board["rows"][:3]:
        if not row["badge_code"]:
            continue
        _db_exec(
            """
            INSERT INTO leaderboard_badges_current(user_id, metric, period, period_key, rank_position, badge_code, awarded_at, is_current)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(user_id, metric, period, period_key) DO UPDATE SET
              rank_position=excluded.rank_position,
              badge_code=excluded.badge_code,
              awarded_at=excluded.awarded_at,
              is_current=excluded.is_current
            """,
            (
                int(row["user_id"]),
                LeaderboardMetric.miles.value,
                LeaderboardPeriod.daily.value,
                board["period_key"],
                int(row["rank_position"]),
                _normalized_badge_code(int(row["rank_position"]), row.get("badge_code")),
                now,
                _bool_db_value(True),
            ),
        )

    source_row = _db_query_one("SELECT COALESCE(MAX(updated_at), 0) AS max_updated_at FROM driver_daily_stats")
    _db_exec(
        """
        INSERT INTO leaderboard_badges_refresh_state(id, daily_period_key, weekly_period_key, monthly_period_key, yearly_period_key, source_updated_at, refreshed_at)
        VALUES(1,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
          daily_period_key=excluded.daily_period_key,
          weekly_period_key=excluded.weekly_period_key,
          monthly_period_key=excluded.monthly_period_key,
          yearly_period_key=excluded.yearly_period_key,
          source_updated_at=excluded.source_updated_at,
          refreshed_at=excluded.refreshed_at
        """,
        (
            current_period_bounds(LeaderboardPeriod.daily).period_key,
            current_period_bounds(LeaderboardPeriod.weekly).period_key,
            current_period_bounds(LeaderboardPeriod.monthly).period_key,
            current_period_bounds(LeaderboardPeriod.yearly).period_key,
            int(source_row["max_updated_at"] or 0) if source_row else 0,
            now,
        ),
    )


def refresh_current_badges_if_needed(max_staleness_seconds: int = 30) -> None:
    now = int(time.time())
    expected_keys = {
        "daily_period_key": current_period_bounds(LeaderboardPeriod.daily).period_key,
        "weekly_period_key": current_period_bounds(LeaderboardPeriod.weekly).period_key,
        "monthly_period_key": current_period_bounds(LeaderboardPeriod.monthly).period_key,
        "yearly_period_key": current_period_bounds(LeaderboardPeriod.yearly).period_key,
    }
    source_row = _db_query_one("SELECT COALESCE(MAX(updated_at), 0) AS max_updated_at FROM driver_daily_stats")
    source_updated_at = int(source_row["max_updated_at"] or 0) if source_row else 0

    state = _db_query_one(
        """
        SELECT daily_period_key, weekly_period_key, monthly_period_key, yearly_period_key, source_updated_at, refreshed_at
        FROM leaderboard_badges_refresh_state
        WHERE id=1
        LIMIT 1
        """
    )
    if state:
        state_dict = dict(state)
        keys_match = all((state_dict.get(key) or "") == value for key, value in expected_keys.items())
        recently_refreshed = now - int(state_dict.get("refreshed_at") or 0) <= max(1, int(max_staleness_seconds))
        source_match = int(state_dict.get("source_updated_at") or 0) == source_updated_at
        if keys_match and recently_refreshed and source_match:
            return

    refresh_current_badges()


def get_current_badges_for_user(user_id: int) -> List[Dict]:
    refresh_current_badges_if_needed()
    rows = _db_query_all(
        """
        SELECT metric, period, period_key, rank_position, badge_code
        FROM leaderboard_badges_current
        WHERE user_id=? AND is_current=? AND metric=? AND period=? AND rank_position IN (1,2,3)
        ORDER BY awarded_at DESC, rank_position ASC
        """,
        (
            int(user_id),
            _bool_db_value(True),
            LeaderboardMetric.miles.value,
            LeaderboardPeriod.daily.value,
        ),
    )
    normalized_rows: List[Dict] = []
    for row in rows:
        item = dict(row)
        item["badge_code"] = _normalized_badge_code(int(item.get("rank_position") or 0), item.get("badge_code"))
        if item["badge_code"]:
            normalized_rows.append(item)
    return normalized_rows



def get_best_current_badge_for_user(user_id: int) -> Dict:
    refresh_current_badges_if_needed()
    rows = _db_query_all(
        """
        SELECT user_id, metric, period, period_key, rank_position
        FROM leaderboard_badges_current
        WHERE user_id=? AND is_current=? AND metric=? AND period=? AND rank_position IN (1,2,3)
        """,
        (
            int(user_id),
            _bool_db_value(True),
            LeaderboardMetric.miles.value,
            LeaderboardPeriod.daily.value,
        ),
    )
    if not rows:
        return {"leaderboard_badge_code": None}
    best = min((dict(row) for row in rows), key=lambda item: int(item.get("rank_position") or 999))
    return {"leaderboard_badge_code": _badge_for_rank(int(best.get("rank_position") or 0))}


def get_best_current_badges_for_users(user_ids: List[int]) -> Dict[int, Dict]:
    refresh_current_badges_if_needed()
    if not user_ids:
        return {}
    placeholders = ",".join(["?" for _ in user_ids])
    rows = _db_query_all(
        f"""
        SELECT user_id, metric, period, period_key, rank_position
        FROM leaderboard_badges_current
        WHERE is_current=? AND metric=? AND period=? AND rank_position IN (1,2,3) AND user_id IN ({placeholders})
        """,
        (
            _bool_db_value(True),
            LeaderboardMetric.miles.value,
            LeaderboardPeriod.daily.value,
            *[int(u) for u in user_ids],
        ),
    )

    by_user: Dict[int, List[Dict]] = {}
    for row in rows:
        item = dict(row)
        uid = int(item["user_id"])
        by_user.setdefault(uid, []).append(item)

    out: Dict[int, Dict] = {}
    for uid, badges in by_user.items():
        best = min(badges, key=lambda item: int(item.get("rank_position") or 999))
        out[uid] = {"leaderboard_badge_code": _badge_for_rank(int(best.get("rank_position") or 0))}
    return out


def _sum_for_user(user_id: int, start_date: date, end_date: date) -> Dict:
    row = _db_query_one(
        """
        SELECT
          COALESCE(SUM(miles_worked), 0) AS miles_worked,
          COALESCE(SUM(hours_worked), 0) AS hours_worked,
          COALESCE(SUM(pickups_recorded), 0) AS pickups_recorded
        FROM driver_daily_stats
        WHERE user_id=? AND nyc_date >= ? AND nyc_date <= ?
        """,
        (int(user_id), start_date.isoformat(), end_date.isoformat()),
    )
    return {
        "miles": round(float(row["miles_worked"] or 0.0), 4),
        "hours": round(float(row["hours_worked"] or 0.0), 4),
        "pickups": int(row["pickups_recorded"] or 0),
    }


def get_overview_for_user(user_id: int) -> Dict:
    out: Dict[str, Dict[str, float]] = {}
    for period in [LeaderboardPeriod.daily, LeaderboardPeriod.weekly, LeaderboardPeriod.monthly, LeaderboardPeriod.yearly]:
        bounds = current_period_bounds(period)
        out[period.value] = _sum_for_user(user_id, bounds.start_date, bounds.end_date)
    return out
