from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one
from leaderboard_models import LeaderboardMetric, LeaderboardPeriod

NYC_TZ = ZoneInfo("America/New_York")

# Phase 1 fixed lifetime-miles progression (authoritative thresholds for levels 1..100).
LEVEL_MILES_THRESHOLDS = [
    0,
    25,
    50,
    75,
    120,
    160,
    195,
    230,
    265,
    300,
    350,
    400,
    450,
    500,
    550,
    620,
    690,
    760,
    830,
    900,
    1000,
    1100,
    1200,
    1300,
    1380,
    1464,
    1548,
    1632,
    1716,
    1800,
    1910,
    2020,
    2130,
    2240,
    2350,
    2460,
    2570,
    2680,
    2790,
    2900,
    3011,
    3122,
    3233,
    3344,
    3456,
    3567,
    3678,
    3789,
    3900,
    4050,
    4185,
    4320,
    4455,
    4590,
    4725,
    4860,
    4995,
    5130,
    5265,
    5400,
    5550,
    5700,
    5850,
    6000,
    6150,
    6300,
    6450,
    6600,
    6750,
    6900,
    7075,
    7250,
    7425,
    7600,
    7800,
    7920,
    8040,
    8160,
    8280,
    8400,
    8520,
    8640,
    8760,
    8880,
    9000,
    9070,
    9140,
    9210,
    9280,
    9350,
    9420,
    9490,
    9560,
    9630,
    9700,
    9762,
    9825,
    9888,
    9950,
    10000,
]


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


def get_level_from_lifetime_miles(lifetime_miles: float) -> int:
    miles = max(0.0, float(lifetime_miles or 0.0))
    if miles >= LEVEL_MILES_THRESHOLDS[-1]:
        return 100
    level = 1
    for idx, threshold in enumerate(LEVEL_MILES_THRESHOLDS):
        if miles >= threshold:
            level = idx + 1
        else:
            break
    return min(100, max(1, level))


def get_title_from_level(level: int) -> str:
    clamped = max(1, min(100, int(level)))
    if clamped == 100:
        return "Legend"
    if clamped >= 75:
        return "Veteran"
    if clamped >= 50:
        return "Pro"
    if clamped >= 25:
        return "Driver"
    return "Rookie"


def get_next_level_miles(level: int) -> Optional[int]:
    clamped = max(1, min(100, int(level)))
    if clamped >= 100:
        return None
    return int(LEVEL_MILES_THRESHOLDS[clamped])


def get_level_progress_from_lifetime_miles(lifetime_miles: float) -> Dict[str, Any]:
    normalized_miles = round(max(0.0, float(lifetime_miles or 0.0)), 4)
    level = get_level_from_lifetime_miles(normalized_miles)
    current_level_miles = int(LEVEL_MILES_THRESHOLDS[level - 1])
    next_level_miles = get_next_level_miles(level)
    miles_to_next_level = 0.0 if next_level_miles is None else round(max(0.0, float(next_level_miles) - normalized_miles), 4)
    return {
        "level": level,
        "title": get_title_from_level(level),
        "lifetime_miles": normalized_miles,
        "current_level_miles": current_level_miles,
        "next_level_miles": next_level_miles,
        "miles_to_next_level": miles_to_next_level,
        "max_level_reached": level == 100,
    }


def get_lifetime_totals_for_user(user_id: int) -> Dict[str, float]:
    row = _db_query_one(
        """
        SELECT
          COALESCE(SUM(miles_worked), 0) AS miles_worked,
          COALESCE(SUM(hours_worked), 0) AS hours_worked
        FROM driver_daily_stats
        WHERE user_id=?
        """,
        (int(user_id),),
    )
    return {
        "miles": round(float(row["miles_worked"] or 0.0), 4),
        "hours": round(float(row["hours_worked"] or 0.0), 4),
    }


def get_progression_for_users(user_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    clean_user_ids = [int(uid) for uid in user_ids]
    if not clean_user_ids:
        return {}

    placeholders = ",".join(["?" for _ in clean_user_ids])
    rows = _db_query_all(
        f"""
        SELECT user_id, COALESCE(SUM(miles_worked), 0) AS lifetime_miles
        FROM driver_daily_stats
        WHERE user_id IN ({placeholders})
        GROUP BY user_id
        """,
        tuple(clean_user_ids),
    )

    miles_by_user: Dict[int, float] = {int(row["user_id"]): float(row["lifetime_miles"] or 0.0) for row in rows}
    return {uid: get_level_progress_from_lifetime_miles(miles_by_user.get(uid, 0.0)) for uid in clean_user_ids}


def _enrich_rows_with_progression(rows: List[Dict]) -> None:
    if not rows:
        return
    progression_by_user = get_progression_for_users([int(row["user_id"]) for row in rows])
    for row in rows:
        progression = progression_by_user.get(int(row["user_id"])) or {}
        row["level"] = progression.get("level")
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
          COALESCE(SUM(hours_worked), 0) AS hours_worked
        FROM driver_daily_stats
        WHERE user_id=? AND nyc_date >= ? AND nyc_date <= ?
        """,
        (int(user_id), start_date.isoformat(), end_date.isoformat()),
    )
    return {
        "miles": round(float(row["miles_worked"] or 0.0), 4),
        "hours": round(float(row["hours_worked"] or 0.0), 4),
    }


def get_overview_for_user(user_id: int) -> Dict:
    out: Dict[str, Dict[str, float]] = {}
    for period in [LeaderboardPeriod.daily, LeaderboardPeriod.weekly, LeaderboardPeriod.monthly, LeaderboardPeriod.yearly]:
        bounds = current_period_bounds(period)
        out[period.value] = _sum_for_user(user_id, bounds.start_date, bounds.end_date)
    return out
