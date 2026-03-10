from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one
from leaderboard_models import LeaderboardMetric, LeaderboardPeriod

NYC_TZ = ZoneInfo("America/New_York")


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
    return datetime.now(timezone.utc).astimezone(NYC_TZ).date()


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


def _badge_for_rank(period: LeaderboardPeriod, rank: int) -> Optional[str]:
    if rank == 1:
        return "RUBY" if period in (LeaderboardPeriod.monthly, LeaderboardPeriod.yearly) else "CROWN"
    if rank == 2:
        return "GOLD"
    if rank == 3:
        return "SILVER"
    return None


def _display_name(row: Dict) -> str:
    email = (row.get("email") or "Driver").strip()
    fallback = email.split("@")[0] if "@" in email else "Driver"
    return ((row.get("display_name") or "").strip() or fallback)[:28]


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
        ranked.append(
            {
                "user_id": int(row["user_id"]),
                "display_name": _display_name(dict(row)),
                "metric_value": round(float(row["metric_value"] or 0.0), 4),
                "rank_position": idx,
                "badge_code": _badge_for_rank(period, idx),
            }
        )
    return {"metric": metric, "period": period, "period_key": bounds.period_key, "rows": ranked}


def get_leaderboard(metric: LeaderboardMetric, period: LeaderboardPeriod, limit: int = 10) -> Dict:
    board = _aggregate_rows(metric, period)
    board["rows"] = board["rows"][: max(1, min(100, int(limit)))]
    return board


def get_my_rank(user_id: int, metric: LeaderboardMetric, period: LeaderboardPeriod) -> Dict:
    board = _aggregate_rows(metric, period)
    my_row = next((row for row in board["rows"] if int(row["user_id"]) == int(user_id)), None)
    return {"metric": metric, "period": period, "period_key": board["period_key"], "row": my_row}


def refresh_current_badges() -> None:
    now = int(time.time())
    _db_exec(
        "UPDATE leaderboard_badges_current SET is_current=? WHERE is_current=?",
        (_bool_db_value(False), _bool_db_value(True)),
    )

    for metric in [LeaderboardMetric.miles, LeaderboardMetric.hours]:
        for period in [LeaderboardPeriod.daily, LeaderboardPeriod.weekly, LeaderboardPeriod.monthly, LeaderboardPeriod.yearly]:
            board = _aggregate_rows(metric, period)
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
                        metric.value,
                        period.value,
                        board["period_key"],
                        int(row["rank_position"]),
                        row["badge_code"],
                        now,
                        _bool_db_value(True),
                    ),
                )


def get_current_badges_for_user(user_id: int) -> List[Dict]:
    rows = _db_query_all(
        """
        SELECT metric, period, period_key, rank_position, badge_code
        FROM leaderboard_badges_current
        WHERE user_id=? AND is_current=?
        ORDER BY awarded_at DESC, metric, period
        """,
        (int(user_id), _bool_db_value(True)),
    )
    return [dict(r) for r in rows]


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
