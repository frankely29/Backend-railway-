from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from core import _db_exec, _db_query_all, _db_query_one
from leaderboard_models import LeaderboardMetric, LeaderboardPeriod

NYC_TZ = ZoneInfo("America/New_York")


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


def previous_completed_period_bounds(report_type: str) -> PeriodBounds:
    today = _today_nyc()
    if report_type == "weekly":
        this_week_start = today - timedelta(days=today.weekday())
        prev_start = this_week_start - timedelta(days=7)
        prev_end = this_week_start - timedelta(days=1)
        return PeriodBounds(prev_start, prev_end, f"{prev_start.isoformat()}_{prev_end.isoformat()}")
    if report_type == "monthly":
        this_month_start = today.replace(day=1)
        prev_end = this_month_start - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
        return PeriodBounds(prev_start, prev_end, prev_start.strftime("%Y-%m"))
    if report_type == "yearly":
        y = today.year - 1
        return PeriodBounds(date(y, 1, 1), date(y, 12, 31), str(y))
    raise ValueError("Unsupported report type")


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


def get_leaderboard(metric: LeaderboardMetric, period: LeaderboardPeriod, limit: int = 10) -> Dict:
    bounds = current_period_bounds(period)
    metric_col = _metric_column(metric)
    rows = _db_query_all(
        f"""
        SELECT s.user_id,
               u.display_name,
               u.email,
               SUM(s.{metric_col}) AS metric_value
        FROM driver_daily_stats s
        JOIN users u ON u.id = s.user_id
        WHERE s.nyc_date >= ? AND s.nyc_date <= ?
        GROUP BY s.user_id, u.display_name, u.email
        ORDER BY metric_value DESC, s.user_id ASC
        LIMIT ?
        """,
        (bounds.start_date.isoformat(), bounds.end_date.isoformat(), max(1, min(100, int(limit)))),
    )

    result_rows: List[Dict] = []
    for idx, r in enumerate(rows, start=1):
        email = (r["email"] or "Driver").strip()
        badge = _badge_for_rank(period, idx)
        result_rows.append(
            {
                "user_id": int(r["user_id"]),
                "display_name": (r["display_name"] or (email.split("@")[0] if "@" in email else "Driver")),
                "metric_value": round(float(r["metric_value"] or 0.0), 4),
                "rank_position": idx,
                "badge_code": badge,
            }
        )
    return {"metric": metric, "period": period, "period_key": bounds.period_key, "rows": result_rows}


def get_my_rank(user_id: int, metric: LeaderboardMetric, period: LeaderboardPeriod) -> Dict:
    board = get_leaderboard(metric, period, limit=10000)
    my_row = next((row for row in board["rows"] if int(row["user_id"]) == int(user_id)), None)
    return {"metric": metric, "period": period, "period_key": board["period_key"], "row": my_row}


def refresh_current_badges() -> None:
    now = int(time.time())
    _db_exec("UPDATE leaderboard_badges_current SET is_current=0 WHERE is_current=1")
    for metric in [LeaderboardMetric.miles, LeaderboardMetric.hours]:
        for period in [LeaderboardPeriod.daily, LeaderboardPeriod.weekly, LeaderboardPeriod.monthly, LeaderboardPeriod.yearly]:
            board = get_leaderboard(metric, period, limit=3)
            for row in board["rows"]:
                badge = _badge_for_rank(period, int(row["rank_position"]))
                if not badge:
                    continue
                _db_exec(
                    """
                    INSERT INTO leaderboard_badges_current(user_id, metric, period, rank_position, badge_code, period_key, awarded_at, is_current)
                    VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (int(row["user_id"]), metric.value, period.value, int(row["rank_position"]), badge, board["period_key"], now, 1),
                )


def get_current_badges_for_user(user_id: int) -> List[Dict]:
    rows = _db_query_all(
        """
        SELECT metric, period, period_key, rank_position, badge_code
        FROM leaderboard_badges_current
        WHERE user_id=? AND is_current=1
        ORDER BY awarded_at DESC
        """,
        (int(user_id),),
    )
    return [dict(r) for r in rows]


def get_email_prefs(user_id: int) -> Dict:
    row = _db_query_one("SELECT * FROM leaderboard_email_prefs WHERE user_id=? LIMIT 1", (int(user_id),))
    if not row:
        now = int(time.time())
        _db_exec(
            "INSERT INTO leaderboard_email_prefs(user_id, weekly_enabled, monthly_enabled, yearly_enabled, created_at, updated_at) VALUES(?,?,?,?,?,?)",
            (int(user_id), 1, 1, 1, now, now),
        )
        return {"weekly_enabled": True, "monthly_enabled": True, "yearly_enabled": True}
    return {
        "weekly_enabled": bool(int(row["weekly_enabled"])),
        "monthly_enabled": bool(int(row["monthly_enabled"])),
        "yearly_enabled": bool(int(row["yearly_enabled"])),
    }


def update_email_prefs(user_id: int, weekly_enabled: bool, monthly_enabled: bool, yearly_enabled: bool) -> Dict:
    now = int(time.time())
    _db_exec(
        """
        INSERT INTO leaderboard_email_prefs(user_id, weekly_enabled, monthly_enabled, yearly_enabled, created_at, updated_at)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
          weekly_enabled=excluded.weekly_enabled,
          monthly_enabled=excluded.monthly_enabled,
          yearly_enabled=excluded.yearly_enabled,
          updated_at=excluded.updated_at
        """,
        (int(user_id), int(weekly_enabled), int(monthly_enabled), int(yearly_enabled), now, now),
    )
    return {"weekly_enabled": weekly_enabled, "monthly_enabled": monthly_enabled, "yearly_enabled": yearly_enabled}


def get_period_summary_for_user(user_id: int, start_date: date, end_date: date) -> Dict:
    row = _db_query_one(
        """
        SELECT
          COALESCE(SUM(miles_worked), 0) AS miles_worked,
          COALESCE(SUM(hours_worked), 0) AS hours_worked,
          COALESCE(SUM(trips_recorded), 0) AS trips_recorded,
          COALESCE(SUM(pickups_recorded), 0) AS pickups_recorded
        FROM driver_daily_stats
        WHERE user_id=? AND nyc_date >= ? AND nyc_date <= ?
        """,
        (int(user_id), start_date.isoformat(), end_date.isoformat()),
    )
    return {
        "miles_worked": round(float(row["miles_worked"] or 0.0), 3),
        "hours_worked": round(float(row["hours_worked"] or 0.0), 3),
        "trips_recorded": int(row["trips_recorded"] or 0),
        "pickups_recorded": int(row["pickups_recorded"] or 0),
    }


def get_rank_for_user_in_bounds(user_id: int, metric: LeaderboardMetric, start_date: date, end_date: date) -> Optional[int]:
    metric_col = _metric_column(metric)
    rows = _db_query_all(
        f"""
        SELECT user_id, SUM({metric_col}) AS metric_value
        FROM driver_daily_stats
        WHERE nyc_date >= ? AND nyc_date <= ?
        GROUP BY user_id
        ORDER BY metric_value DESC, user_id ASC
        """,
        (start_date.isoformat(), end_date.isoformat()),
    )
    for idx, row in enumerate(rows, start=1):
        if int(row["user_id"]) == int(user_id):
            return idx
    return None


def all_users_with_email() -> List[Dict]:
    rows = _db_query_all("SELECT id, email, display_name FROM users WHERE email IS NOT NULL AND TRIM(email) != ''")
    return [dict(r) for r in rows]


def was_report_sent(user_id: int, report_type: str, period_key: str) -> bool:
    row = _db_query_one(
        "SELECT 1 AS x FROM leaderboard_report_log WHERE user_id=? AND report_type=? AND period_key=? LIMIT 1",
        (int(user_id), report_type, period_key),
    )
    return bool(row)


def log_report_attempt(user_id: int, report_type: str, period_key: str, status: str, error_message: str = "") -> None:
    _db_exec(
        """
        INSERT INTO leaderboard_report_log(user_id, report_type, period_key, sent_at, status, error_message)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(user_id, report_type, period_key) DO UPDATE SET
          sent_at=excluded.sent_at,
          status=excluded.status,
          error_message=excluded.error_message
        """,
        (int(user_id), report_type, period_key, int(time.time()), status, (error_message or "")[:500]),
    )
