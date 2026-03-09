from __future__ import annotations

import threading
import time
from typing import Optional

from leaderboard_mailer import render_report_email, report_emails_enabled, send_report_email
from leaderboard_models import LeaderboardMetric
from leaderboard_service import (
    all_users_with_email,
    get_email_prefs,
    get_period_summary_for_user,
    get_rank_for_user_in_bounds,
    log_report_attempt,
    previous_completed_period_bounds,
    was_report_sent,
)

_scheduler_thread: Optional[threading.Thread] = None
_stop_evt = threading.Event()


def _badge_from_ranks(miles_rank: int | None, hours_rank: int | None) -> str | None:
    best = min([r for r in [miles_rank, hours_rank] if r is not None], default=None)
    if best == 1:
        return "RUBY"
    if best == 2:
        return "GOLD"
    if best == 3:
        return "SILVER"
    return None


def run_report_cycle() -> None:
    if not report_emails_enabled():
        return

    for report_type in ["weekly", "monthly", "yearly"]:
        bounds = previous_completed_period_bounds(report_type)
        for user in all_users_with_email():
            uid = int(user["id"])
            prefs = get_email_prefs(uid)
            if not prefs.get(f"{report_type}_enabled", False):
                continue
            if was_report_sent(uid, report_type, bounds.period_key):
                continue

            try:
                summary = get_period_summary_for_user(uid, bounds.start_date, bounds.end_date)
                miles_rank = get_rank_for_user_in_bounds(uid, LeaderboardMetric.miles, bounds.start_date, bounds.end_date)
                hours_rank = get_rank_for_user_in_bounds(uid, LeaderboardMetric.hours, bounds.start_date, bounds.end_date)
                badge = _badge_from_ranks(miles_rank, hours_rank)
                subject, text_body, html_body = render_report_email(
                    (user.get("display_name") or "Driver"),
                    report_type,
                    bounds.period_key,
                    summary,
                    miles_rank,
                    hours_rank,
                    badge,
                )
                send_report_email(str(user["email"]), subject, text_body, html_body)
                log_report_attempt(uid, report_type, bounds.period_key, "sent")
            except Exception as exc:
                log_report_attempt(uid, report_type, bounds.period_key, "failed", str(exc))


def _scheduler_worker(interval_seconds: int) -> None:
    while not _stop_evt.is_set():
        run_report_cycle()
        _stop_evt.wait(timeout=interval_seconds)


def start_leaderboard_scheduler(interval_seconds: int = 3600) -> None:
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _stop_evt.clear()
    _scheduler_thread = threading.Thread(target=_scheduler_worker, args=(max(300, int(interval_seconds)),), daemon=True)
    _scheduler_thread.start()


def stop_leaderboard_scheduler() -> None:
    _stop_evt.set()
