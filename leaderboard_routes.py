from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends

from core import require_user
from leaderboard_models import (
    EmailPrefsPayload,
    EmailPrefsResponse,
    LeaderboardPeriod,
    LeaderboardResponse,
    LeaderboardMetric,
    MyBadgesResponse,
    MyRankResponse,
)
from leaderboard_service import (
    get_current_badges_for_user,
    get_email_prefs,
    get_leaderboard,
    get_my_rank,
    refresh_current_badges,
    update_email_prefs,
)

router = APIRouter(tags=["leaderboard"])


@router.get("/leaderboard", response_model=LeaderboardResponse)
def leaderboard(metric: LeaderboardMetric, period: LeaderboardPeriod, limit: int = 10, user: sqlite3.Row = Depends(require_user)):
    refresh_current_badges()
    data = get_leaderboard(metric, period, limit=limit)
    return {"ok": True, **data}


@router.get("/leaderboard/me", response_model=MyRankResponse)
def leaderboard_me(metric: LeaderboardMetric, period: LeaderboardPeriod, user: sqlite3.Row = Depends(require_user)):
    refresh_current_badges()
    data = get_my_rank(int(user["id"]), metric, period)
    return {"ok": True, **data}


@router.get("/leaderboard/badges/me", response_model=MyBadgesResponse)
def leaderboard_badges_me(user: sqlite3.Row = Depends(require_user)):
    refresh_current_badges()
    return {"ok": True, "badges": get_current_badges_for_user(int(user["id"]))}


@router.get("/leaderboard/email_prefs", response_model=EmailPrefsResponse)
def leaderboard_email_prefs(user: sqlite3.Row = Depends(require_user)):
    prefs = get_email_prefs(int(user["id"]))
    return {"ok": True, **prefs}


@router.post("/leaderboard/email_prefs", response_model=EmailPrefsResponse)
def leaderboard_email_prefs_update(payload: EmailPrefsPayload, user: sqlite3.Row = Depends(require_user)):
    prefs = update_email_prefs(
        int(user["id"]),
        weekly_enabled=payload.weekly_enabled,
        monthly_enabled=payload.monthly_enabled,
        yearly_enabled=payload.yearly_enabled,
    )
    return {"ok": True, **prefs}
