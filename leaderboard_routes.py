from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends

from core import require_user
from leaderboard_models import (
    LeaderboardMetric,
    LeaderboardPeriod,
    LeaderboardResponse,
    MyBadgesResponse,
    MyProgressionResponse,
    MyRankResponse,
    OverviewResponse,
)
from leaderboard_service import (
    get_current_badges_for_user,
    get_leaderboard,
    get_my_rank,
    get_overview_for_user,
    get_progression_for_user,
)

router = APIRouter(tags=["leaderboard"])


@router.get("/leaderboard", response_model=LeaderboardResponse)
def leaderboard(metric: LeaderboardMetric, period: LeaderboardPeriod, limit: int = 10, user: sqlite3.Row = Depends(require_user)):
    data = get_leaderboard(metric, period, limit=limit)
    return {"ok": True, **data}


@router.get("/leaderboard/me", response_model=MyRankResponse)
def leaderboard_me(metric: LeaderboardMetric, period: LeaderboardPeriod, user: sqlite3.Row = Depends(require_user)):
    data = get_my_rank(int(user["id"]), metric, period)
    return {"ok": True, **data}


@router.get("/leaderboard/badges/me", response_model=MyBadgesResponse)
def leaderboard_badges_me(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "badges": get_current_badges_for_user(int(user["id"]))}


@router.get("/leaderboard/overview/me", response_model=OverviewResponse)
def leaderboard_overview_me(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **get_overview_for_user(int(user["id"]))}


@router.get("/leaderboard/progression/me", response_model=MyProgressionResponse)
def leaderboard_progression_me(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "progression": get_progression_for_user(int(user["id"]))}
