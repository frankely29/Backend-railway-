from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class LeaderboardMetric(str, Enum):
    miles = "miles"
    hours = "hours"


class LeaderboardPeriod(str, Enum):
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"
    yearly = "yearly"


class LeaderboardRow(BaseModel):
    user_id: int
    display_name: str
    metric_value: float
    rank_position: int
    badge_code: Optional[str] = None


class MyRankRow(LeaderboardRow):
    pass


class BadgeRow(BaseModel):
    metric: LeaderboardMetric
    period: LeaderboardPeriod
    period_key: str
    rank_position: int
    badge_code: str


class LeaderboardResponse(BaseModel):
    ok: bool = True
    metric: LeaderboardMetric
    period: LeaderboardPeriod
    period_key: str
    rows: List[LeaderboardRow]


class MyRankResponse(BaseModel):
    ok: bool = True
    metric: LeaderboardMetric
    period: LeaderboardPeriod
    period_key: str
    row: Optional[MyRankRow] = None


class MyBadgesResponse(BaseModel):
    ok: bool = True
    badges: List[BadgeRow]


class PeriodTotals(BaseModel):
    miles: float
    hours: float


class OverviewResponse(BaseModel):
    ok: bool = True
    daily: PeriodTotals
    weekly: PeriodTotals
    monthly: PeriodTotals
    yearly: PeriodTotals
