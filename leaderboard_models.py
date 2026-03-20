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
    level: Optional[int] = None
    rank_name: Optional[str] = None
    rank_icon_key: Optional[str] = None
    title: Optional[str] = None


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
    pickups: int = 0


class OverviewResponse(BaseModel):
    ok: bool = True
    daily: PeriodTotals
    weekly: PeriodTotals
    monthly: PeriodTotals
    yearly: PeriodTotals


class ProgressionXpBreakdown(BaseModel):
    miles_xp: int
    hours_xp: int
    report_xp: int


class ProgressionPayload(BaseModel):
    level: int
    rank_name: str
    rank_icon_key: str
    total_xp: int
    current_level_xp: int
    next_level_xp: Optional[int] = None
    xp_to_next_level: int
    max_level_reached: bool
    lifetime_miles: float
    lifetime_hours: float
    lifetime_pickups_recorded: int
    xp_breakdown: ProgressionXpBreakdown


class MyProgressionResponse(BaseModel):
    ok: bool = True
    progression: ProgressionPayload


class RankLadderRow(BaseModel):
    start_level: int
    end_level: int
    rank_name: str
    rank_icon_key: str


class RankLadderResponse(BaseModel):
    ok: bool = True
    rows: List[RankLadderRow]