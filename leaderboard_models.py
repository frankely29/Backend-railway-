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
    """Where a driver's XP came from.

    Declared in full, or Pydantic drops the new sources on the way out and the
    breakdown silently under-reports -- the same filter that cost the ladder
    its prestige pair and the feed its rank key. Defaulted to 0 so a client
    reading a cached payload from before social XP existed still parses.
    """
    miles_xp: int
    hours_xp: int
    report_xp: int
    game_xp: int
    post_xp: int = 0
    comment_xp: int = 0
    like_xp: int = 0


class ProgressionPayload(BaseModel):
    level: int
    rank_name: str
    title: str
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
    # Declared, or Pydantic drops them on the way out.
    #
    # get_rank_ladder() has carried prestige and rank since the ladder was
    # reshaped, and the service test asserted it did. The response model did
    # not list them, so the endpoint returned prestige: null regardless, the
    # app fell back to numbering its rows 1..30, and a driver saw
    # "Warlord II - Prestige 14" instead of prestige 5, rank 2.
    #
    # A test that exercises the function cannot see this. The one that guards
    # it now goes through the HTTP route.
    prestige: int
    rank: int


class RankLadderResponse(BaseModel):
    ok: bool = True
    rows: List[RankLadderRow]
