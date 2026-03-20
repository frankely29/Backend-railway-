from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

BattleType = Literal["daily_miles", "daily_hours", "weekly_miles", "weekly_hours"]
ChallengeStatus = Literal["pending", "active", "declined", "canceled", "expired", "completed"]
ResultCode = Literal["challenger_win", "challenged_win", "tie"]


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class WorkBattleCatalogItem(ApiModel):
    battle_type: BattleType
    metric_key: Literal["miles", "hours"]
    period_key: Literal["daily", "weekly"]


class WorkBattleCatalogResponse(ApiModel):
    ok: bool = True
    items: List[WorkBattleCatalogItem] = Field(default_factory=list)


class WorkBattleUserRow(ApiModel):
    user_id: int
    display_name: str
    avatar_thumb_url: Optional[str] = None
    avatar_url: Optional[str] = None
    online: bool = False
    level: Optional[int] = None
    rank_icon_key: Optional[str] = None
    leaderboard_badge_code: Optional[str] = None


class WorkBattleUsersResponse(ApiModel):
    ok: bool = True
    items: List[WorkBattleUserRow] = Field(default_factory=list)


class WorkBattleChallengeRow(ApiModel):
    id: int
    battle_type: BattleType
    metric_key: Literal["miles", "hours"]
    period_key: Literal["daily", "weekly"]
    status: ChallengeStatus
    challenger_user_id: int
    challenger_display_name: str
    challenged_user_id: int
    challenged_display_name: str
    created_at_ms: int
    expires_at_ms: int
    accepted_at_ms: Optional[int] = None
    ends_at_ms: Optional[int] = None
    result_code: Optional[ResultCode] = None
    opponent_user_id: Optional[int] = None
    opponent_display_name: Optional[str] = None


class WorkBattleChallengeRowsResponse(ApiModel):
    ok: bool = True
    items: List[WorkBattleChallengeRow] = Field(default_factory=list)


class WorkBattleHistoryRow(ApiModel):
    id: int
    battle_type: BattleType
    opponent_user_id: int
    opponent_display_name: str
    result_code: ResultCode
    my_final_value: float
    other_final_value: float
    completed_at_ms: int


class WorkBattleHistoryResponse(ApiModel):
    ok: bool = True
    items: List[WorkBattleHistoryRow] = Field(default_factory=list)


class WorkBattleChallengeCollections(ApiModel):
    ok: bool = True
    incoming: List[WorkBattleChallengeRow] = Field(default_factory=list)
    outgoing: List[WorkBattleChallengeRow] = Field(default_factory=list)
    active: Optional[dict] = None
    history_preview: List[WorkBattleHistoryRow] = Field(default_factory=list)


class WorkBattleCreateIn(ApiModel):
    target_user_id: int
    battle_type: BattleType


class WorkBattleDetail(ApiModel):
    id: int
    challenger_user_id: int
    challenged_user_id: int
    challenger_display_name: str
    challenged_display_name: str
    battle_type: BattleType
    metric_key: Literal["miles", "hours"]
    period_key: Literal["daily", "weekly"]
    status: ChallengeStatus
    created_at_ms: int
    accepted_at_ms: Optional[int] = None
    ends_at_ms: Optional[int] = None
    my_current_value: float = 0.0
    other_current_value: float = 0.0
    my_label: str
    other_label: str
    leader: Optional[str] = None
    result_code: Optional[ResultCode] = None


class WorkBattleDetailResponse(ApiModel):
    ok: bool = True
    item: WorkBattleDetail


class WorkBattleOptionalDetailResponse(ApiModel):
    ok: bool = True
    item: Optional[WorkBattleDetail] = None
