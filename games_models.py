from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

BattleType = Literal["dominoes", "billiards", "daily_miles_time", "weekly_miles_time"]
BattleCategory = Literal["game", "work"]
BattleFormat = Literal["1v1", "2v2"]
GameType = Literal["dominoes", "billiards"]
ChallengeStatus = Literal["pending", "assembling", "accepted", "declined", "canceled", "expired", "active"]
MatchStatus = Literal["active", "completed", "forfeited", "abandoned", "expired"]
RelationshipStatus = Literal["none", "incoming_challenge", "outgoing_challenge", "active_match"]


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class BattleStatsPayload(ApiModel):
    wins: int = 0
    losses: int = 0
    total_matches: int = 0
    matches_played: int = 0
    win_rate: float = 0.0
    dominoes_wins: int = 0
    dominoes_losses: int = 0
    billiards_wins: int = 0
    billiards_losses: int = 0
    work_battle_wins: int = 0
    work_battle_losses: int = 0
    game_xp_earned: int = 0


class RecentBattleRow(ApiModel):
    match_id: int
    category: BattleCategory = "game"
    battle_type: BattleType
    game_type: Optional[BattleType] = None
    game_key: Optional[BattleType] = None
    format: BattleFormat = "1v1"
    result: Literal["win", "loss", "tie"]
    opponent_user_id: Optional[int] = None
    opponent_display_name: Optional[str] = None
    xp_awarded: int
    xp_delta: int = 0
    completed_at: str
    result_summary: Optional[Dict[str, Any]] = None


class GameChallengeCreateIn(ApiModel):
    category: Optional[BattleCategory] = None
    battle_type: Optional[BattleType] = None
    format: Optional[BattleFormat] = None
    target_user_id: Optional[int] = None
    challenged_user_id: Optional[int] = None
    challenger_teammate_user_id: Optional[int] = None
    challenged_teammate_user_id: Optional[int] = None
    game_type: Optional[GameType] = None
    game_key: Optional[GameType] = None
    metadata: Optional[Dict[str, Any]] = None


class GameMoveIn(ApiModel):
    move_type: str
    tile: Optional[List[int]] = None
    side: Optional[Literal["left", "right"]] = None
    angle: Optional[float] = None
    power: Optional[float] = None
    english: Optional[float] = None
    shot_input: Optional[Dict[str, Any]] = None
    result_state: Optional[Dict[str, Any]] = None


class ChallengeRow(ApiModel):
    id: int
    category: BattleCategory = "game"
    battle_type: BattleType
    format: BattleFormat = "1v1"
    game_type: Optional[BattleType] = None
    game_key: Optional[BattleType] = None
    status: ChallengeStatus
    challenger_user_id: int
    challenger_display_name: str
    challenger_avatar_thumb_url: Optional[str] = None
    challenged_user_id: int
    challenged_display_name: str
    challenged_avatar_thumb_url: Optional[str] = None
    challenger_teammate_user_id: Optional[int] = None
    challenger_teammate_display_name: Optional[str] = None
    challenger_teammate_avatar_thumb_url: Optional[str] = None
    challenged_teammate_user_id: Optional[int] = None
    challenged_teammate_display_name: Optional[str] = None
    challenged_teammate_avatar_thumb_url: Optional[str] = None
    other_user_id: Optional[int] = None
    other_user_display_name: Optional[str] = None
    opponent_user_id: Optional[int] = None
    opponent_display_name: Optional[str] = None
    created_at: str
    expires_at: str
    accepted_at: Optional[str] = None
    last_action_at: Optional[str] = None
    declined_at: Optional[str] = None
    canceled_at: Optional[str] = None
    completed_match_id: Optional[int] = None
    seat_state: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class MatchSummary(ApiModel):
    id: int
    category: BattleCategory = "game"
    battle_type: BattleType
    format: BattleFormat = "1v1"
    game_type: Optional[BattleType] = None
    game_key: Optional[BattleType] = None
    status: MatchStatus
    challenger_user_id: int
    challenged_user_id: int
    current_turn_user_id: Optional[int] = None
    player_one_user_id: int
    player_two_user_id: int
    winner_user_id: Optional[int] = None
    loser_user_id: Optional[int] = None
    winner_xp_awarded: int = 0
    loser_xp_awarded: int = 0
    created_at: str
    updated_at: str
    accepted_at: Optional[str] = None
    expires_at: Optional[str] = None
    last_action_at: Optional[str] = None
    completed_at: Optional[str] = None
    opponent_display_name: Optional[str] = None
    result_summary: Optional[Dict[str, Any]] = None


class ChallengeListResponse(ApiModel):
    ok: bool = True
    incoming: List[ChallengeRow] = Field(default_factory=list)
    outgoing: List[ChallengeRow] = Field(default_factory=list)
    active_match: Optional[MatchSummary] = None
    activeMatch: Optional[MatchSummary] = None


class ChallengeRowsResponse(ApiModel):
    ok: bool = True
    items: List[ChallengeRow] = Field(default_factory=list)


class MoveRow(ApiModel):
    move_number: int
    actor_user_id: int
    move_type: str
    move_payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: str


class ViewerGameRelationship(ApiModel):
    status: RelationshipStatus = "none"
    category: Optional[BattleCategory] = None
    battle_type: Optional[BattleType] = None
    game_type: Optional[BattleType] = None
    format: Optional[BattleFormat] = None
    challenge_id: Optional[int] = None
    match_id: Optional[int] = None


class MatchDetail(ApiModel):
    id: int
    challenge_id: Optional[int] = None
    source_challenge_id: Optional[int] = None
    category: BattleCategory = "game"
    battle_type: BattleType
    format: BattleFormat = "1v1"
    game_type: Optional[BattleType] = None
    game_key: Optional[BattleType] = None
    status: MatchStatus
    challenger_user_id: int
    challenged_user_id: int
    player_one_user_id: int
    player_two_user_id: int
    current_turn_user_id: Optional[int] = None
    opponent_user_id: Optional[int] = None
    opponent_display_name: Optional[str] = None
    winner_user_id: Optional[int] = None
    loser_user_id: Optional[int] = None
    winner_xp_awarded: int = 0
    loser_xp_awarded: int = 0
    created_at: str
    updated_at: str
    accepted_at: Optional[str] = None
    expires_at: Optional[str] = None
    last_action_at: Optional[str] = None
    completed_at: Optional[str] = None
    match_state: Dict[str, Any] = Field(default_factory=dict)
    result_summary: Optional[Dict[str, Any]] = None
    moves: List[MoveRow] = Field(default_factory=list)


class MatchResponse(ApiModel):
    ok: bool = True
    match: MatchDetail
    reward_contract: Optional[Dict[str, Any]] = None
    public_notification: Optional[Dict[str, Any]] = None


class HistoryResponse(ApiModel):
    ok: bool = True
    items: List[RecentBattleRow] = Field(default_factory=list)


class ChallengeUsersResponse(ApiModel):
    ok: bool = True
    items: List[Dict[str, Any]] = Field(default_factory=list)
