from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

GameType = Literal["dominoes", "billiards"]
ChallengeStatus = Literal["pending", "accepted", "declined", "canceled", "expired"]
MatchStatus = Literal["active", "completed", "forfeited", "abandoned"]
RelationshipStatus = Literal["none", "incoming_challenge", "outgoing_challenge", "active_match"]


class BattleStatsPayload(BaseModel):
    wins: int = 0
    losses: int = 0
    total_matches: int = 0
    matches_played: int = 0
    win_rate: float = 0.0
    dominoes_wins: int = 0
    dominoes_losses: int = 0
    billiards_wins: int = 0
    billiards_losses: int = 0
    game_xp_earned: int = 0


class RecentBattleRow(BaseModel):
    match_id: int
    game_type: GameType
    game_key: GameType
    result: Literal["win", "loss"]
    opponent_user_id: int
    opponent_display_name: str
    xp_awarded: int
    xp_delta: int = 0
    completed_at: str


class GameChallengeCreateIn(BaseModel):
    target_user_id: Optional[int] = None
    challenged_user_id: Optional[int] = None
    game_type: Optional[GameType] = None
    game_key: Optional[GameType] = None


class GameMoveIn(BaseModel):
    move_type: str
    tile: Optional[List[int]] = None
    side: Optional[Literal["left", "right"]] = None
    angle: Optional[float] = None
    power: Optional[float] = None
    english: Optional[float] = None
    shot_input: Optional[Dict[str, Any]] = None
    result_state: Optional[Dict[str, Any]] = None


class ChallengeRow(BaseModel):
    id: int
    game_type: GameType
    game_key: GameType
    status: ChallengeStatus
    challenger_user_id: int
    challenger_display_name: str
    challenged_user_id: int
    challenged_display_name: str
    other_user_id: Optional[int] = None
    other_user_display_name: Optional[str] = None
    opponent_user_id: Optional[int] = None
    opponent_display_name: Optional[str] = None
    created_at: str
    expires_at: str
    accepted_at: Optional[str] = None
    declined_at: Optional[str] = None
    canceled_at: Optional[str] = None
    completed_match_id: Optional[int] = None


class MatchSummary(BaseModel):
    id: int
    game_type: GameType
    game_key: GameType
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
    completed_at: Optional[str] = None


class ChallengeListResponse(BaseModel):
    ok: bool = True
    incoming: List[ChallengeRow] = Field(default_factory=list)
    outgoing: List[ChallengeRow] = Field(default_factory=list)
    active_match: Optional[MatchSummary] = None
    activeMatch: Optional[MatchSummary] = None


class ChallengeRowsResponse(BaseModel):
    ok: bool = True
    items: List[ChallengeRow] = Field(default_factory=list)


class MoveRow(BaseModel):
    move_number: int
    actor_user_id: int
    move_type: str
    move_payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: str


class ViewerGameRelationship(BaseModel):
    status: RelationshipStatus = "none"
    game_type: Optional[GameType] = None
    challenge_id: Optional[int] = None
    match_id: Optional[int] = None


class MatchDetail(BaseModel):
    id: int
    challenge_id: Optional[int] = None
    source_challenge_id: Optional[int] = None
    game_type: GameType
    game_key: GameType
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
    completed_at: Optional[str] = None
    match_state: Dict[str, Any] = Field(default_factory=dict)
    result_summary: Optional[Dict[str, Any]] = None
    moves: List[MoveRow] = Field(default_factory=list)


class MatchResponse(BaseModel):
    ok: bool = True
    match: MatchDetail
    reward_contract: Optional[Dict[str, Any]] = None
    public_notification: Optional[Dict[str, Any]] = None


class HistoryResponse(BaseModel):
    ok: bool = True
    items: List[RecentBattleRow] = Field(default_factory=list)


class ChallengeUsersResponse(BaseModel):
    ok: bool = True
    items: List[Dict[str, Any]] = Field(default_factory=list)
