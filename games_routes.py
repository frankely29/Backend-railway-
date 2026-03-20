from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends

from core import require_user
from games_models import ChallengeListResponse, ChallengeRow, ChallengeUsersResponse, GameChallengeCreateIn, GameMoveIn, HistoryResponse, MatchResponse
from games_service import (
    accept_challenge,
    cancel_challenge,
    create_challenge,
    decline_challenge,
    forfeit_match,
    get_active_match_for_user,
    get_history_for_user,
    get_match_detail,
    list_challengeable_users,
    list_challenges_for_user,
    list_incoming_challenges_for_user,
    list_outgoing_challenges_for_user,
    submit_move,
)

router = APIRouter(prefix="/games", tags=["games"])


@router.get("/challenges", response_model=ChallengeListResponse)
def game_challenges(user: sqlite3.Row = Depends(require_user)):
    data = list_challenges_for_user(int(user["id"]))
    return {"ok": True, **data}


@router.get("/challenges/incoming", response_model=ChallengeUsersResponse)
def incoming_game_challenges(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "items": list_incoming_challenges_for_user(int(user["id"]))}


@router.get("/challenges/outgoing", response_model=ChallengeUsersResponse)
def outgoing_game_challenges(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "items": list_outgoing_challenges_for_user(int(user["id"]))}


@router.post("/challenges", response_model=ChallengeRow)
def create_game_challenge(payload: GameChallengeCreateIn, user: sqlite3.Row = Depends(require_user)):
    return create_challenge(int(user["id"]), int(payload.target_user_id), payload.game_type)


@router.post("/challenges/{challenge_id}/accept", response_model=MatchResponse)
def accept_game_challenge(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    return accept_challenge(int(challenge_id), int(user["id"]))


@router.post("/challenges/{challenge_id}/decline", response_model=ChallengeRow)
def decline_game_challenge(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    return decline_challenge(int(challenge_id), int(user["id"]))


@router.post("/challenges/{challenge_id}/cancel", response_model=ChallengeRow)
def cancel_game_challenge(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    return cancel_challenge(int(challenge_id), int(user["id"]))


@router.get("/users", response_model=ChallengeUsersResponse)
def game_users(q: str = "", limit: int = 25, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "items": list_challengeable_users(int(user["id"]), q=q, limit=limit)}


@router.get("/matches/active/me", response_model=MatchResponse | None)
def active_match(user: sqlite3.Row = Depends(require_user)):
    return get_active_match_for_user(int(user["id"]))


@router.get("/matches/{match_id}", response_model=MatchResponse)
def match_detail(match_id: int, user: sqlite3.Row = Depends(require_user)):
    return get_match_detail(int(match_id), int(user["id"]))


@router.post("/matches/{match_id}/move", response_model=MatchResponse)
def match_move(match_id: int, payload: GameMoveIn, user: sqlite3.Row = Depends(require_user)):
    return submit_move(int(match_id), int(user["id"]), payload.model_dump(exclude_none=True))


@router.post("/matches/{match_id}/forfeit", response_model=MatchResponse)
def match_forfeit(match_id: int, user: sqlite3.Row = Depends(require_user)):
    return forfeit_match(int(match_id), int(user["id"]))


@router.get("/history/me", response_model=HistoryResponse)
def match_history(user: sqlite3.Row = Depends(require_user)):
    return get_history_for_user(int(user["id"]))
