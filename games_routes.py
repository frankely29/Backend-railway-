from __future__ import annotations

import sqlite3
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from core import require_user
from games_models import GameChallengeCreateIn, GameMoveIn
from games_service import (
    accept_challenge,
    cancel_challenge,
    create_challenge,
    decline_challenge,
    ensure_games_schema,
    forfeit_match,
    get_active_match_for_user,
    get_challenge_dashboard,
    get_games_users,
    get_history_for_user,
    get_incoming_challenges,
    get_match_detail,
    get_outgoing_challenges,
    move_match,
)

router = APIRouter(prefix="/games", tags=["games"])


@router.get("/users")
def games_users(
    q: str = Query(""),
    limit: int = Query(20, ge=1, le=100),
    user: sqlite3.Row = Depends(require_user),
):
    ensure_games_schema()
    items = get_games_users(int(user["id"]), q=q, limit=limit)
    return {"ok": True, "items": items, "rows": items}


@router.post("/challenges")
def games_create_challenge(payload: GameChallengeCreateIn, user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    target_user_id = payload.target_user_id if payload.target_user_id is not None else payload.challenged_user_id
    game_type = payload.game_type if payload.game_type is not None else payload.game_key
    if target_user_id is None or game_type is None:
        raise HTTPException(status_code=400, detail="target_user_id/challenged_user_id and game_type/game_key are required")
    item = create_challenge(int(user["id"]), int(target_user_id), str(game_type))
    return item


@router.get("/challenges")
def games_challenges_dashboard(user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    return get_challenge_dashboard(int(user["id"]))


@router.get("/challenges/incoming")
def games_incoming_challenges(user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    items = get_incoming_challenges(int(user["id"]))
    return {"ok": True, "items": items, "rows": items}


@router.get("/challenges/outgoing")
def games_outgoing_challenges(user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    items = get_outgoing_challenges(int(user["id"]))
    return {"ok": True, "items": items, "rows": items}


@router.post("/challenges/{challenge_id}/accept")
def games_accept_challenge(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    bundle = accept_challenge(int(challenge_id), int(user["id"]))
    return bundle


@router.post("/challenges/{challenge_id}/decline")
def games_decline_challenge(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    return decline_challenge(int(challenge_id), int(user["id"]))


@router.post("/challenges/{challenge_id}/cancel")
def games_cancel_challenge(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    return cancel_challenge(int(challenge_id), int(user["id"]))


@router.get("/matches/active/me")
def games_active_match(user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    active = get_active_match_for_user(int(user["id"]))
    if active is None:
        return None
    return {"match": active, "active_match": active, "activeMatch": active}


@router.get("/matches/{match_id}")
def games_match_detail(match_id: int, user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    return get_match_detail(int(match_id), int(user["id"]))


@router.post("/matches/{match_id}/move")
def games_match_move(match_id: int, payload: GameMoveIn, user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    return move_match(int(match_id), int(user["id"]), payload.model_dump(exclude_none=True))


@router.post("/matches/{match_id}/forfeit")
def games_match_forfeit(match_id: int, user: sqlite3.Row = Depends(require_user)):
    ensure_games_schema()
    return forfeit_match(int(match_id), int(user["id"]))


@router.get("/history/me")
def games_history(user: sqlite3.Row = Depends(require_user), limit: int = Query(20, ge=1, le=100)):
    ensure_games_schema()
    items = get_history_for_user(int(user["id"]), limit=limit)
    return {"ok": True, "items": items, "rows": items}
