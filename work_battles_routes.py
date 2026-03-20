from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends

from core import require_user
from work_battles_models import (
    WorkBattleCatalogResponse,
    WorkBattleChallengeCollections,
    WorkBattleChallengeRowsResponse,
    WorkBattleCreateIn,
    WorkBattleDetailResponse,
    WorkBattleHistoryResponse,
    WorkBattleOptionalDetailResponse,
    WorkBattleUsersResponse,
)
from work_battles_service import (
    CATALOG,
    accept_challenge,
    cancel_challenge,
    create_challenge,
    decline_challenge,
    get_active_challenge_for_user,
    get_challenge_detail,
    get_history_for_user,
    list_challengeable_users,
    list_challenges_for_user,
    list_incoming_challenges_for_user,
    list_outgoing_challenges_for_user,
)

router = APIRouter(prefix="/work-battles", tags=["work-battles"])


@router.get("/catalog", response_model=WorkBattleCatalogResponse)
def work_battles_catalog(user: sqlite3.Row = Depends(require_user)):
    return {
        "ok": True,
        "items": [{"battle_type": key, **value} for key, value in CATALOG.items()],
    }


@router.get("/users", response_model=WorkBattleUsersResponse)
def work_battles_users(q: str = "", limit: int = 25, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "items": list_challengeable_users(int(user["id"]), q=q, limit=limit)}


@router.get("/challenges", response_model=WorkBattleChallengeCollections)
def work_battles_challenges(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **list_challenges_for_user(int(user["id"]))}


@router.get("/challenges/incoming", response_model=WorkBattleChallengeRowsResponse)
def work_battles_incoming(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "items": list_incoming_challenges_for_user(int(user["id"]))}


@router.get("/challenges/outgoing", response_model=WorkBattleChallengeRowsResponse)
def work_battles_outgoing(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "items": list_outgoing_challenges_for_user(int(user["id"]))}


@router.post("/challenges", response_model=WorkBattleDetailResponse)
def work_battles_create(payload: WorkBattleCreateIn, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "item": create_challenge(int(user["id"]), int(payload.target_user_id), payload.battle_type)}


@router.post("/challenges/{challenge_id}/accept", response_model=WorkBattleDetailResponse)
def work_battles_accept(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "item": accept_challenge(int(challenge_id), int(user["id"]))}


@router.post("/challenges/{challenge_id}/decline", response_model=WorkBattleDetailResponse)
def work_battles_decline(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "item": decline_challenge(int(challenge_id), int(user["id"]))}


@router.post("/challenges/{challenge_id}/cancel", response_model=WorkBattleDetailResponse)
def work_battles_cancel(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "item": cancel_challenge(int(challenge_id), int(user["id"]))}


@router.get("/active/me", response_model=WorkBattleOptionalDetailResponse)
def work_battles_active_me(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "item": get_active_challenge_for_user(int(user["id"]))}


@router.get("/challenges/{challenge_id}", response_model=WorkBattleDetailResponse)
def work_battles_detail(challenge_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "item": get_challenge_detail(int(challenge_id), int(user["id"]))}


@router.get("/history/me", response_model=WorkBattleHistoryResponse)
def work_battles_history_me(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "items": get_history_for_user(int(user["id"]))}
