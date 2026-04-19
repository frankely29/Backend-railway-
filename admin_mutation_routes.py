from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, Query

from admin_mutation_models import (
    AdminActionUserResponse,
    AdminUserDetailResponse,
    ClearReportResponse,
    CompExtendRequest,
    CompGrantRequest,
    CompGrantResponse,
    CompListResponse,
    CompRevokeResponse,
    SetAdminRequest,
    SetSuspendedRequest,
)
from admin_mutation_service import (
    clear_pickup_report,
    clear_police_report,
    extend_comp,
    get_admin_user_detail,
    grant_comp,
    list_active_comps,
    revoke_comp,
    set_user_admin,
    set_user_suspended,
)
from admin_security import require_admin_user

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/users/{user_id}/set-admin", response_model=AdminActionUserResponse)
def admin_set_user_admin(
    user_id: int,
    payload: SetAdminRequest,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    return set_user_admin(actor_user_id=int(admin["id"]), user_id=user_id, is_admin=payload.is_admin)


@router.post("/users/{user_id}/set-suspended", response_model=AdminActionUserResponse)
def admin_set_user_suspended(
    user_id: int,
    payload: SetSuspendedRequest,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    return set_user_suspended(actor_user_id=int(admin["id"]), user_id=user_id, is_suspended=payload.is_suspended)


@router.get("/users/{user_id}", response_model=AdminUserDetailResponse)
def admin_user_detail(
    user_id: int,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return get_admin_user_detail(user_id=user_id)


@router.post("/reports/police/{report_id}/clear", response_model=ClearReportResponse)
def admin_clear_police_report(
    report_id: int,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return clear_police_report(report_id=report_id)


@router.post("/reports/pickups/{report_id}/clear", response_model=ClearReportResponse)
def admin_clear_pickup_report(
    report_id: int,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    return clear_pickup_report(report_id=report_id, admin_user_id=int(admin["id"]))


@router.post("/users/{user_id}/comp/grant", response_model=CompGrantResponse)
def admin_grant_comp(
    user_id: int,
    payload: CompGrantRequest,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    return grant_comp(
        actor_user_id=int(admin["id"]),
        user_id=user_id,
        duration_unit=payload.duration_unit,
        duration_value=payload.duration_value,
        reason=payload.reason,
    )


@router.post("/users/{user_id}/comp/extend", response_model=CompGrantResponse)
def admin_extend_comp(
    user_id: int,
    payload: CompExtendRequest,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    return extend_comp(
        actor_user_id=int(admin["id"]),
        user_id=user_id,
        duration_unit=payload.duration_unit,
        duration_value=payload.duration_value,
    )


@router.post("/users/{user_id}/comp/revoke", response_model=CompRevokeResponse)
def admin_revoke_comp(
    user_id: int,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    return revoke_comp(actor_user_id=int(admin["id"]), user_id=user_id)


@router.get("/comps", response_model=CompListResponse)
def admin_list_comps(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    search: str | None = Query(default=None, max_length=200),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return list_active_comps(limit=limit, offset=offset, search=search)
