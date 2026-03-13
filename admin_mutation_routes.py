from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends

from admin_mutation_models import (
    AdminActionUserResponse,
    AdminUserDetailResponse,
    ClearReportResponse,
    SetAdminRequest,
    SetSuspendedRequest,
)
from admin_mutation_service import (
    clear_pickup_report,
    clear_police_report,
    get_admin_user_detail,
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
    _ = admin
    return clear_pickup_report(report_id=report_id)
