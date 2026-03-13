from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, Query

from admin_models import (
    AdminLiveResponse,
    AdminReportsResponse,
    AdminSummaryResponse,
    AdminSystemResponse,
    AdminUsersResponse,
)
from admin_security import require_admin_user
from admin_service import (
    get_admin_live,
    get_admin_pickup_logs,
    get_admin_police_reports,
    get_admin_summary,
    get_admin_system,
    get_admin_users,
)

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/summary", response_model=AdminSummaryResponse)
def admin_summary(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return get_admin_summary()


@router.get("/users", response_model=AdminUsersResponse)
def admin_users(
    limit: int = Query(default=500, ge=1, le=5000),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return {"items": get_admin_users(limit=limit)}


@router.get("/live", response_model=AdminLiveResponse)
def admin_live(
    limit: int = Query(default=1000, ge=1, le=5000),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return {"items": get_admin_live(limit=limit)}


@router.get("/reports/police", response_model=AdminReportsResponse)
def admin_police_reports(
    limit: int = Query(default=500, ge=1, le=5000),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return {"items": get_admin_police_reports(limit=limit)}


@router.get("/reports/pickups", response_model=AdminReportsResponse)
def admin_pickup_reports(
    limit: int = Query(default=500, ge=1, le=5000),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return {"items": get_admin_pickup_logs(limit=limit)}


@router.get("/system", response_model=AdminSystemResponse)
def admin_system(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return get_admin_system()
