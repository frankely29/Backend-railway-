from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from admin_models import (
    AdminLiveResponse,
    AdminReportsResponse,
    AdminSummaryResponse,
    AdminSystemResponse,
    AdminUsersResponse,
)
from admin_security import require_admin_user
from admin_service import (
    get_admin_hotspot_experiment_bins,
    get_admin_live,
    get_admin_micro_hotspot_experiment_bins,
    get_admin_micro_recommendation_outcomes,
    get_admin_pickup_logs,
    get_admin_police_reports,
    get_admin_recommendation_outcomes,
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


@router.get("/experiments/hotspots", response_model=AdminReportsResponse)
def admin_hotspot_experiment_bins(
    limit: int = Query(default=200, ge=1, le=1000),
    zone_id: int | None = Query(default=None),
    since_seconds: int | None = Query(default=None),
    recommended_only: bool | None = Query(default=None),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return {
        "items": get_admin_hotspot_experiment_bins(
            limit=limit,
            zone_id=zone_id,
            since_seconds=since_seconds,
            recommended_only=recommended_only,
        )
    }


@router.get("/experiments/micro_hotspots", response_model=AdminReportsResponse)
def admin_micro_hotspot_experiment_bins(
    limit: int = Query(default=200, ge=1, le=1000),
    zone_id: int | None = Query(default=None),
    cluster_id: str | None = Query(default=None),
    since_seconds: int | None = Query(default=None),
    recommended_only: bool | None = Query(default=None),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return {
        "items": get_admin_micro_hotspot_experiment_bins(
            limit=limit,
            zone_id=zone_id,
            cluster_id=cluster_id,
            since_seconds=since_seconds,
            recommended_only=recommended_only,
        )
    }


@router.get("/experiments/recommendation_outcomes", response_model=AdminReportsResponse)
def admin_recommendation_outcomes(
    limit: int = Query(default=200, ge=1, le=1000),
    zone_id: int | None = Query(default=None),
    cluster_id: str | None = Query(default=None),
    user_id: int | None = Query(default=None),
    outcome_status: str | None = Query(default=None),
    since_seconds: int | None = Query(default=None),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    try:
        items = get_admin_recommendation_outcomes(
            limit=limit,
            zone_id=zone_id,
            cluster_id=cluster_id,
            user_id=user_id,
            outcome_status=outcome_status,
            since_seconds=since_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"items": items}


@router.get("/experiments/micro_recommendation_outcomes", response_model=AdminReportsResponse)
def admin_micro_recommendation_outcomes(
    limit: int = Query(default=200, ge=1, le=1000),
    zone_id: int | None = Query(default=None),
    cluster_id: str | None = Query(default=None),
    user_id: int | None = Query(default=None),
    outcome_status: str | None = Query(default=None),
    since_seconds: int | None = Query(default=None),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    try:
        items = get_admin_micro_recommendation_outcomes(
            limit=limit,
            zone_id=zone_id,
            cluster_id=cluster_id,
            user_id=user_id,
            outcome_status=outcome_status,
            since_seconds=since_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"items": items}


@router.get("/system", response_model=AdminSystemResponse)
def admin_system(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return get_admin_system()
