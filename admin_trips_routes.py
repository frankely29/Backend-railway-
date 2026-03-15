from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, Query

from admin_security import require_admin_user
from admin_trips_models import AdminRecentTripsResponse, AdminTripSummaryResponse
from admin_trips_service import get_admin_recent_trips, get_admin_trips_summary

router = APIRouter(prefix="/admin/trips", tags=["admin-trips"])


@router.get("/summary", response_model=AdminTripSummaryResponse)
def admin_trips_summary(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return get_admin_trips_summary()


@router.get("/recent", response_model=AdminRecentTripsResponse)
def admin_trips_recent(
    limit: int = Query(default=20, ge=1, le=100),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return {"items": get_admin_recent_trips(limit=limit)}