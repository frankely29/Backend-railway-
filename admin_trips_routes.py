from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, Query

from admin_security import require_admin_user
from admin_trips_models import (
    AdminRecentTripsResponse,
    AdminTripSummaryResponse,
    AdminVoidTripPayload,
    AdminVoidTripResponse,
)
from admin_trips_service import get_admin_recent_trips, get_admin_trips_summary, void_admin_recorded_trip

router = APIRouter(prefix="/admin/trips", tags=["admin-trips"])


@router.get("/summary", response_model=AdminTripSummaryResponse)
def admin_trips_summary(
    include_voided: bool = Query(default=False),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return get_admin_trips_summary(include_voided=include_voided)


@router.get("/recent", response_model=AdminRecentTripsResponse)
def admin_trips_recent(
    limit: int = Query(default=20, ge=1, le=100),
    include_voided: bool = Query(default=False),
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    return {"items": get_admin_recent_trips(limit=limit, include_voided=include_voided)}


@router.post("/{trip_id}/void", response_model=AdminVoidTripResponse)
def admin_void_trip(
    trip_id: int,
    payload: AdminVoidTripPayload,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    return void_admin_recorded_trip(trip_id=trip_id, admin_user_id=int(admin["id"]), reason=payload.reason)
