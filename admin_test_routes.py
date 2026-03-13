from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends

from admin_security import require_admin_user
from admin_test_models import AdminDiagnosticResponse
from admin_test_service import (
    test_admin_auth,
    test_backend_status,
    test_frame_current,
    test_me,
    test_pickup_reports,
    test_police_reports,
    test_presence_live,
    test_presence_summary,
    test_timeline,
    test_trips_recent,
    test_trips_summary,
)

router = APIRouter(prefix="/admin/tests", tags=["admin-tests"])


@router.get("/backend-status", response_model=AdminDiagnosticResponse)
def admin_test_backend_status(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_backend_status()


@router.get("/timeline", response_model=AdminDiagnosticResponse)
def admin_test_timeline(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_timeline()


@router.get("/frame-current", response_model=AdminDiagnosticResponse)
def admin_test_frame_current(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_frame_current()


@router.get("/admin-auth", response_model=AdminDiagnosticResponse)
def admin_test_admin_auth(admin: sqlite3.Row = Depends(require_admin_user)):
    return test_admin_auth(admin)


@router.get("/presence-summary", response_model=AdminDiagnosticResponse)
def admin_test_presence_summary(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_presence_summary()


@router.get("/presence-live", response_model=AdminDiagnosticResponse)
def admin_test_presence_live(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_presence_live()


@router.get("/me", response_model=AdminDiagnosticResponse)
def admin_test_me(admin: sqlite3.Row = Depends(require_admin_user)):
    return test_me(admin)


@router.get("/trips-summary", response_model=AdminDiagnosticResponse)
def admin_test_trips_summary(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_trips_summary()


@router.get("/trips-recent", response_model=AdminDiagnosticResponse)
def admin_test_trips_recent(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_trips_recent()


@router.get("/police-reports", response_model=AdminDiagnosticResponse)
def admin_test_police_reports(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_police_reports()


@router.get("/pickup-reports", response_model=AdminDiagnosticResponse)
def admin_test_pickup_reports(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_pickup_reports()
