from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, Response, status

from admin_load_test_models import AdminLoadTestStartRequest
from admin_load_test_service import (
    get_last_load_test_result,
    get_load_test_capabilities,
    get_load_test_status,
    start_load_test,
    stop_load_test,
)
from admin_security import require_admin_user
from admin_test_models import AdminDiagnosticResponse
from admin_test_service import (
    test_admin_auth,
    test_backend_status,
    test_frame_current,
    test_me,
    test_score_frame_integrity,
    test_score_manifest,
    test_score_sql_definitions,
    test_pickup_reports,
    test_pickup_overlay_endpoint,
    test_presence_endpoint,
    test_police_reports,
    test_presence_live,
    test_presence_summary,
    test_timeline,
    test_trips_recent,
    test_trips_summary,
    test_zone_geometry_metrics,
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


@router.get("/score-manifest", response_model=AdminDiagnosticResponse)
def admin_test_score_manifest(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_score_manifest()


@router.get("/score-sql-definitions", response_model=AdminDiagnosticResponse)
def admin_test_score_sql_definitions(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_score_sql_definitions()


@router.get("/zone-geometry-metrics", response_model=AdminDiagnosticResponse)
def admin_test_zone_geometry_metrics(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_zone_geometry_metrics()


@router.get("/score-frame-integrity", response_model=AdminDiagnosticResponse)
def admin_test_score_frame_integrity(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_score_frame_integrity()


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


@router.get("/presence-endpoint", response_model=AdminDiagnosticResponse)
def admin_test_presence_endpoint(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return test_presence_endpoint()


@router.get("/pickup-overlay-endpoint", response_model=AdminDiagnosticResponse)
def admin_test_pickup_overlay_endpoint(admin: sqlite3.Row = Depends(require_admin_user)):
    return test_pickup_overlay_endpoint(admin)


@router.get("/load/capabilities", response_model=AdminDiagnosticResponse)
def admin_load_test_capabilities(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return get_load_test_capabilities()


@router.post("/load/start", response_model=AdminDiagnosticResponse)
def admin_load_test_start(
    payload: AdminLoadTestStartRequest,
    response: Response,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    _ = admin
    body, status_code = start_load_test(payload)
    response.status_code = status_code
    return body


@router.get("/load/status", response_model=AdminDiagnosticResponse)
def admin_load_test_status(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return get_load_test_status()


@router.post("/load/stop", response_model=AdminDiagnosticResponse, status_code=status.HTTP_200_OK)
def admin_load_test_stop(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return stop_load_test()


@router.get("/load/last", response_model=AdminDiagnosticResponse)
def admin_load_test_last(admin: sqlite3.Row = Depends(require_admin_user)):
    _ = admin
    return get_last_load_test_result()
