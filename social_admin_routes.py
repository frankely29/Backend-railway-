"""The moderation queue.

A report button with nothing behind it is worse than no report button: it tells
a driver their complaint was heard and then drops it. These routes are the other
half — the queue, and the two actions that answer a report.

Mounted under the existing /admin prefix and behind require_admin_user, the same
gate every other admin surface uses.
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends

from admin_security import require_admin_user
from social_models import (
    HidePostPayload,
    HidePostResponse,
    ReportCountResponse,
    ReportQueueResponse,
    ResolveReportPayload,
    ResolveReportResponse,
)
from social_moderation import (
    REPORT_OPEN,
    hide_post,
    list_reports,
    open_report_count,
    resolve_report,
    unhide_post,
)

router = APIRouter(prefix="/admin", tags=["admin", "moderation"])


@router.get("/reports", response_model=ReportQueueResponse)
def admin_list_reports(
    status: Optional[str] = REPORT_OPEN,
    limit: int = 50,
    before_id: Optional[int] = None,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    """Newest first, with the open count alongside.

    `status=` empty returns every report regardless of state, which is what you
    want when looking up the history on one driver rather than working the queue.
    """
    return {"ok": True, **list_reports(status=status or None, limit=limit, before_id=before_id)}


@router.get("/reports/count", response_model=ReportCountResponse)
def admin_report_count(admin: sqlite3.Row = Depends(require_admin_user)):
    """Cheap enough to poll for a badge on the admin nav."""
    return {"ok": True, "open_count": open_report_count()}


@router.post("/reports/{report_id}/resolve", response_model=ResolveReportResponse)
def admin_resolve_report(
    report_id: int,
    payload: ResolveReportPayload,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    return {"ok": True, **resolve_report(
        int(admin["id"]), report_id, payload.status, payload.resolution)}


@router.post("/posts/{post_id}/hide", response_model=HidePostResponse)
def admin_hide_post(
    post_id: int,
    payload: HidePostPayload,
    admin: sqlite3.Row = Depends(require_admin_user),
):
    """Takes the post down and closes every open report about it in one action.

    Resolving them separately means a post gets hidden and its reports sit in
    the queue anyway, which is how a queue stops being trustworthy.
    """
    return {"ok": True, **hide_post(int(admin["id"]), post_id, payload.reason)}


@router.post("/posts/{post_id}/unhide", response_model=HidePostResponse)
def admin_unhide_post(post_id: int, admin: sqlite3.Row = Depends(require_admin_user)):
    return {"ok": True, **unhide_post(int(admin["id"]), post_id)}
