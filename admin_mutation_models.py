from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class SetAdminRequest(BaseModel):
    is_admin: bool


class SetSuspendedRequest(BaseModel):
    is_suspended: bool


class AdminActionUserResponse(BaseModel):
    ok: bool = True
    user_id: int
    is_admin: Optional[bool] = None
    is_suspended: Optional[bool] = None


class AdminUserDetailResponse(BaseModel):
    id: int
    email: str
    display_name: Optional[str] = None
    is_admin: bool
    is_suspended: bool
    ghost_mode: bool
    avatar_url: Optional[str] = None
    created_at: Optional[str] = None
    presence: Optional[dict] = None
    pickup_count: Optional[int] = None
    police_report_count: Optional[int] = None


class ClearReportResponse(BaseModel):
    ok: bool = True
    report_id: int
    cleared: bool
