from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


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
    is_disabled: bool = False
    is_suspended: bool
    ghost_mode: bool
    avatar_url: Optional[str] = None
    created_at: Optional[str] = None
    trial_expires_at: Optional[int] = None
    subscription_status: Optional[str] = None
    subscription_provider: Optional[str] = None
    subscription_customer_id: Optional[str] = None
    subscription_id: Optional[str] = None
    subscription_current_period_end: Optional[int] = None
    subscription_comp_reason: Optional[str] = None
    subscription_comp_granted_by: Optional[int] = None
    subscription_comp_granted_at: Optional[int] = None
    subscription_comp_expires_at: Optional[int] = None
    subscription_updated_at: Optional[int] = None
    presence: Optional[dict] = None
    pickup_count: Optional[int] = None
    voided_pickup_count: Optional[int] = None
    police_report_count: Optional[int] = None


class ClearReportResponse(BaseModel):
    ok: bool = True
    report_id: int
    cleared: bool


class CompGrantRequest(BaseModel):
    duration_unit: Literal["hours", "days", "weeks", "forever"]
    duration_value: int = Field(default=0, ge=0, le=10000)
    reason: str = Field(min_length=3, max_length=500)


class CompExtendRequest(BaseModel):
    duration_unit: Literal["hours", "days", "weeks"]
    duration_value: int = Field(ge=1, le=10000)


class CompGrantResponse(BaseModel):
    ok: bool
    user_id: int
    status: str
    comp_expires_at: Optional[int] = None
    comp_reason: Optional[str] = None
    is_comp_forever: bool = False
    days_remaining: Optional[int] = None


class CompRevokeResponse(BaseModel):
    ok: bool
    user_id: int
    status: str


class CompListItem(BaseModel):
    user_id: int
    email: str
    display_name: str
    reason: Optional[str] = None
    granted_by: Optional[int] = None
    granted_at: Optional[int] = None
    expires_at: Optional[int] = None
    days_remaining: Optional[int] = None
    is_forever: bool


class CompListResponse(BaseModel):
    items: list[CompListItem]
    total: int
