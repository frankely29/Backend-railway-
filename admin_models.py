from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class AdminSummaryResponse(BaseModel):
    total_users: int
    admin_users: int
    admins_count: Optional[int] = None
    online_users: int
    ghosted_online_users: int
    police_reports_recent_count: int
    police_reports_count: Optional[int] = None
    pickup_logs_recent_count: int
    pickup_logs_count: Optional[int] = None
    pickup_logs_voided_recent_count: Optional[int] = None
    timeline_ready: bool
    frame_count: int
    leaderboard_status: Dict[str, Any]
    backend_status: str


class AdminUserRow(BaseModel):
    id: int
    email: str
    display_name: Optional[str] = None
    is_admin: bool
    is_suspended: bool = False
    ghost_mode: bool
    avatar_url: Optional[str] = None
    created_at: Optional[str] = None


class AdminUsersResponse(BaseModel):
    items: List[AdminUserRow]


class AdminLiveRow(BaseModel):
    user_id: int
    display_name: Optional[str] = None
    lat: float
    lng: float
    heading: Optional[float] = None
    accuracy: Optional[float] = None
    ghost_mode: bool = False
    updated_at: Optional[str] = None
    leaderboard_badge_code: Optional[str] = None
    leaderboard_has_crown: Optional[bool] = None


class AdminLiveResponse(BaseModel):
    items: List[AdminLiveRow]


class PoliceReportRow(BaseModel):
    id: Optional[int] = None
    user_id: Optional[int] = None
    lat: float
    lng: float
    zone_id: Optional[int] = None
    created_at: Optional[str] = None
    expires_at: Optional[str] = None


class PickupLogRow(BaseModel):
    id: Optional[int] = None
    user_id: Optional[int] = None
    zone_id: Optional[int] = None
    zone_name: Optional[str] = None
    borough: Optional[str] = None
    lat: float
    lng: float
    frame_time: Optional[str] = None
    created_at: Optional[str] = None


class AdminReportsResponse(BaseModel):
    items: List[Dict[str, Any]]


class AdminSystemResponse(BaseModel):
    backend_status: str
    timeline_ready: bool
    frame_count: int
    frames_dir: Optional[str] = None
    data_dir: Optional[str] = None
    leaderboard_status: Dict[str, Any]
    table_counts: Dict[str, Optional[int]]
