from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class AdminTripSummaryResponse(BaseModel):
    total_recorded_trips: int
    trips_last_24h: int
    trips_last_7d: int
    latest_trip_at: Optional[str] = None
    distinct_users_count: Optional[int] = None
    distinct_zones_count: Optional[int] = None
    voided_trips_count: Optional[int] = None


class AdminRecentTripItem(BaseModel):
    id: Optional[int] = None
    user_id: Optional[int] = None
    display_name: Optional[str] = None
    zone_id: Optional[int] = None
    location_id: Optional[int] = None
    zone_name: Optional[str] = None
    borough: Optional[str] = None
    frame_time: Optional[str] = None
    created_at: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    is_voided: bool = False
    voided_at: Optional[str] = None
    void_reason: Optional[str] = None


class AdminRecentTripsResponse(BaseModel):
    items: List[AdminRecentTripItem]


class AdminVoidTripPayload(BaseModel):
    reason: str = Field(..., min_length=5)


class AdminVoidTripResponse(BaseModel):
    ok: bool
    trip_id: int
    voided: bool
    stats_reversed: bool
    preserved_in_audit: bool
