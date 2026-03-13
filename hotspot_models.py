from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class ZoneScoreResult:
    zone_id: int
    final_score: float
    confidence: float
    live_strength: float
    density_penalty: float
    historical_component: float
    live_component: float
    same_timeslot_component: float
    weighted_trip_count: float
    unique_driver_count: int
    recommended: bool


@dataclass
class MicroHotspotScoreResult:
    cluster_id: str
    zone_id: int
    center_lat: float
    center_lng: float
    radius_m: float
    intensity: float
    confidence: float
    weighted_trip_count: float
    unique_driver_count: int
    crowding_penalty: float
    baseline_component: float
    live_component: float
    same_timeslot_component: float
    final_score: float
    recommended: bool
    eta_alignment: Optional[float] = None
