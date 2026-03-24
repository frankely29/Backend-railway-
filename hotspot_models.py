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


@dataclass
class ZoneEarningsShadowResult:
    zone_id: int
    dow_m: int
    bin_start_min: int
    pickups_now: int
    pickups_next: int
    median_driver_pay: Optional[float]
    median_pay_per_min: Optional[float]
    median_pay_per_mile: Optional[float]
    median_request_to_pickup_min: Optional[float]
    short_trip_share_3mi_12min: Optional[float]
    shared_ride_share: Optional[float]
    downstream_next_value_raw: Optional[float]
    demand_now_n: Optional[float]
    demand_next_n: Optional[float]
    pay_n: Optional[float]
    pay_per_min_n: Optional[float]
    pay_per_mile_n: Optional[float]
    pickup_friction_penalty_n: Optional[float]
    short_trip_penalty_n: Optional[float]
    shared_ride_penalty_n: Optional[float]
    downstream_value_n: Optional[float]
    earnings_shadow_score_citywide_v2: Optional[float]
    earnings_shadow_confidence_citywide_v2: Optional[float]
    earnings_shadow_rating_citywide_v2: Optional[int]
    earnings_shadow_bucket_citywide_v2: Optional[str]
    earnings_shadow_color_citywide_v2: Optional[str]
    earnings_shadow_score_manhattan_v2: Optional[float]
    earnings_shadow_confidence_manhattan_v2: Optional[float]
    earnings_shadow_rating_manhattan_v2: Optional[int]
    earnings_shadow_bucket_manhattan_v2: Optional[str]
    earnings_shadow_color_manhattan_v2: Optional[str]
