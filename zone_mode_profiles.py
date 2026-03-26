from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class ZoneScoreProfileWeights:
    demand_now_weight: float = 0.0
    demand_next_weight: float = 0.0
    demand_density_now_weight: float = 0.0
    demand_density_next_weight: float = 0.0
    pay_weight: float = 0.0
    pay_per_min_weight: float = 0.0
    pay_per_mile_weight: float = 0.0
    balanced_trip_quality_weight: float = 0.0
    balanced_trip_share_weight: float = 0.0
    long_trip_share_20plus_weight: float = 0.0
    downstream_weight: float = 0.0
    short_trip_penalty_weight: float = 0.0
    same_zone_retention_penalty_weight: float = 0.0
    pickup_friction_penalty_weight: float = 0.0
    shared_ride_penalty_weight: float = 0.0
    saturation_penalty_weight: float = 0.0
    market_saturation_penalty_weight: float = 0.0


ZONE_MODE_PROFILES: Dict[str, ZoneScoreProfileWeights] = {
    "citywide_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.20,
        demand_next_weight=0.14,
        pay_weight=0.22,
        pay_per_min_weight=0.16,
        pay_per_mile_weight=0.10,
        downstream_weight=0.12,
        short_trip_penalty_weight=0.04,
        pickup_friction_penalty_weight=0.03,
        shared_ride_penalty_weight=0.01,
    ),
    "citywide_v3": ZoneScoreProfileWeights(
        demand_now_weight=0.11,
        demand_next_weight=0.11,
        demand_density_now_weight=0.12,
        demand_density_next_weight=0.09,
        pay_weight=0.07,
        pay_per_min_weight=0.12,
        pay_per_mile_weight=0.07,
        balanced_trip_share_weight=0.10,
        long_trip_share_20plus_weight=0.06,
        downstream_weight=0.11,
        short_trip_penalty_weight=0.0855,
        same_zone_retention_penalty_weight=0.09,
        pickup_friction_penalty_weight=0.045,
        shared_ride_penalty_weight=0.027,
        market_saturation_penalty_weight=0.162,
    ),
    "manhattan_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.14,
        demand_next_weight=0.18,
        pay_weight=0.12,
        pay_per_min_weight=0.20,
        pay_per_mile_weight=0.06,
        downstream_weight=0.18,
        short_trip_penalty_weight=0.16,
        pickup_friction_penalty_weight=0.10,
        shared_ride_penalty_weight=0.04,
    ),
    "manhattan_v3": ZoneScoreProfileWeights(
        demand_now_weight=0.07,
        demand_next_weight=0.08,
        demand_density_now_weight=0.11,
        demand_density_next_weight=0.08,
        pay_weight=0.07,
        pay_per_min_weight=0.15,
        pay_per_mile_weight=0.08,
        balanced_trip_share_weight=0.11,
        long_trip_share_20plus_weight=0.04,
        downstream_weight=0.09,
        short_trip_penalty_weight=0.07695,
        same_zone_retention_penalty_weight=0.126,
        pickup_friction_penalty_weight=0.045,
        shared_ride_penalty_weight=0.018,
        market_saturation_penalty_weight=0.252,
    ),
    "bronx_wash_heights_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.18,
        demand_next_weight=0.22,
        pay_weight=0.08,
        pay_per_min_weight=0.14,
        pay_per_mile_weight=0.05,
        downstream_weight=0.22,
        short_trip_penalty_weight=0.05,
        pickup_friction_penalty_weight=0.06,
        shared_ride_penalty_weight=0.03,
    ),
    "bronx_wash_heights_v3": ZoneScoreProfileWeights(
        demand_now_weight=0.13,
        demand_next_weight=0.15,
        demand_density_now_weight=0.08,
        demand_density_next_weight=0.07,
        pay_weight=0.05,
        pay_per_min_weight=0.09,
        pay_per_mile_weight=0.05,
        balanced_trip_share_weight=0.08,
        long_trip_share_20plus_weight=0.03,
        downstream_weight=0.14,
        short_trip_penalty_weight=0.04275,
        same_zone_retention_penalty_weight=0.054,
        pickup_friction_penalty_weight=0.036,
        shared_ride_penalty_weight=0.018,
        market_saturation_penalty_weight=0.036,
    ),
    "queens_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.15,
        demand_next_weight=0.20,
        pay_weight=0.08,
        pay_per_min_weight=0.12,
        pay_per_mile_weight=0.11,
        downstream_weight=0.22,
        short_trip_penalty_weight=0.06,
        pickup_friction_penalty_weight=0.04,
        shared_ride_penalty_weight=0.02,
    ),
    "queens_v3": ZoneScoreProfileWeights(
        demand_now_weight=0.10,
        demand_next_weight=0.12,
        demand_density_now_weight=0.12,
        demand_density_next_weight=0.10,
        pay_weight=0.05,
        pay_per_min_weight=0.09,
        pay_per_mile_weight=0.10,
        balanced_trip_share_weight=0.09,
        long_trip_share_20plus_weight=0.06,
        downstream_weight=0.12,
        short_trip_penalty_weight=0.0513,
        same_zone_retention_penalty_weight=0.081,
        pickup_friction_penalty_weight=0.027,
        shared_ride_penalty_weight=0.018,
        market_saturation_penalty_weight=0.054,
    ),
    "brooklyn_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.13,
        demand_next_weight=0.18,
        pay_weight=0.09,
        pay_per_min_weight=0.19,
        pay_per_mile_weight=0.10,
        downstream_weight=0.20,
        short_trip_penalty_weight=0.15,
        pickup_friction_penalty_weight=0.07,
        shared_ride_penalty_weight=0.03,
    ),
    "brooklyn_v3": ZoneScoreProfileWeights(
        demand_now_weight=0.09,
        demand_next_weight=0.10,
        demand_density_now_weight=0.12,
        demand_density_next_weight=0.09,
        pay_weight=0.06,
        pay_per_min_weight=0.13,
        pay_per_mile_weight=0.08,
        balanced_trip_share_weight=0.10,
        long_trip_share_20plus_weight=0.06,
        downstream_weight=0.11,
        short_trip_penalty_weight=0.0855,
        same_zone_retention_penalty_weight=0.099,
        pickup_friction_penalty_weight=0.036,
        shared_ride_penalty_weight=0.018,
        market_saturation_penalty_weight=0.063,
    ),
    "staten_island_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.11,
        demand_next_weight=0.17,
        pay_weight=0.18,
        pay_per_min_weight=0.14,
        pay_per_mile_weight=0.10,
        downstream_weight=0.19,
        short_trip_penalty_weight=0.04,
        pickup_friction_penalty_weight=0.05,
        shared_ride_penalty_weight=0.02,
    ),
    "staten_island_v3": ZoneScoreProfileWeights(
        demand_now_weight=0.08,
        demand_next_weight=0.10,
        demand_density_now_weight=0.05,
        demand_density_next_weight=0.05,
        pay_weight=0.11,
        pay_per_min_weight=0.12,
        pay_per_mile_weight=0.10,
        balanced_trip_share_weight=0.08,
        long_trip_share_20plus_weight=0.06,
        downstream_weight=0.13,
        short_trip_penalty_weight=0.02565,
        same_zone_retention_penalty_weight=0.036,
        pickup_friction_penalty_weight=0.027,
        shared_ride_penalty_weight=0.009,
        market_saturation_penalty_weight=0.018,
    ),
}
