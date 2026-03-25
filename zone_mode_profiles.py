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
    long_trip_share_20plus_weight: float = 0.0
    downstream_weight: float = 0.0
    short_trip_penalty_weight: float = 0.0
    same_zone_retention_penalty_weight: float = 0.0
    pickup_friction_penalty_weight: float = 0.0
    shared_ride_penalty_weight: float = 0.0


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
        demand_now_weight=0.12,
        demand_next_weight=0.12,
        demand_density_now_weight=0.14,
        demand_density_next_weight=0.10,
        pay_weight=0.08,
        pay_per_min_weight=0.14,
        pay_per_mile_weight=0.05,
        long_trip_share_20plus_weight=0.14,
        downstream_weight=0.11,
        short_trip_penalty_weight=0.10,
        same_zone_retention_penalty_weight=0.12,
        pickup_friction_penalty_weight=0.05,
        shared_ride_penalty_weight=0.03,
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
        demand_now_weight=0.08,
        demand_next_weight=0.10,
        demand_density_now_weight=0.13,
        demand_density_next_weight=0.09,
        pay_weight=0.08,
        pay_per_min_weight=0.18,
        pay_per_mile_weight=0.04,
        long_trip_share_20plus_weight=0.17,
        downstream_weight=0.11,
        short_trip_penalty_weight=0.10,
        same_zone_retention_penalty_weight=0.15,
        pickup_friction_penalty_weight=0.05,
        shared_ride_penalty_weight=0.02,
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
        demand_now_weight=0.14,
        demand_next_weight=0.16,
        demand_density_now_weight=0.10,
        demand_density_next_weight=0.09,
        pay_weight=0.06,
        pay_per_min_weight=0.10,
        pay_per_mile_weight=0.04,
        long_trip_share_20plus_weight=0.09,
        downstream_weight=0.15,
        short_trip_penalty_weight=0.05,
        same_zone_retention_penalty_weight=0.07,
        pickup_friction_penalty_weight=0.04,
        shared_ride_penalty_weight=0.02,
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
        demand_now_weight=0.11,
        demand_next_weight=0.13,
        demand_density_now_weight=0.13,
        demand_density_next_weight=0.11,
        pay_weight=0.05,
        pay_per_min_weight=0.09,
        pay_per_mile_weight=0.11,
        long_trip_share_20plus_weight=0.12,
        downstream_weight=0.13,
        short_trip_penalty_weight=0.06,
        same_zone_retention_penalty_weight=0.10,
        pickup_friction_penalty_weight=0.03,
        shared_ride_penalty_weight=0.02,
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
        demand_now_weight=0.10,
        demand_next_weight=0.11,
        demand_density_now_weight=0.14,
        demand_density_next_weight=0.10,
        pay_weight=0.06,
        pay_per_min_weight=0.15,
        pay_per_mile_weight=0.07,
        long_trip_share_20plus_weight=0.13,
        downstream_weight=0.12,
        short_trip_penalty_weight=0.12,
        same_zone_retention_penalty_weight=0.13,
        pickup_friction_penalty_weight=0.04,
        shared_ride_penalty_weight=0.02,
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
        demand_next_weight=0.11,
        demand_density_now_weight=0.06,
        demand_density_next_weight=0.06,
        pay_weight=0.12,
        pay_per_min_weight=0.13,
        pay_per_mile_weight=0.09,
        long_trip_share_20plus_weight=0.16,
        downstream_weight=0.14,
        short_trip_penalty_weight=0.03,
        same_zone_retention_penalty_weight=0.05,
        pickup_friction_penalty_weight=0.03,
        shared_ride_penalty_weight=0.01,
    ),
}
