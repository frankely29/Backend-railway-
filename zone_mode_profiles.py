from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class ZoneScoreProfileWeights:
    demand_now_weight: float
    demand_next_weight: float
    pay_weight: float
    pay_per_min_weight: float
    pay_per_mile_weight: float
    downstream_weight: float
    short_trip_penalty_weight: float
    pickup_friction_penalty_weight: float
    shared_ride_penalty_weight: float


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
    "manhattan_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.18,
        demand_next_weight=0.14,
        pay_weight=0.24,
        pay_per_min_weight=0.18,
        pay_per_mile_weight=0.08,
        downstream_weight=0.11,
        short_trip_penalty_weight=0.04,
        pickup_friction_penalty_weight=0.02,
        shared_ride_penalty_weight=0.01,
    ),
    "bronx_wash_heights_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.22,
        demand_next_weight=0.16,
        pay_weight=0.20,
        pay_per_min_weight=0.14,
        pay_per_mile_weight=0.10,
        downstream_weight=0.10,
        short_trip_penalty_weight=0.04,
        pickup_friction_penalty_weight=0.03,
        shared_ride_penalty_weight=0.01,
    ),
    "queens_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.21,
        demand_next_weight=0.15,
        pay_weight=0.21,
        pay_per_min_weight=0.15,
        pay_per_mile_weight=0.10,
        downstream_weight=0.11,
        short_trip_penalty_weight=0.04,
        pickup_friction_penalty_weight=0.02,
        shared_ride_penalty_weight=0.01,
    ),
    "brooklyn_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.21,
        demand_next_weight=0.15,
        pay_weight=0.20,
        pay_per_min_weight=0.15,
        pay_per_mile_weight=0.11,
        downstream_weight=0.11,
        short_trip_penalty_weight=0.04,
        pickup_friction_penalty_weight=0.02,
        shared_ride_penalty_weight=0.01,
    ),
    "staten_island_v2": ZoneScoreProfileWeights(
        demand_now_weight=0.23,
        demand_next_weight=0.16,
        pay_weight=0.19,
        pay_per_min_weight=0.14,
        pay_per_mile_weight=0.11,
        downstream_weight=0.10,
        short_trip_penalty_weight=0.04,
        pickup_friction_penalty_weight=0.02,
        shared_ride_penalty_weight=0.01,
    ),
}
