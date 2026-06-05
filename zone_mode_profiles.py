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
    # "Premium long trip" = trip_time ≥ 2700s (45 min) OR trip_miles ≥ 10.
    # Three orthogonal axes, all normalized [0, 1] per time bin:
    #   share    — how many trips here qualify as premium
    #   avg_min  — when they qualify, how LONG they run in time
    #   avg_mile — when they qualify, how FAR they run in miles
    # Profiles that don't care about premium-long-trip behavior leave
    # these at 0.0; trips_45plus_v3 sets them high.
    premium_long_trip_share_weight: float = 0.0
    premium_long_trip_avg_minutes_weight: float = 0.0
    premium_long_trip_avg_miles_weight: float = 0.0
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
        market_saturation_penalty_weight=0.1936,
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
        demand_density_now_weight=0.11,
        demand_density_next_weight=0.09,
        pay_weight=0.07,
        pay_per_min_weight=0.14,
        pay_per_mile_weight=0.08,
        balanced_trip_share_weight=0.11,
        long_trip_share_20plus_weight=0.02,
        downstream_weight=0.08,
        short_trip_penalty_weight=0.055,
        same_zone_retention_penalty_weight=0.09,
        pickup_friction_penalty_weight=0.032,
        shared_ride_penalty_weight=0.012,
        market_saturation_penalty_weight=0.3872,
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
        market_saturation_penalty_weight=0.121,
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
        market_saturation_penalty_weight=0.121,
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
        market_saturation_penalty_weight=0.121,
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
        demand_now_weight=0.10,
        demand_next_weight=0.12,
        demand_density_now_weight=0.04,
        demand_density_next_weight=0.04,
        pay_weight=0.13,
        pay_per_min_weight=0.13,
        pay_per_mile_weight=0.12,
        balanced_trip_share_weight=0.11,
        long_trip_share_20plus_weight=0.02,
        downstream_weight=0.07,
        short_trip_penalty_weight=0.014,
        same_zone_retention_penalty_weight=0.016,
        pickup_friction_penalty_weight=0.012,
        shared_ride_penalty_weight=0.004,
        # Per driver: zero out saturation for Staten Island so the
        # score never penalizes Staten zones for saturation.
        market_saturation_penalty_weight=0.0,
    ),
    # "45+ trips mode": ranks zones by likelihood of getting a long trip.
    # Designed so a zone scores high when the share of long trips AND the
    # absolute volume of trips are both high — i.e. "many trips here, and
    # a large fraction of them are long ones."
    #
    # Signals used (all already computed per-zone in the live engine):
    #   long_trip_share_20plus_n  — share of trips ≥ 20 min (proxy for
    #                               long-distance trips; the closest
    #                               long-trip signal the data exposes)
    #   demand_now_n              — current volume of pickups
    #   demand_density_now_n      — volume normalized by zone area
    #   pay_per_mile_n            — long trips correlate with high $/mi
    #
    # Weights are tuned so a zone with avg demand but high long_trip_share
    # still beats a zone with high demand but low long_trip_share. Short
    # trip penalty zeroed since short-trip behaviour is irrelevant here.
    # Saturation penalty kept moderate so drivers aren't routed into
    # over-saturated zones even if those zones have long trips.
    "trips_45plus_v3": ZoneScoreProfileWeights(
        # Volume / demand signals — needed for QUANTITY of long trips
        # (share alone isn't enough; a zone with 1 trip that's 45 min
        # long shouldn't outrank a zone with 50 trips half of which
        # are 45 min).
        demand_now_weight=0.10,
        demand_next_weight=0.06,
        demand_density_now_weight=0.10,
        demand_density_next_weight=0.06,
        # Pay-per-mile correlates with trip length, so a soft bonus.
        pay_weight=0.03,
        pay_per_min_weight=0.05,
        pay_per_mile_weight=0.12,
        balanced_trip_share_weight=0.02,
        # Keep a small weight on the 20+ minute share so zones with any
        # long-ish trip volume still get partial credit.
        long_trip_share_20plus_weight=0.05,
        # PRIMARY signals for "45+ trips mode":
        #   share    — how often a qualifying long trip happens here
        #   avg_min  — when one happens, how long it runs in minutes
        #   avg_mile — when one happens, how far it runs in miles
        # Together these reward zones that have MANY premium long trips
        # AND where those trips tend to be even longer.
        premium_long_trip_share_weight=0.22,
        premium_long_trip_avg_minutes_weight=0.10,
        premium_long_trip_avg_miles_weight=0.10,
        downstream_weight=0.04,
        # Short-trip behavior is irrelevant for this mode.
        short_trip_penalty_weight=0.01,
        same_zone_retention_penalty_weight=0.03,
        pickup_friction_penalty_weight=0.03,
        shared_ride_penalty_weight=0.02,
        # Saturation still matters — drivers don't want to be routed
        # into an over-supplied zone even if it has long trips.
        market_saturation_penalty_weight=0.10,
    ),
}


def validate_zone_mode_profiles_for_live_engine() -> None:
    """
    Guard against tuning profile fields that are currently inactive in the live SQL engine.
    These fields are intentionally expected to remain 0.0 until the engine path is updated.
    """
    violations: list[str] = []
    for profile_name, profile in ZONE_MODE_PROFILES.items():
        if float(profile.balanced_trip_quality_weight) != 0.0:
            violations.append(
                f"{profile_name}.balanced_trip_quality_weight={profile.balanced_trip_quality_weight}"
            )
        if float(profile.saturation_penalty_weight) != 0.0:
            violations.append(
                f"{profile_name}.saturation_penalty_weight={profile.saturation_penalty_weight}"
            )

    if violations:
        joined = ", ".join(violations)
        raise RuntimeError(
            "Zone mode profile validation failed: inactive live-engine fields were tuned. "
            "The live SQL engine currently does not use balanced_trip_quality_weight or "
            "saturation_penalty_weight, so they must stay 0.0 unless the engine is updated too. "
            f"Violations: {joined}"
        )
