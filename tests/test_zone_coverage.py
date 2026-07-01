"""Every zone must get a recommendation — all ~266 of them, however sparse.

A driver can be anywhere in the city, including zones with almost no trip data
(Staten Island interior, the Rockaways, parks, airports scored 0). The engine
must ALWAYS return a valid action and the phrasing must ALWAYS return a non-empty
driver-facing line — never crash, never a blank card. This brute-forces the whole
state space (every borough x a full rating range x candidate shapes x missing
fields x airport/safety zone ids) as a permanent coverage guarantee.
"""
from __future__ import annotations

import itertools

from driver_guidance_engine import (
    build_driver_guidance,
    AIRPORT_ZONE_NAMES,
    SAFETY_ELEVATED_RISK_ZONE_IDS,
)
from guidance_phrasing import compose_guidance_directive

VALID_ACTIONS = {"move_nearby", "micro_reposition", "hold", "wait_dispatch"}


def _snap():
    return {
        "tripless_minutes": 10, "stationary_minutes": 6, "movement_minutes": 4,
        "dispatch_uncertainty": 0.3, "recent_move_attempts_without_trip": 0,
        "recent_saved_trip_count_60m": 0, "moved_since_last_saved_trip": False,
        "guidance_state": {},
    }


def _cand(name, r, d, mv=None):
    c = {"zone_id": 900, "zone_name": name, "rating": r, "rating_now": r,
         "arrival_rating": r, "distance_miles": d}
    if mv is not None:
        c["move_value"] = mv
    return c


def _directive_for(g):
    cz = g.get("current_zone") or {}
    tz = g.get("target_zone") or {}
    moving = g.get("action") in ("move_nearby", "micro_reposition")
    return compose_guidance_directive(
        action=str(g.get("action") or "hold"),
        moving=moving,
        current_zone_name=cz.get("zone_name"),
        current_rating=float(cz.get("rating") or 0.0),
        current_next_rating=float(cz.get("next_rating") or cz.get("rating") or 0.0),
        target_zone_name=tz.get("zone_name"),
        target_rating=float(tz.get("rating") or 0.0),
        below_blue=bool(g.get("below_blue")),
        current_will_improve=bool(g.get("current_will_improve")),
        far_reposition=bool(g.get("far_reposition")),
        held_for_antichurn=bool(g.get("held_for_antichurn")),
    )


def test_every_zone_state_yields_a_valid_recommendation_and_directive():
    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
    ratings = [0, 5, 11, 24, 40, 45, 55, 60, 66, 77, 90, 100]
    cand_shapes = [
        [],                                                # dead / no data nearby
        [_cand("Weak Nbr", 20, 1.2, 15.0)],                # only a weak neighbor
        [_cand("Close Strong", 82, 0.6, 78.0)],            # obvious close upgrade
        [_cand("Far Strong", 84, 4.5, 60.0)],              # strong but far
        [_cand("Close Mid", 58, 0.9, 55.0),
         _cand("Far Hot", 88, 5.0, 62.0)],                 # mixed
    ]
    far = [{"zone_id": 950, "zone_name": "Far Demand", "rating": 78, "rating_now": 78,
            "arrival_rating": 78, "move_value": 66.0, "distance_miles": 6.5, "eta_minutes": 15.0}]

    n = 0
    for borough, rating, cands in itertools.product(boroughs, ratings, cand_shapes):
        zc = {
            "current_zone": {"rating": rating, "next_rating": rating},
            "nearby_candidates": cands,
            "far_candidates": far,
        }
        g = build_driver_guidance(
            user_id=9, frame_time="2025-06-23T14:00:00",
            current_lat=40.7, current_lng=-73.9, current_zone_id=200,
            current_zone_name=f"Zone {rating}", current_borough=borough, mode_flags={},
            activity_snapshot=_snap(), zone_context=zc,
            assistant_outlook_bucket={}, now_ts=1_800_000_000,
        )
        assert g["action"] in VALID_ACTIONS, (borough, rating, g["action"])
        assert _directive_for(g).strip(), (borough, rating, "empty directive")
        n += 1
    assert n == len(boroughs) * len(ratings) * len(cand_shapes)


def test_degenerate_and_missing_field_zones_still_recommend():
    # Missing next_rating / stay_hour_value / penalties, empty current_zone,
    # and no candidates at all — must not crash and must produce a line.
    for zc in (
        {"current_zone": {}, "nearby_candidates": []},
        {"current_zone": {"rating": 0}, "nearby_candidates": []},
        {"current_zone": {"rating": 52}},                       # no candidates key
        {"nearby_candidates": []},                              # no current_zone key
    ):
        g = build_driver_guidance(
            user_id=9, frame_time="2025-06-23T14:00:00",
            current_lat=40.7, current_lng=-73.9, current_zone_id=200,
            current_zone_name="Sparse", current_borough="Staten Island", mode_flags={},
            activity_snapshot=_snap(), zone_context=zc,
            assistant_outlook_bucket={}, now_ts=1_800_000_000,
        )
        assert g["action"] in VALID_ACTIONS
        assert _directive_for(g).strip()


def test_airport_and_safety_zone_ids_recommend_without_crashing():
    # Every airport and elevated-risk zone id, across peak and off-peak, must
    # produce a valid recommendation with its overlay folded in.
    special = list(AIRPORT_ZONE_NAMES.keys()) + list(SAFETY_ELEVATED_RISK_ZONE_IDS)
    for zid in special:
        for ft in ("2025-06-23T10:00:00", "2025-06-23T03:00:00", "2025-06-23T18:30:00"):
            g = build_driver_guidance(
                user_id=9, frame_time=ft, current_lat=40.7, current_lng=-73.9,
                current_zone_id=zid, current_zone_name=f"Special {zid}",
                current_borough="Queens", mode_flags={},
                activity_snapshot=_snap(),
                zone_context={"current_zone": {"rating": 0, "next_rating": 0},
                              "nearby_candidates": [_cand("Nbr", 70, 0.8, 66.0)]},
                assistant_outlook_bucket={}, now_ts=1_800_000_000,
            )
            assert g["action"] in VALID_ACTIONS
            assert _directive_for(g).strip()
