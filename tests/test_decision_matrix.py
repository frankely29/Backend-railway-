"""Full stay/move/where/when decision matrix — common-sense guardrails.

Each case is a situation where a sharp driver's call is UNAMBIGUOUS. These lock
the engine to common sense across the whole decision space (not one zone): only
leave when a reachable zone clearly beats staying net of the drive, go to the
closest-best, don't bounce, don't chase a tiny gain across a long deadhead, and
don't abandon a strong rising zone for a marginal hop.
"""
from __future__ import annotations

from driver_guidance_engine import build_driver_guidance


def _inputs(zone_id, name, frame_time, borough="Manhattan"):
    return {
        "user_id": 9, "frame_time": frame_time, "current_lat": 40.75,
        "current_lng": -73.99, "current_zone_id": zone_id, "current_zone_name": name,
        "current_borough": borough, "mode_flags": {}, "assistant_outlook_bucket": {},
        "now_ts": 1_800_000_000,
    }


def _snap(extra=None):
    s = {
        "tripless_minutes": 12, "stationary_minutes": 6, "movement_minutes": 4,
        "dispatch_uncertainty": 0.3, "recent_move_attempts_without_trip": 0,
        "recent_saved_trip_count_60m": 0, "moved_since_last_saved_trip": False,
        "guidance_state": {},
    }
    if extra:
        s.update(extra)
    return s


def _is_move(g):
    return g["action"] in {"move_nearby", "micro_reposition"}


def test_does_not_chase_a_tiny_gain_across_a_long_deadhead():
    # Current zone is fine (60, steady). A zone reads +3 on rating but it's 2.4mi
    # away, so net of the empty drive its move_value (57) is BELOW staying (60).
    # Common sense: don't burn 2.4mi of gas for +3 — stay.
    zc = {
        "current_zone": {"rating": 60, "next_rating": 60, "stay_hour_value": 60.0},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Slightly Better Far", "rating": 63, "rating_now": 63,
             "arrival_rating": 63, "move_value": 57.0, "distance_miles": 2.4},
        ],
    }
    g = build_driver_guidance(**_inputs(200, "Steady Zone", "2025-06-23T14:00:00"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert not _is_move(g)


def test_does_not_abandon_a_strong_rising_zone_for_a_marginal_hop():
    # Current zone is climbing hard (66 -> 74). A nearby zone is 70 now but flat.
    # A veteran rides the rising zone they're already in rather than hop for a
    # zone that won't out-earn it over the hour. Stay.
    zc = {
        "current_zone": {"rating": 66, "next_rating": 74, "rating_40": 75, "rating_60": 75,
                         "stay_hour_value": 72.0, "continuation_raw": 0.6},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Flat Neighbor", "rating": 70, "rating_now": 70,
             "arrival_rating": 70, "move_value": 66.0, "distance_miles": 1.2},
        ],
    }
    g = build_driver_guidance(**_inputs(200, "Rising Zone", "2025-06-21T19:00:00"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert not _is_move(g)


def test_leaves_a_zone_about_to_crater_for_a_stable_strong_neighbor():
    # Busy now (70) but the forecast craters a full bucket (-> 60), and a clearly
    # stronger, close zone holds the hour. A veteran leaves BEFORE it dies.
    zc = {
        "current_zone": {"rating": 70, "next_rating": 60, "rating_40": 58, "rating_60": 57,
                         "stay_hour_value": 61.0},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Holds Strong", "rating": 78, "rating_now": 78,
             "arrival_rating": 78, "move_value": 74.0, "distance_miles": 1.1},
        ],
    }
    g = build_driver_guidance(**_inputs(200, "About To Fade", "2025-06-21T16:00:00"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Holds Strong"


def test_does_not_bounce_after_repeated_moves_for_a_marginal_neighbor():
    # Driver already moved twice without a trip; the nearby zone is only a touch
    # better (move_value barely over staying). Don't churn position again — hold.
    zc = {
        "current_zone": {"rating": 56, "next_rating": 56, "stay_hour_value": 56.0},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Marginal", "rating": 60, "rating_now": 60,
             "arrival_rating": 59, "move_value": 58.0, "distance_miles": 1.6},
        ],
    }
    g = build_driver_guidance(
        **_inputs(200, "Worked Zone", "2025-06-23T15:00:00"),
        activity_snapshot=_snap({"recent_move_attempts_without_trip": 2,
                                 "moved_since_last_saved_trip": True,
                                 "dispatch_uncertainty": 0.5}),
        zone_context=zc)
    assert not _is_move(g)


def test_obvious_upgrade_overrides_a_move_cooldown():
    # A ~2-bucket-better zone (+18) is one block away. Even mid-cooldown, a human
    # always takes it — it's a stable destination, not churn.
    zc = {
        "current_zone": {"rating": 58, "next_rating": 58, "stay_hour_value": 58.0},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Red Hot Close", "rating": 78, "rating_now": 78,
             "arrival_rating": 78, "move_value": 74.0, "distance_miles": 0.7},
        ],
    }
    g = build_driver_guidance(
        **_inputs(200, "Decent Zone", "2025-06-21T20:00:00"),
        activity_snapshot=_snap({
            "recent_move_attempts_without_trip": 3,
            "guidance_state": {"last_guidance_action": "move_nearby",
                               "last_move_guidance_at": 1_800_000_000 - 60},
        }),
        zone_context=zc)
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Red Hot Close"


def test_obvious_upgrade_still_goes_to_the_closest_best_not_the_plus15_zone():
    # Live SoHo 8pm: a +16 zone trips the obvious-upgrade gate (FiDi North 84
    # @0.94mi, move_value 78.7), but a CLOSER zone just under the +15 cutoff has a
    # HIGHER move_value (Lower East Side 82.75 @0.47mi, move_value 80.5). The move
    # must go to the closest-best by move_value, not the zone that tripped the gate.
    zc = {
        "current_zone": {"rating": 68, "next_rating": 68, "stay_hour_value": 65.0,
                         "short_trip_penalty": 0.0, "market_saturation_penalty": 0.0},
        "nearby_candidates": [
            {"zone_id": 8, "zone_name": "Financial District North", "rating": 84, "rating_now": 84,
             "arrival_rating": 84, "move_value": 78.66, "distance_miles": 0.942},
            {"zone_id": 9, "zone_name": "Lower East Side", "rating": 82.75, "rating_now": 82.75,
             "arrival_rating": 83, "move_value": 80.51, "distance_miles": 0.469},
        ],
    }
    g = build_driver_guidance(**_inputs(200, "SoHo", "2025-06-21T20:00:00"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Lower East Side"


def test_clearly_better_zone_stays_the_call_as_current_rating_ticks_up():
    # Stability: a clearly-better, close zone (Lower East Side, move_value ~80)
    # must stay the recommendation across consecutive frames even as the CURRENT
    # zone's rating ticks up (64 -> 71). At 64 LES is a +15 obvious upgrade; at 71
    # it's no longer +15 raw but still +10 move_value — the call must NOT flip from
    # move to hold (the SoHo 7-8pm oscillation a churn-free driver should never see).
    les = {"zone_id": 9, "zone_name": "Lower East Side", "rating": 83, "rating_now": 83,
           "arrival_rating": 83, "move_value": 80.0, "distance_miles": 0.469}
    for cur, nxt, shv in [(64, 71, 67.9), (71, 68, 69.6)]:
        zc = {"current_zone": {"rating": cur, "next_rating": nxt, "stay_hour_value": shv,
                               "short_trip_penalty": 0.0, "market_saturation_penalty": 0.0},
              "nearby_candidates": [dict(les)]}
        g = build_driver_guidance(**_inputs(200, "SoHo", "2025-06-21T19:40:00"),
                                  activity_snapshot=_snap(), zone_context=zc)
        assert g["action"] == "move_nearby", f"flipped to {g['action']} at cur={cur}"
        assert (g["target_zone"] or {}).get("zone_name") == "Lower East Side"


def test_about_to_pick_up_requires_a_sustained_rise_not_a_one_bin_blip():
    # Below blue, +20 blips to blue (61) but the rest of the hour craters (46/45).
    # We must NOT tell the driver to wait on a surge that's already gone — a
    # one-bin spike is not "about to pick up".
    zc = {
        "current_zone": {"rating": 55, "next_rating": 61, "rating_40": 46, "rating_60": 45,
                         "stay_hour_value": 50.0},
        "nearby_candidates": [],
    }
    g = build_driver_guidance(**_inputs(200, "Blip Zone", "2025-06-23T17:00:00", borough="Brooklyn"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert g["current_will_improve"] is False


def test_about_to_pick_up_fires_on_a_sustained_rise():
    # Below blue, +20 reaches blue (62) AND it holds across the hour (64/65) -> a
    # genuine ramp, so "stay, it's about to pick up" is correct.
    zc = {
        "current_zone": {"rating": 55, "next_rating": 62, "rating_40": 64, "rating_60": 65,
                         "stay_hour_value": 62.0},
        "nearby_candidates": [],
    }
    g = build_driver_guidance(**_inputs(200, "Ramp Zone", "2025-06-23T17:00:00", borough="Brooklyn"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert g["current_will_improve"] is True
    assert g["action"] in {"hold", "wait_dispatch"}


def test_held_in_a_short_trip_trap_warns_the_driver():
    # Holding inside a known short-trip trap (high short-trip + saturation penalty)
    # with nothing better reachable. The card must include a factual heads-up, not
    # a bare "stay" — and must NOT claim the offline-until-arrival escape (no move).
    zc = {
        # Both inputs are percentile ranks (median ~0.50), so a genuine trap has
        # to sit in the bad TAIL -- 0.6/0.5 was merely "worse than the median"
        # and used to flag ~22% of the city, including purple zones.
        "current_zone": {"rating": 48, "next_rating": 48, "stay_hour_value": 48.0,
                         "short_trip_penalty": 0.85, "market_saturation_penalty": 0.62},
        "nearby_candidates": [],
    }
    g = build_driver_guidance(**_inputs(200, "Trap Zone", "2025-06-23T15:00:00", borough="Brooklyn"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert not _is_move(g)
    assert g.get("trap_zone") is True
    assert g.get("offline_until_arrival") is False
    assert "short, cheap trips" in (g.get("trap_advice") or "")
    assert "low_trip_trap" in g.get("reason_codes", [])


def test_warns_when_sent_into_a_higher_risk_zone():
    # The engine routes a driver from a non-risk zone INTO a higher-risk one
    # (zone 75). The current-zone safety check never sees the destination, so the
    # driver used to get no warning. Now the card warns about where they're headed
    # and surfaces the rider-rating floor.
    zc = {
        "current_zone": {"rating": 45, "next_rating": 45, "stay_hour_value": 45.0},
        "nearby_candidates": [
            {"zone_id": 75, "zone_name": "East Harlem North", "rating": 66, "rating_now": 66,
             "arrival_rating": 66, "move_value": 62.0, "distance_miles": 1.0},
        ],
    }
    g = build_driver_guidance(**_inputs(200, "Start Zone", "2025-06-23T14:00:00"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "East Harlem North"
    assert g["target_elevated_risk"] is True
    assert g["safety_elevated_risk"] is False
    assert g["safety_min_rider_rating"] == 4.7
    assert "higher-risk" in (g["safety_advice"] or "")
    assert "East Harlem North" in (g["safety_advice"] or "")


def test_takes_a_busier_zone_one_block_over_on_a_smaller_edge():
    # Live Park Slope 11pm: already blue (77) and 'strong-hold' eligible, but
    # Boerum Hill is busier next door (move_value 80.6 @0.6mi) — a +6 net edge over
    # staying (74.3). The flat +8 bucket missed it; a few-minute hop to a clearly
    # busier zone is worth a smaller edge, so we move.
    zc = {
        "current_zone": {"rating": 77, "next_rating": 76, "rating_40": 75, "rating_60": 74,
                         "stay_hour_value": 74.3, "continuation_raw": 0.6},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Boerum Hill", "rating": 81, "rating_now": 81,
             "arrival_rating": 81, "move_value": 80.56, "distance_miles": 0.6},
        ],
    }
    g = build_driver_guidance(**_inputs(200, "Park Slope", "2025-06-21T23:00:00", borough="Brooklyn"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Boerum Hill"


def test_does_not_hop_for_a_small_edge_when_the_better_zone_is_far():
    # Same +6 edge but the better zone is 2.2mi away (not a quick hop) — the full
    # bucket still applies, so we hold the strong zone rather than deadhead for +6.
    zc = {
        "current_zone": {"rating": 77, "next_rating": 76, "rating_40": 75, "rating_60": 74,
                         "stay_hour_value": 74.3, "continuation_raw": 0.6},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Far Better", "rating": 81, "rating_now": 81,
             "arrival_rating": 81, "move_value": 80.56, "distance_miles": 2.2},
        ],
    }
    g = build_driver_guidance(**_inputs(200, "Park Slope", "2025-06-21T23:00:00", borough="Brooklyn"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert not _is_move(g)


def test_dead_area_everywhere_routes_to_the_far_demand():
    # Quiet here and quiet in the whole nearby band, but a strong zone sits within
    # a worthwhile longer drive. A veteran deadheads toward the demand, not sits.
    zc = {
        "current_zone": {"rating": 28, "next_rating": 28, "stay_hour_value": 28.0},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Quiet Neighbor", "rating": 31, "rating_now": 31,
             "arrival_rating": 31, "move_value": 27.0, "distance_miles": 1.5},
        ],
        "far_candidates": [
            {"zone_id": 12, "zone_name": "Strong Far", "rating": 74, "rating_now": 74,
             "arrival_rating": 74, "move_value": 64.0, "distance_miles": 5.5, "eta_minutes": 14.0},
        ],
    }
    g = build_driver_guidance(**_inputs(200, "Dead Zone", "2025-06-23T13:00:00", borough="Queens"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Strong Far"
    assert g.get("far_reposition") is True


def test_manhattan_core_crowding_counts_toward_the_trap():
    # Citywide market saturation is mild, but the Manhattan-core measure says the
    # zone is crowded. The map's assistant already consults that field for
    # Manhattan zones; the server used to ignore it, so the two engines could
    # disagree about whether the same zone was a trap.
    zc = {
        "current_zone": {"rating": 48, "next_rating": 48, "stay_hour_value": 48.0,
                         "short_trip_penalty": 0.85,
                         "market_saturation_penalty": 0.10,
                         "manhattan_core_saturation_penalty": 0.66},
        "nearby_candidates": [],
    }
    g = build_driver_guidance(**_inputs(200, "Core Zone", "2025-06-23T15:00:00", borough="Manhattan"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert g.get("trap_zone") is True


def test_mild_crowding_on_both_measures_is_not_a_trap():
    zc = {
        "current_zone": {"rating": 48, "next_rating": 48, "stay_hour_value": 48.0,
                         "short_trip_penalty": 0.85,
                         "market_saturation_penalty": 0.10,
                         "manhattan_core_saturation_penalty": 0.12},
        "nearby_candidates": [],
    }
    g = build_driver_guidance(**_inputs(200, "Calm Zone", "2025-06-23T15:00:00", borough="Manhattan"),
                              activity_snapshot=_snap(), zone_context=zc)
    assert g.get("trap_zone") is False
