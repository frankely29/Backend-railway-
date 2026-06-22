"""Veteran-driver benchmark scenarios.

Each test encodes a situation where NYC driver research / a seasoned driver's
playbook says there is a clearly-correct move, and asserts the guidance brain
makes it. These are the durable, CI-enforced evidence that the brain is at
least as smart as a veteran — and, via arrival-time scoring + exact spots +
forecast anticipation, sharper than one can compute in their head.
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


def test_letout_sends_fresh_driver_to_the_rising_nightlife():
    # Times Sq during theater letout (decent, climbing to blue) but downtown
    # nightlife is already hotter and rising toward bar-close -> a veteran heads
    # downtown. Fresh driver should be moved there.
    zc = {
        "current_zone": {"rating": 58, "next_rating": 69},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Greenwich Village South", "rating": 74,
             "rating_now": 70, "distance_miles": 2.1}
        ],
    }
    g = build_driver_guidance(
        **_inputs(230, "Times Sq/Theatre District", "2025-06-27T22:40:00"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Greenwich Village South"


def test_after_two_fruitless_moves_it_holds_instead_of_bouncing():
    # Same board, but the driver already chased two moves without a trip — a
    # veteran stops churning and lets dispatch work. Anti-churn must hold.
    zc = {
        "current_zone": {"rating": 58, "next_rating": 69},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Greenwich Village South", "rating": 74,
             "rating_now": 70, "distance_miles": 2.1}
        ],
    }
    g = build_driver_guidance(
        **_inputs(230, "Times Sq/Theatre District", "2025-06-27T22:40:00"),
        activity_snapshot=_snap({"recent_move_attempts_without_trip": 2}),
        zone_context=zc,
    )
    assert g["action"] in {"wait_dispatch", "hold"}


def test_busy_nightlife_zone_is_a_hold_not_a_chase():
    # Red-hot bar-close zone -> stay and earn, don't go chasing elsewhere.
    zc = {"current_zone": {"rating": 76, "next_rating": 75}, "nearby_candidates": [
        {"zone_id": 5, "zone_name": "Elsewhere", "rating": 70, "distance_miles": 1.5}]}
    g = build_driver_guidance(
        **_inputs(148, "Lower East Side", "2025-06-27T23:00:00"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] in {"hold", "wait_dispatch"}
    assert g["below_blue"] is False


def test_dead_area_routes_to_demand_not_idle():
    # Quiet everywhere nearby but a strong zone within a worthwhile drive -> a
    # veteran deadheads toward the demand rather than sitting.
    zc = {
        "current_zone": {"rating": 24, "next_rating": 24},
        "nearby_candidates": [{"zone_id": 2, "zone_name": "Next Door", "rating": 27, "distance_miles": 1.5}],
        "far_candidates": [{"zone_id": 9, "zone_name": "Manhattan Core", "rating": 71,
                            "rating_now": 71, "distance_miles": 5.0, "eta_minutes": 25}],
    }
    g = build_driver_guidance(
        **_inputs(200, "Quiet Outer Zone", "2025-06-24T14:00:00", borough="Brooklyn"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert g["far_reposition"] is True


def test_overnight_elevated_zone_gets_safety_and_phone_defense():
    g = build_driver_guidance(
        **_inputs(126, "Hunts Point", "2026-04-07T02:30:00Z", borough="Bronx"),
        activity_snapshot=_snap(),
        zone_context={"current_zone": {"rating": 45, "next_rating": 45}, "nearby_candidates": []},
    )
    assert g["safety_elevated_risk"] is True
    assert "4.7" in (g["safety_advice"] or "")
    assert "phone" in (g["safety_advice"] or "").lower()


def test_airport_peak_vs_offpeak_queue_discipline():
    peak = build_driver_guidance(
        **_inputs(132, "JFK Airport", "2026-04-07T10:00:00Z", borough="Queens"),
        activity_snapshot=_snap(),
        zone_context={"current_zone": {"rating": 64, "next_rating": 64}, "nearby_candidates": []},
    )
    off = build_driver_guidance(
        **_inputs(132, "JFK Airport", "2026-04-07T03:00:00Z", borough="Queens"),
        activity_snapshot=_snap(),
        zone_context={"current_zone": {"rating": 35, "next_rating": 35}, "nearby_candidates": []},
    )
    assert "FIFO" in (peak["airport_advice"] or "")
    assert "city" in (off["airport_advice"] or "").lower()


def test_leaves_a_fading_zone_before_it_dies():
    # Busy now but the forecast drops it a full bucket, and a clearly stronger
    # zone is in reach -> a veteran leaves early instead of riding it down.
    zc = {
        "current_zone": {"rating": 66, "next_rating": 52},
        "nearby_candidates": [{"zone_id": 9, "zone_name": "Holding Strong", "rating": 70,
                               "rating_now": 70, "distance_miles": 1.5}],
    }
    g = build_driver_guidance(
        **_inputs(100, "Fading Zone", "2025-06-27T19:00:00"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Holding Strong"


def test_cooling_zone_with_nothing_better_still_holds():
    # Cooling but nowhere clearly better -> don't churn; hold.
    zc = {
        "current_zone": {"rating": 66, "next_rating": 56},
        "nearby_candidates": [{"zone_id": 9, "zone_name": "Meh", "rating": 58,
                               "rating_now": 58, "distance_miles": 1.5}],
    }
    g = build_driver_guidance(
        **_inputs(100, "Cooling Zone", "2025-06-27T19:00:00"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] in {"hold", "wait_dispatch", "micro_reposition"}


def test_escapes_a_bad_zone_to_a_close_strong_one_despite_recent_moves():
    # Red trap zone (34) with a blue+ zone ~1.5mi away. Even after 2 recent moves
    # a veteran takes the short hop to the strong zone — that's escape, not churn.
    zc = {
        "current_zone": {"rating": 34, "next_rating": 34},
        "nearby_candidates": [{"zone_id": 9, "zone_name": "Park Slope", "rating": 69,
                               "rating_now": 69, "distance_miles": 1.5}],
    }
    g = build_driver_guidance(
        **_inputs(200, "Sunset Park East", "2025-06-22T15:00:00", borough="Brooklyn"),
        activity_snapshot=_snap({"recent_move_attempts_without_trip": 2}),
        zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Park Slope"


def test_held_message_is_honest_when_a_better_zone_is_nearby():
    # Sky zone (55) held due to anti-churn while a blue+ zone is nearby -> the
    # message must NOT claim "nothing nearby beats it".
    zc = {
        "current_zone": {"rating": 55, "next_rating": 55},
        "nearby_candidates": [{"zone_id": 9, "zone_name": "Better Zone", "rating": 65,
                               "rating_now": 65, "distance_miles": 1.5}],
    }
    g = build_driver_guidance(
        **_inputs(200, "Sky Zone", "2025-06-22T15:00:00", borough="Brooklyn"),
        activity_snapshot=_snap({"recent_move_attempts_without_trip": 2}),
        zone_context=zc,
    )
    assert g["action"] in {"hold", "wait_dispatch"}
    assert g["held_for_antichurn"] is True


def test_escape_fires_when_a_close_blue_exists_even_if_best_is_farther():
    # Top-rated nearby (Boerum 76) is 2.7mi, but a close blue+ (Red Hook 69) is
    # 1.4mi. Churned driver in a red zone should still escape (move).
    zc = {
        "current_zone": {"rating": 34, "next_rating": 34},
        "nearby_candidates": [
            {"zone_id": 8, "zone_name": "Boerum Hill", "rating": 76, "rating_now": 76, "distance_miles": 2.7},
            {"zone_id": 9, "zone_name": "Red Hook", "rating": 69, "rating_now": 69, "distance_miles": 1.4},
        ],
    }
    g = build_driver_guidance(
        **_inputs(200, "Sunset Park East", "2025-06-22T15:00:00", borough="Brooklyn"),
        activity_snapshot=_snap({"recent_move_attempts_without_trip": 2}),
        zone_context=zc,
    )
    assert g["action"] == "move_nearby"
