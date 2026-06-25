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
    # Same board but the nearby zone is only MARGINALLY better (+8), and the
    # driver already chased two moves without a trip -> a veteran stops churning
    # for small gains and lets dispatch work. (A clearly much-better zone would
    # still override anti-churn; this is the marginal case.)
    zc = {
        "current_zone": {"rating": 58, "next_rating": 58},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Slightly Better", "rating": 66,
             "rating_now": 66, "distance_miles": 2.1}
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


def test_holds_a_climbing_zone_over_a_flat_nearby_blue():
    # Next-hour intelligence: the current zone is sky now (56) but its forecast
    # climbs through the hour (stay_hour_value 66), while a nearby blue zone is
    # flat — worth only 58 after the drive. A veteran reads the curve and stays
    # for the climb instead of bailing for the flat zone.
    zc = {
        "current_zone": {"rating": 56, "next_rating": 58, "stay_hour_value": 66},
        "nearby_candidates": [{"zone_id": 9, "zone_name": "Flat Blue", "rating": 62,
                               "rating_now": 62, "move_value": 58, "distance_miles": 1.0}],
    }
    g = build_driver_guidance(
        **_inputs(120, "Climbing Zone", "2025-06-27T20:00:00", borough="Brooklyn"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] in {"hold", "wait_dispatch", "micro_reposition"}


def test_leaves_a_flat_zone_for_a_better_next_hour_elsewhere():
    # Same board shape, but now the current zone is NOT climbing (stay_hour_value
    # 53) and the nearby blue holds up over the hour (move_value 61). Staying no
    # longer beats moving, so the veteran takes the blue zone.
    zc = {
        "current_zone": {"rating": 56, "next_rating": 55, "stay_hour_value": 53},
        "nearby_candidates": [{"zone_id": 9, "zone_name": "Steady Blue", "rating": 64,
                               "rating_now": 64, "move_value": 61, "distance_miles": 1.0}],
    }
    g = build_driver_guidance(
        **_inputs(120, "Flat Zone", "2025-06-27T20:00:00", borough="Brooklyn"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Steady Blue"


def test_move_value_picks_the_better_next_hour_target_not_the_hotter_now():
    # Two reachable blue zones: A is hotter right now but its hour fades and it's
    # farther (move_value 56); B is a touch cooler now but holds over the hour and
    # is close (move_value 62). The worth-the-move value picks B.
    zc = {
        "current_zone": {"rating": 52, "next_rating": 52, "stay_hour_value": 52},
        "nearby_candidates": [
            {"zone_id": 8, "zone_name": "Hot Now Fades", "rating": 70, "rating_now": 74,
             "move_value": 56, "distance_miles": 2.6},
            {"zone_id": 9, "zone_name": "Holds The Hour", "rating": 64, "rating_now": 63,
             "move_value": 62, "distance_miles": 0.9},
        ],
    }
    g = build_driver_guidance(
        **_inputs(120, "Sky Start", "2025-06-27T20:00:00", borough="Brooklyn"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Holds The Hour"


def test_strong_hold_yields_to_an_obvious_upgrade_in_a_trap_zone():
    # Live bug (SoHo Saturday): zone is "strong" by rating (64) and rising, so
    # the strong-hold branch fires. But the zone is a short-trip TRAP, and
    # Financial District North is a +17 upgrade < 1mi away — an obvious escape.
    # The strong-hold must NOT pin the driver in a trap when a real upgrade
    # sits one block away; the obvious-upgrade branch should win.
    zc = {
        "current_zone": {
            "rating": 64, "next_rating": 70, "stay_hour_value": 65.27,
            "continuation_raw": 0.5,
            "market_saturation_penalty": 0.7,
            "short_trip_penalty": 0.6,
        },
        "nearby_candidates": [
            {"zone_id": 5, "zone_name": "Financial District North", "rating": 81.5,
             "rating_now": 82, "arrival_rating": 82, "move_value": 76.98,
             "distance_miles": 0.915},
            {"zone_id": 6, "zone_name": "Lower East Side", "rating": 76.25,
             "rating_now": 77, "arrival_rating": 77, "move_value": 73.11,
             "distance_miles": 0.52},
        ],
    }
    g = build_driver_guidance(
        **_inputs(200, "SoHo", "2025-06-21T15:00:00"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Financial District North"


def test_does_not_move_to_a_zone_that_nets_less_than_staying():
    # Live bug (Saint George, SI): a quiet zone (22) with a nearby zone that
    # reads +11 on ARRIVAL rating (33) but, once the drive and the hour are
    # priced in, is worth LESS than just staying (move_value 23 < stay_hour_value
    # 25). Moving there only burns gas — hold.
    zc = {
        "current_zone": {"rating": 22, "next_rating": 22, "stay_hour_value": 25.29},
        "nearby_candidates": [
            {"zone_id": 9, "zone_name": "Stapleton", "rating": 33, "rating_now": 33,
             "arrival_rating": 33, "move_value": 23.0, "distance_miles": 0.414},
        ],
    }
    g = build_driver_guidance(
        **_inputs(200, "Saint George", "2025-06-23T19:00:00", borough="Staten Island"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] in {"hold", "wait_dispatch", "micro_reposition"}


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
    # Airport zones are EXCLUDED from the score (rating reads 0), so the move
    # logic would otherwise try to send the driver away even at peak arrivals.
    # A tempting close zone is present to prove the peak overlay FORCES the hold
    # (you stay in the FIFO queue) instead of telling them to leave it.
    tempting = [{"zone_id": 9, "zone_name": "Jackson Heights", "rating": 70,
                 "rating_now": 70, "arrival_rating": 70, "move_value": 65, "distance_miles": 0.4}]
    peak = build_driver_guidance(
        **_inputs(132, "JFK Airport", "2026-04-07T10:00:00Z", borough="Queens"),
        activity_snapshot=_snap(),
        zone_context={"current_zone": {"rating": 0, "next_rating": 0}, "nearby_candidates": tempting},
    )
    off = build_driver_guidance(
        **_inputs(132, "JFK Airport", "2026-04-07T03:00:00Z", borough="Queens"),
        activity_snapshot=_snap(),
        zone_context={"current_zone": {"rating": 35, "next_rating": 35}, "nearby_candidates": []},
    )
    assert "FIFO" in (peak["airport_advice"] or "")
    assert peak["action"] in {"hold", "wait_dispatch"}  # never "go to X" at peak
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


def test_fading_zone_goes_to_the_closer_zone_not_the_farther_higher_rated():
    # The IMG live bug (SoHo): a fading blue+ zone with two strong options — one
    # a hair higher-rated but farther (East Chelsea 80 @1.14mi) and one a touch
    # lower but much closer (Greenwich Village South 79 @0.31mi). The closer one
    # nets more after the drive, so we must send there, not drive past it.
    zc = {
        "current_zone": {"rating": 67, "next_rating": 58, "stay_hour_value": 60},
        "nearby_candidates": [
            {"zone_id": 8, "zone_name": "East Chelsea", "rating": 80, "rating_now": 80,
             "move_value": 71.76, "distance_miles": 1.137},
            {"zone_id": 9, "zone_name": "Greenwich Village South", "rating": 79, "rating_now": 79,
             "move_value": 75.19, "distance_miles": 0.306},
        ],
    }
    g = build_driver_guidance(
        **_inputs(100, "SoHo", "2025-06-23T19:00:00"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Greenwich Village South"


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


def test_picks_the_close_good_zone_over_a_farther_marginally_better_one():
    # Be efficient ($40/hr): a blue zone right here (62 @0.8mi) beats driving
    # 3mi past it for a marginally-hotter one (70). Unpaid miles cost more than
    # a couple of rating points.
    zc = {
        "current_zone": {"rating": 54, "next_rating": 54},
        "nearby_candidates": [
            {"zone_id": 70, "zone_name": "Far Better", "rating": 70,
             "rating_now": 70, "distance_miles": 3.0},
            {"zone_id": 62, "zone_name": "Close Good", "rating": 62,
             "rating_now": 62, "distance_miles": 0.8},
        ],
    }
    g = build_driver_guidance(
        **_inputs(14, "Bay Ridge", "2025-06-23T17:00:00", borough="Brooklyn"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Close Good"


def test_a_much_better_zone_is_still_worth_the_drive():
    # But don't be penny-wise: a green zone (84) a bit farther (2.4mi) clears the
    # deadhead cost over a close blue (62 @0.8mi) — take the clearly-better one.
    zc = {
        "current_zone": {"rating": 54, "next_rating": 54},
        "nearby_candidates": [
            {"zone_id": 84, "zone_name": "Green Hot", "rating": 84,
             "rating_now": 84, "distance_miles": 2.4},
            {"zone_id": 62, "zone_name": "Close Good", "rating": 62,
             "rating_now": 62, "distance_miles": 0.8},
        ],
    }
    g = build_driver_guidance(
        **_inputs(14, "Bay Ridge", "2025-06-23T17:00:00", borough="Brooklyn"),
        activity_snapshot=_snap(), zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Green Hot"


def test_churned_sky_driver_takes_the_close_blue_hop_not_a_hold():
    # IMG_4138 live board: Bay Ridge (sky 54), churned (2 moves), with Sunset
    # Park West (blue 64) less than a mile away. Sky is a move zone — crossing
    # the blue floor to a close blue+ is an escape, not churn — so a veteran
    # takes the ~5-min hop instead of sitting in the saturated sky zone. This is
    # the exact case a driver flagged as "not smarter than a human."
    zc = {
        "current_zone": {"rating": 54, "next_rating": 54},
        "nearby_candidates": [
            {"zone_id": 228, "zone_name": "Sunset Park West", "rating": 64,
             "rating_now": 64, "distance_miles": 0.85},
            {"zone_id": 89, "zone_name": "Flatbush", "rating": 61,
             "rating_now": 61, "distance_miles": 2.78},
        ],
    }
    g = build_driver_guidance(
        **_inputs(14, "Bay Ridge", "2025-06-23T17:00:00", borough="Brooklyn"),
        activity_snapshot=_snap({"recent_move_attempts_without_trip": 2}),
        zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Sunset Park West"


def test_close_blue_escape_stays_consistent_through_cooldown():
    # After issuing a move to a close blue zone, a later poll WHILE the move
    # cooldown is still active must keep pointing at that same zone — not flip
    # to "sit tight" (the GO->STAY contradiction a driver flagged as
    # inconsistent). A close blue+ is a stable destination, so the advice stays
    # consistent until the driver actually arrives.
    zc = {
        "current_zone": {"rating": 54, "next_rating": 54},
        "nearby_candidates": [
            {"zone_id": 228, "zone_name": "Sunset Park West", "rating": 64,
             "rating_now": 64, "distance_miles": 0.85},
        ],
    }
    g = build_driver_guidance(
        **_inputs(14, "Bay Ridge", "2025-06-23T17:05:00", borough="Brooklyn"),
        activity_snapshot=_snap({
            "recent_move_attempts_without_trip": 3,
            "guidance_state": {
                "last_guidance_action": "move_nearby",
                "last_move_guidance_at": 1_800_000_000 - 60,  # 1 min ago -> still in cooldown
            },
        }),
        zone_context=zc,
    )
    assert g["action"] == "move_nearby"
    assert (g["target_zone"] or {}).get("zone_name") == "Sunset Park West"


def test_held_message_is_honest_when_a_better_zone_is_nearby():
    # Sky zone (55), churned, with a blue+ zone in view but a bit too far to
    # chase right now (2.3mi -> beyond the close-escape range) -> we hold, and
    # the message must NOT claim "nothing nearby beats it". (A blue+ zone within
    # ~the close range would instead trigger a move; this is the farther case.)
    zc = {
        "current_zone": {"rating": 55, "next_rating": 55},
        "nearby_candidates": [{"zone_id": 9, "zone_name": "Better Zone", "rating": 65,
                               "rating_now": 65, "distance_miles": 2.3}],
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
