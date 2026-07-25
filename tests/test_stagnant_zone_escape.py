"""A hold is only defensible if waiting buys something.

IMG_4537: a driver sitting in a yellow, flat-trend Coney Island was told "Bay
Ridge's busier, but not worth the drive yet." The word "yet" promises a turn,
but the forecast had this zone flat and sub-blue for the whole next hour. Holding
there is not a free option -- it is a decision to bank another bin of sub-blue
earnings for a rise that is not coming.

So a STAGNANT sub-blue zone (no meaningful rise in the next 20/40/60 min) has to
clear a WIDER margin before "staying out-earns moving" blocks a move, and a
clearly better reachable zone wins on a smaller arrival edge. It must still
out-earn staying after the deadhead, so this never sends a driver on a losing
drive.
"""
from __future__ import annotations

from driver_guidance_engine import build_driver_guidance
from guidance_phrasing import compose_guidance_directive


def _snap(**over):
    base = {
        "tripless_minutes": 30.0, "stationary_minutes": 16.0, "movement_minutes": 30.0,
        "zone_dwell_minutes": 25.0, "moved_since_last_saved_trip": False,
        "recent_saved_trip_count_60m": 0, "recent_move_attempts_without_trip": 0,
        "dispatch_uncertainty": 0.4, "guidance_state": {},
    }
    base.update(over)
    return base


def _inputs(zone_id, zone_name, frame_time, borough="Brooklyn"):
    return {
        "user_id": 1, "frame_time": frame_time,
        "current_lat": 40.575, "current_lng": -73.985,
        "current_zone_id": zone_id, "current_zone_name": zone_name,
        "current_borough": borough, "mode_flags": {}, "assistant_outlook_bucket": {},
        "now_ts": 1_760_000_000,
    }


def _coney_island_flat(target_rating=52.0, target_move_value=50.0, distance=2.2):
    """The screenshot: yellow, flat all hour, a busier zone within reach."""
    return {
        "current_zone": {
            "rating": 45.0, "next_rating": 45.0, "rating_40": 46.0, "rating_60": 44.0,
            "stay_hour_value": 45.0, "continuation_n": 0.5,
        },
        "nearby_candidates": [{
            "zone_id": 14, "zone_name": "Bay Ridge", "borough": "Brooklyn",
            "distance_miles": distance, "eta_minutes": 11.0,
            "rating": target_rating, "arrival_rating": target_rating,
            "rating_now": target_rating, "move_value": target_move_value,
        }],
    }


def _guide(zc, snap=None, frame="2025-07-25T14:00:00"):
    return build_driver_guidance(
        **_inputs(21, "Coney Island", frame),
        activity_snapshot=snap or _snap(), zone_context=zc,
    )


def test_flat_sub_blue_zone_is_flagged_stagnant():
    g = _guide(_coney_island_flat())
    assert g["current_is_stagnant"] is True
    assert g["current_will_improve"] is False


def test_stagnant_zone_sends_the_driver_to_the_better_zone():
    """The reported behaviour: it used to hold here."""
    g = _guide(_coney_island_flat())
    assert g["action"] == "move_nearby", g["message"]
    assert (g.get("target_zone") or {}).get("zone_name") == "Bay Ridge"
    assert "below_blue_stagnant" in g["reason_codes"]
    assert "isn't picking up" in g["message"]


def test_a_zone_that_is_climbing_is_not_stagnant_and_still_holds():
    zc = _coney_island_flat()
    zc["current_zone"].update({
        "next_rating": 62.0, "rating_40": 64.0, "rating_60": 63.0, "stay_hour_value": 62.0,
    })
    g = _guide(zc)
    assert g["current_is_stagnant"] is False
    assert g["action"] in {"hold", "wait_dispatch"}
    assert "about to pick up" in g["message"] or g["current_will_improve"]


def test_stagnant_but_the_drive_still_loses_stays_put():
    """The guard: cheaper bar, not a free pass. A target that does not out-earn
    staying after the deadhead must not pull the driver."""
    zc = _coney_island_flat(target_rating=52.0, target_move_value=40.0)
    g = _guide(zc)
    assert g["current_is_stagnant"] is True
    assert g["action"] in {"hold", "wait_dispatch"}, g["message"]


def test_stagnant_with_only_a_marginal_target_stays_put():
    # +3 on arrival is not worth a drive even from a dead zone.
    zc = _coney_island_flat(target_rating=48.0, target_move_value=47.0)
    g = _guide(zc)
    assert g["action"] in {"hold", "wait_dispatch"}


def test_stagnant_zone_does_not_lower_the_bar_for_an_improving_zone():
    """STAGNANT_MOVE_MIN_IMPROVEMENT must not leak into the normal ladder."""
    zc = _coney_island_flat(target_rating=52.0, target_move_value=50.0)
    zc["current_zone"].update({
        "next_rating": 50.0, "rating_40": 54.0, "rating_60": 55.0, "stay_hour_value": 53.0,
    })
    g = _guide(zc)
    # Rising by 10 over the hour -> not stagnant -> +7 target must not pull it.
    assert g["current_is_stagnant"] is False
    assert g["action"] in {"hold", "wait_dispatch"}


def test_stagnant_hold_message_stops_promising_a_turnaround():
    stagnant = compose_guidance_directive(
        action="wait_dispatch", moving=False, current_zone_name="Coney Island",
        current_rating=45, current_next_rating=45, below_blue=True,
        busier_zone_name="Bay Ridge", current_is_stagnant=True,
    )
    assert "isn't picking up" in stagnant
    assert "not worth the drive yet" not in stagnant, "'yet' promises a rise that isn't forecast"

    # A zone that merely isn't worth the drive right now keeps the old wording.
    normal = compose_guidance_directive(
        action="wait_dispatch", moving=False, current_zone_name="Coney Island",
        current_rating=45, current_next_rating=45, below_blue=True,
        busier_zone_name="Bay Ridge", current_is_stagnant=False,
    )
    assert "not worth the drive yet" in normal


def test_above_blue_zone_is_never_stagnant():
    zc = _coney_island_flat()
    zc["current_zone"].update({
        "rating": 72.0, "next_rating": 72.0, "rating_40": 72.0, "rating_60": 72.0,
        "stay_hour_value": 72.0,
    })
    g = _guide(zc)
    assert g["current_is_stagnant"] is False


def test_stay_margin_direction_frees_the_driver_not_traps_them():
    """Regression on the sign of the stay margin.

    stay_beats_moving requires STAYING to win BY the margin, so shrinking it
    makes holding EASIER. A first attempt at this feature lowered the margin for
    stagnant zones, which pinned drivers harder in exactly the zones meant to be
    escaped -- it broke the blue-floor move and the anti-churn honesty check.
    A stagnant zone must face a WIDER bar, so a tie goes to the driver leaving.
    """
    from driver_guidance_engine import (
        HOUR_STAY_PREFERENCE,
        STAGNANT_STAY_EXTRA_MARGIN,
    )
    assert STAGNANT_STAY_EXTRA_MARGIN > 0, "stagnant zones must prove MORE, not less"

    # A move that staying beats by only the ordinary margin must still be allowed
    # out of a stagnant zone (it would have been blocked under the old margin).
    zc = _coney_island_flat(target_rating=53.0, target_move_value=48.0)
    zc["current_zone"]["stay_hour_value"] = 48.0 + HOUR_STAY_PREFERENCE + 0.5
    g = _guide(zc)
    assert g["current_is_stagnant"] is True
    # Staying wins by the ordinary margin but NOT by the wider stagnant one, so
    # the hold must not be justified by "staying out-earns moving".
    assert g.get("held_for_antichurn") is not True
