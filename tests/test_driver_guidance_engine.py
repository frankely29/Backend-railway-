from __future__ import annotations

import sqlite3

from driver_guidance_engine import build_driver_guidance
from pickup_recording_feature import _settle_latest_assistant_guidance_outcome_tx


def _base_guidance_inputs():
    return {
        "user_id": 9,
        "frame_time": "2026-04-07T10:00:00Z",
        "current_lat": 40.75,
        "current_lng": -73.99,
        "current_zone_id": 1,
        "current_zone_name": "Zone One",
        "current_borough": "Manhattan",
        "mode_flags": {},
        "assistant_outlook_bucket": {},
        "now_ts": 1_800_000_000,
    }


def test_guidance_case_a_hold_for_strong_zone_recent_arrival():
    guidance = build_driver_guidance(
        **_base_guidance_inputs(),
        activity_snapshot={
            "tripless_minutes": 8,
            "stationary_minutes": 4,
            "movement_minutes": 7,
            "dispatch_uncertainty": 0.25,
            "recent_move_attempts_without_trip": 0,
            "recent_saved_trip_count_60m": 0,
            "moved_since_last_saved_trip": False,
            "guidance_state": {},
        },
        zone_context={
            "current_zone": {"rating": 68, "next_rating": 66, "continuation_raw": 0.58},
            "nearby_candidates": [{"zone_id": 2, "rating": 73, "distance_miles": 1.2}],
        },
    )
    assert guidance["action"] == "hold"


def test_guidance_case_b_move_nearby_for_material_edge():
    guidance = build_driver_guidance(
        **_base_guidance_inputs(),
        activity_snapshot={
            "tripless_minutes": 34,
            "stationary_minutes": 18,
            "movement_minutes": 0,
            "dispatch_uncertainty": 0.32,
            "recent_move_attempts_without_trip": 0,
            "recent_saved_trip_count_60m": 0,
            "moved_since_last_saved_trip": False,
            "guidance_state": {"last_guidance_action": "hold"},
        },
        zone_context={
            "current_zone": {"rating": 43, "next_rating": 47, "continuation_raw": 0.3},
            "nearby_candidates": [{"zone_id": 9, "rating": 58, "distance_miles": 1.1}],
        },
    )
    assert guidance["action"] == "move_nearby"


def test_guidance_case_c_micro_reposition_when_blue_zone_stationary_without_upgrade():
    # Micro-reposition still applies for a BLUE+ zone (>=60) that's gone quiet
    # with no clearly stronger neighbour — the blue-floor move rule only takes
    # over for sub-blue zones.
    guidance = build_driver_guidance(
        **_base_guidance_inputs(),
        activity_snapshot={
            "tripless_minutes": 30,
            "stationary_minutes": 22,
            "movement_minutes": 2,
            "dispatch_uncertainty": 0.31,
            "recent_move_attempts_without_trip": 1,
            "recent_saved_trip_count_60m": 0,
            "moved_since_last_saved_trip": False,
            "guidance_state": {},
        },
        zone_context={
            "current_zone": {"rating": 62, "next_rating": 61, "continuation_raw": 0.42},
            "nearby_candidates": [{"zone_id": 3, "rating": 64, "distance_miles": 1.6}],
        },
    )
    assert guidance["action"] == "micro_reposition"


def test_guidance_blue_floor_moves_below_blue_zone_to_blue_neighbor():
    # Sky blue (50-59) is not a sit zone: with a neighbour that will be blue+ on
    # arrival, the driver should be moved there even if the edge is modest.
    guidance = build_driver_guidance(
        **_base_guidance_inputs(),
        activity_snapshot={
            "tripless_minutes": 12,
            "stationary_minutes": 6,
            "movement_minutes": 4,
            "dispatch_uncertainty": 0.3,
            "recent_move_attempts_without_trip": 0,
            "recent_saved_trip_count_60m": 0,
            "moved_since_last_saved_trip": False,
            "guidance_state": {},
        },
        zone_context={
            "current_zone": {"rating": 54, "next_rating": 53, "continuation_raw": 0.42},
            "nearby_candidates": [{"zone_id": 3, "zone_name": "Blue Zone", "rating": 61, "distance_miles": 1.6}],
        },
    )
    assert guidance["action"] == "move_nearby"
    assert guidance["below_blue"] is True


def test_guidance_blue_floor_holds_when_zone_about_to_reach_blue():
    # Below blue but climbing to blue+ in the next bin with nothing stronger
    # reachable -> hold and tell the driver it's about to pick up.
    guidance = build_driver_guidance(
        **_base_guidance_inputs(),
        activity_snapshot={
            "tripless_minutes": 10,
            "stationary_minutes": 5,
            "movement_minutes": 3,
            "dispatch_uncertainty": 0.3,
            "recent_move_attempts_without_trip": 0,
            "recent_saved_trip_count_60m": 0,
            "moved_since_last_saved_trip": False,
            "guidance_state": {},
        },
        zone_context={
            "current_zone": {"rating": 54, "next_rating": 63, "continuation_raw": 0.42},
            "nearby_candidates": [{"zone_id": 3, "zone_name": "Weak Zone", "rating": 55, "distance_miles": 1.4}],
        },
    )
    assert guidance["action"] == "wait_dispatch"
    assert guidance["current_will_improve"] is True
    assert guidance["improvement_note"]


def test_guidance_case_d_wait_dispatch_after_recent_move_without_trip():
    guidance = build_driver_guidance(
        **_base_guidance_inputs(),
        activity_snapshot={
            "tripless_minutes": 42,
            "stationary_minutes": 9,
            "movement_minutes": 15,
            "dispatch_uncertainty": 0.63,
            "recent_move_attempts_without_trip": 2,
            "recent_saved_trip_count_60m": 0,
            "moved_since_last_saved_trip": True,
            "guidance_state": {},
        },
        zone_context={
            "current_zone": {"rating": 56, "next_rating": 54, "continuation_raw": 0.47},
            "nearby_candidates": [{"zone_id": 4, "rating": 62, "distance_miles": 1.0}],
        },
    )
    assert guidance["action"] in {"wait_dispatch", "hold"}


def test_guidance_outcome_settle_case_e_trip_after_move_marks_movement():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE assistant_guidance_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            recommended_at INTEGER NOT NULL,
            action TEXT NOT NULL,
            source_zone_id INTEGER,
            target_zone_id INTEGER,
            converted_to_trip INTEGER,
            moved_before_trip INTEGER,
            minutes_to_trip REAL,
            settled_at INTEGER,
            settlement_reason TEXT
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE driver_guidance_state (
            user_id INTEGER PRIMARY KEY,
            recent_move_attempts_without_trip INTEGER NOT NULL DEFAULT 0,
            recent_wait_dispatch_count INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        );
        """
    )
    cur.execute("INSERT INTO driver_guidance_state(user_id, updated_at) VALUES(7, 1700)")
    cur.execute(
        """
        INSERT INTO assistant_guidance_outcomes(user_id, recommended_at, action, source_zone_id, target_zone_id, converted_to_trip)
        VALUES(7, 1000, 'move_nearby', 10, 11, NULL)
        """
    )
    conn.commit()

    result = _settle_latest_assistant_guidance_outcome_tx(cur, 7, 11, 40.7, -73.9, 1300)
    assert result["settled"] is True
    row = cur.execute("SELECT moved_before_trip, settlement_reason FROM assistant_guidance_outcomes WHERE user_id=7").fetchone()
    assert row[0] == 1
    assert row[1] == "trip_after_move_target_zone_match"


def test_guidance_outcome_settle_case_f_hold_trip_without_move_gets_hold_credit():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE assistant_guidance_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            recommended_at INTEGER NOT NULL,
            action TEXT NOT NULL,
            source_zone_id INTEGER,
            target_zone_id INTEGER,
            converted_to_trip INTEGER,
            moved_before_trip INTEGER,
            minutes_to_trip REAL,
            settled_at INTEGER,
            settlement_reason TEXT
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE driver_guidance_state (
            user_id INTEGER PRIMARY KEY,
            recent_move_attempts_without_trip INTEGER NOT NULL DEFAULT 0,
            recent_wait_dispatch_count INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        );
        """
    )
    cur.execute("INSERT INTO driver_guidance_state(user_id, updated_at) VALUES(8, 1700)")
    cur.execute(
        """
        INSERT INTO assistant_guidance_outcomes(user_id, recommended_at, action, source_zone_id, target_zone_id, converted_to_trip)
        VALUES(8, 1000, 'hold', 20, NULL, NULL)
        """
    )
    conn.commit()

    result = _settle_latest_assistant_guidance_outcome_tx(cur, 8, 20, 40.7, -73.9, 1300)
    assert result["settled"] is True
    row = cur.execute("SELECT moved_before_trip, settlement_reason FROM assistant_guidance_outcomes WHERE user_id=8").fetchone()
    assert row[0] == 0
    assert row[1] == "trip_while_holding_zone"


def test_guidance_far_field_points_to_demand_when_local_is_dead():
    # Quiet everywhere within a few miles -> point the driver at the best strong
    # zone within a longer but worthwhile drive instead of just holding.
    guidance = build_driver_guidance(
        **_base_guidance_inputs(),
        activity_snapshot={
            "tripless_minutes": 20,
            "stationary_minutes": 10,
            "movement_minutes": 4,
            "dispatch_uncertainty": 0.3,
            "recent_move_attempts_without_trip": 0,
            "recent_saved_trip_count_60m": 0,
            "moved_since_last_saved_trip": False,
            "guidance_state": {},
        },
        zone_context={
            "current_zone": {"rating": 25, "next_rating": 25, "continuation_raw": 0.2},
            "nearby_candidates": [{"zone_id": 2, "zone_name": "Next Door", "rating": 28, "distance_miles": 1.5}],
            "far_candidates": [{"zone_id": 9, "zone_name": "Times Sq", "rating": 70, "rating_now": 70, "distance_miles": 5.0, "eta_minutes": 25}],
        },
    )
    assert guidance["action"] == "move_nearby"
    assert guidance["far_reposition"] is True
    assert (guidance["target_zone"] or {}).get("zone_name") == "Times Sq"


def test_guidance_far_field_not_triggered_for_a_weak_far_zone():
    # A far zone that isn't clearly better shouldn't earn a long deadhead.
    guidance = build_driver_guidance(
        **_base_guidance_inputs(),
        activity_snapshot={
            "tripless_minutes": 20, "stationary_minutes": 10, "movement_minutes": 4,
            "dispatch_uncertainty": 0.3, "recent_move_attempts_without_trip": 0,
            "recent_saved_trip_count_60m": 0, "moved_since_last_saved_trip": False,
            "guidance_state": {},
        },
        zone_context={
            "current_zone": {"rating": 47, "next_rating": 47, "continuation_raw": 0.2},
            "nearby_candidates": [{"zone_id": 2, "zone_name": "Next Door", "rating": 49, "distance_miles": 1.5}],
            "far_candidates": [{"zone_id": 9, "zone_name": "Sorta Better", "rating": 55, "rating_now": 55, "distance_miles": 6.0, "eta_minutes": 30}],
        },
    )
    assert guidance["far_reposition"] is False


def _elevated_inputs(frame_time):
    base = _base_guidance_inputs()
    base["frame_time"] = frame_time
    base["current_zone_id"] = 126  # Hunts Point — in SAFETY_ELEVATED_RISK_ZONE_IDS
    base["current_borough"] = "Bronx"
    return base


def _quiet_snapshot():
    return {
        "tripless_minutes": 15, "stationary_minutes": 8, "movement_minutes": 4,
        "dispatch_uncertainty": 0.3, "recent_move_attempts_without_trip": 0,
        "recent_saved_trip_count_60m": 0, "moved_since_last_saved_trip": False,
        "guidance_state": {},
    }


def test_safety_overnight_adds_phone_snatch_warning():
    g = build_driver_guidance(
        **_elevated_inputs("2026-04-07T02:00:00Z"),
        activity_snapshot=_quiet_snapshot(),
        zone_context={"current_zone": {"rating": 55, "next_rating": 55}, "nearby_candidates": []},
    )
    assert g["safety_elevated_risk"] is True
    assert g["safety_overnight"] is True
    assert "phone" in (g["safety_advice"] or "").lower()


def test_safety_daytime_has_no_phone_warning():
    g = build_driver_guidance(
        **_elevated_inputs("2026-04-07T14:00:00Z"),
        activity_snapshot=_quiet_snapshot(),
        zone_context={"current_zone": {"rating": 55, "next_rating": 55}, "nearby_candidates": []},
    )
    assert g["safety_elevated_risk"] is True
    assert g["safety_overnight"] is False
    assert "phone" not in (g["safety_advice"] or "").lower()
    assert "4.7" in (g["safety_advice"] or "")
