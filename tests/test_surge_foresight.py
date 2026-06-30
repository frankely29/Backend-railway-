"""Surge foresight must price the bridge/water-crossing detour like the router.

Without it, a surge across the East River reads at its straight-line distance, so
the ETA and the "leave around X:XX" time are too optimistic — the driver gets
sent off too late to make the peak. The detour must fold into the distance, same
as the main move router.
"""
from __future__ import annotations

from main import _find_upcoming_surge


def _pt(rating):
    return {"tracks": {"citywide_v3_shadow": {"rating": rating, "bucket": "blue"}}}


def _surge_zone(zid, name, borough):
    # now 60 -> +20 70 -> +40 80/82 (peak at bin 2, 40 min ahead)
    return {"location_id": zid, "zone_name": name, "borough": borough,
            "points": [_pt(60), _pt(70), _pt(82 if borough == "Brooklyn" else 80)]}


def test_surge_across_the_river_is_priced_with_the_bridge_detour():
    # Two surge zones ~4mi straight-line from a Manhattan driver: one in Manhattan
    # (no crossing) and a HOTTER one in Brooklyn (East River +2.5mi detour). The
    # straight-line 4mi is within range, but +2.5 puts the Brooklyn zone past the
    # 6mi reach — so even though it peaks higher, it must NOT be chosen; the
    # reachable Manhattan surge wins.
    frame_bucket = {
        "201": _surge_zone(201, "Manhattan Surge", "Manhattan"),
        "202": _surge_zone(202, "Brooklyn Surge", "Brooklyn"),
    }
    centroid_lookup = {
        201: {"centroid_lat": 40.75 + 0.058, "centroid_lng": -73.99},  # ~4mi N, same borough
        202: {"centroid_lat": 40.75 - 0.058, "centroid_lng": -73.99},  # ~4mi S, across the river
    }
    best = _find_upcoming_surge(
        frame_bucket=frame_bucket, current_lat=40.75, current_lng=-73.99,
        centroid_lookup=centroid_lookup, frame_key="2025-06-21T22:00:00",
        mode_flags={}, current_zone_id=None, current_rating=50.0,
        current_borough="Manhattan",
    )
    assert best is not None
    assert best["zone_name"] == "Manhattan Surge"


def test_surge_picks_the_closer_one_when_a_hotter_surge_is_much_farther():
    # A close surge (peak 78, ~1mi) vs a hotter one a long deadhead away (peak 82,
    # ~5mi). Net of the unpaid drive the close one is the better pre-position — the
    # foresight must rank by peak MINUS deadhead, like the router, not raw peak.
    def z(zid, name, peak):
        return {"location_id": zid, "zone_name": name, "borough": "Manhattan",
                "points": [_pt(60), _pt(70), _pt(peak)]}
    frame_bucket = {"301": z(301, "Close Surge", 78), "302": z(302, "Far Hot Surge", 82)}
    centroid_lookup = {
        301: {"centroid_lat": 40.75 + 0.0145, "centroid_lng": -73.99},  # ~1mi
        302: {"centroid_lat": 40.75 + 0.0725, "centroid_lng": -73.99},  # ~5mi
    }
    best = _find_upcoming_surge(
        frame_bucket=frame_bucket, current_lat=40.75, current_lng=-73.99,
        centroid_lookup=centroid_lookup, frame_key="2025-06-21T22:00:00",
        mode_flags={}, current_zone_id=None, current_rating=50.0, current_borough="Manhattan",
    )
    assert best is not None
    assert best["zone_name"] == "Close Surge"


def test_same_borough_surge_is_still_found():
    # Control: a single reachable same-borough surge is returned with a sane ETA.
    frame_bucket = {"201": _surge_zone(201, "Manhattan Surge", "Manhattan")}
    centroid_lookup = {201: {"centroid_lat": 40.75 + 0.03, "centroid_lng": -73.99}}
    best = _find_upcoming_surge(
        frame_bucket=frame_bucket, current_lat=40.75, current_lng=-73.99,
        centroid_lookup=centroid_lookup, frame_key="2025-06-21T22:00:00",
        mode_flags={}, current_zone_id=None, current_rating=50.0,
        current_borough="Manhattan",
    )
    assert best is not None
    assert best["zone_name"] == "Manhattan Surge"
    assert best["eta_minutes"] > 0
