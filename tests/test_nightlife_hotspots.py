"""Tests for nightlife_hotspot_builder (dining + nightlife district pulse).

Pure/offline: the builder imports only json/math/time, so these never touch
main.py, the DB, the network, or the heavy geo deps — the clustering,
qualification, let-out schedules, read-time meta, and write path are all
exercised directly.
"""
import json

import nightlife_hotspot_builder as nb


# ---------------------------------------------------------------------------
# The curated list -> 8 districts
# ---------------------------------------------------------------------------
def test_builds_eight_districts_all_mixed():
    districts = nb.build_nightlife_districts()
    assert len(districts) == 8
    for d in districts:
        assert d["member_count"] >= nb.MIN_MEMBERS_PER_DISTRICT
        cats = {m["category"] for m in d["members"]}
        assert cats & nb._DINING, f"{d['label']} has no dining venue"
        assert cats & nb._NIGHTLIFE, f"{d['label']} has no nightlife venue"
        # The pin snaps to a real member venue, not a mid-street centroid.
        assert any(abs(m["lat"] - d["lat"]) < 1e-6 and abs(m["lng"] - d["lng"]) < 1e-6
                   for m in d["members"])


def test_every_district_pulses_at_dinner_letout():
    for d in nb.build_nightlife_districts():
        s = d["dim_schedule"]
        assert s["weekday_only"] is False          # nightlife never "closes"
        assert s["prime"] and s["prime_weekend"]
        # Prime starts in the dinner-let-out evening (8-9pm).
        assert s["prime"][0][0] in (20, 21)
        assert s["prime_weekend"][0][0] in (20, 21)


def test_club_districts_run_latest_on_weekends():
    by_label = {d["label"].split(" +")[0]: d for d in nb.build_nightlife_districts()}
    # Meatpacking (Le Bain) and Williamsburg (Brooklyn Bowl) have nightclubs.
    for anchor in ("Pastis", "Le Crocodile"):
        sched = by_label[anchor]["dim_schedule"]
        assert sched["prime_weekend"][0][1] == 4      # open till 4am Fri/Sat
    # SoHo (Balthazar) is cocktail-tier -> winds down earlier.
    assert by_label["Balthazar"]["dim_schedule"]["prime_weekend"][0][1] == 2


# ---------------------------------------------------------------------------
# Clustering + the "dining AND nightlife, 3+" qualification rule
# ---------------------------------------------------------------------------
def _run_with_pois(pois):
    saved = nb.NIGHTLIFE_POIS
    try:
        nb.NIGHTLIFE_POIS = pois
        return nb.build_nightlife_districts()
    finally:
        nb.NIGHTLIFE_POIS = saved


def test_qualification_rejects_pure_and_undersized_clusters():
    R, B = "upscale_restaurant", "cocktail_bar"
    pois = [
        # pure-dining trio (no nightlife) -> dropped
        ("R1", 40.800, -73.950, R, 2.0, "a"),
        ("R2", 40.801, -73.951, R, 2.0, "b"),
        ("R3", 40.799, -73.949, R, 2.0, "c"),
        # pure-nightlife trio (no dining) -> dropped
        ("B1", 40.600, -73.950, B, 2.0, "d"),
        ("B2", 40.601, -73.951, B, 2.0, "e"),
        ("B3", 40.599, -73.949, B, 2.0, "f"),
        # mixed but only 2 (undersized) -> dropped
        ("R4", 40.850, -73.800, R, 2.0, "g"),
        ("B4", 40.851, -73.801, B, 2.0, "h"),
        # valid: 1 dining + 2 nightlife, tightly grouped -> kept
        ("R5", 40.700, -73.900, R, 2.0, "i"),
        ("B5", 40.701, -73.901, B, 2.0, "j"),
        ("B6", 40.699, -73.899, B, 2.0, "k"),
    ]
    districts = _run_with_pois(pois)
    assert len(districts) == 1
    d = districts[0]
    assert d["member_count"] == 3
    assert {m["name"] for m in d["members"]} == {"R5", "B5", "B6"}


def test_far_apart_venues_do_not_cluster():
    R, B = "upscale_restaurant", "cocktail_bar"
    # 1 dining + 2 bars, but the dining venue is ~1 mile away -> the trio
    # never forms a 3-member cluster, so nothing qualifies.
    pois = [
        ("Far Restaurant", 40.730, -73.990, R, 2.0, "a"),
        ("Bar A",          40.700, -73.990, B, 2.0, "b"),
        ("Bar B",          40.7005, -73.9905, B, 2.0, "c"),
    ]
    assert _run_with_pois(pois) == []


def test_members_within_cluster_radius():
    # Data + clustering invariant: complete-link => every pair within radius.
    for d in nb.build_nightlife_districts():
        ms = d["members"]
        for i in range(len(ms)):
            for j in range(i + 1, len(ms)):
                dist = nb.haversine_miles(ms[i]["lat"], ms[i]["lng"],
                                          ms[j]["lat"], ms[j]["lng"])
                assert dist <= nb.CLUSTER_RADIUS_MI + 1e-9, (
                    f"{d['label']}: {ms[i]['name']}<->{ms[j]['name']} = {dist:.3f}mi")


def test_all_coordinates_are_in_nyc():
    for name, lat, lng, cat, w, addr in nb.NIGHTLIFE_POIS:
        assert 40.5 <= lat <= 40.95, f"{name} lat {lat} out of NYC range"
        assert -74.10 <= lng <= -73.70, f"{name} lng {lng} out of NYC range"
        assert cat in (nb._DINING | nb._NIGHTLIFE), f"{name} bad category {cat}"


# ---------------------------------------------------------------------------
# Read-time meta + write path
# ---------------------------------------------------------------------------
def test_runtime_meta_recomputes_schedule_from_members():
    members = [
        {"name": "Some Restaurant", "category": "upscale_restaurant"},
        {"name": "Some Club", "category": "nightclub"},
    ]
    meta = nb.district_runtime_meta(members)
    assert meta["dim_schedule"]["prime"] == [[21, 2]]
    assert meta["dim_schedule"]["prime_weekend"] == [[21, 4]]   # club -> 4am
    assert "last call" in meta["best_hours"]
    assert meta["category_counts"] == {"upscale_restaurant": 1, "nightclub": 1}
    assert "1 upscale restaurant" in meta["rationale"]


def test_write_is_full_replace_with_valid_rows():
    calls = []
    nb.write_nightlife_districts(lambda *a: calls.append(a))
    # First statement clears the table, then one INSERT per district.
    assert "DELETE FROM nightlife_districts" in calls[0][0]
    inserts = calls[1:]
    assert len(inserts) == 8
    for sql, params in inserts:
        assert "INSERT INTO nightlife_districts" in sql
        assert len(params) == 9                       # 9 columns
        members = json.loads(params[7])               # members_json
        assert isinstance(members, list) and members
        for m in members:
            assert {"name", "category", "weight", "lat", "lng", "address"} <= set(m)
