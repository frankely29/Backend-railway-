from __future__ import annotations

from guidance_phrasing import compose_guidance_directive, spot_phrase, demand_word


def _rail(label):
    return {"label": label, "kind": "rail", "source": "magnet"}


def test_demand_words_are_plain():
    assert demand_word(85) == "red-hot"
    assert demand_word(70) == "very busy"
    assert demand_word(62) == "busy"
    assert demand_word(55) == "lukewarm"
    assert demand_word(20) == "quiet"


def test_spot_phrase_is_natural_by_source_and_kind():
    assert spot_phrase(_rail("Broadway")) == "the Broadway stop"
    assert spot_phrase(_rail("Navy Yard Ferry Station")) == "Navy Yard Ferry Station"
    assert spot_phrase(_rail("Moynihan Train Hall")) == "Moynihan Train Hall"
    assert spot_phrase({"label": "Skillman Ave", "source": "pickup"}) == "the pickup cluster at Skillman Ave"
    assert spot_phrase({"label": "Boro Hotel LIC +3", "address": "29-05 40 Ave", "source": "curated"}) == "Boro Hotel LIC (29-05 40 Ave)"
    assert spot_phrase({"label": "Kings County Hospital", "kind": "hospital", "source": "magnet"}) == "Kings County Hospital"


def test_move_is_a_plain_go_with_payoff_and_eta():
    line = compose_guidance_directive(
        action="move_nearby", moving=True,
        current_zone_name="East Chelsea", current_rating=57, current_next_rating=56,
        target_zone_name="Clinton East", target_rating=69, target_rating_now=66, target_eta=7,
        spot=_rail("50th Street"), below_blue=True,
    )
    assert line.startswith("Go to Clinton East")
    assert "busier than here" in line
    assert "50th Street stop" in line and "7 min" in line


def test_stay_is_plain_and_drops_old_jargon():
    line = compose_guidance_directive(
        action="hold", moving=False,
        current_zone_name="Astoria", current_rating=62, current_next_rating=61,
        spot=_rail("Broadway"), below_blue=False,
    )
    assert line == "Stay in Astoria — busy and steady. Work the Broadway stop."
    for banned in ("the local transit hub", "a visitor draw", "it's working",
                   "best anchor here", "blue but easing", "indigo"):
        assert banned not in line


def test_stay_climbing_and_cooling_differ_in_plain_words():
    climbing = compose_guidance_directive(
        action="hold", moving=False, current_zone_name="Midtown",
        current_rating=66, current_next_rating=73, spot=_rail("42 St"), below_blue=False,
    )
    cooling = compose_guidance_directive(
        action="wait_dispatch", moving=False, current_zone_name="FiDi",
        current_rating=65, current_next_rating=57, spot=_rail("Wall St"), below_blue=False,
    )
    assert "getting busier" in climbing
    assert "slowing down" in cooling


def test_below_blue_improving_says_about_to_pick_up():
    line = compose_guidance_directive(
        action="wait_dispatch", moving=False, current_zone_name="Garment District",
        current_rating=59, current_next_rating=63, spot=_rail("Times Square–42nd Street"),
        below_blue=True, current_will_improve=True,
    )
    assert line.startswith("Stay in Garment District")
    assert "about to pick up" in line


def test_spot_phrase_tags_the_zone_when_asked():
    # A bare street is useless to a driver who navigates by zones — tag it.
    assert spot_phrase({"label": "72nd Street", "source": "pickup"}, zone_name="Bay Ridge") == \
        "the pickup cluster at 72nd Street in Bay Ridge"
    assert spot_phrase(_rail("Broadway"), zone_name="Astoria") == "the Broadway stop in Astoria"
    # No double-tag when the label already names the zone.
    assert spot_phrase({"label": "Bay Ridge Pier", "source": "pickup"}, zone_name="Bay Ridge") == \
        "the pickup cluster at Bay Ridge Pier"


def test_below_blue_hold_names_the_spots_zone_but_move_does_not():
    # Below-blue STAY can get a surge sentence appended (a 2nd zone), so the
    # current-zone spot must say its zone to stay unambiguous.
    hold = compose_guidance_directive(
        action="wait_dispatch", moving=False, current_zone_name="Bay Ridge",
        current_rating=54, current_next_rating=54,
        spot={"label": "72nd Street", "source": "pickup"}, below_blue=True,
    )
    assert "72nd Street in Bay Ridge" in hold
    # A move names the target zone right before the spot and carries no 2nd zone,
    # so it doesn't repeat the zone on the spot.
    move = compose_guidance_directive(
        action="move_nearby", moving=True, current_zone_name="Bay Ridge",
        target_zone_name="Sunset Park West", current_rating=54, current_next_rating=54,
        target_rating=64, target_rating_now=64, target_eta=6,
        spot={"label": "Industry City", "source": "pickup"}, below_blue=True,
    )
    assert "in Sunset Park West" not in move and "Industry City" in move


def test_far_reposition_says_its_slow_and_points_to_demand():
    line = compose_guidance_directive(
        action="move_nearby", moving=True,
        current_zone_name="Great Kills", current_rating=22, current_next_rating=22,
        target_zone_name="Times Sq", target_rating=72, target_rating_now=72, target_eta=28,
        spot=_rail("Times Square–42nd Street"), below_blue=True, far_reposition=True,
    )
    assert line.startswith("It's slow all around here")
    assert "Times Sq" in line and "28 min away" in line
