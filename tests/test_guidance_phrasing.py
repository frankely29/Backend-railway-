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


def test_move_leads_with_busy_descriptor_then_the_zone_name():
    # The most important info — HOW BUSY the destination is — must be the FIRST
    # word, with the zone name right after. Driver glances at the card and
    # immediately sees "Much busier — go to Clinton East" instead of having to
    # parse "Go to Clinton East — much busier than here" backwards.
    line = compose_guidance_directive(
        action="move_nearby", moving=True,
        current_zone_name="East Chelsea", current_rating=57, current_next_rating=56,
        target_zone_name="Clinton East", target_rating=69, target_rating_now=66, target_eta=7,
        spot=_rail("50th Street"), below_blue=True,
    )
    assert line.startswith("Much busier") or line.startswith("Busier")
    assert "go to Clinton East" in line
    assert "50th Street stop" in line and "7 min" in line


def test_move_to_very_busy_leads_with_the_demand_word():
    # Same-bucket move with the destination already busy on arrival — lead with
    # the actual demand word (Very busy / Red-hot) before the zone name.
    line = compose_guidance_directive(
        action="move_nearby", moving=True,
        current_zone_name="Williamsburg", current_rating=70, current_next_rating=70,
        target_zone_name="Greenpoint", target_rating=72, target_rating_now=72, target_eta=5,
        spot=_rail("Greenpoint Avenue"), below_blue=False,
    )
    assert line.startswith("Very busy — go to Greenpoint")


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


def test_stay_trend_reflects_the_rest_of_the_hour_not_just_plus20():
    # Red-hot now (80), still 80 at +20 (so the +20 trend reads "steady"), but it
    # eases over the rest of the hour (avg of +40/+60 ~= 72). The line must say
    # "slowing down", not "steady" — the driver sees the hour, not one bin.
    line = compose_guidance_directive(
        action="hold", moving=False, current_zone_name="Times Sq",
        current_rating=80, current_next_rating=80, spot=None,
        below_blue=False, current_hour_trend_rating=72.0,
    )
    assert "slowing down" in line
    # Control: when the hour holds (still ~80), it stays "steady".
    steady = compose_guidance_directive(
        action="hold", moving=False, current_zone_name="Times Sq",
        current_rating=80, current_next_rating=80, spot=None,
        below_blue=False, current_hour_trend_rating=80.0,
    )
    assert "steady" in steady


def test_antichurn_hold_acknowledges_the_busier_zones_and_explains():
    # IMG_4170: a churned driver in Bay Ridge sees busier zones nearby yet the
    # card said "sit tight ... you've moved a lot without a trip, so let dispatch
    # work" — reading as if it ignored the busier zones. The reworded line names
    # the tactic (chasing hasn't landed a fare) and doesn't lecture about moving.
    line = compose_guidance_directive(
        action="wait_dispatch", moving=False, current_zone_name="Bay Ridge",
        current_rating=46, current_next_rating=46, spot=None,
        below_blue=True, held_for_antichurn=True,
    )
    assert line.startswith("Sit tight in Bay Ridge")
    assert "busier zones" in line and "let a dispatch come" in line
    assert "you've moved a lot" not in line


def test_below_blue_hold_names_a_visibly_busier_zone_instead_of_lying():
    # We're holding sub-blue but a zone the driver can SEE is busier sits nearby
    # (the move just isn't worth the deadhead yet). The line must name it honestly,
    # never claim "nothing nearby beats it" while a redder zone is on the map.
    line = compose_guidance_directive(
        action="wait_dispatch", moving=False, current_zone_name="Bay Ridge",
        current_rating=48, current_next_rating=48, spot=None,
        below_blue=True, busier_zone_name="Sunset Park West",
    )
    assert "Sunset Park West" in line and "not worth the drive yet" in line
    assert "nothing nearby" not in line


def test_below_blue_hold_keeps_plain_line_when_nothing_is_busier():
    line = compose_guidance_directive(
        action="hold", moving=False, current_zone_name="Quiet Outer Zone",
        current_rating=40, current_next_rating=40, spot=None,
        below_blue=True, busier_zone_name=None,
    )
    assert "nothing nearby is better" in line


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


def test_far_reposition_leads_with_destination_demand_then_zone():
    # Far-field still leads with the busy-ness (so the driver sees the payoff
    # first), then names the zone, and explains the local area is slow.
    line = compose_guidance_directive(
        action="move_nearby", moving=True,
        current_zone_name="Great Kills", current_rating=22, current_next_rating=22,
        target_zone_name="Times Sq", target_rating=72, target_rating_now=72, target_eta=28,
        spot=_rail("Times Square–42nd Street"), below_blue=True, far_reposition=True,
    )
    assert line.startswith("Very busy over in Times Sq")
    assert "slow all around here" in line
    assert "28 min away" in line
