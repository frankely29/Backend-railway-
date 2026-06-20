from __future__ import annotations

from guidance_phrasing import compose_guidance_directive, spot_phrase, bucket_word


def _rail(label):
    return {"label": label, "kind": "rail", "source": "magnet"}


def test_bucket_words_match_map_thresholds():
    assert bucket_word(85) == "green"
    assert bucket_word(62) == "blue"
    assert bucket_word(55) == "sky blue"
    assert bucket_word(20) == "red"


def test_spot_phrase_is_natural_by_source_and_kind():
    assert spot_phrase(_rail("Broadway")) == "the Broadway stop"
    # already ends in station/terminal -> not double-suffixed
    assert spot_phrase(_rail("Navy Yard Ferry Station")) == "Navy Yard Ferry Station"
    assert spot_phrase({"label": "Skillman Ave", "source": "pickup"}).startswith("the pickup cluster")
    assert spot_phrase({"label": "Boro Hotel LIC +3", "address": "29-05 40 Ave", "source": "curated"}) == "Boro Hotel LIC (29-05 40 Ave)"
    assert spot_phrase({"label": "Kings County Hospital", "kind": "hospital", "source": "magnet"}) == "Kings County Hospital"


def test_move_contrasts_colors_and_names_target_spot():
    line = compose_guidance_directive(
        action="move_nearby", moving=True,
        current_zone_name="East Chelsea", current_rating=57, current_next_rating=56,
        target_zone_name="Clinton East", target_rating=69, target_rating_now=66, target_eta=7,
        spot=_rail("50th Street"), below_blue=True,
    )
    assert "Clinton East" in line and "indigo" in line and "sky blue" in line
    assert "50th Street stop" in line and "7 min" in line


def test_stay_blue_steady_reads_like_a_dispatcher_not_a_template():
    line = compose_guidance_directive(
        action="hold", moving=False,
        current_zone_name="Astoria", current_rating=62, current_next_rating=61,
        spot=_rail("Broadway"), below_blue=False,
    )
    assert line == "Stay in Astoria — blue and steady. Work the Broadway stop."
    # the old robotic suffixes must be gone
    for banned in ("the local transit hub", "a visitor draw", "it's working", "best anchor here"):
        assert banned not in line


def test_stay_climbing_and_cooling_differ():
    climbing = compose_guidance_directive(
        action="hold", moving=False, current_zone_name="Midtown",
        current_rating=66, current_next_rating=73, spot=_rail("42 St"), below_blue=False,
    )
    cooling = compose_guidance_directive(
        action="wait_dispatch", moving=False, current_zone_name="FiDi",
        current_rating=65, current_next_rating=57, spot=_rail("Wall St"), below_blue=False,
    )
    assert "building" in climbing
    assert "easing" in cooling


def test_below_blue_improving_says_it_builds():
    line = compose_guidance_directive(
        action="wait_dispatch", moving=False, current_zone_name="Garment District",
        current_rating=59, current_next_rating=63, spot=_rail("Times Square–42nd Street"),
        below_blue=True, current_will_improve=True,
    )
    assert "Sit tight" in line and "builds" in line
