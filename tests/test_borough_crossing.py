"""Bridge-detour penalty for water crossings.

Zones across the East River sit face-to-face, so their straight-line edge
distance is tiny — but the drive must detour to a bridge or tunnel. Without the
penalty a Manhattan zone reads as ~0.8mi from a Brooklyn-waterfront driver and
the brain sends them across the harbor instead of to a closer same-side zone
(IMG_4155). These lock in the detour miles so ETA/worth-the-move stay honest.
"""
from __future__ import annotations

import main


def test_water_crossings_add_detour_miles():
    f = main._borough_crossing_penalty_miles
    # Major East River / harbor crossings.
    assert f("Brooklyn", "Manhattan") == 2.5
    assert f("Manhattan", "Brooklyn") == 2.5  # symmetric
    assert f("Queens", "Manhattan") == 2.5
    assert f("Queens", "Bronx") == 2.5
    assert f("Brooklyn", "Staten Island") == 3.0
    # Harlem River — short bridges, smaller detour.
    assert f("Manhattan", "Bronx") == 1.0


def test_land_borders_and_same_borough_have_no_penalty():
    f = main._borough_crossing_penalty_miles
    assert f("Brooklyn", "Queens") == 0.0   # share a land border
    assert f("Brooklyn", "Brooklyn") == 0.0
    assert f("Manhattan", "Manhattan") == 0.0
    assert f(None, "Manhattan") == 0.0       # unknown -> no penalty
    assert f("Brooklyn", "") == 0.0
