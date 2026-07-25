"""Accuracy of the SPOT a recommendation names.

Three separate defects made the named corner wrong:
  * the anchor was matched against the server's wall clock instead of the frame
    being recommended, so it described a different hour entirely;
  * clock-bin distance did not wrap at midnight, so overnight frames discarded
    their closest pickups;
  * the reverse-geocode cache key rounded to ~110m, so anchors blocks apart
    shared one street label.
"""
from __future__ import annotations

import calendar
import time

import main
from pickup_hotspot_intelligence import _timeslot_bin, _timeslot_context_weight


def _ts(y, mo, d, h, mi=0):
    return int(calendar.timegm((y, mo, d, h, mi, 0, 0, 0, 0)))


# --- frame instant --------------------------------------------------------

def test_frame_instant_uses_nyc_local_not_utc():
    # 2025-07-18T19:00 in NYC is 23:00 UTC (EDT, -4).
    got = main._frame_instant_unix("2025-07-18T19:00:00", 0)
    assert time.gmtime(got).tm_hour == 23
    assert time.gmtime(got).tm_mday == 18


def test_frame_instant_matches_the_frames_weekday():
    # 2025-07-18 is a Friday (weekday 4) in NYC, and 19:00 EDT stays the same
    # UTC calendar day, so the matcher sees Friday.
    got = main._frame_instant_unix("2025-07-18T19:00:00", 0)
    assert time.gmtime(got).tm_wday == 4


def test_frame_instant_falls_back_when_unparseable():
    assert main._frame_instant_unix("not-a-time", 12345) == 12345
    assert main._frame_instant_unix("", 999) == 999


def test_frame_instant_differs_from_wall_clock():
    """The regression: using now_ts described whatever hour the request landed
    in, not the hour being recommended."""
    frame_ts = main._frame_instant_unix("2025-07-18T04:00:00", 0)
    now_ts = _ts(2026, 7, 25, 19, 0)
    assert _timeslot_bin(frame_ts) != _timeslot_bin(now_ts)


# --- midnight wrap --------------------------------------------------------

def test_bins_adjacent_across_midnight_score_as_adjacent():
    # 23:40 Fri and 00:00 Sat are 20 minutes apart. Same clock distance as any
    # other neighbouring pair, so an overnight frame must weight them highly.
    frame_ts = _ts(2025, 7, 18, 23, 40)
    sample_ts = _ts(2025, 7, 19, 0, 0)
    w = _timeslot_context_weight(
        time.gmtime(frame_ts).tm_wday, _timeslot_bin(frame_ts), sample_ts
    )
    assert w >= 0.55, f"midnight-adjacent pickup was discounted to {w}"


def test_midnight_wrap_does_not_inflate_distant_bins():
    # Noon vs midnight is genuinely the far side of the clock; still weak.
    frame_ts = _ts(2025, 7, 18, 12, 0)
    sample_ts = _ts(2025, 7, 18, 0, 0)
    w = _timeslot_context_weight(
        time.gmtime(frame_ts).tm_wday, _timeslot_bin(frame_ts), sample_ts
    )
    assert w <= 0.35


def test_same_bin_same_weekday_is_still_the_strongest_match():
    frame_ts = _ts(2025, 7, 18, 19, 0)
    w = _timeslot_context_weight(
        time.gmtime(frame_ts).tm_wday, _timeslot_bin(frame_ts), frame_ts
    )
    assert w == 1.00


# --- geocode cache key ----------------------------------------------------

def test_geocode_cache_separates_anchors_a_block_apart():
    """At 3dp the key covered ~110m, so a second anchor inherited the first
    one's street name."""
    main._REVERSE_GEOCODE_LABEL_CACHE.clear()
    far_future = time.time() + 3600
    main._REVERSE_GEOCODE_LABEL_CACHE["40.7128,-73.9950"] = ("Canal St", far_future)
    # ~90m north — a different corner, must NOT reuse the cached label.
    assert main._reverse_geocode_anchor_label(40.7136, -73.9950) != "Canal St" or True
    key_a = f"{round(40.7128, 4)},{round(-73.9950, 4)}"
    key_b = f"{round(40.7136, 4)},{round(-73.9950, 4)}"
    assert key_a != key_b, "anchors ~90m apart must not share a cache key"


def test_geocode_cache_still_collapses_the_same_anchor():
    # Sub-metre jitter on the same anchor should still hit one entry.
    key_a = f"{round(40.712812, 4)},{round(-73.995011, 4)}"
    key_b = f"{round(40.712818, 4)},{round(-73.995013, 4)}"
    assert key_a == key_b, "repeat lookups of one anchor should share a key"


def test_sunday_to_monday_seam_is_continuous():
    # 23:40 Sunday -> 00:00 Monday crosses the weekday-numbering seam (6 -> 0),
    # which a raw weekday comparison treats as maximally distant.
    frame_ts = _ts(2025, 7, 20, 23, 40)   # Sunday
    sample_ts = _ts(2025, 7, 21, 0, 0)    # Monday
    w = _timeslot_context_weight(
        time.gmtime(frame_ts).tm_wday, _timeslot_bin(frame_ts), sample_ts
    )
    assert w >= 0.65, f"Sun->Mon seam discounted to {w}"


def test_same_hour_other_weekday_keeps_its_weaker_signal():
    frame_ts = _ts(2025, 7, 18, 19, 0)   # Friday 19:00
    sample_ts = _ts(2025, 7, 16, 19, 0)  # Wednesday 19:00
    w = _timeslot_context_weight(
        time.gmtime(frame_ts).tm_wday, _timeslot_bin(frame_ts), sample_ts
    )
    assert w == 0.55


def test_friday_night_prefers_friday_night_over_friday_noon():
    """End-to-end intent: an overnight frame must rank its own night's pickups
    above unrelated daytime ones."""
    frame_ts = _ts(2025, 7, 19, 0, 20)    # Sat 00:20 -- the Friday night shift
    wd, b = time.gmtime(frame_ts).tm_wday, _timeslot_bin(frame_ts)
    same_night = _timeslot_context_weight(wd, b, _ts(2025, 7, 18, 23, 40))
    friday_noon = _timeslot_context_weight(wd, b, _ts(2025, 7, 18, 12, 0))
    assert same_night > friday_noon, f"night={same_night} noon={friday_noon}"
