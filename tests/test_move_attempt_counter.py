"""The anti-churn move counter must count NEW redirections, not re-shown plans.

Regression for the move->move->"sit tight, you've moved a lot" confusion: the
counter (`recent_move_attempts_without_trip`) used to step up on every move
RECOMMENDATION, so reaffirming a single "go to X" across polls inflated it past
the anti-churn threshold and flipped the card to a hold while still pointing at X.
"""
from __future__ import annotations

from main import _next_move_attempts_without_trip as nxt


def test_first_move_counts_as_one_attempt():
    assert nxt(0, "move_nearby", 11, None, "hold") == 1


def test_reaffirming_the_same_target_does_not_inflate():
    # "go to zone 11" re-shown across several polls while en route stays at 1.
    n = nxt(0, "move_nearby", 11, None, "hold")
    for _ in range(5):
        n = nxt(n, "move_nearby", 11, 11, "move_nearby")
    assert n == 1


def test_a_genuine_redirect_to_a_new_zone_increments():
    # move to 11, then redirected to 22 -> that's a real second attempt.
    n = nxt(1, "move_nearby", 22, 11, "move_nearby")
    assert n == 2


def test_hold_steps_the_counter_back_down_and_floors_at_zero():
    assert nxt(2, "wait_dispatch", None, 22, "move_nearby") == 1
    assert nxt(0, "hold", None, None, "hold") == 0


def test_micro_reposition_counts_once_then_holds_steady():
    first = nxt(0, "micro_reposition", None, None, "hold")
    assert first == 1
    # repeated micro-repositions in place don't keep inflating
    assert nxt(first, "micro_reposition", None, None, "micro_reposition") == 1


def test_same_zone_after_a_hold_is_a_new_attempt():
    # move to 11, a hold clears the last target, then move to 11 again -> the
    # hold broke the en-route streak, so re-issuing the move is a new attempt.
    assert nxt(0, "move_nearby", 11, None, "hold") == 1
