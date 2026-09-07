"""Paddle webhook processing.

The receiver had no tests. These cover the ordering guard, which decides whether
an event is applied at all -- the difference between a paid driver being marked
active and being silently skipped.
"""
from __future__ import annotations

import subscription_webhooks as wh


# --- ordering guard --------------------------------------------------------

def test_sibling_events_sharing_a_timestamp_are_all_applied():
    """Paddle emits several events per checkout with an identical occurred_at
    (transaction.completed and subscription.activated arrive together).

    The guard used >=, so whichever landed first stamped subscription_updated_at
    and every sibling was discarded as "older" -- a driver could pay and never be
    marked active.
    """
    occurred = 1_700_000_000
    assert wh._is_stale_event(occurred, occurred) is False


def test_genuinely_older_events_are_still_skipped():
    """Out-of-order redelivery must not roll state backwards."""
    assert wh._is_stale_event(1_700_000_500, 1_700_000_000) is True


def test_newer_events_are_applied():
    assert wh._is_stale_event(1_700_000_000, 1_700_000_500) is False


def test_first_event_for_a_user_is_applied():
    assert wh._is_stale_event(None, 1_700_000_000) is False


def test_unparseable_stored_timestamp_does_not_block_processing():
    """A corrupt column must fail open: skipping would strand the subscription."""
    assert wh._is_stale_event("garbage", 1_700_000_000) is False


# --- status mapping --------------------------------------------------------

def test_paddle_statuses_map_to_internal_values():
    assert wh._paddle_status_to_internal("active") == "active"
    assert wh._paddle_status_to_internal("trialing") == "trialing"
    assert wh._paddle_status_to_internal("past_due") == "past_due"
    assert wh._paddle_status_to_internal("paused") == "paused"


def test_both_cancel_spellings_normalise():
    assert wh._paddle_status_to_internal("canceled") == "cancelled"
    assert wh._paddle_status_to_internal("cancelled") == "cancelled"


def test_unknown_status_returns_none_so_the_column_is_left_alone():
    assert wh._paddle_status_to_internal("something_new") is None
    assert wh._paddle_status_to_internal("") is None


def test_mapped_statuses_are_all_understood_by_the_access_ladder():
    """Any status the webhook can write must mean something to the gate, or a
    driver lands in a state nothing grants or denies deliberately."""
    from subscription_state import PAID_WINDOW_STATUSES
    written = {wh._paddle_status_to_internal(s) for s in
               ("active", "trialing", "past_due", "paused", "canceled", "cancelled")}
    written.discard(None)
    known = PAID_WINDOW_STATUSES | {"paused"}
    assert written <= known, f"webhook can write statuses the ladder ignores: {written - known}"


# --- period extraction -----------------------------------------------------

def test_period_end_read_from_current_billing_period():
    assert wh._extract_period_end(
        {"current_billing_period": {"ends_at": "2026-01-01T00:00:00Z"}}
    ) == "2026-01-01T00:00:00Z"


def test_period_end_falls_back_to_next_billed_at():
    assert wh._extract_period_end({"next_billed_at": "2026-02-01T00:00:00Z"}) == "2026-02-01T00:00:00Z"


def test_missing_period_end_is_none():
    assert wh._extract_period_end({}) is None


def test_iso_timestamps_parse_including_zulu():
    assert wh._iso_to_unix("2026-01-01T00:00:00Z") == 1767225600
    assert wh._iso_to_unix("not-a-date") is None
    assert wh._iso_to_unix("") is None
