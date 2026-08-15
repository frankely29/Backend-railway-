"""Subscription access ladder.

The feature that decides whether a paying driver can use the app had no test
coverage at all. These tests pin the rules that matter, and each one names the
failure it prevents.

Access model: a driver keeps access for the window they have PAID FOR. That
window is `subscription_current_period_end`, and the status tells us whether the
window is legitimate -- not whether it has elapsed.
"""
from __future__ import annotations

import time

import pytest
from fastapi import HTTPException

import core
import subscription_state


class Row(dict):
    """Stand-in for sqlite3.Row (indexable + .keys())."""

    def __getitem__(self, key):
        if key not in self:
            raise KeyError(key)
        return dict.__getitem__(self, key)


def _user(**over):
    base = {
        "id": 1, "is_admin": 0, "email": "d@example.com",
        "trial_expires_at": None,
        "subscription_status": None, "subscription_provider": None,
        "subscription_customer_id": None, "subscription_id": None,
        "subscription_current_period_end": None,
        "subscription_comp_reason": None, "subscription_comp_granted_by": None,
        "subscription_comp_granted_at": None, "subscription_comp_expires_at": None,
        "subscription_updated_at": None,
    }
    base.update(over)
    return Row(base)


FUTURE = lambda: int(time.time()) + 7 * 86400
PAST = lambda: int(time.time()) - 7 * 86400


@pytest.fixture(autouse=True)
def _enforce(monkeypatch):
    # subscription_state does `from core import ENFORCE_TRIAL`, binding its own
    # copy, so both names have to be patched or the two disagree.
    monkeypatch.setattr(core, "ENFORCE_TRIAL", True)
    monkeypatch.setattr(subscription_state, "ENFORCE_TRIAL", True)


def _server_allows(user) -> bool:
    try:
        core._enforce_access_or_admin(user)
        return True
    except HTTPException:
        return False


# --- a Paddle trial is a real subscription --------------------------------

def test_paddle_trialing_subscriber_has_access():
    """Signing up for a plan that starts with a trial must not lock you out.

    Paddle reports these as status 'trialing'. The ladder only accepted
    active/past_due, so a driver who subscribed -- and whose local signup trial
    had already lapsed -- paid and was met with 402.
    """
    u = _user(subscription_status="trialing", subscription_current_period_end=FUTURE())
    assert _server_allows(u) is True
    assert subscription_state.is_subscription_active(u) is True
    assert subscription_state.has_access(u) is True


def test_expired_paddle_trial_loses_access():
    u = _user(subscription_status="trialing", subscription_current_period_end=PAST())
    assert _server_allows(u) is False


# --- cancelling keeps the window you already paid for ----------------------

def test_cancelled_keeps_access_until_the_paid_period_ends():
    """Cancelling mid-cycle must not void the time already bought."""
    u = _user(subscription_status="cancelled", subscription_current_period_end=FUTURE())
    assert _server_allows(u) is True
    assert subscription_state.has_access(u) is True


def test_cancelled_loses_access_once_the_period_ends():
    u = _user(subscription_status="cancelled", subscription_current_period_end=PAST())
    assert _server_allows(u) is False


def test_paused_does_not_grant_access():
    """Paused billing is not a paid window, even with a stale future period."""
    u = _user(subscription_status="paused", subscription_current_period_end=FUTURE())
    assert _server_allows(u) is False


# --- active/past_due baseline ---------------------------------------------

def test_active_within_period_has_access():
    assert _server_allows(_user(subscription_status="active",
                                subscription_current_period_end=FUTURE())) is True


def test_past_due_within_period_keeps_access():
    """Dunning grace: a failed charge must not cut a driver off mid-shift."""
    assert _server_allows(_user(subscription_status="past_due",
                                subscription_current_period_end=FUTURE())) is True


def test_active_but_expired_period_loses_access():
    assert _server_allows(_user(subscription_status="active",
                                subscription_current_period_end=PAST())) is False


def test_active_without_a_period_end_is_not_locked_out():
    """A missing period end is OUR parsing gap, not the driver's problem.

    transaction.completed can arrive without a parseable billing period. The
    ladder required a non-null period, so a driver who had just paid could be
    refused until another webhook happened to fill the column in.
    """
    u = _user(subscription_status="active", subscription_current_period_end=None)
    assert _server_allows(u) is True
    assert subscription_state.has_access(u) is True


def test_past_due_without_a_period_end_is_denied():
    """past_due has no paid window to fall back on, so a null period denies."""
    assert _server_allows(_user(subscription_status="past_due",
                                subscription_current_period_end=None)) is False


# --- status hygiene --------------------------------------------------------

@pytest.mark.parametrize("raw", ["comp", "COMP", " Comp ", "Comp"])
def test_comp_status_is_matched_case_insensitively(raw):
    """The paid-window check normalised case; the comp check did not, so a comp
    written as 'Comp' silently granted nothing."""
    u = _user(subscription_status=raw, subscription_comp_expires_at=None)
    assert _server_allows(u) is True
    assert subscription_state.is_comp_active(u) is True


@pytest.mark.parametrize("raw", ["active", "ACTIVE", " Active "])
def test_active_status_is_matched_case_insensitively(raw):
    assert _server_allows(_user(subscription_status=raw,
                                subscription_current_period_end=FUTURE())) is True


def test_comp_forever_never_expires():
    u = _user(subscription_status="comp", subscription_comp_expires_at=None)
    assert subscription_state.is_comp_forever(u) is True
    assert _server_allows(u) is True


def test_expired_comp_loses_access():
    assert _server_allows(_user(subscription_status="comp",
                                subscription_comp_expires_at=PAST())) is False


# --- trial + admin ---------------------------------------------------------

def test_local_trial_grants_access():
    assert _server_allows(_user(trial_expires_at=FUTURE())) is True


def test_expired_trial_with_nothing_else_is_denied():
    assert _server_allows(_user(trial_expires_at=PAST())) is False


def test_admin_always_has_access():
    assert _server_allows(_user(is_admin=1)) is True


# --- the two implementations must not disagree -----------------------------

@pytest.mark.parametrize("status,period,comp,trial", [
    ("active", FUTURE(), None, None),
    ("active", PAST(), None, None),
    ("trialing", FUTURE(), None, None),
    ("past_due", FUTURE(), None, None),
    ("cancelled", FUTURE(), None, None),
    ("cancelled", PAST(), None, None),
    ("paused", FUTURE(), None, None),
    ("comp", None, FUTURE(), None),
    ("comp", None, PAST(), None),
    (None, None, None, FUTURE()),
    (None, None, None, PAST()),
    (None, None, None, None),
])
def test_me_payload_agrees_with_the_server_gate(status, period, comp, trial):
    """/me drives the paywall UI while core gates the API. If they disagree the
    driver either sees a paywall over a working app or an app that 402s behind a
    hidden paywall."""
    u = _user(subscription_status=status, subscription_current_period_end=period,
              subscription_comp_expires_at=comp, trial_expires_at=trial)
    assert subscription_state.has_access(u) == _server_allows(u), (
        f"status={status} period={period} comp={comp} trial={trial}: "
        f"/me says {subscription_state.has_access(u)}, server says {_server_allows(u)}"
    )


def test_response_payload_shape_is_stable():
    u = _user(subscription_status="active", subscription_current_period_end=FUTURE())
    payload = subscription_state.build_subscription_response(u)
    for key in ("status", "has_access", "days_remaining", "trial_expires_at",
                "subscription_current_period_end", "comp_expires_at",
                "comp_reason", "is_comp_forever"):
        assert key in payload, f"/me payload lost {key}"
    assert payload["has_access"] is True
