"""Admin visibility of comps, and the pre-flight before enforcement is enabled.

Two operational gaps, both of the kind that hide a problem rather than cause one:

  * An expired comp leaves subscription_status='comp' behind permanently -- there
    is no transition. The comps listing filtered expired rows out, so the screen
    read "no comps" while users sat in a status that grants nothing.
  * ENFORCE_TRIAL is off, so nobody is gated and lapsed comps/trials are
    invisible. Turning it on applies the full ladder to everyone at once.
"""
from __future__ import annotations

import inspect
import time

import admin_mutation_service as svc


def test_comps_listing_can_include_expired():
    sig = inspect.signature(svc.list_active_comps)
    assert "include_expired" in sig.parameters
    assert sig.parameters["include_expired"].default is False, \
        "default must stay active-only so the existing screen is unchanged"


def test_expired_filter_is_dropped_when_including_expired():
    src = inspect.getsource(svc.list_active_comps)
    assert "if include_expired:" in src
    assert "expiry_params" in src, "params must track the clause or the query desyncs"


def test_preflight_reason_names_an_expired_comp():
    now = int(time.time())
    row = {"subscription_status": "comp", "subscription_comp_expires_at": now - 86400,
           "trial_expires_at": None, "subscription_current_period_end": None}
    assert svc._preflight_reason(row) == "comp_expired"


def test_preflight_reason_names_an_ended_subscription():
    now = int(time.time())
    row = {"subscription_status": "active", "subscription_comp_expires_at": None,
           "trial_expires_at": None, "subscription_current_period_end": now - 10}
    assert svc._preflight_reason(row) == "subscription_period_ended"


def test_preflight_reason_names_an_expired_trial():
    now = int(time.time())
    row = {"subscription_status": None, "subscription_comp_expires_at": None,
           "trial_expires_at": now - 10, "subscription_current_period_end": None}
    assert svc._preflight_reason(row) == "trial_expired"


def test_preflight_reason_names_a_user_who_never_had_anything():
    row = {"subscription_status": None, "subscription_comp_expires_at": None,
           "trial_expires_at": None, "subscription_current_period_end": None}
    assert svc._preflight_reason(row) == "never_had_trial_or_subscription"


def test_preflight_reports_enforcement_state_and_buckets():
    src = inspect.getsource(svc.access_preflight)
    for key in ("enforcement_currently_on", "would_lose_access", "total_users", "counts"):
        assert key in src, f"pre-flight must report {key}"
    assert "_db_exec" not in src, "pre-flight must be read-only"


def test_preflight_uses_the_same_ladder_as_the_gate():
    """If it re-implemented the rules it would reassure an operator about a
    rollout the real gate then handles differently."""
    src = inspect.getsource(svc.access_preflight)
    assert "is_comp_active" in src and "is_subscription_active" in src and "is_trial_active" in src
