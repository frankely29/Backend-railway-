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


# --------------------------------------------------------------------------
# searching the comps list
# --------------------------------------------------------------------------

def _capture(monkeypatch):
    """Run list_active_comps without a database and hand back the SQL it built."""
    seen = {}

    def count(sql, params):
        seen["count"] = (sql, list(params))
        return {"c": 0}

    def rows(sql, params):
        seen["rows"] = (sql, list(params))
        return []

    monkeypatch.setattr(svc, "_db_query_one", count)
    monkeypatch.setattr(svc, "_db_query_all", rows)
    return seen


def test_comp_search_lowers_both_sides(monkeypatch):
    """This one cannot be proved by querying the test database.

    SQLite's LIKE is case-insensitive and Postgres's is case-sensitive, so a
    search that works here fails on production and no amount of running it
    locally shows that. The SQL itself is the evidence: both sides lowered.
    """
    seen = _capture(monkeypatch)
    svc.list_active_comps(search="Frankely")

    sql, params = seen["rows"]
    for column in ("email", "display_name", "subscription_comp_reason"):
        assert f"LOWER({column}) LIKE" in sql, f"{column} is compared case-sensitively"
    assert all(p == "%frankely%" for p in params[:-2] if isinstance(p, str)), params
    assert "%Frankely%" not in params, "the pattern still carries the typed case"


def test_comp_search_counts_and_lists_the_same_rows(monkeypatch):
    """The count and the page are two queries. They have to filter identically
    or the screen says 40 results and shows 12."""
    seen = _capture(monkeypatch)
    svc.list_active_comps(search="Beta")

    count_sql, count_params = seen["count"]
    rows_sql, rows_params = seen["rows"]
    assert "LOWER(email) LIKE" in count_sql
    # the row query adds LIMIT/OFFSET on the end; everything before must match
    assert rows_params[:len(count_params)] == count_params


def test_an_underscore_in_a_search_is_not_a_wildcard(monkeypatch):
    """% and _ are LIKE wildcards. Searching a comp reason of "beta_tester"
    was also matching "betaXtester"."""
    seen = _capture(monkeypatch)
    svc.list_active_comps(search="beta_tester")

    sql, params = seen["rows"]
    assert "ESCAPE" in sql
    assert "%beta!_tester%" in params, params


def test_a_percent_in_a_search_is_not_a_wildcard(monkeypatch):
    seen = _capture(monkeypatch)
    svc.list_active_comps(search="100%")
    _sql_text, params = seen["rows"]
    assert "%100!%%" in params, params


def test_the_escape_character_itself_is_escaped(monkeypatch):
    """Otherwise searching for "!" builds a pattern ending in a dangling
    escape, which Postgres rejects outright."""
    seen = _capture(monkeypatch)
    svc.list_active_comps(search="wow!")
    _sql_text, params = seen["rows"]
    assert "%wow!!%" in params, params


def test_a_search_is_trimmed_before_it_is_used(monkeypatch):
    seen = _capture(monkeypatch)
    svc.list_active_comps(search="  Frankely  ")
    _sql_text, params = seen["rows"]
    assert "%frankely%" in params, params
