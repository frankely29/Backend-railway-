"""Admin-issued free-access codes.

grant_comp needs a user_id, so it only covers people the admin can already look
up. A code is the other shape: minted by an admin, handed out, redeemed by the
recipient. Access is granted through the existing comp columns so it travels the
same ladder as every other kind of access.
"""
from __future__ import annotations

import inspect

import pytest
from fastapi import HTTPException

import access_tokens as at


# --- code format -----------------------------------------------------------

def test_codes_avoid_characters_that_get_misread():
    """Codes get read aloud and retyped on a phone; I/O/0/1 is where that fails."""
    for ch in "IO01":
        assert ch not in at.CODE_ALPHABET


def test_generated_codes_are_unique_and_well_formed():
    codes = {at.generate_code() for _ in range(500)}
    assert len(codes) == 500, "collision in 500 draws"
    for c in list(codes)[:20]:
        assert c.startswith("JOSEO-")
        assert len(c.split("-")) == 3


@pytest.mark.parametrize("typed", [
    "JOSEO-ABCD-2345", "joseo-abcd-2345", "  JOSEO-abcd-2345  ",
    "JOSEOABCD2345", "joseoabcd2345", "JOSEO abcd 2345", "JOSEO_ABCD_2345",
])
def test_however_the_user_types_it_normalises_to_one_code(typed):
    """A valid code rejected because of a stray space or lowercase reads to the
    user as 'the code you gave me is broken'."""
    assert at.normalize_code(typed) == "JOSEO-ABCD-2345"


def test_blank_input_normalises_to_empty_not_a_bare_prefix():
    for blank in (None, "", "   ", "JOSEO", "joseo-"):
        assert at.normalize_code(blank) == ""


# --- validity states -------------------------------------------------------

def _row(**over):
    base = {"revoked_at": None, "uses": 0, "max_uses": 1, "redeem_by": None}
    base.update(over)
    return base


def test_a_fresh_code_is_active():
    assert at._token_state(_row(), now=1000) == "active"


def test_revoked_beats_every_other_state():
    assert at._token_state(_row(revoked_at=5, uses=0), now=1000) == "revoked"


def test_a_used_up_code_is_not_active():
    assert at._token_state(_row(uses=1, max_uses=1), now=1000) == "used_up"


def test_a_multi_use_code_stays_active_until_exhausted():
    assert at._token_state(_row(uses=4, max_uses=5), now=1000) == "active"
    assert at._token_state(_row(uses=5, max_uses=5), now=1000) == "used_up"


def test_a_code_past_its_redeem_by_is_expired():
    assert at._token_state(_row(redeem_by=999), now=1000) == "expired"
    assert at._token_state(_row(redeem_by=1001), now=1000) == "active"


def test_no_redeem_by_never_expires():
    assert at._token_state(_row(redeem_by=None), now=10**12) == "active"


# --- input validation ------------------------------------------------------

@pytest.mark.parametrize("kwargs", [
    {"access_days": 0}, {"access_days": -5},
    {"redeem_by_days": 0}, {"redeem_by_days": -1},
    {"max_uses": 0}, {"max_uses": -1}, {"max_uses": at.MAX_USES_LIMIT + 1},
])
def test_nonsense_durations_are_rejected(kwargs):
    with pytest.raises(HTTPException) as exc:
        at.create_access_token(actor_user_id=1, **kwargs)
    assert exc.value.status_code == 400


# --- the two expiries are independent --------------------------------------

def test_the_two_expiries_answer_different_questions():
    """access_days limits the ACCESS; redeem_by limits the CODE. Conflating them
    would make 'a code valid for a week granting a year' unexpressible."""
    src = inspect.getsource(at.create_access_token)
    assert "redeem_by_days" in src and "access_days" in src
    sig = inspect.signature(at.create_access_token)
    assert sig.parameters["access_days"].default is None, "omitted = forever access"
    assert sig.parameters["redeem_by_days"].default is None, "omitted = code never expires"


# --- redemption safety -----------------------------------------------------

def test_use_claim_is_atomic_in_sql():
    """Two people redeeming a single-use code at the same instant must not both
    win. The guard has to be in the UPDATE, not a read-then-write."""
    src = inspect.getsource(at.redeem_access_token)
    assert "uses < max_uses" in src, "use count must be claimed atomically"
    assert "revoked_at IS NULL" in src, "a revoke racing a redeem must win"


def test_redemption_grants_through_the_existing_comp_columns():
    """A second access mechanism would be a second thing to keep in sync with
    the ladder, and this codebase has already been bitten by exactly that."""
    src = inspect.getsource(at._apply_comp_from_token)
    assert "subscription_comp_expires_at" in src
    assert "subscription_status" in src


def test_redeeming_never_shortens_access_the_user_already_has():
    """A gift must not take time away: a 7-day code redeemed against a 30-day
    comp keeps the 30 days, and forever beats any date."""
    src = inspect.getsource(at._apply_comp_from_token)
    assert "max(" in src, "the longer window has to win"
    assert "effective_expiry = None" in src, "an existing forever comp must survive"


def test_revoking_does_not_strip_access_already_granted():
    src = inspect.getsource(at.revoke_access_token)
    assert "UPDATE access_tokens" in src
    assert "UPDATE users" not in src, "revoking a leaked code must not cut off its rightful users"


# --- schema ----------------------------------------------------------------

def test_one_redemption_per_user_per_code_is_enforced_by_the_database():
    ddl = " ".join(at.create_access_token_tables_sql("sqlite"))
    assert "UNIQUE INDEX" in ddl and "(code, user_id)" in ddl


@pytest.mark.parametrize("backend,expected", [
    ("sqlite", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("postgres", "BIGSERIAL PRIMARY KEY"),
])
def test_autoincrement_matches_the_backend(backend, expected):
    """The wrong spelling fails at startup on whichever backend wasn't tested."""
    assert expected in " ".join(at.create_access_token_tables_sql(backend))


def test_redeem_route_is_reachable_without_access():
    """The people who need to redeem are exactly the ones the gate is turning
    away. Behind require_user this would 402 before it could grant anything."""
    import subscription_routes
    src = inspect.getsource(subscription_routes)
    assert "/redeem" in src
    assert "Depends(require_user)" not in src
