"""Phase 1: a profile worth following — handles, bio, driver tags, reputation.

The tests aim at the parts that are quietly dangerous rather than the happy
path: case-insensitive handle uniqueness (the impersonation vector), reserved
names (the phishing vector), patch-vs-replace on the identity fields (the
silent-data-loss vector), and the guarantee that nobody is ever handle-less.
"""
from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def app_env(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-identity-")
    data_dir = Path(temp_dir.name)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("COMMUNITY_DB", str(data_dir / "community.db"))
    monkeypatch.setenv("FRAMES_DIR", str(data_dir / "frames"))
    monkeypatch.setenv("JWT_SECRET", "test-jwt-secret-abcdefghijklmnopqrstuvwxyz")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "password123")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_URL", raising=False)

    for name in [
        "core", "chat", "media_store",
        "leaderboard_db", "leaderboard_routes", "leaderboard_service", "leaderboard_tracker",
        "pickup_recording_feature", "games_models", "games_service", "games_routes",
        "social_db", "social_models", "social_service", "social_routes", "social_identity",
        "social_moderation", "social_admin_routes",
        "main",
    ]:
        sys.modules.pop(name, None)

    main = importlib.import_module("main")
    main.startup()

    frames_dir = Path(os.environ["FRAMES_DIR"])
    frames_dir.mkdir(parents=True, exist_ok=True)
    (frames_dir / "timeline.json").write_text(
        json.dumps({"timeline": ["2026-03-19T00:00:00Z"], "count": 1}), encoding="utf-8")
    (frames_dir / "frame_000000.json").write_text(
        json.dumps({"type": "FeatureCollection", "features": []}), encoding="utf-8")

    with TestClient(main.app) as client:
        yield main, client

    temp_dir.cleanup()


def _signup(client: TestClient, email: str, name: str) -> dict:
    res = client.post("/auth/signup",
                      json={"email": email, "password": "password123", "display_name": name})
    assert res.status_code == 200, res.text
    return res.json()


def _h(account: dict) -> dict:
    return {"Authorization": f"Bearer {account['token']}"}


def _me(client: TestClient, account: dict) -> dict:
    res = client.get("/social/me/profile", headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()["profile"]


# --------------------------------------------------------------------------
# every account has a handle
# --------------------------------------------------------------------------

def test_signup_assigns_a_handle(app_env):
    """A profile nobody can link to is the thing Phase 1 exists to fix."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "NightHawk")
    assert _me(client, ana)["handle"] == "NightHawk"


def test_handles_are_derived_from_email_when_the_name_will_not_do(app_env):
    _main, client = app_env
    ana = _signup(client, "queenslocal@example.com", "??")
    assert _me(client, ana)["handle"] == "queenslocal"


def test_two_drivers_with_the_same_name_both_get_usable_handles(app_env):
    _main, client = app_env
    first = _signup(client, "a@example.com", "Mike")
    second = _signup(client, "b@example.com", "Mike")
    h1, h2 = _me(client, first)["handle"], _me(client, second)["handle"]
    assert h1 and h2 and h1 != h2, f"{h1} vs {h2}"


def test_backfill_gives_handles_to_accounts_that_predate_them(app_env):
    """Existing users must not have to log in and pick one."""
    main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    main._db_exec("UPDATE users SET handle=NULL, handle_key=NULL WHERE id=?", (int(ana["id"]),))
    assert _me(client, ana)["handle"] is None

    from social_identity import backfill_handles
    assert backfill_handles() >= 1
    assert _me(client, ana)["handle"]


# --------------------------------------------------------------------------
# claiming a handle
# --------------------------------------------------------------------------

def test_handle_uniqueness_ignores_case(app_env):
    """@NightHawk and @nighthawk must not be two people — that is how
    impersonation works."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")

    assert client.post("/social/me/handle", json={"handle": "NightHawk"},
                       headers=_h(ana)).status_code == 200
    clash = client.post("/social/me/handle", json={"handle": "nighthawk"}, headers=_h(ben))
    assert clash.status_code == 409
    assert "taken" in clash.json()["detail"].lower()


def test_handle_keeps_the_capitals_you_typed(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.post("/social/me/handle", json={"handle": "NightHawk"}, headers=_h(ana))
    assert res.json()["profile"]["handle"] == "NightHawk"


def test_reclaiming_your_own_handle_is_not_a_conflict(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    client.post("/social/me/handle", json={"handle": "NightHawk"}, headers=_h(ana))
    again = client.post("/social/me/handle", json={"handle": "NIGHTHAWK"}, headers=_h(ana))
    assert again.status_code == 200
    assert again.json()["profile"]["handle"] == "NIGHTHAWK"


@pytest.mark.parametrize("handle,why", [
    ("ab", "too short"),
    ("a" * 21, "too long"),
    ("night hawk", "space"),
    ("night.hawk", "dot"),
    ("night-hawk", "dash"),
    ("_hawk", "leading underscore"),
    ("hawk_", "trailing underscore"),
    ("12345", "all digits reads as a user id"),
    ("admin", "reserved"),
    ("support", "reserved"),
    ("joseo", "reserved"),
])
def test_bad_handles_are_refused(app_env, handle, why):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.post("/social/me/handle", json={"handle": handle}, headers=_h(ana))
    assert res.status_code == 400, f"{handle} ({why}) was accepted"


def test_a_leading_at_sign_is_tolerated(app_env):
    """People type the @. Refusing it is a pointless way to fail."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.post("/social/me/handle", json={"handle": "@NightHawk"}, headers=_h(ana))
    assert res.status_code == 200
    assert res.json()["profile"]["handle"] == "NightHawk"


def test_availability_answers_instead_of_erroring(app_env):
    """It is checked while typing, so a malformed handle is an answer with a
    reason, not a 400 the field has to catch."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    client.post("/social/me/handle", json={"handle": "NightHawk"}, headers=_h(ana))

    free = client.get("/social/handles/DayShift/available", headers=_h(ben)).json()
    assert free["available"] is True and free["reason"] is None

    taken = client.get("/social/handles/nighthawk/available", headers=_h(ben)).json()
    assert taken["available"] is False and "taken" in taken["reason"].lower()

    bad = client.get("/social/handles/ab/available", headers=_h(ben))
    assert bad.status_code == 200, "must answer, not raise"
    assert bad.json()["available"] is False and "3 characters" in bad.json()["reason"]

    mine = client.get("/social/handles/NightHawk/available", headers=_h(ana)).json()
    assert mine["available"] is True, "your own handle is available to you"


def test_a_handle_resolves_to_a_profile(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    client.post("/social/me/handle", json={"handle": "NightHawk"}, headers=_h(ana))

    found = client.get("/social/users/by-handle/nighthawk/profile", headers=_h(ben))
    assert found.status_code == 200
    assert found.json()["profile"]["user_id"] == ana["id"]
    assert client.get("/social/users/by-handle/nobody/profile",
                      headers=_h(ben)).status_code == 404


# --------------------------------------------------------------------------
# bio and driver tags
# --------------------------------------------------------------------------

def test_identity_fields_round_trip(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.post("/social/me/identity", json={
        "bio": "Nights only. JFK and LGA.",
        "platforms": ["uber", "lyft"],
        "vehicle_type": "hybrid",
        "driving_since_year": 2017,
    }, headers=_h(ana))
    assert res.status_code == 200, res.text
    profile = res.json()["profile"]
    assert profile["bio"] == "Nights only. JFK and LGA."
    assert profile["platforms"] == ["uber", "lyft"]
    assert profile["vehicle_type"] == "hybrid"
    assert profile["driving_since_year"] == 2017


def test_a_partial_save_does_not_wipe_the_fields_it_omits(app_env):
    """The bug this guards is silent: send only a bio, lose your vehicle."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    client.post("/social/me/identity", json={
        "bio": "first", "platforms": ["uber"], "vehicle_type": "suv",
        "driving_since_year": 2015,
    }, headers=_h(ana))

    client.post("/social/me/identity", json={"bio": "second"}, headers=_h(ana))
    profile = _me(client, ana)
    assert profile["bio"] == "second"
    assert profile["platforms"] == ["uber"], "omitted field must survive"
    assert profile["vehicle_type"] == "suv"
    assert profile["driving_since_year"] == 2015


def test_an_explicit_null_clears_a_field(app_env):
    """Omitted and null have to mean different things, or nothing can be erased."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    client.post("/social/me/identity", json={"bio": "something"}, headers=_h(ana))
    client.post("/social/me/identity", json={"bio": None}, headers=_h(ana))
    assert _me(client, ana)["bio"] is None


def test_unknown_platforms_and_vehicles_are_dropped_not_stored(app_env):
    """Free text here becomes 400 spellings of 'uber', which filters nothing."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.post("/social/me/identity", json={
        "platforms": ["uber", "UBER", "not-a-platform", "lyft"],
        "vehicle_type": "spaceship",
    }, headers=_h(ana))
    profile = res.json()["profile"]
    assert profile["platforms"] == ["uber", "lyft"], "deduped, filtered, lowercased"
    assert profile["vehicle_type"] is None


def test_impossible_start_years_are_refused(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    for year in (1998, 2008, 3000):
        res = client.post("/social/me/identity", json={"driving_since_year": year},
                          headers=_h(ana))
        assert res.status_code == 400, year


def test_bio_whitespace_is_collapsed(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.post("/social/me/identity",
                      json={"bio": "  nights   only\n\nJFK and LGA  "}, headers=_h(ana))
    assert res.json()["profile"]["bio"] == "nights only JFK and LGA"


def test_an_over_long_bio_is_refused_rather_than_truncated(app_env):
    """Silently cutting someone's words and saving the stump is worse than
    telling them it is too long. The model enforces the cap; clean_bio keeps its
    own truncation only as a backstop for internal callers."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.post("/social/me/identity", json={"bio": "x" * 400}, headers=_h(ana))
    assert res.status_code == 422
    assert _me(client, ana)["bio"] is None, "nothing was saved"


def test_option_lists_are_served_to_the_client(app_env):
    """So adding a platform does not require a frontend release."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.get("/social/identity/options", headers=_h(ana)).json()
    assert "uber" in res["platforms"] and "lyft" in res["platforms"]
    assert "ev" in res["vehicle_types"]


# --------------------------------------------------------------------------
# reputation on the profile
# --------------------------------------------------------------------------

def test_profile_carries_a_reputation_block(app_env):
    """Driving history as social proof is the thing a generic photo app cannot
    show. It must be present even for a brand new driver."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    rep = _me(client, ana)["reputation"]
    assert rep is not None
    for key in ("level", "rank_name", "badge_code", "lifetime_miles", "trips_logged"):
        assert key in rep


def test_a_profile_still_renders_when_reputation_cannot_be_read(app_env, monkeypatch):
    """Decoration must not be able to 500 the page it decorates."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    import leaderboard_service

    def boom(*_a, **_k):
        raise RuntimeError("badge cache is cold")

    monkeypatch.setattr(leaderboard_service, "get_progression_for_user", boom)
    monkeypatch.setattr(leaderboard_service, "get_best_current_badge_for_user", boom)

    res = client.get("/social/me/profile", headers=_h(ana))
    assert res.status_code == 200, res.text
    assert res.json()["profile"]["reputation"]["level"] is None


# --------------------------------------------------------------------------
# handles reach the feed
# --------------------------------------------------------------------------

def test_a_post_carries_its_author_handle(app_env):
    """Otherwise a feed card has nothing to link to."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    client.post("/social/me/handle", json={"handle": "NightHawk"}, headers=_h(ana))
    client.post("/social/posts", json={"body": "LGA is moving"}, headers=_h(ana))

    item = client.get("/social/feed", params={"scope": "everyone"},
                      headers=_h(ana)).json()["items"][0]
    assert item["author"]["handle"] == "NightHawk"


def test_identity_routes_require_authentication(app_env):
    _main, client = app_env
    for method, path in [
        ("post", "/social/me/handle"),
        ("post", "/social/me/identity"),
        ("get", "/social/me/handle/suggest"),
        ("get", "/social/handles/abc/available"),
        ("get", "/social/identity/options"),
        ("get", "/social/users/by-handle/abc/profile"),
    ]:
        res = getattr(client, method)(path, **({"json": {}} if method == "post" else {}))
        assert res.status_code in (401, 403, 422), f"{method.upper()} {path} -> {res.status_code}"
