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
# finding people
#
# A network you cannot search is one you can only reach through whoever
# happens to post. Handles were linkable and there was no way to find one
# without already knowing it.
# --------------------------------------------------------------------------


def _search(client, account, q, limit=None):
    params = {"q": q}
    if limit is not None:
        params["limit"] = limit
    res = client.get("/social/search/drivers", params=params, headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()


def _set_handle(client, account, handle):
    res = client.post("/social/me/handle", json={"handle": handle}, headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()["profile"]


def test_a_driver_can_be_found_by_name_and_by_handle(app_env):
    _main, client = app_env
    me = _signup(client, "searcher@example.com", "Searcher")
    _signup(client, "marcus@example.com", "Marcus Reyes")

    by_name = _search(client, me, "marcus")["items"]
    assert [d["display_name"] for d in by_name] == ["Marcus Reyes"]

    # Partial, mid-word, and case-insensitive: people type what they remember.
    assert _search(client, me, "REY")["items"][0]["display_name"] == "Marcus Reyes"
    assert _search(client, me, "reyes")["items"][0]["display_name"] == "Marcus Reyes"

    handle = _search(client, me, "marcus")["items"][0]["handle"]
    assert handle, "every account has a handle; search must return it"
    assert _search(client, me, handle)["items"][0]["display_name"] == "Marcus Reyes"


def test_a_result_carries_the_crest_a_row_draws(app_env):
    """Same as the feed: a name says who, a crest says who they are."""
    _main, client = app_env
    me = _signup(client, "crest-searcher@example.com", "Searcher")
    _signup(client, "crested@example.com", "Crested Driver")

    hit = _search(client, me, "Crested")["items"][0]
    assert hit["rank_icon_key"], "a search row has no rank to draw a crest from"
    from rank_badge_store import valid_rank_icon_keys
    assert hit["rank_icon_key"] in set(valid_rank_icon_keys())
    assert hit["rank_name"]
    assert "avatar_url" in hit and "handle" in hit


def test_the_best_match_comes_first(app_env):
    """"mar" should surface Marcus before Omar.

    A plain LIKE returns them in whatever order the table hands back, which
    puts the person you meant somewhere in the middle.
    """
    _main, client = app_env
    me = _signup(client, "ranker@example.com", "Ranker")
    _signup(client, "omar@example.com", "Omar Haddad")
    _signup(client, "marcus@example.com", "Marcus Reyes")

    names = [d["display_name"] for d in _search(client, me, "mar")["items"]]
    assert "Marcus Reyes" in names and "Omar Haddad" in names
    assert names.index("Marcus Reyes") < names.index("Omar Haddad")


def test_an_exact_handle_wins_outright(app_env):
    _main, client = app_env
    me = _signup(client, "handle-searcher@example.com", "Searcher")
    target = _signup(client, "exact@example.com", "Zzz Last Alphabetically")
    _set_handle(client, target, "nightowl")
    # Someone whose NAME starts with the query would otherwise sort first.
    _signup(client, "nightshift@example.com", "Nightowl Nancy")

    items = _search(client, me, "nightowl")["items"]
    assert items[0]["handle"] == "nightowl", [d["handle"] for d in items]


def test_search_never_reveals_someone_who_blocked_you(app_env):
    """The whole point of filtering, not a side effect.

    A search that still listed them would be a way to check whether you had
    been blocked -- which is exactly what a block is meant to stop.
    """
    _main, client = app_env
    me = _signup(client, "blocked-searcher@example.com", "Searcher")
    blocker = _signup(client, "blocker@example.com", "Blocker Person")

    assert _search(client, me, "Blocker")["items"], "precondition: findable before the block"
    res = client.post(f"/social/users/{me['id']}/block", headers=_h(blocker))
    assert res.status_code == 200, res.text
    assert _search(client, me, "Blocker")["items"] == []

    # And symmetrically: someone I blocked does not come back either.
    other = _signup(client, "other@example.com", "Other Person")
    client.post(f"/social/users/{other['id']}/block", headers=_h(me))
    assert _search(client, me, "Other")["items"] == []


def test_muting_someone_does_not_make_them_unfindable(app_env):
    """Muting hides their posts. It is not a statement that they should
    vanish -- a driver who mutes someone may still want their profile."""
    _main, client = app_env
    me = _signup(client, "muter@example.com", "Muter")
    target = _signup(client, "muted@example.com", "Muted Person")
    res = client.post(f"/social/users/{target['id']}/mute", headers=_h(me))
    assert res.status_code == 200, res.text
    assert _search(client, me, "Muted")["items"], "a muted driver became unfindable"


def test_you_are_never_your_own_search_result(app_env):
    _main, client = app_env
    me = _signup(client, "self@example.com", "Selfsearch Driver")
    assert _search(client, me, "Selfsearch")["items"] == []


def test_a_one_letter_query_is_refused_rather_than_answered(app_env):
    """One letter matches most of the network. That is a list, not a search,
    and it is the expensive query to serve."""
    _main, client = app_env
    me = _signup(client, "short@example.com", "Shortquery")
    _signup(client, "someone@example.com", "Someone Else")
    for q in ("", " ", "s"):
        assert _search(client, me, q)["items"] == [], f"{q!r} returned results"
    assert _search(client, me, "So")["items"], "a two-letter query must still work"


def test_the_limit_is_honoured_and_capped(app_env):
    _main, client = app_env
    me = _signup(client, "limiter@example.com", "Limiter")
    for i in range(6):
        _signup(client, f"crowd{i}@example.com", f"Crowd Member {i}")

    assert len(_search(client, me, "Crowd", limit=3)["items"]) == 3
    # A client asking for a thousand gets the ceiling, not a thousand.
    import social_service
    big = _search(client, me, "Crowd", limit=10_000)["items"]
    assert len(big) <= social_service.SEARCH_DRIVERS_MAX_LIMIT


def test_wildcards_typed_by_a_driver_are_literal_text(app_env):
    """A driver typing % is searching for a percent sign, not asking for
    everyone. Unescaped it would match the whole table."""
    _main, client = app_env
    me = _signup(client, "wild@example.com", "Wildcard Searcher")
    _signup(client, "plain@example.com", "Plain Person")
    assert _search(client, me, "%%")["items"] == []
    assert _search(client, me, "_a")["items"] == []


def test_the_query_comes_back_so_a_stale_response_can_be_dropped(app_env):
    """Typing is faster than the network. Without the query echoed, a slow
    response for "mar" lands on top of the results for "marcus"."""
    _main, client = app_env
    me = _signup(client, "echo@example.com", "Echo")
    assert _search(client, me, "  marcus  ")["query"] == "marcus"
