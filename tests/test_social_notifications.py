"""Notifications — the return loop.

Everything else in the social API is a driver doing something to somebody
else's post, and until now none of it told that somebody. These tests are
about the ways a notifications feature is usually wrong rather than missing:

  - it tells you about yourself, so liking your own post is "news",
  - it repeats, so like/unlike/like leaves three rows and the screen becomes
    something people turn off,
  - it keeps saying things that stopped being true: a like that was taken
    back, a follow that was undone, a post that was deleted,
  - it leaks past a block, which turns blocking someone into a way of hearing
    from them,
  - the unread count and the page disagree,
  - and marking read marks something read that was never on the screen.
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
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-notifications-")
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
        "core", "chat", "media_store", "admin_security",
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


def _h(a: dict) -> dict:
    return {"Authorization": f"Bearer {a['token']}"}


def _post(client: TestClient, a: dict, body: str) -> dict:
    res = client.post("/social/posts", json={"body": body}, headers=_h(a))
    assert res.status_code == 200, res.text
    return res.json()["post"]


def _notes(client: TestClient, a: dict, **params) -> dict:
    res = client.get("/social/notifications", params=params, headers=_h(a))
    assert res.status_code == 200, res.text
    return res.json()


def _unread(client: TestClient, a: dict) -> int:
    res = client.get("/social/notifications/unread", headers=_h(a))
    assert res.status_code == 200, res.text
    return res.json()["unread"]


# --------------------------------------------------------------------------
# the four things that make one
# --------------------------------------------------------------------------

def test_a_like_tells_the_author(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    fan = _signup(client, "b@example.com", "Marco")
    post = _post(client, author, "LGA queue is 40 deep")

    client.post(f"/social/posts/{post['id']}/like", headers=_h(fan))

    page = _notes(client, author)
    assert len(page["items"]) == 1
    note = page["items"][0]
    assert note["kind"] == "like"
    assert note["actor"]["display_name"] == "Marco"
    assert note["post_id"] == post["id"]
    # The row says WHICH post without the client fetching it.
    assert note["post_excerpt"] == "LGA queue is 40 deep"
    assert note["read"] is False
    assert page["unread"] == 1


def test_a_comment_tells_the_author(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    other = _signup(client, "b@example.com", "Marco")
    post = _post(client, author, "anyone at JFK")

    client.post(f"/social/posts/{post['id']}/comments",
                json={"body": "terminal 4 is dead"}, headers=_h(other))

    items = _notes(client, author)["items"]
    assert [i["kind"] for i in items] == ["comment"]
    assert items[0]["comment_id"]


def test_a_reply_tells_the_person_answered_as_well_as_the_author(app_env):
    """Two people are owed one comment, and they are different people."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    first = _signup(client, "b@example.com", "Marco")
    second = _signup(client, "c@example.com", "Dani")
    post = _post(client, author, "anyone at JFK")

    res = client.post(f"/social/posts/{post['id']}/comments",
                      json={"body": "terminal 4 is dead"}, headers=_h(first))
    parent = res.json()["comment"]["id"]
    client.post(f"/social/posts/{post['id']}/comments",
                json={"body": "still?", "parent_id": parent}, headers=_h(second))

    # The post's author hears about both comments on their post.
    assert sorted(i["kind"] for i in _notes(client, author)["items"]) == ["comment", "comment"]
    # The person answered hears about the reply, and only that.
    assert [i["kind"] for i in _notes(client, first)["items"]] == ["reply"]


def test_a_follow_tells_the_followed(app_env):
    _main, client = app_env
    star = _signup(client, "a@example.com", "Author")
    fan = _signup(client, "b@example.com", "Marco")

    client.post(f"/social/users/{star['id']}/follow", headers=_h(fan))

    items = _notes(client, star)["items"]
    assert [i["kind"] for i in items] == ["follow"]
    # A follow is about a person, so there is nothing to open.
    assert items[0]["post_id"] is None
    assert items[0]["comment_id"] is None


# --------------------------------------------------------------------------
# the ways it goes wrong
# --------------------------------------------------------------------------

def test_you_are_never_told_about_yourself(app_env):
    """Liking your own post is not news, and neither is answering yourself."""
    _main, client = app_env
    me = _signup(client, "a@example.com", "Author")
    post = _post(client, me, "mine")

    client.post(f"/social/posts/{post['id']}/like", headers=_h(me))
    res = client.post(f"/social/posts/{post['id']}/comments",
                      json={"body": "adding to this"}, headers=_h(me))
    parent = res.json()["comment"]["id"]
    client.post(f"/social/posts/{post['id']}/comments",
                json={"body": "and this", "parent_id": parent}, headers=_h(me))

    assert _notes(client, me)["items"] == []
    assert _unread(client, me) == 0


def test_liking_twice_leaves_one_notification(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    fan = _signup(client, "b@example.com", "Marco")
    post = _post(client, author, "one")

    for _ in range(3):
        client.post(f"/social/posts/{post['id']}/like", headers=_h(fan))

    assert len(_notes(client, author)["items"]) == 1
    assert _unread(client, author) == 1


def test_an_unlike_takes_its_notification_with_it(app_env):
    """A notification about a like that no longer exists is a lie."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    fan = _signup(client, "b@example.com", "Marco")
    post = _post(client, author, "one")

    client.post(f"/social/posts/{post['id']}/like", headers=_h(fan))
    assert _unread(client, author) == 1

    client.delete(f"/social/posts/{post['id']}/like", headers=_h(fan))
    assert _notes(client, author)["items"] == []
    assert _unread(client, author) == 0

    # And liking again brings it back, rather than being swallowed by a row
    # that was never cleaned up.
    client.post(f"/social/posts/{post['id']}/like", headers=_h(fan))
    assert len(_notes(client, author)["items"]) == 1


def test_an_unfollow_takes_its_notification_with_it(app_env):
    _main, client = app_env
    star = _signup(client, "a@example.com", "Author")
    fan = _signup(client, "b@example.com", "Marco")

    client.post(f"/social/users/{star['id']}/follow", headers=_h(fan))
    client.delete(f"/social/users/{star['id']}/follow", headers=_h(fan))
    assert _notes(client, star)["items"] == []

    # Follow, unfollow, follow leaves exactly one -- not two, and not none.
    client.post(f"/social/users/{star['id']}/follow", headers=_h(fan))
    assert len(_notes(client, star)["items"]) == 1


def test_a_deleted_post_takes_its_notifications_off_the_screen(app_env):
    """"Marco liked your post" that opens nothing is worse than silence."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    fan = _signup(client, "b@example.com", "Marco")
    post = _post(client, author, "gone soon")

    client.post(f"/social/posts/{post['id']}/like", headers=_h(fan))
    assert len(_notes(client, author)["items"]) == 1

    client.delete(f"/social/posts/{post['id']}", headers=_h(author))
    assert _notes(client, author)["items"] == []


def test_a_block_silences_notifications_in_both_directions(app_env):
    """Otherwise blocking someone becomes a way of hearing from them."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    pest = _signup(client, "b@example.com", "Pest")
    post = _post(client, author, "hello")

    client.post(f"/social/users/{pest['id']}/block", headers=_h(author))
    client.post(f"/social/posts/{post['id']}/like", headers=_h(pest))

    assert _notes(client, author)["items"] == []
    assert _unread(client, author) == 0


def test_a_block_after_the_fact_clears_the_screen_too(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    pest = _signup(client, "b@example.com", "Pest")
    post = _post(client, author, "hello")

    client.post(f"/social/posts/{post['id']}/like", headers=_h(pest))
    assert len(_notes(client, author)["items"]) == 1

    client.post(f"/social/users/{pest['id']}/block", headers=_h(author))
    assert _notes(client, author)["items"] == []


# --------------------------------------------------------------------------
# reading them
# --------------------------------------------------------------------------

def test_marking_read_clears_the_badge_and_keeps_the_rows(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    fan = _signup(client, "b@example.com", "Marco")
    post = _post(client, author, "one")
    client.post(f"/social/posts/{post['id']}/like", headers=_h(fan))

    res = client.post("/social/notifications/read", json={}, headers=_h(author))
    assert res.status_code == 200, res.text
    assert res.json()["unread"] == 0

    page = _notes(client, author)
    assert len(page["items"]) == 1, "the rows are gone, not just the badge"
    assert page["items"][0]["read"] is True
    assert page["unread"] == 0


def test_marking_read_to_a_point_leaves_later_ones_unread(app_env):
    """Opening the screen must not mark read something that was never on it."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    one = _signup(client, "b@example.com", "Marco")
    two = _signup(client, "c@example.com", "Dani")
    post = _post(client, author, "one")

    client.post(f"/social/posts/{post['id']}/like", headers=_h(one))
    seen = _notes(client, author)["items"][0]["id"]
    # Arrives while the screen is open, below the cut.
    client.post(f"/social/users/{author['id']}/follow", headers=_h(two))

    client.post("/social/notifications/read", json={"before_id": seen}, headers=_h(author))
    assert _unread(client, author) == 1

    by_kind = {i["kind"]: i["read"] for i in _notes(client, author)["items"]}
    assert by_kind == {"like": True, "follow": False}


def test_the_page_is_newest_first_and_keyset_paged(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    fans = [_signup(client, f"f{i}@example.com", f"Fan{i}") for i in range(3)]
    post = _post(client, author, "one")
    for fan in fans:
        client.post(f"/social/posts/{post['id']}/like", headers=_h(fan))

    first = _notes(client, author, limit=2)
    assert len(first["items"]) == 2
    assert first["items"][0]["id"] > first["items"][1]["id"], "not newest first"
    assert first["next_before_id"] == first["items"][-1]["id"]

    rest = _notes(client, author, limit=2, before_id=first["next_before_id"])
    assert len(rest["items"]) == 1
    assert rest["next_before_id"] is None
    seen = [i["id"] for i in first["items"] + rest["items"]]
    assert len(set(seen)) == 3, "a page repeated a row"


def test_the_badge_and_the_page_agree(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    fan = _signup(client, "b@example.com", "Marco")
    post = _post(client, author, "one")
    client.post(f"/social/posts/{post['id']}/like", headers=_h(fan))
    client.post(f"/social/users/{author['id']}/follow", headers=_h(fan))

    assert _unread(client, author) == _notes(client, author)["unread"] == 2


def test_notifications_need_a_real_account(app_env):
    _main, client = app_env
    assert client.get("/social/notifications").status_code in (401, 403)
    assert client.get("/social/notifications/unread").status_code in (401, 403)
    assert client.post("/social/notifications/read", json={}).status_code in (401, 403)


def test_one_drivers_notifications_are_their_own(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    fan = _signup(client, "b@example.com", "Marco")
    post = _post(client, author, "one")
    client.post(f"/social/posts/{post['id']}/like", headers=_h(fan))

    assert _notes(client, fan)["items"] == []
    assert _unread(client, fan) == 0
