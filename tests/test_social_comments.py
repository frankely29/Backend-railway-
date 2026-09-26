"""Comments on posts: reading, writing, deleting, and who may do which.

The things worth testing here are the ones that are quiet when they break:

  - a comment count under a post that does not match what opening it shows,
  - a block that hides someone's posts but still shows their replies,
  - a post owner unable to remove what appears under their own photo,
  - a deleted comment still counted,
  - and forward paging that loops or skips.
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
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-comments-")
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
        # admin_security holds its own reference to core's db helpers, so
        # leaving it cached points the admin tests here at the PREVIOUS test's
        # torn-down database -- "no such table: users", and only when the file
        # runs as a whole.
        "social_moderation", "social_admin_routes", "admin_security",
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


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _signup(client, email, name):
    res = client.post("/auth/signup", json={
        "email": email, "password": "password123", "display_name": name})
    assert res.status_code == 200, res.text
    return res.json()


def _h(account):
    return {"Authorization": f"Bearer {account['token']}"}


def _post(client, account, body="A post"):
    res = client.post("/social/posts", json={"body": body}, headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()["post"]


def _comment(client, account, post_id, body):
    res = client.post(f"/social/posts/{post_id}/comments",
                      json={"body": body}, headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()


def _comments(client, account, post_id, **params):
    res = client.get(f"/social/posts/{post_id}/comments", params=params, headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()


# --------------------------------------------------------------------------
# writing
# --------------------------------------------------------------------------

def test_a_driver_can_reply_to_a_post(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author)

    out = _comment(client, reader, post["id"], "22 min from the back of the line.")
    assert out["comment"]["body"] == "22 min from the back of the line."
    assert out["comment"]["author"]["display_name"] == "Reader"
    assert out["comment_count"] == 1


def test_an_empty_comment_is_refused(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    for body in ("", "   ", "\n\t "):
        res = client.post(f"/social/posts/{post['id']}/comments",
                          json={"body": body}, headers=_h(author))
        assert res.status_code in (400, 422), f"{body!r} -> {res.status_code}"


def test_whitespace_is_collapsed_not_preserved_wholesale(app_env):
    """A reply pasted out of a note app should not arrive as a column."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    out = _comment(client, author, post["id"], "line   one\n\n\nline    two")
    assert out["comment"]["body"] == "line one line two"


def test_a_comment_on_a_missing_post_is_a_404(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    res = client.post("/social/posts/999999/comments",
                      json={"body": "hello"}, headers=_h(author))
    assert res.status_code == 404


def test_a_comment_on_a_deleted_post_is_a_404(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author)
    assert client.delete(f"/social/posts/{post['id']}", headers=_h(author)).status_code == 200
    res = client.post(f"/social/posts/{post['id']}/comments",
                      json={"body": "hello"}, headers=_h(reader))
    assert res.status_code == 404


def test_comments_need_a_token(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    assert client.get(f"/social/posts/{post['id']}/comments").status_code in (401, 403)
    assert client.post(f"/social/posts/{post['id']}/comments",
                       json={"body": "x"}).status_code in (401, 403)


# --------------------------------------------------------------------------
# reading
# --------------------------------------------------------------------------

def test_comments_read_oldest_first(app_env):
    """A conversation runs forwards, unlike a feed."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    for i in range(4):
        _comment(client, author, post["id"], f"reply {i}")
    bodies = [c["body"] for c in _comments(client, author, post["id"])["items"]]
    assert bodies == ["reply 0", "reply 1", "reply 2", "reply 3"]


def test_paging_forwards_neither_loops_nor_skips(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    for i in range(7):
        _comment(client, author, post["id"], f"reply {i}")

    seen = []
    after = None
    for _ in range(5):
        page = _comments(client, author, post["id"], limit=3, **({"after_id": after} if after else {}))
        seen.extend(c["body"] for c in page["items"])
        after = page["next_after_id"]
        if not after:
            break
    assert seen == [f"reply {i}" for i in range(7)]
    assert len(seen) == len(set(seen)), "a page repeated"


def test_the_last_page_has_no_cursor(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    _comment(client, author, post["id"], "only one")
    page = _comments(client, author, post["id"], limit=10)
    assert page["next_after_id"] is None


def test_a_comment_carries_the_author_the_feed_shows(app_env):
    """Same author shape as a post, so one card renderer serves both."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    client.post("/social/me/identity", json={"platforms": ["uber"]}, headers=_h(author))
    post = _post(client, author)
    _comment(client, author, post["id"], "mine")
    item = _comments(client, author, post["id"])["items"][0]
    assert set(item["author"]) >= {"user_id", "display_name", "handle", "city",
                                   "avatar_url", "level", "rank_icon_key",
                                   "rank_name", "platforms"}
    assert item["author"]["platforms"] == ["uber"]
    # A comment is smaller than a post but it is still someone talking, and
    # the crest is how a reader knows who is worth listening to.
    assert item["author"]["rank_icon_key"], "a comment author has no rank to draw"


# --------------------------------------------------------------------------
# the count
# --------------------------------------------------------------------------

def test_the_feed_carries_a_comment_count(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    assert _comments(client, author, post["id"])["comment_count"] == 0

    _comment(client, author, post["id"], "one")
    _comment(client, author, post["id"], "two")

    feed = client.get("/social/feed", params={"scope": "following"}, headers=_h(author))
    assert feed.status_code == 200, feed.text
    assert feed.json()["items"][0]["comment_count"] == 2


def test_a_single_post_reports_the_same_count_as_the_feed(app_env):
    """Two code paths for one number is how they come to disagree."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    _comment(client, author, post["id"], "one")

    from_feed = client.get("/social/feed", params={"scope": "following"},
                           headers=_h(author)).json()["items"][0]
    alone = client.get(f"/social/posts/{post['id']}", headers=_h(author)).json()["post"]
    assert alone["comment_count"] == from_feed["comment_count"] == 1


def test_a_deleted_comment_stops_being_counted(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    first = _comment(client, author, post["id"], "one")
    _comment(client, author, post["id"], "two")

    res = client.delete(f"/social/comments/{first['comment']['id']}", headers=_h(author))
    assert res.status_code == 200, res.text
    assert res.json()["comment_count"] == 1
    assert len(_comments(client, author, post["id"])["items"]) == 1


# --------------------------------------------------------------------------
# who may delete
# --------------------------------------------------------------------------

def test_you_can_delete_your_own_comment(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author)
    mine = _comment(client, reader, post["id"], "mine")
    assert mine["comment"]["can_delete"] is True
    assert client.delete(f"/social/comments/{mine['comment']['id']}",
                         headers=_h(reader)).status_code == 200


def test_a_post_owner_can_remove_a_reply_under_their_own_post(app_env):
    """They are the one living with what appears under their photo."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author)
    theirs = _comment(client, reader, post["id"], "not mine")

    seen_by_owner = _comments(client, author, post["id"])["items"][0]
    assert seen_by_owner["mine"] is False
    assert seen_by_owner["can_delete"] is True
    assert client.delete(f"/social/comments/{theirs['comment']['id']}",
                         headers=_h(author)).status_code == 200


def test_a_stranger_can_delete_neither(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    other = _signup(client, "c@example.com", "Nobody")
    post = _post(client, author)
    theirs = _comment(client, reader, post["id"], "not yours")

    seen_by_stranger = _comments(client, other, post["id"])["items"][0]
    assert seen_by_stranger["can_delete"] is False
    assert client.delete(f"/social/comments/{theirs['comment']['id']}",
                         headers=_h(other)).status_code == 403


def test_deleting_a_comment_twice_is_a_404_not_a_500(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    mine = _comment(client, author, post["id"], "once")
    cid = mine["comment"]["id"]
    assert client.delete(f"/social/comments/{cid}", headers=_h(author)).status_code == 200
    assert client.delete(f"/social/comments/{cid}", headers=_h(author)).status_code == 404


# --------------------------------------------------------------------------
# blocking reaches the replies too
# --------------------------------------------------------------------------

def test_a_blocked_driver_s_comments_are_hidden_too(app_env):
    """A block that hides posts but shows replies is not a block."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    pest = _signup(client, "b@example.com", "Pest")
    post = _post(client, author)
    _comment(client, pest, post["id"], "unwanted")
    assert len(_comments(client, author, post["id"])["items"]) == 1

    assert client.post(f"/social/users/{pest['id']}/block",
                       headers=_h(author)).status_code == 200
    after = _comments(client, author, post["id"])
    assert after["items"] == []
    assert after["comment_count"] == 0, "the count still included a blocked author"


def test_a_blocked_driver_cannot_reply_to_you_at_all(app_env):
    """Otherwise a block still lets them put words under your photo."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    pest = _signup(client, "b@example.com", "Pest")
    post = _post(client, author)
    assert client.post(f"/social/users/{pest['id']}/block",
                       headers=_h(author)).status_code == 200
    res = client.post(f"/social/posts/{post['id']}/comments",
                      json={"body": "still here"}, headers=_h(pest))
    assert res.status_code == 404


def test_the_count_matches_what_opening_the_post_shows(app_env):
    """A count of 3 that opens to 1 reads as a bug, so both exclude the same
    people."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    friend = _signup(client, "b@example.com", "Friend")
    pest = _signup(client, "c@example.com", "Pest")
    post = _post(client, author)
    _comment(client, friend, post["id"], "useful")
    _comment(client, pest, post["id"], "noise")
    _comment(client, pest, post["id"], "more noise")

    assert client.post(f"/social/users/{pest['id']}/block",
                       headers=_h(author)).status_code == 200
    feed_item = client.get("/social/feed", params={"scope": "following"},
                           headers=_h(author)).json()["items"][0]
    opened = _comments(client, author, post["id"])
    assert feed_item["comment_count"] == len(opened["items"]) == 1


def test_a_muted_driver_s_comments_are_hidden_too(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    loud = _signup(client, "b@example.com", "Loud")
    post = _post(client, author)
    _comment(client, loud, post["id"], "chatter")
    assert client.post(f"/social/users/{loud['id']}/mute",
                       headers=_h(author)).status_code == 200
    assert _comments(client, author, post["id"])["items"] == []


def test_a_hidden_post_hides_its_comments_from_the_id_route(app_env):
    """Otherwise a post nobody can see in the feed is still readable by id."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author)
    _comment(client, author, post["id"], "hello")
    assert client.post(f"/social/users/{author['id']}/block",
                       headers=_h(reader)).status_code == 200
    res = client.get(f"/social/posts/{post['id']}/comments", headers=_h(reader))
    assert res.status_code == 404


# --------------------------------------------------------------------------
# reporting and moderating a reply
# --------------------------------------------------------------------------

def _admin(client):
    res = client.post("/auth/login", json={"email": "admin@example.com",
                                           "password": "password123"})
    assert res.status_code == 200, res.text
    return res.json()


def test_a_comment_can_be_reported(app_env):
    """Somewhere for people to write is somewhere people can be harassed."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    pest = _signup(client, "b@example.com", "Pest")
    post = _post(client, author)
    bad = _comment(client, pest, post["id"], "something vile")

    res = client.post("/social/reports", json={
        "target_type": "comment", "target_id": bad["comment"]["id"],
        "reason": "harassment"}, headers=_h(author))
    assert res.status_code == 200, res.text


def test_reporting_a_comment_that_is_not_there_is_a_404(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    res = client.post("/social/reports", json={
        "target_type": "comment", "target_id": 999999, "reason": "harassment"},
        headers=_h(author))
    assert res.status_code == 404


def test_a_moderator_can_hide_a_reply_and_it_closes_the_report(app_env):
    """The post's owner can already delete replies, but the person being
    harassed should not be the only line of defence."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    pest = _signup(client, "b@example.com", "Pest")
    post = _post(client, author)
    bad = _comment(client, pest, post["id"], "something vile")
    cid = bad["comment"]["id"]

    client.post("/social/reports", json={
        "target_type": "comment", "target_id": cid, "reason": "harassment"},
        headers=_h(author))
    admin = _admin(client)
    before = client.get("/admin/reports/count", headers=_h(admin)).json()["open_count"]
    assert before >= 1

    res = client.post(f"/admin/comments/{cid}/hide", json={"reason": "abuse"},
                      headers=_h(admin))
    assert res.status_code == 200, res.text
    assert res.json()["hidden"] is True
    assert res.json()["post_id"] == post["id"]

    # Gone for everyone, including the author of the post it was under.
    assert _comments(client, author, post["id"])["items"] == []
    assert _comments(client, pest, post["id"])["comment_count"] == 0
    after = client.get("/admin/reports/count", headers=_h(admin)).json()["open_count"]
    assert after == before - 1, "the report was left open after the reply was hidden"


def test_a_moderator_can_put_a_reply_back(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    mine = _comment(client, author, post["id"], "not actually bad")
    cid = mine["comment"]["id"]
    admin = _admin(client)

    client.post(f"/admin/comments/{cid}/hide", json={"reason": "mistake"}, headers=_h(admin))
    assert _comments(client, author, post["id"])["items"] == []
    res = client.post(f"/admin/comments/{cid}/unhide", headers=_h(admin))
    assert res.status_code == 200, res.text
    assert res.json()["hidden"] is False
    assert len(_comments(client, author, post["id"])["items"]) == 1


def test_hiding_a_reply_needs_an_admin(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    mine = _comment(client, author, post["id"], "mine")
    res = client.post(f"/admin/comments/{mine['comment']['id']}/hide",
                      json={"reason": "because"}, headers=_h(author))
    assert res.status_code in (401, 403)


# --------------------------------------------------------------------------
# replying to a reply
# --------------------------------------------------------------------------

def _reply(client, account, post_id, parent_id, body):
    res = client.post(f"/social/posts/{post_id}/comments",
                      json={"body": body, "parent_id": parent_id}, headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()


def test_a_reply_can_answer_another_reply(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author)

    top = _comment(client, reader, post["id"], "Which gate?")
    nested = _reply(client, author, post["id"], top["comment"]["id"], "Terminal 4.")

    assert nested["comment"]["parent_id"] == top["comment"]["id"]
    assert nested["comment"]["reply_to"]["display_name"] == "Reader"
    # Every reply counts, nested or not: the number under the post is how many
    # replies there are, not how many top-level ones.
    assert nested["comment_count"] == 2


def test_a_comment_with_no_parent_is_a_reply_to_the_post(app_env):
    """Which is every comment written before parent_id existed."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    out = _comment(client, author, post["id"], "plain")
    assert out["comment"]["parent_id"] is None
    assert out["comment"]["reply_to"] is None


def test_the_thread_carries_the_shape_back(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author)
    top = _comment(client, reader, post["id"], "Which gate?")
    _reply(client, author, post["id"], top["comment"]["id"], "Terminal 4.")

    items = _comments(client, author, post["id"])["items"]
    assert [c["parent_id"] for c in items] == [None, top["comment"]["id"]]
    assert items[1]["reply_to"]["user_id"] == items[0]["author"]["user_id"]
    assert items[1]["reply_to"]["display_name"] == "Reader"
    # Oldest first, still: a nested reply does not jump the queue.
    assert [c["body"] for c in items] == ["Which gate?", "Terminal 4."]


def test_a_reply_to_a_reply_to_a_reply_keeps_who_it_answered(app_env):
    """Depth is not clamped in the database -- the client draws two levels.

    Flattening here would lose which of the two a third-level reply answered,
    and that is the whole point of the @name on it.
    """
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author)
    top = _comment(client, reader, post["id"], "Which gate?")
    second = _reply(client, author, post["id"], top["comment"]["id"], "Terminal 4.")
    third = _reply(client, reader, post["id"], second["comment"]["id"], "Thanks.")

    assert third["comment"]["parent_id"] == second["comment"]["id"]
    assert third["comment"]["reply_to"]["display_name"] == "Author"


def test_a_parent_on_another_post_is_refused(app_env):
    """Otherwise a reply lands in a thread whose author cannot see where it came from."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    one = _post(client, author, "one")
    two = _post(client, author, "two")
    elsewhere = _comment(client, author, one["id"], "over here")

    res = client.post(f"/social/posts/{two['id']}/comments",
                      json={"body": "no", "parent_id": elsewhere["comment"]["id"]},
                      headers=_h(author))
    assert res.status_code == 404, res.text


def test_a_parent_that_does_not_exist_is_refused(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    res = client.post(f"/social/posts/{post['id']}/comments",
                      json={"body": "no", "parent_id": 999999}, headers=_h(author))
    assert res.status_code == 404, res.text


def test_a_deleted_parent_cannot_be_replied_to(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author)
    top = _comment(client, author, post["id"], "gone in a moment")
    assert client.delete(f"/social/comments/{top['comment']['id']}",
                         headers=_h(author)).status_code == 200

    res = client.post(f"/social/posts/{post['id']}/comments",
                      json={"body": "no", "parent_id": top["comment"]["id"]},
                      headers=_h(author))
    assert res.status_code == 404, res.text


def test_deleting_a_parent_leaves_its_replies_readable(app_env):
    """A reply whose parent went away still has to say who it answered.

    The name comes from the comment row, which is soft deleted, so it survives
    -- without that the @name on an orphan would silently vanish.
    """
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author)
    top = _comment(client, reader, post["id"], "Which gate?")
    _reply(client, author, post["id"], top["comment"]["id"], "Terminal 4.")
    assert client.delete(f"/social/comments/{top['comment']['id']}",
                         headers=_h(reader)).status_code == 200

    items = _comments(client, author, post["id"])["items"]
    assert [c["body"] for c in items] == ["Terminal 4."]
    assert items[0]["reply_to"]["display_name"] == "Reader"


# --------------------------------------------------------------------------
# deleting a post
# --------------------------------------------------------------------------

def test_an_author_can_delete_their_own_post(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    post = _post(client, author, "gone")

    assert client.delete(f"/social/posts/{post['id']}", headers=_h(author)).status_code == 200
    assert client.get(f"/social/posts/{post['id']}", headers=_h(author)).status_code == 404
    feed = client.get("/social/feed", params={"scope": "everyone"}, headers=_h(author))
    assert [p["id"] for p in feed.json()["items"]] == []


def test_somebody_else_s_post_is_not_yours_to_delete(app_env):
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author, "mine")

    assert client.delete(f"/social/posts/{post['id']}", headers=_h(reader)).status_code == 403
    assert client.get(f"/social/posts/{post['id']}", headers=_h(reader)).status_code == 200


def test_a_post_says_whether_it_is_yours(app_env):
    """The delete control is drawn from this, so it has to be right for both."""
    _main, client = app_env
    author = _signup(client, "a@example.com", "Author")
    reader = _signup(client, "b@example.com", "Reader")
    post = _post(client, author, "mine")

    assert client.get(f"/social/posts/{post['id']}",
                      headers=_h(author)).json()["post"]["mine"] is True
    assert client.get(f"/social/posts/{post['id']}",
                      headers=_h(reader)).json()["post"]["mine"] is False
