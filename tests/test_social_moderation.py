"""Block, mute, report — and the queue behind the report button.

The tests concentrate on the ways a moderation feature is usually broken rather
than absent:

  - a block that filters the feed but still serves the photo to anyone with
    the URL, which is a hidden link and not a block,
  - a block that only works one way, so the person blocked still reads
    everything you write,
  - a block you can detect, which turns it into a notification,
  - a mute that leaks, so the muted person can tell,
  - a report button with nothing behind it,
  - and a post hidden by a moderator whose reports stay in the queue anyway.
"""
from __future__ import annotations

import importlib
import io
import json
import os
import struct
import sys
import tempfile
import zlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def app_env(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-moderation-")
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


def _admin(main, client: TestClient) -> dict:
    account = _signup(client, "boss@example.com", "Boss")
    main._db_exec("UPDATE users SET is_admin=1 WHERE id=?", (int(account["id"]),))
    return account


def _h(a: dict) -> dict:
    return {"Authorization": f"Bearer {a['token']}"}


def _post(client: TestClient, a: dict, body: str) -> dict:
    res = client.post("/social/posts", json={"body": body}, headers=_h(a))
    assert res.status_code == 200, res.text
    return res.json()["post"]


def _feed_ids(client: TestClient, a: dict, scope: str = "everyone") -> list:
    res = client.get("/social/feed", params={"scope": scope}, headers=_h(a))
    assert res.status_code == 200, res.text
    return [i["id"] for i in res.json()["items"]]


def _png() -> bytes:
    def chunk(tag: bytes, payload: bytes) -> bytes:
        return (struct.pack(">I", len(payload)) + tag + payload
                + struct.pack(">I", zlib.crc32(tag + payload) & 0xFFFFFFFF))
    w = h = 16
    raw = b"".join(b"\x00" + bytes((x * 7) % 256 for x in range(w * 3)) for _ in range(h))
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b""))


# --------------------------------------------------------------------------
# blocking
# --------------------------------------------------------------------------

def test_block_hides_posts_in_both_directions(app_env):
    """A one-way block still lets the blocked person read everything you write,
    which is not what anyone means by blocking."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    ana_post = _post(client, ana, "from ana")
    ben_post = _post(client, ben, "from ben")

    assert client.post(f"/social/users/{ben['id']}/block",
                       headers=_h(ana)).status_code == 200

    assert ben_post["id"] not in _feed_ids(client, ana)
    assert ana_post["id"] not in _feed_ids(client, ben), "the block must cut both ways"
    assert ana_post["id"] in _feed_ids(client, ana), "your own posts are unaffected"


def test_block_severs_the_follows_in_both_directions(app_env):
    """Leaving them means a blocked person still shows in your follower count
    and still receives your posts."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    client.post(f"/social/users/{ben['id']}/follow", headers=_h(ana))
    client.post(f"/social/users/{ana['id']}/follow", headers=_h(ben))

    client.post(f"/social/users/{ben['id']}/block", headers=_h(ana))

    mine = client.get("/social/me/profile", headers=_h(ana)).json()["profile"]
    theirs = client.get("/social/me/profile", headers=_h(ben)).json()["profile"]
    assert mine["follower_count"] == 0 and mine["following_count"] == 0
    assert theirs["follower_count"] == 0 and theirs["following_count"] == 0


def test_a_blocked_profile_reads_as_not_found(app_env):
    """403 would say "this person blocked you", which turns a block into a
    notification."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    client.post(f"/social/users/{ben['id']}/block", headers=_h(ana))

    assert client.get(f"/social/users/{ben['id']}/profile",
                      headers=_h(ana)).status_code == 404
    assert client.get(f"/social/users/{ana['id']}/profile",
                      headers=_h(ben)).status_code == 404


def test_neither_side_can_follow_across_a_block(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    client.post(f"/social/users/{ben['id']}/block", headers=_h(ana))

    assert client.post(f"/social/users/{ben['id']}/follow",
                       headers=_h(ana)).status_code == 403
    assert client.post(f"/social/users/{ana['id']}/follow",
                       headers=_h(ben)).status_code == 403


def test_a_blocked_authors_photo_stops_being_served(app_env):
    """Filtering the feed while still serving the bytes to anyone holding the
    URL is a hidden link, not a block."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    res = client.post("/social/posts/photo",
                      files={"file": ("q.png", io.BytesIO(_png()), "image/png")},
                      headers=_h(ben))
    assert res.status_code == 200, res.text
    url = res.json()["post"]["image_url"]
    assert client.get(url, headers=_h(ana)).status_code == 200

    client.post(f"/social/users/{ben['id']}/block", headers=_h(ana))
    assert client.get(url, headers=_h(ana)).status_code == 404
    assert client.get(url, headers=_h(ben)).status_code == 200, "the author still sees it"


def test_unblocking_restores_visibility_but_not_the_follows(app_env):
    """Undoing a block is not the same as asking for the relationship back."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    client.post(f"/social/users/{ben['id']}/follow", headers=_h(ana))
    ben_post = _post(client, ben, "from ben")

    client.post(f"/social/users/{ben['id']}/block", headers=_h(ana))
    client.delete(f"/social/users/{ben['id']}/block", headers=_h(ana))

    assert ben_post["id"] in _feed_ids(client, ana)
    assert client.get("/social/me/profile",
                      headers=_h(ana)).json()["profile"]["following_count"] == 0


def test_block_is_idempotent_and_cannot_target_yourself(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    assert client.post(f"/social/users/{ana['id']}/block",
                       headers=_h(ana)).status_code == 400
    for _ in range(2):
        assert client.post(f"/social/users/{ben['id']}/block",
                           headers=_h(ana)).status_code == 200
    assert len(client.get("/social/me/blocked", headers=_h(ana)).json()["items"]) == 1


# --------------------------------------------------------------------------
# muting
# --------------------------------------------------------------------------

def test_mute_is_one_way_and_invisible_to_the_muted(app_env):
    """Offering only the nuclear option means people use the nuclear option."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    ana_post = _post(client, ana, "from ana")
    ben_post = _post(client, ben, "from ben")

    client.post(f"/social/users/{ben['id']}/mute", headers=_h(ana))

    assert ben_post["id"] not in _feed_ids(client, ana)
    assert ana_post["id"] in _feed_ids(client, ben), "muting is not visible to them"
    # and unlike a block, nothing structural changed
    assert client.get(f"/social/users/{ben['id']}/profile",
                      headers=_h(ana)).status_code == 200
    assert client.post(f"/social/users/{ana['id']}/follow",
                       headers=_h(ben)).status_code == 200


def test_unmute_brings_them_back(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    ben_post = _post(client, ben, "from ben")
    client.post(f"/social/users/{ben['id']}/mute", headers=_h(ana))
    client.delete(f"/social/users/{ben['id']}/mute", headers=_h(ana))
    assert ben_post["id"] in _feed_ids(client, ana)


def test_blocked_and_muted_lists_are_readable(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    cal = _signup(client, "c@example.com", "Cal")
    client.post(f"/social/users/{ben['id']}/block", headers=_h(ana))
    client.post(f"/social/users/{cal['id']}/mute", headers=_h(ana))

    blocked = client.get("/social/me/blocked", headers=_h(ana)).json()["items"]
    muted = client.get("/social/me/muted", headers=_h(ana)).json()["items"]
    assert [b["user_id"] for b in blocked] == [ben["id"]]
    assert [m["user_id"] for m in muted] == [cal["id"]]
    assert blocked[0]["handle"], "a handle, so the list is recognisable"


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------

def test_reporting_a_post_queues_it(app_env):
    main, client = app_env
    boss = _admin(main, client)
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    post = _post(client, ben, "something bad")

    res = client.post("/social/reports", json={
        "target_type": "post", "target_id": post["id"],
        "reason": "harassment", "note": "  aimed   at me  ",
    }, headers=_h(ana))
    assert res.status_code == 200, res.text
    assert res.json()["duplicate"] is False

    queue = client.get("/admin/reports", headers=_h(boss)).json()
    assert queue["open_count"] == 1
    item = queue["items"][0]
    assert item["target_type"] == "post" and item["target_id"] == post["id"]
    assert item["target_user_id"] == ben["id"], "grouped by who it is about"
    assert item["reporter_id"] == ana["id"]
    assert item["note"] == "aimed at me", "whitespace collapsed"
    assert item["status"] == "open"


def test_reporting_twice_does_not_flood_the_queue(app_env):
    """A frustrated person taps Report ten times; the queue must survive it."""
    main, client = app_env
    boss = _admin(main, client)
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    post = _post(client, ben, "something bad")

    first = client.post("/social/reports", json={
        "target_type": "post", "target_id": post["id"], "reason": "spam"}, headers=_h(ana))
    again = client.post("/social/reports", json={
        "target_type": "post", "target_id": post["id"], "reason": "spam"}, headers=_h(ana))
    assert again.status_code == 200, "not an error — tapping twice is not a mistake"
    assert again.json()["duplicate"] is True
    assert again.json()["report_id"] == first.json()["report_id"]
    assert client.get("/admin/reports", headers=_h(boss)).json()["open_count"] == 1


def test_two_people_reporting_the_same_post_are_two_reports(app_env):
    main, client = app_env
    boss = _admin(main, client)
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    cal = _signup(client, "c@example.com", "Cal")
    post = _post(client, cal, "something bad")

    for who in (ana, ben):
        client.post("/social/reports", json={
            "target_type": "post", "target_id": post["id"], "reason": "spam"}, headers=_h(who))
    assert client.get("/admin/reports", headers=_h(boss)).json()["open_count"] == 2


def test_you_cannot_report_your_own_content(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    post = _post(client, ana, "mine")
    assert client.post("/social/reports", json={
        "target_type": "post", "target_id": post["id"],
        "reason": "spam"}, headers=_h(ana)).status_code == 400
    assert client.post("/social/reports", json={
        "target_type": "user", "target_id": ana["id"],
        "reason": "spam"}, headers=_h(ana)).status_code == 400


def test_unknown_reasons_and_targets_are_refused(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    assert client.post("/social/reports", json={
        "target_type": "post", "target_id": 1,
        "reason": "i_dislike_them"}, headers=_h(ana)).status_code == 400
    assert client.post("/social/reports", json={
        "target_type": "planet", "target_id": 1,
        "reason": "spam"}, headers=_h(ana)).status_code == 400


def test_reporting_a_missing_post_is_a_404(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    assert client.post("/social/reports", json={
        "target_type": "post", "target_id": 999999,
        "reason": "spam"}, headers=_h(ana)).status_code == 404


def test_report_options_are_served(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.get("/social/reports/options", headers=_h(ana)).json()
    assert "harassment" in res["reasons"] and "personal_info" in res["reasons"]
    assert set(res["targets"]) == {"post", "user", "chat_message"}


# --------------------------------------------------------------------------
# working the queue
# --------------------------------------------------------------------------

def test_hiding_a_post_removes_it_and_closes_its_reports(app_env):
    """A post hidden while its reports sit in the queue is how a queue stops
    being trustworthy."""
    main, client = app_env
    boss = _admin(main, client)
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    post = _post(client, ben, "something bad")
    client.post("/social/reports", json={
        "target_type": "post", "target_id": post["id"], "reason": "hate"}, headers=_h(ana))

    res = client.post(f"/admin/posts/{post['id']}/hide",
                      json={"reason": "slur"}, headers=_h(boss))
    assert res.status_code == 200 and res.json()["hidden"] is True

    assert post["id"] not in _feed_ids(client, ana)
    assert post["id"] not in _feed_ids(client, ben), "hidden from its author too"
    assert client.get(f"/social/posts/{post['id']}", headers=_h(ana)).status_code == 404
    assert client.get("/admin/reports", headers=_h(boss)).json()["open_count"] == 0


def test_hiding_is_distinct_from_the_author_deleting(app_env):
    """"Why did my post vanish" needs an answer, so the two are separate columns."""
    main, client = app_env
    boss = _admin(main, client)
    ben = _signup(client, "b@example.com", "Ben")
    post = _post(client, ben, "something bad")
    client.post(f"/admin/posts/{post['id']}/hide", json={"reason": "slur"}, headers=_h(boss))

    row = main._db_query_one(
        "SELECT deleted_at, hidden_at, hidden_by, hidden_reason FROM posts WHERE id=?",
        (int(post["id"]),))
    assert row["deleted_at"] is None
    assert row["hidden_at"] and int(row["hidden_by"]) == int(boss["id"])
    assert row["hidden_reason"] == "slur"


def test_unhiding_puts_a_post_back(app_env):
    main, client = app_env
    boss = _admin(main, client)
    ben = _signup(client, "b@example.com", "Ben")
    post = _post(client, ben, "a misunderstanding")
    client.post(f"/admin/posts/{post['id']}/hide", json={}, headers=_h(boss))
    assert client.post(f"/admin/posts/{post['id']}/unhide",
                       headers=_h(boss)).json()["hidden"] is False
    assert post["id"] in _feed_ids(client, ben)


def test_a_report_can_be_dismissed(app_env):
    main, client = app_env
    boss = _admin(main, client)
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    post = _post(client, ben, "fine actually")
    rid = client.post("/social/reports", json={
        "target_type": "post", "target_id": post["id"],
        "reason": "spam"}, headers=_h(ana)).json()["report_id"]

    res = client.post(f"/admin/reports/{rid}/resolve",
                      json={"status": "dismissed", "resolution": "not spam"},
                      headers=_h(boss))
    assert res.status_code == 200 and res.json()["status"] == "dismissed"
    assert client.get("/admin/reports", headers=_h(boss)).json()["open_count"] == 0
    assert post["id"] in _feed_ids(client, ana), "a dismissed report changes nothing"

    history = client.get("/admin/reports", params={"status": ""}, headers=_h(boss)).json()
    assert history["items"][0]["resolution"] == "not spam"
    assert history["items"][0]["resolved_by"] == boss["id"]


def test_a_report_cannot_be_resolved_to_a_nonsense_status(app_env):
    main, client = app_env
    boss = _admin(main, client)
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    post = _post(client, ben, "x")
    rid = client.post("/social/reports", json={
        "target_type": "post", "target_id": post["id"],
        "reason": "spam"}, headers=_h(ana)).json()["report_id"]
    assert client.post(f"/admin/reports/{rid}/resolve", json={"status": "open"},
                       headers=_h(boss)).status_code == 400


def test_the_queue_is_admin_only(app_env):
    """The whole point is that it is not self-service."""
    main, client = app_env
    _admin(main, client)
    ana = _signup(client, "a@example.com", "Ana")
    for method, path in [
        ("get", "/admin/reports"),
        ("get", "/admin/reports/count"),
        ("post", "/admin/reports/1/resolve"),
        ("post", "/admin/posts/1/hide"),
        ("post", "/admin/posts/1/unhide"),
    ]:
        res = getattr(client, method)(
            path, headers=_h(ana), **({"json": {"status": "dismissed"}} if method == "post" else {}))
        assert res.status_code == 403, f"{method.upper()} {path} -> {res.status_code}"


def test_moderation_routes_require_authentication(app_env):
    _main, client = app_env
    for method, path in [
        ("post", "/social/users/1/block"),
        ("post", "/social/users/1/mute"),
        ("get", "/social/me/blocked"),
        ("get", "/social/me/muted"),
        ("post", "/social/reports"),
        ("get", "/social/reports/options"),
    ]:
        res = getattr(client, method)(path, **({"json": {}} if method == "post" else {}))
        assert res.status_code in (401, 403, 422), f"{method.upper()} {path} -> {res.status_code}"
