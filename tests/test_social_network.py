"""End-to-end tests for the driver network: posts, likes, follows, feeds.

These run the real app against a real (SQLite) database through TestClient, so
they exercise the routes, the schema and the access gate together. The things
worth testing here are the ones that are easy to get subtly wrong and hard to
notice:

  - a "following" feed that forgets your own posts,
  - a city feed that quietly widens to everybody when you have no city,
  - a deleted post that keeps serving its photo,
  - a follower count that changes when someone deletes a post,
  - and the private following count leaking to other people.
"""
from __future__ import annotations

import importlib
import io
import json
import os
import random
import struct
import sys
import tempfile
import zlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def app_env(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-social-")
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
        # social_identity must be reloaded with the rest: it holds its own
        # reference to core's _db_exec, so leaving it cached points this test at
        # the previous test's torn-down database.
        "social_db", "social_models", "social_service", "social_routes", "social_identity",
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

def _signup(client: TestClient, email: str, name: str, city: str | None = None) -> dict:
    body = {"email": email, "password": "password123", "display_name": name}
    if city is not None:
        body["city"] = city
    res = client.post("/auth/signup", json=body)
    assert res.status_code == 200, res.text
    return res.json()


def _h(account: dict) -> dict:
    return {"Authorization": f"Bearer {account['token']}"}


def _post(client: TestClient, account: dict, body: str, **kwargs) -> dict:
    res = client.post("/social/posts", json={"body": body, **kwargs}, headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()["post"]


def _feed(client: TestClient, account: dict, scope: str = "following", **params) -> dict:
    res = client.get("/social/feed", params={"scope": scope, **params}, headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()


def _png_bytes(width: int = 24, height: int = 24) -> bytes:
    """A minimal valid PNG, built by hand so the tests need no image library."""
    def chunk(tag: bytes, payload: bytes) -> bytes:
        return (struct.pack(">I", len(payload)) + tag + payload
                + struct.pack(">I", zlib.crc32(tag + payload) & 0xFFFFFFFF))

    raw = b"".join(b"\x00" + bytes([(x * 11 + y * 7) % 256 for x in range(width * 3)])
                   for y in range(height))
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw))
            + chunk(b"IEND", b""))


# --------------------------------------------------------------------------
# posting
# --------------------------------------------------------------------------

def test_post_requires_text_or_photo(app_env):
    _main, client = app_env
    driver = _signup(client, "a@example.com", "Ana")
    res = client.post("/social/posts", json={"body": "   "}, headers=_h(driver))
    assert res.status_code == 400
    assert "text or a photo" in res.json()["detail"]


def test_post_appears_in_own_following_feed(app_env):
    """Your own timeline must contain your own posts.

    This is the first thing anyone checks after tapping Post, and a feed that
    only shows other people looks like the post failed.
    """
    _main, client = app_env
    driver = _signup(client, "a@example.com", "Ana")
    created = _post(client, driver, "Queue at LGA is 40 minutes")

    feed = _feed(client, driver, "following")
    assert [item["id"] for item in feed["items"]] == [created["id"]]
    assert feed["items"][0]["mine"] is True


def test_following_feed_includes_followed_drivers_only(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    cal = _signup(client, "c@example.com", "Cal")

    ben_post = _post(client, ben, "Stadium letting out")
    _post(client, cal, "Nothing happening here")

    assert client.post(f"/social/users/{ben['id']}/follow", headers=_h(ana)).status_code == 200

    ids = [item["id"] for item in _feed(client, ana, "following")["items"]]
    assert ids == [ben_post["id"]]


def test_everyone_feed_shows_all_live_posts(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    first = _post(client, ana, "one")
    second = _post(client, ben, "two")

    ids = [item["id"] for item in _feed(client, ana, "everyone")["items"]]
    assert ids == [second["id"], first["id"]], "newest first"


# --------------------------------------------------------------------------
# city
# --------------------------------------------------------------------------

def test_city_feed_is_empty_rather_than_global_without_a_city(app_env):
    """The failure mode worth guarding: showing a driver everyone's posts and
    calling it their city."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")          # no city
    ben = _signup(client, "b@example.com", "Ben", city="Houston, TX")
    _post(client, ben, "Galleria is dead")

    feed = _feed(client, ana, "city")
    assert feed["items"] == []


def test_city_feed_matches_on_a_normalized_key(app_env):
    """"Houston, TX" and "houston tx" are one room, not two."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana", city="Houston, TX")
    ben = _signup(client, "b@example.com", "Ben", city="  houston   tx ")
    phx = _signup(client, "c@example.com", "Cid", city="Phoenix, AZ")

    ben_post = _post(client, ben, "IAH queue moving")
    _post(client, phx, "Sky Harbor is slow")

    ids = [item["id"] for item in _feed(client, ana, "city")["items"]]
    assert ids == [ben_post["id"]]


def test_city_on_a_post_is_frozen_at_write_time(app_env):
    """A driver who moves does not drag their old posts to the new city."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana", city="Houston, TX")
    houston_local = _signup(client, "h@example.com", "Hal", city="Houston, TX")
    phoenix_local = _signup(client, "p@example.com", "Pia", city="Phoenix, AZ")

    houston_post = _post(client, ana, "posted while in Houston")
    assert client.post("/social/me/city", json={"city": "Phoenix, AZ"},
                       headers=_h(ana)).status_code == 200
    phoenix_post = _post(client, ana, "posted after moving")

    houston_ids = [i["id"] for i in _feed(client, houston_local, "city")["items"]]
    phoenix_ids = [i["id"] for i in _feed(client, phoenix_local, "city")["items"]]
    assert houston_post["id"] in houston_ids
    assert houston_post["id"] not in phoenix_ids
    assert phoenix_post["id"] in phoenix_ids


# --------------------------------------------------------------------------
# likes
# --------------------------------------------------------------------------

def test_like_is_idempotent_and_reversible(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    post = _post(client, ana, "worth a like")

    first = client.post(f"/social/posts/{post['id']}/like", headers=_h(ana))
    second = client.post(f"/social/posts/{post['id']}/like", headers=_h(ana))
    assert first.status_code == 200 and second.status_code == 200
    # A double-tap on a flaky connection must not become a 500 or a double count
    assert second.json()["like_count"] == 1
    assert second.json()["liked_by_me"] is True

    removed = client.delete(f"/social/posts/{post['id']}/like", headers=_h(ana))
    assert removed.json() == {"ok": True, "post_id": post["id"],
                              "like_count": 0, "liked_by_me": False}


def test_like_state_is_per_viewer(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    post = _post(client, ana, "hello")
    client.post(f"/social/posts/{post['id']}/like", headers=_h(ben))

    ana_view = _feed(client, ana, "everyone")["items"][0]
    ben_view = _feed(client, ben, "everyone")["items"][0]
    assert ana_view["like_count"] == 1 and ana_view["liked_by_me"] is False
    assert ben_view["like_count"] == 1 and ben_view["liked_by_me"] is True


def test_cannot_like_a_deleted_post(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    post = _post(client, ana, "temporary")
    assert client.delete(f"/social/posts/{post['id']}", headers=_h(ana)).status_code == 200
    assert client.post(f"/social/posts/{post['id']}/like", headers=_h(ana)).status_code == 404


# --------------------------------------------------------------------------
# deleting
# --------------------------------------------------------------------------

def test_only_the_author_can_delete(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    post = _post(client, ana, "mine")

    assert client.delete(f"/social/posts/{post['id']}", headers=_h(ben)).status_code == 403
    assert client.delete(f"/social/posts/{post['id']}", headers=_h(ana)).status_code == 200


def test_deleted_post_leaves_every_feed(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana", city="Austin, TX")
    keep = _post(client, ana, "keep")
    drop = _post(client, ana, "drop")
    client.delete(f"/social/posts/{drop['id']}", headers=_h(ana))

    for scope in ("following", "city", "everyone"):
        ids = [i["id"] for i in _feed(client, ana, scope)["items"]]
        assert drop["id"] not in ids, scope
        assert keep["id"] in ids, scope
    assert client.get(f"/social/posts/{drop['id']}", headers=_h(ana)).status_code == 404


# --------------------------------------------------------------------------
# photos
# --------------------------------------------------------------------------

def test_photo_post_serves_its_image_and_stops_after_delete(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")

    res = client.post(
        "/social/posts/photo",
        files={"file": ("queue.png", io.BytesIO(_png_bytes()), "image/png")},
        data={"body": "the line at 3am"},
        headers=_h(ana),
    )
    assert res.status_code == 200, res.text
    post = res.json()["post"]
    assert post["body"] == "the line at 3am"
    assert post["image_url"] == f"/social/posts/{post['id']}/image"

    served = client.get(post["image_url"], headers=_h(ana))
    assert served.status_code == 200
    assert served.content.startswith(b"\x89PNG")

    # A thumb URL is always offered; it falls back to the original when no
    # thumbnail could be built, so it must never 404 while the post lives.
    assert client.get(post["image_thumb_url"], headers=_h(ana)).status_code == 200

    assert client.delete(f"/social/posts/{post['id']}", headers=_h(ana)).status_code == 200
    assert client.get(post["image_url"], headers=_h(ana)).status_code == 404


def test_photo_post_needs_no_caption(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.post(
        "/social/posts/photo",
        files={"file": ("q.png", io.BytesIO(_png_bytes()), "image/png")},
        headers=_h(ana),
    )
    assert res.status_code == 200, res.text
    assert res.json()["post"]["body"] == ""


def test_rejected_upload_leaves_no_post_behind(app_env):
    """The image is validated before anything is inserted."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    res = client.post(
        "/social/posts/photo",
        files={"file": ("notes.txt", io.BytesIO(b"not an image"), "text/plain")},
        headers=_h(ana),
    )
    assert res.status_code == 400
    assert _feed(client, ana, "everyone")["items"] == []


# --------------------------------------------------------------------------
# follows and profiles
# --------------------------------------------------------------------------

def test_follow_is_idempotent_and_cannot_target_yourself(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")

    assert client.post(f"/social/users/{ana['id']}/follow", headers=_h(ana)).status_code == 400
    first = client.post(f"/social/users/{ben['id']}/follow", headers=_h(ana))
    second = client.post(f"/social/users/{ben['id']}/follow", headers=_h(ana))
    assert second.json()["follower_count"] == 1, "following twice is still one follower"
    assert first.json()["following"] is True

    undone = client.delete(f"/social/users/{ben['id']}/follow", headers=_h(ana))
    assert undone.json()["following"] is False
    assert undone.json()["follower_count"] == 0


def test_following_count_is_private_but_follower_count_is_not(app_env):
    """The product decision: how many people YOU follow is yours alone."""
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    ben = _signup(client, "b@example.com", "Ben")
    cal = _signup(client, "c@example.com", "Cal")

    client.post(f"/social/users/{ben['id']}/follow", headers=_h(ana))
    client.post(f"/social/users/{cal['id']}/follow", headers=_h(ana))
    client.post(f"/social/users/{ana['id']}/follow", headers=_h(ben))

    mine = client.get("/social/me/profile", headers=_h(ana)).json()["profile"]
    assert mine["following_count"] == 2
    assert mine["follower_count"] == 1
    assert mine["is_me"] is True

    theirs = client.get(f"/social/users/{ana['id']}/profile", headers=_h(ben)).json()["profile"]
    assert theirs["following_count"] is None, "must not leak to other drivers"
    assert theirs["follower_count"] == 1
    assert theirs["followed_by_me"] is True
    assert theirs["is_me"] is False


def test_profile_post_count_ignores_deleted_posts(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    _post(client, ana, "one")
    gone = _post(client, ana, "two")
    client.delete(f"/social/posts/{gone['id']}", headers=_h(ana))

    profile = client.get("/social/me/profile", headers=_h(ana)).json()["profile"]
    assert profile["post_count"] == 1


def test_profile_of_unknown_driver_is_404(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    assert client.get("/social/users/999999/profile", headers=_h(ana)).status_code == 404


# --------------------------------------------------------------------------
# paging
# --------------------------------------------------------------------------

def test_pagination_walks_the_whole_feed_without_gaps_or_repeats(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    created = [_post(client, ana, f"post {i}")["id"] for i in range(7)]

    seen: list[int] = []
    before = None
    for _ in range(10):
        page = _feed(client, ana, "everyone", limit=3, **({"before_id": before} if before else {}))
        seen.extend(item["id"] for item in page["items"])
        before = page["next_before_id"]
        if not before:
            break

    assert seen == sorted(created, reverse=True)
    assert len(seen) == len(set(seen)), "no post appears twice"


def test_last_page_reports_no_further_cursor(app_env):
    _main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    for i in range(3):
        _post(client, ana, f"p{i}")
    page = _feed(client, ana, "everyone", limit=10)
    assert len(page["items"]) == 3
    assert page["next_before_id"] is None


# --------------------------------------------------------------------------
# access
# --------------------------------------------------------------------------

def test_every_social_route_requires_authentication(app_env):
    _main, client = app_env
    for method, path in [
        ("get", "/social/feed"),
        ("post", "/social/posts"),
        ("get", "/social/me/profile"),
        ("get", "/social/users/1/profile"),
        ("post", "/social/users/1/follow"),
        ("post", "/social/posts/1/like"),
        ("get", "/social/posts/1/image"),
    ]:
        res = getattr(client, method)(path, **({"json": {}} if method == "post" else {}))
        assert res.status_code in (401, 403), f"{method.upper()} {path} -> {res.status_code}"


def _photographic_png(width: int = 1400, height: int = 1000) -> bytes:
    """A large image that actually produces a smaller JPEG thumbnail.

    Two traps this avoids, both of which make `build_thumbnail` return None
    while the test still looks like it exercised the success path:

      - a flat or solid image compresses so well that ANY thumbnail is larger,
      - pixel-level random noise is adversarial to JPEG specifically.

    Upscaling a small random block gives mid-frequency texture, which is what
    real photographs look like to a DCT.
    """
    from PIL import Image

    rng = random.Random(20260911)
    seed = Image.new("RGB", (width // 16, height // 16))
    seed.putdata([
        (rng.randrange(256), rng.randrange(256), rng.randrange(256))
        for _ in range(seed.width * seed.height)
    ])
    buffer = io.BytesIO()
    seed.resize((width, height), Image.Resampling.BICUBIC).save(buffer, format="PNG")
    return buffer.getvalue()


def test_large_photo_really_gets_a_smaller_thumbnail(app_env):
    """Guards the thumbnail path itself, not just its fallback.

    The other photo test uses a 24px image, which is below THUMB_MAX_EDGE and so
    correctly gets no thumbnail at all -- it proves the fallback, not the
    feature. This one proves a thumbnail is built, stored, served as JPEG, and
    is genuinely smaller than what it replaces.
    """
    main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")
    original = _photographic_png()

    res = client.post(
        "/social/posts/photo",
        files={"file": ("big.png", io.BytesIO(original), "image/png")},
        headers=_h(ana),
    )
    assert res.status_code == 200, res.text
    post = res.json()["post"]

    assert post["image_thumb_url"] == f"/social/posts/{post['id']}/image/thumb", (
        "a real thumbnail should be advertised at the thumb URL, not fall back")
    row = main._db_query_one("SELECT has_thumb FROM posts WHERE id=?", (int(post["id"]),))
    assert bool(row["has_thumb"]) is True

    thumb = client.get(post["image_thumb_url"], headers=_h(ana))
    full = client.get(post["image_url"], headers=_h(ana))
    assert thumb.status_code == 200 and full.status_code == 200
    assert thumb.headers["content-type"].startswith("image/jpeg")
    assert thumb.content.startswith(b"\xff\xd8"), "JPEG magic"
    assert len(thumb.content) < len(full.content), (
        f"thumbnail {len(thumb.content)}B is not smaller than original {len(full.content)}B")


def test_two_posts_in_the_same_second_keep_their_own_photos(app_env):
    """Guards the insert-id race.

    The id has to come from the INSERT itself. Re-reading "newest row for this
    user at this timestamp" looks correct until two posts land in the same
    second -- then the second upload can overwrite the first post's photo and
    one of them silently loses it.
    """
    main, client = app_env
    ana = _signup(client, "a@example.com", "Ana")

    posts = []
    for _ in range(4):
        res = client.post(
            "/social/posts/photo",
            files={"file": ("q.png", io.BytesIO(_png_bytes()), "image/png")},
            headers=_h(ana),
        )
        assert res.status_code == 200, res.text
        posts.append(res.json()["post"])

    ids = [p["id"] for p in posts]
    assert len(set(ids)) == len(ids), "each post got its own id"

    paths = [
        main._db_query_one("SELECT image_path FROM posts WHERE id=?", (int(pid),))["image_path"]
        for pid in ids
    ]
    assert all(paths), "every post kept a photo"
    assert len(set(paths)) == len(paths), "no two posts share one image file"
    for pid in ids:
        assert client.get(f"/social/posts/{pid}/image", headers=_h(ana)).status_code == 200
