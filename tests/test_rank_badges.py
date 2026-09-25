"""The rank badges, stored in the database.

The ladder is ten prestiges of three ranks each, so a badge is one image per
band: band_001 through band_030, and band_005 is prestige 2 rank 2. They live
in the database rather than in the frontend repo, which means new artwork
ships by being uploaded rather than deployed, and no image files enter git.

What is worth testing here is not that an upload round-trips -- it is the
parts that fail quietly if they are wrong:

  * the key is checked against the ladder, so a typo cannot create a badge
    nobody will ever be served;
  * the content type comes from DECODING the bytes, not from what the
    uploader claimed, because a browser handed the wrong type for an image
    renders nothing and reports nothing;
  * the version is the sha256 of the bytes, which is what makes the
    year-long immutable cache safe -- change the artwork and the URL changes
    with it;
  * the manifest never carries the bytes, because a full set of artwork is
    megabytes that a list of ranks does not need;
  * uploading is admin-only, and reading is not;
  * the range named in the refusal message is read off the ladder, because a
    message that says band_050 when the ladder stops at band_030 sends whoever
    is uploading looking for a bug that is not there.
"""
from __future__ import annotations

import hashlib
import importlib
import io
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from PIL import Image


@pytest.fixture()
def app_env(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-rankbadge-")
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
        "social_moderation", "social_admin_routes", "rank_badge_store",
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


def _badge_bytes(fmt: str = "WEBP", size: int = 256, color=(200, 160, 60, 255)) -> bytes:
    img = Image.new("RGBA", (size, size), color)
    out = io.BytesIO()
    img.save(out, format=fmt)
    return out.getvalue()


def _signup(client: TestClient, email: str, name: str) -> dict:
    res = client.post("/auth/signup",
                      json={"email": email, "password": "password123", "display_name": name})
    assert res.status_code == 200, res.text
    return res.json()


def _h(account: dict) -> dict:
    return {"Authorization": f"Bearer {account['token']}"}


def _admin(client: TestClient) -> dict:
    res = client.post("/auth/login",
                      json={"email": "admin@example.com", "password": "password123"})
    assert res.status_code == 200, res.text
    return res.json()


def _upload(client, account, key, raw, filename="badge.webp"):
    return client.post(
        f"/admin/ranks/badge/{key}",
        headers=_h(account),
        files={"file": (filename, raw, "application/octet-stream")},
    )


# ------------------------------------------------------------------ storing

def test_a_badge_round_trips_through_the_database(app_env):
    _main, client = app_env
    admin = _admin(client)
    raw = _badge_bytes()

    res = _upload(client, admin, "band_024", raw)
    assert res.status_code == 200, res.text
    badge = res.json()["badge"]
    assert badge["rank_icon_key"] == "band_024"
    assert badge["width"] == 256 and badge["height"] == 256
    assert badge["byte_size"] == len(raw)

    got = client.get("/ranks/badge/band_024")
    assert got.status_code == 200
    assert got.content == raw, "the bytes served are not the bytes stored"


def test_the_version_is_the_digest_of_the_bytes(app_env):
    """This is what makes a year-long immutable cache safe. If the version
    were a timestamp or a counter, a re-upload of identical artwork would
    needlessly bust every cache; if it did not change with the bytes, new
    artwork would never reach anyone."""
    _main, client = app_env
    admin = _admin(client)
    raw = _badge_bytes()

    badge = _upload(client, admin, "band_001", raw).json()["badge"]
    assert badge["version"] == hashlib.sha256(raw).hexdigest()

    # Same bytes again -> same version, so caches are not disturbed.
    again = _upload(client, admin, "band_001", raw).json()["badge"]
    assert again["version"] == badge["version"]

    # Different artwork -> different version, so caches are.
    other = _badge_bytes(color=(20, 40, 90, 255))
    changed = _upload(client, admin, "band_001", other).json()["badge"]
    assert changed["version"] != badge["version"]


def test_re_uploading_replaces_rather_than_duplicates(app_env):
    _main, client = app_env
    admin = _admin(client)
    _upload(client, admin, "band_005", _badge_bytes())
    _upload(client, admin, "band_005", _badge_bytes(color=(9, 9, 9, 255)))

    manifest = client.get("/ranks/badges").json()
    keys = [item["rank_icon_key"] for item in manifest["items"]]
    assert keys.count("band_005") == 1, "a second upload created a second row"


# ------------------------------------------------------------- refusing bad

def test_a_key_the_ladder_does_not_define_is_refused(app_env):
    """A typo must not create a badge nobody will ever be served."""
    _main, client = app_env
    admin = _admin(client)
    for bad in ["band_031", "band_051", "band_100", "band_0", "band_34",
                "tier-4", "", "band_001; DROP TABLE"]:
        res = _upload(client, admin, bad or "_", _badge_bytes())
        assert res.status_code in (400, 404), f"{bad!r} was accepted: {res.text}"
        if res.status_code == 400:
            from leaderboard_service import RANK_BAND_COUNT
            assert f"band_{RANK_BAND_COUNT:03d}" in res.json()["detail"], (
                f"the refusal names a range the ladder does not have: {res.json()['detail']}")


def test_bytes_that_are_not_an_image_are_refused(app_env):
    _main, client = app_env
    admin = _admin(client)
    res = _upload(client, admin, "band_002", b"this is not an image at all")
    assert res.status_code == 400
    assert "image" in res.json()["detail"].lower()


def test_the_content_type_comes_from_the_bytes_not_the_filename(app_env):
    """A browser handed the wrong content type for an image renders nothing
    and says nothing, so the claim on the way in is never trusted."""
    _main, client = app_env
    admin = _admin(client)
    png = _badge_bytes(fmt="PNG")

    # Uploaded under a lying .webp name.
    badge = _upload(client, admin, "band_003", png, filename="badge.webp").json()["badge"]
    assert badge["content_type"] == "image/png"

    served = client.get("/ranks/badge/band_003")
    assert served.headers["content-type"].startswith("image/png")


def test_an_oversized_image_is_refused(app_env):
    _main, client = app_env
    admin = _admin(client)
    import rank_badge_store

    # Noise, because a flat colour compresses to almost nothing.
    huge = os.urandom(rank_badge_store.MAX_BADGE_BYTES + 1024)
    res = _upload(client, admin, "band_004", huge)
    assert res.status_code in (400, 413), res.text


def test_a_badge_that_is_too_small_to_read_is_refused(app_env):
    _main, client = app_env
    admin = _admin(client)
    res = _upload(client, admin, "band_005", _badge_bytes(size=8))
    assert res.status_code == 400
    assert "minimum" in res.json()["detail"].lower()


# -------------------------------------------------------------------- serve

def test_a_missing_badge_is_a_404_not_an_empty_image(app_env):
    _main, client = app_env
    res = client.get("/ranks/badge/band_099")
    assert res.status_code == 404


def test_the_badge_is_cacheable_and_revalidates(app_env):
    _main, client = app_env
    admin = _admin(client)
    _upload(client, admin, "band_010", _badge_bytes())

    res = client.get("/ranks/badge/band_010")
    assert "immutable" in res.headers["cache-control"]
    etag = res.headers["etag"]

    again = client.get("/ranks/badge/band_010", headers={"If-None-Match": etag})
    assert again.status_code == 304
    assert again.content == b"", "a 304 must not resend the artwork"


def test_the_badge_can_be_composed_onto_a_canvas(app_env):
    """The map draws badges with img.crossOrigin = "anonymous". Without these
    headers the browser fails the load silently and the marker falls back."""
    _main, client = app_env
    admin = _admin(client)
    _upload(client, admin, "band_011", _badge_bytes())
    res = client.get("/ranks/badge/band_011")
    assert res.headers.get("access-control-allow-origin") == "*"
    assert res.headers.get("cross-origin-resource-policy") == "cross-origin"


# ----------------------------------------------------------------- manifest

def test_the_manifest_carries_urls_but_never_the_artwork(app_env):
    """A hundred badges is megabytes a list of ranks does not need."""
    _main, client = app_env
    admin = _admin(client)
    raw = _badge_bytes()
    _upload(client, admin, "band_012", raw)

    body = client.get("/ranks/badges").json()
    item = next(i for i in body["items"] if i["rank_icon_key"] == "band_012")
    assert item["url"] == f"/ranks/badge/band_012?v={item['version']}"
    blob = json.dumps(body)
    assert "image_bytes" not in blob
    assert len(blob) < 50_000, "the manifest is carrying the artwork"


def test_the_manifest_says_how_much_of_the_ladder_is_dressed(app_env):
    """The set arrives a piece at a time; the useful question while it lands
    is which bands are still bare."""
    _main, client = app_env
    admin = _admin(client)

    empty = client.get("/ranks/badges").json()
    assert empty["expected_count"] == 30
    assert empty["count"] == 0
    assert empty["missing_count"] == 30
    assert empty["complete"] is False

    _upload(client, admin, "band_001", _badge_bytes())
    one = client.get("/ranks/badges").json()
    assert one["count"] == 1 and one["missing_count"] == 29

    coverage = client.get("/admin/ranks/badges/coverage", headers=_h(admin)).json()
    assert "band_002" in coverage["missing"]
    assert "band_001" not in coverage["missing"]


def test_the_manifest_is_ordered_by_rank(app_env):
    """band_002 must not sort after band_010, which is why the keys are
    zero-padded in the first place."""
    _main, client = app_env
    admin = _admin(client)
    for key in ["band_010", "band_002", "band_030", "band_001"]:
        _upload(client, admin, key, _badge_bytes())
    keys = [i["rank_icon_key"] for i in client.get("/ranks/badges").json()["items"]]
    assert keys == sorted(keys)
    assert keys == ["band_001", "band_002", "band_010", "band_030"]


# ---------------------------------------------------------------- authority

def test_only_an_admin_can_upload_a_badge(app_env):
    _main, client = app_env
    driver = _signup(client, "driver@example.com", "Driver")
    res = _upload(client, driver, "band_020", _badge_bytes())
    assert res.status_code in (401, 403), res.text
    assert client.get("/ranks/badge/band_020").status_code == 404


def test_a_signed_out_visitor_can_still_see_a_badge(app_env):
    """The artwork is public. A rank on a leaderboard should render for
    anyone who can see the leaderboard."""
    _main, client = app_env
    admin = _admin(client)
    _upload(client, admin, "band_021", _badge_bytes())
    assert client.get("/ranks/badge/band_021").status_code == 200
    assert client.get("/ranks/badges").status_code == 200


def test_only_an_admin_can_delete_a_badge(app_env):
    _main, client = app_env
    admin = _admin(client)
    driver = _signup(client, "driver2@example.com", "Driver Two")
    _upload(client, admin, "band_030", _badge_bytes())

    refused = client.delete("/admin/ranks/badge/band_030", headers=_h(driver))
    assert refused.status_code in (401, 403)
    assert client.get("/ranks/badge/band_030").status_code == 200

    removed = client.delete("/admin/ranks/badge/band_030", headers=_h(admin))
    assert removed.status_code == 200
    assert client.get("/ranks/badge/band_030").status_code == 404


# ----------------------------------------------------- ten prestiges of five

def test_the_ladder_is_ten_prestiges_of_three(app_env):
    _main, _client = app_env
    from leaderboard_service import (
        MAX_LEVEL, PRESTIGE_COUNT, RANKS_PER_PRESTIGE, RANK_BAND_COUNT,
        RANK_LADDER, get_rank_ladder,
    )

    assert PRESTIGE_COUNT == 10
    assert RANKS_PER_PRESTIGE == 3
    assert RANK_BAND_COUNT == PRESTIGE_COUNT * RANKS_PER_PRESTIGE == 30
    assert len(RANK_LADDER) == RANK_BAND_COUNT
    assert len(get_rank_ladder()) == RANK_BAND_COUNT

    # Every XP level belongs to exactly one band, with no gap and no overlap.
    rows = get_rank_ladder()
    assert rows[0]["start_level"] == 1
    assert rows[-1]["end_level"] == MAX_LEVEL
    for earlier, later in zip(rows, rows[1:]):
        assert later["start_level"] == earlier["end_level"] + 1


def test_finishing_a_prestige_rolls_into_the_next(app_env):
    """The last rank of a prestige is followed by the FIRST rank of the next,
    never an extra rank on the old one. That roll-over is the whole shape of
    the ladder, and it is derived here rather than written out: this shape has
    already changed three times."""
    _main, _client = app_env
    from leaderboard_service import (
        PRESTIGE_COUNT, RANKS_PER_PRESTIGE, RANK_BAND_COUNT,
        prestige_and_rank_for_band,
    )
    per = RANKS_PER_PRESTIGE

    assert prestige_and_rank_for_band(1) == {"band": 1, "prestige": 1, "rank": 1}
    assert prestige_and_rank_for_band(per) == {"band": per, "prestige": 1, "rank": per}
    assert prestige_and_rank_for_band(per + 1) == {
        "band": per + 1, "prestige": 2, "rank": 1}
    assert prestige_and_rank_for_band(RANK_BAND_COUNT) == {
        "band": RANK_BAND_COUNT, "prestige": PRESTIGE_COUNT, "rank": per}


def test_the_pair_and_the_band_agree_in_both_directions(app_env):
    _main, _client = app_env
    from leaderboard_service import (
        RANK_BAND_COUNT, band_index_for_prestige_and_rank, prestige_and_rank_for_band,
    )

    for band in range(1, RANK_BAND_COUNT + 1):
        pair = prestige_and_rank_for_band(band)
        assert band_index_for_prestige_and_rank(pair["prestige"], pair["rank"]) == band


def test_a_band_outside_the_ladder_clamps_to_a_real_rank(app_env):
    """A band outside the ladder means the ladder changed under stored data.
    A driver should see the nearest real rank, not an error."""
    _main, _client = app_env
    from leaderboard_service import RANK_BAND_COUNT, prestige_and_rank_for_band

    assert prestige_and_rank_for_band(0)["band"] == 1
    assert prestige_and_rank_for_band(-4)["prestige"] == 1
    assert prestige_and_rank_for_band(RANK_BAND_COUNT + 1)["band"] == RANK_BAND_COUNT
    assert prestige_and_rank_for_band(9999)["band"] == RANK_BAND_COUNT


def test_the_ladder_and_the_manifest_send_the_pair(app_env):
    """Four clients were each parsing the key to get back to the pair, and one
    printed the key itself when it could not."""
    _main, client = app_env
    from leaderboard_service import get_rank_ladder

    row = next(r for r in get_rank_ladder() if r["rank_icon_key"] == "band_005")
    assert (row["prestige"], row["rank"]) == (2, 2)

    admin = _admin(client)
    _upload(client, admin, "band_005", _badge_bytes())
    item = next(i for i in client.get("/ranks/badges").json()["items"]
                if i["rank_icon_key"] == "band_005")
    assert (item["prestige"], item["rank"]) == (2, 2)


# ------------------------------------------------------ the ladder has names

def test_the_ladder_names_every_rank_it_defines(app_env):
    """The backend owns the ladder, so it owns the names.

    This is the invariant that has broken on every reshape: the name table and
    the band count are two numbers that must agree, and nothing forced them to.
    A prestige added without a name, or a fourth rank added with only three
    numerals, would have sent a driver "None II" or "Wyvern undefined".
    """
    _main, _client = app_env
    from leaderboard_service import (
        PRESTIGE_COUNT, PRESTIGE_NAMES, RANKS_PER_PRESTIGE, RANK_NUMERALS,
        get_rank_ladder, rank_title,
    )

    assert len(PRESTIGE_NAMES) == PRESTIGE_COUNT, (
        f"{len(PRESTIGE_NAMES)} names for {PRESTIGE_COUNT} prestiges")
    assert len(RANK_NUMERALS) >= RANKS_PER_PRESTIGE, (
        f"{len(RANK_NUMERALS)} numerals for {RANKS_PER_PRESTIGE} ranks a prestige")
    assert len(set(PRESTIGE_NAMES)) == len(PRESTIGE_NAMES), "two prestiges share a name"

    rows = get_rank_ladder()
    titles = [r["rank_name"] for r in rows]
    assert len(set(titles)) == len(rows), "two bands share a title"
    for row in rows:
        assert row["rank_name"], f"{row['rank_icon_key']} has no name"
        assert row["rank_name"] == rank_title(row["prestige"], row["rank"])
        assert "None" not in row["rank_name"]
        assert "band" not in row["rank_name"].lower(), (
            f"{row['rank_icon_key']} is named after its own key: {row['rank_name']}")


def test_no_rank_is_ever_named_after_its_key(app_env):
    """"Band 034" on a driver's profile is what this whole ladder exists to
    stop. Nothing the backend can return may be the key spelled out."""
    _main, client = app_env
    import re
    from leaderboard_service import _rank_for_level, MAX_LEVEL

    key_shaped = re.compile(r"^band[\s_-]*\d+$", re.I)
    for level in (0, 1, 2, 33, 34, 500, 999, MAX_LEVEL, MAX_LEVEL + 50):
        name = _rank_for_level(level)["rank_name"]
        assert not key_shaped.match(name), f"level {level} is named {name!r}"

    driver = _signup(client, "namecheck@example.com", "Namecheck")
    res = client.get("/leaderboard/progression/me", headers=_h(driver))
    assert res.status_code == 200, res.text
    prog = res.json()["progression"]
    assert not key_shaped.match(prog["rank_name"]), prog["rank_name"]
    assert prog["rank_icon_key"] == "band_001"
