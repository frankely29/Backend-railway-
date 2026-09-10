"""Chat images gain a thumbnail, and it lives and dies with the original.

The upload and serve paths are the risky part of adding thumbnails to a live
system, so these cover the three ways it could go wrong: an upload broken by a
thumbnail failure, a thumbnail that outlives the photo it belongs to, and a
private thumbnail readable by someone who cannot read the photo.
"""
from __future__ import annotations

import io

import pytest
from fastapi import HTTPException

from PIL import Image

import chat
import media_store as m
from media_store import derive_thumb_key


def photo(size=(1600, 1200), fmt="JPEG") -> bytes:
    width, height = size
    buf = bytearray(width * height * 3)
    for i in range(0, len(buf), 3):
        v = (i * 7919) % 251
        buf[i] = v
        buf[i + 1] = (v * 3 + 61) % 256
        buf[i + 2] = (v * 5 + 137) % 256
    out = io.BytesIO()
    Image.frombytes("RGB", size, bytes(buf)).save(out, format=fmt)
    return out.getvalue()


@pytest.fixture
def image_dir(tmp_path, monkeypatch):
    root = tmp_path / "chat_images"
    root.mkdir()
    monkeypatch.setattr(chat, "_CHAT_IMAGE_DIR", root)
    return root


class FakeRequest:
    def __init__(self, method="GET"):
        self.method = method


def image_row(image_path, mime="image/jpeg"):
    return {"message_type": "image", "image_path": image_path, "image_mime_type": mime}


# --------------------------------------------------------------------------
# storing
# --------------------------------------------------------------------------

def test_storing_an_image_also_stores_its_thumbnail(image_dir):
    payload = photo()
    key = chat._store_image_file("public/global", 42, 7, ".jpg", payload)

    assert key == "public/global/user-7-message-42.jpg"
    original = image_dir / key
    thumb = image_dir / derive_thumb_key(key)
    assert original.read_bytes() == payload
    assert thumb.exists()
    assert thumb.stat().st_size < original.stat().st_size


def test_a_thumbnail_failure_never_costs_the_upload(image_dir):
    """The photo is the thing the driver cares about. A slow grid is survivable."""
    payload = b"these bytes are not a decodable image"
    key = chat._store_image_file("public/global", 9, 3, ".jpg", payload)

    assert (image_dir / key).read_bytes() == payload
    assert not (image_dir / derive_thumb_key(key)).exists()


def test_a_thumbnail_crash_never_costs_the_upload(image_dir, monkeypatch):
    def boom(_payload):
        raise RuntimeError("Pillow exploded")

    monkeypatch.setattr(chat, "build_thumbnail", boom)
    key = chat._store_image_file("public/global", 11, 3, ".jpg", photo())
    assert (image_dir / key).exists()
    assert not (image_dir / derive_thumb_key(key)).exists()


def test_audio_is_untouched_by_the_image_changes(tmp_path, monkeypatch):
    """_store_audio_file was refactored onto the shared writer; it must still work."""
    root = tmp_path / "chat_audio"
    root.mkdir()
    monkeypatch.setattr(chat, "_CHAT_AUDIO_DIR", root)
    key = chat._store_audio_file("public/global", 5, 2, ".webm", b"voice bytes")
    assert (root / key).read_bytes() == b"voice bytes"
    assert not (root / derive_thumb_key(key)).exists(), "audio must not get a thumbnail"


# --------------------------------------------------------------------------
# purging — a thumbnail must never outlive its photo
# --------------------------------------------------------------------------

def test_purging_an_expired_image_removes_the_thumbnail_too(image_dir):
    key = chat._store_image_file("public/global", 42, 7, ".jpg", photo())
    assert (image_dir / derive_thumb_key(key)).exists()

    chat._safe_unlink_chat_image(key)

    assert not (image_dir / key).exists()
    assert not (image_dir / derive_thumb_key(key)).exists(), "thumbnail leaked past retention"


def test_purging_still_removes_the_image_when_there_is_no_thumbnail(image_dir):
    key = chat._store_image_file("public/global", 1, 1, ".jpg", b"not an image")
    chat._safe_unlink_chat_image(key)
    assert not (image_dir / key).exists()


def test_rolling_back_a_failed_upload_removes_both(image_dir):
    key = chat._store_image_file("public/global", 42, 7, ".jpg", photo())
    target = image_dir / key

    chat._unlink_image_and_thumb(target)

    assert not target.exists()
    assert not (image_dir / derive_thumb_key(key)).exists()


def test_rollback_tolerates_a_missing_file(image_dir):
    chat._unlink_image_and_thumb(image_dir / "public/global/never-written.jpg")
    chat._unlink_image_and_thumb(None)


def test_a_traversal_key_is_still_refused(image_dir):
    """Adding a derived key must not open a path out of the media root."""
    with pytest.raises(HTTPException):
        chat._resolve_image_path("../../etc/passwd")


# --------------------------------------------------------------------------
# serialising
# --------------------------------------------------------------------------

def test_an_image_message_exposes_both_urls():
    payload = chat._serialize_public_message({
        "id": 42, "room": "global", "user_id": 7, "display_name": "Bee",
        "message": "", "created_at": 1_700_000_000,
        "message_type": "image", "image_path": "public/global/user-7-message-42.jpg",
        "image_mime_type": "image/jpeg",
    })
    assert payload["image_url"] == "/chat/image/public/42"
    assert payload["image_thumb_url"] == "/chat/image/public/42/thumb"


def test_a_private_image_message_exposes_both_urls():
    payload = chat._serialize_private_message({
        "id": 9, "sender_user_id": 2, "recipient_user_id": 3, "text": "",
        "created_at": 1_700_000_000, "message_type": "image",
        "image_path": "private/2-3/user-2-message-9.jpg", "image_mime_type": "image/jpeg",
    })
    assert payload["image_url"] == "/chat/image/private/9"
    assert payload["image_thumb_url"] == "/chat/image/private/9/thumb"


@pytest.mark.parametrize("kind", ["text", "voice"])
def test_a_non_image_message_has_no_thumbnail_url(kind):
    payload = chat._serialize_public_message({
        "id": 1, "room": "global", "user_id": 7, "display_name": "Bee",
        "message": "hello", "created_at": 1_700_000_000,
        "message_type": kind, "image_path": None, "image_mime_type": None,
        "audio_path": None, "audio_duration_ms": None, "audio_mime_type": None,
    })
    assert payload["image_thumb_url"] is None


# --------------------------------------------------------------------------
# serving
# --------------------------------------------------------------------------

def test_serving_a_thumbnail_returns_the_small_jpeg(image_dir):
    key = chat._store_image_file("public/global", 42, 7, ".jpg", photo())
    res = chat._serve_image_thumb(42, image_row(key), FakeRequest())

    assert res.media_type == m.THUMB_MIME_TYPE
    assert len(res.body) == (image_dir / derive_thumb_key(key)).stat().st_size
    assert len(res.body) < (image_dir / key).stat().st_size


def test_a_thumbnail_is_generated_on_first_read_when_missing(image_dir):
    """Images stored before thumbnails existed must not need a backfill job."""
    key = chat._store_image_file("public/global", 42, 7, ".jpg", photo())
    (image_dir / derive_thumb_key(key)).unlink()

    res = chat._serve_image_thumb(42, image_row(key), FakeRequest())

    assert (image_dir / derive_thumb_key(key)).exists(), "was not generated on read"
    assert res.media_type == m.THUMB_MIME_TYPE


def test_serving_falls_back_to_the_original_when_no_thumbnail_is_possible(image_dir):
    key = chat._store_image_file("public/global", 3, 1, ".jpg", b"undecodable bytes")
    res = chat._serve_image_thumb(3, image_row(key), FakeRequest())

    assert res.body == b"undecodable bytes"
    assert res.media_type == "image/jpeg"


def test_thumbnails_are_cached_longer_than_originals(image_dir):
    """A thumbnail is immutable for the life of its message; the browser can keep it."""
    key = chat._store_image_file("public/global", 42, 7, ".jpg", photo())
    thumb = chat._serve_image_thumb(42, image_row(key), FakeRequest())
    full = chat._serve_image("r", 42, image_row(key), FakeRequest())

    assert "86400" in thumb.headers["cache-control"]
    assert thumb.headers["cache-control"].startswith("private")
    assert full.headers["cache-control"].startswith("private")


def test_head_returns_headers_without_the_body(image_dir):
    key = chat._store_image_file("public/global", 42, 7, ".jpg", photo())
    res = chat._serve_image_thumb(42, image_row(key), FakeRequest("HEAD"))
    assert res.body == b""
    assert res.media_type == m.THUMB_MIME_TYPE


def test_a_missing_original_is_a_404_not_a_broken_thumbnail(image_dir):
    with pytest.raises(HTTPException) as exc:
        chat._serve_image_thumb(42, image_row("public/global/gone.jpg"), FakeRequest())
    assert exc.value.status_code == 404


@pytest.mark.parametrize("row", [
    {"message_type": "text", "image_path": None},
    {"message_type": "image", "image_path": None},
])
def test_a_non_image_row_is_a_404(image_dir, row):
    with pytest.raises(HTTPException) as exc:
        chat._serve_image_thumb(1, row, FakeRequest())
    assert exc.value.status_code == 404


@pytest.mark.parametrize("method", ["POST", "DELETE", "PUT"])
def test_write_methods_are_refused(image_dir, method):
    with pytest.raises(HTTPException) as exc:
        chat._serve_image_thumb(1, image_row("x.jpg"), FakeRequest(method))
    assert exc.value.status_code == 405


# --------------------------------------------------------------------------
# routing and authorisation
# --------------------------------------------------------------------------

def test_both_thumbnail_routes_are_registered():
    paths = {r.path for r in chat.router.routes if "thumb" in getattr(r, "path", "")}
    assert paths == {
        "/chat/image/public/{message_id}/thumb",
        "/chat/image/private/{message_id}/thumb",
    }


def test_thumbnail_routes_are_authenticated_like_the_full_image():
    """A thumbnail is still the photo — it cannot be the cheap way around the gate."""
    by_path = {r.path: r for r in chat.router.routes if "/chat/image/" in getattr(r, "path", "")}
    for path, route in by_path.items():
        names = [d.call.__name__ for d in route.dependant.dependencies if d.call]
        assert "require_user" in names, f"{path} is not authenticated"

    full = by_path["/chat/image/private/{message_id}"]
    thumb = by_path["/chat/image/private/{message_id}/thumb"]
    # The private thumb handler must run the same participant check as the full
    # image handler, not just the shared auth dependency.
    import inspect
    assert "Not allowed" in inspect.getsource(thumb.endpoint)
    assert "Not allowed" in inspect.getsource(full.endpoint)
