"""Thumbnail generation and key derivation.

The rule this module has to hold to is that a thumbnail is an optimisation, not
a requirement: anything it cannot do must degrade to serving the original rather
than raise. Most of these tests are about that failure behaviour, because the
success path is the easy half.
"""
from __future__ import annotations

import io
import random

import pytest

from PIL import Image

import media_store as m


def make_image(size=(1600, 1200), mode="RGB", fmt="JPEG"):
    """An encoded image that compresses the way a photograph does.

    Fixture realism matters more here than it looks. A flat colour compresses so
    well that any thumbnail of it is larger, and pixel-level noise is adversarial
    to JPEG specifically — either one makes build_thumbnail correctly return None
    and the test meaningless. Upscaling small random blobs gives smooth,
    mid-frequency texture: large as PNG, small as JPEG, like a real photo.
    """
    rng = random.Random(1234)  # fixed, so a failure is reproducible
    seed = Image.new("RGB", (64, 48))
    seed.putdata([(rng.randrange(256), rng.randrange(256), rng.randrange(256)) for _ in range(64 * 48)])
    img = seed.resize(size, Image.BICUBIC)
    if mode == "RGBA":
        img = img.convert("RGBA")
    elif mode == "P":
        img = img.convert("P", palette=Image.Palette.ADAPTIVE)
    out = io.BytesIO()
    img.save(out, format=fmt)
    return out.getvalue()


# --------------------------------------------------------------------------
# key derivation
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "key,expected",
    [
        ("public/global/user-1-message-42.jpg", "public/global/user-1-message-42.thumb.jpg"),
        ("private/1-2/user-1-message-9.png", "private/1-2/user-1-message-9.thumb.jpg"),
        ("public/room/user-3-message-7.webp", "public/room/user-3-message-7.thumb.jpg"),
        ("public/room/user-3-message-7.gif", "public/room/user-3-message-7.thumb.jpg"),
    ],
)
def test_thumb_key_replaces_the_extension(key, expected):
    assert m.derive_thumb_key(key) == expected


def test_thumb_key_handles_a_filename_with_no_extension():
    assert m.derive_thumb_key("public/room/noextension") == "public/room/noextension.thumb.jpg"


def test_thumb_key_ignores_dots_in_directories():
    """A dotted directory must not be mistaken for the filename's extension."""
    assert m.derive_thumb_key("public/v1.2/message-3") == "public/v1.2/message-3.thumb.jpg"


def test_thumb_key_is_idempotent():
    once = m.derive_thumb_key("a/b.jpg")
    assert m.derive_thumb_key(once) == once


def test_thumb_key_never_collides_with_a_stored_upload():
    """Uploads are named user-N-message-N.<ext>, so no upload can occupy a thumb key."""
    upload = "public/global/user-1-message-42.jpg"
    assert m.derive_thumb_key(upload) != upload
    assert m.is_thumb_key(m.derive_thumb_key(upload))
    assert not m.is_thumb_key(upload)


@pytest.mark.parametrize("bad", ["", "   ", None])
def test_thumb_key_rejects_an_empty_key(bad):
    with pytest.raises(ValueError):
        m.derive_thumb_key(bad)


# --------------------------------------------------------------------------
# generation
# --------------------------------------------------------------------------

def test_thumbnail_shrinks_a_large_photo():
    src = make_image((3000, 2000))
    thumb = m.build_thumbnail(src)
    assert thumb is not None
    assert len(thumb) < len(src)
    with Image.open(io.BytesIO(thumb)) as img:
        assert max(img.size) == m.THUMB_MAX_EDGE
        assert img.format == "JPEG"


def test_thumbnail_preserves_aspect_ratio():
    thumb = m.build_thumbnail(make_image((2000, 1000)))
    with Image.open(io.BytesIO(thumb)) as img:
        assert img.size == (m.THUMB_MAX_EDGE, m.THUMB_MAX_EDGE // 2)


def test_thumbnail_never_upscales():
    """A small image stays small — an upscaled thumbnail is bigger and blurrier."""
    src = make_image((120, 90))
    thumb = m.build_thumbnail(src)
    if thumb is not None:
        with Image.open(io.BytesIO(thumb)) as img:
            assert img.size == (120, 90)


@pytest.mark.parametrize("fmt,mode", [("PNG", "RGBA"), ("PNG", "RGB"), ("WEBP", "RGB"), ("GIF", "P")])
def test_every_accepted_upload_format_produces_a_jpeg(fmt, mode):
    """chat.py accepts jpeg/png/webp/gif; all four must thumbnail to one format."""
    src = make_image((1400, 1000), mode=mode, fmt=fmt)
    thumb = m.build_thumbnail(src)
    assert thumb is not None, f"{fmt}/{mode} produced no thumbnail"
    with Image.open(io.BytesIO(thumb)) as img:
        assert img.format == "JPEG"
        assert img.mode == "RGB"


def test_transparency_is_flattened_onto_white_not_black():
    """A screenshot with alpha must not come back as a black rectangle."""
    img = Image.new("RGBA", (800, 600), (255, 255, 255, 0))
    for x in range(0, 200):
        for y in range(0, 200):
            img.putpixel((x, y), (10, 10, 10, 255))
    buf = io.BytesIO()
    img.save(buf, format="PNG")

    thumb = m.build_thumbnail(buf.getvalue())
    assert thumb is not None
    with Image.open(io.BytesIO(thumb)) as out:
        # Sample well inside the region that was fully transparent.
        r, g, b = out.convert("RGB").getpixel((out.size[0] - 10, out.size[1] - 10))
        assert r > 240 and g > 240 and b > 240, f"transparent area became {(r, g, b)}"


def test_exif_orientation_is_applied():
    """Phone photos carry rotation in EXIF; a thumbnail that ignores it is sideways."""
    img = Image.new("RGB", (400, 200), (120, 60, 60))
    buf = io.BytesIO()
    exif = img.getexif()
    exif[274] = 6  # rotate 90°
    img.save(buf, format="JPEG", exif=exif)

    thumb = m.build_thumbnail(buf.getvalue())
    assert thumb is not None
    with Image.open(io.BytesIO(thumb)) as out:
        assert out.size[1] > out.size[0], "orientation was ignored — thumbnail is still landscape"


# --------------------------------------------------------------------------
# failure behaviour — the half that matters on a live system
# --------------------------------------------------------------------------

@pytest.mark.parametrize("payload", [b"", b"not an image", b"\x89PNG\r\n\x1a\n truncated"])
def test_unreadable_bytes_return_none_instead_of_raising(payload):
    """An upload this cannot read still has to succeed; the original gets served."""
    assert m.build_thumbnail(payload) is None


@pytest.mark.parametrize(
    "size,fmt",
    [((8, 8), "JPEG"), ((32, 24), "PNG"), ((64, 64), "WEBP"), ((400, 400), "JPEG"), ((3000, 2000), "JPEG")],
)
def test_a_returned_thumbnail_is_always_smaller_than_its_source(size, fmt):
    """The invariant behind the 'no gain' guard.

    Storing a thumbnail that is bigger than the original would cost disk and
    bandwidth to serve a worse image, so build_thumbnail must return None rather
    than a larger result — at any input size, including ones already tiny.
    """
    src = make_image(size, fmt=fmt)
    thumb = m.build_thumbnail(src)
    assert thumb is None or len(thumb) < len(src)


def test_a_decompression_bomb_is_refused(monkeypatch):
    """A few KB of PNG can decode to gigapixels; the pixel guard must stop it."""
    monkeypatch.setattr(m, "MAX_DECODE_PIXELS", 1000)
    assert m.build_thumbnail(make_image((2000, 1500))) is None


def test_missing_pillow_degrades_to_the_original(monkeypatch):
    monkeypatch.setattr(m, "Image", None)
    monkeypatch.setattr(m, "ImageOps", None)
    assert m.thumbnails_supported() is False
    assert m.build_thumbnail(make_image()) is None
