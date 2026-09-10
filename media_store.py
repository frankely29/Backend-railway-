"""Thumbnails for uploaded images, and the keys that address them.

Chat images are accepted at up to 8 MB and, until now, were only ever served
back at full size. That is tolerable in a message bubble you scroll past once. It
is unusable in a grid: a 12-image profile or channel view pulls ~100 MB over
cellular before it paints.

So every stored image gets a companion thumbnail, addressed by a key derived from
the original's. Two rules make that safe to introduce on a live system:

  * Generation never fails an upload. If Pillow cannot read the file, the upload
    still succeeds and the original is served in the thumbnail's place. A missing
    thumbnail is a slow grid; a failed upload is a lost photo.
  * Thumbnails are also generated lazily on first read, so images stored before
    this module existed get one without a backfill job.

The key derivation lives here rather than in chat.py because posts (a separate
table, later) will address media the same way, and because a move to object
storage replaces how a key is resolved without touching what a key is.
"""
from __future__ import annotations

import io
import logging
from typing import Optional

try:
    from PIL import Image, ImageOps
except Exception:  # pragma: no cover - Pillow is pinned, but never break uploads over it
    Image = None
    ImageOps = None

_LOGGER = logging.getLogger(__name__)

# The two places a thumbnail is displayed are a ~110px grid tile and a chat
# bubble capped at 220px CSS. 512 covers the larger of those past 2x device
# pixel ratio and still lands in tens of kilobytes; anything sharper is only
# visible in the full-screen viewer, which loads the original anyway.
THUMB_MAX_EDGE = 512
THUMB_MIME_TYPE = "image/jpeg"
THUMB_SUFFIX = ".thumb.jpg"
_THUMB_QUALITY = 82

# A decompression guard. Uploads are capped at 8 MB of *encoded* bytes, which a
# crafted PNG can expand into gigapixels of RGBA and exhaust the container's
# memory. 40 megapixels is far above any phone camera and far below dangerous.
MAX_DECODE_PIXELS = 40_000_000


def derive_thumb_key(image_key: str) -> str:
    """The thumbnail's key for a given image key.

    Deterministic, so nothing has to be recorded in the database: given the
    original's key, the thumbnail's key is always recoverable.
    """
    key = str(image_key or "").strip()
    if not key:
        raise ValueError("image key is required")
    if is_thumb_key(key):
        return key
    base = key.rsplit(".", 1)[0] if "." in key.rsplit("/", 1)[-1] else key
    return f"{base}{THUMB_SUFFIX}"


def is_thumb_key(key: str) -> bool:
    return str(key or "").endswith(THUMB_SUFFIX)


def thumbnails_supported() -> bool:
    return Image is not None and ImageOps is not None


def build_thumbnail(payload: bytes) -> Optional[bytes]:
    """A JPEG thumbnail for `payload`, or None when one should not be used.

    Returns None — meaning "serve the original" — rather than raising, in three
    cases that are all normal rather than exceptional: Pillow is unavailable, the
    bytes are not a readable image, or the thumbnail came out no smaller than the
    original (already-tiny images, mostly).

    Always JPEG. Alpha is flattened onto white: chat uploads are overwhelmingly
    photos, JPEG is several times smaller than PNG for those, and a screenshot
    with transparency reads correctly on a white ground in a grid.
    """
    if not payload:
        return None
    if not thumbnails_supported():
        _LOGGER.warning("Pillow unavailable; serving full-size images")
        return None

    try:
        with Image.open(io.BytesIO(payload)) as img:
            width, height = img.size
            if width * height > MAX_DECODE_PIXELS:
                _LOGGER.warning(
                    "Refusing to thumbnail an oversized image",
                    extra={"width": width, "height": height},
                )
                return None

            oriented = ImageOps.exif_transpose(img)
            if oriented.mode in ("RGBA", "LA", "P"):
                flattened = Image.new("RGB", oriented.size, (255, 255, 255))
                converted = oriented.convert("RGBA")
                flattened.paste(converted, mask=converted.split()[-1])
                oriented = flattened
            elif oriented.mode != "RGB":
                oriented = oriented.convert("RGB")

            # thumbnail() only ever shrinks and preserves aspect ratio, so a
            # small image is left alone instead of being upscaled into mush.
            oriented.thumbnail((THUMB_MAX_EDGE, THUMB_MAX_EDGE), Image.Resampling.LANCZOS)

            out = io.BytesIO()
            oriented.save(out, format="JPEG", quality=_THUMB_QUALITY, optimize=True, progressive=True)
            thumb = out.getvalue()
    except Exception:
        # A corrupt or exotic upload is the user's problem to see, not a reason
        # to reject their photo. Log and fall back to the original.
        _LOGGER.warning("Could not build image thumbnail", exc_info=True)
        return None

    if not thumb or len(thumb) >= len(payload):
        return None
    return thumb
