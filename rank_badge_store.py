"""The rank badges, kept in Postgres.

One image per band of leaderboard_service.RANK_LADDER, which is currently ten
prestiges of three ranks: band_001 through band_030. The band index is the only
thing stored, and the pair a driver sees is derived from it -- band_005 is
prestige 2, rank 2.

Nothing here hardcodes the count. That shape has changed three times already,
and the range in the error messages is built from the ladder for the same
reason: a message that says "band_001 through band_050" when the ladder stops
at 030 sends whoever is uploading off to look for a bug that is not there.

They live in the database rather than in the frontend repo, which means a new
set of artwork ships by uploading it, with no deploy of either side and no
image files in git.

The shape follows artifact_db_store: BYTEA on Postgres, BLOB on SQLite, an
upsert keyed on the row's own identifier, and a sha256 of the bytes stored
beside them. Here that digest does double duty as the cache-busting version --
the URL a client is handed carries it, so a badge can be cached forever and
still change the moment its bytes do.

Nothing in here trusts what it is given. The key must be one the ladder
actually defines, the bytes must decode as an image, and the recorded
content type comes from decoding rather than from whatever the uploader
claimed.
"""
from __future__ import annotations

import hashlib
import io
import time
from typing import Any, Dict, List, Optional

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one

try:
    from PIL import Image
except Exception:  # pragma: no cover - runtime fallback if Pillow is unavailable
    Image = None


# A badge is artwork for a phone, not a poster. This leaves generous room
# above the sizes artwork of this kind runs to while keeping the whole set to
# a few megabytes in the row store, and it is the backstop against someone
# uploading a photograph.
MAX_BADGE_BYTES = 512 * 1024
MIN_BADGE_DIMENSION = 32
MAX_BADGE_DIMENSION = 1024

# What Pillow reports -> what we serve. Anything else is refused rather than
# guessed at: a browser given the wrong content type for an image renders
# nothing and says nothing.
_FORMAT_CONTENT_TYPES = {
    "WEBP": "image/webp",
    "PNG": "image/png",
    "JPEG": "image/jpeg",
}


def _blob_type() -> str:
    return "BYTEA" if DB_BACKEND == "postgres" else "BLOB"


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        value = row[key]
    except Exception:
        try:
            value = getattr(row, key)
        except Exception:
            return default
    return default if value is None else value


def valid_rank_icon_keys() -> List[str]:
    """The keys the ladder defines, as the only ones a badge may use.

    Imported lazily: leaderboard_service pulls in a good deal of runtime, and
    a store module should not drag it into every import of the app.
    """
    from leaderboard_service import RANK_LADDER

    return [key for _start, _end, _name, key in RANK_LADDER]


def is_valid_rank_icon_key(rank_icon_key: str) -> bool:
    return str(rank_icon_key or "") in set(valid_rank_icon_keys())


def rank_icon_key_range() -> str:
    """The valid range, spelled out for an error message, read off the ladder."""
    keys = valid_rank_icon_keys()
    return f"{keys[0]} through {keys[-1]}" if keys else "(the ladder is empty)"


def ensure_rank_badge_schema() -> None:
    _db_exec(
        f"""
        CREATE TABLE IF NOT EXISTS rank_badges (
            rank_icon_key TEXT PRIMARY KEY,
            image_bytes {_blob_type()} NOT NULL,
            content_type TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
            width INTEGER NOT NULL,
            height INTEGER NOT NULL,
            byte_size BIGINT NOT NULL,
            updated_at_unix BIGINT NOT NULL
        )
        """
    )


def inspect_badge_bytes(raw: bytes) -> Dict[str, Any]:
    """Decide what these bytes are, or refuse them.

    Raises ValueError with a message meant for the person uploading, since
    every one of these is something they can correct.
    """
    if not raw:
        raise ValueError("the badge image is empty")
    if len(raw) > MAX_BADGE_BYTES:
        raise ValueError(
            f"the badge image is {len(raw)} bytes; the limit is {MAX_BADGE_BYTES}"
        )
    if Image is None:
        raise ValueError("image support is unavailable on this server")

    try:
        with Image.open(io.BytesIO(raw)) as img:
            img_format = str(img.format or "").upper()
            width, height = int(img.width), int(img.height)
            # verify() is the cheap structural check; it consumes the file, so
            # it goes last and nothing reads from img afterwards.
            img.verify()
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("the badge image could not be read as an image") from exc

    if img_format not in _FORMAT_CONTENT_TYPES:
        raise ValueError(
            f"{img_format or 'that format'} is not a supported badge image; "
            f"use {', '.join(sorted(_FORMAT_CONTENT_TYPES))}"
        )
    if width < MIN_BADGE_DIMENSION or height < MIN_BADGE_DIMENSION:
        raise ValueError(
            f"the badge image is {width}x{height}; the minimum is "
            f"{MIN_BADGE_DIMENSION}x{MIN_BADGE_DIMENSION}"
        )
    if width > MAX_BADGE_DIMENSION or height > MAX_BADGE_DIMENSION:
        raise ValueError(
            f"the badge image is {width}x{height}; the maximum is "
            f"{MAX_BADGE_DIMENSION}x{MAX_BADGE_DIMENSION}"
        )

    return {
        "content_type": _FORMAT_CONTENT_TYPES[img_format],
        "width": width,
        "height": height,
        "byte_size": len(raw),
        "content_sha256": hashlib.sha256(raw).hexdigest(),
    }


def save_rank_badge(rank_icon_key: str, raw: bytes) -> Dict[str, Any]:
    key = str(rank_icon_key or "").strip()
    if not is_valid_rank_icon_key(key):
        raise ValueError(
            f"{key or 'that key'} is not a rank the ladder defines; "
            f"expected {rank_icon_key_range()}"
        )
    meta = inspect_badge_bytes(raw)
    updated_at_unix = int(time.time())
    payload = raw if DB_BACKEND == "postgres" else memoryview(raw)

    conflict_update = (
        """
            image_bytes=EXCLUDED.image_bytes,
            content_type=EXCLUDED.content_type,
            content_sha256=EXCLUDED.content_sha256,
            width=EXCLUDED.width,
            height=EXCLUDED.height,
            byte_size=EXCLUDED.byte_size,
            updated_at_unix=EXCLUDED.updated_at_unix
        """
        if DB_BACKEND == "postgres"
        else """
            image_bytes=excluded.image_bytes,
            content_type=excluded.content_type,
            content_sha256=excluded.content_sha256,
            width=excluded.width,
            height=excluded.height,
            byte_size=excluded.byte_size,
            updated_at_unix=excluded.updated_at_unix
        """
    )
    _db_exec(
        f"""
        INSERT INTO rank_badges (
            rank_icon_key, image_bytes, content_type, content_sha256,
            width, height, byte_size, updated_at_unix
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (rank_icon_key) DO UPDATE SET {conflict_update}
        """,
        (
            key,
            payload,
            meta["content_type"],
            meta["content_sha256"],
            meta["width"],
            meta["height"],
            meta["byte_size"],
            updated_at_unix,
        ),
    )
    return {
        "rank_icon_key": key,
        "version": meta["content_sha256"],
        "content_type": meta["content_type"],
        "width": meta["width"],
        "height": meta["height"],
        "byte_size": meta["byte_size"],
        "updated_at_unix": updated_at_unix,
    }


def get_rank_badge_bytes(rank_icon_key: str) -> Optional[Dict[str, Any]]:
    """The row including its bytes. Only the endpoint that serves an image
    should call this; everything else wants the manifest."""
    key = str(rank_icon_key or "").strip()
    if not key:
        return None
    row = _db_query_one(
        """
        SELECT rank_icon_key, image_bytes, content_type, content_sha256,
               width, height, byte_size, updated_at_unix
        FROM rank_badges WHERE rank_icon_key = ?
        """,
        (key,),
    )
    if row is None:
        return None
    raw = _row_value(row, "image_bytes", b"")
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    return {
        "rank_icon_key": _row_value(row, "rank_icon_key", key),
        "image_bytes": bytes(raw or b""),
        "content_type": _row_value(row, "content_type", "application/octet-stream"),
        "version": _row_value(row, "content_sha256", ""),
        "width": int(_row_value(row, "width", 0) or 0),
        "height": int(_row_value(row, "height", 0) or 0),
        "byte_size": int(_row_value(row, "byte_size", 0) or 0),
        "updated_at_unix": int(_row_value(row, "updated_at_unix", 0) or 0),
    }


def list_rank_badges() -> List[Dict[str, Any]]:
    """Every stored badge, without its bytes.

    image_bytes is deliberately not selected: this is what the manifest
    endpoint serves, and a hundred rows of artwork is megabytes nobody asked
    for on a screen that only needs to know which badges exist.
    """
    rows = _db_query_all(
        """
        SELECT rank_icon_key, content_type, content_sha256,
               width, height, byte_size, updated_at_unix
        FROM rank_badges ORDER BY rank_icon_key ASC
        """
    ) or []
    from leaderboard_service import prestige_and_rank_for_band

    order = {key: index for index, key in enumerate(valid_rank_icon_keys(), start=1)}
    out: List[Dict[str, Any]] = []
    for row in rows:
        key = _row_value(row, "rank_icon_key", "")
        version = _row_value(row, "content_sha256", "")
        pair = prestige_and_rank_for_band(order.get(key, 1))
        out.append(
            {
                "rank_icon_key": key,
                "version": version,
                "url": rank_badge_url(key, version),
                # Sent rather than parsed back out of the key on the client.
                "prestige": pair["prestige"],
                "rank": pair["rank"],
                "content_type": _row_value(row, "content_type", ""),
                "width": int(_row_value(row, "width", 0) or 0),
                "height": int(_row_value(row, "height", 0) or 0),
                "byte_size": int(_row_value(row, "byte_size", 0) or 0),
                "updated_at_unix": int(_row_value(row, "updated_at_unix", 0) or 0),
            }
        )
    return out


def rank_badge_url(rank_icon_key: str, version: Optional[str]) -> str:
    base = f"/ranks/badge/{rank_icon_key}"
    return f"{base}?v={version}" if version else base


def delete_rank_badge(rank_icon_key: str) -> bool:
    key = str(rank_icon_key or "").strip()
    if not key:
        return False
    existed = get_rank_badge_bytes(key) is not None
    _db_exec("DELETE FROM rank_badges WHERE rank_icon_key = ?", (key,))
    return existed


def rank_badge_coverage() -> Dict[str, Any]:
    """How much of the ladder is actually dressed.

    The set arrives a piece at a time, so the useful question while it is
    landing is which bands are still bare -- and it is the same question later,
    when a band has been added to the ladder and nobody drew for it.
    """
    expected = valid_rank_icon_keys()
    stored = {row["rank_icon_key"] for row in list_rank_badges()}
    missing = [key for key in expected if key not in stored]
    return {
        "expected_count": len(expected),
        "stored_count": len(stored),
        "missing_count": len(missing),
        "missing": missing,
        "complete": not missing,
    }
