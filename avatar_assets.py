from __future__ import annotations

import base64
import binascii
import hashlib
import io
from pathlib import Path
from typing import Optional, Tuple

try:
    from PIL import Image, ImageOps
except Exception:  # pragma: no cover - runtime fallback if Pillow is unavailable
    Image = None
    ImageOps = None


AVATAR_THUMB_SIZE = 64
AVATAR_THUMB_FORMAT = "PNG"
AVATAR_THUMB_MIME = "image/png"


def normalize_avatar_data_url(value: Optional[str], max_length: int) -> Optional[str]:
    if value is None:
        return None
    avatar = value.strip()
    if avatar == "":
        return None
    if len(avatar) > int(max_length):
        raise ValueError("avatar_url is too large")
    if not avatar.startswith("data:image/"):
        raise ValueError("avatar_url must be an image data URL")
    if "," not in avatar:
        raise ValueError("avatar_url must be a valid data URL")
    # Validate the base64 payload here, at the request boundary, instead of
    # waiting for _ensure_avatar_thumb_materialized to fail later. Without
    # this check a malformed data URL was written to users.avatar_url first,
    # and then the deferred decode raised an unhandled ValueError that
    # bubbled up as a 500 (after the row was already updated).
    decode_avatar_data_url(avatar)
    return avatar


def avatar_version_for_data_url(avatar_data_url: Optional[str]) -> Optional[str]:
    if not avatar_data_url:
        return None
    digest = hashlib.sha1(avatar_data_url.encode("utf-8")).hexdigest()
    return digest[:16]


def avatar_thumb_relative_path(user_id: int, version: str) -> Path:
    return Path("avatar_thumbs") / str(int(user_id)) / f"{version}.png"


def avatar_thumb_path(data_dir: Path, user_id: int, version: str) -> Path:
    return data_dir / avatar_thumb_relative_path(int(user_id), version)


def avatar_thumb_url(user_id: int, version: Optional[str]) -> Optional[str]:
    if not version:
        return None
    return f"/avatars/thumb/{int(user_id)}?v={version}"


def decode_avatar_data_url(avatar_data_url: str) -> bytes:
    try:
        _, encoded = avatar_data_url.split(",", 1)
    except ValueError as exc:
        raise ValueError("avatar_url must be a valid data URL") from exc
    try:
        return base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("avatar_url must contain valid base64 image data") from exc


def build_avatar_thumb_bytes(avatar_data_url: str) -> bytes:
    raw = decode_avatar_data_url(avatar_data_url)
    if Image is None or ImageOps is None:
        return raw

    try:
        with Image.open(io.BytesIO(raw)) as img:
            fitted = ImageOps.fit(
                ImageOps.exif_transpose(img).convert("RGBA"),
                (AVATAR_THUMB_SIZE, AVATAR_THUMB_SIZE),
                method=Image.Resampling.LANCZOS,
            )
            out = io.BytesIO()
            fitted.save(out, format=AVATAR_THUMB_FORMAT, optimize=True)
            return out.getvalue()
    except Exception as exc:
        raise ValueError("avatar image could not be processed") from exc


def persist_avatar_thumb(data_dir: Path, user_id: int, avatar_data_url: str, version: Optional[str] = None) -> Tuple[Path, str]:
    resolved_version = version or avatar_version_for_data_url(avatar_data_url)
    if not resolved_version:
        raise ValueError("avatar version could not be determined")
    target = avatar_thumb_path(data_dir, int(user_id), resolved_version)
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.write_bytes(build_avatar_thumb_bytes(avatar_data_url))
    for sibling in target.parent.glob("*.png"):
        if sibling != target:
            sibling.unlink(missing_ok=True)
    return target, resolved_version
