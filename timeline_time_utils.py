from __future__ import annotations

import re
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

NYC_TZ = ZoneInfo("America/New_York")
_FRONTEND_LOCAL_ISO_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")
_DATE_ONLY_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATE_TIME_SPACE_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}(?:\.\d+)?)")
_TRAILING_TZ_SUFFIX_PATTERN = re.compile(r"(?:Z|[+-]\d{2}:\d{2})$")
_FRACTIONAL_SECONDS_PATTERN = re.compile(r"(\d{2}:\d{2}:\d{2})\.\d+")


def to_frontend_local_iso(value: Any) -> str:
    if isinstance(value, datetime):
        dt_value = value.astimezone(NYC_TZ) if value.tzinfo is not None else value
        return dt_value.replace(microsecond=0, tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")

    if isinstance(value, date):
        return datetime.combine(value, time.min).strftime("%Y-%m-%dT%H:%M:%S")

    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise ValueError("Invalid datetime value: empty string")

        if _DATE_ONLY_PATTERN.fullmatch(normalized):
            normalized = f"{normalized}T00:00:00"

        match = _DATE_TIME_SPACE_PATTERN.match(normalized)
        if match:
            normalized = f"{match.group(1)}T{match.group(2)}{normalized[match.end():]}"

        normalized = _TRAILING_TZ_SUFFIX_PATTERN.sub("", normalized)
        normalized = _FRACTIONAL_SECONDS_PATTERN.sub(r"\1", normalized)

        if not _FRONTEND_LOCAL_ISO_PATTERN.fullmatch(normalized):
            raise ValueError(f"Invalid datetime format for frontend timeline/frame contract: {value!r}")

        datetime.strptime(normalized, "%Y-%m-%dT%H:%M:%S")
        return normalized

    raise ValueError(f"Unsupported datetime value type for frontend timeline/frame contract: {type(value).__name__}")
