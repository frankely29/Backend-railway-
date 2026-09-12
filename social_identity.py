"""Handles, bios and driver tags — the identity half of a profile.

Phase 1 of the network plan is "a profile worth following". A follow graph
without a profile is a list of strangers, and `display_name` alone cannot carry
one: it is not unique, so two drivers are both "Mike" and neither can be linked
to, mentioned, or found by search.

So the unit of identity becomes the handle. Three rules shape everything here:

1. A handle is claimed case-INSENSITIVELY and displayed case-sensitively.
   `@NightHawk` and `@nighthawk` must not be two people — that is how
   impersonation works — but a driver who typed capitals should keep them. Hence
   two columns: `handle` for display, `handle_key` for uniqueness.

2. Nobody is ever handle-less. A profile with a blank handle cannot be linked
   to, and asking an existing user to pick one before they can use the app they
   already paid for is a wall. Every account gets one derived from what it
   already has, and can change it later.

3. A handle that looks official is refused. `admin`, `support`, `joseo` and
   friends are reserved, because "@support" asking a driver for their password
   is a phishing attack the product handed out for free.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one

HANDLE_MIN = 3
HANDLE_MAX = 20
MAX_BIO_CHARS = 200
MAX_TAG_CHARS = 24
MAX_TAGS = 6

# Letters, digits and underscore. No dots or dashes: they are the characters
# that make two different handles look identical in a sentence.
_HANDLE_OK = re.compile(r"^[A-Za-z0-9_]+$")
_HANDLE_STRIP = re.compile(r"[^A-Za-z0-9_]+")

# Names that would let an account pass for the product or its staff.
RESERVED_HANDLES = {
    "admin", "administrator", "root", "support", "help", "helpdesk", "staff",
    "moderator", "mod", "team", "official", "joseo", "joseoapp", "joseoteam",
    "system", "security", "billing", "payments", "info", "contact", "about",
    "api", "www", "app", "me", "you", "null", "undefined", "anonymous",
    "everyone", "here", "all",
}

# What a driver can say about their own work. Free-form would turn into 400
# spellings of "uber", which is useless for filtering and for finding people.
PLATFORM_CHOICES = ["uber", "lyft", "via", "curb", "juno", "yellow", "green",
                    "black_car", "delivery", "other"]
VEHICLE_CHOICES = ["sedan", "suv", "minivan", "hybrid", "ev", "wheelchair", "other"]


def _now_year() -> int:
    import time
    return int(time.strftime("%Y", time.gmtime(time.time())))


# --------------------------------------------------------------------------
# schema
# --------------------------------------------------------------------------

def _try_exec(sql: str) -> None:
    try:
        _db_exec(sql)
    except Exception:
        pass


def ensure_identity_schema() -> None:
    cols = [
        ("handle", "TEXT"),
        ("handle_key", "TEXT"),
        ("bio", "TEXT"),
        ("platforms", "TEXT"),          # comma-separated, from PLATFORM_CHOICES
        ("vehicle_type", "TEXT"),
        ("driving_since_year", "INTEGER"),
    ]
    for name, kind in cols:
        if DB_BACKEND == "postgres":
            _try_exec(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {name} {kind};")
        else:
            _try_exec(f"ALTER TABLE users ADD COLUMN {name} {kind};")
    # UNIQUE on the key, not on the display form: the whole point is that two
    # handles differing only in case are the same handle.
    _try_exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_handle_key ON users(handle_key);")


# --------------------------------------------------------------------------
# handles
# --------------------------------------------------------------------------

def handle_key(handle: Optional[str]) -> Optional[str]:
    if not handle:
        return None
    return str(handle).strip().lstrip("@").lower() or None


def validate_handle(handle: Optional[str]) -> str:
    """Return the display form, or raise 400 saying exactly what is wrong."""
    raw = str(handle or "").strip().lstrip("@")
    if not raw:
        raise HTTPException(status_code=400, detail="Pick a handle")
    if len(raw) < HANDLE_MIN:
        raise HTTPException(status_code=400, detail=f"Handles are at least {HANDLE_MIN} characters")
    if len(raw) > HANDLE_MAX:
        raise HTTPException(status_code=400, detail=f"Handles are at most {HANDLE_MAX} characters")
    if not _HANDLE_OK.match(raw):
        raise HTTPException(status_code=400, detail="Handles can use letters, numbers and _ only")
    if raw[0] == "_" or raw[-1] == "_":
        raise HTTPException(status_code=400, detail="Handles cannot start or end with _")
    if raw.lower() in RESERVED_HANDLES:
        raise HTTPException(status_code=400, detail="That handle is reserved")
    if raw.isdigit():
        # An all-digit handle is indistinguishable from a user id in a URL.
        raise HTTPException(status_code=400, detail="Handles need at least one letter")
    return raw


def handle_is_free(handle: str, *, for_user_id: Optional[int] = None) -> bool:
    key = handle_key(handle)
    if not key:
        return False
    row = _db_query_one("SELECT id FROM users WHERE handle_key=? LIMIT 1", (key,))
    if not row:
        return True
    return for_user_id is not None and int(row["id"]) == int(for_user_id)


def _seed_from(display_name: Optional[str], email: Optional[str]) -> str:
    """The best starting point we have, before uniquifying it."""
    for candidate in (display_name or "", (email or "").split("@")[0]):
        cleaned = _HANDLE_STRIP.sub("", str(candidate)).strip("_")
        if len(cleaned) >= HANDLE_MIN and not cleaned.isdigit():
            return cleaned[:HANDLE_MAX]
    return "driver"


def suggest_handle(display_name: Optional[str], email: Optional[str],
                   *, taken: Optional[set] = None) -> str:
    """A free handle derived from what the account already has.

    Collisions are resolved with a numeric suffix rather than by refusing: this
    runs at signup, where a taken handle must not be able to fail the signup.
    """
    seed = _seed_from(display_name, email)
    taken = taken or set()

    def free(candidate: str) -> bool:
        return handle_key(candidate) not in taken and handle_is_free(candidate)

    if handle_key(seed) not in RESERVED_HANDLES and free(seed):
        return seed
    base = seed[: HANDLE_MAX - 5].rstrip("_") or "driver"
    for suffix in range(2, 10000):
        candidate = f"{base}{suffix}"
        if free(candidate):
            return candidate
    # Astronomically unlikely; still better than returning something taken.
    import uuid
    return f"driver{uuid.uuid4().hex[:8]}"


def set_handle(user_id: int, handle: str) -> str:
    display = validate_handle(handle)
    key = handle_key(display)
    if not handle_is_free(display, for_user_id=user_id):
        raise HTTPException(status_code=409, detail="That handle is taken")
    try:
        _db_exec("UPDATE users SET handle=?, handle_key=? WHERE id=?", (display, key, int(user_id)))
    except Exception as exc:
        # The unique index is the real arbiter: two people claiming the same
        # handle in the same instant both pass the check above, and exactly one
        # of them gets past this.
        raise HTTPException(status_code=409, detail="That handle is taken") from exc
    return display


def ensure_handle(user_id: int, display_name: Optional[str] = None,
                  email: Optional[str] = None) -> Optional[str]:
    """Give an account a handle if it has none. Never raises."""
    try:
        row = _db_query_one(
            "SELECT handle, handle_key, display_name, email FROM users WHERE id=? LIMIT 1",
            (int(user_id),),
        )
        if not row:
            return None
        existing = row["handle"] if "handle" in row.keys() else None
        if existing:
            return str(existing)
        candidate = suggest_handle(
            display_name if display_name is not None else row["display_name"],
            email if email is not None else row["email"],
        )
        _db_exec(
            "UPDATE users SET handle=?, handle_key=? WHERE id=?",
            (candidate, handle_key(candidate), int(user_id)),
        )
        return candidate
    except Exception:
        return None


def backfill_handles(limit: int = 500) -> int:
    """Hand out handles to accounts that predate them.

    Runs at startup in bounded batches. Everyone who existed before this feature
    has no handle, and a profile that cannot be linked to is the thing Phase 1
    exists to fix -- so they get one without having to log in and pick.
    """
    try:
        rows = _db_query_all(
            "SELECT id, display_name, email FROM users "
            "WHERE handle_key IS NULL OR handle_key='' LIMIT ?",
            (int(limit),),
        )
    except Exception:
        return 0
    claimed: set = set()
    done = 0
    for row in rows:
        try:
            candidate = suggest_handle(row["display_name"], row["email"], taken=claimed)
            _db_exec(
                "UPDATE users SET handle=?, handle_key=? WHERE id=?",
                (candidate, handle_key(candidate), int(row["id"])),
            )
            claimed.add(handle_key(candidate))
            done += 1
        except Exception:
            continue
    return done


# --------------------------------------------------------------------------
# bio and driver tags
# --------------------------------------------------------------------------

def clean_bio(bio: Optional[str]) -> Optional[str]:
    if bio is None:
        return None
    collapsed = " ".join(str(bio).split())[:MAX_BIO_CHARS].strip()
    return collapsed or None


def clean_platforms(platforms: Optional[List[str]]) -> Optional[str]:
    """A closed set, stored as a comma-separated string.

    Free text would become four hundred spellings of "uber", which cannot be
    filtered on and cannot find anyone.
    """
    if platforms is None:
        return None
    seen: List[str] = []
    for item in platforms[:MAX_TAGS]:
        value = str(item or "").strip().lower()[:MAX_TAG_CHARS]
        if value in PLATFORM_CHOICES and value not in seen:
            seen.append(value)
    return ",".join(seen) or None


def clean_vehicle(vehicle: Optional[str]) -> Optional[str]:
    if vehicle is None:
        return None
    value = str(vehicle).strip().lower()
    return value if value in VEHICLE_CHOICES else None


def clean_since_year(year: Optional[int]) -> Optional[int]:
    if year is None:
        return None
    try:
        value = int(year)
    except (TypeError, ValueError):
        return None
    # Rideshare did not exist before 2009, and a year in the future is a typo.
    if 2009 <= value <= _now_year():
        return value
    raise HTTPException(status_code=400, detail=f"Year must be between 2009 and {_now_year()}")


def split_platforms(stored: Any) -> List[str]:
    if not stored:
        return []
    return [p for p in str(stored).split(",") if p]


def update_identity(user_id: int, *, bio: Any = ..., platforms: Any = ...,
                    vehicle_type: Any = ..., driving_since_year: Any = ...) -> Dict[str, Any]:
    """Patch semantics: a field that was not sent is not touched.

    `...` rather than None as the "absent" marker, because None is a legitimate
    value here -- it is how a driver clears their bio.
    """
    sets: List[str] = []
    params: List[Any] = []
    if bio is not ...:
        sets.append("bio=?")
        params.append(clean_bio(bio))
    if platforms is not ...:
        sets.append("platforms=?")
        params.append(clean_platforms(platforms))
    if vehicle_type is not ...:
        sets.append("vehicle_type=?")
        params.append(clean_vehicle(vehicle_type))
    if driving_since_year is not ...:
        sets.append("driving_since_year=?")
        params.append(clean_since_year(driving_since_year))
    if sets:
        params.append(int(user_id))
        _db_exec(f"UPDATE users SET {', '.join(sets)} WHERE id=?", tuple(params))
    return {"updated": len(sets)}
