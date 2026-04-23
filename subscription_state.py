"""Subscription state helpers. Pure functions, no FastAPI imports, no DB side effects."""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

from core import ENFORCE_TRIAL


def get_subscription_fields(user_row) -> Dict[str, Any]:
    """Extract all subscription_* columns from a user row into a plain dict."""

    def _get(key):
        try:
            return user_row[key] if key in user_row.keys() else None
        except Exception:
            return None

    return {
        "status": _get("subscription_status"),
        "provider": _get("subscription_provider"),
        "customer_id": _get("subscription_customer_id"),
        "subscription_id": _get("subscription_id"),
        "current_period_end": _get("subscription_current_period_end"),
        "comp_reason": _get("subscription_comp_reason"),
        "comp_granted_by": _get("subscription_comp_granted_by"),
        "comp_granted_at": _get("subscription_comp_granted_at"),
        "comp_expires_at": _get("subscription_comp_expires_at"),
        "updated_at": _get("subscription_updated_at"),
    }


def is_subscription_active(user_row) -> bool:
    """True if user still has paid-period access based on status + future period end."""
    fields = get_subscription_fields(user_row)
    status = str(fields["status"] or "").strip().lower()
    if status not in {"active", "past_due"}:
        return False
    period_end = fields["current_period_end"]
    if period_end is None:
        return False
    try:
        return int(time.time()) < int(period_end)
    except Exception:
        return False


def is_comp_active(user_row) -> bool:
    """True if user has an active admin-granted comp (status='comp' and not expired).

    A NULL or non-positive comp_expires_at is treated as a forever comp, matching
    the server-side access check in core._enforce_access_or_admin. Keeping both
    in sync prevents the /me payload from reporting has_access=false while the
    server is still granting access.
    """
    fields = get_subscription_fields(user_row)
    if fields["status"] != "comp":
        return False
    comp_expires = fields["comp_expires_at"]
    if comp_expires is None:
        return True
    try:
        comp_expires_int = int(comp_expires)
    except Exception:
        return False
    if comp_expires_int <= 0:
        return True
    return int(time.time()) < comp_expires_int


def is_comp_forever(user_row) -> bool:
    """True if user has comp with no expiration (NULL or non-positive comp_expires_at)."""
    fields = get_subscription_fields(user_row)
    if fields["status"] != "comp":
        return False
    comp_expires = fields["comp_expires_at"]
    if comp_expires is None:
        return True
    try:
        return int(comp_expires) <= 0
    except Exception:
        return False


def is_trial_active(user_row) -> bool:
    """True if user has an active trial (trial_expires_at in the future)."""
    try:
        trial_expires = user_row["trial_expires_at"] if "trial_expires_at" in user_row.keys() else None
        if trial_expires is None:
            return False
        return int(time.time()) < int(trial_expires)
    except Exception:
        return False


def has_access(user_row) -> bool:
    """Master access check — mirrors _enforce_access_or_admin logic for use in responses.

    When ENFORCE_TRIAL is false, access is ungated for any authenticated user,
    so this returns True unconditionally (matches _enforce_access_or_admin early-return).
    """
    if not ENFORCE_TRIAL:
        return True
    try:
        if int(user_row["is_admin"]) == 1:
            return True
    except Exception:
        pass
    if is_comp_active(user_row):
        return True
    if is_subscription_active(user_row):
        return True
    if is_trial_active(user_row):
        return True
    return False


def days_until_subscription_ends(user_row) -> Optional[int]:
    """Days until subscription_current_period_end, or None if no active subscription."""
    if not is_subscription_active(user_row):
        return None
    fields = get_subscription_fields(user_row)
    try:
        seconds = int(fields["current_period_end"]) - int(time.time())
        return max(0, seconds // 86400)
    except Exception:
        return None


def days_until_comp_ends(user_row) -> Optional[int]:
    """Days until comp_expires_at, or None if no comp or forever comp."""
    if not is_comp_active(user_row):
        return None
    if is_comp_forever(user_row):
        return None
    fields = get_subscription_fields(user_row)
    try:
        seconds = int(fields["comp_expires_at"]) - int(time.time())
        return max(0, seconds // 86400)
    except Exception:
        return None


def days_until_trial_ends(user_row) -> Optional[int]:
    """Days until trial_expires_at, or None if no active trial."""
    if not is_trial_active(user_row):
        return None
    try:
        trial_expires = user_row["trial_expires_at"]
        seconds = int(trial_expires) - int(time.time())
        return max(0, seconds // 86400)
    except Exception:
        return None


def build_subscription_response(user_row) -> Dict[str, Any]:
    """Build the `subscription` object returned by /me."""
    fields = get_subscription_fields(user_row)

    days_remaining = None
    if is_comp_active(user_row):
        days_remaining = days_until_comp_ends(user_row)
    elif is_subscription_active(user_row):
        days_remaining = days_until_subscription_ends(user_row)
    elif is_trial_active(user_row):
        days_remaining = days_until_trial_ends(user_row)

    try:
        trial_expires = user_row["trial_expires_at"] if "trial_expires_at" in user_row.keys() else None
    except Exception:
        trial_expires = None

    return {
        "status": fields["status"] or "none",
        "trial_expires_at": int(trial_expires) if trial_expires is not None else None,
        "subscription_current_period_end": int(fields["current_period_end"]) if fields["current_period_end"] is not None else None,
        "comp_expires_at": int(fields["comp_expires_at"]) if fields["comp_expires_at"] is not None else None,
        "comp_reason": fields["comp_reason"],
        "is_comp_forever": is_comp_forever(user_row),
        "days_remaining": days_remaining,
        "has_access": has_access(user_row),
    }
