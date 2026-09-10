"""Admin-issued free-access codes.

grant_comp already covers "admin gives THIS user free access" -- it needs a
user_id, so the person must already have an account the admin can look up. This
covers the other shape: the admin mints a code, hands it to someone, and that
person redeems it themselves.

Access is granted through the EXISTING comp columns rather than a parallel
mechanism, so redeemed access travels the same ladder, shows the same way in
/me, and is visible to the same admin tooling. A second source of access would
be a second thing to keep in sync, and this codebase has already been bitten by
exactly that.

Two independent expiries, because they answer different questions:
  * redeem_by  -- how long the CODE can still be used (an unredeemed code goes
                  stale; a leaked one stops working)
  * access_days -- how long the ACCESS lasts once redeemed
Either may be None, meaning "no limit".
"""
from __future__ import annotations

import logging
import secrets
import time
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from core import _db_exec, _db_query_all, _db_query_one, _db_run_in_transaction, _sql

logger = logging.getLogger(__name__)

# Deliberately excludes I, O, 0, 1 -- codes get read aloud, written down, and
# retyped on a phone, and those four are where that goes wrong.
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
CODE_PREFIX = "JOSEO"
CODE_GROUP_LEN = 4
CODE_GROUPS = 2

MAX_NOTE_LEN = 200
MAX_USES_LIMIT = 10_000


def generate_code() -> str:
    """A short, unambiguous, cryptographically random code."""
    groups = [
        "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_GROUP_LEN))
        for _ in range(CODE_GROUPS)
    ]
    return f"{CODE_PREFIX}-" + "-".join(groups)


def normalize_code(raw: Any) -> str:
    """Canonical form of whatever the user typed.

    People paste codes with stray spaces, lowercase them, or drop the dashes.
    Normalising on both write and read means all of those match the stored code
    instead of failing with "invalid code" on a code that is perfectly valid.
    """
    if raw is None:
        return ""
    text = str(raw).strip().upper()
    text = text.replace(" ", "").replace("_", "").replace("-", "")
    if not text:
        return ""
    if text.startswith(CODE_PREFIX):
        body = text[len(CODE_PREFIX):]
    else:
        body = text
    if not body:
        return ""
    groups = [body[i:i + CODE_GROUP_LEN] for i in range(0, len(body), CODE_GROUP_LEN)]
    return f"{CODE_PREFIX}-" + "-".join(groups)


def _now() -> int:
    return int(time.time())


def comp_reason_for(code: str) -> str:
    """The comp reason a redemption of `code` writes.

    Withdrawal matches on this string, so it has to be produced in exactly one
    place. If the grant and the withdrawal ever disagree about the wording,
    revoking a code silently stops removing the access it granted.
    """
    return f"access code {code}"


def _days_to_seconds(days: Optional[int]) -> Optional[int]:
    if days is None:
        return None
    return int(days) * 86400


def create_access_token(
    *,
    actor_user_id: int,
    access_days: Optional[int] = None,
    redeem_by_days: Optional[int] = None,
    max_uses: int = 1,
    note: str = "",
) -> Dict[str, Any]:
    """Mint a code. access_days/redeem_by_days of None mean no limit."""
    if access_days is not None and int(access_days) <= 0:
        raise HTTPException(status_code=400, detail="access_days must be positive, or omitted for unlimited")
    if redeem_by_days is not None and int(redeem_by_days) <= 0:
        raise HTTPException(status_code=400, detail="redeem_by_days must be positive, or omitted for no expiry")
    uses_allowed = int(max_uses)
    if uses_allowed < 1 or uses_allowed > MAX_USES_LIMIT:
        raise HTTPException(status_code=400, detail=f"max_uses must be between 1 and {MAX_USES_LIMIT}")

    now = _now()
    redeem_by = None
    if redeem_by_days is not None:
        redeem_by = now + _days_to_seconds(int(redeem_by_days))

    # Collision is astronomically unlikely (32^8), but a duplicate primary key
    # would 500 on the admin instead of just minting another code.
    for _ in range(5):
        code = generate_code()
        if _db_query_one("SELECT code FROM access_tokens WHERE code=? LIMIT 1", (code,)) is None:
            break
    else:
        raise HTTPException(status_code=500, detail="Could not allocate a unique code")

    _db_exec(
        """
        INSERT INTO access_tokens
            (code, created_by, created_at, access_days, redeem_by, max_uses, uses, revoked_at, note)
        VALUES (?, ?, ?, ?, ?, ?, 0, NULL, ?)
        """,
        (code, int(actor_user_id), now, access_days, redeem_by, uses_allowed, str(note or "")[:MAX_NOTE_LEN]),
    )
    return {
        "ok": True,
        "code": code,
        "access_days": access_days,
        "grants_forever": access_days is None,
        "redeem_by": redeem_by,
        "max_uses": uses_allowed,
        "uses": 0,
        "note": str(note or "")[:MAX_NOTE_LEN],
        "created_at": now,
    }


def _token_state(row, now: Optional[int] = None) -> str:
    now = _now() if now is None else now
    if row["revoked_at"] is not None:
        return "revoked"
    if int(row["uses"] or 0) >= int(row["max_uses"] or 1):
        return "used_up"
    redeem_by = row["redeem_by"]
    if redeem_by is not None and int(redeem_by) <= now:
        return "expired"
    return "active"


def list_access_tokens(limit: int = 100, offset: int = 0, include_inactive: bool = True) -> Dict[str, Any]:
    rows = _db_query_all(
        """
        SELECT code, created_by, created_at, access_days, redeem_by, max_uses, uses, revoked_at, note
        FROM access_tokens
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
        """,
        (int(limit), int(offset)),
    ) or []
    now = _now()
    items = []
    for row in rows:
        state = _token_state(row, now)
        if not include_inactive and state != "active":
            continue
        items.append({
            "code": row["code"],
            "state": state,
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "access_days": row["access_days"],
            "grants_forever": row["access_days"] is None,
            "redeem_by": row["redeem_by"],
            "max_uses": row["max_uses"],
            "uses": row["uses"],
            "revoked_at": row["revoked_at"],
            "note": row["note"],
        })
    total_row = _db_query_one("SELECT COUNT(*) AS c FROM access_tokens")
    return {"ok": True, "items": items, "total": int(total_row["c"]) if total_row else 0}


def _require_token_row(code: str):
    normalized = normalize_code(code)
    if not normalized:
        raise HTTPException(status_code=400, detail="Enter a code")
    row = _db_query_one(
        "SELECT code, created_at, access_days, redeem_by, max_uses, uses, revoked_at, note"
        " FROM access_tokens WHERE code=? LIMIT 1",
        (normalized,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Code not found")
    return normalized, row


def list_token_redemptions(code: str) -> Dict[str, Any]:
    """Who redeemed this code, and whether their access still comes from it.

    Needed for revocation to be a decision rather than a guess: `traces_to_code`
    is exactly the condition a withdrawal acts on, so the admin can see who
    would lose access before choosing to remove it.
    """
    normalized, _row = _require_token_row(code)
    reason = comp_reason_for(normalized)
    rows = _db_query_all(
        """
        SELECT r.user_id            AS user_id,
               r.redeemed_at        AS redeemed_at,
               u.email              AS email,
               u.display_name       AS display_name,
               u.subscription_status            AS subscription_status,
               u.subscription_comp_reason       AS comp_reason,
               u.subscription_comp_expires_at   AS comp_expires_at
        FROM access_token_redemptions r
        LEFT JOIN users u ON u.id = r.user_id
        WHERE r.code = ?
        ORDER BY r.redeemed_at DESC
        """,
        (normalized,),
    ) or []

    from subscription_state import COMP_STATUS, normalize_status

    now = _now()
    items = []
    for row in rows:
        status = normalize_status(row["subscription_status"])
        expires = row["comp_expires_at"]
        comp_live = status == COMP_STATUS and (expires is None or int(expires) > now)
        items.append({
            "user_id": row["user_id"],
            "email": row["email"],
            "display_name": row["display_name"],
            "redeemed_at": row["redeemed_at"],
            "subscription_status": row["subscription_status"],
            "comp_reason": row["comp_reason"],
            "comp_expires_at": expires,
            "comp_is_forever": comp_live and expires is None,
            "comp_active": comp_live,
            # True == a withdrawal on this code would take this person's access.
            "traces_to_code": comp_live and str(row["comp_reason"] or "") == reason,
        })
    return {"ok": True, "code": normalized, "items": items, "total": len(items)}


def _withdraw_comp_for_code(*, actor_user_id: int, code: str) -> Dict[str, Any]:
    """Pull the comp from everyone whose access still traces to `code`.

    Routed through revoke_comp so a redeemer who ALSO pays keeps their paid
    subscription -- that function already drops them back to "active" rather
    than "none" when their billing period is still open. Reimplementing the
    UPDATE here would quietly cut off a paying customer.
    """
    from admin_mutation_service import revoke_comp

    listing = list_token_redemptions(code)
    withdrawn: List[Dict[str, Any]] = []
    kept: List[Dict[str, Any]] = []
    for item in listing["items"]:
        if not item["traces_to_code"]:
            kept.append({
                "user_id": item["user_id"],
                "email": item["email"],
                "reason": "access does not come from this code" if item["comp_active"] else "no active comp",
            })
            continue
        try:
            result = revoke_comp(actor_user_id=int(actor_user_id), user_id=int(item["user_id"]))
        except HTTPException as exc:
            # A deleted account should not abort withdrawal for everyone else.
            kept.append({"user_id": item["user_id"], "email": item["email"], "reason": str(exc.detail)})
            continue
        withdrawn.append({
            "user_id": item["user_id"],
            "email": item["email"],
            "status_after": result.get("status"),
        })
    logger.info(
        "access_token_access_withdrawn code=%s by=%s withdrawn=%d kept=%d",
        listing["code"], actor_user_id, len(withdrawn), len(kept),
    )
    return {"withdrawn": withdrawn, "kept": kept}


def revoke_access_token(
    *,
    actor_user_id: int,
    code: str,
    withdraw_access: bool = False,
) -> Dict[str, Any]:
    """Stop a code being redeemed, and optionally take back what it granted.

    The two halves are separate on purpose, because they answer different
    questions. Revoking a leaked code should not by default cut off the people
    it was legitimately given to -- but an admin who wants that must be able to
    say so, which is what withdraw_access is for.

    Revocation works from any state. A used-up or expired code cannot be
    redeemed anyway, but revoking it is still the handle for withdrawing the
    access it already handed out.
    """
    normalized, row = _require_token_row(code)
    already_revoked = row["revoked_at"] is not None
    if not already_revoked:
        _db_exec("UPDATE access_tokens SET revoked_at=? WHERE code=?", (_now(), normalized))
        logger.info("access_token_revoked code=%s by=%s", normalized, actor_user_id)

    result: Dict[str, Any] = {
        "ok": True,
        "code": normalized,
        "already_revoked": already_revoked,
        "access_withdrawn": bool(withdraw_access),
        "withdrawn": [],
        "kept": [],
    }
    if withdraw_access:
        result.update(_withdraw_comp_for_code(actor_user_id=actor_user_id, code=normalized))
    result["withdrawn_count"] = len(result["withdrawn"])
    result["kept_count"] = len(result["kept"])
    return result


def restore_access_token(*, actor_user_id: int, code: str) -> Dict[str, Any]:
    """Un-revoke a code, so a revoke made in error is not a one-way door.

    Clearing revoked_at does not necessarily make the code usable again -- it
    may still be used up or past its redeem-by -- so the resulting state is
    returned rather than assumed.
    """
    normalized, row = _require_token_row(code)
    if row["revoked_at"] is None:
        return {"ok": True, "code": normalized, "was_revoked": False, "state": _token_state(row)}
    _db_exec("UPDATE access_tokens SET revoked_at=NULL WHERE code=?", (normalized,))
    logger.info("access_token_restored code=%s by=%s", normalized, actor_user_id)
    refreshed = _db_query_one(
        "SELECT redeem_by, max_uses, uses, revoked_at FROM access_tokens WHERE code=? LIMIT 1",
        (normalized,),
    )
    return {
        "ok": True,
        "code": normalized,
        "was_revoked": True,
        "state": _token_state(refreshed) if refreshed else "active",
    }


def revoke_redemption(*, actor_user_id: int, code: str, user_id: int) -> Dict[str, Any]:
    """Take one person's access back without touching the code itself.

    The case this exists for: a multi-use code handed to a group, one of whom
    should no longer have access. Revoking the code would punish everyone.
    """
    from admin_mutation_service import revoke_comp

    normalized, _row = _require_token_row(code)
    listing = list_token_redemptions(normalized)
    match = next((i for i in listing["items"] if int(i["user_id"]) == int(user_id)), None)
    if match is None:
        raise HTTPException(status_code=404, detail="That user has not redeemed this code")
    if not match["traces_to_code"]:
        # Refusing beats silently doing nothing, and beats removing a comp this
        # code did not grant.
        raise HTTPException(
            status_code=409,
            detail="That user's access does not come from this code — remove it from Comps instead",
        )
    result = revoke_comp(actor_user_id=int(actor_user_id), user_id=int(user_id))
    logger.info("access_token_redemption_revoked code=%s user=%s by=%s", normalized, user_id, actor_user_id)
    return {"ok": True, "code": normalized, "user_id": int(user_id), "status_after": result.get("status")}


BULK_SCAN_PAGE = 500


def _active_codes() -> List[str]:
    """Every code currently in the `active` state, oldest first.

    Pages over raw rows and reuses _token_state rather than re-expressing
    "active" as SQL: two definitions of the same thing would eventually
    disagree, and the SQL copy is the one nothing would notice was wrong.
    """
    codes: List[str] = []
    offset = 0
    while True:
        rows = _db_query_all(
            """
            SELECT code, redeem_by, max_uses, uses, revoked_at
            FROM access_tokens
            ORDER BY created_at ASC
            LIMIT ? OFFSET ?
            """,
            (BULK_SCAN_PAGE, offset),
        ) or []
        if not rows:
            return codes
        now = _now()
        codes.extend(row["code"] for row in rows if _token_state(row, now) == "active")
        if len(rows) < BULK_SCAN_PAGE:
            return codes
        offset += BULK_SCAN_PAGE


def _codes_with_redemptions() -> List[str]:
    rows = _db_query_all(
        "SELECT DISTINCT code FROM access_token_redemptions ORDER BY code ASC"
    ) or []
    return [row["code"] for row in rows]


def revoke_all_active_tokens(*, actor_user_id: int, withdraw_access: bool = False) -> Dict[str, Any]:
    """Kill every code that can still be redeemed. The leaked-everywhere button.

    The two halves deliberately cover different sets:

      * revocation touches only `active` codes. Revoking a used-up or expired
        code changes nothing about whether it can be redeemed, and counting it
        would overstate what happened.
      * withdrawal covers every code that has EVER been redeemed. Spent codes
        are precisely the ones whose access is already out in the wild, so a
        "take it all back" that skipped them would take back almost nothing.
    """
    revoked: List[Dict[str, Any]] = []
    for code in _active_codes():
        revoke_access_token(actor_user_id=actor_user_id, code=code, withdraw_access=False)
        revoked.append({"code": code})

    withdrawn_total = 0
    withdrawn_codes: List[Dict[str, Any]] = []
    if withdraw_access:
        for code in _codes_with_redemptions():
            outcome = _withdraw_comp_for_code(actor_user_id=actor_user_id, code=code)
            count = len(outcome["withdrawn"])
            withdrawn_total += count
            if count:
                withdrawn_codes.append({"code": code, "withdrawn_count": count})

    logger.info(
        "access_tokens_bulk_revoked revoked=%d withdraw_access=%s withdrawn=%d by=%s",
        len(revoked), withdraw_access, withdrawn_total, actor_user_id,
    )
    return {
        "ok": True,
        "revoked_count": len(revoked),
        "withdrawn_count": withdrawn_total,
        "access_withdrawn": bool(withdraw_access),
        "codes": revoked,
        "withdrawn_codes": withdrawn_codes,
    }


def redeem_access_token(*, user_id: int, code: str) -> Dict[str, Any]:
    """Redeem a code for the current user and grant the comp it carries."""
    normalized = normalize_code(code)
    if not normalized:
        raise HTTPException(status_code=400, detail="Enter a code")

    row = _db_query_one(
        "SELECT code, access_days, redeem_by, max_uses, uses, revoked_at FROM access_tokens WHERE code=? LIMIT 1",
        (normalized,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="That code isn't valid")

    now = _now()
    state = _token_state(row, now)
    if state == "revoked":
        raise HTTPException(status_code=410, detail="That code has been revoked")
    if state == "expired":
        raise HTTPException(status_code=410, detail="That code has expired")
    if state == "used_up":
        raise HTTPException(status_code=409, detail="That code has already been used")

    already = _db_query_one(
        "SELECT id FROM access_token_redemptions WHERE code=? AND user_id=? LIMIT 1",
        (normalized, int(user_id)),
    )
    if already:
        raise HTTPException(status_code=409, detail="You've already used that code")

    # Claim a use atomically. Without the `uses < max_uses` guard in the UPDATE,
    # two people redeeming a single-use code at the same moment would both read
    # uses=0 and both be granted.
    def _claim(conn, cur) -> int:
        cur.execute(
            _sql("UPDATE access_tokens SET uses = uses + 1 WHERE code=? AND uses < max_uses AND revoked_at IS NULL"),
            (normalized,),
        )
        return int(cur.rowcount or 0)

    try:
        claimed = _db_run_in_transaction(_claim)
    except Exception as exc:
        logger.error("access token claim failed code=%s user=%s: %s", normalized, user_id, exc)
        raise HTTPException(status_code=500, detail="Could not redeem that code")
    if claimed != 1:
        raise HTTPException(status_code=409, detail="That code has already been used")

    access_days = row["access_days"]
    new_expiry = None if access_days is None else now + _days_to_seconds(int(access_days))

    granted = _apply_comp_from_token(user_id=int(user_id), new_expiry=new_expiry, code=normalized, now=now)

    _db_exec(
        "INSERT INTO access_token_redemptions (code, user_id, redeemed_at) VALUES (?, ?, ?)",
        (normalized, int(user_id), now),
    )
    logger.info("access_token_redeemed code=%s user=%s expires=%s", normalized, user_id, new_expiry)

    return {
        "ok": True,
        "code": normalized,
        "access_expires_at": granted["comp_expires_at"],
        "grants_forever": granted["comp_expires_at"] is None,
    }


def _apply_comp_from_token(*, user_id: int, new_expiry: Optional[int], code: str, now: int) -> Dict[str, Any]:
    """Grant the comp, without ever shortening access the user already has.

    A code is a gift. If someone redeems a 7-day code while holding a 30-day comp
    -- or a forever comp -- naively writing the new expiry would take 23 days off
    them. So the longer window wins, and None (forever) beats every date.

    The reason field follows the same rule: it names the code only when the code
    is what is actually providing the access. Overwriting it unconditionally
    would mis-attribute a stronger pre-existing comp to this code, and revoking
    the code with withdrawal would then strip a grant it never made.
    """
    existing = _db_query_one(
        "SELECT subscription_status, subscription_comp_expires_at, subscription_comp_reason"
        " FROM users WHERE id=? LIMIT 1",
        (int(user_id),),
    )
    if not existing:
        raise HTTPException(status_code=404, detail="User not found")

    from subscription_state import COMP_STATUS, normalize_status

    effective_expiry = new_expiry
    reason = comp_reason_for(code)
    if normalize_status(existing["subscription_status"]) == COMP_STATUS:
        current = existing["subscription_comp_expires_at"]
        if current is None:
            effective_expiry = None          # already forever; keep it
        elif new_expiry is not None:
            effective_expiry = max(int(current), int(new_expiry))
        # The existing comp already covers this window, so the code added
        # nothing -- leave the credit (and the withdrawal handle) where it was.
        if effective_expiry == current and existing["subscription_comp_reason"]:
            reason = str(existing["subscription_comp_reason"])

    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_comp_reason=?,
            subscription_comp_granted_at=?,
            subscription_comp_expires_at=?,
            subscription_updated_at=?
        WHERE id=?
        """,
        ("comp", reason, now, effective_expiry, now, int(user_id)),
    )
    return {"comp_expires_at": effective_expiry, "comp_reason": reason}


def create_access_token_tables_sql(backend: str) -> List[str]:
    """DDL for one backend. Epoch seconds throughout, matching the users table.

    The autoincrement key is the only real divergence -- SQLite spells it
    `INTEGER PRIMARY KEY AUTOINCREMENT`, Postgres `BIGSERIAL PRIMARY KEY` -- and
    getting it wrong fails at startup on whichever backend wasn't tested.
    """
    redemption_pk = (
        "id BIGSERIAL PRIMARY KEY" if backend == "postgres" else "id INTEGER PRIMARY KEY AUTOINCREMENT"
    )
    return [
        """
        CREATE TABLE IF NOT EXISTS access_tokens (
            code TEXT PRIMARY KEY,
            created_by INTEGER NULL,
            created_at INTEGER NOT NULL,
            access_days INTEGER NULL,
            redeem_by INTEGER NULL,
            max_uses INTEGER NOT NULL DEFAULT 1,
            uses INTEGER NOT NULL DEFAULT 0,
            revoked_at INTEGER NULL,
            note TEXT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS access_token_redemptions (
            {redemption_pk},
            code TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            redeemed_at INTEGER NOT NULL
        )
        """,
        # Enforces "one redemption per user per code" in the database, so the
        # check in redeem_access_token cannot be raced past.
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_access_token_redemption_once ON access_token_redemptions (code, user_id)",
        "CREATE INDEX IF NOT EXISTS idx_access_tokens_created ON access_tokens (created_at)",
    ]
