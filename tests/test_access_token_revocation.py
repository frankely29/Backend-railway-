"""Full admin control over a code after it has been handed out.

Revoking used to be one-way and one-dimensional: it blocked future redemptions
and nothing else. These cover the rest of the surface -- withdrawing the access
a code granted, un-revoking, removing one redeemer, and the bulk kill switch --
against a real SQLite database, because every one of them is about what happens
to rows in `users`.

The safety properties are the point of most of these. A withdrawal must not take
access it did not grant, and must not cut off someone who also pays.
"""
from __future__ import annotations

import sqlite3
import time

import pytest
from fastapi import HTTPException

import access_tokens as at
import admin_mutation_service as ams


@pytest.fixture
def db(tmp_path, monkeypatch):
    """Throwaway SQLite wired into BOTH modules' DB helpers.

    revoke_comp lives in admin_mutation_service and holds its own imported
    references, so patching only access_tokens would let the withdrawal path
    write to the real database.
    """
    path = tmp_path / "t.db"
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE users (
            id INTEGER PRIMARY KEY, email TEXT, display_name TEXT,
            is_admin INTEGER DEFAULT 0,
            trial_expires_at INTEGER, subscription_status TEXT,
            subscription_current_period_end INTEGER, subscription_id TEXT,
            subscription_comp_reason TEXT,
            subscription_comp_granted_by INTEGER,
            subscription_comp_granted_at INTEGER,
            subscription_comp_expires_at INTEGER, subscription_updated_at INTEGER
        )"""
    )
    for ddl in at.create_access_token_tables_sql("sqlite"):
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO users (id, email, display_name) VALUES (?,?,?)",
        [(1, "admin@x.com", "Admin"), (2, "b@x.com", "Bee"),
         (3, "c@x.com", "Cee"), (4, "d@x.com", "Dee")],
    )
    conn.commit()

    def _exec(sql, params=()):
        conn.execute(sql, params); conn.commit()

    def _one(sql, params=()):
        return conn.execute(sql, params).fetchone()

    def _all(sql, params=()):
        return conn.execute(sql, params).fetchall()

    def _txn(fn):
        cur = conn.cursor()
        out = fn(conn, cur)
        conn.commit()
        return out

    for mod in (at, ams):
        monkeypatch.setattr(mod, "_db_exec", _exec)
        monkeypatch.setattr(mod, "_db_query_one", _one)
        monkeypatch.setattr(mod, "_db_query_all", _all)
        monkeypatch.setattr(mod, "_db_run_in_transaction", _txn)
        monkeypatch.setattr(mod, "_sql", lambda s: s)
    yield conn
    conn.close()


def _user(db, uid):
    return db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()


def _mint(**kw):
    kw.setdefault("actor_user_id", 1)
    kw.setdefault("access_days", 30)
    return at.create_access_token(**kw)["code"]


# --------------------------------------------------------------------------
# revoke without withdrawal keeps the shipped meaning
# --------------------------------------------------------------------------

def test_plain_revoke_blocks_redemption_but_keeps_granted_access(db):
    code = _mint()
    at.redeem_access_token(user_id=2, code=code)
    assert _user(db, 2)["subscription_status"] == "comp"

    out = at.revoke_access_token(actor_user_id=1, code=code)

    assert out["access_withdrawn"] is False
    assert out["withdrawn_count"] == 0
    assert _user(db, 2)["subscription_status"] == "comp", "a plain revoke must not touch granted access"
    with pytest.raises(HTTPException) as exc:
        at.redeem_access_token(user_id=3, code=code)
    assert exc.value.status_code == 410


def test_revoke_is_idempotent(db):
    code = _mint()
    assert at.revoke_access_token(actor_user_id=1, code=code)["already_revoked"] is False
    assert at.revoke_access_token(actor_user_id=1, code=code)["already_revoked"] is True


def test_used_up_code_can_still_be_revoked(db):
    """The handle for withdrawing access has to work on a spent code."""
    code = _mint(max_uses=1)
    at.redeem_access_token(user_id=2, code=code)
    out = at.revoke_access_token(actor_user_id=1, code=code, withdraw_access=True)
    assert out["withdrawn_count"] == 1
    assert _user(db, 2)["subscription_status"] == "none"


# --------------------------------------------------------------------------
# withdrawal
# --------------------------------------------------------------------------

def test_withdraw_removes_access_from_every_redeemer(db):
    code = _mint(max_uses=3)
    for uid in (2, 3, 4):
        at.redeem_access_token(user_id=uid, code=code)

    out = at.revoke_access_token(actor_user_id=1, code=code, withdraw_access=True)

    assert out["access_withdrawn"] is True
    assert out["withdrawn_count"] == 3
    assert out["kept_count"] == 0
    assert {w["user_id"] for w in out["withdrawn"]} == {2, 3, 4}
    for uid in (2, 3, 4):
        row = _user(db, uid)
        assert row["subscription_status"] == "none"
        assert row["subscription_comp_expires_at"] is None
        assert row["subscription_comp_reason"] is None


def test_withdraw_does_not_touch_a_comp_from_a_different_code(db):
    """Two codes, two people. Revoking one must not disturb the other."""
    code_a, code_b = _mint(), _mint()
    at.redeem_access_token(user_id=2, code=code_a)
    at.redeem_access_token(user_id=3, code=code_b)

    at.revoke_access_token(actor_user_id=1, code=code_a, withdraw_access=True)

    assert _user(db, 2)["subscription_status"] == "none"
    assert _user(db, 3)["subscription_status"] == "comp", "code B's redeemer is unrelated"


def test_withdraw_leaves_a_manually_granted_forever_comp_alone(db):
    """The mis-attribution trap.

    A user already holding a forever comp redeems a 30-day code. The code adds
    nothing, so it must not become the thing that owns their access -- otherwise
    revoking it would strip a grant it never made.
    """
    db.execute(
        "UPDATE users SET subscription_status='comp', subscription_comp_reason='press partner',"
        " subscription_comp_expires_at=NULL WHERE id=2"
    )
    db.commit()
    code = _mint(access_days=30)
    at.redeem_access_token(user_id=2, code=code)
    assert _user(db, 2)["subscription_comp_reason"] == "press partner"

    out = at.revoke_access_token(actor_user_id=1, code=code, withdraw_access=True)

    assert out["withdrawn_count"] == 0
    assert out["kept_count"] == 1
    row = _user(db, 2)
    assert row["subscription_status"] == "comp"
    assert row["subscription_comp_expires_at"] is None, "their forever comp survives"


def test_withdraw_shortens_nothing_when_the_code_extended_an_existing_comp(db):
    """If the code DID extend access, it owns the extension and can take it."""
    now = int(time.time())
    db.execute(
        "UPDATE users SET subscription_status='comp', subscription_comp_reason='old grant',"
        " subscription_comp_expires_at=? WHERE id=2",
        (now + 5 * 86400,),
    )
    db.commit()
    code = _mint(access_days=90)
    at.redeem_access_token(user_id=2, code=code)
    assert _user(db, 2)["subscription_comp_reason"] == at.comp_reason_for(code)

    at.revoke_access_token(actor_user_id=1, code=code, withdraw_access=True)
    assert _user(db, 2)["subscription_status"] == "none"


def test_withdraw_keeps_a_paying_subscriber_paying(db):
    """A redeemer who also pays must not be cut off by a code revocation."""
    now = int(time.time())
    code = _mint()
    at.redeem_access_token(user_id=2, code=code)
    db.execute(
        "UPDATE users SET subscription_id='sub_123', subscription_current_period_end=? WHERE id=2",
        (now + 20 * 86400,),
    )
    db.commit()

    out = at.revoke_access_token(actor_user_id=1, code=code, withdraw_access=True)

    assert out["withdrawn_count"] == 1
    row = _user(db, 2)
    assert row["subscription_status"] == "active", "their real subscription takes over"
    assert row["subscription_comp_reason"] is None


def test_withdraw_ignores_an_already_expired_comp(db):
    code = _mint(access_days=1)
    at.redeem_access_token(user_id=2, code=code)
    db.execute(
        "UPDATE users SET subscription_comp_expires_at=? WHERE id=2", (int(time.time()) - 60,)
    )
    db.commit()

    out = at.revoke_access_token(actor_user_id=1, code=code, withdraw_access=True)
    assert out["withdrawn_count"] == 0
    assert out["kept"][0]["reason"] == "no active comp"


def test_withdraw_on_a_never_redeemed_code_is_a_no_op(db):
    code = _mint()
    out = at.revoke_access_token(actor_user_id=1, code=code, withdraw_access=True)
    assert (out["withdrawn_count"], out["kept_count"]) == (0, 0)


# --------------------------------------------------------------------------
# restore
# --------------------------------------------------------------------------

def test_restore_makes_a_revoked_code_redeemable_again(db):
    code = _mint()
    at.revoke_access_token(actor_user_id=1, code=code)
    out = at.restore_access_token(actor_user_id=1, code=code)

    assert out["was_revoked"] is True
    assert out["state"] == "active"
    assert at.redeem_access_token(user_id=2, code=code)["ok"] is True


def test_restore_reports_the_real_state_not_a_hopeful_one(db):
    """Un-revoking a spent code does not make it usable, and must not claim to."""
    code = _mint(max_uses=1)
    at.redeem_access_token(user_id=2, code=code)
    at.revoke_access_token(actor_user_id=1, code=code)

    out = at.restore_access_token(actor_user_id=1, code=code)
    assert out["state"] == "used_up"
    with pytest.raises(HTTPException):
        at.redeem_access_token(user_id=3, code=code)


def test_restore_on_a_live_code_is_a_no_op(db):
    code = _mint()
    out = at.restore_access_token(actor_user_id=1, code=code)
    assert out["was_revoked"] is False
    assert out["state"] == "active"


def test_restore_does_not_give_access_back(db):
    """Restore is about the code. It must not resurrect a withdrawn comp."""
    code = _mint()
    at.redeem_access_token(user_id=2, code=code)
    at.revoke_access_token(actor_user_id=1, code=code, withdraw_access=True)
    at.restore_access_token(actor_user_id=1, code=code)
    assert _user(db, 2)["subscription_status"] == "none"


# --------------------------------------------------------------------------
# per-redeemer revocation
# --------------------------------------------------------------------------

def test_revoke_one_redeemer_leaves_the_others_and_the_code_alone(db):
    code = _mint(max_uses=3)
    for uid in (2, 3, 4):
        at.redeem_access_token(user_id=uid, code=code)

    out = at.revoke_redemption(actor_user_id=1, code=code, user_id=3)

    assert out["status_after"] == "none"
    assert _user(db, 3)["subscription_status"] == "none"
    assert _user(db, 2)["subscription_status"] == "comp"
    assert _user(db, 4)["subscription_status"] == "comp"
    tokens = at.list_access_tokens()["items"]
    assert next(t for t in tokens if t["code"] == code)["state"] == "used_up"
    assert next(t for t in tokens if t["code"] == code)["revoked_at"] is None


def test_revoke_one_redeemer_rejects_a_non_redeemer(db):
    code = _mint()
    at.redeem_access_token(user_id=2, code=code)
    with pytest.raises(HTTPException) as exc:
        at.revoke_redemption(actor_user_id=1, code=code, user_id=4)
    assert exc.value.status_code == 404


def test_revoke_one_redeemer_refuses_when_access_came_from_elsewhere(db):
    """Better to refuse than to remove a comp this code did not grant."""
    db.execute(
        "UPDATE users SET subscription_status='comp', subscription_comp_reason='press partner',"
        " subscription_comp_expires_at=NULL WHERE id=2"
    )
    db.commit()
    code = _mint(access_days=30)
    at.redeem_access_token(user_id=2, code=code)

    with pytest.raises(HTTPException) as exc:
        at.revoke_redemption(actor_user_id=1, code=code, user_id=2)
    assert exc.value.status_code == 409
    assert _user(db, 2)["subscription_status"] == "comp"


# --------------------------------------------------------------------------
# visibility
# --------------------------------------------------------------------------

def test_redemption_listing_names_who_would_lose_access(db):
    code = _mint(max_uses=2)
    at.redeem_access_token(user_id=2, code=code)
    at.redeem_access_token(user_id=3, code=code)
    at.revoke_redemption(actor_user_id=1, code=code, user_id=3)

    listing = at.list_token_redemptions(code)

    assert listing["total"] == 2
    by_id = {i["user_id"]: i for i in listing["items"]}
    assert by_id[2]["traces_to_code"] is True
    assert by_id[2]["email"] == "b@x.com"
    assert by_id[2]["display_name"] == "Bee"
    assert by_id[3]["traces_to_code"] is False, "already withdrawn"
    assert by_id[3]["comp_active"] is False


def test_redemption_listing_survives_a_deleted_account(db):
    """Accounts can be deleted; the redemption row outlives them.

    The listing must still render, and a withdrawal must not abort partway
    through because one row no longer joins to a user.
    """
    code = _mint(max_uses=2)
    at.redeem_access_token(user_id=2, code=code)
    at.redeem_access_token(user_id=3, code=code)
    db.execute("DELETE FROM users WHERE id=3")
    db.commit()

    listing = at.list_token_redemptions(code)
    assert listing["total"] == 2
    gone = next(i for i in listing["items"] if i["user_id"] == 3)
    assert gone["email"] is None
    assert gone["comp_active"] is False
    assert gone["traces_to_code"] is False

    out = at.revoke_access_token(actor_user_id=1, code=code, withdraw_access=True)
    assert out["withdrawn_count"] == 1, "the surviving redeemer is still handled"
    assert _user(db, 2)["subscription_status"] == "none"


def test_redemption_listing_flags_a_forever_grant(db):
    code = _mint(access_days=None)
    at.redeem_access_token(user_id=2, code=code)
    item = at.list_token_redemptions(code)["items"][0]
    assert item["comp_is_forever"] is True
    assert item["comp_expires_at"] is None


def test_redemption_listing_on_an_unknown_code_is_404(db):
    with pytest.raises(HTTPException) as exc:
        at.list_token_redemptions("JOSEO-ZZZZ-ZZZZ")
    assert exc.value.status_code == 404


def test_redemption_listing_accepts_a_sloppily_typed_code(db):
    code = _mint()
    at.redeem_access_token(user_id=2, code=code)
    sloppy = code.replace("-", "").lower()
    assert at.list_token_redemptions(sloppy)["code"] == code


# --------------------------------------------------------------------------
# bulk revoke
# --------------------------------------------------------------------------

def test_revoke_all_hits_every_active_code_only(db):
    live_a, live_b = _mint(), _mint()
    spent = _mint(max_uses=1)
    at.redeem_access_token(user_id=4, code=spent)
    already = _mint()
    at.revoke_access_token(actor_user_id=1, code=already)

    out = at.revoke_all_active_tokens(actor_user_id=1)

    assert out["revoked_count"] == 2
    assert {c["code"] for c in out["codes"]} == {live_a, live_b}
    states = {t["code"]: t["state"] for t in at.list_access_tokens()["items"]}
    assert states[live_a] == states[live_b] == "revoked"
    assert states[spent] == "used_up", "a spent code is already unusable; leave it labelled that way"


def test_revoke_all_can_withdraw_access_too(db):
    code_a, code_b = _mint(max_uses=2), _mint()
    at.redeem_access_token(user_id=2, code=code_a)
    at.redeem_access_token(user_id=3, code=code_a)
    at.redeem_access_token(user_id=4, code=code_b)

    out = at.revoke_all_active_tokens(actor_user_id=1, withdraw_access=True)

    assert out["withdrawn_count"] == 3
    for uid in (2, 3, 4):
        assert _user(db, uid)["subscription_status"] == "none"


def test_revoke_all_withdrawal_reaches_spent_codes(db):
    """The distinction that makes the kill switch worth having.

    Revocation only means something for codes that are still redeemable, but
    withdrawal has to reach every code ever redeemed -- a fully-used code is the
    likeliest source of access someone should no longer have.
    """
    spent = _mint(max_uses=1)
    at.redeem_access_token(user_id=2, code=spent)
    live = _mint()

    out = at.revoke_all_active_tokens(actor_user_id=1, withdraw_access=True)

    assert out["revoked_count"] == 1, "only the live code was revocable"
    assert out["codes"][0]["code"] == live
    assert out["withdrawn_count"] == 1, "but the spent code's grant still came back"
    assert out["withdrawn_codes"] == [{"code": spent, "withdrawn_count": 1}]
    assert _user(db, 2)["subscription_status"] == "none"


def test_revoke_all_withdrawal_reaches_an_expired_code(db):
    # max_uses > redemptions, so the state is genuinely "expired" and not
    # "used_up" -- _token_state reports used_up first.
    code = _mint(access_days=None, redeem_by_days=1, max_uses=5)
    at.redeem_access_token(user_id=2, code=code)
    db.execute("UPDATE access_tokens SET redeem_by=? WHERE code=?", (int(time.time()) - 60, code))
    db.commit()
    assert at.list_access_tokens()["items"][0]["state"] == "expired"

    out = at.revoke_all_active_tokens(actor_user_id=1, withdraw_access=True)
    assert out["revoked_count"] == 0
    assert out["withdrawn_count"] == 1
    assert _user(db, 2)["subscription_status"] == "none"


def test_revoke_all_without_withdrawal_keeps_access(db):
    code = _mint()
    at.redeem_access_token(user_id=2, code=code)
    out = at.revoke_all_active_tokens(actor_user_id=1)
    assert out["withdrawn_count"] == 0
    assert _user(db, 2)["subscription_status"] == "comp"


def test_revoke_all_on_an_empty_slate_is_a_no_op(db):
    out = at.revoke_all_active_tokens(actor_user_id=1)
    assert (out["revoked_count"], out["withdrawn_count"]) == (0, 0)


def test_active_code_scan_pages_past_its_page_size(db, monkeypatch):
    """The pager must not stop at the first page."""
    monkeypatch.setattr(at, "BULK_SCAN_PAGE", 2)
    codes = {_mint() for _ in range(5)}
    assert set(at._active_codes()) == codes
    assert at.revoke_all_active_tokens(actor_user_id=1)["revoked_count"] == 5
