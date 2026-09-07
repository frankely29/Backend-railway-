"""End-to-end redemption against a real database.

The unit tests pin shape and intent; these run the actual flow -- mint, redeem,
grant, re-redeem, exhaust, revoke, expire -- against real SQLite, because the
behaviour that matters here is what happens to rows.
"""
from __future__ import annotations

import sqlite3
import time

import pytest
from fastapi import HTTPException

import access_tokens as at
import subscription_state


@pytest.fixture
def db(tmp_path, monkeypatch):
    """A throwaway SQLite database wired into the module's DB helpers."""
    path = tmp_path / "t.db"
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE users (
            id INTEGER PRIMARY KEY, email TEXT, is_admin INTEGER DEFAULT 0,
            trial_expires_at INTEGER, subscription_status TEXT,
            subscription_current_period_end INTEGER, subscription_id TEXT,
            subscription_comp_reason TEXT, subscription_comp_granted_at INTEGER,
            subscription_comp_expires_at INTEGER, subscription_updated_at INTEGER
        )"""
    )
    for ddl in at.create_access_token_tables_sql("sqlite"):
        conn.execute(ddl)
    conn.executemany("INSERT INTO users (id, email) VALUES (?,?)",
                     [(1, "a@x.com"), (2, "b@x.com"), (3, "c@x.com")])
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

    monkeypatch.setattr(at, "_db_exec", _exec)
    monkeypatch.setattr(at, "_db_query_one", _one)
    monkeypatch.setattr(at, "_db_query_all", _all)
    monkeypatch.setattr(at, "_db_run_in_transaction", _txn)
    monkeypatch.setattr(at, "_sql", lambda s: s)
    yield conn
    conn.close()


def _user(db, uid):
    return db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()


def test_mint_then_redeem_grants_access(db, monkeypatch):
    monkeypatch.setattr(subscription_state, "ENFORCE_TRIAL", True)
    token = at.create_access_token(actor_user_id=1, access_days=30, note="press")
    assert subscription_state.has_access(_user(db, 2)) is False

    result = at.redeem_access_token(user_id=2, code=token["code"])

    assert result["ok"] is True
    row = _user(db, 2)
    assert subscription_state.has_access(row) is True, "redeeming must actually grant access"
    assert row["subscription_status"] == "comp"
    assert token["code"] in row["subscription_comp_reason"]
    assert result["access_expires_at"] > int(time.time())


def test_a_code_with_no_access_days_grants_forever(db, monkeypatch):
    monkeypatch.setattr(subscription_state, "ENFORCE_TRIAL", True)
    token = at.create_access_token(actor_user_id=1, access_days=None)
    result = at.redeem_access_token(user_id=2, code=token["code"])
    assert result["grants_forever"] is True
    row = _user(db, 2)
    assert row["subscription_comp_expires_at"] is None
    assert subscription_state.is_comp_forever(row) is True


def test_the_code_can_be_typed_in_any_form(db):
    token = at.create_access_token(actor_user_id=1, access_days=7)
    sloppy = token["code"].lower().replace("-", " ")
    assert at.redeem_access_token(user_id=2, code=sloppy)["ok"] is True


def test_a_single_use_code_cannot_be_reused_by_someone_else(db):
    token = at.create_access_token(actor_user_id=1, access_days=7, max_uses=1)
    at.redeem_access_token(user_id=2, code=token["code"])
    with pytest.raises(HTTPException) as exc:
        at.redeem_access_token(user_id=3, code=token["code"])
    assert exc.value.status_code == 409
    assert subscription_state.is_comp_active(_user(db, 3)) is False


def test_the_same_user_cannot_redeem_a_code_twice(db):
    token = at.create_access_token(actor_user_id=1, access_days=7, max_uses=5)
    at.redeem_access_token(user_id=2, code=token["code"])
    with pytest.raises(HTTPException) as exc:
        at.redeem_access_token(user_id=2, code=token["code"])
    assert exc.value.status_code == 409
    uses = db.execute("SELECT uses FROM access_tokens WHERE code=?", (token["code"],)).fetchone()["uses"]
    assert uses == 1, "a rejected second redemption must not burn a use"


def test_a_multi_use_code_serves_several_people(db):
    token = at.create_access_token(actor_user_id=1, access_days=7, max_uses=2)
    at.redeem_access_token(user_id=2, code=token["code"])
    at.redeem_access_token(user_id=3, code=token["code"])
    assert subscription_state.is_comp_active(_user(db, 2)) is True
    assert subscription_state.is_comp_active(_user(db, 3)) is True


def test_redeeming_never_shortens_existing_access(db):
    """The gift rule: a 7-day code must not cost someone 23 days."""
    long_expiry = int(time.time()) + 30 * 86400
    db.execute("UPDATE users SET subscription_status='comp', subscription_comp_expires_at=? WHERE id=2",
               (long_expiry,)); db.commit()
    token = at.create_access_token(actor_user_id=1, access_days=7)
    at.redeem_access_token(user_id=2, code=token["code"])
    assert _user(db, 2)["subscription_comp_expires_at"] == long_expiry


def test_redeeming_never_downgrades_a_forever_comp(db):
    db.execute("UPDATE users SET subscription_status='comp', subscription_comp_expires_at=NULL WHERE id=2")
    db.commit()
    token = at.create_access_token(actor_user_id=1, access_days=7)
    at.redeem_access_token(user_id=2, code=token["code"])
    assert _user(db, 2)["subscription_comp_expires_at"] is None


def test_a_shorter_existing_comp_is_extended(db):
    soon = int(time.time()) + 86400
    db.execute("UPDATE users SET subscription_status='comp', subscription_comp_expires_at=? WHERE id=2", (soon,))
    db.commit()
    token = at.create_access_token(actor_user_id=1, access_days=30)
    at.redeem_access_token(user_id=2, code=token["code"])
    assert _user(db, 2)["subscription_comp_expires_at"] > soon


def test_revoked_codes_stop_working(db):
    token = at.create_access_token(actor_user_id=1, access_days=7)
    at.revoke_access_token(actor_user_id=1, code=token["code"])
    with pytest.raises(HTTPException) as exc:
        at.redeem_access_token(user_id=2, code=token["code"])
    assert exc.value.status_code == 410


def test_revoking_leaves_already_granted_access_intact(db):
    token = at.create_access_token(actor_user_id=1, access_days=7, max_uses=5)
    at.redeem_access_token(user_id=2, code=token["code"])
    at.revoke_access_token(actor_user_id=1, code=token["code"])
    assert subscription_state.is_comp_active(_user(db, 2)) is True


def test_a_code_past_its_redeem_by_is_refused(db):
    token = at.create_access_token(actor_user_id=1, access_days=7, redeem_by_days=1)
    db.execute("UPDATE access_tokens SET redeem_by=? WHERE code=?",
               (int(time.time()) - 10, token["code"])); db.commit()
    with pytest.raises(HTTPException) as exc:
        at.redeem_access_token(user_id=2, code=token["code"])
    assert exc.value.status_code == 410


def test_a_code_can_expire_while_still_granting_long_access(db):
    """The two expiries are independent: redeem within a day, keep a year."""
    token = at.create_access_token(actor_user_id=1, access_days=365, redeem_by_days=1)
    result = at.redeem_access_token(user_id=2, code=token["code"])
    assert result["access_expires_at"] > int(time.time()) + 300 * 86400


def test_an_unknown_code_is_rejected(db):
    with pytest.raises(HTTPException) as exc:
        at.redeem_access_token(user_id=2, code="JOSEO-ZZZZ-9999")
    assert exc.value.status_code == 404


def test_an_empty_code_is_rejected_before_any_lookup(db):
    with pytest.raises(HTTPException) as exc:
        at.redeem_access_token(user_id=2, code="   ")
    assert exc.value.status_code == 400


def test_listing_reports_state_and_usage(db):
    live = at.create_access_token(actor_user_id=1, access_days=7)
    spent = at.create_access_token(actor_user_id=1, access_days=7, max_uses=1)
    at.redeem_access_token(user_id=2, code=spent["code"])
    killed = at.create_access_token(actor_user_id=1, access_days=7)
    at.revoke_access_token(actor_user_id=1, code=killed["code"])

    states = {i["code"]: i["state"] for i in at.list_access_tokens()["items"]}
    assert states[live["code"]] == "active"
    assert states[spent["code"]] == "used_up"
    assert states[killed["code"]] == "revoked"

    active_only = at.list_access_tokens(include_inactive=False)["items"]
    assert [i["code"] for i in active_only] == [live["code"]]
