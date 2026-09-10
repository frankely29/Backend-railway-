"""HTTP surface for access-code revocation.

Two things are worth pinning at this layer rather than in the service tests:

  * the revoke body is OPTIONAL, so the call that shipped before withdrawal
    existed still means "block future redemptions only". If that regressed into
    a required body, every existing caller would start 422-ing; if it regressed
    into defaulting withdraw_access=True, a plain revoke would silently start
    cutting people off.
  * every one of these routes is admin-gated. They hand out and take away paid
    access, so an ungated one is the worst kind of bug to ship.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import admin_mutation_routes as routes
import admin_security

CODE = "JOSEO-ABCD-EFGH"


@pytest.fixture
def client(monkeypatch):
    app = FastAPI()
    app.include_router(routes.router)
    app.dependency_overrides[admin_security.require_admin_user] = lambda: {"id": 7}

    seen: list = []

    def _revoke(*, actor_user_id, code, withdraw_access=False):
        seen.append(("revoke", actor_user_id, code, withdraw_access))
        return {"ok": True}

    def _restore(*, actor_user_id, code):
        seen.append(("restore", actor_user_id, code))
        return {"ok": True}

    def _redemptions(code):
        seen.append(("redemptions", code))
        return {"ok": True, "items": []}

    def _revoke_one(*, actor_user_id, code, user_id):
        seen.append(("revoke_one", actor_user_id, code, user_id))
        return {"ok": True}

    def _bulk(*, actor_user_id, withdraw_access=False):
        seen.append(("bulk", actor_user_id, withdraw_access))
        return {"ok": True}

    monkeypatch.setattr(routes, "revoke_access_token", _revoke)
    monkeypatch.setattr(routes, "restore_access_token", _restore)
    monkeypatch.setattr(routes, "list_token_redemptions", _redemptions)
    monkeypatch.setattr(routes, "revoke_redemption", _revoke_one)
    monkeypatch.setattr(routes, "revoke_all_active_tokens", _bulk)

    c = TestClient(app)
    c.seen = seen
    return c


def test_revoke_with_no_body_does_not_withdraw(client):
    assert client.post(f"/admin/access_tokens/{CODE}/revoke").status_code == 200
    assert client.seen[-1] == ("revoke", 7, CODE, False)


def test_revoke_with_empty_body_does_not_withdraw(client):
    assert client.post(f"/admin/access_tokens/{CODE}/revoke", json={}).status_code == 200
    assert client.seen[-1] == ("revoke", 7, CODE, False)


def test_revoke_can_ask_for_withdrawal(client):
    res = client.post(f"/admin/access_tokens/{CODE}/revoke", json={"withdraw_access": True})
    assert res.status_code == 200
    assert client.seen[-1] == ("revoke", 7, CODE, True)


def test_restore_route(client):
    assert client.post(f"/admin/access_tokens/{CODE}/restore").status_code == 200
    assert client.seen[-1] == ("restore", 7, CODE)


def test_redemptions_listing_route(client):
    assert client.get(f"/admin/access_tokens/{CODE}/redemptions").status_code == 200
    assert client.seen[-1] == ("redemptions", CODE)


def test_single_redemption_revoke_route(client):
    assert client.post(f"/admin/access_tokens/{CODE}/redemptions/42/revoke").status_code == 200
    assert client.seen[-1] == ("revoke_one", 7, CODE, 42)


@pytest.mark.parametrize("confirm", ["", "yes", "revoke", "REVOKE-ALL", "delete all"])
def test_bulk_revoke_needs_the_exact_confirmation(client, confirm):
    res = client.post("/admin/access_tokens/revoke_all", json={"confirm": confirm})
    assert res.status_code in (400, 422)
    assert not any(entry[0] == "bulk" for entry in client.seen)


@pytest.mark.parametrize("confirm", ["REVOKE ALL", "revoke all", "  Revoke All  "])
def test_bulk_revoke_accepts_the_confirmation_case_insensitively(client, confirm):
    res = client.post("/admin/access_tokens/revoke_all", json={"confirm": confirm})
    assert res.status_code == 200
    assert client.seen[-1] == ("bulk", 7, False)


def test_bulk_revoke_passes_withdrawal_through(client):
    res = client.post(
        "/admin/access_tokens/revoke_all",
        json={"confirm": "REVOKE ALL", "withdraw_access": True},
    )
    assert res.status_code == 200
    assert client.seen[-1] == ("bulk", 7, True)


def test_revoke_all_is_not_swallowed_as_a_code(client):
    """/access_tokens/revoke_all must reach the bulk handler, not {code}/revoke."""
    client.post("/admin/access_tokens/revoke_all", json={"confirm": "REVOKE ALL"})
    assert client.seen[-1][0] == "bulk"


def test_every_access_token_route_is_admin_gated():
    gated = 0
    for route in routes.router.routes:
        if "access_token" not in getattr(route, "path", ""):
            continue
        names = [d.call.__name__ for d in route.dependant.dependencies if d.call]
        assert "require_admin_user" in names, f"{route.path} is not admin-gated"
        gated += 1
    assert gated >= 7, f"expected the full access-token surface, saw {gated} routes"
