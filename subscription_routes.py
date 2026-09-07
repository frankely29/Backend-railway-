"""Subscription lifecycle HTTP routes.

All routes in this file use require_user_basic (token + block state only,
no access check) so that users with expired trials/subscriptions can still
reach these routes to pay and unblock themselves.
"""
from __future__ import annotations

import logging
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from core import _db_query_one, require_user_basic
from paddle_client import (
    create_checkout_transaction,
    create_customer_portal_session,
    paddle_is_configured,
)
from access_tokens import redeem_access_token
from admin_mutation_models import RedeemAccessTokenRequest
from subscription_state import build_subscription_response

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/subscription", tags=["subscription"])


@router.post("/checkout")
def subscription_checkout(user: sqlite3.Row = Depends(require_user_basic)):
    """Create a Paddle checkout transaction for the current user."""
    if not paddle_is_configured():
        raise HTTPException(
            status_code=503,
            detail="Payment system is not configured. Please contact support.",
        )

    try:
        result = create_checkout_transaction(
            user_id=int(user["id"]),
            email=str(user["email"]),
        )
    except RuntimeError as exc:
        logger.error("Checkout creation failed for user_id=%s: %s", user["id"], exc)
        raise HTTPException(status_code=502, detail="Payment provider error")
    except Exception:
        logger.exception("Unexpected checkout error for user_id=%s", user["id"])
        raise HTTPException(status_code=500, detail="Checkout failed")

    return {
        "ok": True,
        "checkout_url": result["checkout_url"],
        "transaction_id": result["transaction_id"],
    }


@router.get("/status")
def subscription_status(user: sqlite3.Row = Depends(require_user_basic)):
    """Return the current user's subscription state."""
    return {
        "ok": True,
        "subscription": build_subscription_response(user),
    }


@router.post("/portal")
def subscription_portal(user: sqlite3.Row = Depends(require_user_basic)):
    """Generate a temporary Paddle customer portal session URL."""
    customer_id_raw = (
        user["subscription_customer_id"]
        if "subscription_customer_id" in user.keys()
        else None
    )
    customer_id = str(customer_id_raw) if customer_id_raw else ""

    if not customer_id:
        raise HTTPException(
            status_code=400,
            detail="No active subscription to manage. Subscribe first.",
        )

    if not paddle_is_configured():
        raise HTTPException(
            status_code=503,
            detail="Payment system is not configured. Please contact support.",
        )

    try:
        portal_url = create_customer_portal_session(customer_id)
    except RuntimeError as exc:
        logger.error("Portal session failed for user_id=%s: %s", user["id"], exc)
        raise HTTPException(status_code=502, detail="Payment provider error")
    except Exception:
        logger.exception("Unexpected portal error for user_id=%s", user["id"])
        raise HTTPException(status_code=500, detail="Portal generation failed")

    return {
        "ok": True,
        "portal_url": portal_url,
    }


@router.post("/redeem")
def subscription_redeem(
    payload: RedeemAccessTokenRequest,
    user: sqlite3.Row = Depends(require_user_basic),
):
    """Redeem an admin-issued free-access code.

    require_user_basic, like every other route in this file: the people who need
    to redeem a code are precisely the ones the access gate is currently turning
    away. Behind require_user this would 402 before it could ever grant anything.
    """
    result = redeem_access_token(user_id=int(user["id"]), code=payload.code)
    refreshed = _db_query_one("SELECT * FROM users WHERE id=?", (int(user["id"]),))
    if refreshed is not None:
        result["subscription"] = build_subscription_response(refreshed)
    return result
