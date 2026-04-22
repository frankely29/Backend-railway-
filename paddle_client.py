"""Paddle Billing client wrapper.

All functions here are pure: they take arguments, call Paddle, return dicts.
No database calls, no business logic — that belongs in subscription_routes.py
and subscription_webhooks.py.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from typing import Any, Dict

import httpx

logger = logging.getLogger(__name__)

PADDLE_API_KEY = os.environ.get("PADDLE_API_KEY", "")
PADDLE_PRICE_ID = os.environ.get("PADDLE_PRICE_ID", "")
PADDLE_WEBHOOK_SECRET = os.environ.get("PADDLE_WEBHOOK_SECRET", "")
PADDLE_ENVIRONMENT = os.environ.get("PADDLE_ENVIRONMENT", "sandbox").strip().lower()

PADDLE_API_BASE_SANDBOX = "https://sandbox-api.paddle.com"
PADDLE_API_BASE_LIVE = "https://api.paddle.com"


def _api_base() -> str:
    return PADDLE_API_BASE_LIVE if PADDLE_ENVIRONMENT == "production" else PADDLE_API_BASE_SANDBOX


def _api_headers() -> Dict[str, str]:
    if not PADDLE_API_KEY:
        raise RuntimeError(
            "PADDLE_API_KEY is not configured. Set it in Railway environment variables."
        )
    return {
        "Authorization": f"Bearer {PADDLE_API_KEY}",
        "Content-Type": "application/json",
    }


def paddle_is_configured() -> bool:
    """Check whether Paddle is fully configured."""
    return all([PADDLE_API_KEY, PADDLE_PRICE_ID, PADDLE_WEBHOOK_SECRET])


def create_checkout_transaction(user_id: int, email: str) -> Dict[str, Any]:
    """Create a Paddle transaction for a weekly subscription checkout."""
    if not paddle_is_configured():
        raise RuntimeError("Paddle is not configured; cannot create checkout")

    payload: Dict[str, Any] = {
        "items": [{"price_id": PADDLE_PRICE_ID, "quantity": 1}],
        "customer_email": email,
        "custom_data": {"user_id": str(int(user_id))},
        "collection_mode": "automatic",
    }

    with httpx.Client(timeout=10.0) as client:
        response = client.post(
            f"{_api_base()}/transactions",
            headers=_api_headers(),
            json=payload,
        )

    if response.status_code >= 400:
        logger.error(
            "Paddle create_checkout_transaction failed status=%s body=%s",
            response.status_code,
            response.text[:500],
        )
        raise RuntimeError(f"Paddle API error: {response.status_code}")

    data = response.json().get("data", {})
    checkout_url = data.get("checkout", {}).get("url", "")
    transaction_id = data.get("id", "")
    if not checkout_url:
        logger.error("Paddle response missing checkout.url: %s", response.text[:500])
        raise RuntimeError("Paddle did not return a checkout URL")

    return {
        "checkout_url": checkout_url,
        "transaction_id": transaction_id,
    }


def create_customer_portal_session(customer_id: str) -> str:
    """Create a temporary customer portal session URL."""
    if not PADDLE_API_KEY:
        raise RuntimeError("PADDLE_API_KEY is not configured")
    if not customer_id:
        raise ValueError("customer_id is required")

    with httpx.Client(timeout=10.0) as client:
        response = client.post(
            f"{_api_base()}/customers/{customer_id}/portal-sessions",
            headers=_api_headers(),
            json={},
        )

    if response.status_code >= 400:
        logger.error(
            "Paddle portal session failed status=%s body=%s",
            response.status_code,
            response.text[:500],
        )
        raise RuntimeError(f"Paddle API error: {response.status_code}")

    data = response.json().get("data", {})
    urls = data.get("urls", {})
    general_url = urls.get("general", {}).get("overview", "")
    if not general_url:
        logger.error("Paddle portal response missing general URL: %s", response.text[:500])
        raise RuntimeError("Paddle did not return a portal URL")

    return general_url


def verify_webhook_signature(raw_body: bytes, signature_header: str) -> bool:
    """Verify Paddle webhook signature using HMAC-SHA256."""
    if not PADDLE_WEBHOOK_SECRET:
        logger.error("PADDLE_WEBHOOK_SECRET not configured; rejecting webhook")
        return False
    if not signature_header:
        return False

    try:
        parts = dict(
            item.split("=", 1) for item in signature_header.split(";") if "=" in item
        )
        ts = parts.get("ts", "")
        received_sig = parts.get("h1", "")
        if not ts or not received_sig:
            return False

        try:
            ts_int = int(ts)
            # Tight skew window (60s) limits the replay attempt surface if a
            # signed payload is ever captured. Real duplicates are caught by
            # event_id dedupe in subscription_webhooks.paddle_webhook.
            if abs(int(time.time()) - ts_int) > 60:
                logger.warning("Paddle webhook timestamp too old or too new: %s", ts)
                return False
        except ValueError:
            return False

        signed_payload = f"{ts}:{raw_body.decode('utf-8')}"
        expected_sig = hmac.new(
            PADDLE_WEBHOOK_SECRET.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(expected_sig, received_sig)
    except Exception as exc:
        logger.error("Webhook signature verification crashed: %s", exc)
        return False
