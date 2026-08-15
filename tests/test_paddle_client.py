"""Paddle client: signature verification and configuration gating.

A rejected webhook is not a cosmetic failure -- it is a driver who paid and was
never activated -- so the accept/reject boundary is worth pinning.
"""
from __future__ import annotations

import hashlib
import hmac
import time

import paddle_client as pc


SECRET = "test_webhook_secret"


def _signed(body: bytes, ts: int, secret: str = SECRET) -> str:
    sig = hmac.new(secret.encode(), f"{ts}:{body.decode()}".encode(), hashlib.sha256).hexdigest()
    return f"ts={ts};h1={sig}"


def _with_secret(monkeypatch, secret=SECRET):
    monkeypatch.setattr(pc, "PADDLE_WEBHOOK_SECRET", secret)


def test_valid_signature_accepted(monkeypatch):
    _with_secret(monkeypatch)
    body = b'{"event_id":"evt_1"}'
    assert pc.verify_webhook_signature(body, _signed(body, int(time.time()))) is True


def test_tampered_body_rejected(monkeypatch):
    _with_secret(monkeypatch)
    body = b'{"event_id":"evt_1"}'
    header = _signed(body, int(time.time()))
    assert pc.verify_webhook_signature(b'{"event_id":"evt_EVIL"}', header) is False


def test_wrong_secret_rejected(monkeypatch):
    _with_secret(monkeypatch)
    body = b'{"event_id":"evt_1"}'
    assert pc.verify_webhook_signature(body, _signed(body, int(time.time()), "other")) is False


def test_delivery_latency_within_tolerance_is_accepted(monkeypatch):
    """A cold container start or a queued request can easily put a couple of
    minutes between Paddle signing and us verifying. At the old 60s window that
    legitimate delivery was rejected and the subscription never activated."""
    _with_secret(monkeypatch)
    body = b'{"event_id":"evt_slow"}'
    ts = int(time.time()) - 120
    assert pc.verify_webhook_signature(body, _signed(body, ts)) is True


def test_clock_skew_in_either_direction_is_tolerated(monkeypatch):
    _with_secret(monkeypatch)
    body = b'{"event_id":"evt_skew"}'
    for offset in (-200, 200):
        assert pc.verify_webhook_signature(body, _signed(body, int(time.time()) + offset)) is True


def test_ancient_timestamp_still_rejected(monkeypatch):
    """The window is a latency allowance, not an open door."""
    _with_secret(monkeypatch)
    body = b'{"event_id":"evt_old"}'
    ts = int(time.time()) - (pc.WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS + 60)
    assert pc.verify_webhook_signature(body, _signed(body, ts)) is False


def test_missing_secret_rejects_everything(monkeypatch):
    """Fail closed: an unconfigured secret must never accept a webhook."""
    monkeypatch.setattr(pc, "PADDLE_WEBHOOK_SECRET", "")
    body = b'{"event_id":"evt_1"}'
    assert pc.verify_webhook_signature(body, _signed(body, int(time.time()))) is False


def test_malformed_headers_rejected(monkeypatch):
    _with_secret(monkeypatch)
    body = b'{"event_id":"evt_1"}'
    for header in ("", "garbage", "ts=123", "h1=abc", "ts=notanumber;h1=abc"):
        assert pc.verify_webhook_signature(body, header) is False, header


def test_configuration_requires_every_credential(monkeypatch):
    """A half-configured Paddle must report unconfigured, so the route returns a
    clear 503 instead of failing deeper in with a confusing error."""
    monkeypatch.setattr(pc, "PADDLE_API_KEY", "k")
    monkeypatch.setattr(pc, "PADDLE_PRICE_ID", "p")
    monkeypatch.setattr(pc, "PADDLE_WEBHOOK_SECRET", "s")
    assert pc.paddle_is_configured() is True
    for missing in ("PADDLE_API_KEY", "PADDLE_PRICE_ID", "PADDLE_WEBHOOK_SECRET"):
        monkeypatch.setattr(pc, missing, "")
        assert pc.paddle_is_configured() is False, missing
        monkeypatch.setattr(pc, missing, "x")


def test_api_base_switches_on_environment(monkeypatch):
    """Pointing live traffic at sandbox would take payments that never arrive."""
    monkeypatch.setattr(pc, "PADDLE_ENVIRONMENT", "production")
    assert pc._api_base() == pc.PADDLE_API_BASE_LIVE
    monkeypatch.setattr(pc, "PADDLE_ENVIRONMENT", "sandbox")
    assert pc._api_base() == pc.PADDLE_API_BASE_SANDBOX
