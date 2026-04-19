"""Paddle webhook receiver with durable async processing."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from core import _db_exec, _db_query_all, _db_query_one
from paddle_client import verify_webhook_signature

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/subscription", tags=["subscription-webhooks"])


def _now() -> int:
    return int(time.time())


@router.post("/webhook")
async def paddle_webhook(request: Request, background_tasks: BackgroundTasks):
    """Paddle webhook endpoint with insert-before-200 durability."""
    raw_body = await request.body()
    signature_header = request.headers.get("paddle-signature", "")

    if not verify_webhook_signature(raw_body, signature_header):
        logger.warning("Paddle webhook signature verification failed")
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        event = json.loads(raw_body.decode("utf-8"))
    except Exception as exc:
        logger.error("Paddle webhook JSON parse failed: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_id = str(event.get("event_id") or "").strip()
    event_type = str(event.get("event_type") or "").strip()
    occurred_at_iso = str(event.get("occurred_at") or "")

    if not event_id or not event_type:
        logger.error("Paddle webhook missing event_id/event_type event=%s", event)
        raise HTTPException(status_code=400, detail="Missing event_id or event_type")

    occurred_at_unix = _iso_to_unix(occurred_at_iso)
    if occurred_at_unix is None:
        logger.error("Paddle webhook has invalid occurred_at: %s", occurred_at_iso)
        raise HTTPException(status_code=400, detail="Invalid occurred_at")

    existing = _db_query_one(
        "SELECT event_id, processed_at FROM paddle_webhook_events WHERE event_id=? LIMIT 1",
        (event_id,),
    )
    if existing:
        processed_at = existing["processed_at"] if "processed_at" in existing.keys() else None
        if processed_at is not None:
            logger.info("Paddle webhook duplicate (already processed): %s", event_id)
            return {"ok": True, "outcome": "already_processed"}
        logger.info("Paddle webhook duplicate (unprocessed, reprocessing): %s", event_id)
    else:
        try:
            _db_exec(
                """
                INSERT INTO paddle_webhook_events
                    (event_id, event_type, occurred_at, received_at, processed_at, raw_event_json)
                VALUES (?, ?, ?, ?, NULL, ?)
                """,
                (event_id, event_type, occurred_at_unix, _now(), raw_body.decode("utf-8")),
            )
        except Exception as exc:
            logger.error("Paddle webhook INSERT failed for event_id=%s: %s", event_id, exc)
            raise HTTPException(status_code=500, detail="Storage error")

    background_tasks.add_task(process_paddle_event_by_id, event_id)
    return {"ok": True, "outcome": "enqueued"}


def _iso_to_unix(iso_str: str) -> Optional[int]:
    if not iso_str:
        return None
    try:
        return int(datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def process_paddle_event_by_id(event_id: str) -> None:
    """Process one webhook by event_id."""
    try:
        row = _db_query_one(
            "SELECT event_id, event_type, occurred_at, processed_at, raw_event_json "
            "FROM paddle_webhook_events WHERE event_id=? LIMIT 1",
            (event_id,),
        )
        if not row:
            logger.error("process_paddle_event_by_id: event not found: %s", event_id)
            return

        processed_at = row["processed_at"] if "processed_at" in row.keys() else None
        if processed_at is not None:
            logger.info("process_paddle_event_by_id already processed: %s", event_id)
            return

        event_type = str(row["event_type"])
        occurred_at = int(row["occurred_at"])

        try:
            event = json.loads(row["raw_event_json"])
        except Exception as exc:
            _mark_processed(event_id, user_id=None, outcome=f"json_parse_error:{exc}")
            return

        data = event.get("data", {}) or {}
        custom_data = data.get("custom_data", {}) or {}

        user_id_raw = custom_data.get("user_id")
        try:
            user_id = int(user_id_raw) if user_id_raw is not None else None
        except (ValueError, TypeError):
            user_id = None

        if user_id is None:
            _mark_processed(event_id, user_id=None, outcome="no_user_id_in_custom_data")
            logger.warning("Paddle event has no user_id in custom_data: %s", event_id)
            return

        user_row = _db_query_one(
            "SELECT id, subscription_updated_at FROM users WHERE id=?",
            (user_id,),
        )
        if not user_row:
            _mark_processed(event_id, user_id=user_id, outcome="user_not_found")
            logger.warning("Paddle event references unknown user_id=%s: %s", user_id, event_id)
            return

        last_updated = user_row["subscription_updated_at"] if "subscription_updated_at" in user_row.keys() else None
        if last_updated is not None and int(last_updated) > occurred_at:
            _mark_processed(event_id, user_id=user_id, outcome="skipped_older_than_current")
            logger.info("Paddle event older than stored update, skipping: %s", event_id)
            return

        outcome = _dispatch_event(event_type, user_id, data, occurred_at)
        _mark_processed(event_id, user_id=user_id, outcome=outcome)
    except Exception as exc:
        logger.exception("process_paddle_event_by_id crashed event_id=%s", event_id)
        try:
            _mark_processed(event_id, user_id=None, outcome=f"processing_error:{exc}")
        except Exception:
            pass


def _dispatch_event(event_type: str, user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    if event_type == "subscription.created":
        return _handle_subscription_created(user_id, data, occurred_at)
    if event_type == "subscription.activated":
        return _handle_subscription_activated(user_id, data, occurred_at)
    if event_type == "subscription.updated":
        return _handle_subscription_updated(user_id, data, occurred_at)
    if event_type == "subscription.canceled":
        return _handle_subscription_canceled(user_id, data, occurred_at)
    if event_type == "subscription.past_due":
        return _handle_subscription_past_due(user_id, data, occurred_at)
    if event_type == "transaction.completed":
        return _handle_transaction_completed(user_id, data, occurred_at)
    if event_type == "transaction.payment_failed":
        return _handle_transaction_payment_failed(user_id, data, occurred_at)
    logger.info("Paddle event type not handled: %s (user_id=%s)", event_type, user_id)
    return f"unhandled_event_type:{event_type}"


def _handle_subscription_created(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    subscription_id = str(data.get("id") or "")
    customer_id = str(data.get("customer_id") or "")
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None

    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_provider=?,
            subscription_customer_id=?,
            subscription_id=?,
            subscription_current_period_end=?,
            subscription_updated_at=?
        WHERE id=?
        """,
        ("active", "paddle", customer_id, subscription_id, period_end_unix, occurred_at, user_id),
    )
    return "marked_active_from_created"


def _handle_subscription_activated(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None

    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_current_period_end=?,
            subscription_updated_at=?
        WHERE id=?
        """,
        ("active", period_end_unix, occurred_at, user_id),
    )
    return "marked_active_from_activated"


def _handle_subscription_updated(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None

    if period_end_unix is not None:
        _db_exec(
            """
            UPDATE users SET
                subscription_current_period_end=?,
                subscription_updated_at=?
            WHERE id=?
            """,
            (period_end_unix, occurred_at, user_id),
        )
        return "updated_period_end"

    _db_exec(
        "UPDATE users SET subscription_updated_at=? WHERE id=?",
        (occurred_at, user_id),
    )
    return "updated_no_period_change"


def _handle_subscription_canceled(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_updated_at=?
        WHERE id=?
        """,
        ("cancelled", occurred_at, user_id),
    )
    return "marked_cancelled"


def _handle_subscription_past_due(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_updated_at=?
        WHERE id=?
        """,
        ("past_due", occurred_at, user_id),
    )
    return "marked_past_due"


def _handle_transaction_completed(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None

    if period_end_unix is not None:
        _db_exec(
            """
            UPDATE users SET
                subscription_status=?,
                subscription_current_period_end=?,
                subscription_updated_at=?
            WHERE id=?
            """,
            ("active", period_end_unix, occurred_at, user_id),
        )
        return "transaction_completed_period_extended"
    return "transaction_completed_no_period"


def _handle_transaction_payment_failed(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_updated_at=?
        WHERE id=?
        """,
        ("past_due", occurred_at, user_id),
    )
    return "marked_past_due_from_payment_failed"


def _extract_period_end(data: Dict[str, Any]) -> Optional[str]:
    current_period = data.get("current_billing_period") or data.get("billing_period") or {}
    ends_at = current_period.get("ends_at") or data.get("next_billed_at")
    return str(ends_at) if ends_at else None


def _mark_processed(event_id: str, user_id: Optional[int], outcome: str) -> None:
    _db_exec(
        """
        UPDATE paddle_webhook_events SET
            processed_at=?,
            user_id=?,
            outcome=?
        WHERE event_id=?
        """,
        (_now(), user_id, outcome, event_id),
    )


def replay_unprocessed_events_on_startup() -> int:
    try:
        rows = _db_query_all(
            "SELECT event_id FROM paddle_webhook_events WHERE processed_at IS NULL "
            "ORDER BY received_at ASC"
        )
    except Exception as exc:
        logger.error("Startup replay query failed: %s", exc)
        return 0

    count = 0
    for row in rows:
        event_id = str(row["event_id"])
        try:
            process_paddle_event_by_id(event_id)
            count += 1
        except Exception:
            logger.exception("Startup replay failed for event_id=%s", event_id)

    if count:
        logger.info("Paddle webhook startup replay processed %d unprocessed events", count)
    return count
