"""Paddle webhook receiver with durable async processing."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from core import _db_exec, _db_query_all, _db_query_one, _db_run_in_transaction, _sql
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


def _normalize_email(email: Any) -> str:
    if email is None:
        return ""
    return str(email).strip().lower()


def _lookup_user_id_by_subscription_id(subscription_id: Any) -> Optional[int]:
    subscription_id_text = str(subscription_id or "").strip()
    if not subscription_id_text:
        return None
    row = _db_query_one(
        "SELECT id FROM users WHERE subscription_id=? ORDER BY id ASC LIMIT 1",
        (subscription_id_text,),
    )
    if not row:
        return None
    return int(row["id"])


def _lookup_user_id_by_customer_id(customer_id: Any) -> Optional[int]:
    customer_id_text = str(customer_id or "").strip()
    if not customer_id_text:
        return None
    row = _db_query_one(
        "SELECT id FROM users WHERE subscription_customer_id=? ORDER BY id ASC LIMIT 1",
        (customer_id_text,),
    )
    if not row:
        return None
    return int(row["id"])


def _lookup_user_id_by_email(email: Any) -> Optional[int]:
    normalized = _normalize_email(email)
    if not normalized:
        return None
    row = _db_query_one(
        "SELECT id FROM users WHERE lower(trim(email))=? ORDER BY id ASC LIMIT 1",
        (normalized,),
    )
    if not row:
        return None
    return int(row["id"])


def _resolve_event_user_id(event: Dict[str, Any], data: Dict[str, Any]) -> tuple[Optional[int], str]:
    custom_data = data.get("custom_data", {}) or event.get("custom_data", {}) or {}

    user_id_raw = custom_data.get("user_id")
    try:
        user_id = int(user_id_raw) if user_id_raw is not None else None
    except (ValueError, TypeError):
        user_id = None
    if user_id is not None:
        return user_id, "custom_data.user_id"

    event_type = str(event.get("event_type") or "").strip().lower()
    subscription_id = str(data.get("subscription_id") or "").strip()
    if not subscription_id and event_type.startswith("subscription."):
        subscription_id = str(data.get("id") or "").strip()
    if subscription_id:
        resolved = _lookup_user_id_by_subscription_id(subscription_id)
        if resolved is not None:
            return resolved, "subscription_id"

    customer = data.get("customer") if isinstance(data.get("customer"), dict) else {}
    customer_id = str(data.get("customer_id") or customer.get("id") or "").strip()
    if customer_id:
        resolved = _lookup_user_id_by_customer_id(customer_id)
        if resolved is not None:
            return resolved, "customer_id"

    email_candidates = [
        customer.get("email"),
        data.get("email"),
        data.get("customer_email"),
        data.get("user_email"),
        event.get("email"),
    ]
    for email in email_candidates:
        resolved = _lookup_user_id_by_email(email)
        if resolved is not None:
            return resolved, "email"

    return None, "no_user_mapping_found"


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
        user_id, resolution_source = _resolve_event_user_id(event, data)
        if user_id is None:
            _mark_processed(event_id, user_id=None, outcome="no_user_mapping_found")
            logger.warning("Paddle event user mapping failed (%s): %s", resolution_source, event_id)
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
    if event_type == "subscription.trialing":
        return _handle_subscription_trialing(user_id, data, occurred_at)
    if event_type == "subscription.activated":
        return _handle_subscription_activated(user_id, data, occurred_at)
    if event_type == "subscription.resumed":
        return _handle_subscription_resumed(user_id, data, occurred_at)
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
    subscription_id = str(data.get("id") or data.get("subscription_id") or "").strip() or None
    customer_id = str(data.get("customer_id") or "").strip() or None
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None
    payload_status = _paddle_status_to_internal(str(data.get("status") or ""))

    _db_exec(
        """
        UPDATE users SET
            subscription_status=COALESCE(?, subscription_status, ?),
            subscription_provider=?,
            subscription_customer_id=COALESCE(?, subscription_customer_id),
            subscription_id=COALESCE(?, subscription_id),
            subscription_current_period_end=COALESCE(?, subscription_current_period_end),
            subscription_updated_at=?
        WHERE id=?
        """,
        (payload_status, "trialing", "paddle", customer_id, subscription_id, period_end_unix, occurred_at, user_id),
    )
    return f"created_status:{payload_status or 'preserved_or_trialing'}"


def _handle_subscription_trialing(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    subscription_id = str(data.get("id") or data.get("subscription_id") or "").strip() or None
    customer_id = str(data.get("customer_id") or "").strip() or None
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None
    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_provider=?,
            subscription_customer_id=COALESCE(?, subscription_customer_id),
            subscription_id=COALESCE(?, subscription_id),
            subscription_current_period_end=COALESCE(?, subscription_current_period_end),
            subscription_updated_at=?
        WHERE id=?
        """,
        ("trialing", "paddle", customer_id, subscription_id, period_end_unix, occurred_at, user_id),
    )
    return "marked_trialing"


def _handle_subscription_activated(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    subscription_id = str(data.get("id") or data.get("subscription_id") or "").strip() or None
    customer_id = str(data.get("customer_id") or "").strip() or None
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None

    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_provider=?,
            subscription_customer_id=COALESCE(?, subscription_customer_id),
            subscription_id=COALESCE(?, subscription_id),
            subscription_current_period_end=COALESCE(?, subscription_current_period_end),
            subscription_updated_at=?
        WHERE id=?
        """,
        ("active", "paddle", customer_id, subscription_id, period_end_unix, occurred_at, user_id),
    )
    return "marked_active_from_activated"


def _handle_subscription_resumed(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    subscription_id = str(data.get("id") or data.get("subscription_id") or "").strip() or None
    customer_id = str(data.get("customer_id") or "").strip() or None
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None
    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_provider=?,
            subscription_customer_id=COALESCE(?, subscription_customer_id),
            subscription_id=COALESCE(?, subscription_id),
            subscription_current_period_end=COALESCE(?, subscription_current_period_end),
            subscription_updated_at=?
        WHERE id=?
        """,
        ("active", "paddle", customer_id, subscription_id, period_end_unix, occurred_at, user_id),
    )
    return "marked_active_from_resumed"


def _paddle_status_to_internal(paddle_status: str) -> Optional[str]:
    """Map Paddle's subscription.status to our internal subscription_status value.

    Returns None if the Paddle status is absent or unrecognized so the caller
    can leave subscription_status untouched instead of clobbering it.
    """
    mapping = {
        "active": "active",
        "trialing": "trialing",
        "past_due": "past_due",
        "paused": "paused",
        "canceled": "cancelled",
        "cancelled": "cancelled",
    }
    key = (paddle_status or "").strip().lower()
    return mapping.get(key)


def _handle_subscription_updated(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    subscription_id = str(data.get("id") or data.get("subscription_id") or "").strip() or None
    customer_id = str(data.get("customer_id") or "").strip() or None
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None
    new_status = _paddle_status_to_internal(str(data.get("status") or ""))
    _db_exec(
        """
        UPDATE users SET
            subscription_status=COALESCE(?, subscription_status),
            subscription_provider=?,
            subscription_customer_id=COALESCE(?, subscription_customer_id),
            subscription_id=COALESCE(?, subscription_id),
            subscription_current_period_end=COALESCE(?, subscription_current_period_end),
            subscription_updated_at=?
        WHERE id=?
        """,
        (new_status, "paddle", customer_id, subscription_id, period_end_unix, occurred_at, user_id),
    )
    if new_status is not None and period_end_unix is not None:
        return f"updated_status_and_period:{new_status}"
    if new_status is not None:
        return f"updated_status:{new_status}"
    if period_end_unix is not None:
        return "updated_period_end"
    return "updated_no_period_change"


def _handle_subscription_canceled(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    subscription_id = str(data.get("id") or data.get("subscription_id") or "").strip() or None
    customer_id = str(data.get("customer_id") or "").strip() or None
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None
    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_provider=?,
            subscription_customer_id=COALESCE(?, subscription_customer_id),
            subscription_id=COALESCE(?, subscription_id),
            subscription_current_period_end=COALESCE(?, subscription_current_period_end),
            subscription_updated_at=?
        WHERE id=?
        """,
        ("cancelled", "paddle", customer_id, subscription_id, period_end_unix, occurred_at, user_id),
    )
    return "marked_cancelled"


def _handle_subscription_past_due(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    subscription_id = str(data.get("id") or data.get("subscription_id") or "").strip() or None
    customer_id = str(data.get("customer_id") or "").strip() or None
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None
    _db_exec(
        """
        UPDATE users SET
            subscription_status=?,
            subscription_provider=?,
            subscription_customer_id=COALESCE(?, subscription_customer_id),
            subscription_id=COALESCE(?, subscription_id),
            subscription_current_period_end=COALESCE(?, subscription_current_period_end),
            subscription_updated_at=?
        WHERE id=?
        """,
        ("past_due", "paddle", customer_id, subscription_id, period_end_unix, occurred_at, user_id),
    )
    return "marked_past_due"


def _handle_transaction_completed(user_id: int, data: Dict[str, Any], occurred_at: int) -> str:
    period_end_iso = _extract_period_end(data)
    period_end_unix = _iso_to_unix(period_end_iso) if period_end_iso else None

    # Opportunistically capture customer_id / subscription_id from the transaction payload.
    # Paddle includes these on transaction events. If subscription.created arrived first
    # and already populated them, we use COALESCE to avoid overwriting with NULL when
    # Paddle omits a field on a given event.
    customer_id_raw = data.get("customer_id") or ""
    subscription_id_raw = data.get("subscription_id") or ""
    customer_id = str(customer_id_raw).strip() or None
    subscription_id = str(subscription_id_raw).strip() or None

    outcome = "transaction_completed_no_period"
    if period_end_unix is not None:
        _db_exec(
            """
            UPDATE users SET
                subscription_status=?,
                subscription_current_period_end=?,
                subscription_provider=COALESCE(subscription_provider, ?),
                subscription_customer_id=COALESCE(subscription_customer_id, ?),
                subscription_id=COALESCE(subscription_id, ?),
                subscription_updated_at=?
            WHERE id=?
            """,
            ("active", period_end_unix, "paddle", customer_id, subscription_id, occurred_at, user_id),
        )
        outcome = "transaction_completed_period_extended"
    else:
        # Even without a parseable period_end, capture customer/subscription IDs if
        # present so /subscription/portal works for this user. Still mark updated_at.
        if customer_id or subscription_id:
            _db_exec(
                """
                UPDATE users SET
                    subscription_provider=COALESCE(subscription_provider, ?),
                    subscription_customer_id=COALESCE(subscription_customer_id, ?),
                    subscription_id=COALESCE(subscription_id, ?),
                    subscription_updated_at=?
                WHERE id=?
                """,
                ("paddle", customer_id, subscription_id, occurred_at, user_id),
            )
            outcome = "transaction_completed_ids_captured_no_period"

    # STAGE 4B-B3: send first-paid welcome email only once per user.
    # Atomically claim the slot by flipping first_paid_welcome_sent_at from NULL.
    # Only the caller that wins the UPDATE (rowcount==1) sends the email, which
    # prevents duplicates across webhook retries and concurrent processing.
    #
    # If the send fails we release the claim so that the next qualifying event
    # for this user can try again; otherwise a transient email outage would
    # permanently consume the single chance to send this email.
    try:
        if _claim_first_paid_welcome(user_id, occurred_at):
            user_row = _db_query_one("SELECT email, display_name FROM users WHERE id=?", (user_id,))
            if not user_row:
                _release_first_paid_welcome_claim(user_id, occurred_at)
            else:
                try:
                    from email_service import send_first_paid_welcome

                    send_first_paid_welcome(
                        email=str(user_row["email"]),
                        display_name=str(user_row["display_name"] or "Driver"),
                    )
                except ImportError:
                    logger.warning("email_service.send_first_paid_welcome not available; skipping")
                    _release_first_paid_welcome_claim(user_id, occurred_at)
                except Exception as send_exc:
                    _release_first_paid_welcome_claim(user_id, occurred_at)
                    logger.warning(
                        "First-paid welcome email send failed for user_id=%s, released claim: %s",
                        user_id,
                        send_exc,
                    )
    except Exception as exc:
        logger.warning("First-paid welcome email failed for user_id=%s: %s", user_id, exc)

    return outcome


def _claim_first_paid_welcome(user_id: int, occurred_at: int) -> bool:
    """Atomically mark the user as having received the first-paid welcome email.

    Returns True iff this caller is the one that flipped the column from NULL
    (i.e. it's responsible for sending the email). Returns False if another
    processing pass already claimed it, or if the claim could not be made.
    """

    def _run(conn, cur):
        cur.execute(
            _sql(
                "UPDATE users SET first_paid_welcome_sent_at=? "
                "WHERE id=? AND first_paid_welcome_sent_at IS NULL"
            ),
            (occurred_at, user_id),
        )
        return int(cur.rowcount or 0)

    try:
        return _db_run_in_transaction(_run) == 1
    except Exception as exc:
        logger.warning("first_paid_welcome claim failed for user_id=%s: %s", user_id, exc)
        return False


def _release_first_paid_welcome_claim(user_id: int, claim_occurred_at: int) -> None:
    """Reverse a prior _claim_first_paid_welcome when the send didn't happen.

    Only clears the flag when it still equals our claim timestamp, so we don't
    clobber a later successful claim+send by another processing pass.
    """

    def _run(conn, cur):
        cur.execute(
            _sql(
                "UPDATE users SET first_paid_welcome_sent_at=NULL "
                "WHERE id=? AND first_paid_welcome_sent_at=?"
            ),
            (user_id, claim_occurred_at),
        )
        return int(cur.rowcount or 0)

    try:
        _db_run_in_transaction(_run)
    except Exception as exc:
        logger.warning("first_paid_welcome release failed for user_id=%s: %s", user_id, exc)


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
    # STAGE 4B-B3: send payment-failed email. We've already flipped the user
    # to past_due above, so a silently-dropped email leaves the user unaware
    # they need to update their card. Log at error level (not warning) so the
    # failure is visible to on-call; subsequent dunning webhooks will retry.
    try:
        user_row = _db_query_one("SELECT email, display_name FROM users WHERE id=?", (user_id,))
        if user_row:
            try:
                from email_service import send_payment_failed

                send_payment_failed(user_row)
            except ImportError:
                logger.warning("email_service.send_payment_failed not available; skipping")
            except Exception as send_exc:
                logger.error(
                    "Payment-failed email send failed for user_id=%s; user already marked past_due: %s",
                    user_id,
                    send_exc,
                )
    except Exception as exc:
        logger.error("Payment-failed email flow failed for user_id=%s: %s", user_id, exc)

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
