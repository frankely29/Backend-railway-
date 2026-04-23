"""Resend SDK wrapper for transactional emails.
Functions exist but are not called from any route in Stage 1.
Wired into signup flow in Stage 4."""
from __future__ import annotations

import html
import logging
import os
from typing import Optional

try:
    import resend
except ImportError:
    resend = None

logger = logging.getLogger(__name__)

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "noreply@example.com")
EMAIL_FROM_NAME = os.environ.get("EMAIL_FROM_NAME", "Team Joseo")


def _is_configured() -> bool:
    if resend is None:
        return False
    if not RESEND_API_KEY:
        return False
    return True


def _send(to: str, subject: str, html: str) -> bool:
    if not _is_configured():
        logger.info(f"email_service: skipping send (not configured) to={to} subject={subject!r}")
        return False

    try:
        resend.api_key = RESEND_API_KEY
        resend.Emails.send(
            {
                "from": f"{EMAIL_FROM_NAME} <{EMAIL_FROM}>",
                "to": [to],
                "subject": subject,
                "html": html,
            }
        )
        logger.info(f"email_service: sent to={to} subject={subject!r}")
        return True
    except Exception as exc:
        logger.warning(f"email_service: send failed to={to} error={exc}")
        return False


def send_signup_confirmation(user_row) -> bool:
    """Welcome email sent on signup. Wired into signup flow in Stage 4."""
    try:
        email = user_row["email"]
        display_name = user_row["display_name"] or "Driver"
    except Exception:
        return False

    display_name_html = html.escape(display_name)
    html_body = f"""
<div style="font-family: -apple-system, system-ui, sans-serif; max-width: 600px; margin: 0 auto;">
  <h2>Welcome to Team Joseo, {display_name_html}</h2>
  <p>Your account is ready. You have 7 days of free access.</p>
  <p><strong>Install on your phone for the best experience:</strong></p>
  <ul>
    <li><strong>iPhone:</strong> Open in Safari → Share → Add to Home Screen</li>
    <li><strong>Android:</strong> Chrome menu → Install App</li>
    <li><strong>Tesla:</strong> Open in the car browser and bookmark</li>
  </ul>
  <p>Questions? Reply to this email directly.</p>
  <p style="color: #666; font-size: 12px; margin-top: 40px;">
    Team Joseo — NYC TLC driver map
  </p>
</div>
"""
    return _send(to=email, subject="Welcome to Team Joseo", html=html_body)


def send_payment_failed(user_row, retry_date_iso: Optional[str] = None) -> bool:
    """Payment failed notification. Wired into webhook handler in Stage 2."""
    try:
        email = user_row["email"]
        display_name = user_row["display_name"] or "Driver"
    except Exception:
        return False

    retry_line = (
        f"<p>We'll try again on {html.escape(retry_date_iso)}.</p>"
        if retry_date_iso
        else "<p>Please update your payment method in Settings.</p>"
    )

    display_name_html = html.escape(display_name)
    html_body = f"""
<div style="font-family: -apple-system, system-ui, sans-serif; max-width: 600px; margin: 0 auto;">
  <h2>Payment failed, {display_name_html}</h2>
  <p>We couldn't process your subscription payment.</p>
  {retry_line}
  <p>Update your card in the app → Settings → Subscription.</p>
  <p style="color: #666; font-size: 12px; margin-top: 40px;">
    Team Joseo — NYC TLC driver map
  </p>
</div>
"""
    return _send(to=email, subject="Team Joseo: Payment failed", html=html_body)


def send_launch_email(email: str, display_name: str) -> bool:
    """One-off launch announcement for grandfathered users."""
    safe_name = (display_name or "Driver").strip() or "Driver"

    safe_name_html = html.escape(safe_name)
    html_body = f"""
<div style="font-family: -apple-system, system-ui, sans-serif; max-width: 600px; margin: 0 auto;">
  <h2>Team Joseo is now a paid app, {safe_name_html}</h2>
  <p>Thanks for being an early user. Your account is currently on complimentary access while we transition to paid subscriptions.</p>
  <p><strong>Price:</strong> $8/week after complimentary access ends.</p>
  <p>You can subscribe anytime inside the app from <strong>Settings → Subscription</strong>.</p>
  <p>If you already subscribed, thank you — no action needed.</p>
  <p style="color: #666; font-size: 12px; margin-top: 40px;">
    Team Joseo — NYC TLC driver map
  </p>
</div>
"""
    return _send(
        to=email,
        subject="Team Joseo is now paid — your complimentary access details",
        html=html_body,
    )


def send_first_paid_welcome(email: str, display_name: str) -> bool:
    """Welcome email sent the first time a user successfully pays.

    Called from subscription_webhooks.py _handle_transaction_completed
    only when this is the user's first processed transaction.completed event.
    Signature takes email and display_name directly (not a user_row) to match
    the webhook's call site.
    """
    safe_name = (display_name or "Driver").strip() or "Driver"

    safe_name_html = html.escape(safe_name)
    html_body = f"""
<div style="font-family: -apple-system, system-ui, sans-serif; max-width: 600px; margin: 0 auto;">
  <h2>Welcome to Team Joseo, {safe_name_html}</h2>
  <p>Your subscription is active. Thanks for supporting the project.</p>
  <p><strong>Your subscription includes:</strong></p>
  <ul>
    <li>Live NYC TLC hotspot map with real-time demand signals</li>
    <li>Day-tendency scoring for every neighborhood</li>
    <li>Driver community, chat, and leaderboard</li>
    <li>$8/week — cancel anytime from Settings → Subscription</li>
  </ul>
  <p><strong>Install on your phone for the best experience:</strong></p>
  <ul>
    <li><strong>iPhone:</strong> Open in Safari → Share → Add to Home Screen</li>
    <li><strong>Android:</strong> Chrome menu → Install App</li>
    <li><strong>Tesla:</strong> Open in the car browser and bookmark</li>
  </ul>
  <p>Questions? Reply to this email directly.</p>
  <p style="color: #666; font-size: 12px; margin-top: 40px;">
    Team Joseo — NYC TLC driver map
  </p>
</div>
"""
    return _send(
        to=email,
        subject="Welcome to Team Joseo — subscription active",
        html=html_body,
    )
