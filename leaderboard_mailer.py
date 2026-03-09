from __future__ import annotations

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, Tuple


def report_emails_enabled() -> bool:
    return os.environ.get("REPORT_EMAILS_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def render_report_email(display_name: str, report_type: str, period_key: str, summary: Dict, miles_rank: int | None, hours_rank: int | None, badge: str | None) -> Tuple[str, str, str]:
    title = f"Your {report_type.capitalize()} NYC TLC report ({period_key})"
    who = display_name or "Driver"
    text = (
        f"Hi {who},\n\n"
        f"Here is your {report_type} summary for {period_key}.\n"
        f"Miles worked: {summary['miles_worked']}\n"
        f"Hours worked: {summary['hours_worked']}\n"
        f"Trips recorded: {summary['trips_recorded']}\n"
        f"Pickups recorded: {summary['pickups_recorded']}\n"
        f"Miles rank: #{miles_rank if miles_rank else 'N/A'}\n"
        f"Hours rank: #{hours_rank if hours_rank else 'N/A'}\n"
        f"Badge: {badge or 'None'}\n"
    )
    html = f"""
    <html><body>
      <p>Hi {who},</p>
      <p>Here is your <b>{report_type}</b> summary for <b>{period_key}</b>.</p>
      <ul>
        <li>Miles worked: <b>{summary['miles_worked']}</b></li>
        <li>Hours worked: <b>{summary['hours_worked']}</b></li>
        <li>Trips recorded: <b>{summary['trips_recorded']}</b></li>
        <li>Pickups recorded: <b>{summary['pickups_recorded']}</b></li>
        <li>Miles rank: <b>#{miles_rank if miles_rank else 'N/A'}</b></li>
        <li>Hours rank: <b>#{hours_rank if hours_rank else 'N/A'}</b></li>
        <li>Badge: <b>{badge or 'None'}</b></li>
      </ul>
    </body></html>
    """
    return title, text, html


def send_report_email(to_email: str, subject: str, text_body: str, html_body: str) -> None:
    host = os.environ.get("SMTP_HOST", "").strip()
    if not host:
        raise RuntimeError("SMTP_HOST missing")
    port = int(os.environ.get("SMTP_PORT", "587"))
    username = os.environ.get("SMTP_USERNAME", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "").strip()
    from_email = os.environ.get("SMTP_FROM", username).strip()
    use_tls = os.environ.get("SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes", "on"}

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(host=host, port=port, timeout=30) as server:
        if use_tls:
            server.starttls()
        if username:
            server.login(username, password)
        server.sendmail(from_email, [to_email], msg.as_string())
