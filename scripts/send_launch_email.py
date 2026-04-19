#!/usr/bin/env python3
"""One-off launch email sender for Team Joseo Map.

Sends a launch announcement to grandfathered users (existing users who were
given complimentary access when the subscription product launched).

Idempotent: tracks sends in `subscription_launch_email_log`. Re-running
skips users already recorded.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Any

# Add backend root so imports work when running from scripts/
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import _db_exec, _db_query_all  # noqa: E402
from email_service import send_launch_email  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("launch_email")


def _to_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):
        try:
            return {k: row[k] for k in row.keys()}
        except Exception:
            pass
    return {
        "id": row[0] if len(row) > 0 else None,
        "email": row[1] if len(row) > 1 else None,
        "display_name": row[2] if len(row) > 2 else None,
    }


def _ensure_launch_email_log_schema() -> None:
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS subscription_launch_email_log (
            user_id BIGINT PRIMARY KEY,
            email TEXT NOT NULL,
            sent_at BIGINT NOT NULL,
            outcome TEXT NOT NULL
        );
        """
    )


def _get_eligible_users(limit: int | None = None) -> list[dict[str, Any]]:
    query = """
        SELECT u.id, u.email, u.display_name
        FROM users u
        LEFT JOIN subscription_launch_email_log log ON log.user_id = u.id
        WHERE u.subscription_comp_reason LIKE 'grandfathered:%'
          AND log.user_id IS NULL
          AND u.email IS NOT NULL
          AND u.email != ''
        ORDER BY u.id ASC
    """
    if limit is not None and limit > 0:
        query += " LIMIT ?"
        rows = _db_query_all(query, (int(limit),))
    else:
        rows = _db_query_all(query)
    return [_to_dict(r) for r in rows]


def _record_send(user_id: int, email: str, outcome: str) -> None:
    _db_exec(
        """
        INSERT INTO subscription_launch_email_log(user_id, email, sent_at, outcome)
        VALUES(?, ?, ?, ?)
        """,
        (int(user_id), str(email), int(time.time()), str(outcome)),
    )


def _send_single(email: str, display_name: str) -> str:
    ok = send_launch_email(email=email, display_name=display_name or "Driver")
    return "sent" if ok else "send_failed"


def main() -> int:
    parser = argparse.ArgumentParser(description="Send launch email to grandfathered users")
    parser.add_argument("--dry-run", action="store_true", help="Show counts, do not send")
    parser.add_argument("--limit", type=int, default=None, help="Limit recipients")
    parser.add_argument("--rate-ms", type=int, default=250, help="Delay between sends")
    args = parser.parse_args()

    _ensure_launch_email_log_schema()
    users = _get_eligible_users(limit=args.limit)

    logger.info("Eligible users (not yet emailed): %d", len(users))

    if args.dry_run:
        for u in users[:10]:
            logger.info(
                "Would send to: id=%s email=%s display_name=%s",
                u.get("id"),
                u.get("email"),
                u.get("display_name"),
            )
        if len(users) > 10:
            logger.info("... and %d more", len(users) - 10)
        return 0

    sent = 0
    errors = 0

    for idx, u in enumerate(users, start=1):
        user_id = int(u.get("id") or 0)
        email = str(u.get("email") or "").strip()
        display_name = str(u.get("display_name") or "Driver")

        if not user_id or not email:
            errors += 1
            continue

        outcome = _send_single(email=email, display_name=display_name)
        _record_send(user_id=user_id, email=email, outcome=outcome)

        if outcome == "sent":
            sent += 1
        else:
            errors += 1

        if args.rate_ms > 0:
            time.sleep(args.rate_ms / 1000.0)

        if idx % 25 == 0:
            logger.info("Progress: sent=%d errors=%d processed=%d", sent, errors, idx)

    logger.info("Launch email run complete: sent=%d errors=%d total=%d", sent, errors, sent + errors)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
