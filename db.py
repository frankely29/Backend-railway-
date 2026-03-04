from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator, Optional

import psycopg2
import psycopg2.extras


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set. Attach Postgres in Railway so DATABASE_URL exists.")
    # Railway sometimes uses postgres:// which psycopg2 accepts, but keep it safe:
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    return url


@contextmanager
def get_db() -> Iterator[psycopg2.extensions.connection]:
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        conn = psycopg2.connect(_database_url(), cursor_factory=psycopg2.extras.RealDictCursor)
        yield conn
        conn.commit()
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()