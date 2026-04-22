from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2.pool import PoolError, ThreadedConnectionPool
except ImportError:  # pragma: no cover - exercised in runtime/test import permutations
    psycopg2 = None
    RealDictCursor = None
    PoolError = None
    ThreadedConnectionPool = None
from fastapi import HTTPException, Request

if TYPE_CHECKING:
    from psycopg2.pool import ThreadedConnectionPool as _ThreadedConnectionPool
else:
    _ThreadedConnectionPool = Any

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
COMMUNITY_DB_PATH = Path(os.environ.get("COMMUNITY_DB", str(DATA_DIR / "community.db")))
JWT_SECRET = os.environ.get("JWT_SECRET", "")
POSTGRES_URL = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL")
DB_BACKEND = "postgres" if POSTGRES_URL else "sqlite"
TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "7"))
ENFORCE_TRIAL = str(os.environ.get("ENFORCE_TRIAL", "0")).strip().lower() in ("1", "true", "yes", "on")
POSTGRES_POOL_MIN = max(1, int(os.environ.get("POSTGRES_POOL_MIN", "2")))
POSTGRES_POOL_MAX = max(POSTGRES_POOL_MIN, int(os.environ.get("POSTGRES_POOL_MAX", "24")))
LIVE_TOKEN_TTL_SECONDS = min(90, max(30, int(os.environ.get("LIVE_TOKEN_TTL_SECONDS", "60"))))
# Owner identity. Matches the signup-flow ADMIN_EMAIL bootstrap logic in main.py.
# Duplicated read is intentional: admin_mutation_service imports from core, not main,
# and core cannot import from main without a circular import.
ADMIN_EMAIL = (os.environ.get("ADMIN_EMAIL") or "").strip().lower()


def is_account_owner(user_row) -> bool:
    """Return True if user_row represents the account owner (email matches ADMIN_EMAIL).

    Case-insensitive, whitespace-trimmed comparison. Returns False if ADMIN_EMAIL is
    not set or if user_row has no email attribute/key.
    """
    if not ADMIN_EMAIL:
        return False
    try:
        email = user_row["email"] if "email" in user_row.keys() else None
    except Exception:
        return False
    if not email:
        return False
    try:
        return str(email).strip().lower() == ADMIN_EMAIL
    except Exception:
        return False


class _DynamicDBLock:
    def __init__(self) -> None:
        self._sqlite_lock = threading.RLock()

    def __enter__(self):
        if DB_BACKEND == "sqlite":
            self._sqlite_lock.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        if DB_BACKEND == "sqlite":
            self._sqlite_lock.release()
        return False


_db_lock = _DynamicDBLock()
_postgres_pool_lock = threading.Lock()
_postgres_pool: Optional[_ThreadedConnectionPool] = None


class _PooledPostgresConnection:
    def __init__(self, conn, pool: _ThreadedConnectionPool):
        self._conn = conn
        self._pool = pool
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._pool.putconn(self._conn)

    def __getattr__(self, item: str):
        return getattr(self._conn, item)


def _require_psycopg2_for_postgres() -> None:
    if psycopg2 is None or ThreadedConnectionPool is None or RealDictCursor is None:
        raise RuntimeError(
            "Postgres mode requires psycopg2 to be installed. Either install psycopg2/psycopg2-binary "
            "or unset DATABASE_URL/POSTGRES_URL to use SQLite mode."
        )


def _postgres_conn_pool() -> _ThreadedConnectionPool:
    global _postgres_pool
    _require_psycopg2_for_postgres()
    if _postgres_pool is not None:
        return _postgres_pool
    with _postgres_pool_lock:
        if _postgres_pool is None:
            _postgres_pool = ThreadedConnectionPool(
                minconn=POSTGRES_POOL_MIN,
                maxconn=POSTGRES_POOL_MAX,
                dsn=POSTGRES_URL,
                cursor_factory=RealDictCursor,
            )
    return _postgres_pool


def _db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if DB_BACKEND == "postgres":
        _require_psycopg2_for_postgres()
        pool = _postgres_conn_pool()
        conn = _postgres_getconn_with_retry(pool)
        return _PooledPostgresConnection(conn, pool)

    conn = sqlite3.connect(str(COMMUNITY_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _postgres_getconn_with_retry(pool: _ThreadedConnectionPool):
    retry_delays_seconds = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25]
    last_pool_error = None
    for delay in retry_delays_seconds:
        if delay > 0:
            time.sleep(delay)
        try:
            return pool.getconn()
        except Exception as exc:
            if PoolError is None or not isinstance(exc, PoolError):
                raise
            last_pool_error = exc
    raise RuntimeError("Postgres connection pool exhausted after retries (~750ms).") from last_pool_error


def _sql(sql: str) -> str:
    if DB_BACKEND == "postgres":
        return sql.replace("?", "%s")
    return sql


def _db_exec(sql: str, params: Tuple[Any, ...] = ()) -> None:
    with _db_lock:
        conn = _db()
        try:
            conn.cursor().execute(_sql(sql), params)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _db_query_one(sql: str, params: Tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute(_sql(sql), params)
            return cur.fetchone()
        finally:
            conn.close()


def _db_query_all(sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute(_sql(sql), params)
            return list(cur.fetchall())
        finally:
            conn.close()


def _db_run_in_transaction(fn):
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            result = fn(conn, cur)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _require_jwt_secret() -> None:
    if not JWT_SECRET or len(JWT_SECRET) < 24:
        raise HTTPException(
            status_code=500,
            detail="Server misconfigured: JWT_SECRET missing/too short. Set JWT_SECRET in Railway variables (>=24 chars).",
        )


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))


def _sign(data: bytes) -> str:
    sig = hmac.new(JWT_SECRET.encode("utf-8"), data, hashlib.sha256).digest()
    return _b64url(sig)


def _verify_token(token: str) -> Dict[str, Any]:
    _require_jwt_secret()
    try:
        h, p, s = token.split(".")
        msg = f"{h}.{p}".encode("utf-8")
        expected = _sign(msg)
        if not hmac.compare_digest(expected, s):
            raise ValueError("bad signature")
        payload = json.loads(_b64url_decode(p).decode("utf-8"))
        exp = int(payload.get("exp", 0))
        if exp and int(time.time()) > exp:
            raise ValueError("expired")
        return payload
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


def _flag_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return int(value) == 1

    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off", ""}:
        return False

    try:
        return int(text) == 1
    except Exception:
        return False


def _row_has_key(row: Any, key: str) -> bool:
    if row is None:
        return False
    if hasattr(row, "keys"):
        try:
            return key in row.keys()
        except Exception:
            pass
    if isinstance(row, dict):
        return key in row
    return False


def _user_block_state(row: Any) -> Dict[str, Any]:
    is_disabled = _flag_to_bool(row["is_disabled"]) if _row_has_key(row, "is_disabled") else False
    is_suspended = _flag_to_bool(row["is_suspended"]) if _row_has_key(row, "is_suspended") else False
    reason = None
    detail = None
    if is_disabled:
        reason = "disabled"
        detail = "Account disabled"
    elif is_suspended:
        reason = "suspended"
        detail = "Account suspended"
    return {
        "is_disabled": is_disabled,
        "is_suspended": is_suspended,
        "is_blocked": bool(reason),
        "reason": reason,
        "detail": detail,
    }


def _enforce_user_not_blocked(row: Any) -> None:
    state = _user_block_state(row)
    if state["is_blocked"]:
        raise HTTPException(status_code=403, detail=state["detail"])


def _auth_user_from_request(req: Request) -> sqlite3.Row:
    auth = req.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = auth.split(" ", 1)[1].strip()
    payload = _verify_token(token)
    uid = int(payload.get("uid", 0))
    row = _db_query_one("SELECT * FROM users WHERE id=? LIMIT 1", (uid,))
    if not row:
        raise HTTPException(status_code=401, detail="User not found")
    _enforce_user_not_blocked(row)
    return row


def _enforce_trial_or_admin(user: sqlite3.Row) -> None:
    if not ENFORCE_TRIAL:
        return
    if int(user["is_admin"]) == 1:
        return
    trial_expires_at_raw = user["trial_expires_at"] if "trial_expires_at" in user.keys() else None
    trial_expires_at: Optional[int]
    try:
        trial_expires_at = int(trial_expires_at_raw) if trial_expires_at_raw is not None else None
    except Exception:
        trial_expires_at = None

    if trial_expires_at is None or trial_expires_at <= 0:
        trial_expires_at = int(time.time()) + max(1, TRIAL_DAYS) * 86400
        try:
            _db_exec("UPDATE users SET trial_expires_at=? WHERE id=?", (trial_expires_at, int(user["id"])))
        except Exception:
            return

    if int(time.time()) > trial_expires_at:
        raise HTTPException(status_code=402, detail="Trial expired")


def _enforce_access_or_admin(user: sqlite3.Row) -> None:
    """Full access ladder: admin → comp → active subscription → active trial → HTTP 402."""
    if not ENFORCE_TRIAL:
        return
    if int(user["is_admin"]) == 1:
        return

    now = int(time.time())

    def _normalize_subscription_status(raw_status: Any) -> str:
        if raw_status is None:
            return ""
        return str(raw_status).strip().lower()

    def _status_allows_paid_window_access(raw_status: Any) -> bool:
        return _normalize_subscription_status(raw_status) in {"active", "cancelled", "canceled", "past_due"}

    def _has_paid_window_access(row: sqlite3.Row, now_unix: int) -> bool:
        try:
            raw_status = row["subscription_status"] if "subscription_status" in row.keys() else None
        except Exception:
            raw_status = None
        if not _status_allows_paid_window_access(raw_status):
            return False
        try:
            period_end = row["subscription_current_period_end"] if "subscription_current_period_end" in row.keys() else None
            period_end_int = int(period_end) if period_end is not None else None
        except Exception:
            period_end_int = None
        if period_end_int is None:
            return False
        return now_unix < period_end_int

    try:
        sub_status = user["subscription_status"] if "subscription_status" in user.keys() else None
    except Exception:
        sub_status = None

    if sub_status == "comp":
        try:
            comp_expires = user["subscription_comp_expires_at"] if "subscription_comp_expires_at" in user.keys() else None
            comp_expires_int = int(comp_expires) if comp_expires is not None else None
        except Exception:
            comp_expires_int = None
        if comp_expires_int is None or comp_expires_int <= 0:
            return
        if now < comp_expires_int:
            return

    if _has_paid_window_access(user, now):
        return

    trial_expires_at_raw = user["trial_expires_at"] if "trial_expires_at" in user.keys() else None
    try:
        trial_expires_at = int(trial_expires_at_raw) if trial_expires_at_raw is not None else None
    except Exception:
        trial_expires_at = None

    # No silent auto-grant of a fresh trial here. auth_signup sets trial_expires_at
    # at account creation, and the grandfather migration set it for existing users.
    # A user with NULL/zero trial_expires_at at this point either:
    #   - is a legacy account with no trial record and no comp/subscription → correctly 402
    #   - had their comp/subscription lapse → correctly 402
    # Previously this block auto-issued a fresh 7-day trial, which silently extended
    # access for expired-comp and expired-subscription users. That is not the intended
    # access ladder per the subscription plan.
    if trial_expires_at is not None and trial_expires_at > 0 and now < trial_expires_at:
        return

    raise HTTPException(status_code=402, detail="Subscription required")


def require_user_basic(req: Request) -> sqlite3.Row:
    """Verify token and block state only. Does not enforce subscription access."""
    user = _auth_user_from_request(req)
    return user


def require_user(req: Request) -> sqlite3.Row:
    user = require_user_basic(req)
    _enforce_access_or_admin(user)
    return user


def _clean_display_name(name: str, email: str) -> str:
    n = (name or "").strip()
    if not n:
        n = (email.split("@")[0] if "@" in email else "Driver")
    n = " ".join(n.split())
    if len(n) > 28:
        n = n[:28]
    return n


CURRENT_PBKDF2_ITERATIONS = 200_000


def _hash_password(password: str, salt_b64: Optional[str] = None, iterations: int = CURRENT_PBKDF2_ITERATIONS) -> Tuple[str, str]:
    if salt_b64 is None:
        salt = secrets.token_bytes(16)
        salt_b64 = _b64url(salt)
    else:
        salt = _b64url_decode(salt_b64)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iterations))
    return salt_b64, _b64url(dk)


def _make_token(payload: Dict[str, Any]) -> str:
    _require_jwt_secret()
    header = {"alg": "HS256", "typ": "JWT"}
    h = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    p = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    msg = f"{h}.{p}".encode("utf-8")
    s = _sign(msg)
    return f"{h}.{p}.{s}"


def _make_live_token(*, user_id: int, stream: str, ttl_seconds: Optional[int] = None) -> str:
    now = int(time.time())
    exp = now + int(ttl_seconds or LIVE_TOKEN_TTL_SECONDS)
    return _make_token(
        {
            "typ": "live",
            "uid": int(user_id),
            "scope": str(stream).strip().lower(),
            "iat": now,
            "exp": exp,
        }
    )


def _verify_live_token(token: str, *, expected_stream: Optional[str] = None) -> Dict[str, Any]:
    payload = _verify_token(token)
    if str(payload.get("typ") or "").strip().lower() != "live":
        raise HTTPException(status_code=401, detail="Invalid live token")
    scope = str(payload.get("scope") or "").strip().lower()
    if not scope:
        raise HTTPException(status_code=401, detail="Invalid live token")
    if expected_stream and scope != str(expected_stream).strip().lower():
        raise HTTPException(status_code=403, detail="Live token scope mismatch")
    uid = int(payload.get("uid", 0) or 0)
    if uid <= 0:
        raise HTTPException(status_code=401, detail="Invalid live token")
    return payload
