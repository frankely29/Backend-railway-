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
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, Request

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
COMMUNITY_DB_PATH = Path(os.environ.get("COMMUNITY_DB", str(DATA_DIR / "community.db")))
JWT_SECRET = os.environ.get("JWT_SECRET", "")

_db_lock = threading.Lock()


def _db() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(COMMUNITY_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _db_exec(sql: str, params: Tuple[Any, ...] = ()) -> None:
    with _db_lock:
        conn = _db()
        try:
            conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()


def _db_query_one(sql: str, params: Tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.execute(sql, params)
            return cur.fetchone()
        finally:
            conn.close()


def _db_query_all(sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    with _db_lock:
        conn = _db()
        try:
            cur = conn.execute(sql, params)
            return list(cur.fetchall())
        finally:
            conn.close()


def _try_alter(sql: str) -> None:
    with _db_lock:
        conn = _db()
        try:
            try:
                conn.execute(sql)
                conn.commit()
            except sqlite3.OperationalError:
                pass
        finally:
            conn.close()


def _db_init() -> None:
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT NOT NULL UNIQUE,
          pass_salt TEXT NOT NULL,
          pass_hash TEXT NOT NULL,
          is_admin INTEGER NOT NULL DEFAULT 0,
          is_disabled INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL,
          trial_expires_at INTEGER NOT NULL
        );
        """
    )

    _try_alter("ALTER TABLE users ADD COLUMN display_name TEXT;")
    _try_alter("ALTER TABLE users ADD COLUMN ghost_mode INTEGER NOT NULL DEFAULT 0;")

    _db_exec(
        """
        UPDATE users
        SET display_name = COALESCE(display_name, substr(email, 1, instr(email, '@')-1))
        WHERE display_name IS NULL OR trim(display_name) = '';
        """
    )

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS presence (
          user_id INTEGER PRIMARY KEY,
          lat REAL NOT NULL,
          lng REAL NOT NULL,
          heading REAL,
          accuracy REAL,
          updated_at INTEGER NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          type TEXT NOT NULL,
          user_id INTEGER NOT NULL,
          lat REAL NOT NULL,
          lng REAL NOT NULL,
          text TEXT,
          zone_id INTEGER,
          created_at INTEGER NOT NULL,
          expires_at INTEGER NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS chat_messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          room TEXT NOT NULL,
          user_id INTEGER NOT NULL,
          display_name TEXT NOT NULL,
          message TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )

    _db_exec("CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(type, created_at);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);")
    _db_exec("CREATE INDEX IF NOT EXISTS idx_chat_room_id ON chat_messages(room, id);")


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


def _make_token(payload: Dict[str, Any]) -> str:
    _require_jwt_secret()
    header = {"alg": "HS256", "typ": "JWT"}
    h = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    p = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    msg = f"{h}.{p}".encode("utf-8")
    s = _sign(msg)
    return f"{h}.{p}.{s}"


def _parse_token(token: str) -> Dict[str, Any]:
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


def _hash_password(password: str, salt_b64: Optional[str] = None) -> Tuple[str, str]:
    if salt_b64 is None:
        salt = secrets.token_bytes(16)
        salt_b64 = _b64url(salt)
    else:
        salt = _b64url_decode(salt_b64)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return salt_b64, _b64url(dk)


def _verify_password(password: str, salt_b64: str, expected_hash: str) -> bool:
    _, check = _hash_password(password, salt_b64=salt_b64)
    return hmac.compare_digest(check, expected_hash)


def _auth_user_from_request(req: Request) -> sqlite3.Row:
    auth = req.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = auth.split(" ", 1)[1].strip()
    payload = _parse_token(token)
    uid = int(payload.get("uid", 0))
    row = _db_query_one("SELECT * FROM users WHERE id=? LIMIT 1", (uid,))
    if not row:
        raise HTTPException(status_code=401, detail="User not found")
    if int(row["is_disabled"]) == 1:
        raise HTTPException(status_code=403, detail="Account disabled")
    return row


def require_user(req: Request) -> sqlite3.Row:
    user = _auth_user_from_request(req)
    if int(user["is_admin"]) != 1 and int(time.time()) > int(user["trial_expires_at"]):
        raise HTTPException(status_code=402, detail="Trial expired")
    return user


def _clean_display_name(name: str, email: str) -> str:
    n = (name or "").strip()
    if not n:
        n = (email.split("@")[0] if "@" in email else "Driver")
    n = " ".join(n.split())
    if len(n) > 28:
        n = n[:28]
    return n
