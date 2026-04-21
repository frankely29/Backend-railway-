"""Pytest session bootstrap for the backend test suite.

This conftest runs at pytest session start, BEFORE any test file or production
module is imported by test collection. Its purpose is to set DATA_DIR,
COMMUNITY_DB, FRAMES_DIR, and other env vars to writable, safe defaults so that
production modules like core.py and main.py — whose module-level path
constants are `Path(os.environ.get("DATA_DIR", "/data"))` — read a tmpdir
instead of "/data" (which is not writable on the Ubuntu CI runner).

Individual test fixtures continue to use `monkeypatch.setenv` to override these
defaults with per-test tempdirs. Because this conftest uses
`os.environ.setdefault(...)`, per-test overrides still take precedence. This
conftest only provides a safe fallback for modules imported during pytest
collection (before any fixture runs).

This file intentionally does NOT import any production module, and it does NOT
modify any existing fixture behavior. It is additive only.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path


# ---------------------------------------------------------------------------
# 1. Session-scoped tmpdir used as the default DATA_DIR for any module imported
#    during pytest collection. Individual fixtures override via monkeypatch.
# ---------------------------------------------------------------------------
_SESSION_TMP_ROOT = Path(tempfile.mkdtemp(prefix="tlc-pytest-session-"))

os.environ.setdefault("DATA_DIR", str(_SESSION_TMP_ROOT))
os.environ.setdefault("COMMUNITY_DB", str(_SESSION_TMP_ROOT / "community.db"))
os.environ.setdefault("FRAMES_DIR", str(_SESSION_TMP_ROOT / "frames"))
os.environ.setdefault(
    "JWT_SECRET",
    "test-jwt-secret-abcdefghijklmnopqrstuvwxyz",
)
os.environ.setdefault("ADMIN_EMAIL", "admin@example.com")
os.environ.setdefault("ADMIN_PASSWORD", "password123")


# ---------------------------------------------------------------------------
# 2. Force SQLite mode for tests by removing any Postgres URLs that might be
#    present in the CI environment. Every existing fixture already does this
#    via monkeypatch.delenv, so session-level removal is consistent.
# ---------------------------------------------------------------------------
os.environ.pop("DATABASE_URL", None)
os.environ.pop("POSTGRES_URL", None)


# ---------------------------------------------------------------------------
# 3. Ensure the repository root is on sys.path so tests can `import main`,
#    `import core`, etc. This matches the `sys.path.insert(0, str(REPO_ROOT))`
#    pattern that several test files already do manually.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
