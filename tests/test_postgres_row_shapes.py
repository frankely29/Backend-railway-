"""Postgres returns dict-like rows; SQLite returns tuples. Code has to survive both.

This file exists because of a bug that was invisible for weeks: create_post read
the id from `RETURNING id` as `cur.fetchone()[0]`. That works on SQLite, where a
row is a tuple. The Postgres pool is built with cursor_factory=RealDictCursor,
so a row is dict-like and `[0]` raises KeyError -- a 500 on EVERY post.

The entire test suite runs on SQLite, so the whole thing passed while production
could not create a single post. These tests exercise the Postgres shape directly
rather than the backend, so a database is not needed to catch it.
"""
from __future__ import annotations

import importlib
import re
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi import HTTPException


@pytest.fixture()
def service(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-rowshape-")
    data_dir = Path(temp_dir.name)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("COMMUNITY_DB", str(data_dir / "community.db"))
    monkeypatch.setenv("JWT_SECRET", "test-jwt-secret-abcdefghijklmnopqrstuvwxyz")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    for name in ["core", "social_db", "social_models", "social_identity",
                 "social_moderation", "social_service"]:
        sys.modules.pop(name, None)
    module = importlib.import_module("social_service")
    yield module
    temp_dir.cleanup()


class FakeCursor:
    """One row, handed back in whatever shape the backend would use."""

    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class RealDictRowLike(dict):
    """psycopg2's RealDictRow IS a dict subclass, so a dict is a faithful stand-in:
    `row[0]` raises KeyError on both."""


def test_a_postgres_dict_row_yields_the_id(service):
    # The exact shape production hands back, and the exact call that was failing.
    assert service._returned_id(FakeCursor(RealDictRowLike({"id": 4106}))) == 4106


def test_a_sqlite_tuple_row_still_yields_the_id(service):
    assert service._returned_id(FakeCursor((4106,))) == 4106


def test_the_old_way_really_would_have_failed(service):
    """Proof the test above is testing something.

    If RealDictRow were subscriptable by position this whole class of bug would
    not exist, and this file would be pointless.
    """
    row = RealDictRowLike({"id": 4106})
    with pytest.raises(KeyError):
        _ = row[0]


def test_a_row_that_never_came_back_is_a_clean_500(service):
    """Not a TypeError on None. An insert that returned nothing is a real
    failure and should say so rather than crashing on the next line."""
    with pytest.raises(HTTPException) as caught:
        service._returned_id(FakeCursor(None))
    assert caught.value.status_code == 500


def test_a_dict_row_under_another_column_name_still_works(service):
    """Belt and braces: a RETURNING that aliases the column differently should
    not send this back to square one."""
    assert service._returned_id(FakeCursor(RealDictRowLike({"post_id": 77}))) == 77


def _code_only(path: Path) -> str:
    """Source with docstrings and comments removed.

    These tests assert on the presence and absence of specific call shapes, and
    the comments in this module name those very shapes to explain them -- so a
    naive search matches the prose about the bug rather than the bug.
    """
    text = re.sub(r'"""[\s\S]*?"""', "", path.read_text(encoding="utf-8"))
    return "\n".join(l for l in text.split("\n") if not l.strip().startswith("#"))


def test_no_psycopg_path_reads_a_row_by_position(service):
    """The guard that stops this coming back.

    Any `fetchone()[0]` in social_service.py is a Postgres 500 waiting to
    happen. DuckDB elsewhere in the codebase returns tuples and is fine; this
    module talks to the app database only.
    """
    code = _code_only(Path(service.__file__))
    assert "fetchone()[0]" not in code, (
        "social_service.py reads a row by position again -- on Postgres that is "
        "a KeyError and a 500, and no SQLite test will catch it"
    )


def test_every_returning_insert_goes_through_the_helper(service):
    """A new RETURNING insert that reads the row itself would repeat the bug."""
    code = _code_only(Path(service.__file__))
    returning = code.count("RETURNING id")
    through_helper = code.count("_returned_id(cur)")
    assert returning == through_helper, (
        f"{returning} RETURNING inserts but {through_helper} use _returned_id"
    )
