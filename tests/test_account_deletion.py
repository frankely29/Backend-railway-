"""Deleting an account: does the button actually do the thing?

On production it did not. A driver who had never touched anything could delete
their account; a driver who had posted once got a 500, and both the account and
the post stayed up. The cause is a blind spot this whole suite shares:

    Postgres enforces FOREIGN KEY. SQLite does not, unless the connection runs
    PRAGMA foreign_keys=ON -- and nothing in this backend ever does.

So `DELETE FROM users` quietly succeeded in every test while raising a
foreign-key violation in production. The fixture below turns enforcement on for
the test connection, which makes SQLite behave the way the real database does
and makes this class of bug visible here for the first time.
"""
from __future__ import annotations

import importlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture()
def app_env(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-account-delete-")
    data_dir = Path(temp_dir.name)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("COMMUNITY_DB", str(data_dir / "community.db"))
    monkeypatch.setenv("FRAMES_DIR", str(data_dir / "frames"))
    monkeypatch.setenv("JWT_SECRET", "test-jwt-secret-abcdefghijklmnopqrstuvwxyz")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "password123")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_URL", raising=False)

    for name in [
        "core", "chat", "media_store", "account_runtime",
        "leaderboard_db", "leaderboard_routes", "leaderboard_service", "leaderboard_tracker",
        "pickup_recording_feature", "games_models", "games_service", "games_routes",
        "social_db", "social_models", "social_service", "social_routes", "social_identity",
        "social_moderation", "social_admin_routes", "admin_security",
        "work_battles_db", "access_tokens",
        "main",
    ]:
        sys.modules.pop(name, None)

    main = importlib.import_module("main")
    main.startup()

    frames_dir = Path(os.environ["FRAMES_DIR"])
    frames_dir.mkdir(parents=True, exist_ok=True)
    (frames_dir / "timeline.json").write_text(
        json.dumps({"timeline": ["2026-03-19T00:00:00Z"], "count": 1}), encoding="utf-8")
    (frames_dir / "frame_000000.json").write_text(
        json.dumps({"type": "FeatureCollection", "features": []}), encoding="utf-8")

    with TestClient(main.app) as client:
        yield main, client

    temp_dir.cleanup()


@pytest.fixture()
def enforced(app_env, monkeypatch):
    """The same app, but account deletion runs against a database that enforces
    foreign keys -- which is what production does and what SQLite does not.

    Only account_runtime's connection is swapped. The schema is created by the
    normal startup with enforcement off, exactly as on a real SQLite install;
    what changes is the connection the delete itself runs on.
    """
    import account_runtime
    import core

    def _fk_db():
        conn = sqlite3.connect(str(core.COMMUNITY_DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    monkeypatch.setattr(account_runtime, "_db", _fk_db)
    return app_env


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _signup(client, email, name):
    res = client.post("/auth/signup", json={
        "email": email, "password": "password123", "display_name": name})
    assert res.status_code == 200, res.text
    return res.json()


def _h(account):
    return {"Authorization": f"Bearer {account['token']}"}


def _post(client, account, body="A post"):
    res = client.post("/social/posts", json={"body": body}, headers=_h(account))
    assert res.status_code == 200, res.text
    return res.json()["post"]


def _rows(main, sql, params=()):
    conn = sqlite3.connect(str(__import__("core").COMMUNITY_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _user_exists(main, user_id):
    return bool(_rows(main, "SELECT id FROM users WHERE id=?", (int(user_id),)))


# --------------------------------------------------------------------------
# the blind spot itself
# --------------------------------------------------------------------------

def test_sqlite_does_not_enforce_foreign_keys_here(app_env):
    """The reason every other test in this repo could not have caught the bug.

    If this ever starts failing because the backend turned enforcement on, that
    is good news -- but the `enforced` fixture below becomes redundant and this
    file should be simplified rather than patched around.
    """
    import core

    conn = sqlite3.connect(str(core.COMMUNITY_DB_PATH))
    try:
        enabled = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    finally:
        conn.close()
    assert enabled == 0, "SQLite is enforcing foreign keys now; see the docstring"


def test_the_old_delete_list_really_did_raise(enforced):
    """Non-vacuity: prove the ten-table list this fix replaced actually fails.

    Without this, a passing suite says nothing -- the fix could be covering a
    bug that never existed.
    """
    main, client = enforced
    driver = _signup(client, "old@example.com", "Old List")
    _post(client, driver, "A post, like any driver would write.")

    import core

    conn = sqlite3.connect(str(core.COMMUNITY_DB_PATH))
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        with pytest.raises(sqlite3.IntegrityError):
            # Exactly what the old code did: clear the ten runtime tables, then
            # delete the user. posts still points at them.
            for table in ("presence", "presence_runtime_state", "chat_messages",
                          "events", "pickup_logs", "driver_work_state",
                          "driver_daily_stats", "leaderboard_badges_current"):
                conn.execute(f"DELETE FROM {table} WHERE user_id=?", (driver["id"],))
            conn.execute("DELETE FROM users WHERE id=?", (driver["id"],))
            conn.commit()
    finally:
        conn.rollback()
        conn.close()


# --------------------------------------------------------------------------
# the fix
# --------------------------------------------------------------------------

def test_a_driver_who_posted_can_delete_their_account(enforced):
    main, client = enforced
    driver = _signup(client, "driver@example.com", "Driver")
    post = _post(client, driver, "JFK is stacked, 40 deep.")

    res = client.post("/me/delete_account", headers=_h(driver))
    assert res.status_code == 200, res.text
    assert res.json()["ok"] is True
    assert not _user_exists(main, driver["id"])
    assert not _rows(main, "SELECT id FROM posts WHERE id=?", (post["id"],))


def test_a_driver_tangled_up_with_everyone_can_still_leave(enforced):
    """The realistic case: posts, replies, likes, follows, a block, a report.

    Each of these is a separate foreign key into users(id). Any one of them
    left out of the delete list turns the button back into a 500.
    """
    main, client = enforced
    leaver = _signup(client, "leaver@example.com", "Leaver")
    other = _signup(client, "other@example.com", "Other")

    mine = _post(client, leaver, "Mine.")
    theirs = _post(client, other, "Theirs.")

    # every direction of every relationship
    client.post(f"/social/posts/{theirs['id']}/comments",
                json={"body": "Reply of mine on their post."}, headers=_h(leaver))
    client.post(f"/social/posts/{mine['id']}/comments",
                json={"body": "Reply of theirs on my post."}, headers=_h(other))
    client.post(f"/social/posts/{theirs['id']}/like", headers=_h(leaver))
    client.post(f"/social/posts/{mine['id']}/like", headers=_h(other))
    client.post(f"/social/users/{other['id']}/follow", headers=_h(leaver))
    client.post(f"/social/users/{leaver['id']}/follow", headers=_h(other))
    client.post(f"/social/users/{other['id']}/mute", headers=_h(leaver))
    client.post(f"/social/users/{other['id']}/block", headers=_h(leaver))
    client.post("/social/reports", json={
        "target_type": "post", "target_id": theirs["id"], "reason": "spam"},
        headers=_h(leaver))

    res = client.post("/me/delete_account", headers=_h(leaver))
    assert res.status_code == 200, res.text
    assert not _user_exists(main, leaver["id"])

    # the other driver is untouched, and their post survives
    assert _user_exists(main, other["id"])
    assert _rows(main, "SELECT id FROM posts WHERE id=?", (theirs["id"],))


def test_replies_and_likes_on_a_deleted_drivers_post_go_with_it(enforced):
    """Somebody else's comment cannot outlive the post it was written under."""
    main, client = enforced
    leaver = _signup(client, "leaver@example.com", "Leaver")
    other = _signup(client, "other@example.com", "Other")
    mine = _post(client, leaver, "Mine.")
    client.post(f"/social/posts/{mine['id']}/comments",
                json={"body": "Their reply on my post."}, headers=_h(other))
    client.post(f"/social/posts/{mine['id']}/like", headers=_h(other))

    assert client.post("/me/delete_account", headers=_h(leaver)).status_code == 200

    assert not _rows(main, "SELECT id FROM post_comments WHERE post_id=?", (mine["id"],))
    assert not _rows(main, "SELECT post_id FROM post_likes WHERE post_id=?", (mine["id"],))


def test_the_deleted_driver_stops_appearing_in_other_peoples_feeds(enforced):
    main, client = enforced
    leaver = _signup(client, "leaver@example.com", "Leaver")
    reader = _signup(client, "reader@example.com", "Reader")
    _post(client, leaver, "Should not survive its author.")

    before = client.get("/social/feed?scope=everyone&limit=20", headers=_h(reader)).json()
    assert any(i["author"]["user_id"] == leaver["id"] for i in before["items"])

    assert client.post("/me/delete_account", headers=_h(leaver)).status_code == 200

    after = client.get("/social/feed?scope=everyone&limit=20", headers=_h(reader))
    assert after.status_code == 200, after.text
    assert not any(i["author"]["user_id"] == leaver["id"] for i in after.json()["items"])


def test_the_token_stops_working(enforced):
    main, client = enforced
    driver = _signup(client, "gone@example.com", "Gone")
    _post(client, driver, "Bye.")
    assert client.post("/me/delete_account", headers=_h(driver)).status_code == 200

    res = client.get("/me", headers=_h(driver))
    assert res.status_code in (401, 403, 404), res.status_code


def test_recommendation_history_is_detached_not_destroyed(enforced):
    """The engine learns from outcomes. Take the person, keep the signal."""
    main, client = enforced
    driver = _signup(client, "learner@example.com", "Learner")
    import core

    conn = sqlite3.connect(str(core.COMMUNITY_DB_PATH))
    try:
        for table, cluster_column in (("recommendation_outcomes", "cluster_id"),
                                      ("micro_recommendation_outcomes", "micro_cluster_id")):
            conn.execute(
                f"INSERT INTO {table} (user_id, recommended_at, zone_id, "
                f"{cluster_column}, score, confidence) VALUES (?, ?, ?, ?, ?, ?)",
                (driver["id"], 1_700_000_000, 132, "c-1", 0.81, 0.62))
        conn.commit()
    finally:
        conn.close()

    assert client.post("/me/delete_account", headers=_h(driver)).status_code == 200
    for table in ("recommendation_outcomes", "micro_recommendation_outcomes"):
        rows = _rows(main, f"SELECT user_id FROM {table}")
        assert rows, f"the {table} row was deleted instead of anonymised"
        assert all(r["user_id"] is None for r in rows), table


def test_a_deleted_drivers_photo_leaves_the_disk(enforced, monkeypatch):
    main, client = enforced
    import account_runtime

    unlinked = []
    monkeypatch.setattr(account_runtime, "_safe_unlink_post_image", unlinked.append)

    driver = _signup(client, "shooter@example.com", "Shooter")
    post = _post(client, driver, "With a photo.")
    import core

    conn = sqlite3.connect(str(core.COMMUNITY_DB_PATH))
    try:
        conn.execute("UPDATE posts SET image_path=? WHERE id=?",
                     ("posts/user-1-post-1.jpg", post["id"]))
        conn.commit()
    finally:
        conn.close()

    res = client.post("/me/delete_account", headers=_h(driver))
    assert res.status_code == 200, res.text
    assert unlinked == ["posts/user-1-post-1.jpg"]
    assert res.json()["cleanup"]["post_images_deleted"] == 1


# --------------------------------------------------------------------------
# the guard that outlives this fix
# --------------------------------------------------------------------------

_FK_RE = re.compile(r"FOREIGN\s+KEY\s*\(\s*([A-Za-z0-9_]+)\s*\)\s*REFERENCES\s+users\s*\(", re.I)


def _tables_with_a_users_foreign_key():
    """Read the schema out of the source and report {table: {columns}}."""
    found: dict[str, set[str]] = {}
    for path in sorted(REPO_ROOT.glob("*.py")):
        src = path.read_text(errors="ignore")
        for match in re.finditer(r"CREATE TABLE(?: IF NOT EXISTS)?\s+([A-Za-z0-9_]+)\s*\(", src):
            table = match.group(1)
            i, depth = match.end(), 1
            while i < len(src) and depth:
                if src[i] == "(":
                    depth += 1
                elif src[i] == ")":
                    depth -= 1
                i += 1
            for column in _FK_RE.findall(src[match.end():i - 1]):
                found.setdefault(table, set()).add(column)
    return found


def test_every_table_with_a_users_foreign_key_is_in_the_delete_list():
    """The durable part of this fix.

    A new table with `REFERENCES users(id)` breaks account deletion on
    production and nowhere else. This reads the schema rather than a hand-kept
    list, so adding one fails here instead of in front of a driver.
    """
    import account_runtime

    covered = {table for table, _sql in account_runtime._DELETE_SPECS}
    missing = sorted(set(_tables_with_a_users_foreign_key()) - covered)
    assert not missing, (
        "these tables point at users(id) but are not cleared before the user "
        f"row is deleted, so DELETE FROM users will fail on Postgres: {missing}")


def test_each_covered_table_names_every_column_that_points_at_users():
    """Clearing one of two columns is the same bug, one branch further in."""
    import account_runtime

    by_table = {table: sql for table, sql in account_runtime._DELETE_SPECS}
    problems = []
    for table, columns in sorted(_tables_with_a_users_foreign_key().items()):
        sql = by_table.get(table)
        if sql is None:
            continue  # the test above owns this case
        for column in sorted(columns):
            if f"{column}=?" not in sql:
                problems.append(f"{table}.{column}")
    assert not problems, f"delete clauses never match on: {problems}"


def test_the_delete_list_binds_one_parameter_per_placeholder():
    import account_runtime

    for table, sql in account_runtime._DELETE_SPECS + account_runtime._ANONYMIZE_SPECS:
        assert sql.count("?") >= 1, f"{table} has no placeholder"
        assert sql.strip().upper().startswith(("DELETE", "UPDATE")), table


def test_the_billing_record_is_deliberately_left_alone():
    """Payment history outlives the account. If that ever changes it should be
    a decision, not a diff nobody noticed."""
    import account_runtime

    covered = {table for table, _sql in account_runtime._DELETE_SPECS}
    assert "paddle_webhook_events" not in covered
