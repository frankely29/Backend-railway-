from __future__ import annotations

import base64
import asyncio
import builtins
import importlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request


@pytest.fixture()
def app_env(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-regression-")
    data_dir = Path(temp_dir.name)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("COMMUNITY_DB", str(data_dir / "community.db"))
    monkeypatch.setenv("FRAMES_DIR", str(data_dir / "frames"))
    monkeypatch.setenv("JWT_SECRET", "test-jwt-secret-abcdefghijklmnopqrstuvwxyz")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "password123")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_URL", raising=False)

    for name in ["core", "chat", "account_runtime", "leaderboard_db", "leaderboard_routes", "leaderboard_service", "leaderboard_tracker", "pickup_recording_feature", "games_models", "games_service", "games_routes", "admin_routes", "admin_trips_routes", "admin_trips_service", "admin_mutation_routes", "admin_service", "admin_mutation_service", "admin_security", "admin_test_routes", "admin_test_service", "admin_load_test_service", "admin_load_test_models", "main"]:
        sys.modules.pop(name, None)

    main = importlib.import_module("main")
    main.startup()

    frames_dir = Path(os.environ["FRAMES_DIR"])
    frames_dir.mkdir(parents=True, exist_ok=True)
    (frames_dir / "timeline.json").write_text(json.dumps({"timeline": ["2026-03-19T00:00:00Z"], "count": 1}), encoding="utf-8")
    (frames_dir / "frame_000000.json").write_text(json.dumps({"type": "FeatureCollection", "features": []}), encoding="utf-8")
    day_tendency_dir = data_dir / "day_tendency"
    day_tendency_dir.mkdir(parents=True, exist_ok=True)
    (day_tendency_dir / "model.json").write_text(json.dumps({"version": "test"}), encoding="utf-8")

    with TestClient(main.app) as client:
        yield main, client, data_dir

    temp_dir.cleanup()


def _signup(client: TestClient, email: str, password: str = "password123", display_name: str = "Driver") -> dict:
    response = client.post(
        "/auth/signup",
        json={"email": email, "password": password, "display_name": display_name},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _make_request(path: str) -> Request:
    raw_path, _, query = path.partition("?")
    async def _receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": raw_path,
            "raw_path": raw_path.encode("utf-8"),
            "query_string": query.encode("utf-8"),
            "headers": [],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
            "root_path": "",
        },
        receive=_receive,
    )


def _seed_avatar_png(main_module, user_id: int) -> None:
    avatar_png = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4////fwAJ+wP9KobjigAAAABJRU5ErkJggg==")
    avatar_data_url = "data:image/png;base64," + base64.b64encode(avatar_png).decode("ascii")
    version = main_module.avatar_version_for_data_url(avatar_data_url)
    main_module.persist_avatar_thumb(main_module.DATA_DIR, user_id, avatar_data_url, version)
    main_module._db_exec(
        "UPDATE users SET avatar_url=?, avatar_version=? WHERE id=?",
        (avatar_data_url, version, int(user_id)),
    )


def _reload_module_without_psycopg2(monkeypatch, *, postgres_url: str | None = None):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-core-import-")
    data_dir = Path(temp_dir.name)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("COMMUNITY_DB", str(data_dir / "community.db"))
    monkeypatch.setenv("JWT_SECRET", "test-jwt-secret-abcdefghijklmnopqrstuvwxyz")
    if postgres_url:
        monkeypatch.setenv("DATABASE_URL", postgres_url)
    else:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("POSTGRES_URL", raising=False)

    real_import = builtins.__import__

    def _blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "psycopg2" or name.startswith("psycopg2."):
            raise ImportError("blocked for sqlite fallback test")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)
    sys.modules.pop("core", None)
    module = importlib.import_module("core")
    return module, temp_dir


def test_sqlite_core_import_works_without_psycopg2(monkeypatch):
    core, temp_dir = _reload_module_without_psycopg2(monkeypatch, postgres_url=None)
    try:
        assert core.DB_BACKEND == "sqlite"
        conn = core._db()
        conn.close()
        assert core._db_query_one("SELECT 1 AS v")["v"] == 1
    finally:
        temp_dir.cleanup()


def test_postgres_mode_without_psycopg2_fails_clearly(monkeypatch):
    core, temp_dir = _reload_module_without_psycopg2(monkeypatch, postgres_url="postgresql://example/test")
    try:
        assert core.DB_BACKEND == "postgres"
        with pytest.raises(RuntimeError, match="Postgres mode requires psycopg2"):
            core._db()
    finally:
        temp_dir.cleanup()


def test_postgres_pooling_path_uses_threaded_pool_when_available(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-core-postgres-pool-")
    data_dir = Path(temp_dir.name)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("COMMUNITY_DB", str(data_dir / "community.db"))
    monkeypatch.setenv("JWT_SECRET", "test-jwt-secret-abcdefghijklmnopqrstuvwxyz")
    monkeypatch.setenv("DATABASE_URL", "postgresql://example/test")
    sys.modules.pop("core", None)
    core = importlib.import_module("core")

    class FakeConnection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    class FakePool:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.gotten = []
            self.returned = []

        def getconn(self):
            conn = FakeConnection()
            self.gotten.append(conn)
            return conn

        def putconn(self, conn):
            self.returned.append(conn)

    try:
        monkeypatch.setattr(core, "psycopg2", object())
        monkeypatch.setattr(core, "RealDictCursor", object())
        monkeypatch.setattr(core, "ThreadedConnectionPool", FakePool)
        core._postgres_pool = None
        conn = core._db()
        try:
            assert isinstance(conn, core._PooledPostgresConnection)
            assert conn._pool.kwargs["dsn"] == "postgresql://example/test"
        finally:
            conn.close()
        assert len(conn._pool.gotten) == 1
        assert conn._pool.returned == conn._pool.gotten
    finally:
        temp_dir.cleanup()


def test_auth_presence_chat_leaderboard_admin_and_delete_account(app_env):
    main, client, data_dir = app_env

    alice = _signup(client, "alice@example.com", display_name="Alice")
    bob = _signup(client, "bob@example.com", display_name="Bob")

    alice_headers = _auth_headers(alice["token"])
    bob_headers = _auth_headers(bob["token"])

    me_response = client.get("/me", headers=alice_headers)
    assert me_response.status_code == 200
    assert me_response.json()["email"] == "alice@example.com"

    status_response = client.get("/status")
    assert status_response.status_code == 200
    timeline_response = client.get("/timeline", headers=alice_headers)
    assert timeline_response.status_code == 200
    frame_response = client.get("/frame/0", headers=alice_headers)
    assert frame_response.status_code == 200
    day_tendency_response = client.get("/day_tendency/today")
    assert day_tendency_response.status_code == 200

    presence_update = client.post(
        "/presence/update",
        json={"lat": 40.75, "lng": -73.99, "heading": 90, "accuracy": 5},
        headers=alice_headers,
    )
    assert presence_update.status_code == 200
    client.post(
        "/me/update",
        json={"ghost_mode": True},
        headers=bob_headers,
    )
    bob_presence_update = client.post(
        "/presence/update",
        json={"lat": 40.76, "lng": -73.98, "heading": 120, "accuracy": 5},
        headers=bob_headers,
    )
    assert bob_presence_update.status_code == 200

    presence_all = client.get(
        "/presence/all?mode=full&min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=13",
        headers=alice_headers,
    )
    assert presence_all.status_code == 200
    presence_items = presence_all.json()["items"]
    assert any(item["user_id"] == alice["id"] for item in presence_items)
    assert all(item["user_id"] != bob["id"] for item in presence_items)

    viewport_snapshot = client.get(
        "/presence/viewport?min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=13",
        headers=alice_headers,
    )
    assert viewport_snapshot.status_code == 200
    viewport_payload = viewport_snapshot.json()
    assert viewport_payload["mode"] == "snapshot"
    assert any(item["user_id"] == alice["id"] for item in viewport_payload["items"])
    assert all(item["user_id"] != bob["id"] for item in viewport_payload["items"])
    viewport_cursor = viewport_payload["cursor"]

    bob_unghost = client.post("/me/update", json={"ghost_mode": False}, headers=bob_headers)
    assert bob_unghost.status_code == 200
    viewport_delta_visible = client.get(
        f"/presence/delta?updated_since_ms={viewport_cursor}&min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=13",
        headers=alice_headers,
    )
    assert viewport_delta_visible.status_code == 200
    viewport_delta_visible_payload = viewport_delta_visible.json()
    assert any(item["user_id"] == bob["id"] for item in viewport_delta_visible_payload["items"])
    visible_cursor = viewport_delta_visible_payload["cursor"]

    bob_ghost_again = client.post("/me/update", json={"ghost_mode": True}, headers=bob_headers)
    assert bob_ghost_again.status_code == 200
    viewport_delta_removed = client.get(
        f"/presence/viewport?updated_since_ms={visible_cursor}&min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=13",
        headers=alice_headers,
    )
    assert viewport_delta_removed.status_code == 200
    removed_payload = viewport_delta_removed.json()
    assert any(item["user_id"] == bob["id"] and item["reason"] == 'ghost_mode' for item in removed_payload["removed"])

    presence_summary = client.get("/presence/summary", headers=alice_headers)
    assert presence_summary.status_code == 200
    summary_payload = presence_summary.json()
    assert summary_payload["online_count"] >= 2
    assert summary_payload["ghosted_count"] >= 1

    legacy_chat_send = client.post("/chat/send", json={"message": "legacy hello"}, headers=alice_headers)
    assert legacy_chat_send.status_code == 200
    time.sleep(2.05)
    room_chat_send = client.post("/chat/rooms/global", json={"text": "room hello"}, headers=alice_headers)
    assert room_chat_send.status_code == 200
    recent_chat = client.get("/chat/recent", headers=alice_headers)
    assert recent_chat.status_code == 200
    assert [item["message"] for item in recent_chat.json()["items"]][-2:] == ["legacy hello", "room hello"]
    since_chat = client.get("/chat/since?after_id=0", headers=alice_headers)
    assert since_chat.status_code == 200
    assert len(since_chat.json()["items"]) >= 2
    public_summary = client.get("/chat/public/summary?after=1", headers=alice_headers)
    assert public_summary.status_code == 200
    assert public_summary.json()["room"] == "global"
    assert public_summary.json()["latest_message"]["text"] == "room hello"

    time.sleep(2.05)
    dm_send = client.post(f"/chat/dm/{bob['id']}", json={"text": "dm hello"}, headers=alice_headers)
    assert dm_send.status_code == 200
    dm_recent = client.get(f"/chat/dm/{bob['id']}", headers=alice_headers)
    assert dm_recent.status_code == 200
    assert dm_recent.json()["messages"][0]["text"] == "dm hello"
    private_summary = client.get("/chat/private/summary", headers=bob_headers)
    assert private_summary.status_code == 200
    assert private_summary.json()["total_unread_count"] >= 1
    assert any(thread["other_user_id"] == alice["id"] for thread in private_summary.json()["threads"])
    private_dm = client.get(f"/chat/private/{alice['id']}", headers=bob_headers)
    assert private_dm.status_code == 200
    assert private_dm.json()["messages"][0]["text"] == "dm hello"
    dm_summary = client.get(f"/chat/dm/{bob['id']}/summary?after=0", headers=alice_headers)
    assert dm_summary.status_code == 200
    assert dm_summary.json()["latest_message"]["text"] == "dm hello"

    leaderboard_overview = client.get("/leaderboard/overview/me", headers=alice_headers)
    assert leaderboard_overview.status_code == 200
    leaderboard_progression = client.get("/leaderboard/progression/me", headers=alice_headers)
    assert leaderboard_progression.status_code == 200
    leaderboard_ranks = client.get("/leaderboard/ranks", headers=alice_headers)
    assert leaderboard_ranks.status_code == 200

    admin_login = client.post("/auth/login", json={"email": "admin@example.com", "password": "password123"})
    assert admin_login.status_code == 200
    admin_headers = _auth_headers(admin_login.json()["token"])

    for path in [
        "/admin/summary",
        "/admin/users",
        "/admin/live",
        "/admin/reports/police",
        "/admin/reports/pickups",
        "/admin/system",
        "/admin/tests/backend-status",
        "/admin/tests/timeline",
        "/admin/tests/frame-current",
        "/admin/tests/presence-summary",
        "/admin/tests/presence-live",
        "/admin/tests/pickup-reports",
        "/admin/tests/presence-endpoint",
        "/admin/tests/pickup-overlay-endpoint",
        "/admin/trips/summary",
        "/admin/trips/recent",
    ]:
        response = client.get(path, headers=admin_headers)
        assert response.status_code == 200, (path, response.text)

    suspend_bob = client.post(
        f"/admin/users/{bob['id']}/set-suspended",
        json={"is_suspended": True},
        headers=admin_headers,
    )
    assert suspend_bob.status_code == 200
    suspended_login = client.post("/auth/login", json={"email": "bob@example.com", "password": "password123"})
    assert suspended_login.status_code == 403
    suspended_me = client.get("/me", headers=bob_headers)
    assert suspended_me.status_code == 403
    suspended_profile = client.get(f"/drivers/{bob['id']}/profile", headers=alice_headers)
    assert suspended_profile.status_code == 404
    suspended_presence_delta = client.get(
        f"/presence/delta?updated_since_ms={visible_cursor}&min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=13",
        headers=alice_headers,
    )
    assert suspended_presence_delta.status_code == 200
    assert any(item["user_id"] == bob["id"] and item["reason"] == "suspended" for item in suspended_presence_delta.json()["removed"])

    disable_alice = client.post("/admin/users/disable", json={"user_id": alice["id"], "disabled": True}, headers=admin_headers)
    assert disable_alice.status_code == 200
    disabled_login = client.post("/auth/login", json={"email": "alice@example.com", "password": "password123"})
    assert disabled_login.status_code == 403

    erin = _signup(client, "erin@example.com", display_name="Erin")
    carol = _signup(client, "carol@example.com", display_name="Carol")
    carol_headers = _auth_headers(carol["token"])
    _seed_avatar_png(main, carol["id"])
    client.post("/presence/update", json={"lat": 40.71, "lng": -73.95, "heading": 15, "accuracy": 4}, headers=carol_headers)
    client.post("/chat/send", json={"message": "cleanup public"}, headers=carol_headers)
    time.sleep(2.05)
    client.post(f"/chat/dm/{erin['id']}", json={"text": "cleanup private"}, headers=carol_headers)

    now = int(time.time())
    main._db_exec(
        "INSERT INTO events(type, user_id, lat, lng, text, zone_id, created_at, expires_at) VALUES(?,?,?,?,?,?,?,?)",
        ("police", int(carol["id"]), 40.71, -73.95, "watch out", 1, now, now + 3600),
    )
    main._db_exec(
        """
        INSERT INTO pickup_logs(user_id, lat, lng, zone_id, zone_name, borough, frame_time, created_at, is_voided, counted_for_pickup_stats, guard_reason)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (int(carol["id"]), 40.71, -73.95, 1, "Midtown", "Manhattan", "2026-03-19T00:00:00Z", now, 0, 1, "ok"),
    )
    main._db_exec(
        "INSERT OR REPLACE INTO pickup_guard_state(user_id, last_seen_at, last_lat, last_lng, previous_session_end_at, previous_session_end_lat, previous_session_end_lng, movement_streak_started_at, last_meaningful_motion_at) VALUES(?,?,?,?,?,?,?,?,?)",
        (int(carol["id"]), now, 40.71, -73.95, now - 600, 40.70, -73.96, now - 300, now - 60),
    )
    main._db_exec(
        "INSERT OR REPLACE INTO driver_work_state(user_id, last_seen_at, last_lat, last_lng, last_heading, updated_at) VALUES(?,?,?,?,?,?)",
        (int(carol["id"]), now, 40.71, -73.95, 10.0, now),
    )
    nyc_date = "2026-03-19"
    main._db_exec(
        "INSERT OR REPLACE INTO driver_daily_stats(user_id, nyc_date, miles_worked, hours_worked, trips_recorded, pickups_recorded, heartbeat_count, updated_at) VALUES(?,?,?,?,?,?,?,?)",
        (int(carol["id"]), nyc_date, 10.0, 2.0, 3, 1, 5, now),
    )
    main._db_exec(
        "INSERT OR REPLACE INTO leaderboard_badges_current(user_id, metric, period, period_key, rank_position, badge_code, awarded_at, is_current) VALUES(?,?,?,?,?,?,?,?)",
        (int(carol["id"]), "miles", "daily", nyc_date, 1, "gold", now, 1),
    )
    main._db_exec(
        "INSERT INTO recommendation_outcomes(user_id, recommended_at, zone_id, cluster_id, score, confidence, converted_to_trip, minutes_to_trip) VALUES(?,?,?,?,?,?,?,?)",
        (int(carol["id"]), now, 1, "cluster-a", 0.9, 0.8, 0, None),
    )

    avatar_dir = data_dir / "avatar_thumbs" / str(carol["id"])
    assert avatar_dir.exists()

    cleanup_preview = client.get("/events/pickups/recent", headers=carol_headers)
    assert cleanup_preview.status_code == 200

    delete_account = client.post("/me/delete_account", headers=carol_headers)
    assert delete_account.status_code == 200
    cleanup = delete_account.json()["cleanup"]
    assert cleanup["deleted"]["users"] == 1
    assert cleanup["deleted"]["chat_messages"] >= 1
    assert cleanup["deleted"]["private_chat_messages"] >= 1
    assert cleanup["anonymized"].get("recommendation_outcomes", 0) == 1
    assert not avatar_dir.exists()
    assert main._db_query_one("SELECT id FROM users WHERE id=?", (int(carol["id"]),)) is None
    assert main._db_query_one("SELECT user_id FROM presence WHERE user_id=?", (int(carol["id"]),)) is None
    assert main._db_query_one("SELECT user_id FROM events WHERE user_id=?", (int(carol["id"]),)) is None
    assert main._db_query_one("SELECT user_id FROM pickup_logs WHERE user_id=?", (int(carol["id"]),)) is None
    assert main._db_query_one("SELECT user_id FROM pickup_guard_state WHERE user_id=?", (int(carol["id"]),)) is None
    assert main._db_query_one("SELECT user_id FROM driver_work_state WHERE user_id=?", (int(carol["id"]),)) is None
    assert main._db_query_one("SELECT user_id FROM driver_daily_stats WHERE user_id=?", (int(carol["id"]),)) is None
    assert main._db_query_one("SELECT user_id FROM leaderboard_badges_current WHERE user_id=?", (int(carol["id"]),)) is None
    reco_row = main._db_query_one("SELECT user_id FROM recommendation_outcomes WHERE zone_id=? LIMIT 1", (1,))
    assert reco_row is not None and reco_row["user_id"] is None


def test_pickup_guard_evaluation_and_driver_profile_contract(app_env):
    main, client, _ = app_env
    dana = _signup(client, "dana@example.com", display_name="Dana")
    headers = _auth_headers(dana["token"])

    profile_response = client.get(f"/drivers/{dana['id']}/profile", headers=headers)
    assert profile_response.status_code == 200
    profile_payload = profile_response.json()
    assert profile_payload["user"]["id"] == dana["id"]
    assert "daily" in profile_payload and "weekly" in profile_payload

    from pickup_recording_feature import evaluate_pickup_guard

    guard = evaluate_pickup_guard(user_id=int(dana["id"]), lat=40.73, lng=-73.99, now_ts=int(time.time()))
    assert guard["ok"] is False
    assert guard["code"] in {"pickup_needs_recent_driving", "pickup_same_position", "pickup_cooldown_active"}


def test_chat_sse_requires_auth_and_reports_status(app_env):
    _, client, _ = app_env
    alice = _signup(client, "alice-sse@example.com", display_name="Alice SSE")
    alice_headers = _auth_headers(alice["token"])

    no_auth_public = client.get("/chat/public/events")
    assert no_auth_public.status_code == 401

    no_auth_private = client.get("/chat/private/events")
    assert no_auth_private.status_code == 401

    live_status = client.get("/chat/live/status", headers=alice_headers)
    assert live_status.status_code == 200
    assert live_status.json()["sse"]["heartbeat_seconds"] >= 1
    no_auth_capabilities = client.get("/chat/live/capabilities")
    assert no_auth_capabilities.status_code == 401

    capabilities = client.get("/chat/live/capabilities", headers=alice_headers)
    assert capabilities.status_code == 200
    capabilities_payload = capabilities.json()
    assert capabilities_payload["ok"] is True
    assert capabilities_payload["public"]["enabled"] is True
    assert capabilities_payload["private"]["enabled"] is True
    assert capabilities_payload["ttl_seconds"] == capabilities_payload["live_token_ttl_seconds"]
    assert 30 <= capabilities_payload["ttl_seconds"] <= 90

    chat_module = sys.modules["chat"]
    public_parts = urlsplit(capabilities_payload["public"]["url"])
    public_query = parse_qs(public_parts.query)
    assert public_query["live_token"]
    public_response = asyncio.run(
        chat_module.public_chat_events(
            _make_request(public_parts.path + "?" + public_parts.query),
            None,
            public_query["live_token"][0],
        )
    )
    assert public_response.media_type == "text/event-stream"

    private_parts = urlsplit(capabilities_payload["private"]["url"])
    private_query = parse_qs(private_parts.query)
    assert private_query["live_token"]
    private_response = asyncio.run(
        chat_module.private_summary_events(
            _make_request(private_parts.path + "?" + private_parts.query),
            None,
            private_query["live_token"][0],
        )
    )
    assert private_response.media_type == "text/event-stream"

    invalid_public = client.get("/chat/public/events?live_token=bad-token")
    assert invalid_public.status_code == 401

    invalid_private = client.get("/chat/private/events?live_token=bad-token")
    assert invalid_private.status_code == 401

    scope_mismatch = client.get(f"/chat/private/events?live_token={public_query['live_token'][0]}")
    assert scope_mismatch.status_code == 403


def test_public_chat_sse_delivers_new_message_and_replay(app_env):
    _, client, _ = app_env
    chat_module = sys.modules["chat"]
    alice = _signup(client, "alice-public-sse@example.com", display_name="Alice Public")
    bob = _signup(client, "bob-public-sse@example.com", display_name="Bob Public")
    alice_headers = _auth_headers(alice["token"])
    bob_headers = _auth_headers(bob["token"])

    subscriber, _ = chat_module._live_event_broker.subscribe(chat_module._public_channel("global"), None)
    try:
        send_response = client.post("/chat/send", json={"message": "sse hello"}, headers=bob_headers)
        assert send_response.status_code == 200
        message_event = subscriber.get(timeout=2)
    finally:
        chat_module._live_event_broker.unsubscribe(chat_module._public_channel("global"), subscriber)

    assert message_event["id"]
    assert message_event["event"] == "chat.message"
    assert message_event["data"]["type"] == "chat.message"
    assert message_event["data"]["room"] == "global"
    assert message_event["data"]["text"] == "sse hello"
    assert message_event["data"]["sender_user_id"] == bob["id"]

    replay_subscriber, replay = chat_module._live_event_broker.subscribe(
        chat_module._public_channel("global"),
        int(message_event["id"]) - 1,
    )
    try:
        assert replay
        replay_event = replay[-1]
    finally:
        chat_module._live_event_broker.unsubscribe(chat_module._public_channel("global"), replay_subscriber)

    assert replay_event["id"] == message_event["id"]
    assert replay_event["data"]["message_id"] == message_event["data"]["message_id"]


def test_private_chat_sse_delivers_dm_summary_updates(app_env):
    _, client, _ = app_env
    chat_module = sys.modules["chat"]
    alice = _signup(client, "alice-dm-sse@example.com", display_name="Alice DM")
    bob = _signup(client, "bob-dm-sse@example.com", display_name="Bob DM")
    alice_headers = _auth_headers(alice["token"])
    bob_headers = _auth_headers(bob["token"])

    subscriber, _ = chat_module._live_event_broker.subscribe(chat_module._dm_summary_channel(bob["id"]), None)
    try:
        dm_send = client.post(f"/chat/dm/{bob['id']}", json={"text": "dm sse hello"}, headers=alice_headers)
        assert dm_send.status_code == 200
        dm_event = subscriber.get(timeout=2)
    finally:
        chat_module._live_event_broker.unsubscribe(chat_module._dm_summary_channel(bob["id"]), subscriber)

    assert dm_event["id"]
    assert dm_event["event"] == "dm.thread_updated"
    assert dm_event["data"]["type"] == "dm.thread_updated"
    assert dm_event["data"]["other_user_id"] == alice["id"]
    assert dm_event["data"]["sender_user_id"] == alice["id"]
    assert dm_event["data"]["recipient_user_id"] == bob["id"]
    assert dm_event["data"]["text_preview"] == "dm sse hello"
    assert dm_event["data"]["unread_count"] >= 1


def test_chat_live_tokens_expire_and_respect_block_state(app_env):
    _, client, _ = app_env
    core_module = sys.modules["core"]
    alice = _signup(client, "alice-live-expiry@example.com", display_name="Alice Live Expiry")
    admin_login = client.post("/auth/login", json={"email": "admin@example.com", "password": "password123"})
    assert admin_login.status_code == 200
    admin_headers = _auth_headers(admin_login.json()["token"])

    expired_public = core_module._make_live_token(user_id=alice["id"], stream="public", ttl_seconds=-1)
    expired_response = client.get(f"/chat/public/events?live_token={expired_public}")
    assert expired_response.status_code == 401

    valid_private = core_module._make_live_token(user_id=alice["id"], stream="private", ttl_seconds=60)
    disable_response = client.post(
        "/admin/users/disable",
        json={"user_id": alice["id"], "disabled": True},
        headers=admin_headers,
    )
    assert disable_response.status_code == 200

    disabled_private = client.get(f"/chat/private/events?live_token={valid_private}")
    assert disabled_private.status_code == 403


def test_presence_contract_shapes_remain_stable(app_env):
    main, client, _ = app_env
    alice = _signup(client, "alice-presence-contract@example.com", display_name="Alice Presence Contract")
    bob = _signup(client, "bob-presence-contract@example.com", display_name="Bob Presence Contract")
    alice_headers = _auth_headers(alice["token"])
    bob_headers = _auth_headers(bob["token"])

    alice_update = client.post(
        "/presence/update",
        json={"lat": 40.75, "lng": -73.99, "heading": 45, "accuracy": 5},
        headers=alice_headers,
    )
    assert alice_update.status_code == 200

    viewport_snapshot = client.get(
        "/presence/viewport?min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=13",
        headers=alice_headers,
    )
    assert viewport_snapshot.status_code == 200
    snapshot_payload = viewport_snapshot.json()
    assert snapshot_payload["ok"] is True
    assert snapshot_payload["mode"] == "snapshot"
    assert isinstance(snapshot_payload["items"], list)
    assert snapshot_payload["removed"] == []
    assert isinstance(snapshot_payload["cursor"], int)
    assert isinstance(snapshot_payload["server_time_ms"], int)
    assert snapshot_payload["stale_after_sec"] >= 5
    assert snapshot_payload["server_time_ms"] >= snapshot_payload["cursor"]
    assert {"online_count", "ghosted_count", "visible_count"}.issubset(snapshot_payload.keys())
    assert any(item["user_id"] == alice["id"] for item in snapshot_payload["items"])

    bob_ghost = client.post("/me/update", json={"ghost_mode": True}, headers=bob_headers)
    assert bob_ghost.status_code == 200
    bob_update = client.post(
        "/presence/update",
        json={"lat": 40.76, "lng": -73.98, "heading": 90, "accuracy": 4},
        headers=bob_headers,
    )
    assert bob_update.status_code == 200

    delta_response = client.get(
        f"/presence/delta?updated_since_ms={snapshot_payload['cursor']}&min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=13",
        headers=alice_headers,
    )
    assert delta_response.status_code == 200
    delta_payload = delta_response.json()
    assert delta_payload["ok"] is True
    assert delta_payload["mode"] == "delta"
    assert isinstance(delta_payload["items"], list)
    assert isinstance(delta_payload["removed"], list)
    assert isinstance(delta_payload["cursor"], int)
    assert isinstance(delta_payload["server_time_ms"], int)
    assert delta_payload["stale_after_sec"] >= 5
    assert any(item["user_id"] == bob["id"] and item["reason"] == "ghost_mode" for item in delta_payload["removed"])

    summary_response = client.get("/presence/summary", headers=alice_headers)
    assert summary_response.status_code == 200
    summary_payload = summary_response.json()
    assert summary_payload["ok"] is True
    assert summary_payload["online_count"] >= 2
    assert summary_payload["ghosted_count"] >= 1
    assert summary_payload["visible_count"] >= 1
    assert summary_payload["stale_after_sec"] >= 5

    presence_all = client.get(
        "/presence/all?mode=lite&min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=13",
        headers=alice_headers,
    )
    assert presence_all.status_code == 200
    all_payload = presence_all.json()
    assert all_payload["ok"] is True
    assert isinstance(all_payload["items"], list)
    assert all_payload["visible_count"] == len(all_payload["items"])
    assert all_payload["stale_after_sec"] >= 5
    assert all(item["user_id"] != bob["id"] for item in all_payload["items"])

    _seed_avatar_png(main, int(bob["id"]))
    main._db_exec("UPDATE users SET avatar_version=NULL WHERE id=?", (int(bob["id"]),))
    bob_unghost = client.post("/me/update", json={"ghost_mode": False}, headers=bob_headers)
    assert bob_unghost.status_code == 200
    bob_update_again = client.post(
        "/presence/update",
        json={"lat": 40.761, "lng": -73.981, "heading": 95, "accuracy": 4},
        headers=bob_headers,
    )
    assert bob_update_again.status_code == 200
    presence_with_avatar = client.get(
        "/presence/all?mode=lite&min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=13",
        headers=alice_headers,
    )
    assert presence_with_avatar.status_code == 200
    bob_item = next(item for item in presence_with_avatar.json()["items"] if item["user_id"] == bob["id"])
    assert bob_item["avatar_thumb_url"].startswith("/avatars/thumb/")
    assert bob_item["avatar_version"]


def test_admin_load_test_control_plane(app_env):
    _main, client, _data_dir = app_env
    load_service = importlib.import_module("admin_load_test_service")
    manager = load_service.get_load_test_manager()
    manager.reset_for_tests()

    admin_login = client.post("/auth/login", json={"email": "admin@example.com", "password": "password123"})
    assert admin_login.status_code == 200
    admin_headers = _auth_headers(admin_login.json()["token"])

    user = _signup(client, "load-user@example.com", display_name="Load User")
    user_headers = _auth_headers(user["token"])

    denied = client.get("/admin/tests/load/capabilities", headers=user_headers)
    assert denied.status_code == 403

    capabilities = client.get("/admin/tests/load/capabilities", headers=admin_headers)
    assert capabilities.status_code == 200
    capabilities_payload = capabilities.json()
    assert capabilities_payload["ok"] is True
    assert capabilities_payload["details"]["supported_presets"] == [100, 300, 500, 1000, 1500, 2000]

    start_payload = {
        "preset": 100,
        "duration_sec": 30,
        "mode": "map_core",
        "include_presence_writes": True,
        "include_presence_viewport_reads": True,
        "include_presence_summary_reads": True,
        "include_presence_delta_reads": True,
        "notes": "regression test",
        "seed": 12345,
    }
    started = client.post("/admin/tests/load/start", json=start_payload, headers=admin_headers)
    assert started.status_code == 200, started.text
    started_payload = started.json()
    assert started_payload["ok"] is True
    assert started_payload["details"]["status"] == "running"

    second_start = client.post("/admin/tests/load/start", json=start_payload, headers=admin_headers)
    assert second_start.status_code == 409
    second_payload = second_start.json()
    assert second_payload["ok"] is False
    assert "already active" in second_payload["details"]["message"].lower()

    running_status = client.get("/admin/tests/load/status", headers=admin_headers)
    assert running_status.status_code == 200
    running_details = running_status.json()["details"]
    assert running_details["status"] == "running"
    assert running_details["active_run_id"]

    stop_response = client.post("/admin/tests/load/stop", headers=admin_headers)
    assert stop_response.status_code == 200
    assert stop_response.json()["ok"] is True

    last_status = None
    deadline = time.time() + 5.0
    while time.time() < deadline:
        poll = client.get("/admin/tests/load/status", headers=admin_headers)
        assert poll.status_code == 200
        details = poll.json()["details"]
        if details["status"] in {"stopped", "pass", "fail", "error"} and details["last_result"] is not None:
            last_status = details["last_result"]
            break
        time.sleep(0.1)
    assert last_status is not None
    assert last_status["status"] in {"stopped", "pass", "fail"}
    assert last_status["summary"]
    assert isinstance(last_status["checks"], list)
    assert isinstance(last_status["debug"], dict)
    assert "metrics" in last_status["debug"]

    last_response = client.get("/admin/tests/load/last", headers=admin_headers)
    assert last_response.status_code == 200
    last_payload = last_response.json()
    assert last_payload["ok"] is True
    assert last_payload["details"]["last_result"]["run_id"] == last_status["run_id"]

    manager.reset_for_tests()


def test_presence_all_and_viewport_accept_float_zoom(app_env):
    _main, client, _data_dir = app_env
    alice = _signup(client, "float-zoom@example.com", display_name="Float Zoom")
    alice_headers = _auth_headers(alice["token"])

    presence_update = client.post(
        "/presence/update",
        json={"lat": 40.75, "lng": -73.99, "heading": 90, "accuracy": 5},
        headers=alice_headers,
    )
    assert presence_update.status_code == 200

    presence_all = client.get(
        "/presence/all?mode=full&min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=8.83",
        headers=alice_headers,
    )
    assert presence_all.status_code == 200, presence_all.text
    assert presence_all.json()["ok"] is True

    presence_viewport = client.get(
        "/presence/viewport?min_lat=40.70&min_lng=-74.10&max_lat=40.90&max_lng=-73.80&zoom=8.83",
        headers=alice_headers,
    )
    assert presence_viewport.status_code == 200, presence_viewport.text
    assert presence_viewport.json()["ok"] is True


def test_presence_runtime_state_upsert_uses_boolean_visibility(app_env, monkeypatch):
    main, client, _data_dir = app_env
    alice = _signup(client, "boolean-visible@example.com", display_name="Boolean Visible")
    alice_headers = _auth_headers(alice["token"])

    captured_calls: list[tuple[str, tuple[object, ...] | None]] = []
    original_db_exec = main._db_exec

    def recording_db_exec(sql, params=None):
        captured_calls.append((sql, params))
        return original_db_exec(sql, params)

    monkeypatch.setattr(main, "_db_exec", recording_db_exec)

    response = client.post(
        "/presence/update",
        json={"lat": 40.75, "lng": -73.99, "heading": 90, "accuracy": 5},
        headers=alice_headers,
    )
    assert response.status_code == 200, response.text

    runtime_state_params = next(
        params
        for sql, params in captured_calls
        if sql and "INSERT INTO presence_runtime_state" in sql
    )
    assert runtime_state_params is not None
    assert runtime_state_params[2] is True

    state_row = main._db_query_one(
        "SELECT user_id, changed_at_ms, is_visible, reason FROM presence_runtime_state WHERE user_id=?",
        (int(alice["id"]),),
    )
    assert state_row is not None
    assert bool(state_row["is_visible"]) is True
    assert state_row["reason"] is None


def test_presence_update_ghost_mode_stores_false_visibility_boolean(app_env, monkeypatch):
    main, client, _data_dir = app_env
    ghost = _signup(client, "boolean-hidden@example.com", display_name="Boolean Hidden")
    ghost_headers = _auth_headers(ghost["token"])

    toggle_ghost = client.post("/me/update", json={"ghost_mode": True}, headers=ghost_headers)
    assert toggle_ghost.status_code == 200, toggle_ghost.text

    captured_calls: list[tuple[str, tuple[object, ...] | None]] = []
    original_db_exec = main._db_exec

    def recording_db_exec(sql, params=None):
        captured_calls.append((sql, params))
        return original_db_exec(sql, params)

    monkeypatch.setattr(main, "_db_exec", recording_db_exec)

    response = client.post(
        "/presence/update",
        json={"lat": 40.76, "lng": -73.98, "heading": 120, "accuracy": 5},
        headers=ghost_headers,
    )
    assert response.status_code == 200, response.text

    runtime_state_params = next(
        params
        for sql, params in captured_calls
        if sql and "INSERT INTO presence_runtime_state" in sql
    )
    assert runtime_state_params is not None
    assert runtime_state_params[2] is False
    assert runtime_state_params[3] == "ghost_mode"

    state_row = main._db_query_one(
        "SELECT user_id, changed_at_ms, is_visible, reason FROM presence_runtime_state WHERE user_id=?",
        (int(ghost["id"]),),
    )
    assert state_row is not None
    assert bool(state_row["is_visible"]) is False
    assert state_row["reason"] == "ghost_mode"
