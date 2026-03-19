from __future__ import annotations

import base64
import importlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


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

    for name in ["core", "chat", "leaderboard_db", "leaderboard_service", "leaderboard_tracker", "pickup_recording_feature", "admin_service", "admin_mutation_service", "main"]:
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


def _seed_avatar_png(main_module, user_id: int) -> None:
    avatar_png = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4////fwAJ+wP9KobjigAAAABJRU5ErkJggg==")
    avatar_data_url = "data:image/png;base64," + base64.b64encode(avatar_png).decode("ascii")
    version = main_module.avatar_version_for_data_url(avatar_data_url)
    main_module.persist_avatar_thumb(main_module.DATA_DIR, user_id, avatar_data_url, version)
    main_module._db_exec(
        "UPDATE users SET avatar_url=?, avatar_version=? WHERE id=?",
        (avatar_data_url, version, int(user_id)),
    )


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
