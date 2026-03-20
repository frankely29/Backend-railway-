from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def app_env(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-work-battles-")
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
        "core",
        "chat",
        "leaderboard_db",
        "leaderboard_routes",
        "leaderboard_service",
        "leaderboard_tracker",
        "pickup_recording_feature",
        "work_battles_db",
        "work_battles_models",
        "work_battles_service",
        "work_battles_routes",
        "main",
    ]:
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
        yield main, client

    temp_dir.cleanup()


def _signup(client: TestClient, email: str, display_name: str) -> dict:
    response = client.post(
        "/auth/signup",
        json={"email": email, "password": "password123", "display_name": display_name},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _insert_stats(main_module, user_id: int, nyc_date: str, *, miles: float = 0.0, hours: float = 0.0) -> None:
    main_module._db_exec(
        """
        INSERT INTO driver_daily_stats(user_id, nyc_date, miles_worked, hours_worked, updated_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id, nyc_date) DO UPDATE SET
          miles_worked=excluded.miles_worked,
          hours_worked=excluded.hours_worked,
          updated_at=excluded.updated_at
        """,
        (int(user_id), nyc_date, float(miles), float(hours), 1_700_000_000),
    )


def test_create_pending_daily_miles_challenge(app_env):
    _main, client = app_env
    alice = _signup(client, "wb-alice1@example.com", "Alice")
    bob = _signup(client, "wb-bob1@example.com", "Bob")

    response = client.post(
        "/work-battles/challenges",
        json={"target_user_id": bob["id"], "battle_type": "daily_miles"},
        headers=_headers(alice["token"]),
    )
    assert response.status_code == 200, response.text
    item = response.json()["item"]
    assert item["battle_type"] == "daily_miles"
    assert item["status"] == "pending"
    assert item["challenger_user_id"] == alice["id"]
    assert item["challenged_user_id"] == bob["id"]


def test_prevent_self_challenge(app_env):
    _main, client = app_env
    alice = _signup(client, "wb-alice2@example.com", "Alice")

    response = client.post(
        "/work-battles/challenges",
        json={"target_user_id": alice["id"], "battle_type": "daily_miles"},
        headers=_headers(alice["token"]),
    )
    assert response.status_code == 400


def test_prevent_duplicate_pending_or_active_same_type(app_env):
    _main, client = app_env
    alice = _signup(client, "wb-alice3@example.com", "Alice")
    bob = _signup(client, "wb-bob3@example.com", "Bob")
    cara = _signup(client, "wb-cara3@example.com", "Cara")

    create_one = client.post(
        "/work-battles/challenges",
        json={"target_user_id": bob["id"], "battle_type": "daily_miles"},
        headers=_headers(alice["token"]),
    )
    assert create_one.status_code == 200, create_one.text
    challenge_id = create_one.json()["item"]["id"]

    duplicate = client.post(
        "/work-battles/challenges",
        json={"target_user_id": bob["id"], "battle_type": "daily_miles"},
        headers=_headers(alice["token"]),
    )
    assert duplicate.status_code == 409

    accepted = client.post(f"/work-battles/challenges/{challenge_id}/accept", headers=_headers(bob["token"]))
    assert accepted.status_code == 200, accepted.text

    overlapping = client.post(
        "/work-battles/challenges",
        json={"target_user_id": cara["id"], "battle_type": "daily_miles"},
        headers=_headers(alice["token"]),
    )
    assert overlapping.status_code == 409


def test_accept_challenge_stores_period_fields_and_start_values(app_env):
    main, client = app_env
    alice = _signup(client, "wb-alice4@example.com", "Alice")
    bob = _signup(client, "wb-bob4@example.com", "Bob")

    main._db_exec("UPDATE driver_daily_stats SET miles_worked=miles_worked")
    service = importlib.import_module("work_battles_service")
    now_ms = int(service.datetime(2026, 3, 18, 18, 0, tzinfo=service.timezone.utc).timestamp() * 1000)
    _insert_stats(main, alice["id"], "2026-03-18", miles=12.5)
    _insert_stats(main, bob["id"], "2026-03-18", miles=7.25)

    created = service.create_challenge(alice["id"], bob["id"], "daily_miles", now_ms=now_ms)
    detail = service.accept_challenge(created["id"], bob["id"], now_ms=now_ms)
    row = main._db_query_one("SELECT * FROM work_battle_challenges WHERE id=?", (detail["id"],))

    assert row["period_start_date"] == "2026-03-18"
    assert row["period_end_date"] == "2026-03-18"
    assert row["ends_at_ms"] > now_ms
    assert row["challenger_start_value"] == pytest.approx(12.5)
    assert row["challenged_start_value"] == pytest.approx(7.25)


def test_pending_challenge_expires_after_24_hours(app_env):
    _main, client = app_env
    alice = _signup(client, "wb-alice5@example.com", "Alice")
    bob = _signup(client, "wb-bob5@example.com", "Bob")
    service = importlib.import_module("work_battles_service")

    created = service.create_challenge(alice["id"], bob["id"], "daily_miles", now_ms=1_700_000_000_000)
    service.expire_due_pending_challenges(now_ms=1_700_086_400_001)
    detail = service.get_challenge_detail(created["id"], alice["id"], now_ms=1_700_086_400_001)
    assert detail["status"] == "expired"


def test_active_challenge_finalizes_after_ends_at_ms(app_env):
    main, client = app_env
    alice = _signup(client, "wb-alice6@example.com", "Alice")
    bob = _signup(client, "wb-bob6@example.com", "Bob")
    service = importlib.import_module("work_battles_service")

    _insert_stats(main, alice["id"], "2026-03-17", miles=10)
    _insert_stats(main, bob["id"], "2026-03-17", miles=8)
    created = service.create_challenge(alice["id"], bob["id"], "weekly_miles", now_ms=int(service.datetime(2026, 3, 18, 12, 0, tzinfo=service.timezone.utc).timestamp() * 1000))
    service.accept_challenge(created["id"], bob["id"], now_ms=int(service.datetime(2026, 3, 18, 12, 0, tzinfo=service.timezone.utc).timestamp() * 1000))
    row = main._db_query_one("SELECT ends_at_ms FROM work_battle_challenges WHERE id=?", (created["id"],))
    _insert_stats(main, alice["id"], "2026-03-23", miles=22)
    _insert_stats(main, bob["id"], "2026-03-23", miles=16)

    service.finalize_due_active_challenges(now_ms=int(row["ends_at_ms"]) + 1)
    updated = main._db_query_one("SELECT status, completed_at_ms FROM work_battle_challenges WHERE id=?", (created["id"],))
    assert updated["status"] == "completed"
    assert updated["completed_at_ms"] == int(row["ends_at_ms"]) + 1


def test_completed_winner_uses_trusted_driver_daily_stats_deltas(app_env):
    main, client = app_env
    alice = _signup(client, "wb-alice7@example.com", "Alice")
    bob = _signup(client, "wb-bob7@example.com", "Bob")
    service = importlib.import_module("work_battles_service")
    now_ms = int(service.datetime(2026, 3, 18, 18, 0, tzinfo=service.timezone.utc).timestamp() * 1000)

    _insert_stats(main, alice["id"], "2026-03-18", miles=20)
    _insert_stats(main, bob["id"], "2026-03-18", miles=10)
    created = service.create_challenge(alice["id"], bob["id"], "daily_miles", now_ms=now_ms)
    service.accept_challenge(created["id"], bob["id"], now_ms=now_ms)
    _insert_stats(main, alice["id"], "2026-03-18", miles=31)
    _insert_stats(main, bob["id"], "2026-03-18", miles=14)
    ends_at_ms = main._db_query_one("SELECT ends_at_ms FROM work_battle_challenges WHERE id=?", (created["id"],))["ends_at_ms"]

    detail = service.get_challenge_detail(created["id"], alice["id"], now_ms=int(ends_at_ms) + 1)
    row = main._db_query_one(
        "SELECT result_code, challenger_final_value, challenged_final_value FROM work_battle_challenges WHERE id=?",
        (created["id"],),
    )
    assert detail["status"] == "completed"
    assert row["result_code"] == "challenger_win"
    assert row["challenger_final_value"] == pytest.approx(11.0)
    assert row["challenged_final_value"] == pytest.approx(4.0)


def test_tie_works(app_env):
    main, client = app_env
    alice = _signup(client, "wb-alice8@example.com", "Alice")
    bob = _signup(client, "wb-bob8@example.com", "Bob")
    service = importlib.import_module("work_battles_service")
    now_ms = int(service.datetime(2026, 3, 18, 18, 0, tzinfo=service.timezone.utc).timestamp() * 1000)

    _insert_stats(main, alice["id"], "2026-03-18", hours=2)
    _insert_stats(main, bob["id"], "2026-03-18", hours=1)
    created = service.create_challenge(alice["id"], bob["id"], "daily_hours", now_ms=now_ms)
    service.accept_challenge(created["id"], bob["id"], now_ms=now_ms)
    _insert_stats(main, alice["id"], "2026-03-18", hours=5)
    _insert_stats(main, bob["id"], "2026-03-18", hours=4)
    ends_at_ms = main._db_query_one("SELECT ends_at_ms FROM work_battle_challenges WHERE id=?", (created["id"],))["ends_at_ms"]

    detail = service.get_challenge_detail(created["id"], alice["id"], now_ms=int(ends_at_ms) + 1)
    assert detail["result_code"] == "tie"
    assert detail["status"] == "completed"


def test_work_battle_users_returns_rows(app_env):
    main, client = app_env
    alice = _signup(client, "wb-alice9@example.com", "Alice")
    bob = _signup(client, "wb-bob9@example.com", "Bobby")
    main._db_exec(
        "INSERT INTO presence(user_id, lat, lng, heading, accuracy, updated_at) VALUES(?,?,?,?,?,?)",
        (int(bob["id"]), 40.0, -73.0, None, None, int(importlib.import_module("time").time())),
    )

    response = client.get("/work-battles/users?q=bob&limit=10", headers=_headers(alice["token"]))
    assert response.status_code == 200, response.text
    items = response.json()["items"]
    assert any(item["user_id"] == bob["id"] for item in items)
    assert all(item["user_id"] != alice["id"] for item in items)
    bob_row = next(item for item in items if item["user_id"] == bob["id"])
    assert "level" in bob_row
    assert "rank_icon_key" in bob_row
    assert "leaderboard_badge_code" in bob_row


def test_active_me_returns_null_without_crash(app_env):
    _main, client = app_env
    alice = _signup(client, "wb-alice10@example.com", "Alice")

    response = client.get("/work-battles/active/me", headers=_headers(alice["token"]))
    assert response.status_code == 200, response.text
    assert response.json()["item"] is None
