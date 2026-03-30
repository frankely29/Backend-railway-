from __future__ import annotations

import importlib
import json
import os
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
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-assistant-outlook-")
    data_dir = Path(temp_dir.name)
    frames_dir = data_dir / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("COMMUNITY_DB", str(data_dir / "community.db"))
    monkeypatch.setenv("FRAMES_DIR", str(frames_dir))
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
        "games_models",
        "games_service",
        "games_routes",
        "main",
    ]:
        sys.modules.pop(name, None)

    main = importlib.import_module("main")
    main.startup()

    with TestClient(main.app) as client:
        yield main, client, frames_dir

    temp_dir.cleanup()


def _write_assistant_artifact(frames_dir: Path) -> None:
    payload = {
        "version": 1,
        "bin_minutes": 20,
        "horizon_bins": 6,
        "generated_at": "2026-03-20T00:00:00+00:00",
        "timeline": ["2026-03-20T08:00:00Z"],
        "timeline_index": {
            "2026-03-20T08:00:00Z": {
                "100": {
                    "location_id": "100",
                    "zone_name": "Zone 100",
                    "borough": "Queens",
                    "points": [
                        {
                            "frame_time": "2026-03-20T08:00:00Z",
                            "tracks": {
                                "citywide_v3_shadow": {"rating": 66, "bucket": "blue"},
                                "citywide_shadow": {"rating": 62, "bucket": "blue"},
                            },
                            "raw": {"busy_now_base_n_shadow": 0.32},
                        }
                    ],
                },
                "200": {
                    "location_id": "200",
                    "zone_name": "Zone 200",
                    "borough": "Brooklyn",
                    "points": [
                        {
                            "frame_time": "2026-03-20T08:00:00Z",
                            "tracks": {
                                "citywide_v3_shadow": {"rating": 70, "bucket": "purple"}
                            },
                            "raw": {"busy_now_base_n_shadow": 0.41},
                        }
                    ],
                },
            }
        },
    }
    (frames_dir / "assistant_outlook.json").write_text(json.dumps(payload), encoding="utf-8")


def test_assistant_outlook_single_zone_returns_payload(app_env):
    _main, client, frames_dir = app_env
    _write_assistant_artifact(frames_dir)

    response = client.get(
        "/assistant/outlook",
        params={"frame_time": "2026-03-20T08:00:00Z", "location_ids": "100"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["requested_count"] == 1
    assert payload["returned_count"] == 1
    assert payload["zones"][0]["location_id"] == "100"


def test_assistant_outlook_batch_two_ids_returns_both(app_env):
    _main, client, frames_dir = app_env
    _write_assistant_artifact(frames_dir)

    response = client.get(
        "/assistant/outlook",
        params={"frame_time": "2026-03-20T08:00:00Z", "location_ids": "100,200"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["requested_count"] == 2
    assert payload["returned_count"] == 2
    assert [zone["location_id"] for zone in payload["zones"]] == ["100", "200"]


def test_assistant_outlook_missing_zone_id_is_partial_safe(app_env):
    _main, client, frames_dir = app_env
    _write_assistant_artifact(frames_dir)

    response = client.get(
        "/assistant/outlook",
        params={"frame_time": "2026-03-20T08:00:00Z", "location_ids": "100,999"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["requested_count"] == 2
    assert payload["returned_count"] == 1
    assert [zone["location_id"] for zone in payload["zones"]] == ["100"]


def test_assistant_outlook_invalid_frame_time_returns_error(app_env):
    _main, client, frames_dir = app_env
    _write_assistant_artifact(frames_dir)

    response = client.get(
        "/assistant/outlook",
        params={"frame_time": "2026-03-20T09:00:00Z", "location_ids": "100"},
    )

    assert response.status_code == 404, response.text
    assert "Unknown frame_time" in response.json()["detail"]


def test_assistant_outlook_missing_artifact_returns_clear_error(app_env):
    _main, client, _frames_dir = app_env

    response = client.get(
        "/assistant/outlook",
        params={"frame_time": "2026-03-20T08:00:00Z", "location_ids": "100"},
    )

    assert response.status_code == 409, response.text
    assert "assistant outlook not ready" in response.json()["detail"]
