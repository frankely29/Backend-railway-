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
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-day-tendency-frame-context-")
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
        "games_models",
        "games_service",
        "games_routes",
        "main",
    ]:
        sys.modules.pop(name, None)

    main = importlib.import_module("main")
    main.startup()

    frames_dir = Path(os.environ["FRAMES_DIR"])
    frames_dir.mkdir(parents=True, exist_ok=True)
    (frames_dir / "timeline.json").write_text(
        json.dumps({"timeline": ["2026-03-19T00:00:00Z"], "count": 1}),
        encoding="utf-8",
    )
    (frames_dir / "frame_000000.json").write_text(
        json.dumps({"type": "FeatureCollection", "features": []}),
        encoding="utf-8",
    )

    model = {
        "version": "borough_tendency_v2",
        "generated_at": "2026-03-01T00:00:00+00:00",
        "bin_minutes": 20,
        "borough_weekday_bin": {},
        "borough_bin": {},
        "borough_baseline": {},
        "global_bin": {
            "24": {
                "score": 42,
                "confidence": 0.8,
                "sample_bins": 18,
                "cohort_type": "global_bin",
                "bin_label": "8:00 AM",
                "explain": "citywide bin",
            }
        },
        "global_baseline": {
            "score": 50,
            "confidence": 0.4,
            "sample_bins": 10,
            "cohort_type": "global_baseline",
            "explain": "citywide baseline",
        },
        "scopes": {
            "manhattan_mode": {
                "borough_weekday_bin": {
                    "manhattan|0|24": {
                        "score": 35,
                        "confidence": 1.0,
                        "sample_bins": 9,
                        "cohort_type": "borough_weekday_bin",
                        "bin_label": "8:00 AM",
                        "explain": "manhattan weekday",
                    }
                },
                "borough_bin": {},
                "borough_baseline": {
                    "manhattan": {
                        "score": 45,
                        "confidence": 0.6,
                        "sample_bins": 12,
                        "cohort_type": "borough_baseline",
                        "explain": "manhattan baseline",
                    }
                },
            }
        },
    }
    day_tendency_dir = data_dir / "day_tendency"
    day_tendency_dir.mkdir(parents=True, exist_ok=True)
    (day_tendency_dir / "model.json").write_text(json.dumps(model), encoding="utf-8")

    with TestClient(main.app) as client:
        yield main, client

    temp_dir.cleanup()


def test_day_tendency_frame_context_returns_separated_global_and_local_contexts(app_env, monkeypatch):
    main, client = app_env
    monkeypatch.setattr(
        main,
        "_resolve_borough_from_lat_lng",
        lambda lat, lng: {"borough": "Manhattan", "borough_key": "manhattan"},
    )

    response = client.get(
        "/day_tendency/frame_context",
        params={
            "frame_time": "2025-01-06T08:00:00",
            "lat": 40.7501,
            "lng": -73.9911,
            "manhattan_mode": 1,
        },
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    assert payload["resolved_scope"]["ready"] is True
    assert payload["resolved_scope"]["scope"] == "manhattan_mode"

    assert payload["global_context"]["status"] == "ok"
    assert payload["global_context"]["context_family"] == "global"
    assert payload["global_context"]["scope"] == "citywide"
    assert payload["global_context"]["cohort_type"] == "global_bin"
    assert payload["global_context"]["bin_index"] == 24

    assert payload["local_context"]["status"] == "ok"
    assert payload["local_context"]["context_family"] == "local"
    assert payload["local_context"]["scope"] == "manhattan_mode"
    assert payload["local_context"]["cohort_type"] == "borough_weekday_bin"
    assert payload["local_context"]["bin_index"] == 24

    assert payload["advanced_context"]["global_penalty_cap"] == 3
    assert payload["advanced_context"]["local_penalty_cap"] == 5
    assert payload["advanced_context"]["total_penalty_cap"] == 8
    assert payload["advanced_context"]["bucket_drop_cap"] == 1


def test_day_tendency_frame_context_keeps_global_context_when_location_is_missing(app_env):
    _main, client = app_env

    response = client.get(
        "/day_tendency/frame_context",
        params={"frame_time": "2025-01-06T13:00:00Z"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    assert payload["resolved_scope"]["ready"] is False
    assert payload["resolved_scope"]["reason"] == "waiting_for_location"

    assert payload["global_context"]["status"] == "ok"
    assert payload["global_context"]["scope"] == "citywide"
    assert payload["global_context"]["context_family"] == "global"
    assert payload["global_context"]["bin_index"] == 24

    assert payload["local_context"]["status"] == "waiting_for_location"
    assert payload["local_context"]["context_family"] == "local"
    assert payload["local_context"]["scope"] is None
    assert payload["advanced_context"]["local_penalty_points"] == 0
    assert payload["advanced_context"]["global_penalty_points"] >= 0


def test_day_tendency_frame_context_invalid_frame_time_returns_400(app_env):
    _main, client = app_env

    response = client.get(
        "/day_tendency/frame_context",
        params={"frame_time": "badvalue"},
    )

    assert response.status_code == 400, response.text
    assert "Invalid frame_time" in response.json()["detail"]
