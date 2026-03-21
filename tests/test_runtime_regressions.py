from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from fastapi.testclient import TestClient


@pytest.fixture()
def app_env(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-runtime-regressions-")
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



def _voice_upload_files(content_type: str, filename: str = "voice-note.bin") -> dict[str, tuple[str, bytes, str]]:
    return {"file": (filename, b"voice-note-payload", content_type)}



def test_public_voice_upload_accepts_parameterized_webm_and_canonicalizes_mime(app_env):
    main, client = app_env
    alice = _signup(client, "voice-webm@example.com", "VoiceWebm")

    response = client.post(
        "/chat/rooms/general/voice",
        headers=_headers(alice["token"]),
        files=_voice_upload_files("audio/webm;codecs=opus", filename="voice.webm"),
        data={"duration_ms": "1200", "text": "webm note"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["message_type"] == "voice"
    assert payload["audio_mime_type"] == "audio/webm"

    row = main._db_query_one(
        "SELECT audio_mime_type FROM chat_messages WHERE id=? LIMIT 1",
        (int(payload["id"]),),
    )
    assert row is not None
    assert row["audio_mime_type"] == "audio/webm"

    audio_response = client.get(payload["audio_url"], headers=_headers(alice["token"]))
    assert audio_response.status_code == 200, audio_response.text
    assert audio_response.headers["content-type"] == "audio/webm"



def test_public_voice_upload_accepts_parameterized_ogg_and_canonicalizes_mime(app_env):
    main, client = app_env
    alice = _signup(client, "voice-ogg@example.com", "VoiceOgg")

    response = client.post(
        "/chat/rooms/general/voice",
        headers=_headers(alice["token"]),
        files=_voice_upload_files("audio/ogg;codecs=opus", filename="voice.ogg"),
        data={"duration_ms": "800", "text": "ogg note"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["audio_mime_type"] == "audio/ogg"

    row = main._db_query_one(
        "SELECT audio_mime_type FROM chat_messages WHERE id=? LIMIT 1",
        (int(payload["id"]),),
    )
    assert row is not None
    assert row["audio_mime_type"] == "audio/ogg"

    audio_response = client.get(payload["audio_url"], headers=_headers(alice["token"]))
    assert audio_response.status_code == 200, audio_response.text
    assert audio_response.headers["content-type"] == "audio/ogg"



def test_private_voice_upload_accepts_x_m4a_alias_and_canonicalizes_mime(app_env):
    main, client = app_env
    alice = _signup(client, "voice-m4a-alice@example.com", "VoiceM4AAlice")
    bob = _signup(client, "voice-m4a-bob@example.com", "VoiceM4ABob")

    response = client.post(
        f"/chat/private/{bob['id']}/voice",
        headers=_headers(alice["token"]),
        files=_voice_upload_files("audio/x-m4a", filename="voice.m4a"),
        data={"duration_ms": "2300", "text": "m4a note"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["message_type"] == "voice"
    assert payload["audio_mime_type"] == "audio/mp4"

    row = main._db_query_one(
        "SELECT audio_mime_type FROM private_chat_messages WHERE id=? LIMIT 1",
        (int(payload["id"]),),
    )
    assert row is not None
    assert row["audio_mime_type"] == "audio/mp4"

    audio_response = client.get(payload["audio_url"], headers=_headers(alice["token"]))
    assert audio_response.status_code == 200, audio_response.text
    assert audio_response.headers["content-type"] == "audio/mp4"



def test_voice_upload_rejects_unsupported_audio_type(app_env):
    _main, client = app_env
    alice = _signup(client, "voice-flac@example.com", "VoiceFlac")

    response = client.post(
        "/chat/rooms/general/voice",
        headers=_headers(alice["token"]),
        files=_voice_upload_files("audio/flac", filename="voice.flac"),
        data={"duration_ms": "700", "text": "flac note"},
    )

    assert response.status_code == 400, response.text
    assert response.json()["detail"] == "Unsupported audio format"
