from __future__ import annotations

import io
import os
import sys
import tempfile
import time
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _bootstrap_app():
    temp_dir = tempfile.TemporaryDirectory(prefix="chat-sanity-")
    data_dir = Path(temp_dir.name)
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ["COMMUNITY_DB"] = str(data_dir / "community.db")
    os.environ["JWT_SECRET"] = "chat-sanity-secret-1234567890"

    import main  # pylint: disable=import-outside-toplevel

    main.startup()
    client = TestClient(main.app)
    return temp_dir, main, client


def _make_user(main_module, user_id: int, email: str):
    now = int(time.time())
    salt, password_hash = main_module._hash_password("password123")
    admin_is_bool = main_module._is_bool_column("users", "is_admin")
    disabled_is_bool = main_module._is_bool_column("users", "is_disabled")
    ghost_is_bool = main_module._is_bool_column("users", "ghost_mode")
    main_module._db_exec(
        """
        INSERT INTO users(
            id, email, pass_salt, pass_hash, is_admin, is_disabled, created_at, trial_expires_at,
            display_name, ghost_mode, map_identity_mode
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            user_id,
            email,
            salt,
            password_hash,
            False if admin_is_bool else 0,
            False if disabled_is_bool else 0,
            now,
            now + (30 * 86400),
            email.split("@")[0],
            False if ghost_is_bool else 0,
            "name",
        ),
    )
    return main_module._make_token({"uid": user_id, "email": email, "exp": now + 86400})


def main() -> None:
    temp_dir, main_module, client = _bootstrap_app()
    try:
        token_a = _make_user(main_module, 30_001, "driver1@example.com")
        token_b = _make_user(main_module, 30_002, "driver2@example.com")
        headers_a = {"Authorization": f"Bearer {token_a}"}
        headers_b = {"Authorization": f"Bearer {token_b}"}

        public_message = client.post("/chat/rooms/global", headers=headers_a, json={"text": "hello world"}).json()
        public_list = client.get("/chat/rooms/global", headers=headers_a).json()
        time.sleep(2.1)
        dm_message = client.post("/chat/dm/30002", headers=headers_a, json={"text": "secret hello"}).json()
        dm_list = client.get("/chat/dm/30001", headers=headers_b).json()
        time.sleep(2.1)

        voice_payload = io.BytesIO(b"RIFF....WEBMVOICE")
        voice_resp = client.post(
            "/chat/rooms/global/voice",
            headers=headers_a,
            files={"audio": ("voice.webm", voice_payload, "audio/webm")},
            data={"duration_ms": "1200", "text": "voice note"},
        ).json()
        if "audio_url" not in voice_resp:
            raise RuntimeError(f"Voice response missing audio_url: {voice_resp}")
        audio_get = client.get(voice_resp["audio_url"], headers=headers_a)
        audio_range = client.get(voice_resp["audio_url"], headers={**headers_a, "Range": "bytes=0-7"})

        print(
            {
                "public_message_id": public_message["id"],
                "public_count": len(public_list["messages"]),
                "dm_message_id": dm_message["id"],
                "dm_count": len(dm_list["messages"]),
                "voice_message_id": voice_resp["id"],
                "audio_status": audio_get.status_code,
                "audio_range_status": audio_range.status_code,
                "audio_cache_control": audio_get.headers.get("Cache-Control"),
            }
        )
    finally:
        temp_dir.cleanup()


if __name__ == "__main__":
    main()
