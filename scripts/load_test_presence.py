from __future__ import annotations

import base64
import os
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _bootstrap_app():
    temp_dir = tempfile.TemporaryDirectory(prefix="presence-load-")
    data_dir = Path(temp_dir.name)
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ["COMMUNITY_DB"] = str(data_dir / "community.db")
    os.environ["JWT_SECRET"] = "presence-load-secret-1234567890"
    os.environ["ADMIN_EMAIL"] = "admin@example.com"
    os.environ["ADMIN_PASSWORD"] = "password123"

    import main  # pylint: disable=import-outside-toplevel

    main.startup()
    client = TestClient(main.app)
    return temp_dir, main, client


def _seed_presence(main_module, total_users: int = 1000) -> Dict[str, str]:
    now = int(time.time())
    tokens: Dict[str, str] = {}
    avatar_blob = base64.b64encode(b"presence-avatar-bytes").decode("ascii")
    avatar_data_url = f"data:image/png;base64,{avatar_blob}"
    admin_is_bool = main_module._is_bool_column("users", "is_admin")
    disabled_is_bool = main_module._is_bool_column("users", "is_disabled")
    ghost_is_bool = main_module._is_bool_column("users", "ghost_mode")

    base_user_id = 10_000
    for offset in range(total_users):
        user_id = base_user_id + offset
        email = f"driver{user_id}@example.com"
        salt, password_hash = main_module._hash_password("password123")
        is_ghost = user_id % 10 == 0
        main_module._db_exec(
            """
            INSERT INTO users(
                id, email, pass_salt, pass_hash, is_admin, is_disabled, created_at,
                trial_expires_at, display_name, ghost_mode, avatar_url, avatar_version, map_identity_mode
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                f"Driver {user_id}",
                bool(is_ghost) if ghost_is_bool else (1 if is_ghost else 0),
                avatar_data_url,
                f"seed-{user_id}",
                "avatar",
            ),
        )
        lat = 40.7000 + ((user_id % 250) * 0.00045)
        lng = -74.0200 + ((user_id % 250) * 0.00045)
        main_module._db_exec(
            """
            INSERT INTO presence(user_id, lat, lng, heading, accuracy, updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
              lat=excluded.lat,
              lng=excluded.lng,
              heading=excluded.heading,
              accuracy=excluded.accuracy,
              updated_at=excluded.updated_at
            """,
            (user_id, lat, lng, 90.0, 5.0, now),
        )
        tokens[email] = main_module._make_token({"uid": user_id, "email": email, "exp": now + 86400})
    return tokens


def _exercise_presence(client: TestClient, token: str, concurrency: int, iterations: int) -> Dict[str, float]:
    headers = {"Authorization": f"Bearer {token}", "Accept-Encoding": "gzip"}
    url = "/presence/all?mode=lite&zoom=14&limit=250&min_lat=40.70&min_lng=-74.03&max_lat=40.82&max_lng=-73.90"
    latencies: List[float] = []
    payload_sizes: List[int] = []
    contains_data_url = False

    def one_call() -> None:
        nonlocal contains_data_url
        started = time.perf_counter()
        response = client.get(url, headers=headers)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        body = response.content
        latencies.append(elapsed_ms)
        payload_sizes.append(len(body))
        contains_data_url = contains_data_url or (b"data:image/" in body)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for _ in range(iterations):
            futures = [pool.submit(one_call) for _ in range(concurrency)]
            for future in futures:
                future.result()

    ordered = sorted(latencies)
    p95_index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95))))
    return {
        "requests": len(latencies),
        "avg_ms": round(statistics.mean(latencies), 2),
        "p95_ms": round(ordered[p95_index], 2),
        "avg_bytes": round(statistics.mean(payload_sizes), 2),
        "contains_data_url": contains_data_url,
    }


def main() -> None:
    temp_dir, main_module, client = _bootstrap_app()
    try:
        tokens = _seed_presence(main_module, total_users=1000)
        viewer_token = next(iter(tokens.values()))
        for concurrency in (50, 100, 200):
            result = _exercise_presence(client, viewer_token, concurrency=concurrency, iterations=1)
            print(f"presence concurrency={concurrency}: {result}", flush=True)
        metrics = client.get("/status").json()
        print(f"status metrics: {metrics.get('performance_metrics')}", flush=True)
    finally:
        temp_dir.cleanup()


if __name__ == "__main__":
    main()
