from __future__ import annotations

import json
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
    temp_dir = tempfile.TemporaryDirectory(prefix="pickup-stress-")
    data_dir = Path(temp_dir.name)
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ["COMMUNITY_DB"] = str(data_dir / "community.db")
    os.environ["JWT_SECRET"] = "pickup-stress-secret-1234567890"
    os.environ["ADMIN_EMAIL"] = "admin@example.com"
    os.environ["ADMIN_PASSWORD"] = "password123"

    zones = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"LocationID": 1, "zone": "Zone 1", "borough": "Manhattan"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-74.01, 40.71], [-73.99, 40.71], [-73.99, 40.73], [-74.01, 40.73], [-74.01, 40.71]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"LocationID": 2, "zone": "Zone 2", "borough": "Brooklyn"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-73.98, 40.72], [-73.96, 40.72], [-73.96, 40.74], [-73.98, 40.74], [-73.98, 40.72]]],
                },
            },
        ],
    }
    (data_dir / "taxi_zones.geojson").write_text(json.dumps(zones), encoding="utf-8")

    import main  # pylint: disable=import-outside-toplevel

    main.startup()
    client = TestClient(main.app)
    return temp_dir, main, client


def _seed_pickups(main_module) -> str:
    now = int(time.time())
    salt, password_hash = main_module._hash_password("password123")
    admin_is_bool = main_module._is_bool_column("users", "is_admin")
    disabled_is_bool = main_module._is_bool_column("users", "is_disabled")
    ghost_is_bool = main_module._is_bool_column("users", "ghost_mode")
    admin_user_id = 20_000
    main_module._db_exec(
        """
        INSERT INTO users(
            id, email, pass_salt, pass_hash, is_admin, is_disabled, created_at, trial_expires_at,
            display_name, ghost_mode, map_identity_mode
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            admin_user_id,
            "pickup-admin@example.com",
            salt,
            password_hash,
            True if admin_is_bool else 1,
            False if disabled_is_bool else 0,
            now,
            now + (30 * 86400),
            "Admin",
            False if ghost_is_bool else 0,
            "name",
        ),
    )
    for trip_id in range(1, 240):
        zone_id = 1 if trip_id % 2 == 0 else 2
        lat = 40.715 + ((trip_id % 12) * 0.001)
        lng = -74.005 + ((trip_id % 12) * 0.001)
        main_module._db_exec(
            """
            INSERT INTO pickup_logs(id, user_id, lat, lng, zone_id, zone_name, borough, frame_time, created_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                trip_id,
                admin_user_id,
                lat,
                lng,
                zone_id,
                f"Zone {zone_id}",
                "Manhattan" if zone_id == 1 else "Brooklyn",
                "2026-03-18T12:00:00Z",
                now - (trip_id * 45),
            ),
        )
    return main_module._make_token({"uid": admin_user_id, "email": "pickup-admin@example.com", "exp": now + 86400})


def _exercise(client: TestClient, token: str, path: str, requests_per_wave: int, waves: int) -> Dict[str, float]:
    headers = {"Authorization": f"Bearer {token}", "Accept-Encoding": "gzip"}
    latencies: List[float] = []
    payload_sizes: List[int] = []

    def one_call() -> None:
        started = time.perf_counter()
        response = client.get(path, headers=headers)
        latencies.append((time.perf_counter() - started) * 1000.0)
        payload_sizes.append(len(response.content))

    with ThreadPoolExecutor(max_workers=requests_per_wave) as pool:
        for _ in range(waves):
            futures = [pool.submit(one_call) for _ in range(requests_per_wave)]
            for future in futures:
                future.result()
    ordered = sorted(latencies)
    p95_index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95))))
    return {
        "requests": len(latencies),
        "avg_ms": round(statistics.mean(latencies), 2),
        "p95_ms": round(ordered[p95_index], 2),
        "avg_bytes": round(statistics.mean(payload_sizes), 2),
    }


def main() -> None:
    temp_dir, main_module, client = _bootstrap_app()
    try:
        token = _seed_pickups(main_module)
        same_view = _exercise(
            client,
            token,
            "/events/pickups/recent?limit=60&zone_sample_limit=30&min_lat=40.70&min_lng=-74.03&max_lat=40.82&max_lng=-73.90",
            requests_per_wave=10,
            waves=2,
        )
        nearby_views = _exercise(
            client,
            token,
            "/events/pickups/recent?limit=60&zone_sample_limit=30&min_lat=40.71&min_lng=-74.02&max_lat=40.80&max_lng=-73.92",
            requests_per_wave=10,
            waves=2,
        )
        metrics = client.get("/admin/performance/metrics", headers={"Authorization": f"Bearer {token}"}).json()
        print(f"same_view: {same_view}")
        print(f"nearby_views: {nearby_views}")
        print(f"metrics: {metrics}")
    finally:
        temp_dir.cleanup()


if __name__ == "__main__":
    main()
