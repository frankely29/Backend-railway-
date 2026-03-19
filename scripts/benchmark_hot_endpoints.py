from __future__ import annotations

import argparse
import statistics
import time
from typing import Dict, List

import requests


def _benchmark(url: str, headers: Dict[str, str], runs: int) -> Dict[str, float]:
    latencies: List[float] = []
    payload_sizes: List[int] = []
    status_codes: List[int] = []
    for _ in range(max(1, int(runs))):
        started = time.perf_counter()
        resp = requests.get(url, headers=headers, timeout=30)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        latencies.append(elapsed_ms)
        payload_sizes.append(len(resp.content))
        status_codes.append(resp.status_code)
    ordered = sorted(latencies)
    p95_index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95))))
    return {
        "avg_ms": round(statistics.mean(latencies), 2),
        "p50_ms": round(statistics.median(latencies), 2),
        "p95_ms": round(ordered[p95_index], 2),
        "avg_bytes": round(statistics.mean(payload_sizes), 2),
        "status_code": status_codes[-1],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark hot backend endpoints.")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--frame-index", type=int, default=0)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    headers = {"Authorization": f"Bearer {args.token}", "Accept-Encoding": "gzip"}
    base = args.base_url.rstrip("/")
    targets = {
        "timeline": f"{base}/timeline",
        "frame": f"{base}/frame/{int(args.frame_index)}",
        "presence": f"{base}/presence/all?mode=lite&zoom=14&limit=250&min_lat=40.70&min_lng=-74.03&max_lat=40.82&max_lng=-73.90",
        "presence_viewport": f"{base}/presence/viewport?zoom=14&limit=250&min_lat=40.70&min_lng=-74.03&max_lat=40.82&max_lng=-73.90",
        "presence_delta": f"{base}/presence/delta?updated_since_ms=0&zoom=14&limit=250&min_lat=40.70&min_lng=-74.03&max_lat=40.82&max_lng=-73.90",
        "public_chat_summary": f"{base}/chat/public/summary?after=0",
        "private_chat_summary": f"{base}/chat/private/summary",
        "pickups": f"{base}/events/pickups/recent?limit=50&zone_sample_limit=40&min_lat=40.70&min_lng=-74.03&max_lat=40.82&max_lng=-73.90",
    }

    results = {name: _benchmark(url, headers, args.runs) for name, url in targets.items()}
    for name, payload in results.items():
        print(f"{name}: {payload}")


if __name__ == "__main__":
    main()
