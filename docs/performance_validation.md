# Backend performance validation and rollout notes

## What to measure

- `/timeline` latency, payload size, ETag/304 behavior.
- `/frame/{idx}` latency, payload size, cache hit rate.
- `/presence/all` latency, payload size, viewport cache hit rate, and confirmation that payloads no longer include inline `data:image/` avatar blobs.
- `/events/pickups/recent` latency, payload size, pickup response cache hit rate, hotspot cache hit rate, and score-bundle cache hit rate.
- Avatar thumb response cache hit rate.
- Chat public/private fetch sanity and voice-note playback/range support.

## Synthetic validation scripts

- `python3 scripts/benchmark_hot_endpoints.py --base-url <url> --token <token>`
- `python3 scripts/load_test_presence.py`
- `python3 scripts/stress_pickup_overlay.py`
- `python3 scripts/chat_voice_sanity.py`

## Feature parity checklist

- [x] Auth signup/login/token flow remains unchanged.
- [x] Presence updates still write location heartbeats.
- [x] Ghost mode still filters map visibility through the existing ghost-mode predicate.
- [x] User avatar markers are preserved via `avatar_thumb_url`/cacheable thumb assets instead of inline base64 in hot polling payloads.
- [x] Driver profile payload still includes profile avatar data and leaderboard badge data.
- [x] Police reports endpoints remain unchanged.
- [x] Pickup recording, hotspot overlays, and recommendation logging remain in place with caching/throttling added around the expensive read path.
- [x] Public chat, private chat, and voice-note audio routes remain available.
- [x] Leaderboard/admin/game-related backend surfaces remain untouched.
- [x] Timeline and frame payload shapes are preserved.

## Rollout plan

1. Internal-only validation on a single Railway deployment.
2. Expand to 10 drivers and monitor:
   - response p95 for `/presence/all` and `/events/pickups/recent`
   - Railway egress
   - process RSS / memory
   - avatar thumb hit rate
3. Expand to 25 active users.
4. Expand to 50 active users.
5. Expand to 100 active users.
6. Expand to larger cohorts once:
   - presence payload size remains stable
   - pickup overlay cache hit rate remains healthy
   - CPU/RAM stay within budget

## Worker discipline

- Keep the Procfile on a conservative single `uvicorn` web process unless real CPU saturation shows a need for more workers.
- More workers increase memory use and fragment in-memory caches, which can erase a large portion of the savings added here.

## What still needs a larger future rewrite only if scale tests prove it

- If 200+ simultaneous active map sessions still drive high p95 latency on presence despite viewport caching, move presence fan-out to WebSockets or SSE with shared state.
- If pickup overlay generation remains CPU-heavy after cache tuning under production traffic, move hotspot derivation to a dedicated background materializer.
