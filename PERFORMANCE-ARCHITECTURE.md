# PERFORMANCE ARCHITECTURE

## Active database spine
- The backend supports two runtime modes selected at import time from `DATABASE_URL` / `POSTGRES_URL`:
  - `sqlite` when neither environment variable is present.
  - `postgres` when either environment variable is present.
- SQLite uses a process-local serialized lock around every DB helper call.
- Postgres uses `psycopg2.pool.ThreadedConnectionPool` through the shared `_db()` helper so existing callers keep the same helper signatures while avoiding a fresh connection per query.
- SQL placeholder translation still flows through `_sql()` so callers continue to write `?` placeholders even in Postgres mode.

## Hot paths
### Presence
- `POST /presence/update` writes a single `presence` row, updates `presence_runtime_state`, records leaderboard heartbeat data, and updates pickup-guard movement state.
- `GET /presence/all` remains the compatibility route.
- `GET /presence/viewport` is the preferred snapshot contract and can switch into delta mode when `updated_since_ms` is supplied.
- `GET /presence/delta` is the preferred incremental contract and reads only changed rows from `presence_runtime_state`, returning `items`, `removed`, `cursor`, `server_time_ms`, `online_count`, `ghosted_count`, and `visible_count`.
- `GET /presence/summary` reads aggregate counts only.
- `presence_runtime_state.changed_at_ms` is the delta cursor in milliseconds.
- Ghost-mode users still write presence heartbeats but surface through `removed` tombstones or count fields instead of visible marker payloads.

### Chat
- Public chat remains DB-backed with additive in-process SSE fanout.
- Browser EventSource clients authenticate by calling `/chat/live/capabilities` with Bearer auth, then reconnect with short-lived signed `live_token` URLs.
- Signed SSE tickets are HMAC/JWT-style tokens scoped to a user id plus stream type and clamped to a short 30-90 second initiation TTL.
- Writes always persist first, then publish a compact live event.
- Live event broker keeps:
  - bounded per-subscriber queues,
  - bounded replay history,
  - disconnect cleanup via unsubscribe in the generator `finally` block.
- SSE is additive only; polling routes remain the fallback.

### Pickup / leaderboard / admin
- Pickup guard logic uses `pickup_guard_state` plus recent pickup log lookups.
- Leaderboard progression and badge reads flow from `driver_daily_stats`, `driver_work_state`, and `leaderboard_badges_current`.
- Admin summary endpoints aggregate directly from active runtime tables.

## Active indexes and hot-table coverage
- Presence:
  - `presence(updated_at)`
  - `presence(updated_at DESC, user_id)`
  - `presence(updated_at DESC, lat, lng)`
  - `presence_runtime_state(changed_at_ms DESC, user_id)`
- Public chat:
  - `chat_messages(id)`
  - `chat_messages(room, id)`
  - `chat_messages(created_at)`
  - `chat_messages(room, created_at DESC, id DESC)`
- Private chat:
  - `private_chat_messages(sender_user_id, recipient_user_id, created_at, id)`
  - `private_chat_messages(created_at)`
  - `private_chat_messages(sender_user_id, created_at DESC, id DESC)`
  - `private_chat_messages(recipient_user_id, created_at DESC, id DESC)`
  - `private_chat_messages(recipient_user_id, sender_user_id, read_at)`
- Pickup:
  - `pickup_logs(created_at DESC)`
  - `pickup_logs(zone_id, created_at DESC)`
  - `pickup_logs(user_id, created_at DESC)`
- Recommendation outcomes:
  - `recommendation_outcomes(recommended_at DESC)`
- Leaderboard:
  - `leaderboard_badges_current(user_id, is_current, period, metric)`
  - `driver_daily_stats(nyc_date, user_id)`
  - `driver_daily_stats(user_id, updated_at DESC)`
  - `driver_work_state(updated_at DESC)`

## Caching and bounded runtime state
- Presence viewport responses are short-lived cached snapshots keyed by viewport inputs.
- Pickup recent overlays and pickup hotspot bundles use in-memory TTL caches.
- Timeline and frame artifacts use small in-process caches.
- Live SSE replay is intentionally short and bounded; clients are expected to reconcile with summary routes after resets or reconnect gaps.
- `Last-Event-ID` works only within that bounded in-memory history, so polling summaries remain the recovery truth source.

## Diagnostic helpers
- `scripts/benchmark_hot_endpoints.py` provides reproducible timing checks for hot HTTP routes.
- `scripts/load_test_presence.py` exercises presence update/read behavior.
- `scripts/chat_voice_sanity.py` validates chat voice-note paths.
- `/admin/performance/metrics` exposes server-side counters for cache/gzip behavior.
