# Optimization Changelog

## New routes added
- `GET /presence/viewport`
- `GET /presence/delta`
- `GET /chat/public/summary`
- `GET /chat/rooms/{room}/summary`
- `GET /chat/private/summary`
- `GET /chat/dm/{other_user_id}/summary`

## Old routes preserved
- Presence compatibility: `POST /presence/update`, `GET /presence/all`, `GET /presence/summary`
- Legacy public chat compatibility: `POST /chat/send`, `GET /chat/recent`, `GET /chat/since`
- Existing room chat, DM, private thread, pickup, profile, leaderboard, and admin routes were preserved unchanged at the contract level.

## Indexes added
- `presence(updated_at DESC, lat, lng)`
- `presence_runtime_state(changed_at_ms DESC, user_id)`
- `chat_messages(room, created_at DESC, id DESC)`
- `private_chat_messages(sender_user_id, created_at DESC, id DESC)`
- `private_chat_messages(recipient_user_id, created_at DESC, id DESC)`
- `pickup_logs(user_id, created_at DESC)`
- `driver_daily_stats(nyc_date, user_id)`
- `driver_daily_stats(user_id, updated_at DESC)`
- `driver_work_state(updated_at DESC)`

## Query / path optimizations
- Reused a shared presence row serializer for snapshot and delta payloads.
- Added `presence_runtime_state` to power stable presence cursors and tombstones without breaking legacy full snapshots.
- Switched optimized presence reads to a freshness-first query path with viewport filtering and compact payloads.
- Reduced chat hidden-panel polling cost by adding lightweight summary endpoints instead of forcing full-history reads.
- Removed the N+1 latest-message lookup inside private thread listing by selecting thread latest rows in a single query.

## Moderation / account hardening
- Presence runtime state now updates on ghost-mode changes.
- Presence removals are recorded when users are disabled, suspended, or deleted.
- Existing delete-account cleanup path remains intact and still removes runtime-linked data and storage artifacts.

## Hotspot / timeline
- No breaking contract changes.
- Existing cached artifact-serving path remains the primary runtime path.

## Diagnostics
- Updated `scripts/benchmark_hot_endpoints.py` to benchmark presence viewport / delta and chat summary endpoints in addition to timeline, frame, and pickup overlay paths.

## SSE
- Intentionally deferred for deployment safety.
- New cursor- and summary-based helpers were added so SSE can be layered on later without replacing current HTTP polling routes.
