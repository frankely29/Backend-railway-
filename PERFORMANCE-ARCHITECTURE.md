# Performance Architecture

## Runtime audited before this pass

### Active auth / account runtime
- `POST /auth/signup`, `POST /auth/login`, `GET /me`, `POST /me/update`, `POST /me/change_password`, `POST /me/delete_account` are live in `main.py`.
- `GET /drivers/{user_id}/profile` is the active profile lookup path.
- Disabled and suspended users are rejected during authenticated access through shared auth helpers; profile lookups return `404` for blocked targets.

### Active presence runtime
- Presence write: `POST /presence/update`.
- Full presence read / compatibility snapshot: `GET /presence/all`.
- Online summary: `GET /presence/summary`.
- Legacy SQLAlchemy-only presence module also contains `POST /presence/update` and `GET /presence/nearby` in `presence.py`, but the active FastAPI app uses the `main.py` routes.

### Active chat runtime
- Legacy global chat compatibility routes in `main.py`: `POST /chat/send`, `GET /chat/recent`, `GET /chat/since`.
- Active room chat routes in `chat.py`: `GET /chat/rooms/{room}`, `POST /chat/rooms/{room}`, `POST /chat/rooms/{room}/voice`.
- Active DM compatibility routes in `chat.py`: `GET /chat/dm/{other_user_id}`, `POST /chat/dm/{other_user_id}`, `POST /chat/dm/{other_user_id}/voice`.
- Active private-thread routes in `chat.py`: `GET /chat/private/threads`, `GET /chat/private/{other_user_id}`, `POST /chat/private/{other_user_id}`, `POST /chat/private/{other_user_id}/voice`.
- Before this pass there were no dedicated lightweight public unread or DM inbox summary endpoints.

### Active hotspot / timeline runtime
- `GET /timeline` and `GET /frame/{idx}` serve prebuilt frame artifacts.
- `GET /day_tendency/today` and `GET /day_tendency/date/{ymd}` serve day tendency artifacts.
- `GET /events/pickups/recent` is the active pickup / hotspot overlay read path.

### Active pickup / save runtime
- `POST /events/pickup` forwards to pickup record creation.
- Pickup guard / save logic lives in `pickup_recording_feature.py` and is initialized at startup.
- Admin pickup recording tools remain live under `/admin/pickup-recording/...`.

### Active admin / moderation runtime
- Legacy admin disable and password reset routes remain live in `main.py`: `POST /admin/users/disable`, `POST /admin/users/reset_password`.
- Current admin read tools remain live in `admin_routes.py` under `/admin/...`.
- Current admin mutation tools remain live in `admin_mutation_routes.py` under `/admin/users/{user_id}/set-admin`, `/admin/users/{user_id}/set-suspended`, and report clear paths.

### Current delete-account cleanup path
- `POST /me/delete_account` calls `delete_account_runtime_data()`.
- Cleanup deletes presence, public chat rows, private chat rows, events, pickup logs, pickup guard state, driver work state, daily stats, leaderboard badges, and the user row; recommendation outcomes are anonymized; avatar thumbs and chat audio files are removed from storage.

### DB helper / connection behavior audited
- `core.py` uses per-call SQLite connections guarded by a re-entrant lock.
- When `DATABASE_URL` / `POSTGRES_URL` is present, `core.py` uses a shared `ThreadedConnectionPool` and returns pooled connections.
- Query helpers `_db_exec`, `_db_query_one`, and `_db_query_all` open a connection per operation and close / return it immediately.

## Hot paths identified
1. Presence writes through `POST /presence/update`.
2. Full or viewport presence reads through `GET /presence/all`.
3. Presence summary reads through `GET /presence/summary`.
4. Global room history polling through `/chat/recent`, `/chat/since`, and `/chat/rooms/{room}`.
5. Private thread list polling through `/chat/private/threads`.
6. DM thread polling through `/chat/dm/{other_user_id}` and `/chat/private/{other_user_id}`.
7. Pickup overlay reads through `GET /events/pickups/recent`.
8. Timeline / frame artifact serving through `GET /timeline` and `GET /frame/{idx}`.

## Compatibility / legacy routes kept live
- `POST /chat/send`, `GET /chat/recent`, and `GET /chat/since` remain intact for legacy frontend behavior.
- `GET /presence/all` remains the full compatibility route.
- Existing room chat, DM, profile, pickup, leaderboard, and admin routes remain live.
- No legacy frontend route was removed in this pass.

## Optimized routes added in this pass

### Presence
- `GET /presence/viewport`
  - Additive optimized presence snapshot endpoint.
  - Supports viewport bounds, zoom-based buffering, optional padding, optional limit, and optional delta mode via `updated_since_ms`.
- `GET /presence/delta`
  - Additive optimized delta endpoint.
  - Supports `updated_since_ms`, viewport filtering, removed/tombstone payloads, compact marker payloads, and stable server cursors.

### Chat lightweight summary / unread paths
- `GET /chat/public/summary`
  - Global public chat unread/check-since summary.
- `GET /chat/rooms/{room}/summary`
  - Room-level public chat unread/check-since summary.
- `GET /chat/private/summary`
  - Inbox summary with total unread and per-thread minimal metadata.
- `GET /chat/dm/{other_user_id}/summary`
  - DM thread-level latest message and unread/check-since metadata.

## Query / schema optimizations in this pass
- Added `presence_runtime_state` to track additive presence cursors and removals without breaking existing presence rows.
- Added presence indexes for `(updated_at, user_id)` plus `(updated_at, lat, lng)` to support freshness-first and viewport-filtered reads.
- Added chat indexes for room + created ordering and sender/recipient recent lookups.
- Added pickup `user_id + created_at` index.
- Added leaderboard `nyc_date + user_id`, `user_id + updated_at`, and `driver_work_state.updated_at` indexes.
- Reused existing artifact caching / gzip paths for timeline and frame serving; no breaking contract changes were introduced there.

## Presence correctness rules preserved
- Self presence writes are still accepted through the existing write path.
- Ghost mode users stay hidden from map-visible presence payloads.
- Ghost mode users still contribute to online summary counts when their presence row is fresh.
- Disabled / suspended users are removed from map presence and blocked from authenticated runtime access.
- Delta payloads return tombstones for ghosting, moderation removals, deletes, and viewport exits after a tracked change.

## Chat correctness rules preserved
- Existing full history endpoints remain the source for open room / thread views.
- New summary endpoints only add lightweight unread / latest-message metadata.
- DM target validation still rejects disabled / suspended users.
- Existing message identity, ordering, and timestamps are preserved.

## SSE status
- SSE was intentionally deferred in this pass to avoid a risky deployment-level rewrite.
- The new summary helpers and stable cursor-style lightweight endpoints create a cleaner boundary for a future SSE layer without requiring another large backend refactor.

## Fallback compatibility guarantees
- Frontend clients can continue using old presence and chat routes unchanged.
- New optimized routes are additive and optional.
- Presence delta / viewport payloads are compact but do not alter the legacy snapshot response contract.
- Moderation, ghost mode, delete-account cleanup, pickup recording, profile, leaderboard, and admin flows remain on their current routes.
