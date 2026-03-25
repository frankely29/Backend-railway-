# ACTIVE ARCHITECTURE

## Current pass: Phase 8 Brooklyn v3 live visible cutover (backend only)
- `brooklyn_v3` is now the live visible Brooklyn profile.
- `brooklyn_v2` remains available for fallback/debug comparison.
- `citywide_v3` remains the live visible citywide score.
- `manhattan_v3` remains the live visible Manhattan profile.
- `bronx_wash_heights_v3` remains the live visible Bronx/Wash Heights profile.
- `queens_v3` remains the live visible Queens profile.
- Staten Island visible profile remains unchanged in Phase 8 (`staten_island_v2`).

## Current pass: Phase 7 Queens v3 live visible cutover (backend only)
- `queens_v3` is now the live visible Queens profile.
- `queens_v2` remains available for fallback/debug comparison.
- `citywide_v3` remains the live visible citywide score.
- `manhattan_v3` remains the live visible Manhattan profile.
- `bronx_wash_heights_v3` remains the live visible Bronx/Wash Heights profile.
- Brooklyn / Staten Island visible profiles remain unchanged in Phase 7 (`brooklyn_v2`, `staten_island_v2`).

## Current pass: Phase 6 Bronx/Wash Heights v3 live visible cutover (backend only)
- `bronx_wash_heights_v3` is now the live visible Bronx/Wash Heights profile.
- `bronx_wash_heights_v2` remains available for fallback/debug comparison.
- `citywide_v3` remains the live visible citywide score.
- `manhattan_v3` remains the live visible Manhattan profile.
- Queens / Brooklyn / Staten Island visible profiles remain unchanged in Phase 6 (`queens_v2`, `brooklyn_v2`, `staten_island_v2`).

## Current pass: Phase 4 borough_v3 shadow candidates (backend only)
- Added borough-specific `*_v3` shadow candidates for Manhattan, Bronx/Wash Heights, Queens, Brooklyn, and Staten Island.
- Visible borough profiles remain unchanged in Phase 4 (`manhattan_v2`, `bronx_wash_heights_v2`, `queens_v2`, `brooklyn_v2`, `staten_island_v2`).
- `citywide_v3` remains the live visible citywide score.

## Current pass: Phase 3 citywide_v3 citywide visible cutover
- `citywide_v3` is now the live visible citywide profile in backend rollout metadata.
- `citywide_v2` remains retained and available for comparison/debug in manifest metadata.
- Borough visible profiles remain unchanged in Phase 3 (`manhattan_v2`, `bronx_wash_heights_v2`, `queens_v2`, `brooklyn_v2`, `staten_island_v2`).

## Current pass: Phase 1 density + trip-quality shadow metrics
- Added backend geometry helper `zone_geometry_metrics.py` to compute Taxi Zone polygon area (square miles) from `taxi_zones.geojson` during hotspot builds.
- `build_hotspot.py` now registers temporary DuckDB table `zone_geometry_metrics` and emits additional shadow-only frame properties:
  - zone-size density raw/normalized fields
  - 20+ minute trip-share raw/normalized fields
  - same-zone retention raw/normalized penalty fields
- `zone_earnings_engine.py` now joins zone geometry area and outputs these added metrics per pickup zone x dow x 20-minute bin in shadow form.
- Visible map scores/buckets/colors remain unchanged in this phase.

## App shape
- `main.py` owns the FastAPI app, core auth/profile/presence/events routes, startup schema initialization, and compatibility endpoints.
- `chat.py` is mounted under `/chat` and owns public room chat, DM routes, voice note routes, summary routes, and additive SSE routes.
- `leaderboard_routes.py`, `pickup_recording_feature.py`, `admin_routes.py`, `admin_mutation_routes.py`, and `admin_trips_routes.py` provide the remaining product surfaces.
- `build_hotspot.py` now runs two backend-only scoring paths during frame generation:
  - legacy visible hotspot score (still active for `rating`/`bucket`/`style.fillColor`)
  - shared HVFHV factual shadow engine (`zone_earnings_engine.py`) with emitted citywide + Manhattan + Bronx/Wash Heights + Queens + Brooklyn + Staten Island v2 shadow fields for future phases.

## Active auth routes
- `POST /auth/signup`
- `POST /auth/login`
- `GET /me`
- `POST /me/update`
- `POST /me/change_password`
- `POST /me/delete_account`

Auth uses Bearer tokens signed with `JWT_SECRET`. The auth dependency loads the user row and rejects disabled/suspended users before downstream handlers run.

## Active presence routes
- `POST /presence/update`
- `GET /presence/all` (backward-compatible compatibility snapshot)
- `GET /presence/viewport` (snapshot or delta when `updated_since_ms` is supplied)
- `GET /presence/delta`
- `GET /presence/summary`

Presence visibility is derived from:
- freshness window,
- ghost mode,
- disabled status,
- suspended status.

## Active public chat routes
- Compatibility routes from `main.py`:
  - `POST /chat/send`
  - `GET /chat/recent`
  - `GET /chat/since`
- Current room-based routes from `chat.py`:
  - `GET /chat/rooms/{room}`
  - `GET /chat/rooms/{room}/summary`
  - `POST /chat/rooms/{room}`
  - `POST /chat/rooms/{room}/voice`
  - `GET /chat/public/summary`

## Active private / DM routes
- Legacy-compatible thread routes:
  - `GET /chat/dm/{other_user_id}`
  - `GET /chat/dm/{other_user_id}/summary`
  - `POST /chat/dm/{other_user_id}`
  - `POST /chat/dm/{other_user_id}/voice`
- Current private chat routes:
  - `GET /chat/private/threads`
  - `GET /chat/private/summary`
  - `GET /chat/private/{other_user_id}`
  - `POST /chat/private/{other_user_id}`
  - `POST /chat/private/{other_user_id}/voice`

## Active additive live-chat routes
- `GET /chat/live/capabilities`
- `GET /chat/live/status`
- `GET /chat/public/events`
- `GET /chat/private/events`
- `GET /chat/rooms/{room}/events`

The browser-safe path is:
1. frontend calls `/chat/live/capabilities` with normal Bearer auth,
2. backend returns short-lived signed SSE URLs,
3. EventSource connects with `live_token` query params rather than the long-lived login token.

## Active leaderboard / pickup / admin / police routes
- Police:
  - `POST /events/police`
  - `GET /events/police`
- Pickup:
  - `POST /events/pickup`
  - `GET /events/pickups/recent`
  - pickup-recording router endpoints from `pickup_recording_feature.py`
- Leaderboard:
  - mounted from `leaderboard_routes.py` (`/leaderboard/...`)
- Admin summary/report routes:
  - `/admin/summary`
  - `/admin/users`
  - `/admin/live`
  - `/admin/reports/police`
  - `/admin/reports/pickups`
  - `/admin/system`
  - `/admin/trips/summary`
  - `/admin/trips/recent`
  - mutation routes such as `/admin/users/{user_id}/set-suspended`, `/admin/users/{user_id}/set-admin`, `/admin/users/disable`

## Legacy vs current chat overlap
- `POST /chat/send`, `GET /chat/recent`, and `GET /chat/since` are compatibility wrappers over the same `chat_messages` table used by `/chat/rooms/global`.
- DMs are available through both `/chat/dm/{other_user_id}` compatibility routes and `/chat/private/{other_user_id}` current routes.
- Room-specific SSE exists, but the main Safe Phase 2 contract is `/chat/public/events` plus `/chat/private/events`.

## Current disabled vs suspended behavior
- Disabled users:
  - cannot log in,
  - fail `require_user`,
  - are hidden from public driver profile lookups,
  - are removed from visible presence,
  - are filtered out of public chat history reads and DM thread directory payloads.
- Suspended users:
  - same blocked behavior as disabled users,
  - are also removed from live presence immediately by admin mutation flow.

## Current ghost-mode behavior
- Ghost mode still accepts `POST /presence/update` writes.
- Ghosted users do not appear on public map presence snapshots/deltas.
- Ghosted users still count toward `online_count` but increase `ghosted_count`, while `visible_count` excludes them.
- Ghost mode is distinct from disable/suspend: ghosted users remain authenticated and can still use non-map features unless separately blocked.

## Current delete-account cleanup scope
`/me/delete_account` now removes or cleans runtime-linked data from:
- `presence`
- `presence_runtime_state`
- `chat_messages`
- `private_chat_messages`
- `events`
- `pickup_logs`
- `pickup_guard_state`
- `driver_work_state`
- `driver_daily_stats`
- `leaderboard_badges_current`
- `users`

It also anonymizes `recommendation_outcomes.user_id` and deletes avatar thumbs plus stored chat audio files owned by the user.

## Current DB backend switching behavior
- SQLite mode works even when `psycopg2` is absent.
- Postgres mode requires `psycopg2`; if it is missing, the runtime now fails with a clear Postgres-only error when the DB helpers are used.
- Postgres connections are pooled through a shared threaded pool.

## Team Joseo score rollout (Phase 12 final-live)
- Team Joseo score rollout is now final-live across all visible cutovers: `citywide_v2`, `manhattan_v2`, `bronx_wash_heights_v2`, `queens_v2`, `brooklyn_v2`, and `staten_island_v2`.
- Backend frame generation still emits all shadow/output profile fields plus `scoring_shadow_manifest.json`, now finalized with explicit production-live manifest semantics.
- User-facing wording no longer needs to expose legacy/shadow terminology, while debug/audit tooling continues to preserve technical source truth.
- No score formulas, mode source-selection precedence, presence timing, polling behavior, or real-time presence logic were changed in this phase.
