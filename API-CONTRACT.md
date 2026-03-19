# API Contract

This document marks the hot frontend/backend contracts that must remain backward compatible for the live NYC TLC hotspot system.

## Core map/runtime routes
- `GET /status` returns health, frame readiness, data paths, and performance metrics.
- `GET /timeline` returns the persisted 20-minute-bin timeline payload from `frames/timeline.json`.
- `GET /frame/{idx}` returns a single generated frame payload for the same 20-minute timeline contract.
- `GET /day_tendency/today` and `GET /day_tendency/date/{ymd}` return the separate day-tendency decision layer.

## Auth + profile
- `POST /auth/signup` returns `ok`, `created`, `token`, `id`, `email`, `display_name`, `ghost_mode`, `is_admin`, `trial_expires_at`, and `exp`.
- `POST /auth/login` returns `ok`, `token`, identity fields, and trial expiry metadata.
- `GET /me` returns the current identity plus `avatar_thumb_url`, `avatar_version`, `map_identity_mode`, `ghost_mode`, `is_admin`, `trial_expires_at`, and `leaderboard_badge_code`.
- `POST /me/update`, `POST /me/change_password`, and `POST /me/delete_account` remain available.
- `GET /drivers/{user_id}/profile` keeps returning the nested `user`, `daily`, `weekly`, `monthly`, `yearly`, and `progression` sections expected by the frontend.

## Presence
- `POST /presence/update` still accepts `lat`, `lng`, optional `heading`, and optional `accuracy`.
- `GET /presence/all` still powers map rendering and keeps `mode=full|lite`, viewport filters, zoom, and limit behavior.
- `GET /presence/summary` still returns online and ghosted counts for authenticated viewers.
- Ghost-mode users remain counted in summary while hidden from live rendering payloads.

## Events + pickup overlay
- `POST /events/pickup` stays guarded by pickup eligibility logic.
- `GET /events/pickups/recent` still returns recent points plus zone stats / hotspot attachments used by the overlay.
- Police event/report routes remain intact.

## Chat
- New router contracts remain supported:
  - `GET/POST /chat/rooms/{room}`
  - `POST /chat/rooms/{room}/voice`
  - `GET/POST /chat/dm/{other_user_id}`
  - `POST /chat/dm/{other_user_id}/voice`
  - `GET /chat/private/threads`
  - `GET/POST /chat/private/{other_user_id}`
  - audio fetch routes under `/chat/audio/*`
- Legacy contracts remain supported and backward compatible:
  - `POST /chat/send`
  - `GET /chat/recent`
  - `GET /chat/since`

## Leaderboard
- `GET /leaderboard`
- `GET /leaderboard/me`
- `GET /leaderboard/badges/me`
- `GET /leaderboard/overview/me`
- `GET /leaderboard/progression/me`
- `GET /leaderboard/ranks`

## Admin/ops surface
- Admin summary, users, live, reports, and system routes remain intact.
- Admin mutation routes remain intact, including suspend and report clear flows.
- Admin trips and admin test/diagnostic routes remain intact.

## Compatibility notes
- SQLite fallback remains supported.
- Postgres and SQLite continue to use the same `?`-style call sites via the compatibility layer.
- New cleanup/account-control changes are additive and preserve route availability and payload keys used by the frontend.
