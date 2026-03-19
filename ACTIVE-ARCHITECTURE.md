# Active Architecture

## Canonical active runtime
Treat these files as the primary live backend runtime:
- `main.py`
- `core.py`
- `chat.py`
- `leaderboard_routes.py`
- `leaderboard_service.py`
- `leaderboard_db.py`
- `leaderboard_tracker.py`
- `pickup_recording_feature.py`
- `admin_routes.py`
- `admin_mutation_routes.py`
- `admin_trips_routes.py`
- `admin_test_routes.py`
- `avatar_assets.py`
- `build_hotspot.py`
- `build_day_tendency.py`
- `hotspot_scoring.py`
- `micro_hotspot_scoring.py`
- `hotspot_experiments.py`
- `account_runtime.py` (new helper for active account cleanup semantics)

## What each active file owns
- `main.py`: FastAPI app assembly, startup schema/bootstrap, hotspot timeline/frame endpoints, auth/profile routes, presence endpoints, legacy chat compatibility routes, pickup overlay, and admin-disable/reset routes.
- `core.py`: DB backend selection, SQL placeholder translation, pooled Postgres connection access, SQLite fallback, JWT verification, password hashing, and canonical account-block enforcement.
- `chat.py`: current chat room/DM APIs, voice-note persistence/retention, shared chat validation, and legacy-global chat helper logic reused by `main.py`.
- `leaderboard_*`: live stats schema, progression, ranking, badges, and presence/pickup tracking integration.
- `pickup_recording_feature.py`: guarded pickup-recording subsystem and admin void flow.
- `admin_*`: operations/admin routes and services.
- `account_runtime.py`: comprehensive active-runtime delete-account cleanup and anonymization rules.

## Shared infrastructure dependencies
- Presence is shared infrastructure for live rendering, online counts, leaderboard tracking, and pickup guard movement/session state.
- Chat is dual-generation: the router in `chat.py` is current, while `main.py` still exposes legacy compatibility routes.
- The scheduler/mailer code exists but is not started at app startup.

## Legacy-but-still-compatible layer
These files reflect the older architecture generation and should be kept compatible, but not treated as the first target for runtime improvements:
- `db.py`
- `models.py`
- `users.py`
- `presence.py`
- `events.py`
- `security.py`

## Editing guidance
1. Start with the active runtime files above.
2. Preserve all route contracts and SQLite fallback behavior.
3. Prefer helper extraction/addition over large rewrites of `main.py`.
4. Keep legacy compatibility routes until the frontend/tooling no longer depends on them.
