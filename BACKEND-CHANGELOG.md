# BACKEND CHANGELOG

## Current pass: clean Phase 1 + safe Phase 2

### Database/runtime spine
- Made `psycopg2` optional for SQLite-only imports and startup.
- Added a clear Postgres-only runtime error when Postgres mode is requested without `psycopg2`.
- Kept helper signatures `_db`, `_db_exec`, `_db_query_one`, `_db_query_all`, and `_sql` intact.
- Kept Postgres pooling on the shared `ThreadedConnectionPool` path.

### Account control
- Preserved the canonical `_user_block_state` / `_enforce_user_not_blocked` helpers as the single source of disabled/suspended truth.
- Extended blocked-user enforcement to the new SSE auth path and to chat/profile visibility paths.

### Presence
- Kept `/presence/all` for backward compatibility.
- Preserved and documented `/presence/viewport`, `/presence/delta`, and `/presence/summary`.
- Kept delta cursors in milliseconds via `presence_runtime_state.changed_at_ms`.
- Preserved ghost-mode hiding semantics and deterministic removal reasons.

### Delete-account cleanup
- Expanded runtime cleanup to include `presence_runtime_state` along with chat, pickup, leaderboard, and generated assets.
- Deduplicated filesystem chat-audio cleanup accounting.

### Safe Phase 2 live chat
- Added `/chat/live/capabilities` as the frontend-safe entry point for live-chat discovery.
- Added short-lived signed `live_token` URLs for EventSource usage.
- Updated public/private SSE endpoints to accept either Bearer auth or short-lived live tokens.
- Kept polling routes unchanged as the supported fallback.
- Preserved the existing in-process bounded SSE broker and replay behavior.

### Regression coverage
- Added focused tests for:
  - SQLite import/startup without `psycopg2`
  - Postgres-mode clear failure without `psycopg2`
  - Postgres pool wrapper path
  - live capabilities route
  - live-token SSE auth contract
