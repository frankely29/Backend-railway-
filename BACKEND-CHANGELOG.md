# Backend Changelog

## 2026-03-19

### Performance and scalability
- Reworked `core.py` so SQLite keeps serialized access while Postgres uses a thread-safe connection pool instead of opening a brand-new connection for every single query.
- Preserved the existing helper signatures and `?` placeholder translation used across the codebase.

### Correctness and consistency
- Added one canonical blocked-user rule in `core.py` so login and authenticated routes agree on disabled vs suspended behavior.
- Updated driver-profile and DM-target checks to respect the same blocked-user rule.
- Made admin disable flow clear presence immediately, matching suspension behavior.

### Delete-account safety
- Added `account_runtime.py` to perform comprehensive active-runtime cleanup and recommendation anonymization.
- Extended `/me/delete_account` to return cleanup details while preserving `ok: true`.

### Chat consolidation
- Reused `chat.py` helper logic for legacy global chat send/recent/since routes to reduce drift between the legacy and current chat paths.

### Regression coverage and docs
- Added targeted regression tests for auth, presence, chat, leaderboard, admin routes, pickup guard evaluation, and delete-account cleanup.
- Added active-runtime/API/account-control/delete-account documentation.
