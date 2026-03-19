# BACKEND REGRESSION CHECKLIST

## Startup / database
- [x] SQLite-only local/test startup works when `DATABASE_URL` is unset and `psycopg2` is unavailable.
- [x] Postgres mode fails clearly when requested without `psycopg2`.
- [x] Postgres helper path still uses a threaded connection pool wrapper.

## Auth / account control
- [x] signup works
- [x] login works
- [x] `/me` works
- [x] disabled and suspended behavior is consistent across login and authenticated routes
- [x] blocked users are hidden from driver profile lookups

## Presence
- [x] `/presence/update` works
- [x] `/presence/all` still works for backward compatibility
- [x] `/presence/viewport` works
- [x] `/presence/delta` works with `updated_since_ms`
- [x] ghost-mode hiding still works
- [x] `/presence/summary` works
- [x] admin disable/suspend removes live presence deterministically

## Police / pickup / leaderboard
- [x] police report create/read still works
- [x] pickup recording / guard logic still works
- [x] leaderboard overview/progression/ranks still work

## Chat polling paths
- [x] public chat send/list still works
- [x] DM send/list still works
- [x] polling summary routes still work when SSE is ignored
- [x] public chat reads do not expose disabled/suspended senders
- [x] DM target validation blocks disabled/suspended targets

## Safe Phase 2 live chat
- [x] `/chat/live/capabilities` works with Bearer auth
- [x] public SSE rejects missing/invalid auth
- [x] private SSE rejects missing/invalid auth
- [x] public SSE works with short-lived live token auth
- [x] private SSE works with short-lived live token auth
- [x] public message publish causes a public live event
- [x] DM message publish causes a private summary live event

## Delete-account cleanup
- [x] delete-account cleanup removes presence/runtime/chat/pickup/leaderboard/user rows
- [x] delete-account cleanup anonymizes recommendation outcomes
- [x] delete-account cleanup removes avatar/chat-audio artifacts

## Compatibility
- [x] no route regression for current frontend compatibility surfaces verified by regression tests
