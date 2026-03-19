# SAFE PHASE 2 Chat Regression Checklist

Use this checklist for backend verification after deploying SAFE PHASE 2.

## Public chat compatibility
- [x] Public chat legacy send still works: `POST /chat/send`
- [x] Public chat legacy history still works: `GET /chat/recent`
- [x] Public chat incremental history still works: `GET /chat/since`
- [x] Current room send still works: `POST /chat/rooms/{room}`
- [x] Current room history still works: `GET /chat/rooms/{room}`
- [x] Public summary still works: `GET /chat/public/summary`
- [x] Room summary still works: `GET /chat/rooms/{room}/summary`
- [x] `GET /chat/live/capabilities` requires Bearer auth and returns signed short-lived SSE URLs
- [x] Public SSE connect works for valid authenticated users: `GET /chat/public/events`
- [x] Room SSE connect works for valid authenticated users: `GET /chat/rooms/{room}/events`
- [x] New public message publishes compact live event with stable identifiers
- [x] SSE failure/reconnect fallback still allows polling reconciliation

## DM/private compatibility
- [x] DM legacy send still works: `POST /chat/dm/{other_user_id}`
- [x] DM legacy history still works: `GET /chat/dm/{other_user_id}`
- [x] DM thread summary still works: `GET /chat/dm/{other_user_id}/summary`
- [x] Private send still works: `POST /chat/private/{other_user_id}`
- [x] Private history still works: `GET /chat/private/{other_user_id}`
- [x] Private inbox summary still works: `GET /chat/private/summary`
- [x] Private threads list still works: `GET /chat/private/threads`
- [x] DM SSE connect works for valid authenticated users: `GET /chat/private/events`
- [x] New DM message publishes compact summary/live update with stable identifiers
- [x] Read-path unread updates still reconcile correctly after thread open/read

## Auth / moderation / account safety
- [x] Auth still enforced on polling routes
- [x] Auth still enforced on SSE routes
- [x] Invalid/expired signed SSE tickets fail cleanly
- [x] Disabled users still cannot use chat after auth checks
- [x] Suspended users still cannot use chat after auth checks
- [x] DM target validity still rejects blocked/deleted targets
- [x] Delete-account cleanup still removes public chat rows
- [x] Delete-account cleanup still removes private chat rows and chat audio files
- [x] No new SSE authorization loophole bypasses shared `require_user`

## Platform safety
- [x] SSE remains optional and additive
- [x] Polling clients still work if SSE is ignored entirely
- [x] No presence path regression introduced intentionally in this phase
- [x] No hotspot/timeline regression introduced intentionally in this phase
- [x] No Save/pickup behavior regression introduced intentionally in this phase
- [x] No admin route regression introduced intentionally in this phase
- [x] No full presence streaming added in this phase
- [x] No WebSocket architecture rewrite added in this phase

## Diagnostics / recovery
- [x] `GET /chat/live/status` reports active SSE configuration/channel counts
- [x] Signed SSE ticket TTL/reconnect behavior is documented
- [x] Last-Event-ID replay is bounded and documented
- [x] Replay miss falls back to polling via a compact `reset` event and existing summary/history endpoints
