# SAFE PHASE 2 CHAT RUNTIME

## Scope
This runtime adds live delivery for chat only. It does **not** stream city-wide presence and does **not** replace polling.

## Browser-safe auth model
Plain browser `EventSource` cannot attach custom Bearer headers reliably for this use case, so the runtime now supports a two-step flow:
1. frontend calls `GET /chat/live/capabilities` with the normal Bearer token,
2. backend returns signed short-lived SSE URLs containing a `live_token` query param,
3. frontend uses those URLs with `EventSource`.

## Live token format
- signed with the existing server secret through the same HMAC/JWT-style signing path
- distinct payload type: `typ = "live"`
- scoped by:
  - `uid`
  - `scope` (`public` or `private`)
- short TTL via `LIVE_TOKEN_TTL_SECONDS` (default 60 seconds, clamped to 30-90 seconds)
- validated on connection before the stream starts
- rejected with `401` for invalid/expired signatures and `403` for scope mismatch

The long-lived login token is intentionally not placed directly in EventSource query strings.

## Live endpoints
### `GET /chat/public/events`
- public/global compact message stream
- accepts Bearer auth or `live_token`
- emits compact `chat.message` events

### `GET /chat/private/events`
- per-user DM summary stream
- accepts Bearer auth or `live_token`
- emits compact `dm.thread_updated` / `dm.unread_changed` events

### `GET /chat/rooms/{room}/events`
- optional additive room-specific stream
- same auth model as public SSE

## Event broker behavior
- in-process broker only
- bounded replay history
- bounded subscriber queues
- oldest queued item is dropped on queue overflow to protect the process
- subscribers are removed in a `finally` block on disconnect
- heartbeats are emitted as SSE comments

## Replay / recovery
- `Last-Event-ID` is supported against the bounded in-memory history
- if the requested event is too old, the broker emits a `reset` event
- clients should refetch `/chat/live/capabilities` when a signed URL expires or before reconnecting after a longer disconnect
- clients should reconcile with polling summary routes:
  - `/chat/public/summary`
  - `/chat/private/summary`
  - `/chat/dm/{other_user_id}/summary`

## Block-state enforcement
- connection auth checks disabled/suspended state before the stream starts
- heartbeat intervals also re-check the user row and terminate the stream if the account becomes blocked

## Non-goals preserved in this pass
- no WebSocket rewrite
- no mandatory SSE dependency for clients
- no presence SSE
- no removal of existing polling/list routes
