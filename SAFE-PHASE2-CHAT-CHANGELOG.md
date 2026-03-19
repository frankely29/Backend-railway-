# SAFE PHASE 2 Chat Changelog

## Summary

SAFE PHASE 2 adds optional, additive SSE delivery for chat surfaces only.

Preserved unchanged:
- legacy public chat send/history routes
- current room chat send/history routes
- legacy-compatible DM routes
- current private/DM routes
- polling-oriented summary/list routes
- presence transport and presence polling behavior
- hotspot/timeline logic
- Save/pickup behavior
- admin/account-control behavior except continued shared auth enforcement

## New SSE routes

### Public chat live routes
- `GET /chat/public/events`
  - alias stream for the existing `global` public room
- `GET /chat/rooms/{room}/events`
  - room-scoped SSE stream for compact new-message events

### DM summary live route
- `GET /chat/private/events`
  - per-authenticated-user SSE stream for compact DM thread/unread updates

### Lightweight diagnostics
- `GET /chat/live/status`
  - reports current SSE heartbeat/history config and active in-process channel counts

## Existing routes intentionally left unchanged

### Public/global
- `POST /chat/send`
- `GET /chat/recent`
- `GET /chat/since`
- `GET /chat/rooms/{room}`
- `GET /chat/rooms/{room}/summary`
- `GET /chat/public/summary`
- `POST /chat/rooms/{room}`
- `POST /chat/rooms/{room}/voice`

### DM/private
- `GET /chat/dm/{other_user_id}`
- `GET /chat/dm/{other_user_id}/summary`
- `POST /chat/dm/{other_user_id}`
- `POST /chat/dm/{other_user_id}/voice`
- `GET /chat/private/threads`
- `GET /chat/private/summary`
- `GET /chat/private/{other_user_id}`
- `POST /chat/private/{other_user_id}`
- `POST /chat/private/{other_user_id}/voice`

## Event delivery model

### Internal model
- lightweight in-process broker only
- no full realtime rewrite
- no per-connection DB polling loops
- bounded in-memory replay history
- bounded subscriber queue size
- heartbeats only when idle

### Public event shape
Event name:
- `chat.message`

Compact payload shape:
- `type` = `chat.message`
- `event_id` (in payload for convenience/dedupe)
- `scope` = `public_room`
- `room`
- `message_id`
- `sender_user_id`
- `display_name`
- `text`
- `message_type`
- `created_at`
- optional voice metadata:
  - `audio_url`
  - `audio_duration_ms`
  - `audio_mime_type`

### DM event shapes
Primary event names:
- `dm.thread_updated`
- `dm.unread_changed`
- control/recovery event: `reset`

Compact payload fields used for DM summary delivery:
- `type`
- `event_id` (when broker-published)
- `scope` = `dm_summary`
- `user_id`
- `other_user_id`
- `thread_key`
- `unread_count`
- `total_unread_count`
- when message-backed:
  - `message_id`
  - `sender_user_id`
  - `recipient_user_id`
  - `text_preview`
  - `message_type`
  - `created_at`
  - optional voice metadata

## Auth / moderation semantics

SSE uses the same authenticated user gate as polling routes:
- signed-in user required
- disabled users blocked
- suspended users blocked
- trial/admin gating unchanged
- DM target validation unchanged on send/list paths
- no presence SSE added in this phase

## Reconnect behavior

### Last-Event-ID
- Supported on:
  - `GET /chat/public/events`
  - `GET /chat/rooms/{room}/events`
  - `GET /chat/private/events`
- Support is bounded to the in-process replay buffer.
- If the requested `Last-Event-ID` is older than the retained replay window, the stream emits a compact `reset` event so the client can reconcile with existing polling summary/history routes.

### Fallback assumptions remain
- SSE is optional.
- Polling clients continue to work unchanged.
- If a client misses events, it should reconcile with:
  - `/chat/public/summary`
  - `/chat/rooms/{room}/summary`
  - `/chat/dm/{other_user_id}/summary`
  - `/chat/private/summary`
  - existing history/list endpoints

## Query/index notes

No additional DB indexes were added in this pass.

Reason:
- the backend already had indexes covering public room recent/since access, DM created-at access, and unread-by-recipient lookups
- the new SSE fanout uses in-process publish/notify and does not add per-connection database polling
- per-message unread calculations rely on the existing `private_chat_messages(recipient_user_id, sender_user_id, read_at)` support

## Presence / hotspot safety

Explicitly not changed in SAFE PHASE 2:
- no full presence SSE
- no citywide presence streaming
- no WebSocket rewrite
- no presence write-path rewrite
- no hotspot/timeline changes
- no Save/pickup changes
