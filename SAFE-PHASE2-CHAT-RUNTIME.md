# SAFE PHASE 2 Chat Runtime Audit (Pre-change)

This document captures the backend's observed chat/runtime behavior before SAFE PHASE 2 live-delivery changes.

## Scope reviewed

Reviewed files:
- `main.py`
- `chat.py`
- `core.py`
- `account_runtime.py`
- admin/auth-related helpers that affect blocked/suspended behavior

## Current auth/runtime baseline

### Shared auth gate
- Most chat routes use `require_user` from `core.py`.
- `require_user` authenticates the bearer token, loads the user row, rejects missing users, rejects disabled users, rejects suspended users, and enforces trial/admin access rules.
- As a result, blocked users cannot call the current chat send/list routes after authentication.

### DM target validity
- DM routes call `_ensure_dm_target_exists(other_user_id)`.
- That helper only accepts an existing target user who is not disabled and not suspended.
- If the target is blocked, DM routes return `404 User not found`.
- DM routes also reject self-DM with `400 Cannot message yourself`.

## Current public chat routes

### Legacy global send route
- `POST /chat/send`
- Implemented in `main.py`.
- Input shape: `{ "message": string }`
- Persists into `chat_messages` in room `global` through `send_legacy_global_text_message(...)`.
- Returns compact legacy shape:
  - `ok`
  - `id`
  - `created_at`
  - `display_name`

### Legacy global history/list routes
- `GET /chat/recent?limit=50`
- `GET /chat/since?after_id=0&limit=50`
- Implemented in `main.py`.
- Both read from `chat_messages` filtered to `room='global'`.
- Response shape:
  - `{ "ok": true, "items": [...] }`
- Items are legacy rows with fields like:
  - `id`
  - `user_id`
  - `display_name`
  - `message`
  - `created_at`

### Current room/public routes
- `GET /chat/rooms/{room}`
- `GET /chat/rooms/{room}/summary`
- `GET /chat/public/summary`
- `POST /chat/rooms/{room}`
- `POST /chat/rooms/{room}/voice`
- Implemented in `chat.py`.
- `global` is the effective public room used by the legacy global endpoints.
- Room list route supports:
  - `after` as either numeric id or ISO timestamp
  - `limit`
- Summary routes already exist and are polling-oriented lightweight endpoints.

### Current public message payload shape
List/send responses serialize public room messages roughly as:
- `id`
- `room`
- `user_id`
- `display_name`
- `text`
- `message_type`
- `created_at` (ISO)
- optional voice fields:
  - `audio_url`
  - `audio_duration_ms`
  - `audio_mime_type`

### Current public chat assumptions
- Polling clients can use `GET /chat/recent`, `GET /chat/since`, `GET /chat/rooms/{room}`, and room/global summary routes.
- Legacy and current public routes overlap on the same `chat_messages` table and the same `global` room data.
- No SSE/live stream currently exists.

## Current DM/private chat routes

### Legacy-compatible DM routes
- `GET /chat/dm/{other_user_id}`
- `GET /chat/dm/{other_user_id}/summary`
- `POST /chat/dm/{other_user_id}`
- `POST /chat/dm/{other_user_id}/voice`

These are compatibility routes that return legacy-friendly payloads.

#### DM list behavior
- Returns `{ "room": "dm:low:high", "messages": [...] }`
- Message items include both current DM fields and legacy aliases:
  - `id`
  - `sender_user_id`
  - `recipient_user_id`
  - `text`
  - `message_type`
  - `created_at`
  - optional voice fields
  - legacy aliases:
    - `user_id`
    - `display_name`
    - `sender_display_name`
    - `room`

#### DM send behavior
- Persists into `private_chat_messages`.
- Returns a legacy-enriched message payload with current DM fields plus the aliases above.

### Current private routes
- `GET /chat/private/threads`
- `GET /chat/private/summary`
- `GET /chat/private/{other_user_id}`
- `POST /chat/private/{other_user_id}`
- `POST /chat/private/{other_user_id}/voice`

These are the current structured DM/private endpoints.

#### Private thread/list behavior
- `GET /chat/private/threads` returns thread previews with:
  - `other_user_id`
  - `other_display_name`
  - `display_name`
  - `other_avatar_url`
  - `avatar_url`
  - `last_message_text`
  - `preview_text`
  - `last_message_at`
  - `last_created_at`
  - `last_message_sender_user_id`
  - `unread_count`
- `GET /chat/private/summary` returns:
  - `ok`
  - `thread_count`
  - `total_unread_count`
  - `threads` with compact preview/unread metadata
- `GET /chat/private/{other_user_id}` returns:
  - `other_user_id`
  - `messages`

### Current DM unread/read behavior
- DM thread reads can mark incoming messages read via `mark_read=true`.
- `_mark_private_read(...)` updates `read_at` for inbound messages from the other user.
- `private summary` / `private threads` compute unread counts by counting rows where `recipient_user_id = me` and `read_at IS NULL`.
- `dm/{other_user_id}/summary` already provides a lightweight incremental summary for a single thread.

## Current room/global summary routes already present

### Public summary endpoints already present
- `GET /chat/rooms/{room}/summary`
- `GET /chat/public/summary`

Both currently return:
- `ok`
- `room`
- `latest_message_id`
- `latest_created_at`
- `latest_message`
- `has_newer`
- `unread_count`

These are already suitable for polling-based reconciliation.

### DM summary endpoints already present
- `GET /chat/dm/{other_user_id}/summary`
- `GET /chat/private/summary`

The backend therefore already has polling-friendly compact summary endpoints for both public and DM surfaces.

## Disabled/suspended enforcement in chat paths

### Sender-side enforcement
- All chat endpoints use `require_user`, so disabled/suspended users are blocked before they can send or list chat.
- `POST /auth/login` also blocks disabled/suspended users from logging in.

### Recipient/target-side enforcement
- DM targets are validated with `_ensure_dm_target_exists(...)`.
- Disabled/suspended target users are treated as not found for DM access.

### Public chat visibility
- Public messages store sender display name at insert time in `chat_messages`.
- There is no additional per-read join against current sender block state for existing public messages.
- However, blocked users cannot authenticate and therefore cannot continue sending or reading via protected routes.

## Delete-account effects on chat data

`POST /me/delete_account` calls `delete_account_runtime_data(user_id)`.

That cleanup currently:
- deletes the user's public chat rows from `chat_messages`
- deletes the user's DM rows from `private_chat_messages` where they are sender or recipient
- deletes related chat audio files on disk
- deletes the user row
- also removes presence/events/other runtime rows unrelated to chat

So delete-account currently removes the user's chat history rather than anonymizing it.

## Legacy/current overlap

There is explicit overlap between legacy and current chat paths:
- Legacy global chat routes in `main.py` write/read `chat_messages` with room `global`.
- Current room routes in `chat.py` also write/read `chat_messages` and use the same `global` room.
- Legacy DM room names still exist conceptually (`dm:low:high`) and helper code can generate them for compatibility payloads.
- There is a migration helper in `main.py` that migrates legacy DM rows from `chat_messages` rooms like `dm:%:%` into `private_chat_messages`.
- Current DM writes/reads use `private_chat_messages` directly.

## Polling-oriented assumptions in the current backend

The current backend clearly assumes polling compatibility:
- global incremental polling via `/chat/since`
- room incremental polling via `/chat/rooms/{room}?after=...`
- room/global summary polling via `/chat/rooms/{room}/summary` and `/chat/public/summary`
- DM incremental polling via `/chat/dm/{other_user_id}?after=...` or `/chat/private/{other_user_id}?since_id=...`
- inbox/thread summary polling via `/chat/private/summary` and `/chat/dm/{other_user_id}/summary`

There is no existing push/live delivery system for chat.

## Current frontend dependency likelihood

Based on route overlap and regression coverage, the current frontend likely depends on a mix of:
- legacy global routes:
  - `POST /chat/send`
  - `GET /chat/recent`
  - `GET /chat/since`
- current public routes:
  - `GET /chat/public/summary`
  - `GET /chat/rooms/global`
- current and/or legacy DM routes:
  - `GET/POST /chat/dm/{other_user_id}`
  - `GET /chat/dm/{other_user_id}/summary`
  - `GET /chat/private/summary`
  - `GET /chat/private/{other_user_id}`
  - `POST /chat/private/{other_user_id}`

The regression test file already exercises both legacy and current routes in the same runtime, which strongly suggests frontend/backward-compatibility requirements are intentional.

## Current DB/index posture relevant to SAFE PHASE 2

Already present indexes include:
- `chat_messages(id)`
- `chat_messages(room, id)`
- `chat_messages(created_at)`
- `chat_messages(room, created_at DESC, id DESC)`
- `private_chat_messages(sender_user_id, recipient_user_id, created_at, id)`
- `private_chat_messages(created_at)`
- `private_chat_messages(sender_user_id, created_at DESC, id DESC)`
- `private_chat_messages(recipient_user_id, created_at DESC, id DESC)`
- `private_chat_messages(recipient_user_id, sender_user_id, read_at)`
- `private_chat_messages(legacy_room_message_id)` unique

These already support many of the current recent/since/unread patterns.

## Current live delivery status before SAFE PHASE 2

- No SSE endpoint exists for public chat.
- No SSE endpoint exists for DM inbox/thread activity.
- No Last-Event-ID handling exists for chat.
- No internal chat notifier/pub-sub abstraction exists yet.
- Presence transport is separate and should remain untouched in this pass.
