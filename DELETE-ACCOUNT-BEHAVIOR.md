# DELETE ACCOUNT BEHAVIOR

## Route
- `POST /me/delete_account`
- Requires authenticated, non-blocked user access.

## Runtime policy implemented now
This endpoint performs runtime cleanup rather than silently deleting only the user row.

## Deleted DB data
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

## Anonymized DB data
- `recommendation_outcomes.user_id` is set to `NULL` when present.

## Deleted filesystem/runtime artifacts
- avatar thumbnails under `DATA_DIR/avatar_thumbs/{user_id}`
- owned chat audio files referenced by deleted public/private chat rows

## Presence side effects
Before deleting rows, the backend writes a `presence_runtime_state` tombstone with reason `account_deleted` so delta clients can remove the user deterministically.

## Result shape
The route returns a cleanup summary containing:
- `deleted` counts by table
- `anonymized` counts by table
- `avatar_assets_deleted`
- `chat_audio_deleted`

## Current non-goals
- This pass does not rewrite historical product policy beyond runtime correctness.
- Non-user-keyed aggregate state such as global refresh markers is not touched.
