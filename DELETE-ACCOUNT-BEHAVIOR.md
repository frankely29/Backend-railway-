# Delete Account Behavior

`POST /me/delete_account` now performs comprehensive active-runtime cleanup inside one DB transaction, followed by filesystem cleanup.

## Deleted rows
When the corresponding table exists, the backend deletes user-linked rows from:
- `users`
- `presence`
- `chat_messages`
- `private_chat_messages`
- `events`
- `pickup_logs`
- `pickup_guard_state`
- `driver_work_state`
- `driver_daily_stats`
- `leaderboard_badges_current`

## Anonymized rows
To preserve aggregate recommendation analytics without keeping the user identity attached:
- `recommendation_outcomes.user_id` is set to `NULL` for the deleted user.

## Filesystem cleanup
After DB commit, the backend removes:
- stored chat audio files owned by public/private messages deleted for the user
- avatar thumbnail files under `DATA_DIR/avatar_thumbs/{user_id}`

## Intentional non-actions
- `leaderboard_badges_refresh_state` is not user-linked and is left intact.
- hotspot experiment bin tables are aggregate/system tables and are left intact.

## Response shape
The route still returns `ok: true`, and now also returns a nested `cleanup` object with:
- `user_id`
- `deleted` row counts by table
- `anonymized` row counts by table
- `avatar_assets_deleted`
- `chat_audio_deleted`

This addition is backward compatible because the original top-level success flag remains unchanged.
