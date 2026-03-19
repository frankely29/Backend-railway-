# Regression Checklist

## Auth
- [ ] Signup returns token and account metadata.
- [ ] Login still rejects disabled / suspended users.
- [ ] `GET /me` still returns current user metadata.

## Presence
- [ ] `POST /presence/update` still accepts normal foreground writes.
- [ ] `GET /presence/all` still returns legacy-compatible snapshots.
- [ ] `GET /presence/viewport` returns viewport-scoped marker payloads.
- [ ] `GET /presence/delta` returns only changed markers plus tombstones.
- [ ] Ghost users stay hidden from map-visible payloads.
- [ ] Ghost users still count toward summary totals when presence is fresh.
- [ ] Disable / suspend actions remove active map visibility promptly.

## Chat / Public chat
- [ ] Legacy `/chat/send`, `/chat/recent`, and `/chat/since` routes still work.
- [ ] `/chat/public/summary` and `/chat/rooms/{room}/summary` return lightweight unread / latest-message metadata.
- [ ] Room history ordering remains stable.

## DM / Private messaging
- [ ] `/chat/dm/{other_user_id}` still lists compatibility messages.
- [ ] `/chat/private/{other_user_id}` still lists full thread history.
- [ ] `/chat/private/summary` returns minimal inbox summary / unread counts.
- [ ] `/chat/dm/{other_user_id}/summary` returns thread-level latest metadata.
- [ ] Disabled / suspended DM targets remain inaccessible.

## Ghost behavior
- [ ] Ghost toggle emits a removal to optimized presence consumers.
- [ ] Ghost toggle off allows optimized presence consumers to rediscover the driver without requiring a legacy route switch.

## Hotspot / Timeline
- [ ] `GET /timeline` still serves timeline artifacts.
- [ ] `GET /frame/{idx}` still serves frame artifacts.
- [ ] `GET /events/pickups/recent` still serves pickup overlays.

## Save / Pickup recording
- [ ] `POST /events/pickup` still records pickup/save data under current guard semantics.
- [ ] Pickup admin routes remain available.

## Delete account
- [ ] Delete-account cleanup removes presence, chat, pickup, leaderboard, and storage artifacts per current policy.
- [ ] Recommendation outcomes remain anonymized rather than hard-deleted.

## Admin disable / suspend
- [ ] Disable route still works for legacy admin clients.
- [ ] Suspend route still works for current admin clients.
- [ ] Presence effects are immediate for optimized presence consumers.

## Diagnostics
- [ ] `scripts/benchmark_hot_endpoints.py` runs against a local or deployed backend with a valid token.
