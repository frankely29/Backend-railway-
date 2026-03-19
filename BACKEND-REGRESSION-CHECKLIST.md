# Backend Regression Checklist

Use this checklist before merging backend changes.

## Startup/runtime
- [ ] App starts with Railway-style env vars.
- [ ] SQLite fallback starts locally.
- [ ] `/status` responds successfully.

## Hotspot artifacts
- [ ] `/timeline` responds successfully.
- [ ] `/frame/{idx}` responds successfully.
- [ ] day-tendency endpoints still respond successfully.
- [ ] 20-minute-bin frame/timeline behavior remains unchanged.

## Auth/profile
- [ ] signup/login/me work.
- [ ] `/me/update`, `/me/change_password`, `/me/delete_account` work.
- [ ] `/drivers/{user_id}/profile` still returns the expected frontend data.

## Presence
- [ ] `/presence/update` works.
- [ ] `/presence/all` works.
- [ ] `/presence/summary` works.
- [ ] ghost-mode users remain hidden from live rendering but counted in summary.

## Events/pickups
- [ ] police event routes work.
- [ ] pickup record route still enforces guard rails.
- [ ] pickup recent overlay still returns recent items plus hotspot attachments.

## Chat
- [ ] public room routes work.
- [ ] DM routes work.
- [ ] legacy `/chat/send`, `/chat/recent`, `/chat/since` compatibility still works.
- [ ] voice-note/audio fetch routes still work.

## Leaderboard
- [ ] leaderboard list route works.
- [ ] overview/progression/my-rank routes work.
- [ ] presence/pickup tracking still updates leaderboard state.

## Admin
- [ ] admin summary/users/live/reports/system routes work.
- [ ] admin mutation routes work.
- [ ] admin trip routes work.
- [ ] admin test/diagnostic routes work.

## Account control and cleanup
- [ ] disabled and suspended semantics are deterministic and documented.
- [ ] stale tokens are blocked after disable/suspend.
- [ ] delete-account cleanup removes/anonymizes all active-runtime user-linked data.

## Performance
- [ ] benchmark hot endpoints (`/timeline`, `/frame/{idx}`, `/presence/all`, `/presence/summary`, `/events/pickups/recent`, chat fetch routes, leaderboard overview) before/after significant changes.
- [ ] verify no cache invalidation regressions in timeline/frame/presence/pickup paths.
