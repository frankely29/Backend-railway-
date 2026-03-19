# Account Control Semantics

## Canonical blocked-user rule
A user is considered **blocked** when either of these flags is active:
- `is_disabled = true/1`
- `is_suspended = true/1`

Runtime enforcement now checks both flags through one canonical helper in `core.py`.

## Meaning of each flag
- **Disabled**: long-lived/admin account disable. The account cannot log in or use authenticated routes.
- **Suspended**: temporary/admin suspension. The account cannot log in or use authenticated routes.

## Backward compatibility
- Legacy admin disable behavior is preserved through `POST /admin/users/disable`.
- Newer admin suspension behavior is preserved through `POST /admin-mutations/users/{user_id}/set-suspended`.
- Both actions now agree with auth enforcement because authenticated requests and login both use the same blocked-user rule.

## Token behavior
The backend remains stateless for JWTs, but blocked users are denied on every authenticated request because the token is re-checked against the current DB row. This prevents stale tokens from continuing to participate after a disable/suspend action.

## Product-facing behavior
- Blocked users cannot authenticate successfully.
- Blocked users cannot continue using `/me`, chat, presence, pickup, leaderboard, or admin-protected authenticated flows.
- Blocked users are hidden from driver profile lookup and DM target resolution.
- When a user is disabled or suspended through admin controls, their live presence row is removed to stop map participation immediately.
