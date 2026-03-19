# ACCOUNT CONTROL SEMANTICS

## Canonical helpers
The backend now centralizes account blocking state through:
- `_user_block_state(row)`
- `_enforce_user_not_blocked(row)`

These helpers treat `is_disabled` and `is_suspended` as the canonical blocked states.

## Deterministic meanings
### Disabled
- hard block for authentication and downstream use.
- login returns `403`.
- authenticated routes using `require_user` reject the user.
- profile and DM target lookups treat the user as unavailable.
- visible map presence is removed.

### Suspended
- same request-time blocking behavior as disabled.
- visible map presence is removed immediately when the admin mutation is applied.

### Ghost mode
- not a login/auth block.
- not a DM/public-chat/authentication block by itself.
- only affects map visibility and presence-delta removal behavior.

## Where these semantics are enforced
- Bearer auth / `require_user`
- login flow before token issuance
- driver profile lookup
- DM target validation
- SSE connection auth for public/private live streams
- presence visibility derivation

## Visibility guarantees
- blocked users do not receive fresh authenticated access.
- blocked users do not remain visible on the public presence map.
- blocked users are filtered out of public-chat history reads.
- blocked users are filtered out of DM thread directory payloads and cannot be selected as new DM targets.

## Admin downstream behavior
- suspend: updates `users.is_suspended`, deletes current presence row, writes a `presence_runtime_state` removal reason of `suspended`.
- disable: updates `users.is_disabled`, deletes current presence row, writes a `presence_runtime_state` removal reason of `disabled`.
- unsuspend / enable: recomputes visibility from ghost-mode + block state rather than blindly re-showing the user.
