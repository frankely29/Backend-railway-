# Admin Portal Synthetic Load Test

## What this adds

This backend-only harness adds an **admin-auth protected synthetic load test** under `/admin/tests/load/*`.

### Changelog note
- Added admin-only synthetic load-test control routes for capabilities, start, status, stop, and last result.
- Added a singleton background runner that simulates 100, 300, 500, or 1000 synthetic drivers entirely server-side.
- Added structured pass/fail threshold checks, progress snapshots, and paste-friendly debug payloads for Admin Portal diagnostics.
- Kept the simulation isolated from live presence/chat/leaderboard tables so real users do not see synthetic activity.

## What the synthetic test does

The load test simulates realistic NYC/TLC-style driver behavior using deterministic seeded synthetic drivers and several geographic clusters:
- Midtown / Lower Manhattan
- Downtown Brooklyn
- Long Island City / Astoria
- JFK area
- Upper Manhattan / Bronx edge

Depending on the selected mode/config, the harness can simulate:
- synthetic presence writes
- synthetic viewport reads
- synthetic presence summary reads
- synthetic delta reads
- optional pickup overlay reads
- optional leaderboard reads
- optional chat-lite send/read activity

The browser is only used to:
- start a run
- poll status/progress
- stop a run
- display/copy results

The heavy work runs in a single controlled backend background thread.

## What it does **not** prove

This synthetic test is intentionally safe and isolated, so it does **not** prove:
- exact production latency under real mobile traffic
- real SSE fan-out limits
- database contention from real public chat writes
- behavior of real user-facing presence visibility under true concurrent clients
- autoscaling capacity

It is a diagnostic harness, not a replacement for full production load testing.

## Safety and isolation

The harness is safe by default because it:
- requires admin auth
- allows only one active run at a time
- runs only when explicitly started
- supports cooperative stop
- stores synthetic world state in memory only
- does **not** insert fake drivers into live presence tables
- does **not** write synthetic chat into public user feeds
- does **not** contaminate leaderboard counts shown to users

## Modes

### `map_core` (default)
Enabled by default:
- presence writes
- viewport reads
- summary reads
- delta reads

Disabled by default:
- pickup overlay reads
- leaderboard reads
- chat-lite

### `map_plus_chat`
Same as `map_core`, plus lightweight synthetic chat activity.

### `custom`
Allows the admin UI to explicitly toggle supported operation families.

## Pass / fail rules

Thresholds are defined centrally in the load-test service by preset.

A run is **PASS** only when every enabled hard check passes.
A run is **FAIL** if any enabled hard check fails.
A run can also end as:
- `stopped` when an admin requests cooperative stop
- `error` when the runner crashes internally

Checks include:
- overall error rate
- p95 latency for enabled operations
- RSS memory growth
- optional chat-lite latency checks when chat-lite is enabled

Each result includes:
- short human-readable summary
- explicit reasons for pass/fail
- structured checks with measured values and thresholds
- periodic snapshots
- top errors
- slowest-operation samples
- resource usage deltas

## Result payload shape

All responses stay within the existing admin diagnostics envelope:
- `ok`
- `test_name`
- `checked_at`
- `summary`
- `details`

The richer load-test payload lives inside `details` so the Admin Portal can render it and copy the debug object.

## Usage guidance

- Start with 100 drivers in `map_core` mode.
- Use 300 or 500 for routine validation before releases.
- Use 1000 only when you specifically want a higher synthetic stress scenario.
- If a run fails, copy the debug payload and compare the failed checks, snapshots, and slowest-operation samples.
- Stop an active run before starting another one.
