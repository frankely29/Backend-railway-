# BACKEND CHANGELOG

## Current pass: Phase 1 density + trip-quality shadow metrics (backend)

### Phase 1 shadow metric inputs
- Added `zone_geometry_metrics.py` to compute Taxi Zone area (square miles) from Taxi Zone geometry (`taxi_zones.geojson`) without introducing heavy GIS dependencies.
- Updated `build_hotspot.py` to register temporary DuckDB table `zone_geometry_metrics` for build-time joins and to emit new shadow fields for:
  - zone area and area-normalized pickup density (now/next),
  - 20+ minute trip share,
  - same-zone dropoff share + retention penalty normalization.
- Updated `zone_earnings_engine.py` to output the new raw + normalized density/trip-quality metrics per zone x day-of-week x 20-minute bin.
- Visible scores/colors and active profile formulas remain unchanged (shadow data collection only in this phase).

## Current pass: Phase 12 final production hardening / cleanup (backend)

### Phase 12 manifest finalization
- Finalized `scoring_shadow_manifest.json` output in `build_hotspot.py` to mark Team Joseo rollout as final-live (`engine_release: team-joseo-score-v2-final-live`) while preserving existing emitted shadow fields.
- Manifest now explicitly declares all visible live profiles (`citywide_v2`, `manhattan_v2`, `bronx_wash_heights_v2`, `queens_v2`, `brooklyn_v2`, `staten_island_v2`) and adds production truth metadata for base-color source, community caution source, and unchanged presence timing.
- No score formulas, feature-value calculations, mode scope precedence, API routes, polling, or presence logic were changed.

## Current pass: Phase 9 Staten Island visible cutover support (backend)

### Phase 9 hotspot shadow output
- Activated the `staten_island_v2` profile weights in `zone_mode_profiles.py` for sparse-market stability, pay quality, and downstream-value emphasis.
- Extended `zone_earnings_engine.py` to emit Staten Island v2 shadow score/rating/bucket/color/confidence using the same normalized HVFHV component pipeline as citywide/Manhattan/Bronx-Wash Heights/Queens/Brooklyn.
- Updated `build_hotspot.py` to write Staten Island v2 shadow fields per frame feature while preserving legacy plus all previously active borough/citywide shadow fields.
- Updated `scoring_shadow_manifest.json` output to list all active shadow profiles: `citywide_v2`, `manhattan_v2`, `bronx_wash_heights_v2`, `queens_v2`, `brooklyn_v2`, and `staten_island_v2`.
- Staten Island visible mode now uses the Team Joseo Staten Island score when available (frontend/runtime cutover), while citywide/Manhattan/Bronx-Wash Heights/Queens/Brooklyn cutovers remain active in their scopes.
- No API route, presence, or polling behavior was changed.

## Current pass: Phase 8 Brooklyn visible cutover support (backend)

### Phase 8 hotspot shadow output
- Activated the `brooklyn_v2` profile weights in `zone_mode_profiles.py` for Brooklyn trap-avoidance, downstream value, and pay-efficiency emphasis.
- Extended `zone_earnings_engine.py` to emit Brooklyn v2 shadow score/rating/bucket/color/confidence using the same normalized HVFHV pipeline as citywide/Manhattan/Bronx-Wash Heights/Queens.
- Updated `build_hotspot.py` to write Brooklyn v2 shadow fields per frame feature while preserving legacy, citywide, Manhattan, Bronx/Wash Heights, and Queens fields.
- Updated `scoring_shadow_manifest.json` output to list all active shadow profiles: `citywide_v2`, `manhattan_v2`, `bronx_wash_heights_v2`, `queens_v2`, and `brooklyn_v2`.
- No API route, presence, or polling behavior was changed.

## Current pass: Phase 7 Queens visible cutover support (backend)

### Phase 7 hotspot shadow output
- Activated the `queens_v2` profile weights in `zone_mode_profiles.py` for Queens persistence/downstream/pay-per-mile emphasis with earnings grounding.
- Extended `zone_earnings_engine.py` to emit Queens v2 shadow score/rating/bucket/color/confidence using the same normalized HVFHV component pipeline as citywide/Manhattan/Bronx-Wash Heights.
- Updated `build_hotspot.py` to write Queens v2 shadow fields per frame feature while preserving legacy, citywide, Manhattan, and Bronx/Wash Heights fields.
- Updated `scoring_shadow_manifest.json` output to list all active shadow profiles: `citywide_v2`, `manhattan_v2`, `bronx_wash_heights_v2`, and `queens_v2`.
- No API route, presence, or polling behavior was changed.

## Current pass: Phase 6 Bronx/Wash Heights visible cutover support (backend)

### Phase 6 hotspot shadow output
- Activated the `bronx_wash_heights_v2` profile weights in `zone_mode_profiles.py` for stronger ride-flow + downstream emphasis with earnings-quality grounding.
- Extended `zone_earnings_engine.py` to emit Bronx/Wash Heights v2 shadow score/rating/bucket/color/confidence using the same normalized HVFHV component pipeline as citywide/Manhattan.
- Updated `build_hotspot.py` to write Bronx/Wash Heights v2 shadow fields per frame feature while preserving legacy, citywide, and Manhattan fields.
- Updated `scoring_shadow_manifest.json` output to list all active shadow profiles: `citywide_v2`, `manhattan_v2`, and `bronx_wash_heights_v2`.
- No API route, presence, or polling behavior was changed.

## Current pass: Phase 5 Manhattan visible cutover support (backend)

### Phase 5 hotspot shadow output
- Kept `citywide_v2` intact and activated Manhattan-specific weighting updates in `zone_mode_profiles.py` for `manhattan_v2`.
- Extended `zone_earnings_engine.py` to emit Manhattan v2 shadow score/rating/bucket/color/confidence using the same normalized HVFHV components as citywide (different profile weights only).
- Updated `build_hotspot.py` feature output to include Manhattan v2 shadow fields while retaining all legacy + citywide shadow fields.
- Updated `scoring_shadow_manifest.json` output to list both active shadow profiles: `citywide_v2` and `manhattan_v2`.
- No API route, presence, or polling behavior was changed.

## Current pass: clean Phase 1 + safe Phase 2

### Phase 2 hotspot shadow earnings engine
- Added `zone_mode_profiles.py` with lightweight score profile scaffolding (`citywide_v2` active for this phase; borough profiles pre-created for future phases).
- Added `zone_earnings_engine.py` with a shared HVFHV factual SQL engine that computes backend shadow metrics and a citywide shadow rating/bucket/color output.
- Updated `build_hotspot.py` to keep legacy visible scoring intact while attaching new shadow metrics to frame feature properties.
- Added frame output manifest `scoring_shadow_manifest.json` to document emitted shadow fields/profile/version.
- No API routes, frontend runtime files, or presence real-time behavior were changed.

### Database/runtime spine
- Made `psycopg2` optional for SQLite-only imports and startup.
- Added a clear Postgres-only runtime error when Postgres mode is requested without `psycopg2`.
- Kept helper signatures `_db`, `_db_exec`, `_db_query_one`, `_db_query_all`, and `_sql` intact.
- Kept Postgres pooling on the shared `ThreadedConnectionPool` path.

### Account control
- Preserved the canonical `_user_block_state` / `_enforce_user_not_blocked` helpers as the single source of disabled/suspended truth.
- Extended blocked-user enforcement to the new SSE auth path and to chat/profile visibility paths.

### Presence
- Kept `/presence/all` for backward compatibility.
- Preserved and documented `/presence/viewport`, `/presence/delta`, and `/presence/summary`.
- Kept delta cursors in milliseconds via `presence_runtime_state.changed_at_ms`.
- Preserved ghost-mode hiding semantics and deterministic removal reasons.

### Delete-account cleanup
- Expanded runtime cleanup to include `presence_runtime_state` along with chat, pickup, leaderboard, and generated assets.
- Deduplicated filesystem chat-audio cleanup accounting.

### Safe Phase 2 live chat
- Added `/chat/live/capabilities` as the frontend-safe entry point for live-chat discovery.
- Added short-lived signed `live_token` URLs for EventSource usage.
- Updated public/private SSE endpoints to accept either Bearer auth or short-lived live tokens.
- Kept polling routes unchanged as the supported fallback.
- Preserved the existing in-process bounded SSE broker and replay behavior.

### Regression coverage
- Added focused tests for:
  - SQLite import/startup without `psycopg2`
  - Postgres-mode clear failure without `psycopg2`
  - Postgres pool wrapper path
  - live capabilities route
  - live-token SSE auth contract
