# BACKEND CHANGELOG

## Current pass: Zone-size-aware pickup hotspot footprint (backend)

### Pickup zone hotspot polygons scale down for small zones
- Shrank pickup-zone hotspot polygons in small NYC taxi zones so a single cluster no longer covers a large fraction of the zone. Previously the shaping buffers in `_shape_hotspot_component` were a fixed absolute size, so the same hotspot footprint that is a small slice of a big zone swallowed most of a small one.
- Added a zone-area-aware `zone_scale` (in `_shape_hotspot_component`, main.py) that scales the `expand`/`smooth` buffers down for small zones and leaves zones at/above the reference area (~1 km²) unchanged, floored at `PICKUP_ZONE_HOTSPOT_SMALL_ZONE_MIN_SCALE` so the shape never collapses.
- Added a hard per-hotspot coverage cap (`PICKUP_ZONE_HOTSPOT_MAX_ZONE_COVERAGE`, default 0.38): if a hotspot still exceeds that fraction of its zone, it is eroded inward in bounded steps until under the cap.
- New tunable constants: `PICKUP_ZONE_HOTSPOT_SCALE_REFERENCE_M2` (1,000,000), `PICKUP_ZONE_HOTSPOT_SMALL_ZONE_MIN_SCALE` (0.5), `PICKUP_ZONE_HOTSPOT_MAX_ZONE_COVERAGE` (0.38). Areas are EPSG:3857 m², matching `zone_proj.area` used elsewhere.
- Offline geometry check (single 135 m density cell): small-zone coverage drops from ~86%/69%/46% (0.08/0.10/0.15 km²) to ~26%/24%/24%; zones ≥1 km² are unchanged; all output polygons stay valid and non-empty. Frontend needs no change — it renders whatever polygon geometry the backend sends.

## Current pass: Outer-borough long-trip hotspot expansion (backend)

### long_trip_hotspot_builder POI list — outer-borough clusters
- Added 20 POIs to `NYC_LONG_TRIP_POIS` forming 8 new long-trip "dollar-flag" clusters: Downtown Flushing, Forest Hills, Atlantic Yards, DUMBO, Crown Heights medical, The Hub / South Bronx, 161 St / Yankee Stadium, and St. George (Staten Island).
- Gives the Bronx (0 to 2) and Staten Island (0 to 1) their first long-trip flags and pulls previously isolated POIs (Yankee Stadium, Lincoln/Kings County/SUNY Downstate hospitals, Atlantic Terminal/Barclays, both 1 Hotel Brooklyn Bridge entries) into genuine 3+ clusters.
- Kept the rule unchanged: `CLUSTER_RADIUS_MI = 0.25` (about a 5-minute walk) and `MIN_MEMBERS_PER_HOTSPOT = 3`. No new categories, maps, or dim schedules.
- Added matching `POI_ADDRESSES` entries for every new POI (outer-borough address style, no ", NY").
- Total flags rebuild from 17 to 25; POI list grows to 190. Requires one `POST /admin/long_trip_hotspots/rebuild` for the change to take effect.

## Current pass: Phase 9 Staten Island v3 live visible cutover (backend)

### Phase 9 rollout-state manifest update
- Promoted `staten_island_v3` to the live visible Staten Island profile in backend manifest metadata.
- Kept `staten_island_v2` available for fallback/debug comparison metadata usage.
- Kept `citywide_v3` as the live visible citywide profile.
- Kept `manhattan_v3` as the live visible Manhattan profile.
- Kept `bronx_wash_heights_v3` as the live visible Bronx/Wash Heights profile.
- Kept `queens_v3` as the live visible Queens profile.
- Kept `brooklyn_v3` as the live visible Brooklyn profile.
- Visible v3 rollout is now complete across citywide and all borough modes.

## Current pass: Phase 7 Queens v3 live visible cutover (backend)

### Phase 7 rollout-state manifest update
- Promoted `queens_v3` to the live visible Queens profile in backend manifest metadata.
- Kept `queens_v2` available for fallback/debug comparison metadata usage.
- Kept `citywide_v3` as the live visible citywide profile.
- Kept `manhattan_v3` as the live visible Manhattan profile.
- Kept `bronx_wash_heights_v3` as the live visible Bronx/Wash Heights profile.
- Kept Brooklyn / Staten Island visible profiles unchanged in Phase 7 (`brooklyn_v2`, `staten_island_v2`).

## Current pass: Phase 6 Bronx/Wash Heights v3 live visible cutover (backend)

### Phase 6 rollout-state manifest update
- Promoted `bronx_wash_heights_v3` to the live visible Bronx/Wash Heights profile in backend manifest metadata.
- Kept `bronx_wash_heights_v2` available for fallback/debug comparison metadata usage.
- Kept `citywide_v3` as the live visible citywide profile.
- Kept `manhattan_v3` as the live visible Manhattan profile.
- Kept Queens / Brooklyn / Staten Island visible profiles unchanged in Phase 6 (`queens_v2`, `brooklyn_v2`, `staten_island_v2`).

## Current pass: Phase 4 borough_v3 shadow candidates (backend)

### Phase 4 shadow candidate rollout
- Added borough_v3 shadow candidates for Manhattan, Bronx/Wash Heights, Queens, Brooklyn, and Staten Island in backend scoring/profile plumbing.
- Visible borough scores remain unchanged in Phase 4 (borough v2 profiles stay live for visible outputs).
- `citywide_v3` remains the live visible citywide score.

## Current pass: Phase 3 citywide_v3 live visible citywide profile (backend)

### Phase 3 rollout-state manifest update
- Promoted `citywide_v3` to the live visible citywide profile in backend manifest metadata.
- Kept `citywide_v2` available for comparison/debug metadata usage.
- Kept borough visible profiles unchanged in Phase 3 (`manhattan_v2`, `bronx_wash_heights_v2`, `queens_v2`, `brooklyn_v2`, `staten_island_v2`).

## Current pass: Phase 2 citywide_v3 shadow candidate (backend)

### Phase 2 shadow score rollout
- Added `citywide_v3` shadow score support across the backend hotspot shadow pipeline.
- `citywide_v3` blends raw demand, demand density, long-trip share, pay quality, downstream value, and trap penalties.
- Visible scores/colors remain unchanged in Phase 2 (shadow-only additive output, no live cutover).

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


## Current pass: Artifact freshness self-healing + auto-regeneration (backend)
### Freshness + startup regeneration
- Added backend artifact freshness signatures that combine code dependency hashes, source parquet inventory signatures, zones geometry signature, manifest alignment, and sampled frame field integrity checks.
- Startup now evaluates freshness and automatically triggers generation when artifacts are stale.
- Normal operation no longer requires manual deletion of old frame artifacts before rebuilds; stale outputs are overwritten by regeneration.
