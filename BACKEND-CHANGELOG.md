# BACKEND CHANGELOG

## Current pass: Nightlife & dining district pickup pulse (backend)

### New `nightlife_hotspot_builder.py` + `GET /nightlife_districts`
- Added a **nightlife/dining districts** system — a parallel of the dollar-flag long-trip hotspots for the after-dark crowd. A curated list of NYC venues (high-end restaurants + bars/clubs) is clustered into districts, and each district pulses on the map during its **let-out window** (dinner let-out through last call), the best time to be parked nearby.
- `nightlife_hotspot_builder.py` (new, self-contained): `NIGHTLIFE_POIS` — a research-sourced, hand-curated list of ~41 venues across 8 tight districts (Meatpacking, Lower East Side, SoHo/Nolita, West Village, Flatiron/NoMad, Tribeca, Williamsburg, Greenpoint), each `(name, lat, lng, category, weight, address)`. Reuses the dollar-flag rules — complete-link clustering at `CLUSTER_RADIUS_MI = 0.25` (~5-min walk), keep clusters of `MIN_MEMBERS_PER_DISTRICT = 3` — plus one added rule: a district must **mix dining AND nightlife** (>=1 each), not just three of one kind.
- **Let-out schedule:** `dim_schedule` is computed from the whole member set so the `prime` pulse spans the **8pm dinner let-out through the district's latest close**, with a `prime_weekend` that runs later Fri/Sat (clubs to ~4am). Hour ranges wrap past midnight. `district_runtime_meta()` recomputes `dim_schedule`/`best_hours`/`rationale` at read time, so a schedule edit takes effect on the next GET with no rebuild.
- `main.py`: new `nightlife_districts` table (SQLite + Postgres, same shape as `long_trip_hotspots`), `GET /nightlife_districts` (user) returning districts + per-district `dim_schedule`, `POST /admin/nightlife_districts/rebuild` (admin), and a startup **seed-if-empty** so the map has data on first boot with no manual admin call. The `GET` also **self-heals**: if the table is ever empty (a startup seed that never ran or failed), it seeds on demand — with an in-memory build as a last-resort fallback if the write doesn't take — so the endpoint never returns a blank overlay that leaves the map with nothing to pulse.
- **Keyless / no account** — the data is curated and baked in (no Places API key, sidestepping the Ticketmaster-style account blocker). `tests/test_nightlife_hotspots.py` (9 offline tests): clustering, the mixed-qualification rule (rejects pure-dining / pure-nightlife / undersized), the let-out schedules, `district_runtime_meta`, and the write path.

## Current pass: Keyless sports sources for city events (no key, no account)

### MLB / NHL / NBA home games feed the map without any API key
- The city-events map feature was Ticketmaster-only, but the operator can't create a Ticketmaster account. Added three **keyless, no-account** official league schedule sources so the map shows events out of the box: **MLB** (`statsapi.mlb.com`), **NHL** (`api-web.nhle.com`), **NBA** (`cdn.nba.com` static schedule). Only **home games** of the NYC-metro teams are kept (Yankees/Mets, Rangers/Islanders/Devils, Knicks/Nets), so every kept game lands at a venue already in the coordinate-fallback table.
- `city_events.py`: new pure normalizers `normalize_mlb_game()` / `normalize_nhl_game()` / `normalize_nba_game()` (mirror `normalize_event` — `.get()`-guarded, never raise, return `None` to skip away games, postponed/cancelled/suspended games, unknown venues, or missing fields; every kept game maps to `category="sports"`), new fetchers `fetch_mlb_today()` / `fetch_nhl_today()` / `fetch_nba_today()` (lazy `httpx`, browser UA, today-NYC window), a `_parse_iso_utc()` helper, and a `_BROWSER_UA` (the league CDNs 403 non-browser agents).
- `refresh_city_events_once()` now aggregates **all** sources (Ticketmaster + MLB + NHL + NBA), each isolated in its own try/except so one failing feed can't sink the batch, de-dups on `(source, source_id)`, and returns per-source counts (`src_mlb`, `src_nhl`, `src_nba`, `src_ticketmaster`).
- `start_city_events_refresh()` **always** starts the daemon now (it previously skipped without a Ticketmaster key); `POST /admin/city_events/refresh` dropped its `TICKETMASTER_API_KEY` 503 guard. Ticketmaster stays in the code but **dormant** (returns `[]` without a key) — if the operator ever gets a key, concerts/conventions light up with no rebuild.
- **No frontend change:** the merged `city-events.feature.js` already renders `category="sports"` (orange sprite at the venue + gold let-out pulse) and never reads the `source` field.
- **Setup:** none — the sports feeds run with **no env vars and no account**. `TICKETMASTER_API_KEY` stays optional (for concerts/conventions). Safe to deploy with no keys at all.

## Current pass: City events feed (Ticketmaster → map)

### New `city_events.py` + `GET /city_events`
- Added a **city-events** system: a background daemon thread fetches today's major NYC events (concerts/music, sports, conventions/expos) from the **Ticketmaster Discovery API** and caches them in a new `city_events` table; `GET /city_events` (auth) serves them so the frontend can pin them and highlight the **let-out** surge (best pickup time).
- `city_events.py` (new, self-contained router): `fetch_nyc_events_today()` (httpx, NYC DMA, today window, Music/Sports/Miscellaneous segments), pure `normalize_event()` (→ name/venue/lat/lng/start/category; skips coordinate-less or non-requested events; small NYC-venue coordinate fallback), `ensure_city_events_schema()` (dual SQLite/Postgres, `UNIQUE(source, source_id)`), upsert (`ON CONFLICT … DO UPDATE`) + prune-past, `select_events_for_today()` (keeps events still letting out from earlier today), and a daemon refresh worker (`CITY_EVENTS_REFRESH_SECONDS`, default 1800).
- `POST /admin/city_events/refresh` (admin) forces an immediate refresh for ops/testing.
- `main.py`: import + `include_router`, `ensure_city_events_schema()` + `start_city_events_refresh()` wired into `startup()` (both guarded/non-fatal). `httpx` already in `requirements.txt` (imported lazily inside the fetch).
- **Setup:** set `TICKETMASTER_API_KEY` (free key) on Railway. **Dormant without it** — the worker skips and the endpoint returns an empty list, so it's safe to deploy before the key is set.

## Current pass: Pickup-window correction + holiday/school calendar (backend)

### Time windows corrected to pickups (people leaving), not arrivals
- A dollar-flag pickup is someone *leaving* a building for a long trip, so the arrival-side windows were wrong:
  - **Hotels** — dropped the 3–7pm check-in window (arrivals are drop-offs, not pickups). Now `peak [[6,12]]`, `prime [[7,11]]`: the morning checkout → airport wave. `best_hours`: "Checkout 7am–noon — airport runs (check-in isn't a pickup)".
  - **Corporate** — dropped the 8–10am window (morning is people arriving at the office). Now `peak [[16,20]]`, `prime [[16,19]]`: end-of-day departures. `best_hours`: "Weekday end-of-day 4–8pm (esp. Thu/Fri); closed holidays".
  - Transit (station arrivals → onward rides), hospital (discharges), and school (parent pickup) were already departure-based and are unchanged. `prime ⊆ peak` re-verified for all 12 categories.

### New `holiday_calendar.py` — federal holidays + school recesses, served per request
- Added `holiday_calendar.py`: computes US federal holidays for any year (with the observed Sat→Fri / Sun→Mon shift) from date arithmetic — no external service, deterministic, offline. Also defines recurring NYC-DOE-style school recess ranges (summer, winter, midwinter, spring).
- `GET /long_trip_hotspots` now also returns a `calendar`: `{tz, holidays: [...], seasonal_closures: {private_school: [["YYYY-MM-DD","YYYY-MM-DD"], ...]}}`. Computed per request (recompute-on-read, nothing persisted, no migration).
- The frontend matches its NYC date against this to **close** (dim off, never pulse) the weekday-only flags (offices, schools) on weekends + holidays, and the school flag across summer/recess. Hotels, transit, and hospitals keep running — holiday travel lifts hotel checkouts & transit, and hospitals are 24/7.
- Verified: 2026 federal dates match the official list (incl. Jul 4 Sat → observed Jul 3); a closure simulation darkens the corporate flag on Christmas/holidays and the school flag all summer, while hotels still pulse.

### Hardening + 20-year accuracy (review follow-up)
- School recesses are now computed **per year** (not fixed dates) and served as explicit `[start, end]` ISO ranges: summer anchored to Labor Day, winter, midwinter on the Presidents'-Day week, and spring on the Good-Friday week (Gregorian Computus). Published NYC DOE spring dates are pinned as exact overrides (2025, 2026), since NYC ties spring to Passover/Easter and the computed proxy can't always catch the extended breaks.
- Added `test_holiday_calendar.py`: asserts the federal holidays are correct for **every year 2026–2045** (count, weekday rules, observed shifts), plus Easter/Computus dates and school-range invariants — locking the "accurate for the next 20 years" guarantee into CI.
- Widened the served holiday window to year+2 so a payload stays valid across the New-Year boundary (an observed New-Year's-Day that lands on Dec 31 of the following year).
- Hardened the endpoint against a malformed stored `members_json` row (non-list, or non-dict elements) so it can't 500: coerce to a list, and skip non-dict members in `hotspot_runtime_meta`.

## Current pass: Dollar-flag prime-time pulse signal (backend)

### Per-category `prime` window + read-path schedule/rationale plumbing
- Added a researched `prime` hour-window per category to `CATEGORY_DIM_SCHEDULE` in `long_trip_hotspot_builder.py` — the tightest "best time to be near it" window(s), a strict subset of `peak`. Drives the new pulsing ring at each dollar flag's pole base. Grounded in building busy-hour patterns: airport arrival banks (5–9am / 4–9pm), hospital discharges peaking ~4pm, hotel morning airport runs (7–11am), transit + corporate evening rush (5:30–6:30pm), school pickup (2:30–4pm). Heuristics, not measured trip data.
- Of the 25 live clusters, only 5 dominant categories actually occur (`hotel_luxury` ×12, `transit_hub` ×8, `hospital` ×3, `private_school` ×1, `corporate` ×1), so those are the windows that drive the pulse in practice.
- Nudged two `peak` ranges to keep `prime ⊆ peak`: hospital `[[10,16]] → [[10,17]]` (afternoon discharge peak at 4pm) and corporate `[[8,10],[17,19]] → [[8,10],[16,19]]` (evening rush starts 4pm). Verified `prime ⊆ peak` for all 12 categories.
- `_dim_schedule_for` now emits `prime` alongside `peak`/`off`/`weekday_only`.
- **Fixed a latent gap**: `GET /long_trip_hotspots` previously served only `id/lat/lng/label/dominant_category/member_count/total_weight/members`, so the frontend never received `dim_schedule`, `best_hours`, or `rationale` — the time-of-day dim was dormant and the popup's "Best hours"/"Why this is a hotspot" rows rendered blank. Added `hotspot_runtime_meta()` (shared with the build path via the new `summarize_categories()`), and the endpoint now recomputes and serves `dim_schedule` (incl. `prime`), `best_hours`, `rationale`, and `category_counts` per row.
- Recompute-on-read by design: these are pure functions of the static category tables, so there is **no DB column or migration**, the signal works for already-stored rows, and editing a schedule takes effect on the next request without an admin rebuild. No change to the table schema, the rebuild path, or the POI list.

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
