# BACKEND REGRESSION CHECKLIST

## Startup / database
- [x] SQLite-only local/test startup works when `DATABASE_URL` is unset and `psycopg2` is unavailable.
- [x] Postgres mode fails clearly when requested without `psycopg2`.
- [x] Postgres helper path still uses a threaded connection pool wrapper.

## City events (Ticketmaster)
- [x] `tests/test_city_events.py` passes: `normalize_event` (concert/sports/fallback/skip), start-time parsing, schema→upsert→select→prune roundtrip, no-key fetch returns `[]` (7/7).
- [x] `ensure_city_events_schema()` creates the `city_events` table on both SQLite and Postgres (`UNIQUE(source, source_id)`); upsert uses `ON CONFLICT … DO UPDATE`.
- [x] Feature is dormant without `TICKETMASTER_API_KEY` — worker skips, `GET /city_events` returns an empty list (safe to deploy before the key is set).
- [ ] live: with `TICKETMASTER_API_KEY` set, `POST /admin/city_events/refresh` populates rows and `GET /city_events` returns today's NYC events.

## Auth / account control
- [x] signup works
- [x] login works
- [x] `/me` works
- [x] disabled and suspended behavior is consistent across login and authenticated routes
- [x] blocked users are hidden from driver profile lookups

## Presence
- [x] `/presence/update` works
- [x] `/presence/all` still works for backward compatibility
- [x] `/presence/viewport` works
- [x] `/presence/delta` works with `updated_since_ms`
- [x] ghost-mode hiding still works
- [x] `/presence/summary` works
- [x] admin disable/suspend removes live presence deterministically

## Police / pickup / leaderboard
- [x] police report create/read still works
- [x] pickup recording / guard logic still works
- [x] leaderboard overview/progression/ranks still work

## Chat polling paths
- [x] public chat send/list still works
- [x] DM send/list still works
- [x] polling summary routes still work when SSE is ignored
- [x] public chat reads do not expose disabled/suspended senders
- [x] DM target validation blocks disabled/suspended targets

## Safe Phase 2 live chat
- [x] `/chat/live/capabilities` works with Bearer auth
- [x] public SSE rejects missing/invalid auth
- [x] private SSE rejects missing/invalid auth
- [x] public SSE works with short-lived live token auth
- [x] private SSE works with short-lived live token auth
- [x] public message publish causes a public live event
- [x] DM message publish causes a private summary live event

## Delete-account cleanup
- [x] delete-account cleanup removes presence/runtime/chat/pickup/leaderboard/user rows
- [x] delete-account cleanup anonymizes recommendation outcomes
- [x] delete-account cleanup removes avatar/chat-audio artifacts

## Compatibility
- [x] no route regression for current frontend compatibility surfaces verified by regression tests

## Long-trip hotspots
- [x] `build_long_trip_hotspots()` yields clusters in all five boroughs, each with 3+ members
- [x] every POI in `NYC_LONG_TRIP_POIS` has a `POI_ADDRESSES` entry and a category supported by all category maps
- [x] `POST /admin/long_trip_hotspots/rebuild` then `GET /long_trip_hotspots` returns the rebuilt pins
- [x] `GET /long_trip_hotspots` now serves `dim_schedule` (peak/off/weekday_only/prime), `best_hours`, `rationale`, and `category_counts` per hotspot
- [x] every category's `prime` window is a subset of its `peak` window (a pulsing flag is always at full brightness)
- [x] `hotspot_runtime_meta()` and the build path produce identical `rationale` wording (shared `summarize_categories()`)
- [x] schedule fields are recomputed on read — no new DB column, no migration, no admin rebuild required
- [x] hotel/corporate windows are pickup-only (checkout / end-of-day); no arrival windows; `prime ⊆ peak` still holds
- [x] `holiday_calendar.federal_holidays(2026)` matches the observed federal dates (incl. Jul 4 → observed Jul 3)
- [x] `GET /long_trip_hotspots` returns a `calendar` (holidays + per-category `seasonal_closures`); closure sim darkens offices/school on holidays/summer while hotels keep running
- [x] `test_holiday_calendar.py` passes: federal holidays correct for every year 2026–2045 (rules + observed shifts), Easter/Computus, school-range invariants
- [x] school recesses computed per year (Labor-Day summer, Presidents' midwinter, Good-Friday/Computus spring + published overrides), served as ISO `[start,end]` ranges
- [x] endpoint can't 500 on a malformed `members_json` row (coerced to list; non-dict members skipped)

## Pickup zone hotspots
- [x] `_shape_hotspot_component` shrinks hotspot polygons in small zones and leaves zones >= ~1 km^2 unchanged
- [x] no single zone hotspot covers more than `PICKUP_ZONE_HOTSPOT_MAX_ZONE_COVERAGE` of its zone
- [x] shaped polygons remain valid and non-empty across the zone-size range
