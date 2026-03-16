# NYC TLC Hotspot Map - Verified Arquitecture (Current State, March 16, 2026)

> Note: the repository currently uses the filename `arquitecture.md`. This document keeps that spelling so it can replace the repo file cleanly.

## 0. Fast mental model

This project is no longer just a TLC heatmap.

The current product is a **driver operating system on top of a map**:

1. historical hotspot analytics build NYC-wide 20-minute frames from TLC parquet data
2. the frontend renders those frames with recommendation logic and special borough modes
3. signed-in community features add live driver presence, police reports, pickup reports, public chat, and DMs
4. account/profile features add ghost mode, avatar/name map identity, progression, and badges
5. admin tools, diagnostics, pickup guard rails, day tendency, leaderboard, radio, and games all run inside the same application shell

That is the current architecture truth the final refinement phase should work from.

---

## 1. What I reviewed for this version

This version was rebuilt from the latest uploaded material, not from older assumptions.

Reviewed inputs:

- frontend repository zip: `Frontend-github-pages--main.zip`
- backend repository zip: `Backend-railway--main.zip`
- current HTML shell snapshot
- the prior three markdown docs
- uploaded Railway screenshots showing service topology, variables, and database tables

---

## 2. Verified repository snapshot

## 2.1 File counts and code-size snapshot

### Frontend repo
- Verified file count: **21 total**
- Verified non-dot file count: **21**
- Verified code snapshot (HTML/CSS/JS/Procfile): **14,834 lines / 554,273 chars / 50,544 words**
- Verified full text snapshot (including docs): **15,520 lines / 575,689 chars / 53,718 words**

### Backend repo
- Verified file count: **42 total**
- Verified non-dot file count: **40**
- Verified code snapshot (Python/text runtime files): **8,813 lines / 318,378 chars / 27,023 words**
- Verified full text snapshot (including docs and dotfiles): **9,851 lines / 350,166 chars / 31,069 words**

### Concentration that matters
- `app.js` alone is about **45.3%** of the verified frontend code-line count.
- `app.js` + `app.part2.js` together are about **68.4%** of the verified frontend code-line count.
- `main.py` alone is about **38.3%** of the verified backend code-line count.
- The top four backend files (`main.py`, `pickup_recording_feature.py`, `build_day_tendency.py`, `leaderboard_service.py`) together are about **59.6%** of the verified backend code-line count.

That means the codebase is modularizing, but its risk is still concentrated in a few very large files.

## 2.2 Full frontend inventory

- `Procfile`
- `admin.actions.js`
- `admin.components.js`
- `admin.live.js`
- `admin.panel.css`
- `admin.panel.js`
- `admin.reports.js`
- `admin.system.js`
- `admin.tests.js`
- `admin.trips.js`
- `admin.users.js`
- `app.js`
- `app.part2.js`
- `app.part3.js`
- `arquitecture.md`
- `day-tendency.js`
- `docs/kq-94-5-verification.md`
- `index.html`
- `package.json`
- `pickup-recording.feature.js`
- `server.js`

### Frontend notes
- The frontend now includes **14 local runtime scripts** referenced by `index.html`.
- The HTML shell contains **79 element IDs**, which makes `index.html` an architectural file, not just a wrapper.
- The dock currently exposes **7 primary buttons**: colors, modes, chat, games, leaderboard, music, profile.
- The radio panel currently exposes **5 stations**: HOT 97, La Mega 97.9, KQ 94.5, Alofoke 99.3 FM, and Z100.
- The frontend includes a dedicated **admin UI bundle** (`admin.*.js` + `admin.panel.css`), which earlier docs under-described.

## 2.3 Full backend inventory

- `.python-version`
- `.tool-versions`
- `Procfile`
- `admin_models.py`
- `admin_mutation_models.py`
- `admin_mutation_routes.py`
- `admin_mutation_service.py`
- `admin_routes.py`
- `admin_security.py`
- `admin_service.py`
- `admin_test_models.py`
- `admin_test_routes.py`
- `admin_test_service.py`
- `admin_trips_models.py`
- `admin_trips_routes.py`
- `admin_trips_service.py`
- `arquitecture.md`
- `build_day_tendency.py`
- `build_hotspot.py`
- `chat.py`
- `core.py`
- `day-tendency.js`
- `db.py`
- `events.py`
- `hotspot_experiments.py`
- `hotspot_models.py`
- `hotspot_scoring.py`
- `leaderboard_db.py`
- `leaderboard_mailer.py`
- `leaderboard_models.py`
- `leaderboard_routes.py`
- `leaderboard_scheduler.py`
- `leaderboard_service.py`
- `leaderboard_tracker.py`
- `main.py`
- `micro_hotspot_scoring.py`
- `models.py`
- `pickup_recording_feature.py`
- `presence.py`
- `requirements.txt`
- `security.py`
- `users.py`

### Backend notes
- The backend now includes a real **admin subsystem**, **pickup-recording subsystem**, **leaderboard subsystem**, **chat router**, and **day-tendency build pipeline**.
- The backend contains **70 active routes** across files that are actually included by `main.py`.
- The backend repo also contains a stray **`day-tendency.js`** frontend-style file. Treat it as misplaced or historical unless you intentionally keep a shared copy strategy.
- The SQLAlchemy-style files (`db.py`, `models.py`, `users.py`, `presence.py`, `events.py`, `security.py`) still exist, but they are not the primary runtime path used by `main.py`.

## 2.4 Largest frontend files

| path | lines | chars | words |
|---|---|---|---|
| app.js | 6716 | 234343 | 23249 |
| app.part2.js | 3424 | 143989 | 12276 |
| index.html | 1849 | 59445 | 4870 |
| day-tendency.js | 514 | 17031 | 1538 |
| pickup-recording.feature.js | 481 | 21296 | 1864 |
| admin.tests.js | 417 | 18649 | 1766 |
| app.part3.js | 332 | 14131 | 1134 |
| admin.panel.js | 320 | 11018 | 1023 |

## 2.5 Largest backend files

| path | lines | chars | words |
|---|---|---|---|
| main.py | 3377 | 127377 | 10654 |
| pickup_recording_feature.py | 744 | 27417 | 2289 |
| build_day_tendency.py | 572 | 23071 | 1903 |
| leaderboard_service.py | 556 | 20674 | 1695 |
| build_hotspot.py | 352 | 11151 | 1130 |
| admin_service.py | 344 | 11743 | 1012 |
| admin_test_service.py | 280 | 11541 | 945 |
| chat.py | 240 | 7542 | 681 |

## 2.6 What this version corrects from the prior v2 docs

This version fixes several architecture-document drift problems from the earlier markdown:

- the file counts were too low
- the code-size counts were too low
- the admin portal and admin test surface were not documented as first-class architecture
- the pickup-recording subsystem was under-described
- the day-tendency subsystem was barely represented
- the map-identity/avatar system was not treated as architecture
- the profile + DM flow was missing from the real product model
- the micro-hotspot experiment/logging layer was missing
- the scheduler/mailer existence in the leaderboard subsystem was not called out
- the inactive legacy SQLAlchemy path was not clearly separated from the active runtime path

---

## 3. Verified deployment topology

## 3.1 Current deployment model

```text
Browser
  -> Railway frontend service
      -> Express static server (server.js, package.json, Procfile)
      -> serves index.html + JS/CSS assets
      -> hardcodes window.API_BASE to Railway backend URL
  -> Railway backend service
      -> FastAPI app in main.py
      -> included routers: chat, leaderboard, pickup recording, admin, admin mutations, admin trips, admin tests
  -> Postgres service
      -> community + operations tables
  -> Railway volume / file plane
      -> parquet inputs, taxi_zones.geojson, frames, timeline, day tendency model, build artifacts
```

## 3.2 What is code-verified versus screenshot-verified

### Code-verified
- Frontend is served by Express using:
  - `package.json` -> `npm start`
  - `server.js`
  - frontend `Procfile` -> `web: npm start`
- Backend is served by FastAPI using:
  - backend `Procfile` -> `web: uvicorn main:app --host 0.0.0.0 --port $PORT`
- The current frontend shell hardcodes:
  - `window.API_BASE = "https://web-production-78f67.up.railway.app"`

### Screenshot-verified
- Railway deployment includes a **web service**, **Postgres**, and attached **persistent volumes**
- The database screenshots show operational tables including:
  - `chat_messages`
  - `events`
  - `pickup_logs`
  - `presence`
  - `users`

### Important correction
The earlier doc stated a specific frontend public hostname as if it was architecture truth. That is not provable from the current source snapshot alone. The source clearly proves the backend base URL; it does **not** prove the final frontend hostname. Treat the frontend hostname as deployment metadata, not as source-of-truth architecture.

---

## 4. Frontend architecture

## 4.1 `index.html` is a major application shell

The current HTML shell is doing far more than mounting a map div. It defines:

- the full-screen MapLibre map container
- a weather FX canvas layer
- a sign-in/sign-up overlay
- colors, modes, chat, and music panel placeholders
- the dock drawer system
- the dock button row
- auto-center and record-trip floating controls
- the bottom timeline slider and bubble
- weather and online-driver badges
- the radio modal
- the debug toggle/panel
- script loading for the entire runtime bundle

The CSS in `index.html` also includes layout and interaction rules for:

- dock scrolling and edge hints
- ghost/community controls
- chat UI
- kill feed
- map identity/avatar markers
- chess and UNO panels
- leaderboard rendering
- radio modal behavior
- admin launcher interaction
- weather overlay and day/night presentation

So `index.html` is part shell, part styling system, and part feature registry.

## 4.2 `app.js` responsibilities

`app.js` remains the main frontend runtime spine. Verified responsibilities include:

- app bootstrapping and global helpers
- timeline loading and current-frame rendering
- hotspot zone styling and per-zone popup content
- recommendation calculation and Google Maps navigation link construction
- special mode logic:
  - Manhattan Mode
  - Staten Island Mode
  - Bronx/Wash Heights Mode
  - Queens Mode
  - Brooklyn Mode
- location, heading, orientation, and auto-center behavior
- presence rendering for other drivers
- adaptive rendering behavior under higher density
- online badge counts including visible versus ghosted users
- police report posting and overlay refresh
- pickup overlay rendering:
  - recent pickup points
  - zone stats
  - qualified zone hotspots
  - top-level micro-hotspots
- profile shell interactions
- weather badge and night-theme switching
- drawer/dock orchestration
- radio station launch wiring
- sign-in, sign-up, change password, delete account, and ghost-mode toggles
- admin-session sync hooks

`app.js` is still the highest-risk frontend file.

## 4.3 `app.part2.js` responsibilities

`app.part2.js` is now a second core runtime, not a side module.

Verified responsibilities include:

- the newer room/DM chat UI
- chat polling and unread-state management
- kill-feed overlay logic
- incoming/outgoing chat audio and audio-session priming
- per-driver profile modal loading through `/drivers/{user_id}/profile`
- direct-message thread UI
- map identity system:
  - `name` versus `avatar` mode
  - avatar selection
  - crop modal
  - avatar preview/remove
  - zoom-responsive label/avatar behavior
- self and remote driver identity rendering on markers
- chess vs CPU
- UNO vs CPU

Earlier docs did not give this file enough architectural weight.

## 4.4 `app.part3.js` responsibilities

`app.part3.js` is a dedicated leaderboard UI module. Verified features:

- miles/hours switch
- daily/weekly/monthly/yearly switch
- top-list view versus “See All Users”
- my-rank card
- summary card
- progression/rank/title line
- podium badge rendering

That is a clean, real subsystem.

## 4.5 `pickup-recording.feature.js`

This file is not a trivial helper. It is the client-side control layer for a guarded workflow:

- submit `/events/pickup`
- extract and surface structured backend errors
- show pickup-guard notices and cooldown timing
- trigger overlay refresh after successful trip record
- pass progression deltas and level-up hooks back into the UI
- expose admin-oriented pickup helpers/tests

This file should stay separate.

## 4.6 `day-tendency.js`

This file adds a second decision layer beyond the hotspot map. Verified responsibilities:

- fetch the day-tendency endpoint
- track movement/material GPS changes
- re-query when needed
- render a vertical score/band/borough meter under the online badge
- include mode flags in requests so the tendency display respects current map context

This was largely missing from earlier docs, but it is now part of the product.

## 4.7 Admin frontend bundle

The frontend now ships a real admin panel surface:

- `admin.panel.js` -> launcher, portal shell, tabs, cache, refresh
- `admin.components.js` -> shared rendering helpers
- `admin.actions.js` -> mutations like set admin, set suspended, clear reports, void trips
- `admin.users.js` -> user list and moderation controls
- `admin.live.js` -> live presence view
- `admin.reports.js` -> police and pickup report management
- `admin.system.js` -> health/system summary
- `admin.trips.js` -> recorded-trip summary and recent logs
- `admin.tests.js` -> operational test harness
- `admin.panel.css` -> dedicated admin styling

This is one of the biggest missing pieces in the old docs.

---

## 5. Backend architecture

## 5.1 Runtime core

The active backend runtime is the combination of:

- `main.py` for app creation, startup, legacy endpoints, analytics endpoints, auth/account endpoints, presence endpoints, and some event/admin endpoints
- `core.py` for DB backend selection, shared SQL helpers, token verification, password hashing, and auth enforcement
- included routers:
  - `chat.py`
  - `leaderboard_routes.py`
  - `pickup_recording_feature.py`
  - `admin_routes.py`
  - `admin_mutation_routes.py`
  - `admin_trips_routes.py`
  - `admin_test_routes.py`

## 5.2 `core.py` architecture truth

`core.py` is the real cross-cutting backend spine.

It currently defines:

- `DATA_DIR` and `COMMUNITY_DB_PATH`
- DB backend selection:
  - Postgres when `DATABASE_URL` or `POSTGRES_URL` exists
  - SQLite otherwise
- shared `_db`, `_db_exec`, `_db_query_one`, `_db_query_all`, and `_sql` helpers
- custom HMAC-signed token creation/verification
- PBKDF2 password hashing at **200,000 iterations**
- trial enforcement gate behind `ENFORCE_TRIAL`
- `require_user` request auth

Important consequence: the app is **not** currently using a formal JWT library or SQLAlchemy session layer as its primary runtime path.

## 5.3 `main.py` responsibilities

`main.py` still acts as the operational monolith. Verified responsibilities include:

- startup initialization
- database/table initialization and migrations
- frame/day-tendency readiness checks
- hotspot generation endpoints
- timeline/frame read endpoints
- zone/parquet upload endpoints
- auth signup/login
- `/me`, `/drivers/{user_id}/profile`, `/me/update`, `/me/change_password`, `/me/delete_account`
- presence update/all/summary
- legacy chat endpoints
- police report endpoints
- pickup record endpoint
- pickup overlay endpoint
- legacy admin disable/reset-password endpoints

It also contains important schema migration logic for:

- `ghost_mode`
- `avatar_url`
- `map_identity_mode`
- `is_suspended`
- pickup-log voiding fields
- experiment tables

## 5.4 Analytics and scoring subsystem

The analytics layer is now bigger than the old docs captured.

### `build_hotspot.py`
Builds the historical hotspot frames and timeline from TLC parquet data and zone geometry.

Verified characteristics:
- 20-minute bins by default
- percentile-rank normalization
- per-window and per-zone normalization
- bucket/color mapping:
  - green
  - purple
  - blue
  - sky
  - yellow
  - red

### `build_day_tendency.py`
Builds the separate borough/time-slot tendency model.

Verified cohort families:
- `borough_weekday_bin`
- `borough_bin`
- `borough_baseline`
- `global_bin`
- `global_baseline`

### `hotspot_scoring.py`
Adds live scoring that blends:
- historical support
- recency-weighted live support
- same-timeslot support
- density penalty
- active-driver network strength

### `micro_hotspot_scoring.py`
Creates compact recommended micro-hotspots inside zones using a projected grid and ETA-aware scoring.

### `hotspot_experiments.py`
Logs evaluation data into:
- `hotspot_experiment_bins`
- `micro_hotspot_experiment_bins`
- `recommendation_outcomes`

This experiment layer is architecture, not trivia.

## 5.5 Auth, identity, and profile subsystem

Verified current behavior:

- first signed-up user becomes admin
- `ADMIN_EMAIL` can force admin
- `ADMIN_BOOTSTRAP_TOKEN` can grant admin at signup
- `_ensure_admin_seed()` can create/promote the configured admin user at startup
- `/me` returns:
  - display name
  - avatar URL
  - map identity mode
  - ghost mode
  - admin flag
  - trial expiry
  - current leaderboard badge
- `/drivers/{user_id}/profile` returns:
  - driver card
  - daily/weekly/monthly/yearly stats
  - ranks
  - progression
  - best badge

The map identity/avatar system is now part of backend account architecture too, not just frontend UI.

## 5.6 Presence and work-tracking subsystem

Presence is now doing more than “show dots on a map.”

Verified current behavior:

- `/presence/update` writes a heartbeat
- updates with accuracy worse than 50 meters are ignored
- presence update also feeds:
  - `leaderboard_tracker.record_presence_heartbeat`
  - `pickup_recording_feature.record_pickup_presence_heartbeat`
- `/presence/all` requires auth and filters out ghost-mode users
- `/presence/summary` returns:
  - online count
  - ghosted count
  - visible count

`leaderboard_tracker.py` then derives miles/hours from meaningful motion, using:
- NYC 4 AM business-day boundary
- max counted gap: 300 seconds
- max speed sanity cap: 100 mph
- minimum meaningful movement: 0.01 miles
- minimum meaningful speed: 1 mph

That makes presence a shared signal for both community rendering and driver progression.

## 5.7 Chat subsystem

The backend chat story is currently dual-generation.

### Current router (`chat.py`)
- room endpoint pair:
  - `GET /chat/rooms/{room}`
  - `POST /chat/rooms/{room}`
- DM endpoint pair:
  - `GET /chat/dm/{other_user_id}`
  - `POST /chat/dm/{other_user_id}`
- 600-char max message length
- 2-second per-user rate limit

### Legacy endpoints still in `main.py`
- `POST /chat/send`
- `GET /chat/recent`
- `GET /chat/since`

This is an intentional transition risk that the final refinement phase should acknowledge.

## 5.8 Leaderboard subsystem

The leaderboard subsystem is now mature enough to be treated as its own architecture block.

### Data layer
`leaderboard_db.py` initializes:
- `driver_work_state`
- `driver_daily_stats`
- `leaderboard_badges_current`
- `leaderboard_badges_refresh_state`

### Service layer
`leaderboard_service.py` provides:
- leaderboard lists
- my-rank lookups
- overview blocks
- progression blocks
- podium badge derivation
- rank ladder from **Recruit** up to **Road Legend**
- XP weighting:
  - 8 XP per mile
  - 30 XP per hour
  - 20 XP per reported pickup
  - pickup XP cap of 25 reports/day

### API layer
`leaderboard_routes.py` exposes:
- `/leaderboard`
- `/leaderboard/me`
- `/leaderboard/badges/me`
- `/leaderboard/overview/me`
- `/leaderboard/progression/me`

### Scheduler/mailer note
`leaderboard_scheduler.py` and `leaderboard_mailer.py` exist, but the scheduler is **not started in current app startup**. So leaderboard email/report automation exists in code, but not yet as verified runtime behavior.

## 5.9 Pickup-recording subsystem

The pickup-recording system is one of the most important missing pieces from earlier docs.

Verified architecture includes:

- schema setup for `pickup_guard_state`
- guarded save evaluation before trip logging
- cooldown and anti-spam logic
- motion/session/relocation checks
- progression delta generation
- soft-void admin workflow
- test endpoints for health, guard evaluation, simulation, and filter smoke tests

Important guard constants in code:
- cooldown: 600 seconds
- minimum driving time: 360 seconds
- session-break threshold: 480 seconds
- stale-motion threshold: 180 seconds
- relocation minimum: 0.25 miles
- same-position maximum: 0.08 miles

This is a real product-protection system, not a cosmetic feature.

## 5.10 Admin subsystem

The backend admin surface now has four major route groups.

### Admin data routes
- `/admin/summary`
- `/admin/users`
- `/admin/live`
- `/admin/reports/police`
- `/admin/reports/pickups`
- `/admin/system`

### Admin mutation routes
- set admin
- set suspended
- fetch user detail
- clear police report
- clear pickup report

### Admin trip routes
- trip summary
- recent recorded trips, with optional voided inclusion

### Admin diagnostic routes
- backend status
- timeline
- frame current
- admin auth
- presence summary/live/shared endpoint checks
- me endpoint check
- trips summary/recent
- police reports
- pickup reports
- pickup-overlay endpoint

This is now a first-class operations console for the map.

## 5.11 Legacy or non-primary backend files

These files still matter, but they are not the main runtime path:

- `db.py`
- `models.py`
- `users.py`
- `presence.py`
- `events.py`
- `security.py`

They reflect an older SQLAlchemy-style architecture.

That means the backend currently has two architectural generations in the repository:
1. the active `main.py` + `core.py` + included-router path
2. the older SQLAlchemy/APIRouter path that is mostly no longer wired into startup

This needs to be documented so future work does not accidentally refactor the wrong layer.

---

## 6. Storage and schema layout

## 6.1 File/volume plane

Used for durable analytics artifacts:

- parquet input files
- `taxi_zones.geojson`
- hotspot frame files
- `timeline.json`
- day-tendency model output
- build locks and transient build products

## 6.2 Database plane

Operational tables verified from code include:

- `users`
- `presence`
- `events`
- `pickup_logs`
- `chat_messages`
- `hotspot_experiment_bins`
- `micro_hotspot_experiment_bins`
- `recommendation_outcomes`
- `driver_work_state`
- `driver_daily_stats`
- `leaderboard_badges_current`
- `leaderboard_badges_refresh_state`
- `pickup_guard_state`

## 6.3 Important schema truths

- The app supports both SQLite and Postgres through a lightweight compatibility layer.
- Postgres boolean columns are normalized at startup where needed.
- `users` now contains newer identity/control columns:
  - `display_name`
  - `ghost_mode`
  - `avatar_url`
  - `map_identity_mode`
  - `is_suspended`
- `pickup_logs` now contains soft-void and counting fields:
  - `is_voided`
  - `voided_at`
  - `voided_by_admin_user_id`
  - `void_reason`
  - `counted_for_pickup_stats`
  - `guard_reason`

---

## 7. End-to-end data flows

## 7.1 Hotspot generation flow

1. parquet files + zone geometry live on the volume
2. `/generate` triggers hotspot frame build
3. `build_hotspot.py` writes timeline + frame artifacts
4. startup and status endpoints detect readiness
5. frontend loads `/timeline` then `/frame/{idx}`
6. frontend styles polygons and recommendation logic from the current frame

## 7.2 Day-tendency flow

1. startup checks whether the tendency model exists and is current
2. if frames exist but the model is missing/stale, startup rebuilds day tendency
3. frontend `day-tendency.js` requests today/date endpoints
4. the tendency meter renders score/band/borough context under the online badge

## 7.3 Presence -> leaderboard -> pickup-guard flow

1. frontend sends `/presence/update`
2. backend writes `presence`
3. backend also updates:
   - leaderboard work-state/daily stats
   - pickup guard movement/session state
4. `/presence/all` and `/presence/summary` power driver rendering and the online badge
5. leaderboard and pickup systems both depend on the same heartbeat stream

## 7.4 Pickup-record flow

1. user presses record trip
2. frontend sends `/events/pickup`
3. backend pickup guard validates cooldown, motion, and relocation rules
4. if allowed, `pickup_logs` is written
5. progression counters and overlays update
6. pickup overlay refreshes zone stats + hotspots + micro-hotspots
7. admin can later inspect or void a recorded trip

## 7.5 Chat flow

1. signed-in user opens chat/profile DM thread
2. frontend polls room or DM endpoints
3. backend returns messages from `chat_messages`
4. frontend updates unread badge, kill feed, and optional audio feedback

## 7.6 Admin flow

1. admin signs in like a normal user
2. frontend detects `me.is_admin`
3. admin launcher becomes visible
4. admin panel queries `/admin/*`, `/admin/trips/*`, and `/admin/tests/*`
5. panel can mutate users/reports/trips through admin action endpoints

---

## 8. Current runtime truths that matter before final refinements

## 8.1 This is a Railway web app, not a GitHub Pages-only artifact

The frontend repo name still suggests GitHub Pages heritage, but the current verified runtime is Railway + Express.

## 8.2 The product scope is bigger than “map + heat colors”

Current product scope includes:

- hotspot decision support
- special mode logic
- live community overlays
- driver profile/progression/badges
- public chat and DMs
- admin operations
- pickup guard rails
- day tendency
- radio and games

Any refinement plan that ignores those layers will under-scope the work.

## 8.3 `is_suspended` and `is_disabled` are not the same thing

A key architectural inconsistency exists right now:

- admin UI/mutations actively manage `is_suspended`
- auth enforcement in the active runtime checks `is_disabled`

So “suspended” currently appears to be an admin metadata/control flag, not the actual access-blocking flag. That needs an explicit product decision during refinements.

## 8.4 Delete-account cleanup is partial

`/me/delete_account` removes:

- presence
- chat messages
- events
- user row

It does **not** obviously clean up every related leaderboard/admin/pickup auxiliary table. That is an architecture and privacy follow-up item.

## 8.5 The leaderboard scheduler exists but is not active runtime

Do not assume automated mails/reports are live just because the code exists.

## 8.6 The backend still allows very open CORS

Current code uses:

- `allow_origins=["*"]`
- `allow_credentials=False`

That is workable for current testing, but it should be a conscious deployment decision, not an undocumented default.

## 8.7 The frontend deployment contract is still too hardcoded

`window.API_BASE` is hardcoded in the HTML shell. That is simple, but it means deployment changes still require code edits or HTML rewrites.

---

## 9. Product-critical invariants

Do not break these while refining the system:

1. the 20-minute timeline/frame contract
2. the honesty of zone colors and recommendation text
3. ghost mode hiding the driver from other drivers
4. pickup-recording guard rails
5. presence-derived leaderboard/progression accounting
6. admin visibility into users, live state, reports, and trips
7. profile badge/progression rendering
8. DM/public chat continuity
9. the map staying mobile-first and one-screen operational

---

## 10. Best current mental model

The cleanest accurate mental model is:

> **This is a mobile-first NYC driver operations platform whose central canvas is a map.**

The map is the core interaction surface, but the architecture also includes:

- analytics generation
- live operations
- identity/account state
- social/community functions
- engagement/progression features
- admin/diagnostic tooling

That is the architecture this repo actually has today.
