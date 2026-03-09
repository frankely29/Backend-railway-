# NYC TLC Hotspot Map — Verified Architecture (Current State)

> Note: the repository currently spells this file `arquitecture.md`. I am preserving that naming convention in this updated version so it can be dropped into the repo without forcing an unrelated rename.

## 1. Purpose

This system is a data-driven NYC hotspot map for FHV/Uber drivers. The core job of the product is to help the driver decide **where to go next** by combining:

- 20-minute hotspot frames generated from TLC parquet trip data
- live driver location/presence
- community event reporting (police + trip logs)
- on-map navigation/recommendation helpers
- chat and lightweight community tools

The product is not just a static heatmap. It is a split system with a generated analytics layer plus a live community layer.

---

## 2. Verified deployment topology

## Source control

There are **two GitHub repositories**:

### Frontend repo
- Repo name: `frankely29/Frontend-github-pages-`
- Verified files:
  - `index.html`
  - `app.js`
  - `app.part2.js`
  - `arquitecture.md`
  - `server.js`
  - `package.json`
  - `Procfile`
  - `docs/kq-94-5-verification.md`

### Backend repo
- Repo name: `frankely29/Backend-railway-`
- Verified files:
  - `main.py`
  - `build_hotspot.py`
  - `chat.py`
  - `core.py`
  - `db.py`
  - `events.py`
  - `models.py`
  - `presence.py`
  - `security.py`
  - `users.py`
  - `requirements.txt`
  - `Procfile`
  - plus `.python-version` and `.tool-versions`

## Deployment model

Despite the frontend repo name, the current verified deployment is **not “GitHub Pages frontend + Railway backend”**.

The current live model is:

### Frontend Railway environment
- Separate Railway environment/project for the frontend
- Service name shown in screenshots: `Frontend-github-pages-`
- Public domain shown in screenshots: `teamjoseo.up.railway.app`
- Port shown in screenshots: `8080`
- Source repo connected to Railway: `frankely29/Frontend-github-pages-`
- Branch connected to production: `main`
- Service variable shown in screenshots: `API_BASE=https://web-production-78f67.up.railway.app/`

### Backend Railway environment
- Separate Railway environment/project for the backend
- Service name shown in screenshots: `web`
- Public domain shown in screenshots: `web-production-78f67.up.railway.app`
- Source repo connected to Railway: `frankely29/Backend-railway-`
- Branch connected to production: `main`
- Backend is connected to:
  - a **Postgres database service**
  - a **persistent volume** (`web-volume`)

### Database service
- Railway Postgres is online
- Screenshots show the following tables currently exist:
  - `chat_messages`
  - `events`
  - `pickup_logs`
  - `presence`
  - `users`

### Storage split (runtime truth)
- **Volume** stores trip input data and generated artifacts
- **Postgres** stores community and operational data

This matches the user’s intended architecture:
- parquet/data assets live on the volume
- trip/community/auth/presence records live in Postgres

---

## 3. File inventory and size snapshot

## Frontend repo inventory
- File count: **8**
- Main file sizes:
  - `index.html` — 1467 lines / 3635 words / 42440 chars
  - `app.js` — 4299 lines / 14723 words / 140201 chars
  - `app.part2.js` — 391 lines / 1667 words / 15292 chars
  - `arquitecture.md` — 367 lines / 1696 words / 12057 chars
  - `server.js` — 27 lines
  - `package.json` — 11 lines

## Backend repo inventory
- File count: **14**
- Main file sizes:
  - `main.py` — 1347 lines / 4183 words / 46227 chars
  - `build_hotspot.py` — 352 lines / 1130 words / 11151 chars
  - `chat.py` — 190 lines / 526 words / 5700 chars
  - `core.py` — 176 lines / 489 words / 5240 chars
  - supporting modules are smaller and mostly architectural leftovers / alternates

Interpretation:
- the **frontend runtime is concentrated in `app.js`**
- the **backend runtime is concentrated in `main.py` + `build_hotspot.py` + `chat.py` + `core.py`**

---

## 4. Frontend architecture

## 4.1 Runtime stack

The frontend is a browser-based **MapLibre GL JS** application.

### What loads it
- `index.html` loads the map shell, UI, and script entrypoints
- `server.js` serves the static files with Express
- `Procfile` runs `npm start`
- `package.json` defines the Railway start command

So the frontend deployment is a **static-ish Node/Express wrapper** around a browser app.

## 4.2 Frontend files and responsibilities

### `index.html`
This is the UI shell and DOM contract for the whole app.

It contains:
- the full-screen map container
- weather effects canvas
- sign-in overlay (`lockedOverlay`)
- email/password/display-name/ghost-mode inputs
- legend panel
- modes panel
- chat placeholder panel
- music panel
- dock drawer UI
- dock buttons
- floating `Record Trip` button
- bottom slider and time label
- weather badge
- online-drivers badge
- radio modal
- debug panel
- hardcoded `window.API_BASE`
- script loading for `app.js` and `app.part2.js`

### `app.js`
This is the main frontend brain. It currently owns almost everything except the newer room-based chat module.

Major responsibilities verified from code:

#### A. API base and network helpers
- defaults to `https://web-production-78f67.up.railway.app`
- allows override via `window.API_BASE`
- centralizes `fetchJSON`, `postJSON`, `getJSONAuth`

#### B. Timeline logic
- loads `/timeline`
- aligns the slider to NYC local time
- computes minute-of-week values
- selects the closest frame to the current NYC time
- loads `/frame/{idx}`

#### C. Map initialization
- creates a `maplibregl.Map`
- uses a **CARTO Voyager raster basemap**
- configures glyphs via MapLibre demo glyph endpoint
- starts centered around NYC with zoom ≈ 10.2
- adds sources/layers for:
  - `zones`
  - `zone-labels`
  - `pickup-points`
  - heatmap/circle layers for pickups

#### D. Zone rendering
- renders frame polygons into MapLibre GeoJSON sources
- applies bucket/color-driven fill styling
- adds outlines
- restores zone click popups

#### E. Zone labels
- computes label points to keep labels inside polygons
- uses zoom-based visibility rules
- shrinks labels as zoom changes
- tries to act more like major map apps instead of floating random labels

#### F. Staten Island Mode
- persisted in localStorage
- re-scores Staten Island zones locally using percentile logic
- avoids borough-wide compression making Staten Island look permanently weak

#### G. Manhattan Mode
- persisted in localStorage
- applies a Manhattan-focused scoring/recommendation adjustment
- gives an alternate local ranking view in core Manhattan

#### H. Recommendation engine
- scores reachable destination options using bucket/rating plus distance
- favors Blue and better areas
- updates the recommendation text
- drives the external Navigate link target

#### I. Drawer/dock system
- single dock-driven UI for:
  - Colors
  - Modes
  - Chat
  - Music
  - Profile
- builds HTML for each panel dynamically
- handles panel open/close and active button state

#### J. Slider UX
- renders the bottom time slider
- shows slider bubble labels
- formats time as NYC day/time

#### K. Pickup overlay
- fetches `/events/pickups/recent`
- builds a pickup heatmap / circles overlay
- stores aggregated zone stats client-side
- shows community trip popups with zone, borough, age, and sample context

#### L. Weather
- manages a weather badge
- applies optional night basemap effects and overlay effects
- determines night/day visual state

#### M. Geolocation and heading
- tracks the driver’s true location
- tracks heading / bearing / speed
- attempts to reject obvious GPS spikes
- keeps the self-arrow pinned to real coordinates
- rotates the arrow based on actual heading rather than arbitrary map moves

#### N. Auto-center / map following
- auto-center can be toggled on/off
- user exploration can disable auto-center
- programmatic moves are guarded to reduce jitter

#### O. Presence / other drivers
- pushes own presence to `/presence/update`
- pulls active drivers from `/presence/all`
- filters stale users client-side
- renders other-driver markers with labels
- label collision is handled separately from marker anchor so marker coordinates stay truthful

#### P. Auth / profile / ghost mode
- uses localStorage keys:
  - `community_token_v1`
  - `community_email_v1`
  - `community_display_name_v1`
- supports signup/login
- reads `/me`
- updates display name / ghost mode through `/me/update`
- includes change password and delete account UI flows

#### Q. Community actions
- reports police via `/events/police`
- records pickups via `/events/pickup`
- shows recent pickup overlay via `/events/pickups/recent`

#### R. Radio
- HOT 97
- La Mega 97.9
- KQ 94.5
- Z100
- dock buttons + radio modal iframe

#### S. Debug support
- hidden debug panel
- exposes base URL, status, timeline, frame, bounds, layers, etc.

### `app.part2.js`
This is the newer chat module and is intentionally self-contained.

Responsibilities:
- owns chat UI rendering inside the dock drawer
- reads JWT token from localStorage
- polls room-based chat endpoints
- sends messages to room-based endpoint
- tracks last seen cursor
- renders in-panel messages
- renders kill-feed overlay
- plays a beep for unseen messages from other users
- adds notification dot to the dock chat button

This file is important because it means the **chat system is already split away from `app.js`** even though most other features are not.

### `server.js`
- Express static server
- serves repo root as static site
- disables cache for critical files (`app.js`, `index.html`, `style.css`)
- serves `index.html` for `/`

This explains how the frontend runs inside Railway instead of pure GitHub Pages.

---

## 5. Backend architecture

## 5.1 Runtime stack

The backend is a **FastAPI** application with a generated analytics/data layer and an operational/community layer.

Main runtime modules:
- `main.py`
- `build_hotspot.py`
- `chat.py`
- `core.py`

## 5.2 Backend files and responsibilities

### `main.py`
`main.py` is the current runtime entrypoint.

It does all of the following:
- configures paths and environment defaults
- configures FastAPI + CORS
- manages frame generation state
- initializes schema
- optionally seeds an admin user
- auto-detects existing frames on startup
- auto-starts frame generation if data + zones exist but frames do not
- serves frame/timeline/status routes
- serves auth/profile routes
- serves presence routes
- serves police/pickup routes
- serves admin routes
- still contains **legacy chat endpoints**
- also includes the **new room-based chat router** from `chat.py`

### `build_hotspot.py`
This is the batch hotspot generator.

Input expectations:
- one or more `*.parquet` trip files in `/data`
- `taxi_zones.geojson` in `/data`

Processing model:
- reads parquet through DuckDB
- bins data into 20-minute windows (default)
- computes a Mon-based day-of-week timeline
- uses percentile-based ranking instead of naive min/max scaling
- weights volume higher than pay
- writes frame artifacts to `/data/frames`

Output shape:
- `/data/frames/timeline.json`
- `/data/frames/frame_000000.json`, etc.

Each polygon feature includes at least:
- `LocationID`
- `zone_name`
- `borough`
- `rating`
- `bucket`
- `pickups`
- `avg_driver_pay`
- `style.fillColor`

### `core.py`
This file provides shared runtime utilities used by `main.py` and `chat.py`.

Responsibilities:
- selects database backend
  - Postgres when `DATABASE_URL` / `POSTGRES_URL` exists
  - SQLite fallback otherwise
- opens DB connections
- adapts SQL placeholders for Postgres vs SQLite
- issues DB queries and writes
- implements JWT signing/verification
- implements PBKDF2 password hashing
- resolves the current user from bearer token

### `chat.py`
This is the **newer** room-based chat API.

Routes:
- `GET /chat/rooms/{room}`
- `POST /chat/rooms/{room}`

Features:
- room names
- `after` cursor support by id or ISO timestamp
- rate limiting (2 seconds per user)
- normalized output structure for the new frontend chat module

### `db.py`, `models.py`, `users.py`, `presence.py`, `events.py`, `security.py`
These files represent an alternate / older / parallel architecture based on SQLAlchemy-style models and modular routers.

Important current-state note:
- they are **present in the repo**
- but `main.py` currently does **not** include these routers into the live app
- the live runtime is still centered on `main.py` + `core.py` + `chat.py`

That makes them documentation-relevant, but not the primary source of runtime truth.

---

## 6. Storage architecture

## 6.1 Persistent volume

The backend volume is the file-storage side of the system.

Verified and code-confirmed volume responsibilities:
- stores parquet inputs
- stores `taxi_zones.geojson`
- stores generated frames
- stores `timeline.json`
- stores temporary DuckDB spill directory (`duckdb_tmp`)
- stores generation lock file (`.generate.lock`)

Important directories/paths in code:
- `DATA_DIR` default: `/data`
- `FRAMES_DIR` default: `/data/frames`
- timeline path: `/data/frames/timeline.json`

## 6.2 Postgres database

The Postgres database is the operational/community store in the live architecture.

From screenshots and runtime schema code, it stores at least:
- users
- presence
- events
- pickup_logs
- chat_messages

Role separation:
- **volume** = large raw inputs + generated frame artifacts
- **Postgres** = user/community/operational data

This is the correct separation for the current product.

---

## 7. Data pipeline and application flow

## 7.1 Build/generation flow

1. Backend starts.
2. `startup()` creates `DATA_DIR` and `FRAMES_DIR`.
3. Backend initializes schema.
4. Backend checks if frames already exist.
5. If frames exist, `/timeline` and `/frame/{idx}` are immediately usable.
6. If frames do not exist, but zone geometry + parquet files are present, backend starts generation automatically.
7. `build_hotspot.py` reads parquet files and zone geometry.
8. DuckDB computes per-window hotspot metrics.
9. Backend writes timeline + frame JSON files to the volume.
10. Frontend can then consume them.

## 7.2 Frontend boot flow

1. Railway serves `index.html`.
2. `index.html` sets `window.API_BASE`.
3. `app.js` boots and configures the map.
4. Frontend loads `/status` for sanity checks.
5. Frontend loads `/timeline`.
6. Frontend chooses the closest NYC-local frame to the current time.
7. Frontend loads `/frame/{idx}`.
8. Polygons + labels render.
9. Weather, slider, dock, recommendation engine, and community features boot.
10. If signed in, presence + pickup overlay + chat become active.

## 7.3 Presence flow

1. User signs in and receives bearer token.
2. Frontend obtains geolocation.
3. Frontend periodically sends `/presence/update`.
4. Backend accepts updates even if ghost mode is enabled.
5. `/presence/all` excludes ghosted users from shared results.
6. Frontend renders visible drivers.

## 7.4 Community trip flow

1. Driver presses `Record Trip`.
2. Frontend sends `/events/pickup` with location + zone context.
3. Backend writes to `pickup_logs` and also writes an event row of type `pickup`.
4. Frontend requests `/events/pickups/recent`.
5. Overlay is rendered as heatmap + circles with popup details.

## 7.5 Chat flow

1. Frontend chat module reads JWT from localStorage.
2. `app.part2.js` polls `GET /chat/rooms/global`.
3. New messages are shown in panel or kill feed.
4. Sending uses `POST /chat/rooms/global`.
5. Backend rate-limits sends and stores messages in `chat_messages`.

---

## 8. API surface (current runtime)

## Core/data endpoints
- `GET /`
- `GET /status`
- `GET /generate`
- `GET /generate_status`
- `GET /timeline`
- `GET /frame/{idx}`
- `POST /upload_zones_geojson`
- `POST /upload_parquet`

## Auth/profile endpoints
- `POST /auth/signup`
- `POST /auth/login`
- `GET /me`
- `POST /me/update`
- `POST /me/change_password`
- `POST /me/delete_account`

## Presence endpoints
- `POST /presence/update`
- `GET /presence/all`

## Events / community endpoints
- `POST /events/police`
- `GET /events/police`
- `POST /events/pickup`
- `GET /events/pickups/recent`

## Chat endpoints
### New room-based endpoints
- `GET /chat/rooms/{room}`
- `POST /chat/rooms/{room}`

### Legacy endpoints still present in `main.py`
- `POST /chat/send`
- `GET /chat/recent`
- `GET /chat/since`

## Admin endpoints
- `GET /admin/users`
- `POST /admin/users/disable`
- `POST /admin/users/reset_password`

---

## 9. Current runtime truths that matter

## 9.1 The frontend is currently Railway-served
This is a major documentation correction.

The repo name still references GitHub Pages, but the actual deployed frontend currently runs through Railway using Express/Node.

## 9.2 The backend is currently Postgres-backed in production
The code supports SQLite fallback, but the live Railway environment has a Postgres attachment and `DATABASE_URL` is present.

So the production runtime truth is:
- Postgres primary
- SQLite fallback only

## 9.3 The volume is essential
The map cannot produce hotspot frames without the volume because that is where parquet files and zone geometry live.

## 9.4 `main.py` is still too large
The current backend runtime works, but it is heavily centralized.

## 9.5 There are parallel backend architectures in the repo
The repo contains both:
- the actual live `main.py`-centric runtime
- a partial modular SQLAlchemy-style architecture that is not currently wired into the live app

That creates documentation and maintenance confusion.

## 9.6 Chat exists in two generations
- old/legacy flat chat endpoints remain in `main.py`
- new room-based chat endpoints live in `chat.py`
- frontend chat uses the room-based version

The product works best if the room-based system becomes the single source of truth.

---

## 10. Documentation drift and what was wrong in the old architecture file

The previous `arquitecture.md` is partially useful, but it is now outdated in several important ways.

Incorrect or stale items in the old doc:
- says frontend repo is only GitHub Pages
- says `index.html` is Leaflet-based
- says polygon rendering is Leaflet
- says backend storage is SQLite-centered
- omits `app.part2.js`
- omits `server.js`, `package.json`, and frontend `Procfile`
- does not reflect current Railway frontend deployment
- does not explain the split between Postgres and volume
- does not document the duplicate legacy/new chat systems
- does not document the alternate unused backend modules clearly

---

## 11. Risks and technical debt

## High-risk areas
- `app.js` is large and change-sensitive
- `main.py` is large and mixes many responsibilities
- duplicate backend architecture files can cause confusion
- duplicate chat generations increase maintenance risk
- map correctness depends on the volume being healthy and frame generation succeeding
- presence accuracy is a product-critical trust feature

## Product-critical invariants that should not be broken
- real driver markers must stay pinned to true coordinates
- ghost mode must hide the user from the map, not break their chat/event abilities
- timeline must stay aligned to NYC time
- 20-minute bin logic must stay consistent between backend generator and frontend slider assumptions
- pickup overlay must reflect community data, not synthetic approximations

---

## 12. Suggested next documentation tasks

## P0
- Replace the current repo `arquitecture.md` with this updated version
- Add a separate `API-CONTRACT.md` documenting every endpoint, payload, and response shape
- Add a `DEPLOYMENT.md` with Railway env names, domains, variables, and volume/database responsibilities

## P1
- Add `DATA-PIPELINE.md` for parquet → DuckDB → frame JSON generation
- Add `COMMUNITY-SYSTEM.md` for auth, presence, ghost mode, police, pickup logs, and chat
- Add `FRONTEND-MODULES.md` describing which features live in `app.js` vs `app.part2.js`

## P2
- Add a small architecture decision record explaining why runtime is Railway + Postgres + volume instead of GitHub Pages-only
- Add an explicit note about whether the repo should keep the filename `arquitecture.md` or rename it later to `architecture.md`

---

## 13. Best current mental model

The simplest accurate mental model of this map is:

**Frontend Railway service**
- serves static browser app
- points to backend with `API_BASE`
- renders MapLibre map, slider, presence, events, radio, chat UI

**Backend Railway service**
- serves FastAPI endpoints
- generates hotspot frames from parquet + zone geometry
- stores operational state in Postgres
- stores data inputs and frame artifacts on the volume

**Volume**
- parquet + zone geojson + frames

**Postgres**
- users + presence + events + pickup logs + chat messages

That is the current verified production architecture.
