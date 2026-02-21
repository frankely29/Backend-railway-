# NYC TLC FHV Hotspot Map — Architecture & Data Contract

## Goal (Non-negotiable)
This map is for NYC TLC FHV (Uber/Lyft) drivers.
All map visuals and recommendations are strictly data-driven from Railway backend outputs.

## Repositories
### 1) Frontend (GitHub Pages)
- Repo: frontend pages
- Files:
  - index.html
  - app.js
- Hosting:
  - GitHub Pages serves static UI only.
- Data source:
  - Frontend reads ALL demand data from Railway API.
  - No local mock data is allowed in production.

### 2) Backend (Railway)
- Repo: backend
- Files:
  - main.py (FastAPI HTTP API)
  - build_hotspot.py (DuckDB aggregation + frame writer)
  - requirements.txt
  - Procfile
- Storage:
  - Railway Volume mounted at /data
  - /data persists across deploys

## Data on Railway Volume (/data)
Required:
- /data/taxi_zones.geojson
  - GeoJSON FeatureCollection of NYC Taxi Zones
  - Each Feature must contain properties.LocationID (int)
  - Used for polygon geometry lookup

- /data/fhvhv_tripdata_YYYY-MM.parquet
  - TLC FHVHV trip parquet files (Uber/Lyft high volume)

Generated:
- /data/frames/timeline.json
- /data/frames/frame_000000.json ... frame_000503.json
- /data/duckdb_tmp/ (DuckDB spill directory)

## API Endpoints (Backend)
Base URL:
- https://<railway-app>.up.railway.app

### Health
GET /status
Returns:
- zones_present (bool)
- parquets (list)
- has_timeline (bool)
- generate_status (job state)

### Uploads
POST /upload_zones_geojson
- multipart/form-data file=<taxi_zones.geojson>
Writes:
- /data/taxi_zones.geojson

POST /upload_parquet
- multipart/form-data file=<fhvhv_tripdata_YYYY-MM.parquet>
Writes:
- /data/<filename>

### Generation (Async)
GET /generate?bin_minutes=20&min_trips_per_window=25
Behavior:
- Returns 202 immediately with {state:"started"}
- Starts background job:
  - Reads all matching parquets in /data
  - Aggregates to 20-minute bins by NYC day-of-week + time
  - Writes frames to /data/frames/

GET /generate_status
Returns:
- state: idle | running | done | error
- if error: trace included

### Data serving
GET /timeline
- Returns /data/frames/timeline.json
- 404 if not generated yet

GET /frame/{idx}
- Returns /data/frames/frame_{idx:06d}.json
- 404 if out of range or not generated

## Timeline & Frames Contract
### /timeline
{
  "timeline": ["YYYY-MM-DDTHH:MM:SS", ...],
  "count": <int>
}

### /frame/{idx}
{
  "time": "YYYY-MM-DDTHH:MM:SS",
  "polygons": {
    "type":"FeatureCollection",
    "features":[
      {
        "type":"Feature",
        "geometry": <GeoJSON Polygon or MultiPolygon>,
        "properties":{
          "LocationID": <int>,
          "rating": <1..100>,
          "pickups": <int>,
          "avg_driver_pay": <float|null>,
          "style":{
            "fillColor":"#RRGGBB",
            "fillOpacity": 0.82,
            "weight": 0
          }
        }
      }
    ]
  }
}

## Rating Rules (Strict)
rating is always 1..100.

Color buckets:
- Green (Best): rating >= 80
- Blue (Medium): rating >= 60
- Sky (Normal): rating >= 40
- Red (Avoid): rating < 40

## Time Rules (Strict)
- Frontend slider label is NYC local time.
- Slider initializes to the closest NYC 20-minute window ("week wrap" supported).
- Backend bins are 20-minute windows by NYC day-of-week and minute-of-day.

## Known Failure Modes & Fixes
1) /timeline says "not ready"
- Run /generate and monitor /generate_status until done.

2) Upload zones shows size_mb = 0
- Wrong file uploaded (e.g. .shx)
- Must upload a real .geojson with polygons.

3) 502 during generate
- Usually timeout or memory.
- Use async /generate implementation and streaming row writes to frames.

4) Huge single JSON outputs
- Avoid single /hotspots_20min.json > 100MB.
- Use per-frame files in /data/frames/ instead.

## Rebuild procedure (from scratch)
1) Upload taxi_zones.geojson
2) Upload fhvhv_tripdata parquet(s)
3) Call /generate
4) Verify /timeline + /frame/0
5) Load GitHub Pages frontend
