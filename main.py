import os
from typing import List

from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

from build_hotspots import build_hotspots, ensure_taxi_zones_geojson

APP_NAME = "NYC TLC Hotspot Backend"

DATA_DIR = os.getenv("DATA_DIR", "/data")
OUTPUT_NAME_DEFAULT = os.getenv("OUTPUT_NAME", "hotspots_20min.json")
TAXI_ZONES_PATH = os.path.join(DATA_DIR, "taxi_zones.geojson")

app = FastAPI(title=APP_NAME)

# Allow GitHub Pages to call Railway
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def list_parquets() -> List[str]:
    if not os.path.isdir(DATA_DIR):
        return []
    return sorted([f for f in os.listdir(DATA_DIR) if f.lower().endswith(".parquet")])

def out_path(output_name: str) -> str:
    return os.path.join(DATA_DIR, output_name)

def file_mb(path: str) -> float:
    return round(os.path.getsize(path) / (1024 * 1024), 2)

class StatusResponse(BaseModel):
    status: str
    data_dir: str
    parquets: List[str]
    has_output: bool
    output_mb: float
    output_name: str
    taxi_zones_present: bool

@app.get("/")
def root():
    return {"ok": True, "service": APP_NAME}

@app.get("/status", response_model=StatusResponse)
def status(output: str = Query(default=OUTPUT_NAME_DEFAULT)):
    op = out_path(output)
    return StatusResponse(
        status="ok",
        data_dir=DATA_DIR,
        parquets=list_parquets(),
        has_output=os.path.isfile(op),
        output_mb=file_mb(op) if os.path.isfile(op) else 0.0,
        output_name=output,
        taxi_zones_present=os.path.isfile(TAXI_ZONES_PATH),
    )

# Optional: upload files from Swagger/curl (not required if already in volume)
@app.post("/upload_parquet")
async def upload_parquet(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".parquet"):
        raise HTTPException(status_code=400, detail="File must be .parquet")

    os.makedirs(DATA_DIR, exist_ok=True)
    dest = os.path.join(DATA_DIR, file.filename)

    with open(dest, "wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)

    return {"ok": True, "saved": file.filename, "size_mb": file_mb(dest)}

@app.post("/upload_taxi_zones_geojson")
async def upload_taxi_zones_geojson(file: UploadFile = File(...)):
    if not (file.filename.lower().endswith(".geojson") or file.filename.lower().endswith(".json")):
        raise HTTPException(status_code=400, detail="File must be .geojson or .json")

    os.makedirs(DATA_DIR, exist_ok=True)
    dest = TAXI_ZONES_PATH

    with open(dest, "wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)

    return {"ok": True, "saved_as": "taxi_zones.geojson", "size_mb": file_mb(dest)}

@app.post("/generate")
def generate(
    bin_minutes: int = Query(20, ge=5, le=120),
    min_trips_per_window: int = Query(10, ge=1, le=999999),
    output: str = Query(OUTPUT_NAME_DEFAULT),

    # Bucket thresholds for rating (1..100)
    normal_lo: int = Query(40, ge=1, le=100),
    medium_lo: int = Query(60, ge=1, le=100),
    best_lo: int = Query(80, ge=1, le=100),
):
    os.makedirs(DATA_DIR, exist_ok=True)

    if not os.path.isfile(TAXI_ZONES_PATH):
        ensure_taxi_zones_geojson(DATA_DIR)

    if not os.path.isfile(TAXI_ZONES_PATH):
        raise HTTPException(
            status_code=400,
            detail="Missing taxi_zones.geojson in /data. Upload via /upload_taxi_zones_geojson."
        )

    if not list_parquets():
        raise HTTPException(status_code=400, detail="No parquet files found in /data.")

    op = out_path(output)
    result = build_hotspots(
        data_dir=DATA_DIR,
        taxi_zones_geojson_path=TAXI_ZONES_PATH,
        output_path=op,
        bin_minutes=bin_minutes,
        min_trips_per_window=min_trips_per_window,
        normal_lo=normal_lo,
        medium_lo=medium_lo,
        best_lo=best_lo,
    )
    return {
        "ok": True,
        "output": os.path.basename(op),
        "size_mb": file_mb(op),
        **result
    }

@app.get("/hotspots_20min.json")
def hotspots_default():
    p = out_path(OUTPUT_NAME_DEFAULT)
    if not os.path.isfile(p):
        raise HTTPException(status_code=404, detail="hotspots_20min.json not found. Call /generate first.")
    return FileResponse(p, media_type="application/json", filename="hotspots_20min.json")

@app.get("/hotspots")
def hotspots(output: str = Query(OUTPUT_NAME_DEFAULT)):
    p = out_path(output)
    if not os.path.isfile(p):
        raise HTTPException(status_code=404, detail=f"{output} not found. Call /generate first.")
    return FileResponse(p, media_type="application/json", filename=os.path.basename(p))
