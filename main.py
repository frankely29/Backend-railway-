from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from pathlib import Path
import traceback

from build_hotspots import build_hotspots_json

app = FastAPI()

DATA_DIR = Path("/data")            # IMPORTANT: Railway volume mount
OUT_PATH = DATA_DIR / "hotspots_20min.json"  # store output in volume too (persistent)


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/status")
def status():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    parquets = sorted([p.name for p in DATA_DIR.glob("fhvhv_tripdata_*.parquet")])
    has_output = OUT_PATH.exists()
    return {
        "status": "ok",
        "data_dir": str(DATA_DIR),
        "parquets": parquets,
        "has_output": has_output,
        "output_mb": round(OUT_PATH.stat().st_size / 1024 / 1024, 2) if has_output else 0,
    }


@app.post("/upload_parquet")
async def upload_parquet(file: UploadFile = File(...)):
    """
    Upload .parquet into the Railway volume at /data (persistent).
    """
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        out_path = DATA_DIR / file.filename
        content = await file.read()
        out_path.write_bytes(content)
        return {"saved": str(out_path), "size_mb": round(len(content) / 1024 / 1024, 2)}
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.post("/generate")
def generate(
    bin_minutes: int = 20,
    min_trips_per_window: int = 10,
):
    """
    Reads parquet files from /data and generates hotspots_20min.json into /data (persistent).
    """
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        parquets = sorted(DATA_DIR.glob("fhvhv_tripdata_*.parquet"))
        if not parquets:
            return JSONResponse(
                {"error": "No parquet files found in /data. Upload first via /upload_parquet."},
                status_code=400,
            )

        build_hotspots_json(
            parquet_files=parquets,
            out_path=OUT_PATH,
            bin_minutes=bin_minutes,
            min_trips_per_window=min_trips_per_window,
        )

        return {
            "ok": True,
            "output": OUT_PATH.name,
            "size_mb": round(OUT_PATH.stat().st_size / 1024 / 1024, 2),
        }
    except Exception as e:
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()}, status_code=500)


@app.get("/hotspots_20min.json")
def get_hotspots():
    if not OUT_PATH.exists():
        return JSONResponse(
            {"error": "hotspots_20min.json not generated yet. Call POST /generate first."},
            status_code=404,
        )
    return FileResponse(str(OUT_PATH), media_type="application/json", filename="hotspots_20min.json")