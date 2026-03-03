from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, Header
from sqlalchemy.orm import Session
from db import get_db
from models import PoliceReport, PickupLog
from users import get_user_id_from_auth

router = APIRouter()

POLICE_TTL_MIN = 30

@router.post("/events/police")
def report_police(payload: dict, authorization: str | None = Header(default=None), db: Session = Depends(get_db)):
    uid = get_user_id_from_auth(authorization)
    lat = float(payload.get("lat"))
    lng = float(payload.get("lng"))
    zone_id = payload.get("zone_id", None)

    now = datetime.now(timezone.utc)
    r = PoliceReport(
        user_id=uid,
        lat=lat,
        lng=lng,
        zone_id=zone_id,
        created_at=now,
        expires_at=now + timedelta(minutes=POLICE_TTL_MIN),
    )
    db.add(r)
    db.commit()
    return {"ok": True}

@router.get("/events/police")
def get_police(max_age_min: int = 30, db: Session = Depends(get_db)):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_min)
    rows = (
        db.query(PoliceReport)
        .filter(PoliceReport.created_at >= cutoff)
        .filter(PoliceReport.expires_at >= datetime.now(timezone.utc))
        .all()
    )
    return [{
        "id": r.id,
        "lat": r.lat,
        "lng": r.lng,
        "zone_id": r.zone_id,
        "created_at": r.created_at.isoformat(),
        "expires_at": r.expires_at.isoformat(),
    } for r in rows]

@router.post("/events/pickup")
def log_pickup(payload: dict, authorization: str | None = Header(default=None), db: Session = Depends(get_db)):
    uid = get_user_id_from_auth(authorization)
    zone_id = str(payload.get("zone_id"))
    lat = float(payload.get("lat"))
    lng = float(payload.get("lng"))

    p = PickupLog(user_id=uid, zone_id=zone_id, lat=lat, lng=lng)
    db.add(p)
    db.commit()
    return {"ok": True}