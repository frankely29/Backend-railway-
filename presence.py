from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session
from db import get_db
from models import Presence, User
from users import get_user_id_from_auth

router = APIRouter()

@router.post("/presence/update")
def presence_update(payload: dict, authorization: str | None = Header(default=None), db: Session = Depends(get_db)):
    uid = get_user_id_from_auth(authorization)

    lat = float(payload.get("lat"))
    lng = float(payload.get("lng"))
    heading = payload.get("heading", None)
    speed_mps = payload.get("speed_mps", None)

    now = datetime.now(timezone.utc)
    row = db.query(Presence).filter(Presence.user_id == uid).first()
    if not row:
        row = Presence(user_id=uid, lat=lat, lng=lng, heading=heading, speed_mps=speed_mps, updated_at=now)
        db.add(row)
    else:
        row.lat = lat
        row.lng = lng
        row.heading = heading
        row.speed_mps = speed_mps
        row.updated_at = now

    db.commit()
    return {"ok": True}

@router.get("/presence/nearby")
def presence_nearby(max_age_sec: int = 20, db: Session = Depends(get_db)):
    # public list: shows everyone who updated recently
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=max_age_sec)

    rows = (
        db.query(Presence, User)
        .join(User, User.id == Presence.user_id)
        .filter(Presence.updated_at >= cutoff)
        .all()
    )

    out = []
    for p, u in rows:
        out.append({
            "user_id": u.id,
            "display_name": u.display_name,
            "avatar_url": u.avatar_url,
            "lat": p.lat,
            "lng": p.lng,
            "heading": p.heading,
            "updated_at": p.updated_at.isoformat(),
        })
    return out