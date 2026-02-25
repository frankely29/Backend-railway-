# ======================================================================================
# Friends / Presence (simple realtime sharing)
# - Users enter a username once; stored on client
# - Client sends location/heading updates
# - Server keeps an in-memory active list; users expire after 30 minutes inactivity
# ======================================================================================
from pydantic import BaseModel, Field
from typing import Dict, Optional
import time as _time

PRESENCE_TTL_SECONDS = 30 * 60  # 30 minutes

class PresenceJoin(BaseModel):
    client_id: str = Field(..., min_length=6, max_length=80)
    username: str = Field(..., min_length=1, max_length=24)

class PresenceUpdate(BaseModel):
    client_id: str = Field(..., min_length=6, max_length=80)
    lat: float
    lng: float
    heading: Optional[float] = None  # degrees 0..360, optional
    speed_mps: Optional[float] = None

class PresenceSignOut(BaseModel):
    client_id: str = Field(..., min_length=6, max_length=80)

# client_id -> record
_presence: Dict[str, dict] = {}

def _presence_prune(now: float):
    dead = []
    for cid, rec in _presence.items():
        if now - float(rec.get("last_seen", 0)) > PRESENCE_TTL_SECONDS:
            dead.append(cid)
    for cid in dead:
        _presence.pop(cid, None)

@app.post("/presence/join")
async def presence_join(p: PresenceJoin):
    now = _time.time()
    _presence_prune(now)

    username = p.username.strip()
    if not username:
        raise HTTPException(status_code=400, detail="username required")

    _presence[p.client_id] = {
        "client_id": p.client_id,
        "username": username[:24],
        "lat": None,
        "lng": None,
        "heading": None,
        "speed_mps": None,
        "last_seen": now,
    }
    return {"ok": True, "ttl_seconds": PRESENCE_TTL_SECONDS}

@app.post("/presence/update")
async def presence_update(u: PresenceUpdate):
    now = _time.time()
    _presence_prune(now)

    rec = _presence.get(u.client_id)
    if not rec:
        rec = {
            "client_id": u.client_id,
            "username": "Driver",
            "lat": None,
            "lng": None,
            "heading": None,
            "speed_mps": None,
            "last_seen": now,
        }
        _presence[u.client_id] = rec

    rec["lat"] = float(u.lat)
    rec["lng"] = float(u.lng)
    rec["heading"] = None if u.heading is None else float(u.heading)
    rec["speed_mps"] = None if u.speed_mps is None else float(u.speed_mps)
    rec["last_seen"] = now

    return {"ok": True}

@app.post("/presence/signout")
async def presence_signout(p: PresenceSignOut):
    _presence.pop(p.client_id, None)
    return {"ok": True}

@app.get("/presence/users")
async def presence_users(client_id: Optional[str] = None):
    now = _time.time()
    _presence_prune(now)

    out = []
    for cid, rec in _presence.items():
        if client_id and cid == client_id:
            continue
        if rec.get("lat") is None or rec.get("lng") is None:
            continue
        out.append({
            "client_id": cid,
            "username": rec.get("username") or "Driver",
            "lat": rec["lat"],
            "lng": rec["lng"],
            "heading": rec.get("heading"),
            "speed_mps": rec.get("speed_mps"),
            "last_seen": rec.get("last_seen"),
        })

    return {"ttl_seconds": PRESENCE_TTL_SECONDS, "users": out}