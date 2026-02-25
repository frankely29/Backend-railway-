from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import time
import threading

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================
# Presence System
# =============================

PRESENCE_TIMEOUT_MS = 30 * 60 * 1000  # 30 minutes

_presence_lock = threading.Lock()
_presence_store = {}  # key=username_lower -> data

def now_ms():
    return int(time.time() * 1000)

def normalize_username(u: str) -> str:
    return (u or "").strip().lower()

class PresencePayload(BaseModel):
    username: str
    session_token: str
    lat: float | None = None
    lng: float | None = None
    heading: float | None = None
    ts: int | None = None

def cleanup_presence():
    now = now_ms()
    dead = []
    for k, v in _presence_store.items():
        if now - v["updated_at_ms"] > PRESENCE_TIMEOUT_MS:
            dead.append(k)
    for k in dead:
        _presence_store.pop(k, None)

@app.post("/presence/signin")
def presence_signin(p: PresencePayload):
    key = normalize_username(p.username)

    with _presence_lock:
        cleanup_presence()

        _presence_store[key] = {
            "display_name": p.username.strip(),
            "session_token": p.session_token,
            "lat": None,
            "lng": None,
            "heading": None,
            "updated_at_ms": now_ms(),
        }

    return {"ok": True}

@app.post("/presence/update")
def presence_update(p: PresencePayload):
    key = normalize_username(p.username)

    with _presence_lock:
        cleanup_presence()

        user = _presence_store.get(key)
        if not user:
            return {"ok": False, "error": "not_signed_in"}

        if user["session_token"] != p.session_token:
            return {"ok": False, "error": "bad_session"}

        if p.lat is not None and p.lng is not None:
            user["lat"] = float(p.lat)
            user["lng"] = float(p.lng)

        if p.heading is not None:
            user["heading"] = float(p.heading)

        user["updated_at_ms"] = p.ts or now_ms()

    return {"ok": True}

@app.post("/presence/signout")
def presence_signout(p: PresencePayload):
    key = normalize_username(p.username)

    with _presence_lock:
        user = _presence_store.get(key)
        if not user:
            return {"ok": True}

        if user["session_token"] != p.session_token:
            return {"ok": False, "error": "bad_session"}

        _presence_store.pop(key, None)

    return {"ok": True}

@app.get("/presence/list")
def presence_list():
    with _presence_lock:
        cleanup_presence()

        users = []
        for key, v in _presence_store.items():
            if v["lat"] is not None and v["lng"] is not None:
                users.append({
                    "username": v["display_name"],
                    "lat": v["lat"],
                    "lng": v["lng"],
                    "heading": v["heading"],
                    "updated_at_ms": v["updated_at_ms"],
                })

        users.sort(key=lambda x: x["username"].lower())

        return {
            "users": users,
            "timeout_ms": PRESENCE_TIMEOUT_MS
        }