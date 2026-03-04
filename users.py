# users.py
from fastapi import APIRouter, Depends, HTTPException, Header, UploadFile, File
from sqlalchemy.orm import Session
from db import get_db
from models import User, SubscriptionState
from security import hash_password, verify_password, create_token, decode_token
import os
from pathlib import Path
from PIL import Image
import uuid

router = APIRouter()
AVATAR_DIR = Path("/data/avatars")
AVATAR_DIR.mkdir(parents=True, exist_ok=True)
MAX_SIZE = (300, 300)

def get_user_id_from_auth(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing token")
    token = authorization.split(" ", 1)[1]
    try:
        return decode_token(token)
    except:
        raise HTTPException(401, "Invalid token")

@router.post("/auth/signup")
def signup(payload: dict, db: Session = Depends(get_db)):
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password") or ""
    display_name = (payload.get("display_name") or "").strip()

    if not email or "@" not in email: raise HTTPException(400, "Invalid email")
    if len(password) < 8: raise HTTPException(400, "Password min 8 chars")
    if not display_name: raise HTTPException(400, "Display name required")

    if db.query(User).filter(User.email == email).first():
        raise HTTPException(400, "Email already in use")

    u = User(
        email=email,
        password_hash=hash_password(password),
        display_name=display_name,
        avatar_url=None
    )
    db.add(u)
    db.flush()

    now = datetime.now(timezone.utc)
    s = SubscriptionState(user_id=u.id, trial_start=now, trial_end=now + timedelta(days=7), status="trial")
    db.add(s)
    db.commit()

    return {"token": create_token(u.id), "user_id": u.id}

@router.post("/auth/login")
def login(payload: dict, db: Session = Depends(get_db)):
    email = (payload.get("email") or "").strip().lower()
    u = db.query(User).filter(User.email == email).first()
    if not u or not verify_password(payload.get("password"), u.password_hash):
        raise HTTPException(401, "Bad credentials")
    return {"token": create_token(u.id), "user_id": u.id}

# === NEW: Avatar upload ===
@router.post("/me/avatar")
def upload_avatar(file: UploadFile = File(...), authorization: str = Header(), db: Session = Depends(get_db)):
    uid = get_user_id_from_auth(authorization)
    user = db.query(User).filter(User.id == uid).first()
    if not user: raise HTTPException(401, "User not found")

    if not file.content_type.startswith("image/"):
        raise HTTPException(400, "Image only")

    ext = file.filename.split(".")[-1].lower()
    if ext not in ["jpg", "jpeg", "png"]: raise HTTPException(400, "jpg/png only")

    filename = f"{uid}_{uuid.uuid4().hex[:8]}.jpg"
    save_path = AVATAR_DIR / filename

    img = Image.open(file.file).convert("RGB")
    img.thumbnail(MAX_SIZE)
    img.save(save_path, "JPEG", quality=85)

    user.avatar_url = f"/avatars/{filename}"   # we'll serve this in main.py
    db.commit()
    return {"avatar_url": user.avatar_url}

# === Profile (for map + ghost mode) ===
@router.get("/me")
def get_me(authorization: str = Header(), db: Session = Depends(get_db)):
    uid = get_user_id_from_auth(authorization)
    u = db.query(User).filter(User.id == uid).first()
    sub = db.query(SubscriptionState).filter_by(user_id=uid).first()
    return {
        "id": u.id,
        "display_name": u.display_name,
        "avatar_url": u.avatar_url,
        "ghost_mode": u.ghost_mode,          # we'll add this column
        "trial_active": sub and datetime.now(timezone.utc) <= sub.trial_end
    }

@router.patch("/me")
def update_me(payload: dict, authorization: str = Header(), db: Session = Depends(get_db)):
    uid = get_user_id_from_auth(authorization)
    u = db.query(User).filter(User.id == uid).first()
    if "ghost_mode" in payload:
        u.ghost_mode = bool(payload["ghost_mode"])
    if "display_name" in payload:
        u.display_name = payload["display_name"].strip()
    db.commit()
    return {"ok": True}