from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from db import get_db
from models import User, SubscriptionState
from security import hash_password, verify_password, create_token, decode_token

router = APIRouter()

TRIAL_DAYS = 7

def get_user_id_from_auth(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    token = authorization.split(" ", 1)[1].strip()
    try:
        return decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

@router.post("/auth/signup")
def signup(payload: dict, db: Session = Depends(get_db)):
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password") or ""
    display_name = (payload.get("display_name") or "").strip()

    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email")
    if len(password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    if not display_name:
        raise HTTPException(400, "Display name required")

    exists = db.query(User).filter(User.email == email).first()
    if exists:
        raise HTTPException(400, "Email already in use")

    u = User(email=email, password_hash=hash_password(password), display_name=display_name)
    db.add(u)
    db.flush()

    now = datetime.now(timezone.utc)
    s = SubscriptionState(
        user_id=u.id,
        trial_start=now,
        trial_end=now + timedelta(days=TRIAL_DAYS),
        status="trial",
    )
    db.add(s)
    db.commit()

    token = create_token(u.id)
    return {"token": token}

@router.post("/auth/login")
def login(payload: dict, db: Session = Depends(get_db)):
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password") or ""
    u = db.query(User).filter(User.email == email).first()
    if not u or not verify_password(password, u.password_hash):
        raise HTTPException(401, "Invalid credentials")
    u.last_login_at = datetime.now(timezone.utc)
    db.commit()
    return {"token": create_token(u.id)}

@router.get("/me")
def me(authorization: str | None = Header(default=None), db: Session = Depends(get_db)):
    uid = get_user_id_from_auth(authorization)
    u = db.query(User).filter(User.id == uid).first()
    if not u:
        raise HTTPException(401, "User not found")

    sub = db.query(SubscriptionState).filter(SubscriptionState.user_id == uid).first()
    now = datetime.now(timezone.utc)
    trial_active = bool(sub and now <= sub.trial_end)

    return {
        "id": u.id,
        "email": u.email,
        "display_name": u.display_name,
        "avatar_url": u.avatar_url,
        "role": u.role,
        "trial_end": sub.trial_end.isoformat() if sub else None,
        "trial_active": trial_active,
        "subscription_status": sub.status if sub else "trial",
    }

@router.patch("/me")
def update_me(payload: dict, authorization: str | None = Header(default=None), db: Session = Depends(get_db)):
    uid = get_user_id_from_auth(authorization)
    u = db.query(User).filter(User.id == uid).first()
    if not u:
        raise HTTPException(401, "User not found")

    if "display_name" in payload:
        name = (payload.get("display_name") or "").strip()
        if not name:
            raise HTTPException(400, "display_name cannot be empty")
        u.display_name = name

    db.commit()
    return {"ok": True}