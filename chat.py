from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session
from db import get_db
from models import ChatMessage, User
from users import get_user_id_from_auth

router = APIRouter()

@router.post("/chat/send")
def chat_send(payload: dict, authorization: str | None = Header(default=None), db: Session = Depends(get_db)):
    uid = get_user_id_from_auth(authorization)
    room = (payload.get("room") or "global").strip()
    text = (payload.get("text") or "").strip()
    if not text:
        raise HTTPException(400, "text required")
    if len(text) > 500:
        raise HTTPException(400, "text too long")

    msg = ChatMessage(room=room, user_id=uid, text=text)
    db.add(msg)
    db.commit()
    return {"id": msg.id, "created_at": msg.created_at.isoformat()}

@router.get("/chat/since")
def chat_since(room: str = "global", since_id: str | None = None, limit: int = 50, db: Session = Depends(get_db)):
    q = db.query(ChatMessage).filter(ChatMessage.room == room).order_by(ChatMessage.created_at.asc())
    if since_id:
        # simple: fetch msg timestamp of since_id, then return newer
        anchor = db.query(ChatMessage).filter(ChatMessage.id == since_id).first()
        if anchor:
            q = q.filter(ChatMessage.created_at > anchor.created_at)

    msgs = q.limit(min(200, max(1, limit))).all()

    # join user display name (simple)
    user_ids = {m.user_id for m in msgs}
    users = db.query(User).filter(User.id.in_(list(user_ids))).all()
    umap = {u.id: u for u in users}

    out = []
    for m in msgs:
        u = umap.get(m.user_id)
        out.append({
            "id": m.id,
            "room": m.room,
            "user_id": m.user_id,
            "display_name": (u.display_name if u else "Unknown"),
            "avatar_url": (u.avatar_url if u else None),
            "text": m.text,
            "created_at": m.created_at.isoformat(),
        })
    return out