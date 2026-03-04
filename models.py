import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import Column, String, DateTime, Float, Integer, ForeignKey, Text
from sqlalchemy.orm import relationship
from db import Base

def utcnow():
    return datetime.now(timezone.utc)

class User(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    avatar_url = Column(String, nullable=True)
    role = Column(String, nullable=False, default="user")
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    last_login_at = Column(DateTime(timezone=True), nullable=True)

class SubscriptionState(Base):
    __tablename__ = "subscription_state"
    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    trial_start = Column(DateTime(timezone=True), nullable=False)
    trial_end = Column(DateTime(timezone=True), nullable=False)
    status = Column(String, nullable=False, default="trial")  # trial|active|expired

class Presence(Base):
    __tablename__ = "presence"
    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    heading = Column(Float, nullable=True)
    speed_mps = Column(Float, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)

class PoliceReport(Base):
    __tablename__ = "police_reports"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    zone_id = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)

class PickupLog(Base):
    __tablename__ = "pickup_logs"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    zone_id = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)

class DriverSession(Base):
    __tablename__ = "driver_sessions"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    start_ts = Column(DateTime(timezone=True), nullable=False)
    end_ts = Column(DateTime(timezone=True), nullable=True)
    seconds = Column(Integer, nullable=True)
    auto_reason = Column(String, nullable=False, default="movement")

class ChatMessage(Base):
    __tablename__ = "chat_messages"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    room = Column(String, nullable=False, default="global", index=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)