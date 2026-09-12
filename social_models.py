"""Request and response shapes for the driver network."""
from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field

MAX_BODY_CHARS = 2000
MAX_CITY_CHARS = 80
MAX_ZONE_NAME_CHARS = 120
MAX_HANDLE_CHARS = 20
MAX_BIO_CHARS = 200


class FeedScope(str, Enum):
    """Which slice of the network a feed request wants.

    `following` includes the viewer's own posts. A timeline that hides what you
    just wrote looks broken, and "did it post?" is the first thing anyone checks.
    """

    following = "following"
    city = "city"
    everyone = "everyone"


class CreatePostPayload(BaseModel):
    body: str = Field(default="", max_length=MAX_BODY_CHARS)
    lat: Optional[float] = None
    lng: Optional[float] = None
    zone_name: Optional[str] = Field(default=None, max_length=MAX_ZONE_NAME_CHARS)
    zone_rating: Optional[int] = Field(default=None, ge=0, le=100)


class SetCityPayload(BaseModel):
    city: str = Field(default="", max_length=MAX_CITY_CHARS)


class PostAuthor(BaseModel):
    user_id: int
    display_name: str
    handle: Optional[str] = None
    city: Optional[str] = None
    avatar_url: Optional[str] = None
    # Who is talking. A driver weighing "the lot is moving" wants to know the
    # person saying it has actually driven, and on what. Both stay optional:
    # a new driver with no trips logged and no platform set still posts, and a
    # cold progression cache must cost a badge, never the feed.
    level: Optional[int] = None
    platforms: List[str] = []


class Post(BaseModel):
    id: int
    author: PostAuthor
    body: str
    image_url: Optional[str] = None
    image_thumb_url: Optional[str] = None
    city: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    zone_name: Optional[str] = None
    zone_rating: Optional[int] = None
    like_count: int = 0
    liked_by_me: bool = False
    mine: bool = False
    created_at: int


class FeedResponse(BaseModel):
    ok: bool = True
    scope: FeedScope
    items: List[Post] = []
    next_before_id: Optional[int] = None


class PostResponse(BaseModel):
    ok: bool = True
    post: Post


class LikeResponse(BaseModel):
    ok: bool = True
    post_id: int
    like_count: int
    liked_by_me: bool


class FollowResponse(BaseModel):
    ok: bool = True
    user_id: int
    following: bool
    follower_count: int


class Reputation(BaseModel):
    """Driving history as social proof — the thing a generic photo app cannot
    show. Every field is optional: a cold badge cache must not break a profile."""

    level: Optional[int] = None
    rank_name: Optional[str] = None
    title: Optional[str] = None
    badge_code: Optional[str] = None
    lifetime_miles: Optional[float] = None
    lifetime_hours: Optional[float] = None
    trips_logged: Optional[int] = None


class SetHandlePayload(BaseModel):
    handle: str = Field(min_length=1, max_length=MAX_HANDLE_CHARS + 1)


class UpdateIdentityPayload(BaseModel):
    """Patch semantics — an omitted field is left alone, an explicit null clears
    it. That distinction is why every field defaults to None AND the route reads
    `model_fields_set` rather than the values."""

    bio: Optional[str] = Field(default=None, max_length=MAX_BIO_CHARS)
    platforms: Optional[List[str]] = None
    vehicle_type: Optional[str] = None
    driving_since_year: Optional[int] = None


class HandleAvailability(BaseModel):
    ok: bool = True
    handle: str
    available: bool
    reason: Optional[str] = None


class Profile(BaseModel):
    user_id: int
    display_name: str
    handle: Optional[str] = None
    city: Optional[str] = None
    avatar_url: Optional[str] = None
    bio: Optional[str] = None
    platforms: List[str] = []
    vehicle_type: Optional[str] = None
    driving_since_year: Optional[int] = None
    reputation: Optional[Reputation] = None
    post_count: int = 0
    follower_count: int = 0
    # Only ever populated for the profile's owner. The product decision is that
    # how many people YOU follow is nobody else's business; how many follow you
    # is public. Two different fields rather than one nullable number, so a
    # client cannot accidentally render the private one.
    following_count: Optional[int] = None
    followed_by_me: bool = False
    is_me: bool = False


class ProfileResponse(BaseModel):
    ok: bool = True
    profile: Profile


class IdentityOptionsResponse(BaseModel):
    """The closed sets, served rather than hard-coded in the client -- otherwise
    adding a platform means shipping a frontend release."""

    ok: bool = True
    platforms: List[str] = []
    vehicle_types: List[str] = []


class ReportPayload(BaseModel):
    target_type: str
    target_id: int
    reason: str
    note: Optional[str] = Field(default=None, max_length=500)


class ReportCreated(BaseModel):
    ok: bool = True
    report_id: int
    # True when this person already had an open report on this thing. Reported
    # as success, not as an error: tapping Report twice is not a mistake worth
    # showing someone an error for.
    duplicate: bool = False


class RelationshipResponse(BaseModel):
    ok: bool = True
    user_id: int
    blocked: Optional[bool] = None
    muted: Optional[bool] = None


class MutedOrBlockedUser(BaseModel):
    user_id: int
    display_name: str
    handle: Optional[str] = None
    created_at: int


class RelationshipListResponse(BaseModel):
    ok: bool = True
    items: List[MutedOrBlockedUser] = []


class ReportOptionsResponse(BaseModel):
    ok: bool = True
    reasons: List[str] = []
    targets: List[str] = []


class ReportItem(BaseModel):
    id: int
    reporter_id: int
    reporter_name: Optional[str] = None
    target_type: str
    target_id: int
    target_user_id: Optional[int] = None
    target_name: Optional[str] = None
    target_handle: Optional[str] = None
    reason: str
    note: Optional[str] = None
    status: str
    created_at: int
    resolved_at: Optional[int] = None
    resolved_by: Optional[int] = None
    resolution: Optional[str] = None


class ReportQueueResponse(BaseModel):
    ok: bool = True
    items: List[ReportItem] = []
    next_before_id: Optional[int] = None
    open_count: int = 0


class ResolveReportPayload(BaseModel):
    status: str
    resolution: Optional[str] = Field(default=None, max_length=500)


class HidePostPayload(BaseModel):
    reason: Optional[str] = Field(default=None, max_length=500)


class ReportCountResponse(BaseModel):
    ok: bool = True
    open_count: int = 0


class ResolveReportResponse(BaseModel):
    ok: bool = True
    report_id: int
    status: str


class HidePostResponse(BaseModel):
    ok: bool = True
    post_id: int
    hidden: bool


class OkResponse(BaseModel):
    ok: bool = True
