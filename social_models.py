"""Request and response shapes for the driver network."""
from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field

MAX_BODY_CHARS = 2000
MAX_CITY_CHARS = 80
MAX_ZONE_NAME_CHARS = 120


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
    city: Optional[str] = None
    avatar_url: Optional[str] = None


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


class Profile(BaseModel):
    user_id: int
    display_name: str
    city: Optional[str] = None
    avatar_url: Optional[str] = None
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


class OkResponse(BaseModel):
    ok: bool = True
