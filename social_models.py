"""Request and response shapes for the driver network."""
from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field

MAX_BODY_CHARS = 2000
# Shorter than a post on purpose. A reply is a reply; anything longer is a post
# of its own, and a wall of text under someone else's photo buries the thread.
MAX_COMMENT_CHARS = 600
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
    # Declared, or Pydantic drops them on the way out.
    #
    # This is the second time: RankLadderRow shipped without its prestige
    # pair for the same reason, the service computed it correctly the whole
    # time, and every driver saw "Prestige 14" on a ladder of ten. A response
    # model is not documentation, it is a filter.
    #
    # rank_icon_key is what a client draws the crest from AND derives the
    # ladder position from -- one of thirty. `level` above is the XP engine's,
    # out of a thousand, and is not for display; it stays for the clients that
    # already read it.
    rank_icon_key: Optional[str] = None
    rank_name: Optional[str] = None
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
    comment_count: int = 0
    mine: bool = False
    created_at: int


class CreateCommentPayload(BaseModel):
    body: str = Field(min_length=1, max_length=MAX_COMMENT_CHARS)
    # The comment this one answers. Omitted or null is a reply to the post
    # itself, which is every comment written before this field existed.
    parent_id: Optional[int] = None


class ReplyTo(BaseModel):
    user_id: int
    display_name: str
    handle: Optional[str] = None


class Comment(BaseModel):
    id: int
    post_id: int
    author: PostAuthor
    body: str
    # Exactly what was answered; null for a reply to the post. The database
    # keeps the real shape and the client draws two levels, so a reply to a
    # reply-to-a-reply sits at the same indent as its parent.
    parent_id: Optional[int] = None
    # Who wrote that parent, for the "@name" on a nested reply -- at one indent
    # there is otherwise nothing to say which of the two it answered.
    reply_to: Optional[ReplyTo] = None
    mine: bool = False
    # True when the viewer can remove it: their own comment, or any comment on
    # a post they own. Sent rather than derived, so the client cannot get the
    # rule subtly wrong and offer a delete that 403s.
    can_delete: bool = False
    created_at: int


class CommentsResponse(BaseModel):
    ok: bool = True
    post_id: int
    items: List[Comment] = []
    next_after_id: Optional[int] = None
    comment_count: int = 0


class CommentResponse(BaseModel):
    ok: bool = True
    comment: Comment
    comment_count: int = 0


class CommentCountResponse(BaseModel):
    """What a delete returns: the post, and what its count is now.

    The count comes back so the client repaints from the server rather than
    decrementing its own copy -- two people deleting at once makes local
    arithmetic wrong.
    """

    ok: bool = True
    post_id: int
    comment_count: int = 0


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
    # The key the crest and the ladder position both come from. Without it a
    # profile can name a rank but cannot say which of the thirty it is.
    rank_icon_key: Optional[str] = None
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


class HideCommentResponse(BaseModel):
    ok: bool = True
    comment_id: int
    # The post too, so an admin acting from the queue can jump straight to the
    # thread the reply is in rather than looking it up.
    post_id: int
    hidden: bool


class OkResponse(BaseModel):
    ok: bool = True


# ---------------------------------------------------------------- notifications

class NotificationActor(BaseModel):
    """Deliberately not the full Profile.

    A notification row needs a face, a name and a level, and nothing else.
    Serving the whole profile here would mean the reputation lookup and the
    follower counts run once per row on a screen that is mostly one person
    appearing repeatedly.
    """
    user_id: int
    display_name: str
    handle: Optional[str] = None
    avatar_url: Optional[str] = None
    level: Optional[int] = None
    # Declared, or Pydantic drops them on the way out.
    #
    # This is the second time: RankLadderRow shipped without its prestige
    # pair for the same reason, the service computed it correctly the whole
    # time, and every driver saw "Prestige 14" on a ladder of ten. A response
    # model is not documentation, it is a filter.
    #
    # rank_icon_key is what a client draws the crest from AND derives the
    # ladder position from -- one of thirty. `level` above is the XP engine's,
    # out of a thousand, and is not for display; it stays for the clients that
    # already read it.
    rank_icon_key: Optional[str] = None
    rank_name: Optional[str] = None


class Notification(BaseModel):
    id: int
    # like | comment | reply | follow -- see NOTIFICATION_KINDS in the service.
    # A string rather than an enum so the server can add a kind without every
    # older client refusing to parse the whole page.
    kind: str
    created_at: int
    read: bool = False
    actor: NotificationActor
    # Absent for a follow, which is about a person rather than a post.
    post_id: Optional[int] = None
    comment_id: Optional[int] = None
    # The post's first line, so the row can say WHICH post without the client
    # fetching every post named on the screen.
    post_excerpt: Optional[str] = None


class NotificationsResponse(BaseModel):
    ok: bool = True
    items: List[Notification] = []
    next_before_id: Optional[int] = None
    # Sent with the page as well as on its own, so opening the screen and
    # reading the badge is one request rather than two.
    unread: int = 0


class UnreadResponse(BaseModel):
    ok: bool = True
    unread: int = 0


class MarkReadPayload(BaseModel):
    """Inclusive, and optional.

    Marking everything read is the common case. `before_id` exists so opening
    the screen cannot mark something read that arrived while it was open and
    was never actually on it.
    """
    before_id: Optional[int] = None
