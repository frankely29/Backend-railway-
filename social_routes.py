"""HTTP surface for the driver network.

Reading is free; joining in is not. The feed, a post, its comments and a profile
sit behind `require_user_basic` -- signed in, not necessarily paid -- because
seeing what other drivers are saying is the reason to subscribe, and a wall in
front of it sells nothing. Posting, commenting, liking, following and every
identity change sit behind `require_user`, the same gate as the map and chat.

Which of the two a route uses is the whole access policy for this file, so it is
pinned by tests/test_access_enforcement_default.py rather than left to memory.

The two image routes are the exception in shape rather than in access: they
return bytes instead of JSON and are the only endpoints a browser will hit with
an <img> tag.
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, Response, UploadFile

from core import require_user, require_user_basic
from media_store import THUMB_MIME_TYPE, derive_thumb_key
from social_moderation import (
    REPORT_REASONS,
    REPORT_TARGETS,
    block_user,
    create_report,
    list_blocked,
    list_muted,
    mute_user,
    unblock_user,
    unmute_user,
)
from social_identity import (
    PLATFORM_CHOICES,
    VEHICLE_CHOICES,
    handle_is_free,
    set_handle,
    suggest_handle,
    update_identity,
    validate_handle,
)
from social_models import (
    Comment,
    CommentCountResponse,
    DriverSearchResponse,
    CommentResponse,
    CommentsResponse,
    CreateCommentPayload,
    CreatePostPayload,
    FeedResponse,
    FeedScope,
    FollowResponse,
    HandleAvailability,
    IdentityOptionsResponse,
    RelationshipListResponse,
    RelationshipResponse,
    ReportCreated,
    ReportOptionsResponse,
    ReportPayload,
    SetHandlePayload,
    UpdateIdentityPayload,
    LikeResponse,
    MarkReadPayload,
    NotificationsResponse,
    OkResponse,
    PostResponse,
    ProfileResponse,
    SetCityPayload,
    UnreadResponse,
)
from social_service import (
    _resolve_image_path,
    _row_value,
    create_comment,
    create_post,
    delete_comment,
    delete_post,
    get_comments,
    follow_user,
    get_feed,
    get_post,
    get_profile,
    get_profile_by_handle,
    search_drivers,
    get_notifications,
    get_user_posts,
    like_post,
    mark_notifications_read,
    post_media_row,
    set_user_city,
    unfollow_user,
    unlike_post,
    unread_notification_count,
)

router = APIRouter(tags=["social"])


# --------------------------------------------------------------------------
# feed
#
# Reading is free; joining in is not. An unpaid driver can see what the
# community is saying -- that is the reason to subscribe -- but cannot post,
# comment, like or follow. Which routes are free is decided here, by the
# dependency each one declares, and pinned by tests/test_access_enforcement_default.py.
# --------------------------------------------------------------------------

@router.get("/social/feed", response_model=FeedResponse)
def social_feed(
    scope: FeedScope = FeedScope.following,
    limit: Optional[int] = None,
    before_id: Optional[int] = None,
    user: sqlite3.Row = Depends(require_user_basic),
):
    data = get_feed(int(user["id"]), scope, limit=limit, before_id=before_id)
    return {"ok": True, "scope": scope, **data}


@router.get("/social/users/{user_id}/posts", response_model=FeedResponse)
def social_user_posts(
    user_id: int,
    limit: Optional[int] = None,
    before_id: Optional[int] = None,
    user: sqlite3.Row = Depends(require_user_basic),
):
    data = get_user_posts(int(user["id"]), user_id, limit=limit, before_id=before_id)
    return {"ok": True, "scope": FeedScope.everyone, **data}


# --------------------------------------------------------------------------
# posts
# --------------------------------------------------------------------------

@router.post("/social/posts", response_model=PostResponse)
def social_create_post(payload: CreatePostPayload, user: sqlite3.Row = Depends(require_user)):
    post = create_post(
        user,
        payload.body,
        lat=payload.lat,
        lng=payload.lng,
        zone_name=payload.zone_name,
        zone_rating=payload.zone_rating,
    )
    return {"ok": True, "post": post}


@router.post("/social/posts/photo", response_model=PostResponse)
async def social_create_photo_post(
    file: UploadFile = File(...),
    body: str = Form(default=""),
    lat: Optional[float] = Form(default=None),
    lng: Optional[float] = Form(default=None),
    zone_name: Optional[str] = Form(default=None),
    zone_rating: Optional[int] = Form(default=None),
    user: sqlite3.Row = Depends(require_user),
):
    """Multipart, because a photo post is a file plus its caption in one tap.

    Separate from the JSON route rather than one endpoint that accepts both:
    FastAPI cannot express "JSON body OR multipart" on a single operation
    without hand-rolling the parsing, and hand-rolled multipart parsing is not
    worth one fewer URL.
    """
    post = create_post(
        user,
        body,
        lat=lat,
        lng=lng,
        zone_name=zone_name,
        zone_rating=zone_rating,
        upload=file,
    )
    return {"ok": True, "post": post}


@router.get("/social/posts/{post_id}", response_model=PostResponse)
def social_get_post(post_id: int, user: sqlite3.Row = Depends(require_user_basic)):
    return {"ok": True, "post": get_post(int(user["id"]), post_id)}


@router.delete("/social/posts/{post_id}", response_model=OkResponse)
def social_delete_post(post_id: int, user: sqlite3.Row = Depends(require_user)):
    delete_post(int(user["id"]), post_id)
    return {"ok": True}


# --------------------------------------------------------------------------
# post media
# --------------------------------------------------------------------------

# A week. A post's photo is written once inside create_post and never touched
# again -- there is no edit route, ids are never reused, and thumbnails are made
# at write time rather than backfilled -- so the bytes behind one of these URLs
# are fixed for the life of the post.
#
# Not `immutable`, though. That removes the browser's ability to ever correct
# itself, and with an ETag the gain over a 304 is one round trip on a reload.
# The avatar route can afford `immutable` because its URL carries a version;
# these URLs do not, so revalidation stays available.
POST_IMAGE_CACHE_SECONDS = 7 * 24 * 3600


def _file_etag(target) -> str:
    """Size and mtime, the way a static file server does it.

    Cheap -- one stat, no read -- and it changes if the bytes ever do, which
    matters more than being a true content hash for files this size.
    """
    stat = target.stat()
    return f'"post-{int(stat.st_size)}-{int(stat.st_mtime)}"'


def _if_none_match(request: Request, etag: str) -> bool:
    # Deliberately a local copy of main.py's matcher rather than an import:
    # main imports this module, so reaching back for eight lines would make the
    # import circular.
    raw = request.headers.get("if-none-match", "")
    if not raw or not etag:
        return False
    return any(candidate.strip() == etag for candidate in raw.split(","))


def _serve(target, media_type: str, request: Request) -> Response:
    if request.method.upper() not in {"GET", "HEAD"}:
        raise HTTPException(status_code=405, detail="Method not allowed")
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="Image file missing")

    etag = _file_etag(target)
    headers = {
        # `private`, because these need a token. A shared cache holding them
        # would serve one driver's photo to whoever asked next.
        "Cache-Control": f"private, max-age={POST_IMAGE_CACHE_SECONDS}",
        "ETag": etag,
    }
    # A 304 costs a stat and a header. The old five-minute window with no ETag
    # meant a driver scrolling the same feed pulled every photo off the disk
    # again every five minutes, in full.
    if _if_none_match(request, etag):
        return Response(status_code=304, headers=headers)
    return Response(
        content=b"" if request.method.upper() == "HEAD" else target.read_bytes(),
        media_type=media_type or "application/octet-stream",
        headers=headers,
    )


@router.api_route("/social/posts/{post_id}/image", methods=["GET", "HEAD"])
def social_post_image(post_id: int, request: Request, user: sqlite3.Row = Depends(require_user)):
    row = post_media_row(post_id, int(user["id"]))
    return _serve(
        _resolve_image_path(str(_row_value(row, "image_path"))),
        str(_row_value(row, "image_mime_type", "") or ""),
        request,
    )


@router.api_route("/social/posts/{post_id}/image/thumb", methods=["GET", "HEAD"])
def social_post_image_thumb(post_id: int, request: Request, user: sqlite3.Row = Depends(require_user)):
    """Falls back to the original when no thumbnail exists.

    A post whose thumbnail failed at upload time should still render in a grid;
    serving the full image is slower but correct, and 404 here would leave a
    hole in the feed for something that is only a size optimisation.
    """
    row = post_media_row(post_id, int(user["id"]))
    image_path = str(_row_value(row, "image_path"))
    thumb = _resolve_image_path(derive_thumb_key(image_path))
    if thumb.exists() and thumb.is_file():
        return _serve(thumb, THUMB_MIME_TYPE, request)
    return _serve(
        _resolve_image_path(image_path),
        str(_row_value(row, "image_mime_type", "") or ""),
        request,
    )


# --------------------------------------------------------------------------
# likes
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# comments
# --------------------------------------------------------------------------

@router.get("/social/posts/{post_id}/comments", response_model=CommentsResponse)
def social_comments(
    post_id: int,
    limit: Optional[int] = None,
    after_id: Optional[int] = None,
    user: sqlite3.Row = Depends(require_user_basic),
):
    """Oldest first, paging forwards.

    A conversation is read in the order it happened, and paging forwards means
    a new reply lands at the end where the reader already is, rather than
    shifting everything above it.
    """
    return {"ok": True, **get_comments(int(user["id"]), post_id, limit=limit, after_id=after_id)}


@router.post("/social/posts/{post_id}/comments", response_model=CommentResponse)
def social_create_comment(post_id: int, payload: CreateCommentPayload,
                          user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **create_comment(user, post_id, payload.body,
                                         parent_id=payload.parent_id)}


@router.delete("/social/comments/{comment_id}", response_model=CommentCountResponse)
def social_delete_comment(comment_id: int, user: sqlite3.Row = Depends(require_user)):
    """Your own comment, or anything under your own post."""
    return {"ok": True, **delete_comment(int(user["id"]), comment_id)}


@router.post("/social/posts/{post_id}/like", response_model=LikeResponse)
def social_like(post_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **like_post(int(user["id"]), post_id)}


@router.delete("/social/posts/{post_id}/like", response_model=LikeResponse)
def social_unlike(post_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **unlike_post(int(user["id"]), post_id)}


# --------------------------------------------------------------------------
# follows and profiles
# --------------------------------------------------------------------------

@router.post("/social/users/{user_id}/follow", response_model=FollowResponse)
def social_follow(user_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **follow_user(int(user["id"]), user_id)}


@router.delete("/social/users/{user_id}/follow", response_model=FollowResponse)
def social_unfollow(user_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **unfollow_user(int(user["id"]), user_id)}


@router.get("/social/users/{user_id}/profile", response_model=ProfileResponse)
def social_profile(user_id: int, user: sqlite3.Row = Depends(require_user_basic)):
    return {"ok": True, "profile": get_profile(int(user["id"]), user_id)}


@router.get("/social/me/profile", response_model=ProfileResponse)
def social_my_profile(user: sqlite3.Row = Depends(require_user_basic)):
    me = int(user["id"])
    return {"ok": True, "profile": get_profile(me, me)}


@router.post("/social/me/city", response_model=OkResponse)
def social_set_city(payload: SetCityPayload, user: sqlite3.Row = Depends(require_user)):
    set_user_city(int(user["id"]), payload.city)
    return {"ok": True}


# --------------------------------------------------------------------------
# identity
# --------------------------------------------------------------------------

@router.get("/social/search/drivers", response_model=DriverSearchResponse)
def social_search_drivers(
    q: str = "",
    limit: int = 20,
    user: sqlite3.Row = Depends(require_user_basic),
):
    """Find other drivers by name or handle.

    require_user_basic, not require_user: searching is reading, and a driver
    whose subscription has lapsed should still be able to find people.

    Blocked drivers are filtered inside the service rather than here, because
    leaving them out of the RESULTS is the whole point -- a search that still
    listed someone who blocked you would be a way to find that out.
    """
    return {"ok": True, **search_drivers(int(user["id"]), q, limit)}


@router.get("/social/users/by-handle/{handle}/profile", response_model=ProfileResponse)
def social_profile_by_handle(handle: str, user: sqlite3.Row = Depends(require_user_basic)):
    """A handle is the name people link to, so it has to resolve to a profile."""
    return {"ok": True, "profile": get_profile_by_handle(int(user["id"]), handle)}


@router.get("/social/handles/{handle}/available", response_model=HandleAvailability)
def social_handle_available(handle: str, user: sqlite3.Row = Depends(require_user)):
    """Checked live while typing, so it answers rather than raising.

    The validation rules return a 400 from validate_handle when a handle is
    malformed; here that is not an error, it is the answer -- and the reason is
    what the field shows underneath.
    """
    try:
        display = validate_handle(handle)
    except HTTPException as exc:
        return {"ok": True, "handle": str(handle), "available": False,
                "reason": str(exc.detail)}
    free = handle_is_free(display, for_user_id=int(user["id"]))
    return {"ok": True, "handle": display, "available": free,
            "reason": None if free else "That handle is taken"}


@router.post("/social/me/handle", response_model=ProfileResponse)
def social_set_handle(payload: SetHandlePayload, user: sqlite3.Row = Depends(require_user)):
    me = int(user["id"])
    set_handle(me, payload.handle)
    return {"ok": True, "profile": get_profile(me, me)}


@router.get("/social/me/handle/suggest", response_model=HandleAvailability)
def social_suggest_handle(user: sqlite3.Row = Depends(require_user)):
    display_name = user["display_name"] if "display_name" in user.keys() else None
    email = user["email"] if "email" in user.keys() else None
    candidate = suggest_handle(display_name, email)
    return {"ok": True, "handle": candidate, "available": True, "reason": None}


@router.post("/social/me/identity", response_model=ProfileResponse)
def social_update_identity(payload: UpdateIdentityPayload,
                           user: sqlite3.Row = Depends(require_user)):
    """Patch, not replace.

    Reading `model_fields_set` is what separates "the client did not send a bio"
    from "the client is clearing the bio" -- both arrive as None otherwise, and
    conflating them means every partial save wipes the fields it did not touch.
    """
    sent = payload.model_fields_set
    update_identity(
        int(user["id"]),
        bio=payload.bio if "bio" in sent else ...,
        platforms=payload.platforms if "platforms" in sent else ...,
        vehicle_type=payload.vehicle_type if "vehicle_type" in sent else ...,
        driving_since_year=payload.driving_since_year if "driving_since_year" in sent else ...,
    )
    me = int(user["id"])
    return {"ok": True, "profile": get_profile(me, me)}


@router.get("/social/identity/options", response_model=IdentityOptionsResponse)
def social_identity_options(user: sqlite3.Row = Depends(require_user)):
    """The closed sets the client should render, rather than hard-coding them."""
    return {"ok": True, "platforms": PLATFORM_CHOICES, "vehicle_types": VEHICLE_CHOICES}


# --------------------------------------------------------------------------
# block, mute, report
# --------------------------------------------------------------------------

@router.post("/social/users/{user_id}/block", response_model=RelationshipResponse)
def social_block(user_id: int, user: sqlite3.Row = Depends(require_user)):
    """Mutual and structural: it also severs the follows in both directions."""
    return {"ok": True, **block_user(int(user["id"]), user_id)}


@router.delete("/social/users/{user_id}/block", response_model=RelationshipResponse)
def social_unblock(user_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **unblock_user(int(user["id"]), user_id)}


@router.post("/social/users/{user_id}/mute", response_model=RelationshipResponse)
def social_mute(user_id: int, user: sqlite3.Row = Depends(require_user)):
    """One-way and silent. Their posts leave your feed; nothing else changes."""
    return {"ok": True, **mute_user(int(user["id"]), user_id)}


@router.delete("/social/users/{user_id}/mute", response_model=RelationshipResponse)
def social_unmute(user_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **unmute_user(int(user["id"]), user_id)}


@router.get("/social/me/blocked", response_model=RelationshipListResponse)
def social_list_blocked(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "items": list_blocked(int(user["id"]))}


@router.get("/social/me/muted", response_model=RelationshipListResponse)
def social_list_muted(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "items": list_muted(int(user["id"]))}


@router.post("/social/reports", response_model=ReportCreated)
def social_report(payload: ReportPayload, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **create_report(
        int(user["id"]), payload.target_type, payload.target_id,
        payload.reason, payload.note)}


@router.get("/social/reports/options", response_model=ReportOptionsResponse)
def social_report_options(user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "reasons": REPORT_REASONS, "targets": REPORT_TARGETS}


# --------------------------------------------------------------------------
# notifications
#
# require_user, not require_user_basic: these are about things other drivers
# did to your posts, and an unpaid driver has no posts -- reading the feed is
# free, being part of it is not. Same line every other write-side route draws.
# --------------------------------------------------------------------------

@router.get("/social/notifications", response_model=NotificationsResponse)
def social_notifications(limit: Optional[int] = None, before_id: Optional[int] = None,
                         user: sqlite3.Row = Depends(require_user)):
    """Newest first, keyset paged on before_id -- the same shape as the feed."""
    return {"ok": True, **get_notifications(int(user["id"]), limit=limit,
                                            before_id=before_id)}


@router.get("/social/notifications/unread", response_model=UnreadResponse)
def social_notifications_unread(user: sqlite3.Row = Depends(require_user)):
    """Just the number, for the badge.

    Its own route because the badge is polled and the page is not: this reads
    one indexed count, where the page joins users and posts for twenty rows.
    """
    return {"ok": True, "unread": unread_notification_count(int(user["id"]))}


@router.post("/social/notifications/read", response_model=UnreadResponse)
def social_notifications_read(payload: MarkReadPayload,
                              user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, **mark_notifications_read(int(user["id"]),
                                                  before_id=payload.before_id)}
