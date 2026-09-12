"""HTTP surface for the driver network.

Everything here sits behind `require_user`, the same gate as the map and chat --
the network is part of the subscription, not a free tier alongside it.

The two image routes are the exception in shape rather than in access: they
still require a signed-in subscriber, but they return bytes instead of JSON and
they are the only endpoints a browser will hit with an <img> tag.
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, Response, UploadFile

from core import require_user
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
    OkResponse,
    PostResponse,
    ProfileResponse,
    SetCityPayload,
)
from social_service import (
    _resolve_image_path,
    _row_value,
    create_post,
    delete_post,
    follow_user,
    get_feed,
    get_post,
    get_profile,
    get_profile_by_handle,
    get_user_posts,
    like_post,
    post_media_row,
    set_user_city,
    unfollow_user,
    unlike_post,
)

router = APIRouter(tags=["social"])


# --------------------------------------------------------------------------
# feed
# --------------------------------------------------------------------------

@router.get("/social/feed", response_model=FeedResponse)
def social_feed(
    scope: FeedScope = FeedScope.following,
    limit: Optional[int] = None,
    before_id: Optional[int] = None,
    user: sqlite3.Row = Depends(require_user),
):
    data = get_feed(int(user["id"]), scope, limit=limit, before_id=before_id)
    return {"ok": True, "scope": scope, **data}


@router.get("/social/users/{user_id}/posts", response_model=FeedResponse)
def social_user_posts(
    user_id: int,
    limit: Optional[int] = None,
    before_id: Optional[int] = None,
    user: sqlite3.Row = Depends(require_user),
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
def social_get_post(post_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "post": get_post(int(user["id"]), post_id)}


@router.delete("/social/posts/{post_id}", response_model=OkResponse)
def social_delete_post(post_id: int, user: sqlite3.Row = Depends(require_user)):
    delete_post(int(user["id"]), post_id)
    return {"ok": True}


# --------------------------------------------------------------------------
# post media
# --------------------------------------------------------------------------

def _serve(target, media_type: str, request: Request) -> Response:
    if request.method.upper() not in {"GET", "HEAD"}:
        raise HTTPException(status_code=405, detail="Method not allowed")
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="Image file missing")
    return Response(
        content=b"" if request.method.upper() == "HEAD" else target.read_bytes(),
        media_type=media_type or "application/octet-stream",
        headers={"Cache-Control": "private, max-age=300"},
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
def social_profile(user_id: int, user: sqlite3.Row = Depends(require_user)):
    return {"ok": True, "profile": get_profile(int(user["id"]), user_id)}


@router.get("/social/me/profile", response_model=ProfileResponse)
def social_my_profile(user: sqlite3.Row = Depends(require_user)):
    me = int(user["id"])
    return {"ok": True, "profile": get_profile(me, me)}


@router.post("/social/me/city", response_model=OkResponse)
def social_set_city(payload: SetCityPayload, user: sqlite3.Row = Depends(require_user)):
    set_user_city(int(user["id"]), payload.city)
    return {"ok": True}


# --------------------------------------------------------------------------
# identity
# --------------------------------------------------------------------------

@router.get("/social/users/by-handle/{handle}/profile", response_model=ProfileResponse)
def social_profile_by_handle(handle: str, user: sqlite3.Row = Depends(require_user)):
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
