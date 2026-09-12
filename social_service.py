"""The driver network: posts, likes, follows, profiles.

Reads are the hot path here -- a feed is opened far more often than it is
written -- so every list endpoint costs a fixed THREE queries regardless of page
size: one for the page of posts, one for its like counts, one for which of them
the viewer already liked. No per-row lookups, no N+1.

Media is deliberately NOT reimplemented. chat.py already solved uploading an
image safely: MIME allow-list, byte cap, a path resolver that refuses to escape
the media directory, an atomic write and a best-effort thumbnail. Rewriting that
here would mean owning a second copy of a path-traversal check, which is exactly
the kind of duplication that eventually diverges in the wrong direction. So the
helpers are imported, and posts write into their own subdirectory under the same
root.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from fastapi import HTTPException, UploadFile

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one, _db_run_in_transaction, _sql
from media_store import derive_thumb_key
from social_identity import split_platforms
from social_moderation import hidden_author_ids, is_blocked_either_way
from social_models import MAX_BODY_CHARS, MAX_CITY_CHARS, FeedScope

# chat.py owns the vetted upload path; see the module docstring.
from chat import (  # noqa: F401  (re-exported for tests)
    _read_upload_image,
    _resolve_image_path,
    _store_image_file,
    _unlink_image_and_thumb,
)

_LOGGER = logging.getLogger(__name__)

POST_IMAGE_SUBDIR = "posts"

DEFAULT_PAGE = 30
MAX_PAGE = 100

# A person types; a script floods. Ten posts a minute is far above what anyone
# does by hand and far below what makes a feed unreadable.
POST_RATE_WINDOW_SECONDS = 60
POST_RATE_MAX = 10

_CITY_SPLIT = re.compile(r"[^a-z0-9]+")


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def _now() -> int:
    return int(time.time())


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    """Rows come back as sqlite3.Row or dict depending on the backend."""
    if row is None:
        return default
    try:
        if hasattr(row, "keys") and key in row.keys():
            value = row[key]
            return default if value is None else value
    except Exception:
        pass
    if isinstance(row, dict):
        value = row.get(key)
        return default if value is None else value
    return default


def normalize_city_key(city: Optional[str]) -> Optional[str]:
    """"Houston, TX" and " houston tx " are the same room.

    Returns None for anything that normalises to nothing, so a user who clears
    their city is genuinely city-less rather than a member of the "" room.
    """
    if not city:
        return None
    parts = [p for p in _CITY_SPLIT.split(str(city).strip().lower()) if p]
    return "-".join(parts) or None


def clean_city(city: Optional[str]) -> Optional[str]:
    if not city:
        return None
    collapsed = " ".join(str(city).split())[:MAX_CITY_CHARS].strip()
    return collapsed or None


def clean_body(body: Optional[str]) -> str:
    if not body:
        return ""
    return str(body).strip()[:MAX_BODY_CHARS]


def _clamp_limit(limit: Optional[int]) -> int:
    try:
        value = int(limit) if limit is not None else DEFAULT_PAGE
    except (TypeError, ValueError):
        return DEFAULT_PAGE
    return max(1, min(MAX_PAGE, value))


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y")


def _image_urls(post_id: int, image_path: Optional[str], has_thumb: Any) -> Tuple[Optional[str], Optional[str]]:
    """A post with no thumbnail points its thumb URL at the original.

    The frontend renders whichever URL it is given (app.part8.js does the same
    for chat), so falling back here means a failed thumbnail costs quality, not
    a broken image.
    """
    if not image_path:
        return None, None
    full = f"/social/posts/{int(post_id)}/image"
    thumb = f"{full}/thumb" if _bool(has_thumb) else full
    return full, thumb


# --------------------------------------------------------------------------
# city
# --------------------------------------------------------------------------

def set_user_city(user_id: int, city: Optional[str]) -> Dict[str, Optional[str]]:
    display = clean_city(city)
    key = normalize_city_key(display)
    _db_exec("UPDATE users SET city=?, city_key=? WHERE id=?", (display, key, int(user_id)))
    return {"city": display, "city_key": key}


def get_user_city_key(user_id: int) -> Optional[str]:
    row = _db_query_one("SELECT city_key FROM users WHERE id=? LIMIT 1", (int(user_id),))
    return _row_value(row, "city_key")


# --------------------------------------------------------------------------
# assembling posts
# --------------------------------------------------------------------------

_POST_COLUMNS = """
    p.id AS id, p.user_id AS user_id, p.body AS body, p.image_path AS image_path,
    p.has_thumb AS has_thumb, p.lat AS lat, p.lng AS lng, p.zone_name AS zone_name,
    p.zone_rating AS zone_rating, p.created_at AS created_at,
    u.display_name AS display_name, u.handle AS handle, u.city AS author_city,
    u.avatar_url AS avatar_url
"""



def _visibility_clause(viewer_id: int) -> Tuple[str, List[Any]]:
    """SQL that removes what this viewer must not see: moderator-hidden posts,
    plus anyone they blocked, anyone who blocked them, and anyone they muted.

    Returned as a fragment rather than applied inside one query, because the
    same rule has to hold for the feed, a profile's posts and a single post --
    and a rule enforced in only two of three places is not enforced.
    """
    parts = ["p.hidden_at IS NULL"]
    params: List[Any] = []
    hidden = hidden_author_ids(int(viewer_id))
    if hidden:
        placeholders = ",".join("?" for _ in hidden)
        parts.append(f"p.user_id NOT IN ({placeholders})")
        params.extend(int(uid) for uid in hidden)
    return " AND ".join(parts), params


def _like_state(post_ids: Sequence[int], viewer_id: int) -> Tuple[Dict[int, int], set]:
    """Two queries for the whole page: how many likes, and which ones are mine."""
    if not post_ids:
        return {}, set()
    placeholders = ",".join("?" for _ in post_ids)
    params = tuple(int(pid) for pid in post_ids)

    counts: Dict[int, int] = {}
    for row in _db_query_all(
        f"SELECT post_id, COUNT(*) AS n FROM post_likes WHERE post_id IN ({placeholders}) GROUP BY post_id",
        params,
    ):
        counts[int(_row_value(row, "post_id", 0))] = int(_row_value(row, "n", 0))

    mine = {
        int(_row_value(row, "post_id", 0))
        for row in _db_query_all(
            f"SELECT post_id FROM post_likes WHERE user_id=? AND post_id IN ({placeholders})",
            (int(viewer_id),) + params,
        )
    }
    return counts, mine


def _serialize(row: Any, viewer_id: int, counts: Dict[int, int], mine: set) -> Dict[str, Any]:
    post_id = int(_row_value(row, "id", 0))
    author_id = int(_row_value(row, "user_id", 0))
    image_url, thumb_url = _image_urls(post_id, _row_value(row, "image_path"), _row_value(row, "has_thumb"))
    avatar = _row_value(row, "avatar_url")
    return {
        "id": post_id,
        "author": {
            "user_id": author_id,
            "display_name": str(_row_value(row, "display_name", "Driver")),
            "handle": _row_value(row, "handle"),
            "city": _row_value(row, "author_city"),
            "avatar_url": f"/avatars/thumb/{author_id}" if avatar else None,
        },
        "body": str(_row_value(row, "body", "")),
        "image_url": image_url,
        "image_thumb_url": thumb_url,
        "city": _row_value(row, "author_city"),
        "lat": _row_value(row, "lat"),
        "lng": _row_value(row, "lng"),
        "zone_name": _row_value(row, "zone_name"),
        "zone_rating": _row_value(row, "zone_rating"),
        "like_count": counts.get(post_id, 0),
        "liked_by_me": post_id in mine,
        "mine": author_id == int(viewer_id),
        "created_at": int(_row_value(row, "created_at", 0)),
    }


def _page(rows: Iterable[Any], viewer_id: int, limit: int) -> Dict[str, Any]:
    rows = list(rows)
    # One row was fetched beyond the page purely to answer "is there more?" --
    # cheaper and more honest than returning a cursor that turns out to be empty.
    has_more = len(rows) > limit
    rows = rows[:limit]
    post_ids = [int(_row_value(r, "id", 0)) for r in rows]
    counts, mine = _like_state(post_ids, viewer_id)
    items = [_serialize(r, viewer_id, counts, mine) for r in rows]
    return {
        "items": items,
        "next_before_id": post_ids[-1] if (has_more and post_ids) else None,
    }


# --------------------------------------------------------------------------
# feeds
# --------------------------------------------------------------------------

def get_feed(viewer_id: int, scope: FeedScope, limit: Optional[int] = None,
             before_id: Optional[int] = None) -> Dict[str, Any]:
    limit = _clamp_limit(limit)
    fetch = limit + 1
    viewer_id = int(viewer_id)
    visibility, visibility_params = _visibility_clause(viewer_id)
    where = ["p.deleted_at IS NULL", visibility]
    params: List[Any] = list(visibility_params)

    if scope == FeedScope.following:
        # Your own posts belong in your timeline. A feed that hides what you
        # just wrote reads as a failed post.
        where.append(
            "(p.user_id = ? OR p.user_id IN (SELECT followee_id FROM follows WHERE follower_id = ?))"
        )
        params.extend([viewer_id, viewer_id])
    elif scope == FeedScope.city:
        city_key = get_user_city_key(viewer_id)
        if not city_key:
            # No city set means no city feed -- not everybody's feed. Silently
            # widening this would show a Houston driver the global timeline and
            # call it Houston.
            return {"items": [], "next_before_id": None}
        where.append("p.city_key = ?")
        params.append(city_key)

    if before_id:
        where.append("p.id < ?")
        params.append(int(before_id))

    sql = (
        f"SELECT {_POST_COLUMNS} FROM posts p JOIN users u ON u.id = p.user_id "
        f"WHERE {' AND '.join(where)} ORDER BY p.id DESC LIMIT ?"
    )
    rows = _db_query_all(sql, tuple(params) + (fetch,))
    return _page(rows, viewer_id, limit)


def get_user_posts(viewer_id: int, author_id: int, limit: Optional[int] = None,
                   before_id: Optional[int] = None) -> Dict[str, Any]:
    limit = _clamp_limit(limit)
    visibility, visibility_params = _visibility_clause(viewer_id)
    where = ["p.deleted_at IS NULL", visibility, "p.user_id = ?"]
    params: List[Any] = list(visibility_params) + [int(author_id)]
    if before_id:
        where.append("p.id < ?")
        params.append(int(before_id))
    rows = _db_query_all(
        f"SELECT {_POST_COLUMNS} FROM posts p JOIN users u ON u.id = p.user_id "
        f"WHERE {' AND '.join(where)} ORDER BY p.id DESC LIMIT ?",
        tuple(params) + (limit + 1,),
    )
    return _page(rows, int(viewer_id), limit)


def get_post(viewer_id: int, post_id: int) -> Dict[str, Any]:
    visibility, visibility_params = _visibility_clause(viewer_id)
    row = _db_query_one(
        f"SELECT {_POST_COLUMNS} FROM posts p JOIN users u ON u.id = p.user_id "
        f"WHERE p.id=? AND p.deleted_at IS NULL AND {visibility} LIMIT 1",
        (int(post_id),) + tuple(visibility_params),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Post not found")
    counts, mine = _like_state([int(post_id)], int(viewer_id))
    return _serialize(row, int(viewer_id), counts, mine)


# --------------------------------------------------------------------------
# writing
# --------------------------------------------------------------------------

def _enforce_post_rate(user_id: int) -> None:
    since = _now() - POST_RATE_WINDOW_SECONDS
    row = _db_query_one(
        "SELECT COUNT(*) AS n FROM posts WHERE user_id=? AND created_at >= ?",
        (int(user_id), since),
    )
    if int(_row_value(row, "n", 0)) >= POST_RATE_MAX:
        raise HTTPException(status_code=429, detail="You're posting too quickly. Try again in a minute.")


def create_post(user: Any, body: Optional[str], lat: Optional[float] = None,
                lng: Optional[float] = None, zone_name: Optional[str] = None,
                zone_rating: Optional[int] = None,
                upload: Optional[UploadFile] = None) -> Dict[str, Any]:
    user_id = int(_row_value(user, "id", 0))
    if not user_id:
        raise HTTPException(status_code=401, detail="Not signed in")

    clean = clean_body(body)
    # Read and validate the image BEFORE inserting anything. A rejected upload
    # should not leave an empty post behind.
    payload: Optional[bytes] = None
    extension = ""
    mime_type = ""
    if upload is not None:
        payload, mime_type, extension = _read_upload_image(upload)

    if not clean and payload is None:
        raise HTTPException(status_code=400, detail="A post needs text or a photo")

    _enforce_post_rate(user_id)

    city_key = get_user_city_key(user_id)
    now = _now()
    values = (
        user_id, clean, None, False, city_key,
        float(lat) if lat is not None else None,
        float(lng) if lng is not None else None,
        (str(zone_name).strip() or None) if zone_name else None,
        int(zone_rating) if zone_rating is not None else None,
        now,
    )
    columns = ("user_id, body, image_path, has_thumb, city_key, lat, lng, "
               "zone_name, zone_rating, created_at")

    def _insert(_conn, cur) -> int:
        """Take the id from the INSERT itself.

        Re-reading "the newest row for this user at this timestamp" would be a
        race: two posts in the same second could hand back the other one, and
        the photo would then be attached to the wrong post.
        """
        if DB_BACKEND == "postgres":
            cur.execute(
                _sql(f"INSERT INTO posts({columns}) VALUES(?,?,?,?,?,?,?,?,?,?) RETURNING id"),
                values,
            )
            return int(cur.fetchone()[0])
        cur.execute(_sql(f"INSERT INTO posts({columns}) VALUES(?,?,?,?,?,?,?,?,?,?)"), values)
        return int(cur.lastrowid)

    post_id = int(_db_run_in_transaction(_insert) or 0)
    if not post_id:
        raise HTTPException(status_code=500, detail="Post could not be saved")

    if payload is not None:
        try:
            relative = _store_image_file(POST_IMAGE_SUBDIR, post_id, user_id, extension, payload)
        except Exception:
            # The row exists but its photo does not, and a post that silently
            # loses the photo is worse than one that never appeared.
            _db_exec("DELETE FROM posts WHERE id=?", (post_id,))
            _LOGGER.warning("Post image could not be stored", exc_info=True)
            raise HTTPException(status_code=500, detail="Photo could not be saved")
        has_thumb = _resolve_image_path(derive_thumb_key(relative)).exists()
        _db_exec(
            "UPDATE posts SET image_path=?, image_mime_type=?, has_thumb=? WHERE id=?",
            (relative, mime_type, bool(has_thumb), post_id),
        )

    return get_post(user_id, post_id)


def delete_post(user_id: int, post_id: int) -> None:
    row = _db_query_one(
        "SELECT id, user_id, image_path FROM posts WHERE id=? AND deleted_at IS NULL LIMIT 1",
        (int(post_id),),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Post not found")
    if int(_row_value(row, "user_id", 0)) != int(user_id):
        raise HTTPException(status_code=403, detail="That isn't your post")

    _db_exec("UPDATE posts SET deleted_at=? WHERE id=?", (_now(), int(post_id)))

    # The row survives so counts stay stable; the bytes do not. Someone deleting
    # a photo means it should stop existing, not stop being listed.
    image_path = _row_value(row, "image_path")
    if image_path:
        try:
            _unlink_image_and_thumb(_resolve_image_path(str(image_path)))
        except Exception:
            _LOGGER.warning("Could not unlink post media", exc_info=True)


# --------------------------------------------------------------------------
# likes
# --------------------------------------------------------------------------

def _live_post_or_404(post_id: int) -> Any:
    row = _db_query_one(
        "SELECT id, user_id FROM posts WHERE id=? AND deleted_at IS NULL LIMIT 1",
        (int(post_id),),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Post not found")
    return row


def _like_summary(post_id: int, viewer_id: int) -> Dict[str, Any]:
    counts, mine = _like_state([int(post_id)], int(viewer_id))
    return {
        "post_id": int(post_id),
        "like_count": counts.get(int(post_id), 0),
        "liked_by_me": int(post_id) in mine,
    }


def like_post(user_id: int, post_id: int) -> Dict[str, Any]:
    _live_post_or_404(post_id)
    existing = _db_query_one(
        "SELECT post_id FROM post_likes WHERE post_id=? AND user_id=? LIMIT 1",
        (int(post_id), int(user_id)),
    )
    if not existing:
        # Liking twice is a double-tap, not an error -- the endpoint is
        # idempotent so a flaky connection cannot produce a 500.
        _db_exec(
            "INSERT INTO post_likes(post_id, user_id, created_at) VALUES(?,?,?)",
            (int(post_id), int(user_id), _now()),
        )
    return _like_summary(post_id, user_id)


def unlike_post(user_id: int, post_id: int) -> Dict[str, Any]:
    _live_post_or_404(post_id)
    _db_exec(
        "DELETE FROM post_likes WHERE post_id=? AND user_id=?",
        (int(post_id), int(user_id)),
    )
    return _like_summary(post_id, user_id)


# --------------------------------------------------------------------------
# follows
# --------------------------------------------------------------------------

def _follower_count(user_id: int) -> int:
    row = _db_query_one(
        "SELECT COUNT(*) AS n FROM follows WHERE followee_id=?", (int(user_id),)
    )
    return int(_row_value(row, "n", 0))


def _following_count(user_id: int) -> int:
    row = _db_query_one(
        "SELECT COUNT(*) AS n FROM follows WHERE follower_id=?", (int(user_id),)
    )
    return int(_row_value(row, "n", 0))


def _user_or_404(user_id: int) -> Any:
    row = _db_query_one(
        "SELECT id, display_name, handle, city, avatar_url, bio, platforms, "
        "vehicle_type, driving_since_year FROM users WHERE id=? LIMIT 1",
        (int(user_id),),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Driver not found")
    return row


def follow_user(follower_id: int, followee_id: int) -> Dict[str, Any]:
    if int(follower_id) == int(followee_id):
        raise HTTPException(status_code=400, detail="You can't follow yourself")
    _user_or_404(followee_id)
    if is_blocked_either_way(int(follower_id), int(followee_id)):
        # Symmetric on purpose: it must not be possible to work out that someone
        # blocked you by watching which follows succeed.
        raise HTTPException(status_code=403, detail="You can't follow this driver")
    existing = _db_query_one(
        "SELECT follower_id FROM follows WHERE follower_id=? AND followee_id=? LIMIT 1",
        (int(follower_id), int(followee_id)),
    )
    if not existing:
        _db_exec(
            "INSERT INTO follows(follower_id, followee_id, created_at) VALUES(?,?,?)",
            (int(follower_id), int(followee_id), _now()),
        )
    return {
        "user_id": int(followee_id),
        "following": True,
        "follower_count": _follower_count(followee_id),
    }


def unfollow_user(follower_id: int, followee_id: int) -> Dict[str, Any]:
    _user_or_404(followee_id)
    _db_exec(
        "DELETE FROM follows WHERE follower_id=? AND followee_id=?",
        (int(follower_id), int(followee_id)),
    )
    return {
        "user_id": int(followee_id),
        "following": False,
        "follower_count": _follower_count(followee_id),
    }


def _reputation_for(user_id: int) -> Dict[str, Any]:
    """Level, rank, badge and lifetime totals, from the leaderboard service.

    Imported lazily and wrapped: this is decoration on a profile, and a profile
    that 500s because a badge cache is cold is a worse outcome than one that
    renders without a badge. Every field is optional to the client.
    """
    out: Dict[str, Any] = {
        "level": None, "rank_name": None, "title": None, "badge_code": None,
        "lifetime_miles": None, "lifetime_hours": None, "trips_logged": None,
    }
    try:
        from leaderboard_service import (
            get_best_current_badge_for_user,
            get_progression_for_user,
        )
        progression = get_progression_for_user(int(user_id)) or {}
        out["level"] = progression.get("level")
        out["rank_name"] = progression.get("rank_name")
        out["title"] = progression.get("title")
        out["lifetime_miles"] = progression.get("lifetime_miles")
        out["lifetime_hours"] = progression.get("lifetime_hours")
        out["trips_logged"] = progression.get("lifetime_pickups_recorded")
    except Exception:
        _LOGGER.warning("Could not read progression for profile", exc_info=True)
    try:
        from leaderboard_service import get_best_current_badge_for_user
        out["badge_code"] = (get_best_current_badge_for_user(int(user_id)) or {}).get(
            "leaderboard_badge_code")
    except Exception:
        _LOGGER.warning("Could not read badge for profile", exc_info=True)
    return out


def get_profile(viewer_id: int, user_id: int) -> Dict[str, Any]:
    row = _user_or_404(user_id)
    is_me = int(viewer_id) == int(user_id)
    if not is_me and is_blocked_either_way(int(viewer_id), int(user_id)):
        # 404 rather than 403: "this person blocked you" is itself information,
        # and handing it over turns a block into a notification.
        raise HTTPException(status_code=404, detail="Driver not found")
    posts = _db_query_one(
        "SELECT COUNT(*) AS n FROM posts WHERE user_id=? AND deleted_at IS NULL",
        (int(user_id),),
    )
    followed = _db_query_one(
        "SELECT follower_id FROM follows WHERE follower_id=? AND followee_id=? LIMIT 1",
        (int(viewer_id), int(user_id)),
    )
    avatar = _row_value(row, "avatar_url")
    return {
        "user_id": int(user_id),
        "display_name": str(_row_value(row, "display_name", "Driver")),
        "handle": _row_value(row, "handle"),
        "city": _row_value(row, "city"),
        "avatar_url": f"/avatars/thumb/{int(user_id)}" if avatar else None,
        "bio": _row_value(row, "bio"),
        "platforms": split_platforms(_row_value(row, "platforms")),
        "vehicle_type": _row_value(row, "vehicle_type"),
        "driving_since_year": _row_value(row, "driving_since_year"),
        "post_count": int(_row_value(posts, "n", 0)),
        "follower_count": _follower_count(user_id),
        # Private by product decision: how many people you follow is yours.
        "following_count": _following_count(user_id) if is_me else None,
        "followed_by_me": bool(followed),
        "is_me": is_me,
        "reputation": _reputation_for(user_id),
    }


def get_profile_by_handle(viewer_id: int, handle: str) -> Dict[str, Any]:
    """A handle is the linkable name, so it has to resolve to a profile."""
    from social_identity import handle_key as _key
    row = _db_query_one(
        "SELECT id FROM users WHERE handle_key=? LIMIT 1", (_key(handle),))
    if not row:
        raise HTTPException(status_code=404, detail="Driver not found")
    return get_profile(int(viewer_id), int(_row_value(row, "id", 0)))


def post_media_row(post_id: int, viewer_id: Optional[int] = None) -> Any:
    """The media columns of a post this viewer is allowed to see.

    The visibility rule has to reach the bytes too. Filtering a blocked author
    out of the feed while still serving their photo to anyone holding the URL
    is not a block, it is a hidden link.
    """
    row = _db_query_one(
        "SELECT id, user_id, image_path, image_mime_type, has_thumb FROM posts "
        "WHERE id=? AND deleted_at IS NULL AND hidden_at IS NULL LIMIT 1",
        (int(post_id),),
    )
    if not row or not _row_value(row, "image_path"):
        raise HTTPException(status_code=404, detail="Image not found")
    if viewer_id is not None:
        author_id = int(_row_value(row, "user_id", 0))
        if author_id != int(viewer_id) and is_blocked_either_way(int(viewer_id), author_id):
            raise HTTPException(status_code=404, detail="Image not found")
    return row
