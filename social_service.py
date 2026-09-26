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
from avatar_assets import avatar_thumb_url, avatar_version_for_data_url
from social_identity import split_platforms
from social_moderation import hidden_author_ids, is_blocked_either_way
from social_models import MAX_BODY_CHARS, MAX_CITY_CHARS, MAX_COMMENT_CHARS, FeedScope

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


def _avatar_url(row: Any, user_id: int) -> Optional[str]:
    """The avatar URL WITH its version, the way every other module builds it.

    /avatars/thumb/{id} is served with `public, max-age=30 days, immutable`.
    `immutable` means the browser will not revalidate at all, so an unversioned
    URL shows a driver's OLD picture for a month after they change it -- and
    the ETag on that route never gets a chance to run.

    main.py, work_battles_service.py and games_service.py all go through
    avatar_thumb_url(); this module was hand-building the path and dropping the
    ?v=, so every feed card, comment and profile had the stale-avatar bug while
    the rest of the app did not.

    The version column can be empty on rows that predate it, so it falls back
    to hashing the data URL exactly as _avatar_version_for_row does.
    """
    stored = _row_value(row, "avatar_url")
    if not stored:
        return None
    version = _row_value(row, "avatar_version")
    if not version:
        version = avatar_version_for_data_url(str(stored))
    return avatar_thumb_url(int(user_id), version)


def _returned_id(cur: Any) -> int:
    """The id from a Postgres `RETURNING id`, whatever shape the row comes in.

    The connection pool is built with cursor_factory=RealDictCursor, so rows are
    dict-like and `cur.fetchone()[0]` raises KeyError. That is not a subtle
    failure -- it is a 500 on every write that uses RETURNING -- but it is an
    INVISIBLE one, because SQLite hands back tuples and every test runs on
    SQLite. Both shapes are handled here so one helper covers every call site.
    """
    row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=500, detail="Row could not be saved")
    try:
        return int(row["id"])
    except (TypeError, KeyError, IndexError):
        return int(list(dict(row).values())[0] if hasattr(row, "keys") else row[0])


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
    u.avatar_url AS avatar_url, u.avatar_version AS avatar_version,
    u.platforms AS author_platforms
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


def _author_levels(author_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    """Standing for each author on the page, in one query.

    This is what makes a driver network different from a photo app: you can see
    that the person telling you the lot is moving has actually driven. But it is
    still decoration -- a cold or broken progression cache must degrade to a card
    without a badge, never to a feed that 500s. Hence the blanket except.

    Batched deliberately. _reputation_for() is per-user, and calling it inside
    the serializer would be one leaderboard round trip per post.

    It returns the RANK KEY as well as the level now, because the key is the
    only thing a client can draw a crest from -- and because the level here is
    the XP engine's, numbered to a thousand. A driver's rank is one of thirty,
    and a feed card reading "LVL 445" beside a crest that goes up to thirty is
    two answers to the same question. The level is still carried for the
    clients that already take it; nothing should put it on screen.
    """
    ids = sorted({int(uid) for uid in author_ids})
    if not ids:
        return {}
    try:
        from leaderboard_service import get_progression_for_users
        progression = get_progression_for_users(ids) or {}
    except Exception:
        _LOGGER.warning("Could not read progression for a feed page", exc_info=True)
        return {}
    out: Dict[int, Dict[str, Any]] = {}
    for uid in ids:
        entry = progression.get(uid) or progression.get(str(uid)) or {}
        level = entry.get("level")
        key = str(entry.get("rank_icon_key") or "").strip()
        out[uid] = {
            "level": int(level) if isinstance(level, (int, float)) else None,
            "rank_icon_key": key or None,
            "rank_name": str(entry.get("rank_name") or "").strip() or None,
        }
    return out


def _author_standing(levels: Optional[Dict[int, Dict[str, Any]]], author_id: int) -> Dict[str, Any]:
    """One author's row out of that batch, with every field present.

    Present-but-null rather than absent: a client that has to test for the key
    before reading it ends up with three different spellings of "unknown".
    """
    entry = (levels or {}).get(int(author_id)) or {}
    return {
        "level": entry.get("level"),
        "rank_icon_key": entry.get("rank_icon_key"),
        "rank_name": entry.get("rank_name"),
    }


def _serialize(row: Any, viewer_id: int, counts: Dict[int, int], mine: set,
               levels: Optional[Dict[int, Optional[int]]] = None,
               comments: Optional[Dict[int, int]] = None) -> Dict[str, Any]:
    post_id = int(_row_value(row, "id", 0))
    author_id = int(_row_value(row, "user_id", 0))
    image_url, thumb_url = _image_urls(post_id, _row_value(row, "image_path"), _row_value(row, "has_thumb"))
    return {
        "id": post_id,
        "author": {
            "user_id": author_id,
            "display_name": str(_row_value(row, "display_name", "Driver")),
            "handle": _row_value(row, "handle"),
            "city": _row_value(row, "author_city"),
            "avatar_url": _avatar_url(row, author_id),
            # Who is talking, not just what they said. All optional: a driver
            # with no trips logged and no platform set still posts.
            #
            # rank_icon_key is what a card draws the crest from. `level` is the
            # XP engine's, out of a thousand, and is not for display -- the
            # number a driver knows is their rank out of thirty, which the
            # client derives from the key.
            **_author_standing(levels, author_id),
            "platforms": split_platforms(_row_value(row, "author_platforms")),
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
        # From this viewer's point of view: comments by people they blocked are
        # excluded, so the number under a post matches what opening it shows.
        "comment_count": (comments or {}).get(post_id, 0),
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
    levels = _author_levels([int(_row_value(r, "user_id", 0)) for r in rows])
    comments = comment_counts(post_ids, viewer_id)
    items = [_serialize(r, viewer_id, counts, mine, levels, comments) for r in rows]
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
    # The same post has to look the same however it was fetched. Skipping the
    # level here would drop the badge the moment a driver opened a post from
    # the feed that had one.
    author_id = int(_row_value(row, "user_id", 0))
    return _serialize(row, int(viewer_id), counts, mine, _author_levels([author_id]),
                      comment_counts([int(post_id)], int(viewer_id)))


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
            # dict(...)["id"], NOT [0]. The pool is built with
            # cursor_factory=RealDictCursor, so every Postgres row is dict-like
            # and [0] raises KeyError -- which is a 500 on every post. SQLite
            # returns tuples, so the whole test suite passed while production
            # could not create a single post. chat.py does it this way already.
            return int(_returned_id(cur))
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
    post = _live_post_or_404(post_id)
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
    _notify(_row_value(post, "user_id", 0), user_id, "like", post_id=post_id)
    return _like_summary(post_id, user_id)


def unlike_post(user_id: int, post_id: int) -> Dict[str, Any]:
    post = _live_post_or_404(post_id)
    _db_exec(
        "DELETE FROM post_likes WHERE post_id=? AND user_id=?",
        (int(post_id), int(user_id)),
    )
    # The like is gone, so the notification about it has to go too -- otherwise
    # the screen keeps saying something that is no longer true, and tapping it
    # shows a post with no like on it.
    _unnotify(_row_value(post, "user_id", 0), user_id, "like", post_id=post_id)
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
        "SELECT id, display_name, handle, city, avatar_url, avatar_version, bio, platforms, "
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
    _notify(followee_id, follower_id, "follow")
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
    # Same reason as an unlike: following and unfollowing repeatedly must not
    # leave a trail, and "started following you" from someone who no longer
    # does is noise.
    _unnotify(followee_id, follower_id, "follow")
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
        "level": None, "rank_icon_key": None, "rank_name": None,
        "title": None, "badge_code": None,
        "lifetime_miles": None, "lifetime_hours": None, "trips_logged": None,
    }
    try:
        from leaderboard_service import (
            get_best_current_badge_for_user,
            get_progression_for_user,
        )
        progression = get_progression_for_user(int(user_id)) or {}
        out["level"] = progression.get("level")
        # The key the crest is drawn from, and the only thing that says WHICH
        # of the thirty ranks this is. The level beside it is the XP engine's,
        # out of a thousand, and is not what a profile should print.
        out["rank_icon_key"] = str(progression.get("rank_icon_key") or "").strip() or None
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
    return {
        "user_id": int(user_id),
        "display_name": str(_row_value(row, "display_name", "Driver")),
        "handle": _row_value(row, "handle"),
        "city": _row_value(row, "city"),
        "avatar_url": _avatar_url(row, int(user_id)),
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


SEARCH_DRIVERS_MAX_LIMIT = 30


def search_drivers(viewer_id: int, query: str, limit: int = 20) -> Dict[str, Any]:
    """Find drivers by name or handle.

    A network you cannot search is a network you can only reach through
    whoever happens to post. Handles existed and were linkable; there was no
    way to find one without already knowing it.

    Matching is a prefix-first ranking rather than a plain LIKE, because
    "mar" should surface Marcus before it surfaces Omar: an exact handle,
    then a name or handle STARTING with the query, then anything containing
    it. The sort is stable on display name so the same query gives the same
    order twice.

    What it will not return, ever:
      - anyone blocked in either direction. The feed and the profile already
        hide them; a search that still listed them would be a way to check
        whether you had been blocked.
      - disabled or suspended accounts.
      - the viewer. Searching for people means other people.

    Muted drivers ARE returned. Muting hides someone's posts, it is not a
    statement that they should become unfindable, and a driver who mutes
    someone then wants to open their profile should be able to.
    """
    from social_identity import handle_key as _handle_key
    from social_moderation import blocked_either_way

    raw = str(query or "").strip()
    if len(raw) < 2:
        # One letter matches most of the network; that is a list, not a
        # search, and it is the expensive query to serve.
        return {"items": [], "query": raw}
    safe_limit = max(1, min(int(limit or 20), SEARCH_DRIVERS_MAX_LIMIT))

    # LIKE metacharacters in a user's own query are literal text to them.
    escaped = raw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    starts = f"{escaped}%"
    contains = f"%{escaped}%"
    exact_handle = _handle_key(raw) or ""

    rows = _db_query_all(
        """
        SELECT id, display_name, handle, city, avatar_url, avatar_version
        FROM users
        WHERE (LOWER(display_name) LIKE LOWER(?) ESCAPE '\\'
               OR LOWER(COALESCE(handle, '')) LIKE LOWER(?) ESCAPE '\\')
          AND COALESCE(is_disabled, 0) = 0
          AND COALESCE(is_suspended, 0) = 0
          AND id <> ?
        LIMIT ?
        """,
        (contains, contains, int(viewer_id), safe_limit * 4),
    ) or []

    hidden = blocked_either_way(int(viewer_id))
    lowered = raw.lower()

    def rank(row: Any) -> tuple:
        name = str(_row_value(row, "display_name", "") or "").lower()
        handle = str(_row_value(row, "handle", "") or "").lower()
        if exact_handle and handle == exact_handle:
            tier = 0
        elif handle.startswith(lowered) or name.startswith(lowered):
            tier = 1
        else:
            tier = 2
        return (tier, name, int(_row_value(row, "id", 0)))

    kept = [r for r in rows if int(_row_value(r, "id", 0)) not in hidden]
    kept.sort(key=rank)
    kept = kept[:safe_limit]

    standing = _author_levels([int(_row_value(r, "id", 0)) for r in kept])
    items = []
    for row in kept:
        uid = int(_row_value(row, "id", 0))
        items.append({
            "user_id": uid,
            "display_name": str(_row_value(row, "display_name", "Driver")),
            "handle": _row_value(row, "handle"),
            "city": _row_value(row, "city"),
            "avatar_url": _avatar_url(row, uid),
            # The crest goes beside a driver here for the same reason it does
            # in the feed: a name alone says who, a crest says who they are.
            **_author_standing(standing, uid),
        })
    return {"items": items, "query": raw}


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


# --------------------------------------------------------------------------
# comments
# --------------------------------------------------------------------------

_COMMENT_COLUMNS = """
    c.id AS id, c.post_id AS post_id, c.user_id AS user_id, c.body AS body,
    c.parent_id AS parent_id, c.created_at AS created_at,
    u.display_name AS display_name, u.handle AS handle, u.city AS author_city,
    u.avatar_url AS avatar_url, u.avatar_version AS avatar_version,
    u.platforms AS author_platforms
"""


def _comment_parents(rows: Iterable[Any]) -> Dict[int, Dict[str, Any]]:
    """Who wrote each comment that something in this page replied to.

    One query for the whole page rather than one per reply. Parents are almost
    always already in the page -- ids ascend and a thread is paged forwards, so
    a reply cannot arrive before what it answers -- but a deleted or
    blocked-away parent is not, and a reply whose "@name" silently vanishes is
    worse than one that keeps it.
    """
    ids = sorted({
        int(_row_value(r, "parent_id"))
        for r in rows
        if _row_value(r, "parent_id") is not None
    })
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    found = _db_query_all(
        f"SELECT c.id AS id, c.user_id AS user_id, u.display_name AS display_name, "
        f"u.handle AS handle FROM post_comments c JOIN users u ON u.id = c.user_id "
        f"WHERE c.id IN ({placeholders})",
        tuple(ids),
    )
    out: Dict[int, Dict[str, Any]] = {}
    for row in found:
        out[int(_row_value(row, "id", 0))] = {
            "user_id": int(_row_value(row, "user_id", 0)),
            "display_name": str(_row_value(row, "display_name", "Driver")),
            "handle": _row_value(row, "handle"),
        }
    return out


def _resolve_parent(post_id: int, parent_id: Optional[int]) -> Optional[int]:
    """A parent has to be a live comment on this same post, or it is not one.

    Without the post check a driver could hang a reply off a comment on
    somebody else's post and have it appear in a thread they cannot even see.
    """
    if parent_id in (None, 0, ""):
        return None
    try:
        wanted = int(parent_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="That reply does not exist")
    row = _db_query_one(
        "SELECT id FROM post_comments WHERE id=? AND post_id=? AND deleted_at IS NULL LIMIT 1",
        (wanted, int(post_id)),
    )
    if not row:
        raise HTTPException(status_code=404, detail="That reply does not exist")
    return wanted


def _comment_visibility(viewer_id: int) -> Tuple[str, List[Any]]:
    """The same rule the feed uses, on comments.

    A block that hides someone's posts but still shows their replies under
    yours is not a block. Written as a fragment for the same reason
    _visibility_clause is: a rule enforced in some of the places is not
    enforced.
    """
    parts = ["c.deleted_at IS NULL", "c.hidden_at IS NULL"]
    params: List[Any] = []
    hidden = hidden_author_ids(int(viewer_id))
    if hidden:
        placeholders = ",".join("?" for _ in hidden)
        parts.append(f"c.user_id NOT IN ({placeholders})")
        params.extend(int(uid) for uid in hidden)
    return " AND ".join(parts), params


def comment_counts(post_ids: Sequence[int], viewer_id: int) -> Dict[int, int]:
    """How many comments each post has, from this viewer's point of view.

    Blocked and muted authors are excluded here too, so the number under a post
    matches the number of comments that actually appear when it is opened. A
    count of 3 that opens to 1 reads as a bug.

    One grouped query for the whole page, like the like counts -- and no
    denormalised column, for the same reason: counters drift, joins do not.
    """
    ids = [int(pid) for pid in post_ids]
    if not ids:
        return {}
    visibility, visibility_params = _comment_visibility(int(viewer_id))
    placeholders = ",".join("?" for _ in ids)
    rows = _db_query_all(
        f"SELECT c.post_id AS post_id, COUNT(*) AS n FROM post_comments c "
        f"WHERE c.post_id IN ({placeholders}) AND {visibility} GROUP BY c.post_id",
        tuple(ids) + tuple(visibility_params),
    )
    out: Dict[int, int] = {}
    for row in rows:
        out[int(_row_value(row, "post_id", 0))] = int(_row_value(row, "n", 0))
    return out


def _serialize_comment(row: Any, viewer_id: int, post_author_id: int,
                       levels: Optional[Dict[int, Optional[int]]] = None,
                       parents: Optional[Dict[int, Dict[str, Any]]] = None) -> Dict[str, Any]:
    author_id = int(_row_value(row, "user_id", 0))
    mine = author_id == int(viewer_id)
    raw_parent = _row_value(row, "parent_id")
    parent_id = int(raw_parent) if raw_parent is not None else None
    return {
        "id": int(_row_value(row, "id", 0)),
        "post_id": int(_row_value(row, "post_id", 0)),
        # The comment this one answered, exactly -- null for a reply to the
        # post. The database keeps the real shape; the two-level render is the
        # client's, which is why nothing here clamps a depth.
        "parent_id": parent_id,
        # Who that was. A reply nested under a reply is drawn at the same
        # indent as its parent, so without a name on it there is nothing to say
        # which of the two it answered.
        "reply_to": (parents or {}).get(parent_id) if parent_id else None,
        "author": {
            "user_id": author_id,
            "display_name": str(_row_value(row, "display_name", "Driver")),
            "handle": _row_value(row, "handle"),
            "city": _row_value(row, "author_city"),
            "avatar_url": _avatar_url(row, author_id),
            **_author_standing(levels, author_id),
            "platforms": split_platforms(_row_value(row, "author_platforms")),
        },
        "body": str(_row_value(row, "body", "")),
        "mine": mine,
        # Your own comment, or anything under your own post. Sent rather than
        # left to the client to work out, so it cannot offer a delete that 403s.
        "can_delete": mine or int(post_author_id) == int(viewer_id),
        "created_at": int(_row_value(row, "created_at", 0)),
    }


def get_comments(viewer_id: int, post_id: int, limit: Optional[int] = None,
                 after_id: Optional[int] = None) -> Dict[str, Any]:
    """A page of comments, oldest first.

    Forwards, unlike the feed: a conversation is read in the order it happened,
    and paging forwards means new replies land at the end where a reader is
    already looking, instead of shifting everything they have read.
    """
    post = _live_post_or_404(post_id)
    # A post you cannot see has no comments you can see. Without this, a blocked
    # author's post is invisible in the feed but readable by id.
    post_visibility, post_params = _visibility_clause(int(viewer_id))
    visible = _db_query_one(
        f"SELECT p.id FROM posts p WHERE p.id=? AND p.deleted_at IS NULL AND {post_visibility} LIMIT 1",
        (int(post_id),) + tuple(post_params),
    )
    if not visible:
        raise HTTPException(status_code=404, detail="Post not found")

    limit = _clamp_limit(limit)
    fetch = limit + 1
    visibility, visibility_params = _comment_visibility(int(viewer_id))
    where = [f"c.post_id = ?", visibility]
    params: List[Any] = [int(post_id)] + list(visibility_params)
    if after_id:
        where.append("c.id > ?")
        params.append(int(after_id))

    rows = list(_db_query_all(
        f"SELECT {_COMMENT_COLUMNS} FROM post_comments c JOIN users u ON u.id = c.user_id "
        f"WHERE {' AND '.join(where)} ORDER BY c.id ASC LIMIT ?",
        tuple(params) + (fetch,),
    ))
    has_more = len(rows) > limit
    rows = rows[:limit]
    post_author_id = int(_row_value(post, "user_id", 0))
    levels = _author_levels([int(_row_value(r, "user_id", 0)) for r in rows])
    parents = _comment_parents(rows)
    items = [_serialize_comment(r, viewer_id, post_author_id, levels, parents) for r in rows]
    return {
        "post_id": int(post_id),
        "items": items,
        "next_after_id": items[-1]["id"] if (has_more and items) else None,
        "comment_count": comment_counts([int(post_id)], int(viewer_id)).get(int(post_id), 0),
    }


def create_comment(user: Any, post_id: int, body: str,
                   parent_id: Optional[int] = None) -> Dict[str, Any]:
    post = _live_post_or_404(post_id)
    viewer_id = int(_row_value(user, "id", 0))

    # You cannot reply to a post you are not allowed to see, and the check has
    # to be here rather than only on the read path: otherwise a blocked driver
    # can still put words under someone's photo.
    post_visibility, post_params = _visibility_clause(viewer_id)
    visible = _db_query_one(
        f"SELECT p.id FROM posts p WHERE p.id=? AND p.deleted_at IS NULL AND {post_visibility} LIMIT 1",
        (int(post_id),) + tuple(post_params),
    )
    if not visible:
        raise HTTPException(status_code=404, detail="Post not found")

    text = " ".join(str(body or "").split()).strip()
    if not text:
        raise HTTPException(status_code=400, detail="Say something first")
    text = text[:MAX_COMMENT_CHARS]

    # Checked before the insert, not after: a reply hung off a comment on
    # somebody else's post would appear in a thread its author cannot see.
    parent = _resolve_parent(int(post_id), parent_id)

    now = _now()
    columns = "post_id, user_id, parent_id, body, created_at"
    values = (int(post_id), viewer_id, parent, text, now)

    def _insert(_conn, cur) -> int:
        # RETURNING on Postgres, lastrowid on SQLite -- reading back "the newest
        # row for this user" would attach the wrong id when two comments land in
        # the same second.
        if DB_BACKEND == "postgres":
            cur.execute(_sql(f"INSERT INTO post_comments({columns}) VALUES(?,?,?,?,?) RETURNING id"), values)
            return int(_returned_id(cur))
        cur.execute(_sql(f"INSERT INTO post_comments({columns}) VALUES(?,?,?,?,?)"), values)
        return int(cur.lastrowid)

    comment_id = int(_db_run_in_transaction(_insert) or 0)

    row = _db_query_one(
        f"SELECT {_COMMENT_COLUMNS} FROM post_comments c JOIN users u ON u.id = c.user_id "
        f"WHERE c.id=? LIMIT 1",
        (comment_id,),
    )
    if not row:
        raise HTTPException(status_code=500, detail="Comment could not be read back")
    post_author_id = int(_row_value(post, "user_id", 0))

    # Two different people can be owed this one comment: whoever wrote the post,
    # and whoever wrote the comment being answered. Both are told, and _notify
    # drops the duplicate when they are the same person -- and drops it again
    # when either of them is the one typing.
    _notify(post_author_id, viewer_id, "comment", post_id=post_id, comment_id=comment_id)
    if parent:
        parent_row = _db_query_one(
            "SELECT user_id FROM post_comments WHERE id=? LIMIT 1", (int(parent),)
        )
        if parent_row:
            _notify(_row_value(parent_row, "user_id", 0), viewer_id, "reply",
                    post_id=post_id, comment_id=comment_id)
    levels = _author_levels([viewer_id])
    return {
        "comment": _serialize_comment(row, viewer_id, post_author_id, levels,
                                      _comment_parents([row])),
        "comment_count": comment_counts([int(post_id)], viewer_id).get(int(post_id), 0),
    }


def delete_comment(user_id: int, comment_id: int) -> Dict[str, Any]:
    """Your own comment, or anything under your own post.

    The post owner can remove replies because they are the one living with what
    appears under their photo -- the same reason a comment is soft deleted:
    a reply that vanishes takes the reply below it out of context, and the row
    staying keeps the thread readable to a moderator afterwards.
    """
    row = _db_query_one(
        "SELECT c.id AS id, c.post_id AS post_id, c.user_id AS user_id, "
        "p.user_id AS post_author_id "
        "FROM post_comments c JOIN posts p ON p.id = c.post_id "
        "WHERE c.id=? AND c.deleted_at IS NULL LIMIT 1",
        (int(comment_id),),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Comment not found")
    author_id = int(_row_value(row, "user_id", 0))
    post_author_id = int(_row_value(row, "post_author_id", 0))
    if int(user_id) not in (author_id, post_author_id):
        raise HTTPException(status_code=403, detail="That isn't yours to delete")

    post_id = int(_row_value(row, "post_id", 0))
    _db_exec("UPDATE post_comments SET deleted_at=? WHERE id=?", (_now(), int(comment_id)))
    return {
        "post_id": post_id,
        "comment_count": comment_counts([post_id], int(user_id)).get(post_id, 0),
    }


# --------------------------------------------------------------------------
# notifications
#
# The return loop. Everything above this point is a driver doing something to
# somebody else's post; none of it told that somebody. A social network where
# being liked, answered or followed is silent gives nobody a reason to come
# back, which is the whole reason this exists.
#
# Four rules, and each one is a thing that makes a notifications screen bad:
#
#   - You are never notified about yourself. Liking your own post is not news.
#   - One row per (recipient, actor, kind, subject). Like, unlike, like again
#     leaves one row, not three -- and an unlike takes its row away, because a
#     notification about a like that no longer exists is a lie.
#   - A block silences both directions, for the same reason follow does: it
#     must not be possible to learn you were blocked by watching what arrives.
#   - Writing one can never fail the thing that caused it. A like that 500s
#     because the notification insert raced is a worse bug than a missing
#     notification, so every write here is best-effort.
# --------------------------------------------------------------------------

NOTIFICATION_KINDS = ("like", "comment", "reply", "follow")

# What the screen can page through in one go. The same ceiling as the feed:
# nobody scrolls two hundred notifications, and an unbounded LIMIT is how one
# account with a popular post takes a page of the database with it.
_MAX_NOTIFICATIONS = 50


def _notify(user_id: Any, actor_id: Any, kind: str,
            post_id: Any = None, comment_id: Any = None) -> None:
    """Best effort, by design -- see the note above."""
    try:
        recipient = int(user_id or 0)
        actor = int(actor_id or 0)
    except (TypeError, ValueError):
        return
    if not recipient or not actor or recipient == actor:
        return
    if kind not in NOTIFICATION_KINDS:
        return
    try:
        if is_blocked_either_way(recipient, actor):
            return
        # 0 rather than NULL, so the unique index can do its job -- see
        # social_db.py. The INSERT is guarded by a read rather than relying on
        # the index alone, because a constraint violation on Postgres poisons
        # the transaction it happens in.
        subject_post = int(post_id or 0)
        subject_comment = int(comment_id or 0)
        existing = _db_query_one(
            "SELECT id FROM social_notifications WHERE user_id=? AND actor_id=? "
            "AND kind=? AND post_id=? AND comment_id=? LIMIT 1",
            (recipient, actor, str(kind), subject_post, subject_comment),
        )
        if existing:
            return
        _db_exec(
            "INSERT INTO social_notifications"
            "(user_id, actor_id, kind, post_id, comment_id, created_at) "
            "VALUES(?,?,?,?,?,?)",
            (recipient, actor, str(kind), subject_post, subject_comment, _now()),
        )
    except Exception:  # pragma: no cover - never break the caller
        _LOGGER.warning("notification write failed", exc_info=True)


def _unnotify(user_id: Any, actor_id: Any, kind: str,
              post_id: Any = None, comment_id: Any = None) -> None:
    """Take a notification back when the thing it describes is undone."""
    try:
        _db_exec(
            "DELETE FROM social_notifications WHERE user_id=? AND actor_id=? "
            "AND kind=? AND post_id=? AND comment_id=?",
            (int(user_id or 0), int(actor_id or 0), str(kind),
             int(post_id or 0), int(comment_id or 0)),
        )
    except Exception:  # pragma: no cover
        _LOGGER.warning("notification delete failed", exc_info=True)


def unread_notification_count(user_id: int) -> int:
    row = _db_query_one(
        "SELECT COUNT(*) AS n FROM social_notifications "
        "WHERE user_id=? AND read_at IS NULL",
        (int(user_id),),
    )
    return int(_row_value(row, "n", 0) or 0)


def _serialize_notification(row: Any, levels: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    actor_id = int(_row_value(row, "actor_id", 0))
    post_id = int(_row_value(row, "post_id", 0) or 0)
    comment_id = int(_row_value(row, "comment_id", 0) or 0)
    return {
        "id": int(_row_value(row, "id", 0)),
        "kind": str(_row_value(row, "kind", "") or ""),
        "created_at": int(_row_value(row, "created_at", 0) or 0),
        "read": _row_value(row, "read_at", None) is not None,
        "actor": {
            "user_id": actor_id,
            "display_name": str(_row_value(row, "actor_name", "") or "Driver"),
            "handle": _row_value(row, "actor_handle", None),
            "avatar_url": _avatar_url(row, actor_id),
            **_author_standing(levels, actor_id),
        },
        # 0 means "this kind has no subject"; the client shows nothing to tap.
        "post_id": post_id or None,
        "comment_id": comment_id or None,
        # The post's own words, so a notification says WHICH post without the
        # client having to fetch every one of them to find out.
        "post_excerpt": _notification_excerpt(row),
    }


def _notification_excerpt(row: Any) -> Optional[str]:
    body = _row_value(row, "post_body", None)
    if body is None:
        return None
    text = str(body).strip()
    if not text:
        return None
    return text if len(text) <= 80 else (text[:79].rstrip() + "…")


def get_notifications(user_id: int, limit: Optional[int] = None,
                      before_id: Optional[int] = None) -> Dict[str, Any]:
    """Newest first, keyset paged on id, exactly like the feed.

    A notification whose post has since been deleted is dropped rather than
    shown: "Marco liked your post" that opens nothing is worse than silence.
    The row is left alone -- posts are soft deleted and can come back.
    """
    viewer = int(user_id)
    take = max(1, min(int(limit or 20), _MAX_NOTIFICATIONS))
    where = ["n.user_id=?"]
    args: List[Any] = [viewer]
    if before_id:
        where.append("n.id < ?")
        args.append(int(before_id))
    args.append(take + 1)

    rows = _db_query_all(
        "SELECT n.id AS id, n.kind AS kind, n.created_at AS created_at, "
        "n.read_at AS read_at, n.post_id AS post_id, n.comment_id AS comment_id, "
        "u.id AS actor_id, u.display_name AS actor_name, u.handle AS actor_handle, "
        "u.avatar_url AS avatar_url, u.avatar_version AS avatar_version, "
        "p.body AS post_body, p.deleted_at AS post_deleted_at "
        "FROM social_notifications n "
        "JOIN users u ON u.id = n.actor_id "
        "LEFT JOIN posts p ON p.id = n.post_id "
        f"WHERE {' AND '.join(where)} ORDER BY n.id DESC LIMIT ?",
        tuple(args),
    ) or []

    # A blocked or muted actor goes quiet here too, so blocking someone clears
    # them out of this screen rather than only out of the feed.
    hidden = hidden_author_ids(viewer)
    kept = []
    for row in rows:
        if int(_row_value(row, "actor_id", 0)) in hidden:
            continue
        if int(_row_value(row, "post_id", 0) or 0) and _row_value(row, "post_deleted_at", None):
            continue
        kept.append(row)

    has_more = len(kept) > take
    page = kept[:take]
    levels = _author_levels([int(_row_value(r, "actor_id", 0)) for r in page])
    items = [_serialize_notification(row, levels) for row in page]
    return {
        "items": items,
        "next_before_id": items[-1]["id"] if (items and has_more) else None,
        "unread": unread_notification_count(viewer),
    }


def mark_notifications_read(user_id: int, before_id: Optional[int] = None) -> Dict[str, Any]:
    """Mark everything read, or everything down to a point.

    `before_id` is inclusive and exists so opening the screen cannot mark
    something read that arrived while it was open and was never on it.
    """
    args: List[Any] = [_now(), int(user_id)]
    sql = "UPDATE social_notifications SET read_at=? WHERE user_id=? AND read_at IS NULL"
    if before_id:
        sql += " AND id <= ?"
        args.append(int(before_id))
    _db_exec(sql, tuple(args))
    return {"unread": unread_notification_count(int(user_id))}
