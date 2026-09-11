"""Schema for the driver network: posts, likes, follows.

Three deliberate choices, because each one is the kind of thing that is painful
to change once there are rows:

1. Posts are SOFT deleted. `deleted_at` rather than DELETE, because a like or a
   follower count that silently changes when someone removes a post reads as a
   bug to everyone who saw the old number. Media IS unlinked on delete, so the
   bytes go even though the row stays.

2. There are NO denormalised counters. A like count lives in post_likes and is
   read with one grouped query per page of feed, not a column that has to be
   kept in step with the rows it counts. Counters drift; joins do not.

3. `city_key` is copied onto the post at write time. It is the author's city at
   the moment they posted, not a lookup through users -- so a driver who moves
   from Houston to Phoenix does not retroactively move a year of posts with
   them, and the Houston feed stays what Houston actually saw.

Unlike chat, nothing here expires. chat.py sweeps messages after 7 or 30 days
because a chat room is a conversation; a feed is a record, and a social network
whose posts evaporate is not a social network.
"""
from __future__ import annotations

from core import DB_BACKEND, _db_exec


def _try_exec(sql: str) -> None:
    """Index and ALTER statements that are safe to attempt repeatedly."""
    try:
        _db_exec(sql)
    except Exception:
        pass


def _ensure_user_city_columns() -> None:
    """`city` is what the driver typed; `city_key` is what we match on.

    Kept as two columns rather than normalising on read: matching needs to be
    exact and cheap, and display needs to keep the driver's own capitalisation.
    """
    if DB_BACKEND == "postgres":
        _try_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS city TEXT;")
        _try_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS city_key TEXT;")
    else:
        _try_exec("ALTER TABLE users ADD COLUMN city TEXT;")
        _try_exec("ALTER TABLE users ADD COLUMN city_key TEXT;")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_users_city_key ON users(city_key);")


def init_social_schema() -> None:
    _ensure_user_city_columns()

    if DB_BACKEND == "postgres":
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS posts (
              id BIGSERIAL PRIMARY KEY,
              user_id BIGINT NOT NULL,
              body TEXT NOT NULL DEFAULT '',
              image_path TEXT,
              image_mime_type TEXT,
              has_thumb BOOLEAN NOT NULL DEFAULT FALSE,
              city_key TEXT,
              lat DOUBLE PRECISION,
              lng DOUBLE PRECISION,
              zone_name TEXT,
              zone_rating INTEGER,
              created_at BIGINT NOT NULL,
              deleted_at BIGINT,
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS follows (
              follower_id BIGINT NOT NULL,
              followee_id BIGINT NOT NULL,
              created_at BIGINT NOT NULL,
              PRIMARY KEY(follower_id, followee_id),
              FOREIGN KEY(follower_id) REFERENCES users(id),
              FOREIGN KEY(followee_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS post_likes (
              post_id BIGINT NOT NULL,
              user_id BIGINT NOT NULL,
              created_at BIGINT NOT NULL,
              PRIMARY KEY(post_id, user_id),
              FOREIGN KEY(post_id) REFERENCES posts(id),
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
    else:
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS posts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              body TEXT NOT NULL DEFAULT '',
              image_path TEXT,
              image_mime_type TEXT,
              has_thumb INTEGER NOT NULL DEFAULT 0,
              city_key TEXT,
              lat REAL,
              lng REAL,
              zone_name TEXT,
              zone_rating INTEGER,
              created_at INTEGER NOT NULL,
              deleted_at INTEGER,
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS follows (
              follower_id INTEGER NOT NULL,
              followee_id INTEGER NOT NULL,
              created_at INTEGER NOT NULL,
              PRIMARY KEY(follower_id, followee_id),
              FOREIGN KEY(follower_id) REFERENCES users(id),
              FOREIGN KEY(followee_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS post_likes (
              post_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              created_at INTEGER NOT NULL,
              PRIMARY KEY(post_id, user_id),
              FOREIGN KEY(post_id) REFERENCES posts(id),
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )

    # Every feed query is "newest first, not deleted", narrowed by author or by
    # city -- so the indexes lead with the narrowing column and end with id DESC,
    # which is also the keyset cursor.
    _try_exec("CREATE INDEX IF NOT EXISTS idx_posts_live ON posts(deleted_at, id DESC);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(user_id, deleted_at, id DESC);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_posts_city ON posts(city_key, deleted_at, id DESC);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_follows_followee ON follows(followee_id);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_post_likes_post ON post_likes(post_id);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_post_likes_user ON post_likes(user_id, post_id);")
