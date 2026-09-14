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

4. Comments store the REAL shape and are DRAWN two levels deep. `parent_id` is
   exactly the comment that was answered -- null for a reply to the post -- and
   the client renders a reply to a reply-to-a-reply at the same indent as its
   parent, with an "@name" saying which of the two it answered.

   This started as one level, and the reasons written here for keeping it that
   way were that a tree needs collapsing, "load more replies", and an order
   that is not just time. Drawing two levels needs none of them: the indent
   cannot grow, so nothing has to collapse; every reply is in the one
   forward-paged stream ordered by id, so there is no second query to "load
   more replies"; and the order is still just time.

   Flattening in the database instead would have been less code here and a
   worse record: it throws away which comment was actually answered, which is
   precisely what the "@name" needs. Storage keeps the truth, the render
   decides how much of it fits on a phone.

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


def _ensure_comment_parent_column() -> None:
    """The CREATE TABLE above only runs on a database that does not exist yet.

    Every database that already has comments in it needs the column added, and
    this is the only way it gets there. Null everywhere it lands, which is what
    a reply to the post means -- so every existing comment keeps the shape it
    already had and nothing has to be backfilled.
    """
    if DB_BACKEND == "postgres":
        _try_exec("ALTER TABLE post_comments ADD COLUMN IF NOT EXISTS parent_id BIGINT;")
    else:
        _try_exec("ALTER TABLE post_comments ADD COLUMN parent_id INTEGER;")
    # Reading a thread groups children by parent; without this that is a scan
    # of every comment on the post for every parent in it.
    _try_exec(
        "CREATE INDEX IF NOT EXISTS idx_post_comments_parent "
        "ON post_comments(post_id, parent_id);"
    )


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
            CREATE TABLE IF NOT EXISTS post_comments (
              id BIGSERIAL PRIMARY KEY,
              post_id BIGINT NOT NULL,
              user_id BIGINT NOT NULL,
              -- The comment this one answered, exactly; null for a reply to
              -- the post. Not clamped: the client draws two levels, and
              -- flattening here would lose which comment the "@name" refers to.
              parent_id BIGINT,
              body TEXT NOT NULL,
              created_at BIGINT NOT NULL,
              deleted_at BIGINT,
              hidden_at BIGINT,
              hidden_by BIGINT,
              hidden_reason TEXT,
              FOREIGN KEY(post_id) REFERENCES posts(id),
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
            CREATE TABLE IF NOT EXISTS post_comments (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              post_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              -- See the Postgres table above.
              parent_id INTEGER,
              body TEXT NOT NULL,
              created_at INTEGER NOT NULL,
              deleted_at INTEGER,
              hidden_at INTEGER,
              hidden_by INTEGER,
              hidden_reason TEXT,
              FOREIGN KEY(post_id) REFERENCES posts(id),
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
    # Comments read oldest-first within one post (a conversation runs forwards,
    # unlike a feed), so this index ends with id ASC rather than DESC.
    _try_exec("CREATE INDEX IF NOT EXISTS idx_post_comments_post "
              "ON post_comments(post_id, deleted_at, id);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_post_comments_author "
              "ON post_comments(user_id, deleted_at);")
    # After the CREATE TABLE above, not before it: on a database that does not
    # exist yet there is nothing to ALTER, and the index would have no column.
    _ensure_comment_parent_column()
    _try_exec("CREATE INDEX IF NOT EXISTS idx_follows_followee ON follows(followee_id);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_post_likes_post ON post_likes(post_id);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_post_likes_user ON post_likes(user_id, post_id);")
