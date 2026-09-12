"""Block, mute and report — the controls that have to exist before strangers
can post photos to strangers.

The three are deliberately different tools, because conflating them is how
moderation features end up useless:

  BLOCK is mutual and structural. Neither of you sees the other anywhere, and
  the follow edges between you are severed in both directions. It is the tool
  for "this person is a problem", and it has to be symmetric: a block that only
  hides them from you still lets them read everything you write, which is not
  what anyone means by blocking.

  MUTE is one-way and silent. Their posts leave your feed; nothing else changes
  and they are never told. It is the tool for "I don't want to read this", which
  is a far more common need than "this person is dangerous" — and offering only
  the nuclear option means people use the nuclear option.

  REPORT tells someone with authority. It changes nothing the reporter sees, so
  it is paired with a block or a mute in the UI rather than used alone.

A moderator hiding a post is NOT the same as its author deleting it, so it is a
separate column. Collapsing them loses the distinction between "the author
changed their mind" and "we took this down", which is exactly the thing you need
to know when the author asks why their post vanished.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Set

from fastapi import HTTPException

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one

MAX_NOTE_CHARS = 500

REPORT_REASONS = [
    "harassment",       # targeted abuse of a person
    "spam",             # advertising, referral links, repetition
    "nudity",           # sexual content
    "violence",         # threats or graphic content
    "hate",             # attacks on a protected characteristic
    "impersonation",    # pretending to be another driver or the product
    "personal_info",    # someone's plate, address, phone number
    "other",
]

REPORT_TARGETS = ["post", "user", "chat_message"]

REPORT_OPEN = "open"
REPORT_ACTIONED = "actioned"
REPORT_DISMISSED = "dismissed"
REPORT_STATUSES = [REPORT_OPEN, REPORT_ACTIONED, REPORT_DISMISSED]


def _now() -> int:
    return int(time.time())


def _row_value(row: Any, key: str, default: Any = None) -> Any:
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


def _try_exec(sql: str) -> None:
    try:
        _db_exec(sql)
    except Exception:
        pass


# --------------------------------------------------------------------------
# schema
# --------------------------------------------------------------------------

def ensure_moderation_schema() -> None:
    if DB_BACKEND == "postgres":
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS user_blocks (
              blocker_id BIGINT NOT NULL,
              blocked_id BIGINT NOT NULL,
              created_at BIGINT NOT NULL,
              PRIMARY KEY(blocker_id, blocked_id),
              FOREIGN KEY(blocker_id) REFERENCES users(id),
              FOREIGN KEY(blocked_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS user_mutes (
              muter_id BIGINT NOT NULL,
              muted_id BIGINT NOT NULL,
              created_at BIGINT NOT NULL,
              PRIMARY KEY(muter_id, muted_id),
              FOREIGN KEY(muter_id) REFERENCES users(id),
              FOREIGN KEY(muted_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS content_reports (
              id BIGSERIAL PRIMARY KEY,
              reporter_id BIGINT NOT NULL,
              target_type TEXT NOT NULL,
              target_id BIGINT NOT NULL,
              target_user_id BIGINT,
              reason TEXT NOT NULL,
              note TEXT,
              status TEXT NOT NULL DEFAULT 'open',
              created_at BIGINT NOT NULL,
              resolved_at BIGINT,
              resolved_by BIGINT,
              resolution TEXT,
              FOREIGN KEY(reporter_id) REFERENCES users(id)
            );
            """
        )
        _try_exec("ALTER TABLE posts ADD COLUMN IF NOT EXISTS hidden_at BIGINT;")
        _try_exec("ALTER TABLE posts ADD COLUMN IF NOT EXISTS hidden_by BIGINT;")
        _try_exec("ALTER TABLE posts ADD COLUMN IF NOT EXISTS hidden_reason TEXT;")
    else:
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS user_blocks (
              blocker_id INTEGER NOT NULL,
              blocked_id INTEGER NOT NULL,
              created_at INTEGER NOT NULL,
              PRIMARY KEY(blocker_id, blocked_id),
              FOREIGN KEY(blocker_id) REFERENCES users(id),
              FOREIGN KEY(blocked_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS user_mutes (
              muter_id INTEGER NOT NULL,
              muted_id INTEGER NOT NULL,
              created_at INTEGER NOT NULL,
              PRIMARY KEY(muter_id, muted_id),
              FOREIGN KEY(muter_id) REFERENCES users(id),
              FOREIGN KEY(muted_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS content_reports (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              reporter_id INTEGER NOT NULL,
              target_type TEXT NOT NULL,
              target_id INTEGER NOT NULL,
              target_user_id INTEGER,
              reason TEXT NOT NULL,
              note TEXT,
              status TEXT NOT NULL DEFAULT 'open',
              created_at INTEGER NOT NULL,
              resolved_at INTEGER,
              resolved_by INTEGER,
              resolution TEXT,
              FOREIGN KEY(reporter_id) REFERENCES users(id)
            );
            """
        )
        _try_exec("ALTER TABLE posts ADD COLUMN hidden_at INTEGER;")
        _try_exec("ALTER TABLE posts ADD COLUMN hidden_by INTEGER;")
        _try_exec("ALTER TABLE posts ADD COLUMN hidden_reason TEXT;")

    _try_exec("CREATE INDEX IF NOT EXISTS idx_blocks_blocked ON user_blocks(blocked_id);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_mutes_muter ON user_mutes(muter_id);")
    # The moderation queue is read as "open reports, oldest first" -- the oldest
    # open report is the one that has been waiting longest, which is the one
    # that matters.
    _try_exec("CREATE INDEX IF NOT EXISTS idx_reports_queue ON content_reports(status, created_at);")
    _try_exec("CREATE INDEX IF NOT EXISTS idx_reports_target ON content_reports(target_type, target_id);")
    _try_exec(
        "CREATE INDEX IF NOT EXISTS idx_reports_reporter ON content_reports(reporter_id, target_type, target_id);")


# --------------------------------------------------------------------------
# blocks
# --------------------------------------------------------------------------

def _user_exists(user_id: int) -> bool:
    return _db_query_one("SELECT id FROM users WHERE id=? LIMIT 1", (int(user_id),)) is not None


def block_user(blocker_id: int, blocked_id: int) -> Dict[str, Any]:
    """Blocking also severs the follow edges, in both directions.

    Leaving them in place means a blocked person still appears in your follower
    count and still receives your posts in their following feed -- which is
    exactly what the block was for.
    """
    blocker_id, blocked_id = int(blocker_id), int(blocked_id)
    if blocker_id == blocked_id:
        raise HTTPException(status_code=400, detail="You can't block yourself")
    if not _user_exists(blocked_id):
        raise HTTPException(status_code=404, detail="Driver not found")

    existing = _db_query_one(
        "SELECT blocker_id FROM user_blocks WHERE blocker_id=? AND blocked_id=? LIMIT 1",
        (blocker_id, blocked_id),
    )
    if not existing:
        _db_exec(
            "INSERT INTO user_blocks(blocker_id, blocked_id, created_at) VALUES(?,?,?)",
            (blocker_id, blocked_id, _now()),
        )
    _db_exec(
        "DELETE FROM follows WHERE (follower_id=? AND followee_id=?) OR (follower_id=? AND followee_id=?)",
        (blocker_id, blocked_id, blocked_id, blocker_id),
    )
    return {"user_id": blocked_id, "blocked": True}


def unblock_user(blocker_id: int, blocked_id: int) -> Dict[str, Any]:
    """Unblocking does not restore the follows. They were a relationship, and
    undoing a block is not the same as asking for it back."""
    _db_exec(
        "DELETE FROM user_blocks WHERE blocker_id=? AND blocked_id=?",
        (int(blocker_id), int(blocked_id)),
    )
    return {"user_id": int(blocked_id), "blocked": False}


def blocked_either_way(user_id: int) -> Set[int]:
    """Everyone invisible to this user because of a block, in either direction."""
    rows = _db_query_all(
        "SELECT blocked_id AS other FROM user_blocks WHERE blocker_id=? "
        "UNION SELECT blocker_id AS other FROM user_blocks WHERE blocked_id=?",
        (int(user_id), int(user_id)),
    )
    return {int(_row_value(r, "other", 0)) for r in rows}


def is_blocked_either_way(a: int, b: int) -> bool:
    row = _db_query_one(
        "SELECT blocker_id FROM user_blocks "
        "WHERE (blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?) LIMIT 1",
        (int(a), int(b), int(b), int(a)),
    )
    return row is not None


def muted_by(user_id: int) -> Set[int]:
    rows = _db_query_all("SELECT muted_id FROM user_mutes WHERE muter_id=?", (int(user_id),))
    return {int(_row_value(r, "muted_id", 0)) for r in rows}


def hidden_author_ids(viewer_id: int) -> Set[int]:
    """Authors whose posts must not appear in this viewer's feeds."""
    return blocked_either_way(viewer_id) | muted_by(viewer_id)


# --------------------------------------------------------------------------
# mutes
# --------------------------------------------------------------------------

def mute_user(muter_id: int, muted_id: int) -> Dict[str, Any]:
    muter_id, muted_id = int(muter_id), int(muted_id)
    if muter_id == muted_id:
        raise HTTPException(status_code=400, detail="You can't mute yourself")
    if not _user_exists(muted_id):
        raise HTTPException(status_code=404, detail="Driver not found")
    existing = _db_query_one(
        "SELECT muter_id FROM user_mutes WHERE muter_id=? AND muted_id=? LIMIT 1",
        (muter_id, muted_id),
    )
    if not existing:
        _db_exec(
            "INSERT INTO user_mutes(muter_id, muted_id, created_at) VALUES(?,?,?)",
            (muter_id, muted_id, _now()),
        )
    return {"user_id": muted_id, "muted": True}


def unmute_user(muter_id: int, muted_id: int) -> Dict[str, Any]:
    _db_exec(
        "DELETE FROM user_mutes WHERE muter_id=? AND muted_id=?",
        (int(muter_id), int(muted_id)),
    )
    return {"user_id": int(muted_id), "muted": False}


def _list_relationship(sql: str, user_id: int) -> List[Dict[str, Any]]:
    rows = _db_query_all(sql, (int(user_id),))
    return [
        {
            "user_id": int(_row_value(r, "id", 0)),
            "display_name": str(_row_value(r, "display_name", "Driver")),
            "handle": _row_value(r, "handle"),
            "created_at": int(_row_value(r, "created_at", 0)),
        }
        for r in rows
    ]


def list_blocked(user_id: int) -> List[Dict[str, Any]]:
    return _list_relationship(
        "SELECT u.id, u.display_name, u.handle, b.created_at FROM user_blocks b "
        "JOIN users u ON u.id = b.blocked_id WHERE b.blocker_id=? ORDER BY b.created_at DESC",
        user_id,
    )


def list_muted(user_id: int) -> List[Dict[str, Any]]:
    return _list_relationship(
        "SELECT u.id, u.display_name, u.handle, m.created_at FROM user_mutes m "
        "JOIN users u ON u.id = m.muted_id WHERE m.muter_id=? ORDER BY m.created_at DESC",
        user_id,
    )


# --------------------------------------------------------------------------
# reports
# --------------------------------------------------------------------------

def _clean_note(note: Optional[str]) -> Optional[str]:
    if not note:
        return None
    collapsed = " ".join(str(note).split())[:MAX_NOTE_CHARS].strip()
    return collapsed or None


def _resolve_target_owner(target_type: str, target_id: int) -> Optional[int]:
    """Who the report is actually about, so the queue can group by person."""
    if target_type == "post":
        row = _db_query_one("SELECT user_id FROM posts WHERE id=? LIMIT 1", (int(target_id),))
        if not row:
            raise HTTPException(status_code=404, detail="Post not found")
        return int(_row_value(row, "user_id", 0))
    if target_type == "user":
        if not _user_exists(int(target_id)):
            raise HTTPException(status_code=404, detail="Driver not found")
        return int(target_id)
    if target_type == "chat_message":
        for table, column in (("chat_messages", "user_id"),
                              ("private_chat_messages", "sender_user_id")):
            try:
                row = _db_query_one(
                    f"SELECT {column} AS owner FROM {table} WHERE id=? LIMIT 1", (int(target_id),))
            except Exception:
                row = None
            if row:
                return int(_row_value(row, "owner", 0))
        # Chat is swept on a retention timer, so a message can legitimately be
        # gone by the time someone reports it. That is worth recording, not
        # rejecting -- the report still tells you something.
        return None
    return None


def create_report(reporter_id: int, target_type: str, target_id: int,
                  reason: str, note: Optional[str] = None) -> Dict[str, Any]:
    reporter_id = int(reporter_id)
    if target_type not in REPORT_TARGETS:
        raise HTTPException(status_code=400, detail="Unknown report target")
    if reason not in REPORT_REASONS:
        raise HTTPException(status_code=400, detail="Unknown report reason")

    owner = _resolve_target_owner(target_type, int(target_id))
    if owner is not None and owner == reporter_id:
        raise HTTPException(status_code=400, detail="You can't report your own content")

    # One open report per person per thing. Without this, tapping Report twice
    # -- or a frustrated person tapping it ten times -- floods the queue and
    # buries the reports nobody has seen yet.
    duplicate = _db_query_one(
        "SELECT id FROM content_reports WHERE reporter_id=? AND target_type=? AND target_id=? "
        "AND status=? LIMIT 1",
        (reporter_id, target_type, int(target_id), REPORT_OPEN),
    )
    if duplicate:
        return {"report_id": int(_row_value(duplicate, "id", 0)), "duplicate": True}

    _db_exec(
        "INSERT INTO content_reports(reporter_id, target_type, target_id, target_user_id, "
        "reason, note, status, created_at) VALUES(?,?,?,?,?,?,?,?)",
        (reporter_id, target_type, int(target_id), owner, reason,
         _clean_note(note), REPORT_OPEN, _now()),
    )
    row = _db_query_one(
        "SELECT id FROM content_reports WHERE reporter_id=? AND target_type=? AND target_id=? "
        "ORDER BY id DESC LIMIT 1",
        (reporter_id, target_type, int(target_id)),
    )
    return {"report_id": int(_row_value(row, "id", 0)), "duplicate": False}


def list_reports(status: Optional[str] = REPORT_OPEN, limit: int = 50,
                 before_id: Optional[int] = None) -> Dict[str, Any]:
    where: List[str] = []
    params: List[Any] = []
    if status:
        if status not in REPORT_STATUSES:
            raise HTTPException(status_code=400, detail="Unknown status")
        where.append("r.status = ?")
        params.append(status)
    if before_id:
        where.append("r.id < ?")
        params.append(int(before_id))
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    limit = max(1, min(200, int(limit)))
    rows = _db_query_all(
        "SELECT r.id, r.reporter_id, r.target_type, r.target_id, r.target_user_id, r.reason, "
        "r.note, r.status, r.created_at, r.resolved_at, r.resolved_by, r.resolution, "
        "reporter.display_name AS reporter_name, target.display_name AS target_name, "
        "target.handle AS target_handle "
        "FROM content_reports r "
        "LEFT JOIN users reporter ON reporter.id = r.reporter_id "
        "LEFT JOIN users target ON target.id = r.target_user_id "
        f"{clause} ORDER BY r.id DESC LIMIT ?",
        tuple(params) + (limit + 1,),
    )
    rows = list(rows)
    has_more = len(rows) > limit
    rows = rows[:limit]
    items = [
        {
            "id": int(_row_value(r, "id", 0)),
            "reporter_id": int(_row_value(r, "reporter_id", 0)),
            "reporter_name": _row_value(r, "reporter_name"),
            "target_type": str(_row_value(r, "target_type", "")),
            "target_id": int(_row_value(r, "target_id", 0)),
            "target_user_id": _row_value(r, "target_user_id"),
            "target_name": _row_value(r, "target_name"),
            "target_handle": _row_value(r, "target_handle"),
            "reason": str(_row_value(r, "reason", "")),
            "note": _row_value(r, "note"),
            "status": str(_row_value(r, "status", REPORT_OPEN)),
            "created_at": int(_row_value(r, "created_at", 0)),
            "resolved_at": _row_value(r, "resolved_at"),
            "resolved_by": _row_value(r, "resolved_by"),
            "resolution": _row_value(r, "resolution"),
        }
        for r in rows
    ]
    return {
        "items": items,
        "next_before_id": items[-1]["id"] if (has_more and items) else None,
        "open_count": open_report_count(),
    }


def open_report_count() -> int:
    row = _db_query_one(
        "SELECT COUNT(*) AS n FROM content_reports WHERE status=?", (REPORT_OPEN,))
    return int(_row_value(row, "n", 0))


def resolve_report(admin_id: int, report_id: int, status: str,
                   resolution: Optional[str] = None) -> Dict[str, Any]:
    if status not in (REPORT_ACTIONED, REPORT_DISMISSED):
        raise HTTPException(status_code=400, detail="Resolve to actioned or dismissed")
    row = _db_query_one("SELECT id, status FROM content_reports WHERE id=? LIMIT 1",
                        (int(report_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Report not found")
    _db_exec(
        "UPDATE content_reports SET status=?, resolved_at=?, resolved_by=?, resolution=? WHERE id=?",
        (status, _now(), int(admin_id), _clean_note(resolution), int(report_id)),
    )
    return {"report_id": int(report_id), "status": status}


# --------------------------------------------------------------------------
# taking a post down
# --------------------------------------------------------------------------

def hide_post(admin_id: int, post_id: int, reason: Optional[str] = None) -> Dict[str, Any]:
    """A moderator taking a post down, recorded separately from the author
    deleting it -- so "why did my post disappear" has an answer."""
    row = _db_query_one("SELECT id FROM posts WHERE id=? LIMIT 1", (int(post_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Post not found")
    _db_exec(
        "UPDATE posts SET hidden_at=?, hidden_by=?, hidden_reason=? WHERE id=?",
        (_now(), int(admin_id), _clean_note(reason), int(post_id)),
    )
    # Every open report about this post is now answered.
    _db_exec(
        "UPDATE content_reports SET status=?, resolved_at=?, resolved_by=?, resolution=? "
        "WHERE target_type=? AND target_id=? AND status=?",
        (REPORT_ACTIONED, _now(), int(admin_id), "post hidden", "post", int(post_id), REPORT_OPEN),
    )
    return {"post_id": int(post_id), "hidden": True}


def unhide_post(admin_id: int, post_id: int) -> Dict[str, Any]:
    row = _db_query_one("SELECT id FROM posts WHERE id=? LIMIT 1", (int(post_id),))
    if not row:
        raise HTTPException(status_code=404, detail="Post not found")
    _db_exec(
        "UPDATE posts SET hidden_at=NULL, hidden_by=NULL, hidden_reason=NULL WHERE id=?",
        (int(post_id),),
    )
    return {"post_id": int(post_id), "hidden": False}
