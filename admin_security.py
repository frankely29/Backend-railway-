from __future__ import annotations

import sqlite3

from fastapi import Depends, HTTPException, Request

from core import _auth_user_from_request


def _flag_to_int(value) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if value is None:
        return 0
    return int(value)


def require_admin_user(req: Request) -> sqlite3.Row:
    user = _auth_user_from_request(req)
    if _flag_to_int(user["is_admin"]) != 1:
        raise HTTPException(status_code=403, detail="Admin only")
    return user


AdminUserDep = Depends(require_admin_user)
