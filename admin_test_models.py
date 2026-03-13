from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel


class AdminDiagnosticResponse(BaseModel):
    ok: bool
    test_name: str
    checked_at: str
    summary: str
    details: Dict[str, Any]
