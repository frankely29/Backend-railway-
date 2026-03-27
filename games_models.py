from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


class GameChallengeCreateIn(BaseModel):
    target_user_id: Optional[int] = None
    challenged_user_id: Optional[int] = None
    game_type: Optional[str] = None
    game_key: Optional[str] = None


class GameMoveIn(BaseModel):
    move_type: str
    tile: Optional[list[int]] = None
    side: Optional[str] = None
    angle: Optional[float] = None
    power: Optional[float] = None
    shot_input: Optional[dict[str, Any]] = None
    result_state: Optional[dict[str, Any]] = None
