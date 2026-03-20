from __future__ import annotations

import random
from typing import Any, Iterable

from fastapi import HTTPException

DOMINO_SET: list[tuple[int, int]] = [(a, b) for a in range(7) for b in range(a, 7)]


def _seed_for_match(match_id: int) -> int:
    return 7000 + int(match_id) * 17


def normalize_tile(tile: Iterable[int]) -> tuple[int, int]:
    values = list(tile)
    if len(values) != 2:
        raise HTTPException(status_code=400, detail="Domino tile must contain two pips")
    a = int(values[0])
    b = int(values[1])
    if not (0 <= a <= 6 and 0 <= b <= 6):
        raise HTTPException(status_code=400, detail="Domino tile pips must be between 0 and 6")
    return tuple(sorted((a, b)))


def create_initial_state(player_one_user_id: int, player_two_user_id: int, match_id: int) -> dict[str, Any]:
    deck = DOMINO_SET.copy()
    random.Random(_seed_for_match(match_id)).shuffle(deck)
    player_one_hand = [list(tile) for tile in deck[:7]]
    player_two_hand = [list(tile) for tile in deck[7:14]]
    stock = [list(tile) for tile in deck[14:]]
    first_turn = min(int(player_one_user_id), int(player_two_user_id))
    return {
        "game_type": "dominoes",
        "rules": "Double-six draw dominoes. Draw when blocked, pass only when no legal move and the boneyard is empty. Lower hand pip total wins blocked rounds.",
        "board": [],
        "left_end": None,
        "right_end": None,
        "hands": {
            str(int(player_one_user_id)): player_one_hand,
            str(int(player_two_user_id)): player_two_hand,
        },
        "stock": stock,
        "boneyard": stock,
        "passes_in_row": 0,
        "last_action": None,
        "turn_user_id": first_turn,
        "result_summary": None,
    }


def _tile_in_hand(hand: list[list[int]], tile: tuple[int, int]) -> bool:
    return list(tile) in hand


def legal_play_sides(state: dict[str, Any], tile: tuple[int, int]) -> set[str]:
    board = state.get("board") or []
    if not board:
        return {"left", "right"}
    left_end = int(state["left_end"])
    right_end = int(state["right_end"])
    sides: set[str] = set()
    a, b = tile
    if a == left_end or b == left_end:
        sides.add("left")
    if a == right_end or b == right_end:
        sides.add("right")
    return sides


def _orient_tile_for_side(tile: tuple[int, int], side: str, state: dict[str, Any]) -> list[int]:
    a, b = tile
    if not state.get("board"):
        return [a, b]
    if side == "left":
        left_end = int(state["left_end"])
        return [b, a] if a == left_end else [a, b]
    right_end = int(state["right_end"])
    return [a, b] if a == right_end else [b, a]


def hand_total(hand: list[list[int]]) -> int:
    return sum(int(tile[0]) + int(tile[1]) for tile in hand)


def player_has_legal_play(state: dict[str, Any], user_id: int) -> bool:
    hand = state.get("hands", {}).get(str(int(user_id)), [])
    return any(legal_play_sides(state, tuple(tile)) for tile in hand)


def _opponent_user_id(player_one_user_id: int, player_two_user_id: int, actor_user_id: int) -> int:
    return int(player_one_user_id) if int(player_two_user_id) == int(actor_user_id) else int(player_two_user_id)


def finalize_blocked(player_one_user_id: int, player_two_user_id: int, state: dict[str, Any]) -> dict[str, Any]:
    player_one = int(player_one_user_id)
    player_two = int(player_two_user_id)
    hand_one = state["hands"].get(str(player_one), [])
    hand_two = state["hands"].get(str(player_two), [])
    total_one = hand_total(hand_one)
    total_two = hand_total(hand_two)
    if total_one < total_two:
        winner, loser = player_one, player_two
    elif total_two < total_one:
        winner, loser = player_two, player_one
    else:
        winner, loser = min(player_one, player_two), max(player_one, player_two)
    state["result_summary"] = {
        "reason": "blocked",
        "player_one_pips": total_one,
        "player_two_pips": total_two,
    }
    return {"winner_user_id": winner, "loser_user_id": loser, "state": state}


def apply_move(
    state: dict[str, Any],
    *,
    player_one_user_id: int,
    player_two_user_id: int,
    actor_user_id: int,
    move: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    move_type = str(move.get("move_type") or "").strip().lower()
    actor_key = str(int(actor_user_id))
    hand = list(state.get("hands", {}).get(actor_key, []))
    if move_type == "play_tile":
        tile = normalize_tile(move.get("tile") or [])
        side = str(move.get("side") or "").strip().lower()
        if side not in {"left", "right"}:
            raise HTTPException(status_code=400, detail="Dominoes play_tile requires side=left or side=right")
        if not _tile_in_hand(hand, tile):
            raise HTTPException(status_code=409, detail="Tile is not in your hand")
        legal_sides = legal_play_sides(state, tile)
        if side not in legal_sides:
            raise HTTPException(status_code=409, detail="That tile cannot be played on the requested side")
        oriented = _orient_tile_for_side(tile, side, state)
        hand.remove(list(tile))
        board = list(state.get("board") or [])
        if not board:
            board = [oriented]
        elif side == "left":
            board.insert(0, oriented)
        else:
            board.append(oriented)
        state["board"] = board
        state["left_end"] = int(board[0][0])
        state["right_end"] = int(board[-1][1])
        state["hands"][actor_key] = hand
        state["passes_in_row"] = 0
        state["last_action"] = {"type": "play_tile", "tile": oriented, "side": side, "actor_user_id": int(actor_user_id)}
        if not hand:
            state["result_summary"] = {"reason": "emptied_hand"}
            other = _opponent_user_id(player_one_user_id, player_two_user_id, actor_user_id)
            return state, {"completed": True, "winner_user_id": int(actor_user_id), "loser_user_id": other}
        state["turn_user_id"] = _opponent_user_id(player_one_user_id, player_two_user_id, actor_user_id)
        return state, {"completed": False}

    if move_type == "draw_tile":
        if player_has_legal_play(state, actor_user_id):
            raise HTTPException(status_code=409, detail="You already have a legal dominoes play")
        stock = list(state.get("stock") or state.get("boneyard") or [])
        if not stock:
            raise HTTPException(status_code=409, detail="Boneyard is empty")
        drawn_tile = stock.pop(0)
        hand.append(drawn_tile)
        state["stock"] = stock
        state["boneyard"] = stock
        state["hands"][actor_key] = hand
        state["last_action"] = {"type": "draw_tile", "tile": drawn_tile, "actor_user_id": int(actor_user_id)}
        return state, {"completed": False}

    if move_type == "pass":
        if state.get("stock") or state.get("boneyard"):
            raise HTTPException(status_code=409, detail="You cannot pass while the boneyard still has tiles")
        if player_has_legal_play(state, actor_user_id):
            raise HTTPException(status_code=409, detail="You still have a legal dominoes play")
        state["passes_in_row"] = int(state.get("passes_in_row") or 0) + 1
        state["last_action"] = {"type": "pass", "actor_user_id": int(actor_user_id)}
        state["turn_user_id"] = _opponent_user_id(player_one_user_id, player_two_user_id, actor_user_id)
        if int(state["passes_in_row"]) >= 2:
            blocked = finalize_blocked(player_one_user_id, player_two_user_id, state)
            return blocked["state"], {"completed": True, "winner_user_id": blocked["winner_user_id"], "loser_user_id": blocked["loser_user_id"]}
        return state, {"completed": False}

    raise HTTPException(status_code=400, detail="Unsupported dominoes move")
