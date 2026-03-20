from __future__ import annotations

import copy
import random
from typing import Any, Iterable

from fastapi import HTTPException

DOMINO_SET: list[tuple[int, int]] = [(a, b) for a in range(7) for b in range(a, 7)]
TARGET_SCORE = 100


def _seed_for_match(match_id: int, round_number: int = 1) -> int:
    return 7000 + (int(match_id) * 17) + (int(round_number) * 97)


def normalize_tile(tile: Iterable[int]) -> tuple[int, int]:
    values = list(tile)
    if len(values) != 2:
        raise HTTPException(status_code=400, detail="Domino tile must contain two pips")
    a = int(values[0])
    b = int(values[1])
    if not (0 <= a <= 6 and 0 <= b <= 6):
        raise HTTPException(status_code=400, detail="Domino tile pips must be between 0 and 6")
    return tuple(sorted((a, b)))


def _default_seats(player_ids: list[int], fmt: str) -> list[dict[str, Any]]:
    ordered = [int(uid) for uid in player_ids]
    if fmt == "2v2":
        if len(ordered) != 4:
            raise HTTPException(status_code=400, detail="Dominoes 2v2 requires four seats")
        return [
            {"user_id": ordered[0], "seat_index": 0, "team_no": 1, "seat_role": "captain"},
            {"user_id": ordered[1], "seat_index": 1, "team_no": 2, "seat_role": "captain"},
            {"user_id": ordered[2], "seat_index": 2, "team_no": 1, "seat_role": "teammate"},
            {"user_id": ordered[3], "seat_index": 3, "team_no": 2, "seat_role": "teammate"},
        ]
    if len(ordered) != 2:
        raise HTTPException(status_code=400, detail="Dominoes 1v1 requires two seats")
    return [
        {"user_id": ordered[0], "seat_index": 0, "team_no": 1, "seat_role": "solo"},
        {"user_id": ordered[1], "seat_index": 1, "team_no": 2, "seat_role": "solo"},
    ]


def _seat_map(state: dict[str, Any]) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    for seat in list(state.get("seats") or []):
        try:
            result[int(seat["user_id"])] = dict(seat)
        except Exception:
            continue
    return result


def _user_ids_in_turn_order(state: dict[str, Any]) -> list[int]:
    seats = sorted(list(state.get("seats") or []), key=lambda item: int(item.get("seat_index") or 0))
    return [int(seat["user_id"]) for seat in seats]


def _next_turn_user_id(state: dict[str, Any], actor_user_id: int) -> int:
    order = _user_ids_in_turn_order(state)
    if not order:
        raise HTTPException(status_code=500, detail="Dominoes state is missing turn order")
    current_index = order.index(int(actor_user_id))
    return order[(current_index + 1) % len(order)]


def _deal_round(*, player_ids: list[int], fmt: str, match_id: int, round_number: int) -> tuple[dict[str, list[list[int]]], list[list[int]]]:
    deck = DOMINO_SET.copy()
    random.Random(_seed_for_match(match_id, round_number)).shuffle(deck)
    hand_size = 7
    hands: dict[str, list[list[int]]] = {}
    cursor = 0
    for user_id in player_ids:
        hands[str(int(user_id))] = [list(tile) for tile in deck[cursor : cursor + hand_size]]
        cursor += hand_size
    stock = [list(tile) for tile in deck[cursor:]]
    if fmt == "2v2":
        stock = []
    return hands, stock


def _exposed_ends_score(board: list[list[int]]) -> int:
    if not board:
        return 0
    if len(board) == 1:
        first = board[0]
        total = int(first[0]) + int(first[1])
    else:
        total = int(board[0][0]) + int(board[-1][1])
    return total if total > 0 and total % 5 == 0 else 0


def _round_state_payload(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "board": [],
        "left_end": None,
        "right_end": None,
        "passes_in_row": 0,
        "result_summary": None,
        "last_action": None,
    }


def create_initial_state(
    player_ids: list[int],
    *,
    match_id: int,
    fmt: str = "1v1",
    seats: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized_format = "2v2" if str(fmt or "1v1").strip().lower() == "2v2" else "1v1"
    normalized_seats = copy.deepcopy(seats or _default_seats(player_ids, normalized_format))
    ordered_ids = [int(seat["user_id"]) for seat in sorted(normalized_seats, key=lambda item: int(item.get("seat_index") or 0))]
    hands, stock = _deal_round(player_ids=ordered_ids, fmt=normalized_format, match_id=int(match_id), round_number=1)
    team_scores = {"1": 0, "2": 0}
    return {
        "game_type": "dominoes",
        "battle_type": "dominoes",
        "variant": "all_fives",
        "format": normalized_format,
        "rules": "All Fives to 100. Score on exposed ends divisible by five. Challenges expire after 24h; active matches expire after 24h of inactivity.",
        "target_score": TARGET_SCORE,
        "round_number": 1,
        "seats": normalized_seats,
        "team_scores": team_scores,
        "hands": hands,
        "stock": stock,
        "boneyard": stock,
        "turn_user_id": ordered_ids[0],
        "round_starter_user_id": ordered_ids[0],
        "boneyard_count": len(stock),
        **_round_state_payload({}),
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
    hand = list(state.get("hands", {}).get(str(int(user_id)), []))
    return any(legal_play_sides(state, tuple(tile)) for tile in hand)


def _award_points_for_play(state: dict[str, Any], actor_user_id: int) -> int:
    seat = _seat_map(state).get(int(actor_user_id))
    if not seat:
        return 0
    points = _exposed_ends_score(list(state.get("board") or []))
    if points <= 0:
        return 0
    team_key = str(int(seat.get("team_no") or 1))
    team_scores = dict(state.get("team_scores") or {})
    team_scores[team_key] = int(team_scores.get(team_key) or 0) + int(points)
    state["team_scores"] = team_scores
    return points


def _team_pip_totals(state: dict[str, Any]) -> dict[int, int]:
    totals: dict[int, int] = {}
    seat_by_user = _seat_map(state)
    for user_key, hand in dict(state.get("hands") or {}).items():
        seat = seat_by_user.get(int(user_key))
        if not seat:
            continue
        team_no = int(seat.get("team_no") or 1)
        totals[team_no] = int(totals.get(team_no) or 0) + hand_total(list(hand or []))
    return totals


def _round_completion_score_from_pips(winning_total: int, losing_total: int) -> int:
    raw = max(0, int(losing_total) - int(winning_total))
    return raw - (raw % 5)


def _start_next_round(state: dict[str, Any], *, match_id: int, starter_user_id: int) -> dict[str, Any]:
    state["round_number"] = int(state.get("round_number") or 1) + 1
    order = _user_ids_in_turn_order(state)
    hands, stock = _deal_round(player_ids=order, fmt=str(state.get("format") or "1v1"), match_id=int(match_id), round_number=int(state["round_number"]))
    state["hands"] = hands
    state["stock"] = stock
    state["boneyard"] = stock
    state["boneyard_count"] = len(stock)
    state["turn_user_id"] = int(starter_user_id)
    state["round_starter_user_id"] = int(starter_user_id)
    state.update(_round_state_payload(state))
    return state


def _outcome_payload(*, winner_team_no: int, loser_team_no: int | None, state: dict[str, Any], reason: str, points_awarded: int, finishing_user_id: int | None = None) -> dict[str, Any]:
    winners = [int(seat["user_id"]) for seat in list(state.get("seats") or []) if int(seat.get("team_no") or 0) == int(winner_team_no)]
    losers = [int(seat["user_id"]) for seat in list(state.get("seats") or []) if loser_team_no is not None and int(seat.get("team_no") or 0) == int(loser_team_no)]
    return {
        "winner_team_no": int(winner_team_no),
        "winner_user_ids": winners,
        "loser_team_no": int(loser_team_no) if loser_team_no is not None else None,
        "loser_user_ids": losers,
        "reason": reason,
        "points_awarded": int(points_awarded),
        "finishing_user_id": int(finishing_user_id) if finishing_user_id is not None else None,
    }


def _complete_or_continue_round(state: dict[str, Any], *, match_id: int, winner_team_no: int, loser_team_no: int, reason: str, points_awarded: int, finishing_user_id: int | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    state["result_summary"] = {
        "reason": reason,
        "winner_team_no": int(winner_team_no),
        "loser_team_no": int(loser_team_no),
        "points_awarded": int(points_awarded),
        "team_scores": copy.deepcopy(state.get("team_scores") or {}),
        "finishing_user_id": int(finishing_user_id) if finishing_user_id is not None else None,
    }
    team_scores = dict(state.get("team_scores") or {})
    if int(team_scores.get(str(winner_team_no)) or 0) >= TARGET_SCORE:
        return state, {"completed": True, **_outcome_payload(winner_team_no=winner_team_no, loser_team_no=loser_team_no, state=state, reason=reason, points_awarded=points_awarded, finishing_user_id=finishing_user_id)}
    starter = int(finishing_user_id) if finishing_user_id is not None else _next_turn_user_id(state, state.get("round_starter_user_id") or _user_ids_in_turn_order(state)[0])
    next_state = _start_next_round(state, match_id=int(match_id), starter_user_id=starter)
    return next_state, {"completed": False, "round_complete": True, **_outcome_payload(winner_team_no=winner_team_no, loser_team_no=loser_team_no, state=state, reason=reason, points_awarded=points_awarded, finishing_user_id=finishing_user_id)}


def finalize_blocked(state: dict[str, Any], *, match_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    totals = _team_pip_totals(state)
    team_one_total = int(totals.get(1) or 0)
    team_two_total = int(totals.get(2) or 0)
    if team_one_total == team_two_total:
        state["result_summary"] = {
            "reason": "blocked_tie",
            "team_pips": {"1": team_one_total, "2": team_two_total},
            "points_awarded": 0,
            "team_scores": copy.deepcopy(state.get("team_scores") or {}),
        }
        return state, {"completed": True, "winner_team_no": None, "winner_user_ids": [], "loser_user_ids": [], "reason": "blocked_tie", "points_awarded": 0, "tie": True}
    winner_team_no = 1 if team_one_total < team_two_total else 2
    loser_team_no = 2 if winner_team_no == 1 else 1
    points = _round_completion_score_from_pips(min(team_one_total, team_two_total), max(team_one_total, team_two_total))
    team_scores = dict(state.get("team_scores") or {})
    team_key = str(winner_team_no)
    team_scores[team_key] = int(team_scores.get(team_key) or 0) + int(points)
    state["team_scores"] = team_scores
    return _complete_or_continue_round(state, match_id=int(match_id), winner_team_no=winner_team_no, loser_team_no=loser_team_no, reason="blocked", points_awarded=points)


def apply_move(
    state: dict[str, Any],
    *,
    match_id: int,
    actor_user_id: int,
    move: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    move_type = str(move.get("move_type") or "").strip().lower()
    actor_key = str(int(actor_user_id))
    hand = list(state.get("hands", {}).get(actor_key, []))
    seat = _seat_map(state).get(int(actor_user_id))
    if not seat:
        raise HTTPException(status_code=403, detail="You are not seated in this dominoes match")

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
        state["boneyard_count"] = len(list(state.get("stock") or state.get("boneyard") or []))
        scored_points = _award_points_for_play(state, int(actor_user_id))
        state["last_action"] = {
            "type": "play_tile",
            "tile": oriented,
            "side": side,
            "actor_user_id": int(actor_user_id),
            "scored_points": int(scored_points),
        }
        if not hand:
            team_no = int(seat.get("team_no") or 1)
            losing_team = 2 if team_no == 1 else 1
            totals = _team_pip_totals(state)
            points = _round_completion_score_from_pips(int(totals.get(team_no) or 0), int(totals.get(losing_team) or 0))
            team_scores = dict(state.get("team_scores") or {})
            team_key = str(team_no)
            team_scores[team_key] = int(team_scores.get(team_key) or 0) + int(points)
            state["team_scores"] = team_scores
            return _complete_or_continue_round(state, match_id=int(match_id), winner_team_no=team_no, loser_team_no=losing_team, reason="emptied_hand", points_awarded=points, finishing_user_id=int(actor_user_id))
        state["turn_user_id"] = _next_turn_user_id(state, int(actor_user_id))
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
        state["boneyard_count"] = len(stock)
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
        state["turn_user_id"] = _next_turn_user_id(state, int(actor_user_id))
        required_passes = 4 if str(state.get("format") or "1v1") == "2v2" else 2
        if int(state["passes_in_row"]) >= required_passes:
            return finalize_blocked(state, match_id=int(match_id))
        return state, {"completed": False}

    raise HTTPException(status_code=400, detail="Unsupported dominoes move")
