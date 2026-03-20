from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def app_env(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-games-")
    data_dir = Path(temp_dir.name)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("COMMUNITY_DB", str(data_dir / "community.db"))
    monkeypatch.setenv("FRAMES_DIR", str(data_dir / "frames"))
    monkeypatch.setenv("JWT_SECRET", "test-jwt-secret-abcdefghijklmnopqrstuvwxyz")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "password123")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_URL", raising=False)

    for name in [
        "core",
        "chat",
        "leaderboard_db",
        "leaderboard_service",
        "leaderboard_tracker",
        "pickup_recording_feature",
        "games_models",
        "games_service",
        "games_routes",
        "main",
    ]:
        sys.modules.pop(name, None)

    main = importlib.import_module("main")
    main.startup()

    frames_dir = Path(os.environ["FRAMES_DIR"])
    frames_dir.mkdir(parents=True, exist_ok=True)
    (frames_dir / "timeline.json").write_text(json.dumps({"timeline": ["2026-03-19T00:00:00Z"], "count": 1}), encoding="utf-8")
    (frames_dir / "frame_000000.json").write_text(json.dumps({"type": "FeatureCollection", "features": []}), encoding="utf-8")
    day_tendency_dir = data_dir / "day_tendency"
    day_tendency_dir.mkdir(parents=True, exist_ok=True)
    (day_tendency_dir / "model.json").write_text(json.dumps({"version": "test"}), encoding="utf-8")

    with TestClient(main.app) as client:
        yield main, client

    temp_dir.cleanup()


def _signup(client: TestClient, email: str, display_name: str) -> dict:
    response = client.post(
        "/auth/signup",
        json={"email": email, "password": "password123", "display_name": display_name},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _accept_match(client: TestClient, challenger: dict, challenged: dict, *, game_type: str) -> dict:
    create_response = client.post(
        "/games/challenges",
        json={"target_user_id": challenged["id"], "game_type": game_type},
        headers=_headers(challenger["token"]),
    )
    assert create_response.status_code == 200, create_response.text
    challenge_id = create_response.json()["id"]
    accept_response = client.post(
        f"/games/challenges/{challenge_id}/accept",
        headers=_headers(challenged["token"]),
    )
    assert accept_response.status_code == 200, accept_response.text
    return accept_response.json()


def _set_match_state(main_module, match_id: int, state: dict, *, current_turn_user_id: int, status: str = "active") -> None:
    main_module._db_exec(
        "UPDATE game_matches SET match_state_json=?, current_turn_user_id=?, status=?, updated_at=? WHERE id=?",
        (json.dumps(state, separators=(",", ":")), int(current_turn_user_id), status, 1_700_000_000, int(match_id)),
    )


def test_progression_reaches_level_1000_and_rank_bands(app_env):
    _main, _client = app_env
    leaderboard_service = importlib.import_module("leaderboard_service")

    assert leaderboard_service.MAX_LEVEL == 1000
    assert len(leaderboard_service.LEVEL_XP_THRESHOLDS) == 1000
    assert leaderboard_service.get_level_from_lifetime_xp(0) == 1
    assert leaderboard_service.get_level_from_lifetime_xp(leaderboard_service.LEVEL_XP_THRESHOLDS[99]) == 100
    assert leaderboard_service.get_level_from_lifetime_xp(leaderboard_service.LEVEL_XP_THRESHOLDS[100]) == 101
    assert leaderboard_service.get_level_from_lifetime_xp(leaderboard_service.LEVEL_XP_THRESHOLDS[998]) == 999
    assert leaderboard_service.get_level_from_lifetime_xp(leaderboard_service.LEVEL_XP_THRESHOLDS[999]) == 1000
    assert leaderboard_service.get_next_level_xp(1) == leaderboard_service.LEVEL_XP_THRESHOLDS[1]
    assert leaderboard_service.get_next_level_xp(100) == leaderboard_service.LEVEL_XP_THRESHOLDS[100]
    assert leaderboard_service.get_next_level_xp(101) == leaderboard_service.LEVEL_XP_THRESHOLDS[101]
    assert leaderboard_service.get_next_level_xp(999) == leaderboard_service.LEVEL_XP_THRESHOLDS[999]
    assert leaderboard_service.get_next_level_xp(1000) is None
    rows = leaderboard_service.get_rank_ladder()
    assert len(rows) == 100
    assert rows[0]["rank_icon_key"] == "band_001"
    assert rows[-1]["rank_icon_key"] == "band_100"


def test_progression_endpoint_keeps_rank_icon_separate_from_podium_badges(app_env):
    _main, client = app_env
    alice = _signup(client, "prog-alice@example.com", "ProgAlice")

    progression_response = client.get("/leaderboard/progression/me", headers=_headers(alice["token"]))
    assert progression_response.status_code == 200, progression_response.text
    progression = progression_response.json()["progression"]
    assert progression["level"] == 1
    assert progression["rank_name"]
    assert progression["title"] == progression["rank_name"]
    assert progression["rank_icon_key"] == "band_001"
    assert progression["rank_icon_key"] not in {"crown", "silver", "bronze"}
    for field in [
        "total_xp",
        "current_level_xp",
        "next_level_xp",
        "xp_to_next_level",
        "max_level_reached",
    ]:
        assert field in progression

    badges_response = client.get("/leaderboard/badges/me", headers=_headers(alice["token"]))
    assert badges_response.status_code == 200, badges_response.text
    assert "badges" in badges_response.json()


def test_challenge_create_decline_cancel_and_permissions(app_env):
    _main, client = app_env
    alice = _signup(client, "alice@example.com", "Alice")
    bob = _signup(client, "bob@example.com", "Bob")
    cara = _signup(client, "cara@example.com", "Cara")

    self_response = client.post(
        "/games/challenges",
        json={"target_user_id": alice["id"], "game_type": "dominoes"},
        headers=_headers(alice["token"]),
    )
    assert self_response.status_code == 400

    create_response = client.post(
        "/games/challenges",
        json={"target_user_id": bob["id"], "game_type": "dominoes"},
        headers=_headers(alice["token"]),
    )
    assert create_response.status_code == 200, create_response.text
    challenge_id = create_response.json()["id"]

    duplicate_response = client.post(
        "/games/challenges",
        json={"target_user_id": bob["id"], "game_type": "dominoes"},
        headers=_headers(alice["token"]),
    )
    assert duplicate_response.status_code == 409

    wrong_accept = client.post(f"/games/challenges/{challenge_id}/accept", headers=_headers(cara["token"]))
    assert wrong_accept.status_code == 403

    decline_response = client.post(f"/games/challenges/{challenge_id}/decline", headers=_headers(bob["token"]))
    assert decline_response.status_code == 200
    assert decline_response.json()["status"] == "declined"

    create_response = client.post(
        "/games/challenges",
        json={"target_user_id": bob["id"], "game_type": "billiards"},
        headers=_headers(alice["token"]),
    )
    challenge_id = create_response.json()["id"]
    cancel_response = client.post(f"/games/challenges/{challenge_id}/cancel", headers=_headers(alice["token"]))
    assert cancel_response.status_code == 200
    assert cancel_response.json()["status"] in {"cancelled", "canceled"}

    users_response = client.get("/games/users?q=bo&limit=10", headers=_headers(alice["token"]))
    assert users_response.status_code == 200
    user_items = users_response.json()["items"]
    assert any(item["user_id"] == bob["id"] for item in user_items)
    assert all(item["user_id"] != alice["id"] for item in user_items)


def test_games_contract_supports_frontend_alias_fields(app_env):
    _main, client = app_env
    alice = _signup(client, "alias-alice@example.com", "AliasAlice")
    bob = _signup(client, "alias-bob@example.com", "AliasBob")

    users_response = client.get("/games/users?q=alias&limit=10", headers=_headers(alice["token"]))
    assert users_response.status_code == 200, users_response.text
    bob_row = next(item for item in users_response.json()["items"] if item["user_id"] == bob["id"])
    assert bob_row["rank_icon_key"].startswith("band_")
    assert "avatar_thumb_url" in bob_row
    assert "avatar_url" in bob_row
    assert "leaderboard_badge_code" in bob_row

    challenge_response = client.post(
        "/games/challenges",
        json={"challenged_user_id": bob["id"], "game_key": "dominoes"},
        headers=_headers(alice["token"]),
    )
    assert challenge_response.status_code == 200, challenge_response.text
    challenge = challenge_response.json()
    assert challenge["game_key"] == "dominoes"
    assert challenge["opponent_user_id"] == bob["id"]
    assert challenge["other_user_display_name"] == "AliasBob"

    outgoing_response = client.get("/games/challenges/outgoing", headers=_headers(alice["token"]))
    assert outgoing_response.status_code == 200, outgoing_response.text
    outgoing_item = outgoing_response.json()["items"][0]
    assert outgoing_item["game_key"] == "dominoes"
    assert outgoing_item["challenged_display_name"] == "AliasBob"
    assert outgoing_item["opponent_display_name"] == "AliasBob"

    incoming_response = client.get("/games/challenges/incoming", headers=_headers(bob["token"]))
    assert incoming_response.status_code == 200, incoming_response.text
    incoming_item = incoming_response.json()["items"][0]
    assert incoming_item["game_key"] == "dominoes"
    assert incoming_item["challenger_display_name"] == "AliasAlice"
    assert incoming_item["other_user_display_name"] == "AliasAlice"

    dashboard_response = client.get("/games/challenges", headers=_headers(bob["token"]))
    assert dashboard_response.status_code == 200, dashboard_response.text
    assert "activeMatch" in dashboard_response.json()

    accept_response = client.post(f"/games/challenges/{challenge['id']}/accept", headers=_headers(bob["token"]))
    assert accept_response.status_code == 200, accept_response.text
    match = accept_response.json()["match"]
    assert match["game_key"] == "dominoes"
    assert match["opponent_user_id"] == alice["id"]
    assert match["opponent_display_name"] == "AliasAlice"
    assert "result_summary" in match

    active_match_response = client.get("/games/matches/active/me", headers=_headers(alice["token"]))
    assert active_match_response.status_code == 200, active_match_response.text
    assert active_match_response.json()["match"]["id"] == match["id"]


def test_dominoes_match_completion_awards_xp_once_and_profile_stats(app_env):
    main, client = app_env
    leaderboard_service = importlib.import_module("leaderboard_service")
    alice = _signup(client, "dom-alice@example.com", "DomAlice")
    bob = _signup(client, "dom-bob@example.com", "DomBob")

    accepted = _accept_match(client, alice, bob, game_type="dominoes")
    match_id = accepted["match"]["id"]
    player_one = accepted["match"]["player_one_user_id"]
    player_two = accepted["match"]["player_two_user_id"]
    state = {
        "game_type": "dominoes",
        "rules": "test",
        "board": [],
        "left_end": None,
        "right_end": None,
        "hands": {str(player_one): [[0, 0]], str(player_two): [[1, 1]]},
        "stock": [],
        "passes_in_row": 0,
        "last_action": None,
        "turn_user_id": player_one,
        "result_summary": None,
    }
    _set_match_state(main, match_id, state, current_turn_user_id=player_one)

    winner = alice if alice["id"] == player_one else bob
    loser = bob if winner is alice else alice
    move_response = client.post(
        f"/games/matches/{match_id}/move",
        json={"move_type": "play_tile", "tile": [0, 0], "side": "left"},
        headers=_headers(winner["token"]),
    )
    assert move_response.status_code == 200, move_response.text
    payload = move_response.json()
    assert payload["match"]["status"] == "completed"
    assert payload["reward_contract"]["xp_awarded"] == 60
    assert payload["match"]["winner_xp_awarded"] == 60
    assert payload["match"]["loser_xp_awarded"] == 20

    progression_once = leaderboard_service.get_progression_for_user(int(winner["id"]))
    progression_twice = client.get(f"/games/matches/{match_id}", headers=_headers(winner["token"])).json()["reward_contract"]
    assert progression_once["xp_breakdown"]["game_xp"] == 60
    assert progression_twice["xp_awarded"] == 60
    assert leaderboard_service.get_progression_for_user(int(winner["id"]))["xp_breakdown"]["game_xp"] == 60

    loser_progression = leaderboard_service.get_progression_for_user(int(loser["id"]))
    assert loser_progression["xp_breakdown"]["game_xp"] == 20

    viewer = main._db_query_one("SELECT * FROM users WHERE id=? LIMIT 1", (int(loser["id"]),))
    profile = main.driver_profile(int(winner["id"]), viewer=viewer)
    assert profile["battle_stats"]["wins"] == 1
    assert profile["battle_record"]["total_matches"] == 1
    assert profile["battle_stats"]["matches_played"] == 1
    assert profile["recent_battles"][0]["match_id"] == match_id

    second_accept = client.post(
        f"/games/challenges/{accepted['match']['challenge_id']}/accept",
        headers=_headers(bob["token"]),
    )
    assert second_accept.status_code == 200
    assert second_accept.json()["match"]["id"] == match_id
    match_count = main._db_query_one("SELECT COUNT(*) AS c FROM game_matches WHERE challenge_id=?", (int(accepted["match"]["challenge_id"]),))
    assert int(match_count["c"] or 0) == 1


def test_cannot_move_out_of_turn(app_env):
    main, client = app_env
    alice = _signup(client, "turn-alice@example.com", "TurnAlice")
    bob = _signup(client, "turn-bob@example.com", "TurnBob")
    accepted = _accept_match(client, alice, bob, game_type="dominoes")
    match_id = accepted["match"]["id"]
    player_one = accepted["match"]["player_one_user_id"]
    player_two = accepted["match"]["player_two_user_id"]
    state = {
        "game_type": "dominoes",
        "rules": "test",
        "board": [[1, 1]],
        "left_end": 1,
        "right_end": 1,
        "hands": {str(player_one): [[1, 2]], str(player_two): [[3, 3]]},
        "stock": [],
        "passes_in_row": 0,
        "last_action": None,
        "turn_user_id": player_two,
        "result_summary": None,
    }
    _set_match_state(main, match_id, state, current_turn_user_id=player_two)

    wrong_player = alice if alice["id"] == player_one else bob
    move_response = client.post(
        f"/games/matches/{match_id}/move",
        json={"move_type": "play_tile", "tile": [1, 2], "side": "right"},
        headers=_headers(wrong_player["token"]),
    )
    assert move_response.status_code == 409


def test_billiards_match_completion_awards_xp_once(app_env):
    main, client = app_env
    leaderboard_service = importlib.import_module("leaderboard_service")
    alice = _signup(client, "bill-alice@example.com", "BillAlice")
    bob = _signup(client, "bill-bob@example.com", "BillBob")
    accepted = _accept_match(client, alice, bob, game_type="billiards")
    match_id = accepted["match"]["id"]
    player_one = accepted["match"]["player_one_user_id"]
    player_two = accepted["match"]["player_two_user_id"]
    state = {
        "game_type": "billiards",
        "rules": "test",
        "turn_user_id": player_one,
        "players": {
            str(player_one): {"targets_remaining": 0, "targets_cleared": 3, "black_unlocked": True},
            str(player_two): {"targets_remaining": 2, "targets_cleared": 1, "black_unlocked": False},
        },
        "table": {"width": 100, "height": 50, "cue_ball": {"x": 18, "y": 25}, "final_ball": {"x": 82, "y": 25, "pocketed": False}},
        "last_shot": None,
        "result_summary": None,
    }
    _set_match_state(main, match_id, state, current_turn_user_id=player_one)

    winner = alice if alice["id"] == player_one else bob
    move_response = client.post(
        f"/games/matches/{match_id}/move",
        json={"move_type": "shot", "angle": 1.57, "power": 0.72},
        headers=_headers(winner["token"]),
    )
    assert move_response.status_code == 200, move_response.text
    payload = move_response.json()
    assert payload["match"]["status"] == "completed"
    assert payload["match"]["winner_xp_awarded"] == 60
    assert payload["reward_contract"]["xp_awarded"] == 60
    assert leaderboard_service.get_progression_for_user(int(winner["id"]))["xp_breakdown"]["game_xp"] == 60


def test_public_battle_result_event_helper(app_env):
    _main, client = app_env
    chat_module = importlib.import_module("chat")
    winner = _signup(client, "winner@example.com", "Winner")

    subscriber, replay = chat_module._live_event_broker.subscribe(chat_module._public_channel("global"), None)
    assert replay == []
    try:
        payload = chat_module.publish_public_battle_notification(
            {
                "match_id": 77,
                "game_type": "dominoes",
                "winner_user_id": 1,
                "winner_display_name": "Winner",
                "loser_user_id": 2,
                "loser_display_name": "Loser",
                "winner_xp_awarded": 60,
                "winner_new_level": 2,
                "completed_at": "2026-03-20T00:00:00+00:00",
            }
        )
        chat_module.publish_public_battle_chat_message(
            author_user_id=int(winner["id"]),
            winner_display_name="Winner",
            loser_display_name="Loser",
            game_type="dominoes",
            winner_xp_awarded=60,
        )
        envelope = subscriber.get(timeout=1)
    finally:
        chat_module._live_event_broker.unsubscribe(chat_module._public_channel("global"), subscriber)

    assert payload["type"] == "battle_result"
    assert envelope["event"] == "battle_result"
    assert envelope["data"]["match_id"] == 77

    message_rows = _main._db_query_all(
        "SELECT display_name, message FROM chat_messages WHERE room='global' ORDER BY id DESC LIMIT 1"
    )
    assert message_rows
    assert message_rows[0]["display_name"] == "Battle Results"
    assert "Winner beat Loser in Dominoes" in message_rows[0]["message"]
