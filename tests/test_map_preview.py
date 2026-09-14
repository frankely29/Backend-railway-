"""The map preview: a few minutes of map, then the lock.

Timed on the server on purpose. A browser-side timer resets on reinstall, on
clearing site data, and in a private window, so the "preview" would be unlimited
for anyone who noticed. Everything here drives core's real gate.
"""
from __future__ import annotations

import importlib
import os

import pytest
from fastapi import HTTPException


def _core_with(**env):
    prev = {k: os.environ.get(k) for k in env}
    os.environ.update({k: str(v) for k, v in env.items()})
    try:
        import core
        return importlib.reload(core)
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@pytest.fixture(autouse=True)
def _restore_core():
    yield
    import core
    importlib.reload(core)


class _Row(dict):
    """Stands in for a DB row: keys() plus subscript, like sqlite3.Row."""
    def keys(self):          # noqa: D102
        return list(super().keys())


def _lapsed_user(**over):
    row = _Row({
        "id": 7,
        "is_admin": 0,
        "subscription_status": "none",
        "subscription_comp_expires_at": None,
        "subscription_current_period_end": None,
        "trial_expires_at": 1,          # long expired
        "map_preview_started_at": None,
    })
    row.update(over)
    return row


class _Req:
    def __init__(self, path):
        self.url = type("U", (), {"path": path})()


def _run(core, user, path, now=None, writes=None):
    """Drive the real gate; capture the stamp it would write."""
    if writes is not None:
        def _fake_exec(sql, params=()):
            writes.append((sql, params))
            user["map_preview_started_at"] = params[0]
        core._db_exec = _fake_exec
    if now is not None:
        core.time = type("T", (), {"time": staticmethod(lambda: now)})()
    core._enforce_access_or_admin(user, _Req(path))


def test_a_lapsed_driver_gets_a_first_look_at_the_map():
    core = _core_with(ENFORCE_TRIAL="1", MAP_PREVIEW_SECONDS="300")
    user, writes = _lapsed_user(), []
    _run(core, user, "/frame/0", now=1000, writes=writes)
    assert writes, "the preview was allowed but never stamped, so it can never end"
    assert user["map_preview_started_at"] == 1000


def test_the_preview_ends():
    core = _core_with(ENFORCE_TRIAL="1", MAP_PREVIEW_SECONDS="300")
    user = _lapsed_user(map_preview_started_at=1000)
    _run(core, user, "/frame/0", now=1000 + 299)          # inside: fine
    with pytest.raises(HTTPException) as caught:
        _run(core, user, "/frame/0", now=1000 + 300)      # on the second: done
    assert caught.value.status_code == 402


def test_a_fresh_token_does_not_buy_another_preview():
    """The whole reason this is server-side: reinstalling must not reset it."""
    core = _core_with(ENFORCE_TRIAL="1", MAP_PREVIEW_SECONDS="300")
    user = _lapsed_user(map_preview_started_at=1000)
    with pytest.raises(HTTPException):
        _run(core, user, "/frame/0", now=1000 + 4000)     # same day, spent


def test_the_preview_comes_back_after_the_reset_window():
    core = _core_with(ENFORCE_TRIAL="1", MAP_PREVIEW_SECONDS="300",
                      MAP_PREVIEW_RESET_SECONDS="86400")
    user, writes = _lapsed_user(map_preview_started_at=1000), []
    _run(core, user, "/frame/0", now=1000 + 86400, writes=writes)
    assert user["map_preview_started_at"] == 1000 + 86400


def test_the_preview_opens_the_map_and_nothing_else():
    """It is a map preview. The feed is free on its own terms; chat, games and
    the leaderboard are not part of the deal."""
    core = _core_with(ENFORCE_TRIAL="1", MAP_PREVIEW_SECONDS="300")
    for path in ("/chat/rooms/main", "/leaderboard", "/games/challenges",
                 "/social/posts/1/comments"):
        with pytest.raises(HTTPException) as caught:
            _run(core, _lapsed_user(), path, now=1000, writes=[])
        assert caught.value.status_code == 402, path


def test_a_preview_that_cannot_be_recorded_is_refused():
    """If the stamp fails to write the preview would never expire."""
    core = _core_with(ENFORCE_TRIAL="1", MAP_PREVIEW_SECONDS="300")
    def _boom(sql, params=()):
        raise RuntimeError("db down")
    core._db_exec = _boom
    core.time = type("T", (), {"time": staticmethod(lambda: 1000)})()
    with pytest.raises(HTTPException):
        core._enforce_access_or_admin(_lapsed_user(), _Req("/frame/0"))


def test_setting_it_to_zero_restores_the_old_behaviour():
    core = _core_with(ENFORCE_TRIAL="1", MAP_PREVIEW_SECONDS="0")
    with pytest.raises(HTTPException):
        _run(core, _lapsed_user(), "/frame/0", now=1000, writes=[])


def test_a_paid_driver_never_touches_the_preview_clock():
    core = _core_with(ENFORCE_TRIAL="1", MAP_PREVIEW_SECONDS="300")
    paid = _lapsed_user(trial_expires_at=10**12)
    writes = []
    _run(core, paid, "/frame/0", now=1000, writes=writes)
    assert not writes, "an in-trial driver had their one preview spent for them"


def test_the_countdown_matches_the_gate():
    """/me tells the client how long it has; both must read the same clock."""
    core = _core_with(ENFORCE_TRIAL="1", MAP_PREVIEW_SECONDS="300")
    assert core.map_preview_remaining(_lapsed_user(), 1000) == 300
    started = _lapsed_user(map_preview_started_at=1000)
    assert core.map_preview_remaining(started, 1000 + 120) == 180
    assert core.map_preview_remaining(started, 1000 + 300) == 0
    assert core.map_preview_remaining(started, 1000 + 90000) == 300   # rolled over
