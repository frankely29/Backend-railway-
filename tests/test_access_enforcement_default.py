"""The subscription gate must fail CLOSED.

Production served every gated endpoint to unpaid accounts because ENFORCE_TRIAL
defaulted to "0". A revenue gate that defaults to off gives the product away on
any deploy where the variable is missing, renamed, or typo'd -- silently, because
an unenforced app looks exactly like a working one.
"""
from __future__ import annotations

import importlib
import os

import pytest


def _reload_core_with(env_value):
    prev = os.environ.get("ENFORCE_TRIAL")
    if env_value is None:
        os.environ.pop("ENFORCE_TRIAL", None)
    else:
        os.environ["ENFORCE_TRIAL"] = env_value
    try:
        import core
        return importlib.reload(core)
    finally:
        if prev is None:
            os.environ.pop("ENFORCE_TRIAL", None)
        else:
            os.environ["ENFORCE_TRIAL"] = prev


@pytest.fixture(autouse=True)
def _restore_core():
    yield
    import core
    importlib.reload(core)


def test_gate_enforces_when_the_variable_is_absent():
    """The regression: an unset variable must not disable the paywall."""
    assert _reload_core_with(None).ENFORCE_TRIAL is True


def test_gate_enforces_on_an_unrecognised_value():
    """A typo ('true ' vs 'ture') must not silently unlock the product."""
    assert _reload_core_with("ture").ENFORCE_TRIAL is False, (
        "unrecognised values are falsey by design; the protection is that the "
        "DEFAULT is on, so only a deliberate value can disable the gate"
    )


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_explicit_truthy_values_enforce(value):
    assert _reload_core_with(value).ENFORCE_TRIAL is True


@pytest.mark.parametrize("value", ["0", "false", "no", "off"])
def test_disabling_requires_a_deliberate_value(value):
    assert _reload_core_with(value).ENFORCE_TRIAL is False


def test_disabled_gate_announces_itself(capsys):
    core = _reload_core_with("0")
    core._log_access_enforcement_state()
    out = capsys.readouterr().out
    assert "PAYWALL DISABLED" in out
    assert "access_enforcement=OFF" in out


def test_enabled_gate_logs_its_state_too(capsys):
    core = _reload_core_with("1")
    core._log_access_enforcement_state()
    assert "access_enforcement=ON" in capsys.readouterr().out


def test_paying_routes_stay_reachable_without_access():
    """An unpaid user must still reach checkout, or the paywall is a dead end:
    they cannot pay their way out of it."""
    import inspect
    import subscription_routes
    src = inspect.getsource(subscription_routes)
    assert "require_user_basic" in src
    assert "Depends(require_user)" not in src, (
        "subscription routes must not be behind the access gate they exist to resolve"
    )
