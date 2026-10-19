"""Direct transport observation tests; no sockets or profile access."""

from types import SimpleNamespace

import pytest

from hermes_onebot_fleet.config import Account
from hermes_onebot_fleet.health import HealthState
from hermes_onebot_fleet.transport import ActionUncertain, OneBotConnection


def connection():
    c = OneBotConnection(
        Account("10001", "ws://127.0.0.1:1", "FAKE_TOKEN"), "not-used-token-123456789", lambda _: None
    )
    c._ws = SimpleNamespace(closed=False)
    return c


@pytest.mark.asyncio
@pytest.mark.parametrize("good,ready", [(True, True), (False, False), (None, True), ("bad", True), (0, True)])
async def test_backend_good_is_independent_and_unknown_is_not_false(good, ready):
    c = connection()

    async def rpc(action, params, **kwargs):
        return {"user_id": "10001"} if action == "get_login_info" else {"online": True, "good": good}

    c.rpc = rpc
    assert await c.check_health() is ready
    assert c.observation["good"] is (good if type(good) is bool else None)
    assert c.observation["identity"] == "verified"


@pytest.mark.asyncio
async def test_mismatch_is_retained_even_when_status_fails_and_connection_closes(tmp_path):
    c = connection()
    health = HealthState(tmp_path / "health.db", "onebot-fleet")
    c._on_observation = lambda data: health.observe("10001", **data)

    async def rpc(action, params, **kwargs):
        if action == "get_login_info":
            return {"user_id": "99999"}
        c._ws.closed = True
        raise ActionUncertain("fake read failure")

    c.rpc = rpc
    assert not await c.check_health()
    assert c.observation["identity"] == "mismatch"
    assert not health.is_eligible("10001")
    c._ws.closed = False

    async def recovered(action, params, **kwargs):
        return {"user_id": "10001"} if action == "get_login_info" else {"online": True, "good": True}

    c.rpc = recovered
    assert await c.check_health()
    assert not health.is_eligible("10001")  # Observation recovery never resets a circuit.


@pytest.mark.asyncio
async def test_diagnostic_failure_cannot_enable_readiness_or_leak_exception(caplog):
    c = connection()

    async def rpc(action, params, **kwargs):
        return {"user_id": "10001"} if action == "get_login_info" else {"online": True}

    def broken(data):
        raise OSError("FAKE_SECRET_MUST_NOT_LOG")

    c.rpc = rpc
    c._on_observation = broken
    assert not await c.check_health()
    assert "FAKE_SECRET_MUST_NOT_LOG" not in caplog.text
