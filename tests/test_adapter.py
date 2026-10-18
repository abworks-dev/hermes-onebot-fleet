"""Adapter behavior against a small host contract double; real-host probe is separate."""

import importlib
import json
import sys
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace

import pytest

from hermes_qq_failover.config import Settings
from hermes_qq_failover.store import Store
from hermes_qq_failover.transport import ActionUncertain


@pytest.fixture
def adapter_module(monkeypatch):
    gateway = ModuleType("gateway")
    config = ModuleType("gateway.config")
    config.Platform = str
    platforms = ModuleType("gateway.platforms")
    base = ModuleType("gateway.platforms.base")

    class Host:
        pass

    @dataclass
    class Result:
        success: bool
        message_id: str | None = None
        error: str | None = None
        retryable: bool = False

    base.BasePlatformAdapter = Host
    base.MessageEvent = SimpleNamespace
    base.MessageType = SimpleNamespace(TEXT="text")
    base.SendResult = Result
    for name, module in [
        ("gateway", gateway),
        ("gateway.config", config),
        ("gateway.platforms", platforms),
        ("gateway.platforms.base", base),
    ]:
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.delitem(sys.modules, "hermes_qq_failover.adapter", raising=False)
    module = importlib.import_module("hermes_qq_failover.adapter")
    yield module
    sys.modules.pop("hermes_qq_failover.adapter", None)


def settings():
    return Settings.from_mapping(
        {
            "accounts": [
                {"id": "10001", "ws_url": "ws://127.0.0.1:3001", "token_env": "TOKEN_A", "priority": 0},
                {"id": "10002", "ws_url": "ws://127.0.0.1:3002", "token_env": "TOKEN_B", "priority": 1},
            ],
            "allowed_users": ["20001"],
            "allowed_groups": ["30001"],
        }
    )


class Connection:
    def __init__(self, account, healthy=True, ambiguous=False):
        self.account, self.healthy, self.ambiguous = account, healthy, ambiguous
        self.calls = []

    async def rpc(self, action, params):
        self.calls.append((action, params))
        if action == "get_group_member_info":
            return {"user_id": self.account}
        if self.ambiguous:
            raise ActionUncertain("delivered but acknowledgement lost")
        return {"message_id": 42}


def make(module, tmp_path):
    import asyncio

    adapter = object.__new__(module.QQFailoverAdapter)
    adapter.settings = settings()
    adapter._store = Store(tmp_path / "state.db")
    adapter._send_lock = asyncio.Lock()
    adapter._connections = {a.id: Connection(a.id) for a in adapter.settings.accounts}
    return adapter


@pytest.mark.asyncio
async def test_failover_selects_standby_without_changing_target(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    a._connections["10001"].healthy = False
    result = await a.send("qqf:qq-failover:group:30001", "hello")
    assert result.success
    assert not a._connections["10001"].calls
    action, params = a._connections["10002"].calls[-1]
    assert action == "send_group_msg" and params["group_id"] == 30001


@pytest.mark.asyncio
async def test_ambiguous_send_never_tries_standby(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    a._connections["10001"].ambiguous = True
    result = await a.send("qqf:qq-failover:private:20001", "hello")
    assert not result.success and not result.retryable
    assert not a._connections["10002"].calls
    assert a._store.status()["outbound"] == {"uncertain": 1}


@pytest.mark.asyncio
async def test_outbound_acl_and_namespace(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    for address in [
        "qqf:other:group:30001",
        "qqf:qq-failover:group:99999",
        "qqf:qq-failover:private:99999",
        "group:30001",
    ]:
        assert not (await a.send(address, "hello")).success
    assert all(not c.calls for c in a._connections.values())


@pytest.mark.asyncio
async def test_structured_text_and_context_dedup(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    token = adapter_module.INGRESS.set("logical-inbound")
    try:
        for _ in range(2):
            assert (await a.send("qqf:qq-failover:private:20001", "[CQ:at,qq=all]")).success
    finally:
        adapter_module.INGRESS.reset(token)
    calls = a._connections["10001"].calls
    assert len(calls) == 1
    assert calls[0][1]["message"] == [{"type": "text", "data": {"text": "[CQ:at,qq=all]"}}]


@pytest.mark.asyncio
async def test_ingress_duplicates_from_accounts_are_one_handoff(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    a._receive_events = True
    raw = {
        "post_type": "message",
        "self_id": 10001,
        "user_id": 20001,
        "message_type": "group",
        "group_id": 30001,
        "message_id": 11,
        "time": 100,
        "message": [{"type": "at", "data": {"qq": "10001"}}, {"type": "text", "data": {"text": "hi"}}],
    }
    await a._on_event(a.settings.accounts[0], raw)
    raw.update(self_id=10002, message_id=22)
    await a._on_event(a.settings.accounts[1], raw)
    assert a._store.status()["inbound"] == {"queued": 1}
    payload = json.loads(a._store.claim()["payload"])
    assert payload["user_id"] == "20001"


@pytest.mark.asyncio
async def test_cancelled_send_is_immediately_uncertain(adapter_module, tmp_path):
    import asyncio

    a = make(adapter_module, tmp_path)
    entered = asyncio.Event()

    async def hanging_rpc(action, params):
        entered.set()
        await asyncio.Event().wait()

    a._connections["10001"].rpc = hanging_rpc
    task = asyncio.create_task(a.send("qqf:qq-failover:private:20001", "hello"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert a._store.status()["outbound"] == {"uncertain": 1}
    assert not a._connections["10002"].calls


@pytest.mark.asyncio
async def test_preflight_disconnect_can_select_standby(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)

    async def disconnected(action, params):
        raise ConnectionError("read-only preflight failed")

    a._connections["10001"].rpc = disconnected
    assert (await a.send("qqf:qq-failover:group:30001", "hello")).success
    assert a._connections["10002"].calls[-1][0] == "send_group_msg"


@pytest.mark.asyncio
async def test_repeated_connect_does_not_recover_live_state(adapter_module, tmp_path):
    import asyncio

    from hermes_qq_failover.lock import InstanceLock

    a = make(adapter_module, tmp_path)
    a._lifecycle_lock = asyncio.Lock()
    a._running = False
    a._lock = InstanceLock(tmp_path / "instance.lock")
    a._path = tmp_path / "state.db"
    a._tasks = []
    a._receive_events = False
    starts = []

    async def start():
        starts.append(1)

    async def stop():
        pass

    async def idle():
        await asyncio.Event().wait()

    for connection in a._connections.values():
        connection.start = start
        connection.stop = stop
    a._health_loop = idle
    try:
        assert await a.connect()
        a._store.enqueue("active", {})
        a._store.claim()
        assert await a.connect(is_reconnect=True)
        assert len(starts) == 2 and len(a._tasks) == 1
        assert a._store.status()["inbound"] == {"handing_off": 1}
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_host_terminal_retry_contract(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    a._connections["10001"].ambiguous = True
    result = await a.send("qqf:qq-failover:private:20001", "hello")
    assert a._send_retry_is_final(result)


@pytest.mark.asyncio
async def test_reply_anchor_beats_stale_native_task_context(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    token = adapter_module.INGRESS.set("stale-native-drain-context")
    try:
        for message in ("incoming-one", "incoming-two"):
            assert (await a.send("qqf:qq-failover:private:20001", "same answer", reply_to=message)).success
    finally:
        adapter_module.INGRESS.reset(token)
    assert len(a._connections["10001"].calls) == 2


@pytest.mark.asyncio
async def test_queued_work_rechecks_revoked_policy(adapter_module, tmp_path):
    import asyncio
    from dataclasses import asdict, replace

    from hermes_qq_failover.protocol import normalize_event

    a = make(adapter_module, tmp_path)
    raw = {
        "post_type": "message",
        "self_id": 10001,
        "user_id": 20001,
        "message_type": "group",
        "group_id": 30001,
        "message_id": 1,
        "time": 100,
        "message": [{"type": "at", "data": {"qq": "10001"}}, {"type": "text", "data": {"text": "hi"}}],
    }
    item = normalize_event(raw, a.settings.accounts[0], a.settings)
    a._store.enqueue(item.dedup_key, asdict(item))
    a.settings = replace(a.settings, allowed_groups=frozenset())
    a._message_handler = True

    async def forbidden_dispatch(event):
        pytest.fail("revoked group reached the host")

    a.handle_message = forbidden_dispatch
    task = asyncio.create_task(a._drain())

    async def wait_denied():
        while a._store.status()["inbound"] != {"denied": 1}:
            await asyncio.sleep(0)

    try:
        await asyncio.wait_for(wait_denied(), 2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
