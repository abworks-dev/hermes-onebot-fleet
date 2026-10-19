"""Adapter behavior against a small host contract double; real-host probe is separate."""

import importlib
import json
import sys
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace

import pytest

from hermes_onebot_fleet.account_state import AccountState
from hermes_onebot_fleet.config import Settings
from hermes_onebot_fleet.protocol import Incoming
from hermes_onebot_fleet.store import Store
from hermes_onebot_fleet.transport import ActionUncertain


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
    monkeypatch.delitem(sys.modules, "hermes_onebot_fleet.adapter", raising=False)
    module = importlib.import_module("hermes_onebot_fleet.adapter")
    yield module
    sys.modules.pop("hermes_onebot_fleet.adapter", None)


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


def test_native_registration_uses_fleet_identifiers(adapter_module):
    from hermes_onebot_fleet.plugin import register

    class Context:
        def register_platform(self, **kwargs):
            self.entry = kwargs

    context = Context()
    register(context)
    assert context.entry["name"] == "onebot_fleet"
    assert context.entry["adapter_factory"] is adapter_module.OneBotFleetAdapter
    assert context.entry["allowed_users_env"] == "ONEBOT_FLEET_ALLOWED_USERS"
    assert context.entry["allow_all_env"] == "ONEBOT_FLEET_ALLOW_ALL_USERS"
    assert context.entry["cron_deliver_env_var"] == "ONEBOT_FLEET_HOME_CHANNEL"
    address = adapter_module.target("onebot-fleet", "group", "30001")
    assert address == "obf:onebot-fleet:group:30001"
    assert adapter_module.parse_target(address) == ("onebot-fleet", "group", "30001")


def admit_reply(adapter, key, kind="private", source="10001"):
    from dataclasses import asdict

    item = Incoming(
        key,
        key,
        "20001",
        "30001" if kind == "group" else "20001",
        kind,
        "question",
        source,
        100,
        "onebot-fleet",
        ("10001",),
    )
    payload = asdict(item)
    payload["_admission_revision"] = adapter._admission.snapshot(source)["revision"]
    adapter._store.enqueue(key, payload)
    adapter._store.inbound_result(key, "handed_off")


def make(module, tmp_path, *, anchors=True):
    import asyncio

    adapter = object.__new__(module.OneBotFleetAdapter)
    adapter.settings = settings()
    adapter._store = Store(tmp_path / "state.db")
    adapter._send_lock = asyncio.Lock()
    adapter._connections = {a.id: Connection(a.id) for a in adapter.settings.accounts}
    adapter._admission = AccountState(
        tmp_path / "accounts.db", "onebot-fleet", [a.id for a in adapter.settings.accounts]
    )
    from hermes_onebot_fleet.delivery_budget import DeliveryBudget
    from hermes_onebot_fleet.health import HealthState

    budget_path = tmp_path / "budget.db"
    import time

    now = [time.time()]
    if budget_path.exists():
        now[0] = max(now[0], DeliveryBudget.open_existing(budget_path).status()["now"]) + 11

    def progressing_clock():
        now[0] += 11
        return now[0]

    adapter._budget = DeliveryBudget(
        budget_path, "onebot-fleet", adapter.settings.budget, clock=progressing_clock
    )
    adapter._budget_wait = {}
    adapter._health = HealthState(tmp_path / "health.db", "onebot-fleet")
    if anchors:
        for key in ("inbound-1", "logical-inbound", "incoming-one", "incoming-two"):
            admit_reply(adapter, key)
        admit_reply(adapter, "group-inbound-1", kind="group")
    return adapter


@pytest.mark.asyncio
async def test_failover_selects_standby_without_changing_target(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    a._connections["10001"].healthy = False
    result = await a.send("obf:onebot-fleet:group:30001", "hello", reply_to="group-inbound-1")
    assert result.success
    assert not a._connections["10001"].calls
    action, params = a._connections["10002"].calls[-1]
    assert action == "send_group_msg" and params["group_id"] == 30001


@pytest.mark.asyncio
async def test_ambiguous_send_never_tries_standby(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    a._connections["10001"].ambiguous = True
    result = await a.send("obf:onebot-fleet:private:20001", "hello", reply_to="inbound-1")
    assert not result.success and not result.retryable
    assert not a._connections["10002"].calls
    assert a._store.status()["outbound"] == {"uncertain": 1}


@pytest.mark.asyncio
async def test_outbound_acl_and_namespace(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    for address in [
        "obf:other:group:30001",
        "obf:onebot-fleet:group:99999",
        "obf:onebot-fleet:private:99999",
        "group:30001",
    ]:
        assert not (await a.send(address, "hello", reply_to="inbound-1")).success
    assert all(not c.calls for c in a._connections.values())


@pytest.mark.asyncio
async def test_structured_text_and_context_dedup(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    token = adapter_module.INGRESS.set("logical-inbound")
    try:
        for _ in range(2):
            assert (
                await a.send("obf:onebot-fleet:private:20001", "[CQ:at,qq=all]", reply_to="logical-inbound")
            ).success
    finally:
        adapter_module.INGRESS.reset(token)
    calls = a._connections["10001"].calls
    assert len(calls) == 1
    assert calls[0][1]["message"] == [{"type": "text", "data": {"text": "[CQ:at,qq=all]"}}]


@pytest.mark.asyncio
async def test_ingress_duplicates_from_accounts_are_one_handoff(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path, anchors=False)
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
    task = asyncio.create_task(a.send("obf:onebot-fleet:private:20001", "hello", reply_to="inbound-1"))
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
    assert (await a.send("obf:onebot-fleet:group:30001", "hello", reply_to="group-inbound-1")).success
    assert a._connections["10002"].calls[-1][0] == "send_group_msg"


@pytest.mark.asyncio
async def test_repeated_connect_does_not_recover_live_state(adapter_module, tmp_path):
    import asyncio

    from hermes_onebot_fleet.lock import InstanceLock

    a = make(adapter_module, tmp_path, anchors=False)
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
    result = await a.send("obf:onebot-fleet:private:20001", "hello", reply_to="inbound-1")
    assert a._send_retry_is_final(result)


@pytest.mark.asyncio
async def test_reply_anchor_beats_stale_native_task_context(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    token = adapter_module.INGRESS.set("stale-native-drain-context")
    try:
        for message in ("incoming-one", "incoming-two"):
            assert (await a.send("obf:onebot-fleet:private:20001", "same answer", reply_to=message)).success
    finally:
        adapter_module.INGRESS.reset(token)
    assert len(a._connections["10001"].calls) == 2


@pytest.mark.asyncio
async def test_contextless_cron_fallback_cannot_send_or_mint_id(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    for _ in range(2):
        # Simulates live send then the host's outer fallback, even if it
        # reformats content or supplies a job_id (not an occurrence identity).
        result = await a.send("obf:onebot-fleet:private:20001", "background", metadata={"job_id": "same-job"})
        assert not result.success and not result.retryable
        assert "durable publisher ID" in result.error
    assert not a._store.status()["outbound"]
    assert all(not c.calls for c in a._connections.values())


def test_staged_plugins_use_fresh_profile_scoped_modules(adapter_module, tmp_path, monkeypatch):
    import importlib.util
    import sys
    from types import SimpleNamespace

    from hermes_onebot_fleet.cli import main

    contexts = []
    for i in range(2):
        home = tmp_path / str(i)
        home.mkdir()
        (home / "config.yaml").write_text("plugins: {}\n", encoding="utf-8")
        monkeypatch.setattr(sys, "argv", ["fleet", "stage-plugin", "--hermes-home", str(home)])
        main()
        path = home / "plugins" / "onebot-fleet" / "__init__.py"
        name = f"fleet_scoped_test_{i}"
        spec = importlib.util.spec_from_file_location(
            name, path, submodule_search_locations=[str(path.parent)]
        )
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        try:
            spec.loader.exec_module(module)
            entries = []
            module.register(SimpleNamespace(register_platform=lambda **kwargs: entries.append(kwargs)))
            implementation = entries[0]["adapter_factory"]
            assert implementation.__module__.startswith(name + ".src.hermes_onebot_fleet.")
            contexts.append(implementation)
        finally:
            for key in list(sys.modules):
                if key == name or key.startswith(name + "."):
                    sys.modules.pop(key, None)
    assert contexts[0] is not contexts[1]


@pytest.mark.asyncio
async def test_stale_ingress_context_does_not_authorize_background(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    token = adapter_module.INGRESS.set("stale-accepted-chat-context")
    try:
        assert not (await a.send("obf:onebot-fleet:private:20001", "background")).success
    finally:
        adapter_module.INGRESS.reset(token)
    assert all(not c.calls for c in a._connections.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt", [True, {}, "secret-shaped-value", 4.2, None])
async def test_invalid_remote_receipt_is_uncertain(adapter_module, tmp_path, receipt):
    a = make(adapter_module, tmp_path)

    async def invalid(action, params):
        return {"message_id": receipt}

    a._connections["10001"].rpc = invalid
    result = await a._publish_once("invalid-receipt", "obf:onebot-fleet:private:20001", "hello")
    assert result["state"] == "unknown"
    assert a._store.status()["outbound"] == {"uncertain": 1}
    assert not a._connections["10002"].calls


@pytest.mark.asyncio
async def test_stop_failure_still_releases_transports_tasks_and_lock(adapter_module, tmp_path):
    import asyncio

    from hermes_onebot_fleet.lock import InstanceLock

    a = make(adapter_module, tmp_path)
    a._running = True
    a._lock = InstanceLock(tmp_path / "instance.lock")
    a._lock.acquire()
    a._tasks = [asyncio.create_task(asyncio.sleep(10))]
    tasks = a._tasks.copy()
    stopped = []

    async def stop_connection():
        stopped.append(1)

    for c in a._connections.values():
        c.stop = stop_connection

    class BrokenPublisher:
        async def stop(self):
            raise asyncio.CancelledError()

    a._publisher = BrokenPublisher()
    with pytest.raises(asyncio.CancelledError):
        await a._stop()
    assert all(t.done() for t in tasks) and len(stopped) == 2
    other = InstanceLock(tmp_path / "instance.lock")
    other.acquire()
    other.release()


@pytest.mark.asyncio
async def test_publication_callback_has_stable_outbound_receipt(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    for _ in range(2):
        result = await a._publish_once("stable-business-slot", "obf:onebot-fleet:private:20001", "hello")
        assert result == {"state": "sent", "message_id": "42", "account": "10001"}
    assert len(a._connections["10001"].calls) == 1
    assert adapter_module.PUBLICATION.get() is None


@pytest.mark.asyncio
async def test_publication_unknown_never_replays_or_switches_account(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    a._connections["10001"].ambiguous = True
    for _ in range(2):
        result = await a._publish_once("lost-ack-slot", "obf:onebot-fleet:private:20001", "hello")
        assert result["state"] == "unknown"
    assert len(a._connections["10001"].calls) == 1
    assert not a._connections["10002"].calls


@pytest.mark.asyncio
async def test_publication_offline_is_known_no_send(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    for c in a._connections.values():
        c.healthy = False
    with pytest.raises(adapter_module.NoRoute):
        await a._publish_once("offline-slot", "obf:onebot-fleet:private:20001", "hello")
    assert not a._store.status()["outbound"]


@pytest.mark.asyncio
async def test_queued_work_rechecks_revoked_policy(adapter_module, tmp_path):
    import asyncio
    from dataclasses import asdict, replace

    from hermes_onebot_fleet.protocol import normalize_event

    a = make(adapter_module, tmp_path, anchors=False)
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
