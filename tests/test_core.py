"""Host-free core contracts, identity isolation, and fake-RPC delivery safety."""

import asyncio
import importlib
import subprocess
import sys
import time
from dataclasses import asdict, replace
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from test_adapter import adapter_module as adapter_module

from onebot_fleet import FleetCore, FleetSendResult
from onebot_fleet import core as core_module
from onebot_fleet.config import Settings
from onebot_fleet.delivery_budget import DeliveryBudget
from onebot_fleet.protocol import Incoming
from onebot_fleet.publisher import Deferred, NoRoute
from onebot_fleet.transport import ActionUncertain

PRIVATE = "obf:onebot-fleet:private:20001"
GROUP = "obf:onebot-fleet:group:30001"
PURE_MODULES = (
    "config",
    "protocol",
    "publisher",
    "transport",
    "store",
    "health",
    "delivery_budget",
    "read_requests",
    "reconnect",
    "stop_loss",
    "account_state",
    "lock",
)


def settings(**changes):
    data = {
        "accounts": [
            {"id": "10001", "ws_url": "ws://127.0.0.1:3001", "token_env": "TOKEN_A"},
            {"id": "10002", "ws_url": "ws://127.0.0.1:3002", "token_env": "TOKEN_B", "priority": 1},
        ],
        "allowed_users": ["20001"],
        "allowed_groups": ["30001"],
    }
    data.update(changes)
    return Settings.from_mapping(data)


class FakeConnection:
    def __init__(self, account, token, callback, **kwargs):
        self.account = account
        self.token = token
        self.callback = callback
        self.healthy = True
        self.calls = []
        self.starts = 0
        self.stops = 0
        self.ambiguous = False

    async def start(self):
        self.starts += 1

    async def stop(self):
        self.stops += 1

    async def check_health(self, **kwargs):
        pass

    async def rpc(self, action, params):
        self.calls.append((action, params))
        if action == "get_group_member_info":
            return {"user_id": self.account.id}
        if self.ambiguous:
            raise ActionUncertain("fake peer lost the acknowledgement")
        return {"message_id": 42}


def make(tmp_path, monkeypatch, *, cls=FleetCore, handler=None, receive_events=False, config=None):
    monkeypatch.setattr(core_module, "OneBotConnection", FakeConnection)
    return cls(
        config or settings(),
        state_path=tmp_path / "core.sqlite3",
        secrets={"TOKEN_A": "a" * 20, "TOKEN_B": "b" * 20, "PUBLISHER_TOKEN": "p" * 40},
        message_handler=handler,
        receive_events=receive_events,
    )


def enqueue(fleet, key="incoming-one"):
    item = Incoming(key, key, "20001", "20001", "private", "question", "10001", 100, "onebot-fleet", ())
    payload = asdict(item)
    payload["_admission_revision"] = fleet._admission.snapshot("10001")["revision"]
    fleet._store.enqueue(key, payload)
    return item


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0)


@pytest.mark.parametrize("name", PURE_MODULES)
def test_pure_module_identity(name):
    canonical = importlib.import_module(f"onebot_fleet.{name}")
    legacy = importlib.import_module(f"hermes_onebot_fleet.{name}")
    assert canonical is legacy


def test_import_is_host_free_in_fresh_interpreter():
    source = """
import importlib.abc
import sys
class NoHost(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'gateway', 'hermes_constants', 'astrbot'}:
            raise AssertionError('unexpected host import: ' + fullname)
sys.meta_path.insert(0, NoHost())
from onebot_fleet import FleetCore, FleetSendResult
from onebot_fleet.config import Settings
from onebot_fleet.protocol import Incoming
assert not any(n.split('.')[0] in {'gateway', 'hermes_constants', 'astrbot'} for n in sys.modules)
"""
    result = subprocess.run([sys.executable, "-c", source], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


def test_profile_scoped_aliases_do_not_escape_or_cross_profiles(monkeypatch):
    root = Path(__file__).resolve().parents[1] / "src"
    loaded = []
    for prefix in ("fleet_profile_a.src", "fleet_profile_b._vendor"):
        parent_name, _, leaf = prefix.rpartition(".")
        parent = ModuleType(parent_name)
        parent.__path__ = []
        monkeypatch.setitem(sys.modules, parent_name, parent)
        namespace = ModuleType(prefix)
        namespace.__path__ = [str(root)]
        monkeypatch.setitem(sys.modules, prefix, namespace)
        setattr(parent, leaf, namespace)
        for name in PURE_MODULES:
            canonical = importlib.import_module(f"{prefix}.onebot_fleet.{name}")
            legacy = importlib.import_module(f"{prefix}.hermes_onebot_fleet.{name}")
            assert canonical is legacy
            assert canonical is not importlib.import_module(f"onebot_fleet.{name}")
        loaded.append(importlib.import_module(f"{prefix}.onebot_fleet.core"))
    try:
        assert loaded[0].Settings is not loaded[1].Settings
        assert loaded[0].Incoming is not loaded[1].Incoming
        assert loaded[0].PUBLICATION is not loaded[1].PUBLICATION
        assert loaded[0].ActionUncertain is not loaded[1].ActionUncertain
        assert loaded[0].Settings is not Settings
    finally:
        for name in list(sys.modules):
            if name.startswith(("fleet_profile_a.", "fleet_profile_b.")):
                sys.modules.pop(name, None)


def test_initialization_is_lazy_and_captures_only_supplied_secrets(tmp_path, monkeypatch):
    fleet = make(tmp_path, monkeypatch)
    assert fleet._store is None and not fleet._running
    assert not list(tmp_path.iterdir())
    assert fleet._connections["10001"].token() == "a" * 20
    monkeypatch.setenv("TOKEN_A", "changed-in-process" * 2)
    assert fleet._connections["10001"].token() == "a" * 20
    with pytest.raises(ValueError, match="token environment variable"):
        FleetCore(settings(), state_path=tmp_path / "unused", secrets={})


@pytest.mark.asyncio
async def test_core_lifecycle_is_idempotent_and_uses_explicit_path(tmp_path, monkeypatch):
    fleet = make(tmp_path, monkeypatch)
    try:
        assert await fleet.connect()
        assert await fleet.connect(is_reconnect=True)
        assert fleet._store.path == tmp_path / "core.sqlite3"
        assert all(peer.starts == 1 for peer in fleet._connections.values())
        assert len(fleet._tasks) == 1
    finally:
        await fleet.disconnect()
    assert not fleet._running and not fleet._tasks
    assert all(peer.stops == 1 for peer in fleet._connections.values())


@pytest.mark.asyncio
async def test_core_send_returns_pure_result_and_requires_durable_origin(tmp_path, monkeypatch):
    fleet = make(tmp_path, monkeypatch)
    await fleet.connect()
    try:
        denied = await fleet.send(PRIVATE, "hello", metadata={"publication_id": "forged"})
        assert type(denied) is FleetSendResult
        assert not denied.success and not denied.retryable
        assert "durable publisher ID" in denied.error
        item = enqueue(fleet)
        fleet._store.inbound_result(item.dedup_key, "handed_off")
        for _ in range(2):
            result = await fleet.send(PRIVATE, "hello", reply_to=item.dedup_key)
            assert type(result) is FleetSendResult
            assert result == FleetSendResult(True, "42")
        assert len(fleet._connections["10001"].calls) == 1
        assert not fleet._connections["10002"].calls
    finally:
        await fleet.disconnect()


@pytest.mark.asyncio
async def test_core_unknown_never_replays_or_changes_account(tmp_path, monkeypatch):
    fleet = make(tmp_path, monkeypatch)
    await fleet.connect()
    try:
        fleet._connections["10001"].ambiguous = True
        for _ in range(2):
            assert await fleet._publish_once("same-slot", PRIVATE, "hello") == {"state": "unknown"}
        assert len(fleet._connections["10001"].calls) == 1
        assert not fleet._connections["10002"].calls
        assert fleet._store.status()["outbound"] == {"uncertain": 1}
        assert core_module.PUBLICATION.get() is None
    finally:
        await fleet.disconnect()


@pytest.mark.asyncio
async def test_core_publication_gates_budget_and_known_no_route(tmp_path, monkeypatch):
    fleet = make(tmp_path, monkeypatch)
    await fleet.connect()
    now = [time.time()]
    fleet._budget = DeliveryBudget(
        tmp_path / "fixed-budget.db", "onebot-fleet", fleet.settings.budget, clock=lambda: now[0]
    )
    try:
        assert (await fleet._publish_once("one", PRIVATE, "first"))["state"] == "sent"
        fleet._connections["10001"].healthy = False
        with pytest.raises(Deferred):
            await fleet._publish_once("two", PRIVATE, "second")
        assert not fleet._connections["10002"].calls
        now[0] += 10
        assert (await fleet._publish_once("two", PRIVATE, "second"))["account"] == "10002"
        now[0] += 10
        fleet._connections["10002"].healthy = False
        with pytest.raises(NoRoute):
            await fleet._publish_once("three", PRIVATE, "third")
        assert fleet._budget.status()["reservations_total"] == 2
    finally:
        await fleet.disconnect()


@pytest.mark.asyncio
async def test_drain_awaits_authorized_handler_before_handed_off(tmp_path, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    received = []

    async def handle(item):
        received.append(item)
        assert core_module.INGRESS.get() == item.dedup_key
        entered.set()
        await release.wait()

    fleet = make(tmp_path, monkeypatch, handler=handle, receive_events=True)
    await fleet.connect()
    item = enqueue(fleet)
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert fleet._store.inbound(item.dedup_key)["state"] == "handing_off"
        release.set()
        await until(lambda: fleet._store.inbound(item.dedup_key)["state"] == "handed_off")
        assert len(received) == 1
        assert isinstance(received[0], Incoming)
        assert received[0].dedup_key == item.dedup_key
        assert received[0].text == item.text
        # A host handoff is not an execution result or evidence of a QQ reply.
        assert fleet._store.status()["outbound"] == {}
        assert core_module.INGRESS.get() is None
    finally:
        await fleet.disconnect()


@pytest.mark.asyncio
async def test_hook_dispatch_without_message_handler(tmp_path, monkeypatch):
    received = []

    class HostCore(FleetCore):
        async def _dispatch_incoming(self, item):
            received.append(item)

    fleet = make(tmp_path, monkeypatch, cls=HostCore, receive_events=True)
    await fleet.connect()
    item = enqueue(fleet)
    try:
        await until(lambda: fleet._store.inbound(item.dedup_key)["state"] == "handed_off")
        assert len(received) == 1
        assert isinstance(received[0], Incoming)
        assert received[0].dedup_key == item.dedup_key
        assert received[0].text == item.text
    finally:
        await fleet.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("revocation", ["policy", "account", "health", "revision", "age"])
async def test_core_drain_rechecks_queued_authorization(tmp_path, monkeypatch, revocation):
    received = []

    async def handle(item):
        received.append(item)

    fleet = make(tmp_path, monkeypatch, handler=handle)
    await fleet.connect()
    item = enqueue(fleet)
    if revocation == "policy":
        fleet.settings = replace(fleet.settings, allowed_users=frozenset())
    elif revocation == "account":
        fleet._admission.change("10001", "pause", "hold")
    elif revocation == "health":
        fleet._health.report("10001", "account_anomaly")
    elif revocation == "revision":
        fleet._admission.change("10001", "pause", "hold")
        fleet._admission.change("10001", "allow", "reviewed")
    else:
        monkeypatch.setattr(fleet, "_fresh", lambda *_: False)
    task = asyncio.create_task(fleet._drain())
    try:
        await until(lambda: fleet._store.inbound(item.dedup_key)["state"] == "denied")
        assert not received
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await fleet.disconnect()


@pytest.mark.asyncio
async def test_cancelled_handoff_is_uncertain_not_success(tmp_path, monkeypatch):
    entered = asyncio.Event()

    async def handle(item):
        entered.set()
        await asyncio.Event().wait()

    fleet = make(tmp_path, monkeypatch, handler=handle, receive_events=True)
    await fleet.connect()
    item = enqueue(fleet)
    await asyncio.wait_for(entered.wait(), 2)
    await fleet.disconnect()
    assert fleet._store.inbound(item.dedup_key)["state"] == "uncertain"
    assert fleet._store.status()["outbound"] == {}


@pytest.mark.asyncio
async def test_core_publisher_target_cannot_expand_destination_policy(tmp_path, monkeypatch):
    config = settings(
        publisher={
            "enabled": True,
            "port": 18791,
            "clients": [
                {
                    "id": "scheduler",
                    "token_env": "PUBLISHER_TOKEN",
                    "targets": {"other": "obf:onebot-fleet:private:29999"},
                }
            ],
        }
    )
    with pytest.raises(ValueError, match="publisher target exceeds"):
        make(tmp_path, monkeypatch, config=config)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_missing_handler_leaves_ingress_queued(tmp_path, monkeypatch):
    fleet = make(tmp_path, monkeypatch, receive_events=True)
    await fleet.connect()
    item = enqueue(fleet)
    # One synchronous drain iteration reaches the no-handler sleep without
    # claiming the row. This avoids timing-based assumptions about worker speed.
    slept = asyncio.Event()
    original_sleep = asyncio.sleep

    async def observe_sleep(delay):
        slept.set()
        await original_sleep(delay)

    monkeypatch.setattr(core_module.asyncio, "sleep", observe_sleep)
    try:
        await asyncio.wait_for(slept.wait(), 2)
        assert fleet._store.inbound(item.dedup_key)["state"] == "queued"
    finally:
        await fleet.disconnect()


@pytest.mark.asyncio
async def test_failed_handler_is_uncertain_and_not_replayed(tmp_path, monkeypatch):
    calls = []

    async def fail(item):
        calls.append(item)
        raise RuntimeError("fake host rejected its handoff")

    fleet = make(tmp_path, monkeypatch, handler=fail, receive_events=True)
    await fleet.connect()
    item = enqueue(fleet)
    try:
        await until(lambda: fleet._store.inbound(item.dedup_key)["state"] == "uncertain")
        fleet._store.recover()
        assert fleet._store.claim() is None
        assert len(calls) == 1
        assert fleet._store.status()["outbound"] == {}
    finally:
        await fleet.disconnect()


@pytest.mark.asyncio
async def test_native_edge_preserves_host_initialization_path_event_and_result(
    adapter_module, tmp_path, monkeypatch
):
    initialized = []

    def host_init(self, config, platform):
        initialized.append((config, platform))
        self._message_handler = None

    monkeypatch.setattr(adapter_module.BasePlatformAdapter, "__init__", host_init)
    monkeypatch.setattr(adapter_module, "OneBotConnection", FakeConnection)
    shared = ModuleType("gateway.platforms._shared")
    shared.get_scoped_secret = lambda name: "s" * 40
    constants = ModuleType("hermes_constants")
    constants.get_hermes_home = lambda: tmp_path
    monkeypatch.setitem(sys.modules, shared.__name__, shared)
    monkeypatch.setitem(sys.modules, constants.__name__, constants)
    config = SimpleNamespace(
        extra={
            "accounts": [{"id": "10001", "ws_url": "ws://127.0.0.1:3001", "token_env": "SCOPED_TOKEN"}],
            "allowed_users": ["20001"],
            "state_path": "custom.sqlite3",
        }
    )
    fleet = adapter_module.OneBotFleetAdapter(config)
    assert initialized == [(config, "onebot_fleet")]
    assert isinstance(fleet, FleetCore)
    assert fleet.__class__.__module__ == "hermes_onebot_fleet.adapter"
    assert fleet._path == tmp_path / "plugin-data" / "onebot-fleet" / "custom.sqlite3"
    assert fleet._connections["10001"].token() == "s" * 40
    fleet._message_handler = True
    fleet.build_source = lambda **kwargs: SimpleNamespace(**kwargs)
    events = []

    async def handle_message(event):
        events.append(event)
        assert adapter_module.INGRESS.get() == event.message_id

    fleet.handle_message = handle_message
    await fleet.connect()
    item = enqueue(fleet)
    try:
        await until(lambda: fleet._store.inbound(item.dedup_key)["state"] == "handed_off")
        assert len(events) == 1
        event = events[0]
        assert event.text == item.text and event.message_type == "text"
        assert event.source.chat_id == PRIVATE and event.source.chat_type == "dm"
        assert event.source.scope_id == "onebot-fleet"
        assert event.message_id == item.dedup_key and event.metadata == {}
        assert event.timestamp.timestamp() == item.timestamp
        result = await fleet.send(PRIVATE, "reply", reply_to=item.dedup_key)
        assert type(result) is adapter_module.SendResult
        assert not isinstance(result, FleetSendResult)
        assert result.success and result.message_id == "42"
        assert not fleet._send_retry_is_final(result)
        assert fleet._send_retry_is_final(adapter_module.SendResult(False))
    finally:
        await fleet.disconnect()
