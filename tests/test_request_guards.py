"""Offline integration guards: fake RPCs and clocks, never sockets or QQ."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from test_adapter import adapter_module as adapter_module
from test_adapter import make

from hermes_onebot_fleet.alerts import AlertWatcher
from hermes_onebot_fleet.config import Account
from hermes_onebot_fleet.delivery_budget import BudgetPolicy, DeliveryBudget
from hermes_onebot_fleet.health import HealthState
from hermes_onebot_fleet.publisher import Publisher, PublisherClient
from hermes_onebot_fleet.read_requests import ReadRequests
from hermes_onebot_fleet.transport import ActionUncertain, OneBotConnection

GROUP = "obf:onebot-fleet:group:30001"
PRIVATE = "obf:onebot-fleet:private:20001"


class Clock:
    def __init__(self, value=1000):
        self.value = value

    def __call__(self):
        return self.value


def connection(clock, *, reads=None):
    account = Account("10001", "ws://127.0.0.1:3001", "TOKEN_A")
    transport = OneBotConnection(account, None, lambda event: None, clock=clock, reads=reads)
    transport._ws = SimpleNamespace(closed=False)
    return transport


def health_rpc(calls):
    async def rpc(action, params, timeout):
        calls.append((action, params, timeout))
        if action == "get_login_info":
            return {"user_id": 10001}
        assert action == "get_status"
        return {"online": True, "good": True}

    return rpc


def guarded_adapter(module, tmp_path, clock):
    adapter = make(module, tmp_path)
    # Separate from make()'s deliberately progressing compatibility clocks.
    adapter._reads = ReadRequests(
        tmp_path / "guard-reads.db", adapter.settings.logical_id, adapter.settings.reads, clock=clock
    )
    adapter._budget = DeliveryBudget(
        tmp_path / "guard-budget.db",
        adapter.settings.logical_id,
        BudgetPolicy(min_interval_seconds=1, target_interval_seconds=1),
        clock=clock,
    )
    adapter._connections["10002"].healthy = False
    return adapter


@pytest.mark.asyncio
@pytest.mark.parametrize("idle,interval", [(False, 60), (True, 180)])
async def test_periodic_health_cadence_and_exact_boundary(idle, interval):
    clock, calls = Clock(), []
    transport = connection(clock)
    transport.rpc = health_rpc(calls)
    assert await transport.check_health()  # Initial explicit verification is fresh.
    assert [call[0] for call in calls] == ["get_login_info", "get_status"]
    for offset in (0, 5, 30, interval - 0.001):
        clock.value = 1000 + offset
        assert await transport.check_health(periodic=True, idle=idle)
        assert len(calls) == 2
    clock.value = 1000 + interval
    assert await transport.check_health(periodic=True, idle=idle)
    assert len(calls) == 4
    clock.value += interval - 0.001
    assert await transport.check_health(periodic=True, idle=idle)
    assert len(calls) == 4
    clock.value = 1000 + interval * 2
    assert await transport.check_health(periodic=True, idle=idle)
    assert len(calls) == 6


@pytest.mark.asyncio
async def test_failed_health_exponential_backoff_caps_and_resets_on_recovery():
    clock, calls = Clock(), []
    transport = connection(clock)
    failing = True

    async def rpc(action, params, timeout):
        calls.append(action)
        if failing:
            raise ActionUncertain("probe result unknown")
        return {"user_id": 10001} if action == "get_login_info" else {"online": True, "good": True}

    transport.rpc = rpc
    assert not await transport.check_health()
    for expected_delay in (120, 240, 480, 600, 600):
        start, before = clock.value, len(calls)
        clock.value = start + expected_delay - 0.001
        assert not await transport.check_health(periodic=True)
        assert len(calls) == before
        clock.value = start + expected_delay
        assert not await transport.check_health(periodic=True)
        assert len(calls) == before + 1
    failing = False
    clock.value += 600
    assert await transport.check_health(periodic=True)
    before = len(calls)
    clock.value += 59.999
    assert await transport.check_health(periodic=True)
    assert len(calls) == before
    clock.value += 0.001
    assert await transport.check_health(periodic=True)
    assert len(calls) == before + 2


@pytest.mark.asyncio
async def test_idle_health_failure_respects_both_idle_floor_and_backoff():
    clock, calls = Clock(), []
    transport = connection(clock)

    async def rpc(action, params, timeout):
        calls.append(action)
        raise ActionUncertain("unknown")

    transport.rpc = rpc
    assert not await transport.check_health(idle=True)
    clock.value += 179.999
    assert not await transport.check_health(periodic=True, idle=True)
    assert len(calls) == 1
    clock.value = 1180
    assert not await transport.check_health(periodic=True, idle=True)
    assert len(calls) == 2
    clock.value += 239.999
    assert not await transport.check_health(periodic=True, idle=True)
    assert len(calls) == 2
    clock.value = 1420
    assert not await transport.check_health(periodic=True, idle=True)
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_disconnected_health_never_dispatches_rpc():
    transport = connection(Clock())

    async def forbidden_rpc(*args, **kwargs):
        pytest.fail("disconnected connection dispatched a health RPC")

    transport.rpc = forbidden_rpc
    for ws in (None, SimpleNamespace(closed=True)):
        transport._ws = ws
        for periodic in (False, True):
            assert not await transport.check_health(periodic=periodic, idle=True)
    assert transport._health_task is None


@pytest.mark.asyncio
async def test_explicit_health_check_is_fresh_even_within_periodic_cooldown():
    calls, transport = [], connection(Clock())
    transport.rpc = health_rpc(calls)
    assert await transport.check_health()
    assert await transport.check_health(periodic=True)
    assert len(calls) == 2
    assert await transport.check_health()
    assert len(calls) == 4


@pytest.mark.asyncio
async def test_same_socket_health_singleflight_survives_cancelled_waiter():
    transport = connection(Clock())
    started, release, calls = asyncio.Event(), asyncio.Event(), []

    async def rpc(action, params, timeout):
        calls.append(action)
        if action == "get_login_info":
            started.set()
            await release.wait()
            return {"user_id": 10001}
        return {"online": True, "good": True}

    transport.rpc = rpc
    first = asyncio.create_task(transport.check_health())
    second = None
    try:
        await asyncio.wait_for(started.wait(), 1)
        second = asyncio.create_task(transport.check_health(periodic=True))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not transport._health_task.cancelled()
        assert not transport._health_task.done()
        release.set()
        assert await asyncio.wait_for(second, 1)
        assert calls == ["get_login_info", "get_status"]
        assert transport.healthy
    finally:
        release.set()
        await asyncio.gather(*(task for task in (first, second) if task is not None), return_exceptions=True)
        await transport.stop()


@pytest.mark.asyncio
async def test_obsolete_socket_probe_cannot_authenticate_replacement_socket():
    transport = connection(Clock())
    started, release, calls = asyncio.Event(), asyncio.Event(), []

    async def rpc(action, params, timeout):
        calls.append(action)
        if action == "get_login_info":
            if len(calls) == 1:
                started.set()
                await release.wait()
            return {"user_id": 10001}
        return {"online": True, "good": True}

    transport.rpc = rpc
    old = asyncio.create_task(transport.check_health())
    try:
        await asyncio.wait_for(started.wait(), 1)
        transport._ws = SimpleNamespace(closed=False)
        release.set()
        assert not await asyncio.wait_for(old, 1)
        assert not transport.healthy
        assert transport.observation["identity"] == "unknown"
        assert not transport._ready.is_set()
        # The new socket must ignore the old socket's schedule and prove identity.
        assert await transport.check_health(periodic=True)
        assert calls == ["get_login_info", "get_status", "get_login_info", "get_status"]
    finally:
        release.set()
        await asyncio.gather(old, return_exceptions=True)
        await transport.stop()


@pytest.mark.asyncio
async def test_exhausted_send_budget_skips_membership_and_all_new_charges(
    adapter_module, tmp_path, monkeypatch
):
    clock = Clock()
    adapter = guarded_adapter(adapter_module, tmp_path, clock)
    adapter._budget = DeliveryBudget(
        tmp_path / "exhausted-budget.db", "onebot-fleet", BudgetPolicy(global_per_hour=1), clock=clock
    )
    assert adapter._budget.reserve("earlier-send", PRIVATE)["allowed"]
    budget_before = adapter._budget.path.read_bytes()
    reads_before = adapter._reads.path.read_bytes()

    def forbidden_reserve(*args, **kwargs):
        pytest.fail("early budget rejection must not reserve another send")

    monkeypatch.setattr(adapter._budget, "reserve", forbidden_reserve)
    result = await adapter.send(GROUP, "new answer", reply_to="group-inbound-1")
    assert not result.success and result.error == "shared sending budget denied"
    assert all(not c.calls for c in adapter._connections.values())
    assert adapter._budget.status()["reservations_total"] == 1
    assert adapter._reads.status()["reservations_total"] == 0
    assert adapter._store.status()["outbound"] == {}
    assert adapter._budget.path.read_bytes() == budget_before
    assert adapter._reads.path.read_bytes() == reads_before


@pytest.mark.asyncio
async def test_uncertain_member_reads_share_cooldown_across_publishers_intents_and_restart(
    adapter_module, tmp_path
):
    clock, calls = Clock(), []
    adapter = guarded_adapter(adapter_module, tmp_path, clock)
    clients = [
        PublisherClient("source-a", "PUB_A", {"news": GROUP}),
        PublisherClient("source-b", "PUB_B", {"news": GROUP}),
    ]

    def install_uncertain_rpc(instance):
        async def rpc(action, params):
            calls.append((action, params))
            assert action == "get_group_member_info", "unknown member evidence must never authorize a send"
            raise ActionUncertain("member response lost")

        instance._connections["10001"].rpc = rpc

    install_uncertain_rpc(adapter)
    for client, intent in ((clients[0], "slot-1"), (clients[1], "slot-1"), (clients[0], "slot-2")):
        with pytest.raises(adapter_module.NoRoute):
            await adapter._publish_once(Publisher.stable_key(client.id, intent), GROUP, "publication")
    assert len(calls) == 1
    assert adapter._reads.status()["reservations_total"] == 1
    assert adapter._budget.status()["reservations_total"] == 0
    assert adapter._health.status()["circuits"] == []  # Unknown is not a group restriction.
    assert adapter._store.status()["outbound"] == {}

    restarted = guarded_adapter(adapter_module, tmp_path, clock)
    restarted._reads = ReadRequests.open_existing(adapter._reads.path, clock=clock)
    install_uncertain_rpc(restarted)
    clock.value = 1014.999
    with pytest.raises(adapter_module.NoRoute):
        await restarted._publish_once(
            Publisher.stable_key(clients[1].id, "slot-after-restart"), GROUP, "new intent"
        )
    assert len(calls) == 1
    clock.value = 1015
    with pytest.raises(adapter_module.NoRoute):
        await restarted._publish_once(Publisher.stable_key(clients[1].id, "slot-later"), GROUP, "later")
    assert len(calls) == 2
    assert restarted._reads.status()["reservations_total"] == 2
    assert restarted._budget.status()["reservations_total"] == 0
    assert restarted._store.status()["outbound"] == {}


@pytest.mark.asyncio
async def test_read_storage_failure_latches_business_delivery_without_rpc(
    adapter_module, tmp_path, monkeypatch
):
    clock = Clock()
    adapter = guarded_adapter(adapter_module, tmp_path, clock)

    def broken_storage(*args, **kwargs):
        raise OSError("simulated read sidecar failure")

    with monkeypatch.context() as patch:
        patch.setattr(adapter._reads, "reserve_member", broken_storage)
        result = await adapter.send(GROUP, "first", reply_to="group-inbound-1")
    assert not result.success
    assert adapter._reads_failed
    assert adapter._reads.status()["reservations_total"] == 0
    # Repairing storage alone is not permission to clear the operational latch.
    for target, reply in ((GROUP, "group-inbound-1"), (PRIVATE, "inbound-1")):
        result = await adapter.send(target, "after repair", reply_to=reply)
        assert not result.success
    assert all(not c.calls for c in adapter._connections.values())
    assert adapter._budget.status()["reservations_total"] == 0
    assert adapter._store.status()["outbound"] == {}


@pytest.mark.asyncio
async def test_positive_membership_is_never_cached_as_permission_to_send(adapter_module, tmp_path):
    clock = Clock()
    adapter = guarded_adapter(adapter_module, tmp_path, clock)
    primary = adapter._connections["10001"]
    assert (await adapter.send(GROUP, "first", reply_to="group-inbound-1")).success
    assert [action for action, _ in primary.calls] == ["get_group_member_info", "send_group_msg"]
    clock.value = 1002  # Sending budget is available, but member cooldown is not.
    result = await adapter.send(GROUP, "different intent", reply_to="group-inbound-1")
    assert not result.success
    assert len(primary.calls) == 2
    assert adapter._budget.status()["reservations_total"] == 1
    assert adapter._reads.status()["reservations_total"] == 1

    async def no_longer_member(action, params):
        primary.calls.append((action, params))
        assert action == "get_group_member_info"
        return {"user_id": "99999"}

    primary.rpc = no_longer_member
    clock.value = 1015
    result = await adapter.send(GROUP, "new membership required", reply_to="group-inbound-1")
    assert not result.success
    assert [action for action, _ in primary.calls] == [
        "get_group_member_info",
        "send_group_msg",
        "get_group_member_info",
    ]
    assert adapter._reads.status()["reservations_total"] == 2
    assert adapter._budget.status()["reservations_total"] == 1


@pytest.mark.asyncio
async def test_monitor_tick_uses_periodic_idle_policy_and_separate_heartbeat(
    adapter_module, tmp_path, monkeypatch
):
    clock = Clock()
    adapter = guarded_adapter(adapter_module, tmp_path, clock)
    adapter._health = HealthState(tmp_path / "monitor-health.db", "onebot-fleet", clock=clock)
    adapter._health.observe("10001", connected=True, identity="verified", online=True, good=True)
    adapter._admission.change("10002", "pause", "maintenance")
    checked = []
    for account_id, item in adapter._connections.items():

        async def check_health(*, periodic, idle, account_id=account_id):
            checked.append((account_id, periodic, idle))
            return account_id == "10001"

        item.check_health = check_health

    async def stop_after_tick(delay):
        assert delay == 5
        raise asyncio.CancelledError

    clock.value = 1100
    with monkeypatch.context() as patch:
        patch.setattr(adapter_module.asyncio, "sleep", stop_after_tick)
        with pytest.raises(asyncio.CancelledError):
            await adapter._health_loop()
    assert sorted(checked) == [("10001", True, False), ("10002", True, True)]
    snapshot = adapter._health.status()
    primary = next(row for row in snapshot["accounts"] if row["account_id"] == "10001")
    assert primary["updated_at"] == 1000
    assert adapter._health.observer_updated_at() == 1100
    assert adapter._health.events()[0]["code"] == "pool_available"
    assert all(not item.calls for item in adapter._connections.values())


@pytest.mark.parametrize("pool_event", ["pool_available", "pool_unavailable"])
def test_alert_watcher_uses_local_heartbeat_not_sparse_remote_probes(tmp_path, pool_event):
    clock = Clock()
    health = HealthState(tmp_path / "health.db", "onebot-fleet", clock=clock)
    health.observe("10001", connected=True, identity="verified", online=True, good=True)
    assert health.observer_updated_at() == 1000  # Legacy journal fallback.
    output = tmp_path / "alerts.jsonl"
    watcher = AlertWatcher(health.path, output, clock=clock, stale_after=30)
    for now in (1005, 1030, 1060, 1090):
        clock.value = now
        health.report("10001", pool_event)
        assert watcher.poll()["observer_state"] == "healthy"
        assert health.status()["accounts"][0]["updated_at"] == 1000
        assert health.observer_updated_at() == now
    assert not any(
        row.get("code") == "observer_stale" for row in map(json.loads, output.read_text().splitlines())
    )
    # The monitor stops, despite the remote connection's last good observation.
    clock.value = 1120
    assert watcher.poll()["observer_state"] == "healthy"  # Exact stale threshold.
    clock.value = 1120.001
    assert watcher.poll()["observer_state"] == "stale"
    assert health.observer_updated_at() == 1090
    # Manual CLI reset is not proof that the monitor is still running.
    clock.value = 1125
    health.reset("10001", reason="operator review")
    assert health.observer_updated_at() == 1090
    assert watcher.poll()["observer_state"] == "stale"
    clock.value = 1130
    health.report("10001", "pool_available")
    assert watcher.poll()["observer_state"] == "healthy"
    stale = [
        row for row in map(json.loads, output.read_text().splitlines()) if row.get("code") == "observer_stale"
    ]
    assert [row["state"] for row in stale] == ["active", "resolved"]


def test_reset_without_pool_events_does_not_refresh_legacy_observer_fallback(tmp_path):
    clock = Clock()
    health = HealthState(tmp_path / "health.db", "onebot-fleet", clock=clock)
    assert health.observer_updated_at() is None
    health.observe("10001", connected=True, identity="verified", online=True, good=True)
    clock.value = 1100
    health.reset("10001", reason="operator review")
    assert health.status()["accounts"][0]["updated_at"] == 1000
    assert health.observer_updated_at() == 1000
    watcher = AlertWatcher(health.path, tmp_path / "alerts.jsonl", clock=clock)
    assert watcher.poll()["observer_state"] == "stale"
