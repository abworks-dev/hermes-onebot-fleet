"""Aggregate stop-loss bridge contracts using fake RPCs only: never sockets or QQ."""

import asyncio
import hashlib
import json
from dataclasses import replace

import pytest
from test_adapter import adapter_module as adapter_module
from test_adapter import make

from hermes_onebot_fleet.alerts import AlertWatcher
from hermes_onebot_fleet.delivery_budget import DeliveryBudget
from hermes_onebot_fleet.health import HealthState
from hermes_onebot_fleet.publisher import NoRoute
from hermes_onebot_fleet.read_requests import ReadRequests
from hermes_onebot_fleet.stop_loss import StopLoss
from hermes_onebot_fleet.transport import ActionError, ActionUncertain

PRIVATE = "obf:onebot-fleet:private:20001"
GROUP = "obf:onebot-fleet:group:30001"


class Clock:
    def __init__(self, value=1000):
        self.value = value

    def __call__(self):
        return self.value


def adapter(module, tmp_path, clock):
    result = make(module, tmp_path)
    result.settings = replace(
        result.settings, allowed_users=frozenset(str(value) for value in range(20001, 20010))
    )
    for attribute, factory, policy in (
        ("_budget", DeliveryBudget, result.settings.budget),
        ("_reads", ReadRequests, result.settings.reads),
        ("_stoploss", StopLoss, result.settings.stoploss),
    ):
        setattr(
            result,
            attribute,
            factory(tmp_path / ("integration" + attribute + ".db"), "onebot-fleet", policy, clock=clock),
        )
    result._health = HealthState(tmp_path / "integration-health.db", "onebot-fleet", clock=clock)
    return result


async def publish(module, fleet, identity, address=PRIVATE, content="payload"):
    token = module.PUBLICATION.set("publication:" + identity)
    try:
        return await fleet.send(address, content)
    finally:
        module.PUBLICATION.reset(token)


def intent(identity, address=PRIVATE, content="payload"):
    return hashlib.sha256(
        json.dumps(["publication:" + identity, address, content, None], ensure_ascii=False).encode()
    ).hexdigest()


def calls(fleet):
    return {account: list(connection.calls) for account, connection in fleet._connections.items()}


def rejecting_rpc(connection):
    async def rpc(action, params):
        connection.calls.append((action, params))
        if action == "get_group_member_info":
            return {"user_id": connection.account}
        raise ActionError(action, 100)

    connection.rpc = rpc


async def three_distinct_rejections(module, fleet, clock):
    rejecting_rpc(fleet._connections["10001"])
    for index in range(3):
        address = f"obf:onebot-fleet:private:{20001 + index}"
        result = await publish(module, fleet, f"reject-{index}", address)
        assert not result.success and not result.retryable
        assert not fleet._connections["10002"].calls
        clock.value += 11
    assert fleet._stoploss.status()["failures_total"] == 3
    assert fleet._stoploss.status()["business_held"]


@pytest.mark.asyncio
async def test_account_threshold_stops_all_entries_without_member_read_or_bot_switch(
    adapter_module, tmp_path
):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    await three_distinct_rejections(adapter_module, fleet, clock)
    state = fleet._stoploss.status()
    assert not state["fleet_held"] and len(state["held_accounts"]) == 1
    assert not fleet._stoploss.is_eligible("10001")
    assert fleet._stoploss.is_eligible("10002")  # A scope veto is NOT permission to switch bots.
    before = calls(fleet)
    fleet._admission.promote("10002", "reviewed preference")
    for address in (GROUP, "obf:onebot-fleet:private:20004"):
        assert not (await publish(adapter_module, fleet, "held-" + address, address)).success
    assert not (await fleet.send(GROUP, "reply", reply_to="group-inbound-1")).success
    with pytest.raises(NoRoute):
        await fleet._publish_once("held-publisher", GROUP, "publisher")
    assert await fleet._choose("group", "30001") is None
    assert calls(fleet) == before  # Includes the read-only member RPC, not merely sends.
    assert fleet._budget.status()["reservations_total"] == 3


@pytest.mark.asyncio
async def test_two_accounts_mixed_failures_latch_fleet_without_postdispatch_fallback(
    adapter_module, tmp_path
):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    rejecting_rpc(fleet._connections["10001"])
    attempts = [("10001", 20001), ("10001", 20002), ("10002", 20001), ("10002", 20003)]
    for index, (account, target) in enumerate(attempts):
        if account == "10002":
            fleet._connections["10001"].healthy = False
            secondary = fleet._connections["10002"]

            async def unknown_rpc(action, params):
                secondary.calls.append((action, params))
                raise ActionUncertain("wire result unknown")

            secondary.rpc = unknown_rpc
        before = calls(fleet)
        address = f"obf:onebot-fleet:private:{target}"
        result = await publish(adapter_module, fleet, f"mixed-{index}", address)
        assert not result.success and not result.retryable
        assert len(fleet._connections[account].calls) == len(before[account]) + 1
        other = "10002" if account == "10001" else "10001"
        assert fleet._connections[other].calls == before[other]
        after = calls(fleet)
        assert not (await publish(adapter_module, fleet, f"mixed-{index}", address)).success
        assert calls(fleet) == after  # A stable uncertain/rejected intent is never replayed.
        clock.value += 11
    state = fleet._stoploss.status()
    assert state["fleet_held"] and not state["held_accounts"]
    assert state["fleet_window"] == {"failures": 4, "targets": 3, "accounts": 2}
    evidence = [row for row in fleet._stoploss.audit() if row["kind"] == "evidence"]
    assert sorted(row["code"] for row in evidence) == [
        "outcome_unknown",
        "outcome_unknown",
        "target_rejected",
        "target_rejected",
    ]
    before = calls(fleet)
    fleet._connections["10001"].healthy = True
    assert not (await publish(adapter_module, fleet, "fleet-held-new", GROUP)).success
    assert calls(fleet) == before


@pytest.mark.asyncio
async def test_read_preflight_and_known_predispatch_failure_are_not_aggregate_evidence(
    adapter_module, tmp_path
):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    primary = fleet._connections["10001"]
    fleet._connections["10002"].healthy = False
    mode = "timeout"

    async def rpc(action, params):
        primary.calls.append((action, params))
        if action == "get_group_member_info":
            if mode == "timeout":
                raise TimeoutError("inconclusive member read")
            return {"user_id": "99999"}
        raise ConnectionError("known no dispatch")

    primary.rpc = rpc
    for mode in ("timeout", "wrong-member"):
        assert not (await publish(adapter_module, fleet, "read-" + mode, GROUP)).success
        assert fleet._stoploss.audit() == []
        clock.value += 16
    assert [action for action, _ in primary.calls] == ["get_group_member_info"] * 2
    result = await publish(adapter_module, fleet, "disconnected-before-dispatch")
    assert not result.success and not result.retryable
    assert fleet._store.outbound(intent("disconnected-before-dispatch"))["state"] == "failed"
    assert fleet._stoploss.status()["failures_total"] == 0
    assert fleet._stoploss.audit() == []
    assert not fleet._connections["10002"].calls


@pytest.mark.asyncio
async def test_dispatched_unknown_invalid_ack_and_cancel_charge_once_each(adapter_module, tmp_path):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    primary = fleet._connections["10001"]
    entered = asyncio.Event()

    async def rpc(action, params):
        primary.calls.append((action, params))
        if params["user_id"] == 20001:
            raise ActionUncertain("acknowledgment lost")
        if params["user_id"] == 20002:
            return {"message_id": True}
        entered.set()
        await asyncio.Event().wait()

    primary.rpc = rpc
    for index in range(2):
        address = f"obf:onebot-fleet:private:{20001 + index}"
        for _ in range(2):
            result = await publish(adapter_module, fleet, f"uncertain-{index}", address)
            assert not result.success and not result.retryable
        assert len(primary.calls) == index + 1
        clock.value += 11
    address = "obf:onebot-fleet:private:20003"
    task = asyncio.create_task(publish(adapter_module, fleet, "cancelled", address))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert not (await publish(adapter_module, fleet, "cancelled", address)).success
    assert len(primary.calls) == 3 and not fleet._connections["10002"].calls
    assert fleet._stoploss.status()["failures_total"] == 3
    evidence = [row for row in fleet._stoploss.audit() if row["kind"] == "evidence"]
    assert len(evidence) == 3 and {row["code"] for row in evidence} == {"outcome_unknown"}
    assert fleet._store.status()["outbound"] == {"uncertain": 3}
    fleet._stoploss.reset("10001", reason="reviewed aggregate incident")
    before = calls(fleet)
    for index, identity in enumerate(("uncertain-0", "uncertain-1", "cancelled")):
        address = f"obf:onebot-fleet:private:{20001 + index}"
        assert not (await publish(adapter_module, fleet, identity, address)).success
    assert calls(fleet) == before
    assert fleet._store.status()["outbound"] == {"uncertain": 3}
    assert fleet._stoploss.status()["failures_total"] == 3


@pytest.mark.asyncio
async def test_ack_health_admission_time_and_restart_cannot_release_stoploss(adapter_module, tmp_path):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    primary = fleet._connections["10001"]
    rejecting_rpc(primary)
    assert not (await publish(adapter_module, fleet, "first-failure")).success
    clock.value += 11
    original = primary.rpc

    async def acknowledged(action, params):
        primary.calls.append((action, params))
        return {"message_id": 42}

    primary.rpc = acknowledged
    assert (await publish(adapter_module, fleet, "ack-between", "obf:onebot-fleet:private:20004")).success
    assert fleet._stoploss.status()["failures_total"] == 1
    primary.rpc = original
    for index in (2, 3):
        clock.value += 11
        address = f"obf:onebot-fleet:private:{20000 + index}"
        assert not (await publish(adapter_module, fleet, f"failure-{index}", address)).success
    assert fleet._stoploss.status()["business_held"]
    for index in range(1, 4):
        address = f"obf:onebot-fleet:private:{20000 + index}"
        fleet._health.report("10001", "send_ack", address)
        fleet._health.reset("10001", address, reason="reviewed target")
    fleet._health.reset("10001", reason="reviewed account")
    for account in ("10001", "10002"):
        fleet._admission.change(account, "pause", "maintenance")
        fleet._admission.change(account, "allow", "reviewed admission")
    fleet._admission.promote("10002", "reviewed preference")
    clock.value += 1000
    assert fleet._stoploss.status()["business_held"]
    restarted = adapter(adapter_module, tmp_path, clock)
    assert restarted._stoploss.status()["business_held"]
    assert not (await publish(adapter_module, restarted, "restart-new-direct", GROUP)).success
    assert all(not connection.calls for connection in restarted._connections.values())
    assert restarted._stoploss.status()["failures_total"] == 3


@pytest.mark.asyncio
async def test_aggregate_reset_does_not_refund_or_erase_other_safety_scopes(adapter_module, tmp_path):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    await three_distinct_rejections(adapter_module, fleet, clock)
    fleet._admission.change("10001", "quarantine", "operator review")
    fleet._admission.change("10002", "pause", "maintenance")
    health_before = fleet._health.status()
    admission_before = fleet._admission.status()
    charges_before = fleet._budget.status()["reservations_total"]
    before = calls(fleet)
    fleet._stoploss.reset(reason="fleet reviewed")
    assert fleet._stoploss.status()["business_held"]  # Fleet reset does not release account hold.
    fleet._stoploss.reset("10001", reason="account reviewed")
    assert not fleet._stoploss.status()["business_held"]
    assert fleet._health.status() == health_before
    assert fleet._admission.status() == admission_before
    assert fleet._budget.status()["reservations_total"] == charges_before == 3
    key = intent("reject-0")
    assert fleet._budget.check(key, PRIVATE)["reason"] == "prior_reservation"
    assert fleet._store.outbound(key)["state"] == "failed"
    assert not (await publish(adapter_module, fleet, "reject-0")).success
    assert not (await publish(adapter_module, fleet, "new-quarantined")).success
    fleet._admission.change("10001", "allow", "reviewed admission")
    assert not (await publish(adapter_module, fleet, "new-target-still-latched")).success
    assert calls(fleet) == before
    assert fleet._stoploss.status()["failures_total"] == 3


@pytest.mark.asyncio
async def test_stoploss_bridge_storage_failures_latch_process_hold_without_failover(
    adapter_module, tmp_path, monkeypatch, caplog
):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    rejecting_rpc(fleet._connections["10001"])

    def unavailable(*args, **kwargs):
        raise RuntimeError("private exception text must not leak")

    with monkeypatch.context() as patch:
        patch.setattr(fleet._stoploss, "record", unavailable)
        assert not (await publish(adapter_module, fleet, "failed-persistence")).success
    assert fleet._stoploss_failed
    assert fleet._stoploss.status()["failures_total"] == 0
    assert not fleet._connections["10002"].calls
    before = calls(fleet)
    fleet._admission.promote("10002", "reviewed preference")
    assert not (await publish(adapter_module, fleet, "after-record-repair", GROUP)).success
    assert calls(fleet) == before
    assert "private exception text" not in caplog.text
    assert "evidence persistence failed" in caplog.text

    second = adapter(adapter_module, tmp_path / "second", clock)
    with monkeypatch.context() as patch:
        patch.setattr(second._stoploss, "status", unavailable)
        assert not (await publish(adapter_module, second, "failed-status", GROUP)).success
    assert second._stoploss_failed
    assert not (await publish(adapter_module, second, "after-status-repair")).success
    assert all(not connection.calls for connection in second._connections.values())
    assert second._budget.status()["reservations_total"] == 0
    assert second._reads.status()["reservations_total"] == 0


@pytest.mark.asyncio
async def test_monitor_stoploss_idle_pool_alert_and_observer_heartbeat_are_separate(
    adapter_module, tmp_path, monkeypatch
):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    for account in ("10001", "10002"):
        fleet._health.observe(account, connected=True, identity="verified", online=True, good=True)
    await three_distinct_rejections(adapter_module, fleet, clock)
    checked = []
    for account, connection in fleet._connections.items():

        async def check_health(*, periodic, idle, account=account):
            checked.append((account, periodic, idle))
            return True

        connection.check_health = check_health

    async def stop_after_tick(delay):
        assert delay == 5
        raise asyncio.CancelledError

    before = calls(fleet)
    with monkeypatch.context() as patch:
        patch.setattr(adapter_module.asyncio, "sleep", stop_after_tick)
        with pytest.raises(asyncio.CancelledError):
            await fleet._health_loop()
    assert sorted(checked) == [("10001", True, True), ("10002", True, True)]
    assert calls(fleet) == before
    assert fleet._health.events()[0]["code"] == "pool_unavailable"
    assert any(
        row["code"] == "pool_unavailable" and row["state"] == "active" for row in fleet._health.alerts()
    )
    assert {row["updated_at"] for row in fleet._health.status()["accounts"]} == {1000}
    assert fleet._health.observer_updated_at() == clock.value
    watcher = AlertWatcher(fleet._health.path, tmp_path / "alerts.jsonl", clock=clock, stale_after=30)
    assert watcher.poll()["observer_state"] == "healthy"
    clock.value += 31
    assert watcher.poll()["observer_state"] == "stale"
    assert fleet._stoploss.status()["business_held"]
