"""Resilience orchestration uses only fake peers and temporary ledgers."""

import time

import pytest
from test_adapter import adapter_module as adapter_module
from test_adapter import make

from hermes_onebot_fleet.delivery_budget import BudgetPolicy, DeliveryBudget
from hermes_onebot_fleet.publisher import PUBLICATION_CREATED, Deferred, NoRoute
from hermes_onebot_fleet.transport import ActionError

GROUP = "obf:onebot-fleet:group:30001"
PRIVATE = "obf:onebot-fleet:private:20001"


def fixed_budget(a, path):
    now = [1000.0]
    a._budget = DeliveryBudget(path / "fixed-budget.db", "onebot-fleet", BudgetPolicy(), clock=lambda: now[0])
    return now


@pytest.mark.asyncio
async def test_budget_shared_after_account_switch_and_preserves_intent(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    now = fixed_budget(a, tmp_path)
    assert (await a._publish_once("one", PRIVATE, "first"))["account"] == "10001"
    a._connections["10001"].healthy = False
    with pytest.raises(Deferred):
        await a._publish_once("two", PRIVATE, "second")
    assert not a._connections["10002"].calls
    assert a._budget.status()["reservations_total"] == 1
    now[0] += 10
    assert (await a._publish_once("two", PRIVATE, "second"))["account"] == "10002"
    assert a._budget.status()["reservations_total"] == 2


@pytest.mark.asyncio
async def test_native_and_publisher_share_budget_without_native_retry(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    now = fixed_budget(a, tmp_path)
    assert (await a.send(PRIVATE, "native", reply_to="inbound-1")).success
    with pytest.raises(Deferred):
        await a._publish_once("background", PRIVATE, "background")
    now[0] += 10
    assert (await a._publish_once("background", PRIVATE, "background"))["state"] == "sent"
    denied = await a.send(PRIVATE, "new native", reply_to="incoming-one")
    assert not denied.success and not denied.retryable
    assert a._budget.status()["reservations_total"] == 2


@pytest.mark.asyncio
async def test_target_rejection_is_local_scope_and_no_alternate_replay(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    first = a._connections["10001"]
    original = first.rpc

    async def reject_send(action, params):
        if action.startswith("send_"):
            first.calls.append((action, params))
            raise ActionError(action, 1404)
        return await original(action, params)

    first.rpc = reject_send
    rejected = await a._publish_once("reject", GROUP, "one")
    assert rejected["state"] == "rejected"
    assert a._health.is_eligible("10001")
    assert not a._health.is_eligible("10001", GROUP)
    assert not a._connections["10002"].calls
    first.rpc = original
    assert (await a._publish_once("other", PRIVATE, "two"))["account"] == "10001"
    assert (await a._publish_once("new-group", GROUP, "three"))["account"] == "10002"
    assert a._admission.status()["current_primary"] == "10001"
    again = await a._publish_once("reject", GROUP, "one")
    assert again["state"] == "rejected"


@pytest.mark.asyncio
async def test_unknown_stays_unknown_after_reset_and_pause(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    a._connections["10001"].ambiguous = True
    assert (await a._publish_once("ambiguous", PRIVATE, "one"))["state"] == "unknown"
    assert not a._health.is_eligible("10001", PRIVATE)
    a._health.reset("10001", PRIVATE, reason="operator review")
    for account in a.settings.accounts:
        a._admission.change(account.id, "pause", "hold")
    assert (await a._publish_once("ambiguous", PRIVATE, "one"))["state"] == "unknown"
    assert not a._connections["10002"].calls
    assert a._budget.status()["reservations_total"] == 1


@pytest.mark.asyncio
async def test_latched_account_health_blocks_ingress_and_survives_restart(adapter_module, tmp_path):
    from test_account_gates import event

    a = make(adapter_module, tmp_path, anchors=False)
    a._receive_events = True
    a._health.observe("10001", connected=True, identity="mismatch", online=True, good=True)
    await a._on_event(a.settings.accounts[0], event())
    assert not a._store.status()["inbound"]
    a = make(adapter_module, tmp_path, anchors=False)
    assert not a._health.is_eligible("10001")
    assert (await a._publish_once("standby", PRIVATE, "one"))["account"] == "10002"
    a._health.reset("10001", reason="verified independently")
    assert a._health.is_eligible("10001")
    assert a._admission.status()["current_primary"] == "10002"


@pytest.mark.asyncio
async def test_age_crossing_preflight_is_terminal_no_send(adapter_module, tmp_path, monkeypatch):
    a = make(adapter_module, tmp_path)

    class Clock:
        value = time.time()

        def time(self):
            return self.value

    clock = Clock()
    monkeypatch.setattr(adapter_module, "time", clock)
    original = a._connections["10001"].rpc

    async def cross(action, params):
        if action == "get_group_member_info":
            clock.value += 2
        return await original(action, params)

    a._connections["10001"].rpc = cross
    token = PUBLICATION_CREATED.set(clock.value - 899)
    try:
        result = await a._publish_once("aging", GROUP, "one")
        assert result["state"] == "rejected"
        assert all(action != "send_group_msg" for action, _ in a._connections["10001"].calls)
        assert list(a._store.status()["outbound"].items()) == [("failed", 1)]
        assert (await a._publish_once("aging", GROUP, "one"))["state"] == "rejected"
    finally:
        PUBLICATION_CREATED.reset(token)


@pytest.mark.asyncio
async def test_expired_before_selection_never_touches_peer(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    token = PUBLICATION_CREATED.set(time.time() - 901)
    try:
        assert (await a._publish_once("expired", GROUP, "one"))["state"] == "rejected"
        assert not any(c.calls for c in a._connections.values())
        assert not a._store.status()["outbound"]
    finally:
        PUBLICATION_CREATED.reset(token)


@pytest.mark.asyncio
async def test_health_failure_cannot_overwrite_known_ack(adapter_module, tmp_path, monkeypatch):
    a = make(adapter_module, tmp_path)

    def fail(*args, **kwargs):
        raise OSError("fake local diagnostic failure")

    monkeypatch.setattr(a._health, "report", fail)
    assert (await a._publish_once("known", PRIVATE, "one"))["state"] == "sent"
    assert (await a._publish_once("known", PRIVATE, "one"))["state"] == "sent"
    with pytest.raises(NoRoute):
        await a._publish_once("new", PRIVATE, "two")


@pytest.mark.asyncio
async def test_circuit_change_inside_durable_claim_vetoes_rpc(adapter_module, tmp_path, monkeypatch):
    a = make(adapter_module, tmp_path)
    original = a._store.begin_send

    def claim(*args):
        ok = original(*args)
        a._health.report("10001", "account_anomaly")
        return ok

    monkeypatch.setattr(a._store, "begin_send", claim)
    assert (await a._publish_once("held", PRIVATE, "one"))["state"] == "rejected"
    assert not a._connections["10001"].calls
    assert a._budget.status()["reservations_total"] == 1


def test_pool_restoration_resolves_only_pool_alert(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    a._health.report("10001", "pool_unavailable")
    a._health.report("10001", "account_anomaly")
    a._health.report("10001", "pool_available")
    assert not a._health.is_eligible("10001")
    pool = [x for x in a._health.alerts() if x["code"] == "pool_unavailable"]
    assert pool[0]["state"] == "resolved"


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", [False, True])
async def test_slow_claim_cannot_collapse_actual_dispatch_spacing(adapter_module, tmp_path, ambiguous):
    a = make(adapter_module, tmp_path)
    now = fixed_budget(a, tmp_path)
    original_claim = a._store.begin_send
    first = [True]
    wire_times = []

    def delayed_claim(*args, **kwargs):
        if first[0]:
            first[0] = False
            now[0] += 20
        return original_claim(*args, **kwargs)

    a._store.begin_send = delayed_claim
    for peer in a._connections.values():
        original_rpc = peer.rpc

        async def observed_rpc(action, params, rpc=original_rpc):
            if action.startswith("send_"):
                wire_times.append(now[0])
            return await rpc(action, params)

        peer.rpc = observed_rpc
    if ambiguous:
        a._connections["10001"].ambiguous = True
    result = await a._publish_once("slow-first", PRIVATE, "one")
    assert result["state"] == ("unknown" if ambiguous else "sent")
    assert wire_times == [1020.0]
    with pytest.raises(Deferred):
        await a._publish_once("slow-second", PRIVATE, "two")
    assert wire_times == [1020.0]
    now[0] += 10
    assert (await a._publish_once("slow-second", PRIVATE, "two"))["state"] == "sent"
    assert wire_times == [1020.0, 1030.0]


@pytest.mark.asyncio
async def test_cooldown_write_failure_preserves_ack_and_holds_future_work(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    fixed_budget(a, tmp_path)

    def unavailable(key):
        raise OSError("private disk diagnostic must not escape")

    a._budget.complete = unavailable
    first = await a._publish_once("known-ack", PRIVATE, "one")
    assert first["state"] == "sent"
    assert a._budget_failed
    assert (await a._publish_once("known-ack", PRIVATE, "one")) == first
    with pytest.raises(NoRoute):
        await a._publish_once("new-intent", PRIVATE, "two")
    assert sum(len(peer.calls) for peer in a._connections.values()) == 1
