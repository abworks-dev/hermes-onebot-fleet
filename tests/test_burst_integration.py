"""Minute caps veto all adapter entries before read-only preflight; fake peers only."""

import pytest
from test_adapter import adapter_module as adapter_module
from test_stoploss_integration import GROUP, Clock, adapter, calls, intent

from hermes_onebot_fleet.delivery_budget import DeliveryBudget
from hermes_onebot_fleet.publisher import Deferred


@pytest.mark.asyncio
async def test_target_minute_cap_shares_native_clients_accounts_and_restart(adapter_module, tmp_path):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    assert (await fleet.send(GROUP, "native", reply_to="group-inbound-1")).success
    for index in (1, 2):
        clock.value = 1000 + index * 20
        assert (await fleet._publish_once(f"client-{index}:sent", GROUP, "payload"))["state"] == "sent"
    assert fleet._budget.status()["reservations_total"] == 3
    clock.value = 1050
    before = calls(fleet)
    denied = await fleet.send(GROUP, "new native", reply_to="group-inbound-1")
    assert not denied.success and not denied.retryable
    with pytest.raises(Deferred) as deferred:
        await fleet._publish_once("client-b:pending", GROUP, "payload")
    assert deferred.value.next_at == 1060
    assert calls(fleet) == before
    assert fleet._store.outbound(intent("client-b:pending", GROUP)) is None
    assert fleet._budget.status()["reservations_total"] == 3

    # Restart/reopen and a different preferred account do not mint extra slots.
    fleet._budget = DeliveryBudget(
        tmp_path / "integration_budget.db", "onebot-fleet", fleet.settings.budget, clock=clock
    )
    fleet._budget.recover()
    fleet._admission.promote("10002", "reviewed preference")
    with pytest.raises(Deferred):
        await fleet._publish_once("client-b:pending", GROUP, "payload")
    assert calls(fleet) == before
    clock.value = 1060
    result = await fleet._publish_once("client-b:pending", GROUP, "payload")
    assert result["state"] == "sent" and result["account"] == "10002"
    assert fleet._budget.status()["reservations_total"] == 4
    sent = calls(fleet)
    assert (await fleet._publish_once("client-b:pending", GROUP, "payload"))["state"] == "sent"
    assert calls(fleet) == sent


@pytest.mark.asyncio
async def test_global_minute_cap_vetoes_new_group_without_membership_query(adapter_module, tmp_path):
    clock = Clock()
    fleet = adapter(adapter_module, tmp_path, clock)
    for index in range(6):
        clock.value = 1000 + index * 10
        address = f"obf:onebot-fleet:private:{20001 + index}"
        assert (await fleet._publish_once(f"client-{index}:sent", address, "payload"))["state"] == "sent"
    clock.value = 1055
    before = calls(fleet)
    with pytest.raises(Deferred) as deferred:
        await fleet._publish_once("group:pending", GROUP, "payload")
    assert deferred.value.next_at == 1060
    assert calls(fleet) == before
    assert fleet._reads.status()["reservations_total"] == 0
    assert fleet._budget.status()["reservations_total"] == 6
    clock.value = 1060
    assert (await fleet._publish_once("group:pending", GROUP, "payload"))["state"] == "sent"
    assert fleet._reads.status()["reservations_total"] == 1
