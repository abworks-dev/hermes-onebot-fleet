"""P0 regressions use local ledgers and fake host/sockets, never QQ."""

import asyncio
import json

import pytest
from test_adapter import adapter_module as adapter_module
from test_adapter import make

from hermes_onebot_fleet.account_state import AccountState


def event():
    return {
        "post_type": "message",
        "self_id": 10001,
        "user_id": 20001,
        "message_type": "group",
        "group_id": 30001,
        "message_id": 77,
        "time": 100,
        "message": [
            {"type": "at", "data": {"qq": "10001"}},
            {"type": "text", "data": {"text": "question"}},
        ],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["pause", "quarantine", "observe"])
async def test_online_blocked_account_neither_ingests_nor_sends(adapter_module, tmp_path, action):
    a = make(adapter_module, tmp_path, anchors=False)
    a._receive_events = True
    operator = AccountState.open_existing(a._admission.path)
    operator.change("10001", action, "manual review")
    assert a._connections["10001"].healthy  # Health cannot override admission.
    await a._on_event(a.settings.accounts[0], event())
    assert not a._store.status()["inbound"]
    receipt = await a._publish_once("safe-new-item", "obf:onebot-fleet:group:30001", "hello")
    assert receipt["account"] == "10002"
    assert not a._connections["10001"].calls


@pytest.mark.asyncio
async def test_recovery_never_auto_failbacks_and_manual_promote_is_explicit(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)

    async def publish(key):
        return await a._publish_once(key, "obf:onebot-fleet:private:20001", "hello")

    assert (await publish("first"))["account"] == "10001"
    a._connections["10001"].healthy = False
    assert (await publish("second"))["account"] == "10002"
    a._connections["10001"].healthy = True
    assert (await publish("third"))["account"] == "10002"
    # New adapter and both healthy sockets do not reset persisted preference.
    a = make(adapter_module, tmp_path)
    assert (await publish("fourth"))["account"] == "10002"
    a._admission.promote("10001", "reviewed manual promotion")
    assert (await publish("fifth"))["account"] == "10001"


@pytest.mark.asyncio
async def test_pause_allow_does_not_resurrect_queued_or_model_work(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path, anchors=False)
    a._receive_events = True
    await a._on_event(a.settings.accounts[0], event())
    row = a._store.claim()
    a._store.inbound_result(row["key"], "handed_off")
    a._admission.change("10001", "pause", "review")
    a._admission.change("10001", "allow", "review completed")
    assert a._admitted_payload(row["payload"]) is None
    result = await a.send("obf:onebot-fleet:group:30001", "late model reply", reply_to=row["key"])
    assert not result.success and "admission" in result.error
    assert all(not c.calls for c in a._connections.values())
    assert not a._store.status()["outbound"]


@pytest.mark.asyncio
async def test_queued_pause_is_denied_before_host_handoff(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path, anchors=False)
    a._receive_events = True
    await a._on_event(a.settings.accounts[0], event())
    a._admission.change("10001", "quarantine", "identity verification required")
    a._message_handler = True

    async def forbidden(_):
        pytest.fail("quarantined queued work reached Hermes")

    a.handle_message = forbidden
    task = asyncio.create_task(a._drain())

    async def denied():
        while a._store.status()["inbound"] != {"denied": 1}:
            await asyncio.sleep(0)

    try:
        await asyncio.wait_for(denied(), 2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_pause_during_preflight_is_fenced_even_after_allow(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    original = a._connections["10001"].rpc

    async def revoking(action, params):
        result = await original(action, params)
        a._admission.change("10001", "pause", "review")
        a._admission.change("10001", "allow", "manual re-admission")
        return result

    a._connections["10001"].rpc = revoking
    receipt = await a._publish_once("preflight-fence", "obf:onebot-fleet:group:30001", "hello")
    assert receipt["account"] == "10002"
    assert [c[0] for c in a._connections["10001"].calls] == ["get_group_member_info"]


@pytest.mark.asyncio
async def test_pause_after_selection_is_known_no_send(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    choose = a._choose

    async def revoking(kind, identifier):
        selected = await choose(kind, identifier)
        for account in a.settings.accounts:
            a._admission.change(account.id, "pause", "stop business")
        return selected

    a._choose = revoking
    with pytest.raises(adapter_module.NoRoute):
        await a._publish_once("no-dispatch", "obf:onebot-fleet:private:20001", "hello")
    assert not a._store.status()["outbound"]
    assert all(not c.calls for c in a._connections.values())


@pytest.mark.asyncio
async def test_unknown_legacy_and_cross_target_reply_anchors_fail_closed(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    row = a._store.inbound("inbound-1")
    legacy = json.loads(row["payload"])
    legacy.pop("_admission_revision")
    a._store.enqueue("legacy", legacy)
    a._store.inbound_result("legacy", "handed_off")
    for anchor in ("unknown", "legacy", "group-inbound-1"):
        result = await a.send("obf:onebot-fleet:private:20001", "hello", reply_to=anchor)
        assert not result.success
    assert all(not c.calls for c in a._connections.values())


@pytest.mark.asyncio
async def test_inflight_send_cannot_be_recalled_and_is_not_replayed(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    original = a._connections["10001"].rpc

    async def already_dispatched(action, params):
        # A pause after side-effect dispatch cannot undo the network operation.
        result = await original(action, params)
        a._admission.change("10001", "quarantine", "stop after dispatch")
        return result

    a._connections["10001"].rpc = already_dispatched
    receipt = await a._publish_once("already-inflight", "obf:onebot-fleet:private:20001", "hello")
    assert receipt["state"] == "sent"
    receipt = await a._publish_once("already-inflight", "obf:onebot-fleet:private:20001", "hello")
    assert receipt["state"] == "sent"
    assert len(a._connections["10001"].calls) == 1
    assert not a._connections["10002"].calls
