"""Focused review regressions: fake host/RPCs and temporary local ledgers only."""

import json

import pytest
from test_adapter import adapter_module as adapter_module
from test_adapter import make

from hermes_onebot_fleet.account_state import AccountState

PRIVATE = "obf:onebot-fleet:private:20001"
GROUP = "obf:onebot-fleet:group:30001"


def private_event():
    return {
        "post_type": "message",
        "self_id": 10001,
        "user_id": 20001,
        "message_type": "private",
        "message_id": 77,
        "time": 100,
        "message": [{"type": "text", "data": {"text": "question"}}],
    }


@pytest.mark.asyncio
async def test_ingress_does_not_stamp_old_work_with_post_revocation_revision(
    adapter_module, tmp_path, monkeypatch
):
    a = make(adapter_module, tmp_path, anchors=False)
    a._receive_events = True
    operator = AccountState.open_existing(a._admission.path)
    original = a._admission.is_allowed
    transitioned = False

    def revoke_and_allow():
        nonlocal transitioned
        operator.change("10001", "pause", "review stop")
        operator.change("10001", "allow", "manual recovery")
        transitioned = True

    def interleaved_allowed(account_id, revision=None):
        allowed = original(account_id, revision)
        # The former implementation performed this read before separately
        # reading revision. A different process can commit in that interval.
        if not transitioned and revision is None:
            revoke_and_allow()
        return allowed

    monkeypatch.setattr(a._admission, "is_allowed", interleaved_allowed)
    await a._on_event(a.settings.accounts[0], private_event())
    # A single atomic snapshot has no inter-read interval; revoke its captured
    # permit immediately after intake instead.
    if not transitioned:
        revoke_and_allow()
    row = a._store.claim()
    assert row is not None
    assert json.loads(row["payload"])["_admission_revision"] == 0
    assert a._admitted_payload(row["payload"]) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("revoked", ["selected", "source"])
@pytest.mark.parametrize("reallow", [False, True])
async def test_revocation_during_send_reservation_never_enters_rpc(
    adapter_module, tmp_path, monkeypatch, revoked, reallow
):
    a = make(adapter_module, tmp_path)
    operator = AccountState.open_existing(a._admission.path)
    # Keep the source anchor and selected sender distinct to exercise both fences.
    operator.promote("10002", "review test selection")
    account_id = "10002" if revoked == "selected" else "10001"
    original = a._store.begin_send
    transitioned = False

    def reserve_then_revoke(*args):
        nonlocal transitioned
        reserved = original(*args)
        if not transitioned:
            operator.change(account_id, "pause", "stop before rpc")
            if reallow:
                operator.change(account_id, "allow", "manual recovery before rpc")
            transitioned = True
        return reserved

    monkeypatch.setattr(a._store, "begin_send", reserve_then_revoke)

    async def deliver():
        if revoked == "selected":
            receipt = await a._publish_once("reservation-race", PRIVATE, "hello")
            assert receipt["state"] == "rejected"  # Never NoRoute/requeue.
        else:
            result = await a.send(PRIVATE, "hello", reply_to="inbound-1")
            assert not result.success and not result.retryable

    await deliver()
    assert all(not connection.calls for connection in a._connections.values())
    assert a._store.status()["outbound"] == {"failed": 1}
    operator.change(account_id, "allow", "manual recovery after rejection")
    await deliver()
    assert all(not connection.calls for connection in a._connections.values())
    assert a._store.status()["outbound"] == {"failed": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["admission", "health"])
async def test_global_failure_during_membership_probe_persists_standby_primary(
    adapter_module, tmp_path, monkeypatch, failure
):
    a = make(adapter_module, tmp_path)
    operator = AccountState.open_existing(a._admission.path)
    original = a._connections["10001"].rpc

    async def lose_primary_during_probe(action, params):
        result = await original(action, params)
        assert action == "get_group_member_info"
        if failure == "admission":
            operator.change("10001", "pause", "stop during membership probe")
        else:
            a._connections["10001"].healthy = False
        return result

    monkeypatch.setattr(a._connections["10001"], "rpc", lose_primary_during_probe)
    receipt = await a._publish_once("preflight-failover", GROUP, "hello")
    assert receipt["account"] == "10002"
    assert operator.status()["current_primary"] == "10002"
    if failure == "admission":
        operator.change("10001", "allow", "manual readmission without promotion")
    else:
        a._connections["10001"].healthy = True
    monkeypatch.setattr(a._connections["10001"], "rpc", original)
    receipt = await a._publish_once("after-recovery", PRIVATE, "hello")
    assert receipt["account"] == "10002"


@pytest.mark.asyncio
async def test_target_only_membership_failure_does_not_change_global_primary(
    adapter_module, tmp_path, monkeypatch
):
    a = make(adapter_module, tmp_path)
    original = a._connections["10001"].rpc

    async def missing_membership(action, params):
        if action == "get_group_member_info":
            return {"user_id": "99999"}
        return await original(action, params)

    monkeypatch.setattr(a._connections["10001"], "rpc", missing_membership)
    receipt = await a._publish_once("group-only-fallback", GROUP, "hello")
    assert receipt["account"] == "10002"
    assert a._admission.status()["current_primary"] == "10001"
    receipt = await a._publish_once("unrelated-private-target", PRIVATE, "hello")
    assert receipt["account"] == "10001"


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous,expected", [(False, "sent"), (True, "unknown")])
async def test_existing_publication_receipt_survives_all_accounts_paused(
    adapter_module, tmp_path, ambiguous, expected
):
    a = make(adapter_module, tmp_path)
    a._connections["10001"].ambiguous = ambiguous
    first = await a._publish_once("existing-receipt", PRIVATE, "hello")
    assert first["state"] == expected
    operator = AccountState.open_existing(a._admission.path)
    for account in a.settings.accounts:
        operator.change(account.id, "pause", "stop business")
    before = {key: list(connection.calls) for key, connection in a._connections.items()}
    # A past dispatched send is never a new, provably unattempted NoRoute.
    second = await a._publish_once("existing-receipt", PRIVATE, "hello")
    assert second == first
    assert {key: connection.calls for key, connection in a._connections.items()} == before


@pytest.mark.asyncio
async def test_first_attempt_with_all_accounts_paused_remains_known_no_route(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path)
    for account in a.settings.accounts:
        a._admission.change(account.id, "pause", "stop before any dispatch")
    with pytest.raises(adapter_module.NoRoute):
        await a._publish_once("never-attempted", PRIVATE, "hello")
    assert a._store.status()["outbound"] == {}
    assert all(not connection.calls for connection in a._connections.values())
