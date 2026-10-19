"""Extra admission/freshness/config boundaries; fake host and temp files only."""

import asyncio
import time

import pytest
from test_account_gates import event
from test_adapter import adapter_module as adapter_module
from test_adapter import make

from hermes_onebot_fleet.config import Settings

GROUP = "obf:onebot-fleet:group:30001"


@pytest.mark.asyncio
async def test_target_sending_circuit_does_not_drop_readable_ingress(adapter_module, tmp_path):
    a = make(adapter_module, tmp_path, anchors=False)
    a._receive_events = True
    a._health.report("10001", "target_rejected", GROUP)
    await a._on_event(a.settings.accounts[0], event())
    row = a._store.claim()
    assert row is not None
    item = a._admitted_payload(row["payload"])
    assert item is not None
    a._store.inbound_result(item.dedup_key, "handed_off")
    reply = await a.send(GROUP, "new reply", reply_to=item.dedup_key)
    assert reply.success
    assert not a._connections["10001"].calls
    assert a._connections["10002"].calls[-1][0] == "send_group_msg"
    assert a._admission.status()["current_primary"] == "10001"


@pytest.mark.asyncio
async def test_expired_queued_handoff_is_denied_without_host_call(adapter_module, tmp_path, monkeypatch):
    a = make(adapter_module, tmp_path, anchors=False)
    a._receive_events = True
    await a._on_event(a.settings.accounts[0], event())
    original = a._store.claim

    def expired_claim():
        row = original()
        if row is not None:
            row = dict(row)
            row["created"] = time.time() - 901
        return row

    monkeypatch.setattr(a._store, "claim", expired_claim)
    done = asyncio.Event()
    original_result = a._store.inbound_result

    def finish(*args):
        original_result(*args)
        done.set()

    monkeypatch.setattr(a._store, "inbound_result", finish)
    calls = []
    a._message_handler = object()

    async def host(message):
        calls.append(message)

    a.handle_message = host
    task = asyncio.create_task(a._drain())
    try:
        await asyncio.wait_for(done.wait(), 1)
        assert a._store.status()["inbound"] == {"denied": 1}
        assert calls == []
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize(
    "bad",
    [
        None,
        False,
        0,
        [],
        {"enabled": False},
        {"global_per_hour": 0},
        {"min_interval_seconds": float("nan")},
        {"max_pending_age_seconds": True},
    ],
)
def test_budget_cannot_be_disabled_or_malformed(bad):
    with pytest.raises((ValueError, TypeError)):
        Settings.from_mapping(
            {"accounts": [{"id": "10001", "ws_url": "ws://127.0.0.1:1", "token_env": "TOKEN"}], "budget": bad}
        )
