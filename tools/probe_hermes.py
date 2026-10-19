"""Read-only native host contract probe; no profile config or network changes.

Run via the selected Hermes interpreter/bootstrap, not the development venv.
"""

import asyncio
import inspect
import json
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import hermes_constants
from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry
from gateway.platform_registry import platform_registry as registry
from gateway.platforms import _shared

from hermes_onebot_fleet.account_state import AccountState
from hermes_onebot_fleet.delivery_budget import DeliveryBudget
from hermes_onebot_fleet.health import HealthState
from hermes_onebot_fleet.plugin import register
from hermes_onebot_fleet.protocol import normalize_event
from hermes_onebot_fleet.store import Store


class Context:
    def register_platform(self, **kwargs):
        registry.register(PlatformEntry(**kwargs))
        self.entry = registry.get(kwargs["name"])


async def probe():
    context = Context()
    register(context)
    original_secret = _shared.get_scoped_secret
    original_home = hermes_constants.get_hermes_home
    with tempfile.TemporaryDirectory(prefix="onebot-fleet-native-probe-") as directory:
        _shared.get_scoped_secret = lambda name: "FAKE_PROBE_TOKEN_NOT_A_REAL_SECRET"
        hermes_constants.get_hermes_home = lambda: Path(directory)
        try:
            config = PlatformConfig(
                enabled=True,
                extra={
                    "accounts": [
                        {"id": "10001", "ws_url": "ws://127.0.0.1:1", "token_env": "ONEBOT_FLEET_PROBE_TOKEN"}
                    ],
                    "allowed_users": ["20001"],
                    "allowed_groups": ["30001"],
                    "group_sessions_per_user": True,
                    "thread_sessions_per_user": True,
                },
            )
            assert context.entry.validate_config(config)
            adapter = context.entry.adapter_factory(config)
            from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType

            assert isinstance(adapter, BasePlatformAdapter)
            source = adapter.build_source(
                chat_id="obf:onebot-fleet:group:30001",
                chat_type="group",
                user_id="20001",
                message_id="probe",
                scope_id="onebot-fleet",
            )
            event = MessageEvent(
                text="probe", source=source, message_type=MessageType.TEXT, message_id="probe", metadata={}
            )
            assert source.user_id == "20001"
            assert (await adapter.get_chat_info(source.chat_id))["type"] == "group"
            # Exercise the native retry body, excluding only its metrics decorator
            # so this probe cannot emit delivery telemetry into a real profile.
            native_send = inspect.unwrap(BasePlatformAdapter._send_with_retry)
            # The actual native wrapper cannot authorize unkeyed cron sends.
            # No transport is started and denied calls never touch a ledger.
            for text in ("native proactive attempt", "outer plain-text fallback"):
                denied = await native_send(adapter, source.chat_id, text, base_delay=0)
                assert not denied.success and not denied.retryable
                assert "durable publisher ID" in denied.error
            # P0 positive and revoked-revision reply paths use temporary plugin
            # ledgers and a fake transport, never a profile or actual socket.
            adapter._store = Store(Path(directory) / "probe.sqlite3")
            adapter._admission = AccountState(
                Path(directory) / "probe.accounts.sqlite3", adapter.settings.logical_id, ["10001"]
            )
            adapter._health = HealthState(Path(directory) / "health.sqlite3", adapter.settings.logical_id)
            adapter._budget = DeliveryBudget(
                Path(directory) / "budget.sqlite3", adapter.settings.logical_id, adapter.settings.budget
            )
            item = normalize_event(
                {
                    "post_type": "message",
                    "self_id": 10001,
                    "user_id": 20001,
                    "message_type": "group",
                    "group_id": 30001,
                    "message_id": 1,
                    "time": 100,
                    "message": [
                        {"type": "at", "data": {"qq": "10001"}},
                        {"type": "text", "data": {"text": "probe"}},
                    ],
                },
                adapter.settings.accounts[0],
                adapter.settings,
            )
            assert item is not None
            payload = asdict(item)
            payload["_admission_revision"] = adapter._admission.snapshot("10001")["revision"]
            adapter._store.enqueue(item.dedup_key, payload)
            adapter._store.inbound_result(item.dedup_key, "handed_off")
            actions = []

            class FakeTransport:
                healthy = True

                async def rpc(self, action, params):
                    actions.append(action)
                    if action == "get_group_member_info":
                        return {"user_id": 10001}
                    assert action == "send_group_msg"
                    return {"message_id": 42}

            adapter._connections = {"10001": FakeTransport()}
            admitted = await native_send(
                adapter, source.chat_id, "admitted probe", reply_to=item.dedup_key, base_delay=0
            )
            assert admitted.success and admitted.message_id == "42"
            adapter._admission.change("10001", "pause", "probe only")
            adapter._admission.change("10001", "allow", "probe re-admission")
            revoked = await native_send(
                adapter, source.chat_id, "late probe", reply_to=item.dedup_key, base_delay=0
            )
            assert not revoked.success and "admission" in revoked.error
            assert actions == ["get_group_member_info", "send_group_msg"]
            # Exercise the REAL host retry/fallback body, not just send() in a mock.
            from gateway.platforms.base import SendResult

            calls = []

            async def uncertain_send(**kwargs):
                calls.append(kwargs)
                return SendResult(
                    success=False, error="delivery outcome uncertain; no automatic resend", retryable=False
                )

            adapter.send = uncertain_send
            result = await native_send(adapter, source.chat_id, "probe", base_delay=0)
            assert not result.success and len(calls) == 1, "host replayed an uncertain send"
            # No connections started: validates ABC, native event shapes and terminal retry contract.
            print(
                json.dumps(
                    {
                        "native_adapter": type(adapter).__name__,
                        "platform": source.platform.value,
                        "chat_id": source.chat_id,
                        "metadata": event.metadata,
                        "network_started": False,
                        "profile_modified": False,
                        "uncertain_send_attempts": len(calls),
                        "native_unkeyed_attempts_denied": 2,
                        "native_anchored_send_attempts": actions.count("send_group_msg"),
                        "native_re_admitted_stale_reply_denied": not revoked.success,
                    }
                )
            )
        finally:
            _shared.get_scoped_secret = original_secret
            hermes_constants.get_hermes_home = original_home


if __name__ == "__main__":
    asyncio.run(probe())
