"""Read-only native host contract probe; no profile config or network changes.

Run via the selected Hermes interpreter/bootstrap, not the development venv.
"""

import asyncio
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import hermes_constants
from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry
from gateway.platform_registry import platform_registry as registry
from gateway.platforms import _shared

from hermes_onebot_fleet.plugin import register


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
            # The actual native wrapper cannot authorize unkeyed cron sends.
            # No transport is started and denied calls never touch a ledger.
            for text in ("native proactive attempt", "outer plain-text fallback"):
                denied = await adapter._send_with_retry(source.chat_id, text, base_delay=0)
                assert not denied.success and not denied.retryable
                assert "durable publisher ID" in denied.error
            # Exercise the REAL host retry/fallback wrapper, not just send() in a mock.
            from gateway.platforms.base import SendResult

            calls = []

            async def uncertain_send(**kwargs):
                calls.append(kwargs)
                return SendResult(
                    success=False, error="delivery outcome uncertain; no automatic resend", retryable=False
                )

            adapter.send = uncertain_send
            result = await adapter._send_with_retry(source.chat_id, "probe", base_delay=0)
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
                    }
                )
            )
        finally:
            _shared.get_scoped_secret = original_secret
            hermes_constants.get_hermes_home = original_home


if __name__ == "__main__":
    asyncio.run(probe())
