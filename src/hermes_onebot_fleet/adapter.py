"""Native Hermes edge around the single host-independent Fleet safety core."""

from __future__ import annotations

import asyncio as asyncio
import time
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

from .config import Settings
from .protocol import Incoming
from .publisher import PUBLICATION_CREATED as PUBLICATION_CREATED
from .publisher import Deferred as Deferred
from .publisher import NoRoute as NoRoute
from .transport import OneBotConnection

# Hermes loads staged plugins under profile-specific namespaces. Import the
# sibling core in that namespace, never a globally cached adapter/host class.
_prefix, _separator, _ = __package__.rpartition(".")
_core = import_module(f"{_prefix}.onebot_fleet.core" if _separator else "onebot_fleet.core")
FleetCore = _core.FleetCore
FleetSendResult = _core.FleetSendResult
target = _core.target
parse_target = _core.parse_target
INGRESS = _core.INGRESS
PUBLICATION = _core.PUBLICATION


class OneBotFleetAdapter(FleetCore, BasePlatformAdapter):
    def __init__(self, config, *, receive_events=True):
        BasePlatformAdapter.__init__(self, config, Platform("onebot_fleet"))
        from gateway.platforms._shared import get_scoped_secret
        from hermes_constants import get_hermes_home

        settings = Settings.from_mapping(config.extra)
        # Resolve only while the owning Hermes profile is bound. The shared core
        # gets explicit scoped values, not access to host globals or env fallback.
        names = {a.token_env for a in settings.accounts}
        names.update(client.token_env for client in settings.publisher.clients)
        secrets = {name: get_scoped_secret(name) for name in names}
        path = settings.state_path
        if not path.is_absolute():
            # Preserve the pre-extraction state location exactly.
            path = Path(get_hermes_home()) / "plugin-data" / "onebot-fleet" / path
        FleetCore.__init__(
            self,
            settings,
            state_path=path,
            secrets=secrets,
            message_handler=getattr(self, "_message_handler", None),
            receive_events=receive_events,
        )

    @staticmethod
    def _make_connection(*args, **kwargs):
        # Keep the native module's constructor seam for host probes/test doubles.
        return OneBotConnection(*args, **kwargs)

    @staticmethod
    def _fresh(created, max_age):
        # The native module clock is intentionally patchable independently.
        return created is None or (type(created) in (int, float) and 0 <= time.time() - created <= max_age)

    def _dispatch_ready(self):
        return bool(getattr(self, "_message_handler", None))

    async def _dispatch_incoming(self, item: Incoming):
        source = self.build_source(
            chat_id=target(self.settings.logical_id, item.chat_type, item.chat_id),
            chat_type="dm" if item.chat_type == "private" else "group",
            user_id=item.user_id,
            user_name=item.user_id,
            scope_id=self.settings.logical_id,
            message_id=item.dedup_key,
        )
        event = MessageEvent(
            text=item.text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=item.dedup_key,
            metadata={},
            timestamp=datetime.fromtimestamp(item.timestamp, tz=UTC),
        )
        await self.handle_message(event)

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        result = await FleetCore.send(self, chat_id, content, reply_to=reply_to, metadata=metadata)
        # Host fixtures and staged profiles may each have a different real result
        # class. The core never imports or caches one of those runtime types.
        return SendResult(
            success=result.success,
            message_id=result.message_id,
            error=result.error,
            retryable=result.retryable,
        )

    def _send_retry_is_final(self, result):
        # retryable=False alone does not suppress Hermes' plain-text fallback.
        # Reformatting after a lost ACK can duplicate delivery. Every failure is
        # terminal for this intent; no alternate bot/fallback send is permitted.
        return not result.success
