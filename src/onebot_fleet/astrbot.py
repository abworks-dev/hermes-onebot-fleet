"""Conservative AstrBot platform bridge; importing this module opens no runtime state.

Only native plain-text replies are supported. A successful AstrBot send means an
acknowledged FleetCore receipt, never merely a queued or attempted operation.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from astrbot.api.event import MessageChain
from astrbot.api.message_components import Plain
from astrbot.api.platform import (
    AstrBotMessage,
    AstrMessageEvent,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
)
from astrbot.core.platform.astr_message_event import current_message_event

from .config import Settings
from .core import FleetCore, target


class FleetDeliveryError(RuntimeError):
    """A terminal native reply failure; messages never contain upstream secrets."""

    def __init__(self, code: str = "delivery_not_confirmed") -> None:
        self.code = code
        super().__init__(f"onebot_fleet: {code}")


@dataclass(frozen=True)
class FleetReceipt:
    content_hash: str
    success: bool
    message_id: str | None = None
    error: str | None = None


def _plain_text(message: MessageChain) -> str:
    chain = getattr(message, "chain", None)
    if not isinstance(chain, list) or not chain:
        raise FleetDeliveryError("plain_text_required")
    # Do not use get_plain_text(): it silently discards unsupported media.
    if any(type(component) is not Plain or not isinstance(component.text, str) for component in chain):
        raise FleetDeliveryError("unsupported_component")
    text = "".join(component.text for component in chain)
    if not text or len(text) > 4000:
        raise FleetDeliveryError("invalid_text_length")
    return text


class FleetMessageEvent(AstrMessageEvent):
    def __init__(
        self, platform: OneBotFleetPlatform, message: AstrBotMessage, *, anchor: str | None = None
    ) -> None:
        super().__init__(message.message_str, message, platform.meta(), message.session_id)
        self._fleet_platform = platform
        self._fleet_anchor = anchor
        self._fleet_umo = self.unified_msg_origin
        self._fleet_target = platform._message_target(message)
        self._fleet_lock = asyncio.Lock()
        self._fleet_terminal: FleetDeliveryError | None = None
        self._fleet_receipts: list[FleetReceipt] = []
        self._fleet_confirmed: dict[str, FleetReceipt] = {}

    @property
    def fleet_receipts(self) -> tuple[FleetReceipt, ...]:
        return tuple(self._fleet_receipts)

    def _fail(self, code: str) -> FleetDeliveryError:
        if self._fleet_terminal is None:
            self._fleet_terminal = FleetDeliveryError(code)
        self.stop_event()
        self.clear_result()
        return self._fleet_terminal

    async def send(self, message: MessageChain) -> None:
        async with self._fleet_lock:
            if self._fleet_terminal is not None:
                raise self._fleet_terminal
            try:
                self._fleet_platform._require_active()
                if not self._fleet_anchor or self.unified_msg_origin != self._fleet_umo:
                    raise FleetDeliveryError("reply_anchor_required")
                text = _plain_text(message)
                digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
                if digest in self._fleet_confirmed:
                    return  # Concurrent/repeated identical replies use the same durable key.
                core = self._fleet_platform._core
                if core is None:
                    raise FleetDeliveryError("platform_not_running")
                try:
                    result = await core.send(self._fleet_target, text, reply_to=self._fleet_anchor)
                except asyncio.CancelledError:
                    self._fleet_receipts.append(FleetReceipt(digest, False, error="delivery_interrupted"))
                    self._fail("delivery_interrupted")
                    raise
                except Exception:
                    self._fleet_receipts.append(FleetReceipt(digest, False, error="delivery_not_confirmed"))
                    raise FleetDeliveryError("delivery_not_confirmed") from None
                if not result.success or not result.message_id:
                    self._fleet_receipts.append(FleetReceipt(digest, False, error="delivery_not_confirmed"))
                    raise FleetDeliveryError("delivery_not_confirmed")
                receipt = FleetReceipt(digest, True, str(result.message_id))
                self._fleet_receipts.append(receipt)
                self._fleet_confirmed[digest] = receipt
                # The host's base implementation marks metrics/_has_send_oper.
                # It must NEVER be invoked before a confirmed transport receipt.
                await super().send(message)
            except FleetDeliveryError as exc:
                raise self._fail(exc.code) from None
            except Exception:
                # Host bookkeeping must not leak raw exceptions or permit a
                # later segment to bypass an unexpected failure.
                raise self._fail("host_send_bookkeeping_failed") from None

    async def send_streaming(self, generator: Any, use_fallback: bool = False) -> None:
        raise self._fail("streaming_not_supported")

    def get_client(self) -> object:
        raise FleetDeliveryError("raw_client_not_supported")


class OneBotFleetPlatform(Platform):
    """Manager-compatible platform; the plugin owns the revocable capability."""

    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
        *,
        capability: Callable[[], bool] | None = None,
    ) -> None:
        self._capability = capability
        self._require_active()
        if platform_config.get("enable") is not True:
            raise FleetDeliveryError("platform_not_enabled")
        platform_id = platform_config.get("id")
        if not isinstance(platform_id, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", platform_id) is None:
            raise FleetDeliveryError("invalid_platform_id")
        super().__init__(platform_config, event_queue)
        self._platform_id = platform_id
        self._platform_settings = platform_settings
        try:
            self.settings = Settings.from_mapping(platform_config.get("fleet", {}))
        except Exception:
            raise FleetDeliveryError("invalid_fleet_config") from None
        self._core: FleetCore | None = None
        self._stop = asyncio.Event()
        self._running = False

    def _require_active(self) -> None:
        if self._capability is None or not self._capability():
            raise FleetDeliveryError("plugin_not_active")
        config = getattr(self, "config", None)
        if config is not None and config.get("enable") is not True:
            raise FleetDeliveryError("platform_not_enabled")

    def meta(self) -> PlatformMetadata:
        return PlatformMetadata(
            name="onebot_fleet",
            description="OneBot Fleet conservative plain-text replies",
            id=self._platform_id,
            support_streaming_message=False,
            support_proactive_message=False,
        )

    async def run(self) -> None:
        self._require_active()
        if self._running or self._stop.is_set():
            raise FleetDeliveryError("platform_already_started_or_stopped")
        self._running = True
        try:
            # Resolve only named environment variables. No tokens enter config,
            # messages, raw_message, exceptions or the plugin's metadata.
            names = {account.token_env for account in self.settings.accounts}
            names.update(client.token_env for client in self.settings.publisher.clients)
            secrets = {name: os.environ.get(name, "") for name in names}
            self._core = FleetCore(
                self.settings,
                state_path=Path(self.settings.state_path),
                secrets=secrets,
                message_handler=self._on_incoming,
                receive_events=True,
            )
            self._require_active()
            await self._core.connect()
            await self._stop.wait()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise FleetDeliveryError("platform_runtime_failed") from None
        finally:
            try:
                await self._disconnect()
            finally:
                self._running = False

    async def _disconnect(self) -> None:
        core, self._core = self._core, None
        if core is not None:
            try:
                await core.disconnect()
            except Exception:
                raise FleetDeliveryError("platform_shutdown_failed") from None

    async def terminate(self) -> None:
        self._stop.set()
        await self._disconnect()

    def _message_target(self, message: AstrBotMessage) -> str:
        if message.type == MessageType.GROUP_MESSAGE:
            kind = "group"
        elif message.type == MessageType.FRIEND_MESSAGE:
            kind = "private"
        else:
            raise FleetDeliveryError("unsupported_message_type")
        return target(self.settings.logical_id, kind, message.session_id)

    def create_event(self, message: AstrBotMessage) -> FleetMessageEvent:
        # Public host convenience API MUST NOT manufacture a reply permission.
        return FleetMessageEvent(self, message)

    async def _on_incoming(self, item: Any) -> None:
        self._require_active()
        if item.logical_id != self.settings.logical_id or item.chat_type not in {"private", "group"}:
            raise FleetDeliveryError("invalid_incoming")
        message = AstrBotMessage()
        message.type = MessageType.GROUP_MESSAGE if item.chat_type == "group" else MessageType.FRIEND_MESSAGE
        message.self_id = self.settings.logical_id
        message.session_id = item.chat_id
        message.message_id = item.dedup_key
        message.sender = MessageMember(user_id=item.user_id, nickname=None)
        message.message = [Plain(item.text)]
        message.message_str = item.text
        message.timestamp = item.timestamp
        message.raw_message = {"platform": "onebot_fleet", "message_id": item.dedup_key}
        if item.chat_type == "group":
            message.group_id = item.chat_id
        event = FleetMessageEvent(self, message, anchor=item.dedup_key)
        event.is_at_or_wake_command = True  # Core has already admitted mention/private ingress.
        # Successful queue admission is the handoff, not completion of host work.
        # A bounded full queue raises; do not stall the core's single drain forever.
        try:
            self.commit_event(event)
        except Exception:
            raise FleetDeliveryError("event_queue_unavailable") from None

    async def send_by_session(self, session: Any, message_chain: MessageChain) -> None:
        self._require_active()
        event = current_message_event.get()
        if (
            not isinstance(event, FleetMessageEvent)
            or event._fleet_platform is not self
            or not event._execution_active
            or str(session) != event._fleet_umo
            or event.unified_msg_origin != event._fleet_umo
        ):
            # Context.send_message ignores False; failure MUST raise.
            raise FleetDeliveryError("active_same_session_reply_required")
        await event.send(message_chain)

    def get_client(self) -> object:
        raise FleetDeliveryError("raw_client_not_supported")
