"""Thin native Hermes edge. All task execution and approval state stays in Hermes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from contextvars import ContextVar
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

from .config import Settings, qq_id
from .lock import InstanceLock
from .protocol import Incoming, currently_admitted, normalize_event
from .store import Store
from .transport import ActionError, ActionUncertain, OneBotConnection

log = logging.getLogger(__name__)
INGRESS = ContextVar("onebot_fleet_ingress", default=None)


def target(logical_id: str, kind: str, identifier: str) -> str:
    if kind not in {"private", "group"}:
        raise ValueError("invalid target type")
    return f"obf:{logical_id}:{kind}:{qq_id(identifier)}"


def parse_target(value: str, logical_id: str | None = None):
    parts = value.split(":")
    if len(parts) != 4 or parts[0] != "obf" or parts[2] not in {"group", "private"}:
        raise ValueError("target must be obf:<logical_id>:private|group:<QQ id>")
    if logical_id is not None and parts[1] != logical_id:
        raise ValueError("target belongs to another logical adapter")
    if not parts[1] or any(
        c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in parts[1]
    ):
        raise ValueError("invalid target namespace")
    return parts[1], parts[2], qq_id(parts[3])


class OneBotFleetAdapter(BasePlatformAdapter):
    def __init__(self, config, *, receive_events=True):
        super().__init__(config, Platform("onebot_fleet"))
        from gateway.platforms._shared import get_scoped_secret
        from hermes_constants import get_hermes_home

        self.settings = Settings.from_mapping(config.extra)
        # Capture only this platform's scoped credentials while the owning profile is bound.
        secrets = {a.token_env: get_scoped_secret(a.token_env) for a in self.settings.accounts}
        for account in self.settings.accounts:
            account.resolve_token(secrets)
        path = self.settings.state_path
        if not path.is_absolute():
            path = Path(get_hermes_home()) / "plugin-data" / "onebot-fleet" / path
        self._path = path
        self._lock = InstanceLock(str(path) + ".lock")
        self._store = None
        self._receive_events = receive_events
        self._tasks = []
        self._connections = {}
        self._send_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._running = False
        for account in self.settings.accounts:

            async def callback(raw, current=account):
                await self._on_event(current, raw)

            self._connections[account.id] = OneBotConnection(
                account, lambda current=account: current.resolve_token(secrets), callback
            )

    async def connect(self, *, is_reconnect=False):
        async with self._lifecycle_lock:
            if self._running:
                return True
            self._lock.acquire()
            try:
                self._store = Store(self._path)
                self._store.recover()
                for connection in self._connections.values():
                    await connection.start()
                self._tasks.append(asyncio.create_task(self._health_loop()))
                if self._receive_events:
                    self._tasks.append(asyncio.create_task(self._drain()))
                self._running = True
                # All accounts may initially be offline; reconnect loops remain live.
                return True
            except BaseException:
                await self._stop()
                raise

    async def disconnect(self):
        async with self._lifecycle_lock:
            await self._stop()

    async def _stop(self):
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        await asyncio.gather(*(c.stop() for c in self._connections.values()), return_exceptions=True)
        self._lock.release()

    async def _on_event(self, account, raw):
        if not self._receive_events or self._store is None:
            return
        item = normalize_event(raw, account, self.settings)
        if item:
            self._store.enqueue(item.dedup_key, asdict(item))

    async def _health_loop(self):
        while True:
            await asyncio.gather(
                *(c.check_health() for c in self._connections.values()), return_exceptions=True
            )
            await asyncio.sleep(5)

    async def _drain(self):
        while True:
            if not self._message_handler:
                await asyncio.sleep(0.1)
                continue
            row = self._store.claim()
            if row is None:
                await asyncio.sleep(0.1)
                continue
            try:
                item = Incoming(**json.loads(row["payload"]))
                if not currently_admitted(item, self.settings):
                    self._store.inbound_result(row["key"], "denied", "policy changed before handoff")
                    continue
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
                token = INGRESS.set(item.dedup_key)
                try:
                    await self.handle_message(event)
                finally:
                    INGRESS.reset(token)
                # This acknowledges the HOST HANDOFF, not execution or successful reply.
                self._store.inbound_result(item.dedup_key, "handed_off")
            except asyncio.CancelledError:
                self._store.inbound_result(row["key"], "uncertain", "cancelled during handoff")
                raise
            except Exception as exc:
                self._store.inbound_result(row["key"], "uncertain", type(exc).__name__)
                log.warning("QQ handoff uncertain: %s", type(exc).__name__)

    async def _choose(self, kind, identifier):
        for account in sorted(self.settings.accounts, key=lambda a: (a.priority, a.id)):
            connection = self._connections[account.id]
            if not connection.healthy:
                continue
            if kind == "group":
                try:
                    member = await connection.rpc(
                        "get_group_member_info",
                        {"group_id": int(identifier), "user_id": int(account.id), "no_cache": True},
                    )
                    if str(member.get("user_id")) != account.id:
                        continue
                except (ActionError, ActionUncertain, ConnectionError, AttributeError):
                    continue  # Read-only preflight; safe to examine a standby.
            return account, connection
        return None

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        try:
            _, kind, identifier = parse_target(chat_id, self.settings.logical_id)
            permitted = self.settings.allowed_groups if kind == "group" else self.settings.allowed_users
            if identifier not in permitted:
                return SendResult(success=False, error="destination denied", retryable=False)
            if not isinstance(content, str) or not content or len(content) > 4000:
                return SendResult(
                    success=False, error="text must contain 1..4000 characters", retryable=False
                )
            async with self._send_lock:
                chosen = await self._choose(kind, identifier)
                if chosen is None:
                    return SendResult(
                        success=False, error="no verified account available for target", retryable=False
                    )
                account, connection = chosen
                # Native final replies carry the canonical event ID in reply_to.
                # Prefer it over inherited task context (native FIFO drains can retain
                # an earlier task context). Contextless proactive/recovery sends get
                # unique IDs; host redelivery is explicitly NOT exactly-once here.
                origin = reply_to or INGRESS.get() or uuid.uuid4().hex
                key = hashlib.sha256(
                    json.dumps([origin, chat_id, content, reply_to], ensure_ascii=False).encode()
                ).hexdigest()
                if not self._store.begin_send(key, chat_id, content, account.id):
                    previous = self._store.outbound(key)
                    return SendResult(
                        success=previous["state"] == "sent",
                        message_id=previous["message_id"],
                        error=None
                        if previous["state"] == "sent"
                        else "prior delivery unresolved; do not replay",
                        retryable=False,
                    )
                params = {
                    "message": [{"type": "text", "data": {"text": content}}],
                    "group_id" if kind == "group" else "user_id": int(identifier),
                }
                try:
                    result = await connection.rpc(
                        "send_group_msg" if kind == "group" else "send_private_msg", params
                    )
                    message_id = result.get("message_id") if isinstance(result, dict) else None
                    if message_id is None:
                        raise ActionUncertain("send acknowledged without message id")
                    self._store.send_result(key, "sent", message_id=message_id)
                    return SendResult(success=True, message_id=str(message_id))
                except ActionError:
                    self._store.send_result(key, "failed", error="OneBot explicitly rejected send")
                    return SendResult(success=False, error="OneBot rejected send", retryable=False)
                except asyncio.CancelledError:
                    self._store.send_result(key, "uncertain", error="cancelled during send")
                    raise
                except Exception as exc:
                    self._store.send_result(key, "uncertain", error=type(exc).__name__)
                    return SendResult(
                        success=False,
                        error="delivery outcome uncertain; no automatic resend",
                        retryable=False,
                    )
        except (ValueError, TypeError):
            return SendResult(success=False, error="invalid or unauthorized QQ target", retryable=False)

    def _send_retry_is_final(self, result):
        # retryable=False alone does NOT stop Hermes' plain-text fallback.
        # Every failure here is terminal for this attempt: reformatting or choosing
        # a second bot after dispatch can duplicate a message with a lost ACK.
        return not result.success

    async def get_chat_info(self, chat_id):
        _, kind, identifier = parse_target(chat_id, self.settings.logical_id)
        allowed = self.settings.allowed_groups if kind == "group" else self.settings.allowed_users
        if identifier not in allowed:
            raise ValueError("destination denied")
        return {"name": f"QQ {kind} {identifier}", "type": "dm" if kind == "private" else "group"}
