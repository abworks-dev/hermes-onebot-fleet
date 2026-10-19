"""Thin native Hermes edge. All task execution and approval state stays in Hermes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from contextvars import ContextVar
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

from .account_state import AccountState
from .config import Settings, qq_id
from .delivery_budget import DeliveryBudget
from .health import HealthState
from .lock import InstanceLock
from .protocol import Incoming, currently_admitted, normalize_event
from .publisher import PUBLICATION_CREATED, Deferred, NoRoute, Publisher
from .store import Store
from .transport import ActionError, ActionUncertain, OneBotConnection

log = logging.getLogger(__name__)
INGRESS = ContextVar("onebot_fleet_ingress", default=None)
# Only the authenticated, durable publication service sets this context.
PUBLICATION = ContextVar("onebot_fleet_publication", default=None)


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
        self._admission = None
        self._health = None
        self._budget = None
        self._budget_wait = {}
        self._receive_events = receive_events
        self._tasks = []
        self._connections = {}
        self._send_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._running = False
        self._publisher = None
        self._publish_secrets = {}
        for client in self.settings.publisher.clients:
            self._publish_secrets[client.token_env] = get_scoped_secret(client.token_env)
            client.resolve_token(self._publish_secrets)
            for address in client.targets.values():
                _, kind, identifier = parse_target(address, self.settings.logical_id)
                allowed = self.settings.allowed_groups if kind == "group" else self.settings.allowed_users
                if identifier not in allowed:
                    raise ValueError("publisher target exceeds Fleet destination policy")
        for account in self.settings.accounts:

            async def callback(raw, current=account):
                await self._on_event(current, raw)

            self._connections[account.id] = OneBotConnection(
                account,
                lambda current=account: current.resolve_token(secrets),
                callback,
                on_observation=lambda observation, identifier=account.id: self._observe_health(
                    identifier, observation
                ),
            )

    async def connect(self, *, is_reconnect=False):
        async with self._lifecycle_lock:
            if self._running:
                return True
            self._lock.acquire()
            try:
                self._store = Store(self._path)
                self._store.recover()
                self._admission = AccountState(
                    self._path.with_name(self._path.name + ".accounts.sqlite3"),
                    self.settings.logical_id,
                    [a.id for a in self.settings.accounts],
                )
                self._health_failed = False
                self._health = HealthState(
                    self._path.with_name(self._path.name + ".health.sqlite3"), self.settings.logical_id
                )
                self._budget = DeliveryBudget(
                    self._path.with_name(self._path.name + ".budget.sqlite3"),
                    self.settings.logical_id,
                    self.settings.budget,
                )
                self._budget.recover()
                self._budget_failed = False
                for connection in self._connections.values():
                    await connection.start()
                self._tasks.append(asyncio.create_task(self._health_loop()))
                if self._receive_events:
                    self._tasks.append(asyncio.create_task(self._drain()))
                if self.settings.publisher.enabled:
                    self._publisher = Publisher(
                        self.settings.publisher,
                        self._path.with_name(self._path.name + ".publications.sqlite3"),
                        self._publish_secrets,
                        self._publish_once,
                    )
                    await self._publisher.start()
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
        publisher = getattr(self, "_publisher", None)
        try:
            if publisher is not None:
                await publisher.stop()
        finally:
            self._publisher = None
            try:
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)
                self._tasks.clear()
                await asyncio.gather(*(c.stop() for c in self._connections.values()), return_exceptions=True)
            finally:
                self._lock.release()

    def _account_allowed(self, account_id, revision=None):
        return (
            self._admission is not None
            and account_id in self._connections
            and self._admission.is_allowed(account_id, revision)
        )

    def _observe_health(self, account_id, observation):
        try:
            self._health.observe(account_id, **observation)
        except Exception:
            self._health_failed = True
            log.error("Fleet diagnostic store unavailable; business delivery held")

    def _report_health(self, account_id, kind, address=None):
        try:
            self._health.report(account_id, kind, address)
        except Exception:
            self._health_failed = True
            log.error("Fleet diagnostic store unavailable; business delivery held")

    def _operational(self, account_id, address=None):
        health = getattr(self, "_health", None)
        return (
            health is not None
            and not getattr(self, "_health_failed", False)
            and not getattr(self, "_budget_failed", False)
            and health.is_eligible(account_id, address)
        )

    def _admitted_payload(self, payload):
        """Old/unknown revisions fail closed, including work queued before upgrade."""
        try:
            data = json.loads(payload)
            if not isinstance(data, dict):
                return None
            revision = data.pop("_admission_revision", None)
            if type(revision) is not int:
                return None
            item = Incoming(**data)
            if not currently_admitted(item, self.settings):
                return None
            if not self._account_allowed(item.physical_account, revision) or not self._operational(
                item.physical_account
            ):
                return None
            return item
        except (ValueError, TypeError, KeyError):
            return None

    def _reply_admitted(self, reply_to, chat_id):
        if not isinstance(reply_to, str) or self._store is None:
            return False
        row = self._store.inbound(reply_to)
        if row is None or row["state"] not in {"handing_off", "handed_off"}:
            return False
        item = self._admitted_payload(row["payload"])
        return item is not None and target(item.logical_id, item.chat_type, item.chat_id) == chat_id

    async def _on_event(self, account, raw):
        if not self._receive_events or self._store is None or self._admission is None:
            return
        admission = self._admission.snapshot(account.id)
        if admission["state"] != "allowed" or not self._operational(account.id):
            return
        revision = admission["revision"]
        item = normalize_event(raw, account, self.settings)
        if item and self._operational(account.id):
            payload = asdict(item)
            payload["_admission_revision"] = revision
            self._store.enqueue(item.dedup_key, payload)

    async def _health_loop(self):
        while True:
            await asyncio.gather(
                *(c.check_health() for c in self._connections.values()), return_exceptions=True
            )
            for account_id, connection in self._connections.items():
                observation = getattr(connection, "observation", None)
                if observation is not None:
                    self._observe_health(account_id, observation)
            if not any(
                c.healthy and self._account_allowed(a) and self._operational(a)
                for a, c in self._connections.items()
            ):
                # Local durable alert, never a message sent through the broken QQ pool.
                self._report_health(self.settings.accounts[0].id, "pool_unavailable")
            else:
                self._report_health(self.settings.accounts[0].id, "pool_available")
            await asyncio.sleep(5)

    @staticmethod
    def _fresh(created, max_age):
        return created is None or (type(created) in (int, float) and 0 <= time.time() - created <= max_age)

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
                item = self._admitted_payload(row["payload"])
                if item is None or not self._fresh(
                    row["created"], self._budget.policy.max_pending_age_seconds
                ):
                    self._store.inbound_result(
                        row["key"], "denied", "admission revoked or pending expired before handoff"
                    )
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
        if self._admission is None:
            return None
        accounts = sorted(self.settings.accounts, key=lambda a: (a.priority, a.id))
        address = target(self.settings.logical_id, kind, identifier)
        candidates = [
            a.id
            for a in accounts
            if self._account_allowed(a.id) and self._connections[a.id].healthy and self._operational(a.id)
        ]
        primary = self._admission.select_primary(candidates)
        # Target-specific membership failure does not change the global leader.
        accounts.sort(key=lambda a: a.id != primary)
        invalidated = set()
        for account in accounts:
            connection = self._connections[account.id]
            admission = self._admission.snapshot(account.id)
            if admission["state"] != "allowed" or not connection.healthy or not self._operational(account.id):
                invalidated.add(account.id)
                continue
            revision = admission["revision"]
            if not self._operational(account.id, address):
                continue  # Target circuit cannot consume the entire account or leader preference.
            member_ready = True
            if kind == "group":
                try:
                    async with asyncio.timeout(2.0):
                        member = await connection.rpc(
                            "get_group_member_info",
                            {"group_id": int(identifier), "user_id": int(account.id), "no_cache": True},
                        )
                    member_ready = str(member.get("user_id")) == account.id
                    if not member_ready:
                        self._report_health(account.id, "target_unavailable", address)
                except ActionError:
                    member_ready = False
                    self._report_health(account.id, "target_unavailable", address)
                except (ActionUncertain, ConnectionError, AttributeError, TimeoutError):
                    # An inconclusive read is NOT evidence of a group restriction.
                    member_ready = False
            if (
                self._account_allowed(account.id, revision)
                and connection.healthy
                and self._operational(account.id)
            ):
                if not member_ready:
                    continue  # Target-only failure does not change global preference.
                if invalidated:
                    # Revocation/transport loss across awaited preflight is a
                    # global failover, not just a one-target membership fallback.
                    self._admission.select_primary(
                        [
                            a.id
                            for a in sorted(self.settings.accounts, key=lambda a: (a.priority, a.id))
                            if a.id not in invalidated
                            and self._account_allowed(a.id)
                            and self._connections[a.id].healthy
                            and self._operational(a.id)
                        ]
                    )
                return account, connection, revision
            invalidated.add(account.id)
        return None

    @staticmethod
    def _recorded_result(row):
        return SendResult(
            success=row["state"] == "sent",
            message_id=row["message_id"],
            error=None if row["state"] == "sent" else "prior delivery unresolved; do not replay",
            retryable=False,
        )

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
            origin = PUBLICATION.get() or reply_to
            if not origin:
                # Native cron has a second, outer fallback not governed by
                # _send_retry_is_final. Unkeyed proactive sends must fail closed;
                # background callers use the durable publisher, deliver=local.
                return SendResult(
                    success=False,
                    error="background publication requires durable publisher ID",
                    retryable=False,
                )
            async with self._send_lock:
                key = hashlib.sha256(
                    json.dumps([origin, chat_id, content, reply_to], ensure_ascii=False).encode()
                ).hexdigest()
                if self._store is None:
                    return SendResult(success=False, error="adapter ledger unavailable", retryable=False)
                previous = self._store.outbound(key)
                if previous is not None:
                    # Admission changes cannot erase an ACK or turn an already
                    # ambiguous attempt into a fresh NoRoute/pending retry.
                    return self._recorded_result(previous)
                if not PUBLICATION.get() and not self._reply_admitted(reply_to, chat_id):
                    return SendResult(
                        success=False, error="reply admission revoked or unavailable", retryable=False
                    )
                # Old pending work is not flushed in a recovery burst. Never mint
                # a fresh publication ID to bypass this deliberate expiry.
                created = PUBLICATION_CREATED.get()
                if not PUBLICATION.get():
                    anchor = self._store.inbound(reply_to)
                    created = anchor["created"] if anchor else None
                budget = getattr(self, "_budget", None)
                if budget is None:
                    return SendResult(success=False, error="shared budget unavailable", retryable=False)
                if not self._fresh(created, budget.policy.max_pending_age_seconds):
                    return SendResult(success=False, error="pending delivery expired", retryable=False)
                chosen = await self._choose(kind, identifier)
                if chosen is None:
                    return SendResult(
                        success=False, error="no verified account available for target", retryable=False
                    )
                account, connection, revision = chosen
                # Preflight awaited the peer: revoke stale permits after any pause,
                # including pause -> allow while that read-only RPC was in flight.
                if (
                    not self._account_allowed(account.id, revision)
                    or not connection.healthy
                    or not self._operational(account.id, chat_id)
                ):
                    return SendResult(
                        success=False, error="no verified account available for target", retryable=False
                    )
                if not PUBLICATION.get() and not self._reply_admitted(reply_to, chat_id):
                    return SendResult(
                        success=False, error="reply admission revoked or unavailable", retryable=False
                    )
                # Native final replies carry canonical event IDs in reply_to;
                # publisher IDs come from the authenticated immutable publication
                # record, NOT caller-provided metadata or a per-attempt UUID.
                permit = budget.reserve(key, chat_id)
                if not permit["allowed"]:
                    # No send ledger claim or mutating RPC; publisher may wait on
                    # the SAME ID. A native reply is terminal, never backgrounded.
                    if PUBLICATION.get() and permit["reason"] != "prior_reservation":
                        self._budget_wait[key] = permit["next_at"]
                    return SendResult(success=False, error="shared sending budget denied", retryable=False)
                self._budget_wait.pop(key, None)
                if not self._store.begin_send(key, chat_id, content, account.id):
                    return self._recorded_result(self._store.outbound(key))
                params = {
                    "message": [{"type": "text", "data": {"text": content}}],
                    "group_id" if kind == "group" else "user_id": int(identifier),
                }
                # SQLite insertion can block behind another local writer. Recheck
                # AFTER it, not only before it. A known pre-dispatch revocation
                # keeps a terminal no-send tombstone; it is not a retry permit.
                max_age = budget.policy.max_pending_age_seconds
                reply_allowed = PUBLICATION.get() or self._reply_admitted(reply_to, chat_id)
                if (
                    not reply_allowed
                    or not self._account_allowed(account.id, revision)
                    or not connection.healthy
                    or not self._operational(account.id, chat_id)
                ):
                    self._store.send_result(key, "failed", error="admission revoked before dispatch")
                    return SendResult(
                        success=False, error="admission revoked before dispatch", retryable=False
                    )
                if not self._fresh(created, max_age):
                    self._store.send_result(key, "failed", error="pending delivery expired before dispatch")
                    return SendResult(
                        success=False, error="pending delivery expired before dispatch", retryable=False
                    )
                try:
                    result = await connection.rpc(
                        "send_group_msg" if kind == "group" else "send_private_msg", params
                    )
                    message_id = result.get("message_id") if isinstance(result, dict) else None
                    if (
                        isinstance(message_id, bool)
                        or not isinstance(message_id, (int, str))
                        or not re.fullmatch(r"-?[0-9]{1,64}", str(message_id))
                    ):
                        raise ActionUncertain("send acknowledged without valid message id")
                    self._store.send_result(key, "sent", message_id=message_id)
                    self._report_health(account.id, "send_ack", chat_id)
                    return SendResult(success=True, message_id=str(message_id))
                except ActionError:
                    # An explicit rejection is target evidence, NOT proof of a
                    # frozen account. Never try a second account for this intent.
                    self._report_health(account.id, "target_rejected", chat_id)
                    self._store.send_result(key, "failed", error="OneBot explicitly rejected send")
                    return SendResult(success=False, error="OneBot rejected send", retryable=False)
                except asyncio.CancelledError:
                    self._report_health(account.id, "outcome_unknown", chat_id)
                    self._store.send_result(key, "uncertain", error="cancelled during send")
                    raise
                except Exception as exc:
                    self._report_health(account.id, "outcome_unknown", chat_id)
                    self._store.send_result(key, "uncertain", error=type(exc).__name__)
                    return SendResult(
                        success=False,
                        error="delivery outcome uncertain; no automatic resend",
                        retryable=False,
                    )
                finally:
                    try:
                        budget.complete(key)
                    except Exception:
                        # Preserve known ACKs and ambiguous results, but do not
                        # continue sending without a persisted cooldown fence.
                        self._budget_failed = True
                        log.error("Fleet cooldown persistence failed; business delivery held")
        except (ValueError, TypeError):
            return SendResult(success=False, error="invalid or unauthorized QQ target", retryable=False)

    async def _publish_once(self, publication_key, chat_id, content):
        """Sole publication side effect; uses existing owned connections only."""
        token = PUBLICATION.set("publication:" + publication_key)
        try:
            result = await self.send(chat_id, content)
        finally:
            PUBLICATION.reset(token)
        if result.error == "no verified account available for target":
            # Read-only account/group preflight only; no send RPC was attempted.
            raise NoRoute()
        origin = "publication:" + publication_key
        key = hashlib.sha256(
            json.dumps([origin, chat_id, content, None], ensure_ascii=False).encode()
        ).hexdigest()
        row = self._store.outbound(key)
        next_at = self._budget_wait.pop(key, None)
        if row is None and next_at is not None:
            raise Deferred(next_at)
        if result.success:
            return {
                "state": "sent",
                "message_id": result.message_id,
                "account": row["account"] if row else None,
            }
        if row and row["state"] in {"sending", "uncertain"}:
            return {"state": "unknown"}
        return {"state": "rejected"}

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
