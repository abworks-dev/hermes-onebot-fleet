"""Authenticated forward-WebSocket OneBot v11 client, one instance per account."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from typing import Any

import aiohttp

from .config import Account, qq_id
from .read_requests import ReadPolicy
from .reconnect import ReconnectPolicy

log = logging.getLogger(__name__)


class ActionError(RuntimeError):
    """The server explicitly rejected an action (safe to distinguish from doubt)."""

    def __init__(self, action: str, retcode: int):
        self.action = action
        self.retcode = retcode
        super().__init__(f"OneBot action rejected (retcode={retcode})")


class ActionUncertain(RuntimeError):
    """A dispatched request may have executed; never automatically replay it."""


class OneBotConnection:
    MAX_MESSAGE_BYTES = 256 * 1024
    BACKOFF_INITIAL = 1.0
    BACKOFF_MAX = 60.0

    def __init__(
        self,
        account: Account,
        token: str | Callable[[], str] | None,
        on_event: Callable[[dict[str, Any]], Any],
        *,
        on_observation: Callable[[dict[str, Any]], Any] | None = None,
        reads: ReadPolicy | None = None,
        clock: Callable[[], float] = time.monotonic,
        reconnect: ReconnectPolicy | None = None,
        random_sample: Callable[[], float] = random.random,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ):
        self.account = account
        self._token = token
        self._on_event = on_event
        self._on_observation = on_observation
        self._reads = ReadPolicy() if reads is None else reads
        self._clock = clock
        self._reconnect = ReconnectPolicy() if reconnect is None else reconnect
        self.BACKOFF_INITIAL = self._reconnect.initial_seconds
        self.BACKOFF_MAX = self._reconnect.max_seconds
        self._random_sample, self._sleep = random_sample, sleep
        self._verified_since = None
        self._health_task: asyncio.Task | None = None
        self._health_ws = None
        self._health_next_at = 0.0
        self._health_finished_at = None
        self._health_backoff = self._reads.health_interval_seconds
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._runner: asyncio.Task | None = None
        self._pending: dict[str, tuple[asyncio.Future, str]] = {}
        self._healthy = False
        self._identity = "unknown"
        self._online = None
        self._good = None
        self._ready = asyncio.Event()
        self._stopping = False
        self._events: asyncio.Queue = asyncio.Queue(maxsize=256)
        self.dropped_events = 0
        self.callback_errors = 0

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    @property
    def healthy(self) -> bool:
        return self.connected and self._healthy

    @property
    def observation(self) -> dict:
        return {
            "connected": self.connected,
            "identity": self._identity,
            "online": self._online if self.connected else None,
            "good": self._good if self.connected else None,
        }

    async def start(self) -> None:
        """Start background reconnecting; use connect() to await confirmed health."""
        if self._runner is None or self._runner.done():
            self._stopping = False
            self._runner = asyncio.create_task(self._run())

    async def connect(self, timeout: float = 10.0) -> None:
        await self.start()
        await asyncio.wait_for(self._ready.wait(), timeout)

    async def stop(self) -> None:
        self._stopping = True
        self._healthy = False
        self._ready.clear()
        self._fail_pending()
        if self._runner is not None:
            self._runner.cancel()
            with suppress(asyncio.CancelledError):
                await self._runner
            self._runner = None
        if self._health_task is not None:
            self._health_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._health_task
            self._health_task = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._ws = None

    def _fail_pending(self) -> None:
        for future, _ in self._pending.values():
            if not future.done():
                future.set_exception(ActionUncertain("connection closed after request dispatch"))
        self._pending.clear()

    async def rpc(self, action: str, params: Mapping[str, Any] | None = None, timeout: float = 10.0) -> Any:
        if (
            not isinstance(action, str)
            or not action
            or (params is not None and not isinstance(params, Mapping))
            or timeout <= 0
        ):
            raise ValueError("invalid RPC arguments")
        ws = self._ws
        if ws is None or ws.closed:
            raise ConnectionError("OneBot is disconnected; request not dispatched")
        echo = uuid.uuid4().hex
        # Serialize before dispatch so malformed local data is not ambiguous.
        payload = json.dumps({"action": action, "params": dict(params or {}), "echo": echo}, allow_nan=False)
        future = asyncio.get_running_loop().create_future()
        self._pending[echo] = (future, action)
        try:
            try:
                # Bound BOTH socket write and response wait. A timeout after
                # beginning dispatch is ambiguous, never permission to replay.
                async with asyncio.timeout(timeout):
                    await ws.send_str(payload)
                    return await future
            except (ActionError, ActionUncertain):
                raise
            except TimeoutError:
                raise ActionUncertain("action response timed out after dispatch") from None
            except (aiohttp.ClientError, OSError, RuntimeError):
                raise ActionUncertain("connection failed during request dispatch") from None
        finally:
            self._pending.pop(echo, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()  # consume a concurrent disconnect exception

    async def check_health(self, *, periodic: bool = False, idle: bool = False) -> bool:
        """Coalesce checks on the same socket; periodic callers obey cooldown.

        A new socket always requires fresh identity verification. Explicit checks
        are used for initial verification; normal scheduling must use periodic=True.
        Cancellation of one waiter cannot cancel the shared read sequence.
        """
        ws = self._ws
        if ws is None or ws.closed:
            return False
        task = self._health_task
        if task is not None and not task.done() and self._health_ws is ws:
            return await asyncio.shield(task)
        due = self._health_next_at
        if idle and self._health_finished_at is not None:
            due = max(due, self._health_finished_at + self._reads.idle_health_interval_seconds)
        if periodic and self._health_ws is ws and self._clock() < due:
            return self.healthy
        # An old socket's read may finish later, but cannot authenticate this one.
        if task is not None and not task.done():
            task.cancel()
        self._health_ws = ws
        task = self._health_task = asyncio.create_task(self._scheduled_health(ws, idle))
        return await asyncio.shield(task)

    async def _scheduled_health(self, ws, idle) -> bool:
        verified = await self._probe_health()
        if self._ws is ws:
            interval = (
                self._reads.idle_health_interval_seconds if idle else self._reads.health_interval_seconds
            )
            if verified:
                self._health_backoff = self._reads.health_interval_seconds
            else:
                self._health_backoff = min(self._health_backoff * 2, self._reads.health_backoff_max_seconds)
                interval = max(interval, self._health_backoff)
            self._health_finished_at = self._clock()
            self._health_next_at = self._health_finished_at + interval
        return verified

    async def _probe_health(self) -> bool:
        # A periodic probe must not create an event-loss window on a connection
        # whose identity was already verified. New sockets start unverified.
        ws = self._ws
        identity, online, good = "unknown", None, None
        try:
            login = await self.rpc("get_login_info", {}, timeout=5)
            identity = (
                "verified"
                if isinstance(login, dict) and qq_id(login.get("user_id")) == self.account.id
                else "mismatch"
            )
            status = await self.rpc("get_status", {}, timeout=5)
            online = status.get("online") if isinstance(status, dict) else None
            good = status.get("good") if isinstance(status, dict) else None
            online = online if type(online) is bool else None
            good = good if type(good) is bool else None
            verified = identity == "verified" and online is True and good is not False
        except (ConnectionError, ActionError, ActionUncertain, ValueError):
            verified = False
            online, good = None, None
        # A late response from an obsolete probe cannot authenticate a new socket.
        if self._ws is not ws:
            return False
        was_healthy = self._healthy
        self._identity, self._online, self._good = identity, online, good
        self._healthy = verified and self.connected
        await self._emit_observation()
        if not self._healthy:
            self._verified_since = None
        elif not was_healthy or self._verified_since is None:
            self._verified_since = self._clock()
        if self._healthy:
            self._ready.set()
        else:
            self._ready.clear()
        return self._healthy

    async def _emit_observation(self) -> None:
        if self._on_observation is not None:
            try:
                result = self._on_observation(self.observation)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                self._healthy = False
                log.error("OneBot observation persistence failed; readiness denied")

    def _get_token(self) -> str:
        token = self._token() if callable(self._token) else self._token
        if token is None:
            return self.account.resolve_token()
        if not isinstance(token, str) or len(token) < 16 or any(c.isspace() or ord(c) < 32 for c in token):
            raise ValueError("token must contain at least 16 non-whitespace characters")
        return token

    async def _run(self) -> None:
        delay = self.BACKOFF_INITIAL
        try:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
            while not self._stopping:
                reader = worker = None
                try:
                    token = self._get_token()
                    async with self._session.ws_connect(
                        self.account.ws_url,
                        headers={"Authorization": "Bearer " + token},
                        max_msg_size=self.MAX_MESSAGE_BYTES,
                        heartbeat=20,
                        timeout=aiohttp.ClientWSTimeout(ws_close=0.5),
                    ) as ws:
                        self._ws = ws
                        self._identity, self._online, self._good = "unknown", None, None
                        self._health_finished_at = None
                        reader = asyncio.create_task(self._read(ws))
                        worker = asyncio.create_task(self._dispatch())
                        if not await self.check_health():
                            raise ConnectionError("OneBot identity or online status not confirmed")
                        await reader
                except (aiohttp.ClientError, OSError, ValueError, ConnectionError, TimeoutError):
                    pass  # Never log exceptions carrying endpoint headers or peer data.
                finally:
                    # A briefly authenticated socket is still a flap. Only a
                    # continuously healthy interval may reset connection backoff.
                    if (
                        self._healthy
                        and self._verified_since is not None
                        and self._clock() - self._verified_since >= self._reconnect.stable_seconds
                    ):
                        delay = self.BACKOFF_INITIAL
                    self._verified_since = None
                    self._healthy = False
                    self._ready.clear()
                    self._fail_pending()
                    self._ws = None
                    if self._health_task is not None and not self._health_task.done():
                        self._health_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await self._health_task
                    await self._emit_observation()
                    for task in (reader, worker):
                        if task is not None:
                            task.cancel()
                            with suppress(asyncio.CancelledError):
                                await task
                    while not self._events.empty():
                        self._events.get_nowait()
                if not self._stopping:
                    await self._sleep(self._reconnect.delay(delay, self._random_sample()))
                    delay = min(delay * 2, self.BACKOFF_MAX)
        finally:
            self._healthy = False
            self._ready.clear()
            self._fail_pending()
            self._ws = None
            if self._session is not None:
                await self._session.close()

    async def _read(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for message in ws:
            if message.type != aiohttp.WSMsgType.TEXT:
                continue
            if len(message.data.encode("utf-8")) > self.MAX_MESSAGE_BYTES:
                await ws.close(code=1009)
                return
            try:
                event = json.loads(message.data)
            except (ValueError, RecursionError):
                continue
            if not isinstance(event, dict):
                continue
            if "echo" in event:
                echo = event["echo"]
                if not isinstance(echo, str) or echo not in self._pending:
                    continue
                future, action = self._pending[echo]
                if future.done():
                    continue
                retcode = event.get("retcode")
                if type(retcode) is not int:
                    continue
                if retcode != 0:
                    future.set_exception(ActionError(action, retcode))
                elif event.get("status") == "ok":
                    future.set_result(event.get("data"))
                continue
            if not self.healthy or event.get("post_type") != "message":
                continue
            try:
                if (
                    qq_id(event.get("self_id")) != self.account.id
                    or qq_id(event.get("user_id")) == self.account.id
                ):
                    continue
            except ValueError:
                continue
            try:
                self._events.put_nowait(event)
            except asyncio.QueueFull:
                self.dropped_events += 1
                if self.dropped_events & (self.dropped_events - 1) == 0:
                    log.warning("QQ transport queue overflow; dropped count=%d", self.dropped_events)

    async def _dispatch(self) -> None:
        while True:
            event = await self._events.get()
            try:
                result = self._on_event(event)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                self.callback_errors += 1
                if self.callback_errors & (self.callback_errors - 1) == 0:
                    log.warning(
                        "QQ event callback failed: %s; count=%d", type(exc).__name__, self.callback_errors
                    )
