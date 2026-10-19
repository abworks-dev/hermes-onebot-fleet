"""Capability-scoped loopback publication queue; independent of Hermes and QQ.

Only the adapter's injected callback may send anything. A durable ``sending``
record is the uncertainty boundary: interrupted attempts are never replayed.
Tokens are captured from an explicit, already profile-scoped mapping, not from
process environment variables. SQLite payloads are discarded after 24 hours;
idempotency tombstones and attempt history survive payload expiry.

Optional per-client quiet hours retain accepted work as pending, without any
new delivery callback during [start, end). Asia/Shanghai means fixed UTC+08:00
for contemporary schedules, not historical DST rules; no timezone database or
silent timezone fallback is used. Existing in-flight work is cancelled at the
next quiet boundary and becomes unknown, since a prior RPC cannot be undone.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import sqlite3
import time
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from aiohttp import web

from .lock import InstanceLock

_NAME = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_ALIAS = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")
_PUBLICATION_ID = re.compile(r"[A-Za-z0-9_.:-]{1,160}\Z")
_TARGET = re.compile(r"obf:[A-Za-z0-9_-]{1,128}:(?:private|group):[1-9][0-9]*\Z")
_STATES = ("pending", "sending", "sent", "unknown", "rejected", "expired")
_MAX_PAYLOAD = 16 * 1024
_PAYLOAD_TTL = 24 * 3600
_MAX_PENDING = 1000
# Internal callback provenance only; callers cannot set this through HTTP payloads.
PUBLICATION_CREATED = ContextVar("onebot_fleet_publication_created", default=None)


class NoRoute(Exception):
    """Delivery guarantees that NO mutating RPC was attempted; retry is safe."""


class Deferred(NoRoute):
    """A durable shared budget refused admission BEFORE any side-effect claim."""

    def __init__(self, next_at: float):
        self.next_at = next_at
        super().__init__("shared delivery budget deferred")


@dataclass(frozen=True)
class PublisherQuietHours:
    """Daily [start, end) quiet period in modern Asia/Shanghai (fixed UTC+8)."""

    start: str
    end: str
    time_zone: str

    def __post_init__(self) -> None:
        for value in (self.start, self.end):
            if not isinstance(value, str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value):
                raise ValueError("publisher quiet hours require strict HH:MM")
        if self.start == self.end:
            raise ValueError("publisher quiet hours start and end must differ")
        if not isinstance(self.time_zone, str) or self.time_zone != "Asia/Shanghai":
            raise ValueError("publisher quiet hours time_zone must be Asia/Shanghai")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> PublisherQuietHours:
        if not isinstance(mapping, Mapping) or set(mapping) != {"start", "end", "time_zone"}:
            raise ValueError("invalid publisher quiet hours fields")
        return cls(**mapping)

    @staticmethod
    def _minute(value: str) -> int:
        hour, minute = value.split(":")
        return int(hour) * 60 + int(minute)

    def is_quiet(self, timestamp: float) -> bool:
        minute = int(((timestamp + 8 * 3600) % 86400) // 60)
        start, end = self._minute(self.start), self._minute(self.end)
        if start < end:
            return start <= minute < end
        return minute >= start or minute < end

    def seconds_until_start(self, timestamp: float) -> float:
        return (self._minute(self.start) * 60 - (timestamp + 8 * 3600) % 86400) % 86400


@dataclass(frozen=True)
class PublisherClient:
    id: str
    token_env: str
    targets: Mapping[str, str]
    max_per_hour: int = 6
    quiet_hours: PublisherQuietHours | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not _NAME.fullmatch(self.id):
            raise ValueError("invalid publisher client id")
        if not isinstance(self.token_env, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.token_env):
            raise ValueError("publisher token_env must name an environment variable")
        if not isinstance(self.targets, Mapping) or not self.targets:
            raise ValueError("publisher targets must be a nonempty alias mapping")
        for alias, target in self.targets.items():
            if not isinstance(alias, str) or not _ALIAS.fullmatch(alias):
                raise ValueError("invalid publisher target alias")
            if not isinstance(target, str) or not _TARGET.fullmatch(target):
                raise ValueError("invalid publisher canonical target")
        if type(self.max_per_hour) is not int or not 1 <= self.max_per_hour <= 60:
            raise ValueError("publisher max_per_hour must be 1..60")
        if self.quiet_hours is not None and not isinstance(self.quiet_hours, PublisherQuietHours):
            raise ValueError("invalid publisher quiet hours policy")
        object.__setattr__(self, "targets", MappingProxyType(dict(self.targets)))

    def resolve_token(self, secrets: Mapping[str, str]) -> str:
        token = secrets.get(self.token_env)
        if (
            not isinstance(token, str)
            or len(token) < 32
            or any(c.isspace() or ord(c) < 33 or ord(c) == 127 for c in token)
        ):
            raise ValueError("publisher token must contain at least 32 non-whitespace characters")
        try:
            token.encode("utf-8")
        except UnicodeError:
            raise ValueError("invalid publisher token") from None
        return token


# RequestKey was introduced after aiohttp 3.12; keep the declared lower bound
# usable while avoiding string-key warnings on newer releases.
_CLIENT_KEY = (
    web.RequestKey("publisher_client", PublisherClient) if hasattr(web, "RequestKey") else "publisher_client"
)


@dataclass(frozen=True)
class PublisherSettings:
    enabled: bool = False
    port: int = 18791
    clients: tuple[PublisherClient, ...] = ()

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise ValueError("publisher enabled must be boolean")
        if type(self.port) is not int or not 1024 <= self.port <= 65535:
            raise ValueError("publisher port must be 1024..65535")
        if not isinstance(self.clients, tuple) or any(
            not isinstance(c, PublisherClient) for c in self.clients
        ):
            raise ValueError("invalid publisher clients")
        if len({c.id for c in self.clients}) != len(self.clients):
            raise ValueError("duplicate publisher client ids")
        if self.enabled and not self.clients:
            raise ValueError("enabled publisher requires clients")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> PublisherSettings:
        if not isinstance(mapping, Mapping) or set(mapping) - {"enabled", "port", "clients"}:
            raise ValueError("unknown publisher settings fields")
        enabled = mapping.get("enabled", False)
        if enabled is True and "port" not in mapping:
            raise ValueError("enabled publisher requires an explicit port")
        raw_clients = mapping.get("clients", [])
        if not isinstance(raw_clients, list):
            raise ValueError("publisher clients must be a list")
        clients = []
        for item in raw_clients:
            if (
                not isinstance(item, Mapping)
                or set(item) - {"id", "token_env", "targets", "max_per_hour", "quiet_hours"}
                or not {"id", "token_env", "targets"} <= set(item)
            ):
                raise ValueError("invalid publisher client fields")
            fields = dict(item)
            if "quiet_hours" in fields:
                fields["quiet_hours"] = PublisherQuietHours.from_mapping(fields["quiet_hours"])
            clients.append(PublisherClient(**fields))
        return cls(enabled=enabled, port=mapping.get("port", 18791), clients=tuple(clients))


Deliver = Callable[[str, str, str], Awaitable[dict]]


def _json_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


class Publisher:
    """One loopback HTTP server, one SQLite connection and one dispatch worker."""

    NO_ROUTE_BACKOFF = 1.0
    ROUTE_BACKOFF = 2.0
    BODY_TIMEOUT = 2.0
    DELIVERY_TIMEOUT = 30.0
    SHUTDOWN_TIMEOUT = 0.5

    def __init__(
        self,
        settings: PublisherSettings,
        state_path: str | Path,
        secrets: Mapping[str, str],
        deliver: Deliver,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.settings = settings
        self._clock = time.time if clock is None else clock
        self.state_path = Path(state_path)
        self._lock = InstanceLock(str(self.state_path) + ".lock")
        self._clients = {client.id: client for client in settings.clients}
        self._tokens = []
        for client in settings.clients:
            digest = hashlib.sha256(client.resolve_token(secrets).encode("utf-8")).digest()
            if any(hmac.compare_digest(digest, existing) for _, existing in self._tokens):
                raise ValueError("publisher clients must have distinct tokens")
            self._tokens.append((client.id, digest))
        self._deliver = deliver
        self._db: sqlite3.Connection | None = None
        self._runner: web.AppRunner | None = None
        self._worker: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._route_retry: dict[tuple[str, str], float] = {}
        self._lifecycle = asyncio.Lock()
        self._stopping = True

    @staticmethod
    def stable_key(client_id: str, publication_id: str) -> str:
        encoded = json.dumps([client_id, publication_id], separators=(",", ":")).encode("utf-8")
        return "publication:" + hashlib.sha256(encoded).hexdigest()

    async def start(self) -> None:
        async with self._lifecycle:
            if self._runner is not None or not self.settings.enabled:
                return
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                self._lock.acquire()
                self._db = sqlite3.connect(self.state_path)
                self._db.row_factory = sqlite3.Row
                self._db.execute("PRAGMA journal_mode=WAL")
                self._db.execute("PRAGMA synchronous=FULL")
                self._db.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS publications (
                        client TEXT NOT NULL, id TEXT NOT NULL,
                        payload_sha TEXT NOT NULL, target_alias TEXT NOT NULL,
                        canonical_target TEXT, text TEXT,
                        status TEXT NOT NULL, created REAL NOT NULL,
                        retry_at REAL NOT NULL DEFAULT 0,
                        message_id TEXT, account TEXT, error TEXT,
                        PRIMARY KEY (client, id)
                    );
                    CREATE INDEX IF NOT EXISTS publication_pending
                        ON publications(status, retry_at, created);
                    CREATE TABLE IF NOT EXISTS attempts (
                        attempt_id INTEGER PRIMARY KEY,
                        client TEXT NOT NULL, id TEXT NOT NULL, attempted REAL NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS publication_rate ON attempts(client, attempted);
                    """
                )
                with self._db:
                    self._db.execute(
                        "UPDATE publications SET status='unknown', error='delivery_unknown' WHERE status='sending'"
                    )
                self._expire()
                app = web.Application(client_max_size=_MAX_PAYLOAD, middlewares=[self._guard])
                app.router.add_post("/v1/publications", self._post)
                app.router.add_get("/v1/publications/{id}", self._get)
                app.router.add_get("/v1/status", self._status)
                self._runner = web.AppRunner(app, access_log=None, shutdown_timeout=self.SHUTDOWN_TIMEOUT)
                await self._runner.setup()
                await web.TCPSite(self._runner, "127.0.0.1", self.settings.port).start()
                self._stopping = False
                self._wake.set()
                self._worker = asyncio.create_task(self._run(), name="onebot-publication-worker")
            except BaseException:
                await self._close()
                raise

    async def stop(self) -> None:
        async with self._lifecycle:
            # An interrupted owner shutdown must still settle sending and release
            # its HTTP sockets, SQLite connection and exclusive process lock.
            cleanup = asyncio.create_task(self._close())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise

    async def _close(self) -> None:
        self._stopping = True
        try:
            if self._worker is not None:
                self._worker.cancel()
                await asyncio.gather(self._worker, return_exceptions=True)
                self._worker = None
        finally:
            try:
                if self._runner is not None:
                    await self._runner.cleanup()
                    self._runner = None
            finally:
                try:
                    if self._db is not None:
                        self._db.close()
                        self._db = None
                finally:
                    self._lock.release()

    def _expire(self) -> None:
        now = self._clock()
        with self._db:
            self._db.execute(
                "UPDATE publications SET status='expired', error='payload_expired' "
                "WHERE status='pending' AND created<=?",
                (now - _PAYLOAD_TTL,),
            )
            self._db.execute(
                "UPDATE publications SET text=NULL, canonical_target=NULL "
                "WHERE created<=? AND (text IS NOT NULL OR canonical_target IS NOT NULL)",
                (now - _PAYLOAD_TTL,),
            )
            self._db.execute("DELETE FROM attempts WHERE attempted<=?", (now - 3600,))

    @web.middleware
    async def _guard(self, request: web.Request, handler: Callable) -> web.StreamResponse:
        # Authenticate even unknown paths, preflights and incorrect Host values.
        headers = request.headers.getall("Authorization", [])
        auth = headers[0] if len(headers) == 1 else ""
        try:
            candidate = (
                hashlib.sha256(auth[7:].encode("utf-8")).digest() if auth.startswith("Bearer ") else b""
            )
        except UnicodeError:
            candidate = b""
        client_id = None
        for identifier, digest in self._tokens:
            if hmac.compare_digest(candidate, digest):
                client_id = identifier
        if client_id is None:
            return web.json_response({"error": "unauthorized"}, status=401)
        if request.headers.getall("Host", []) not in (
            [f"127.0.0.1:{self.settings.port}"],
            [f"localhost:{self.settings.port}"],
        ):
            return web.json_response({"error": "invalid_host"}, status=403)
        if "Origin" in request.headers:
            return web.json_response({"error": "origin_not_allowed"}, status=403)
        if request.query_string:
            return web.json_response({"error": "query_not_allowed"}, status=400)
        if self._stopping:
            return web.json_response({"error": "unavailable"}, status=503)
        request[_CLIENT_KEY] = self._clients[client_id]
        try:
            return await handler(request)
        except web.HTTPException as exc:
            # aiohttp's default 404/405 messages can reflect request components.
            return web.json_response({"error": "request_rejected"}, status=exc.status)
        except Exception:
            # Never reflect exception text or ask aiohttp to log payloads/credentials.
            return web.json_response({"error": "unavailable"}, status=503)

    @staticmethod
    def _view(row: sqlite3.Row) -> dict:
        return {
            "client": row["client"],
            "publication_id": row["id"],
            "status": row["status"],
            "message_id": row["message_id"],
            "account": row["account"],
            "error": row["error"],
        }

    def _attempts(self, client_id: str, now: float) -> int:
        return self._db.execute(
            "SELECT COUNT(*) FROM attempts WHERE client=? AND attempted>?", (client_id, now - 3600)
        ).fetchone()[0]

    async def _post(self, request: web.Request) -> web.Response:
        if request.content_type != "application/json":
            return web.json_response({"error": "json_required"}, status=415)
        if request.content_length is not None and request.content_length > _MAX_PAYLOAD:
            return web.json_response({"error": "payload_too_large"}, status=413)
        body = bytearray()
        try:
            async with asyncio.timeout(self.BODY_TIMEOUT):
                async for chunk in request.content.iter_chunked(4096):
                    body.extend(chunk)
                    if len(body) > _MAX_PAYLOAD:
                        return web.json_response({"error": "payload_too_large"}, status=413)
        except TimeoutError:
            return web.json_response({"error": "request_timeout"}, status=408)
        try:
            payload = json.loads(body.decode("utf-8"), object_pairs_hook=_json_object)
            if not isinstance(payload, dict) or set(payload) != {"publication_id", "target", "text"}:
                raise ValueError("invalid fields")
            identifier, alias, text = payload["publication_id"], payload["target"], payload["text"]
            if not isinstance(identifier, str) or not _PUBLICATION_ID.fullmatch(identifier):
                raise ValueError("invalid id")
            if not isinstance(alias, str) or not _ALIAS.fullmatch(alias):
                raise ValueError("invalid alias")
            if not isinstance(text, str) or not 1 <= len(text) <= 1800:
                raise ValueError("invalid text")
            text.encode("utf-8")
        except (ValueError, UnicodeError, RecursionError):
            return web.json_response({"error": "invalid_publication"}, status=400)
        client = request[_CLIENT_KEY]
        canonical = client.targets.get(alias)
        if canonical is None:
            return web.json_response({"error": "target_not_allowed"}, status=403)
        digest = hashlib.sha256(
            json.dumps([canonical, text], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        self._expire()
        existing = self._db.execute(
            "SELECT * FROM publications WHERE client=? AND id=?", (client.id, identifier)
        ).fetchone()
        if existing is not None:
            if not hmac.compare_digest(existing["payload_sha"], digest):
                return web.json_response({"error": "publication_conflict"}, status=409)
            return web.json_response(self._view(existing), status=202)
        now = self._clock()
        if self._attempts(client.id, now) >= client.max_per_hour:
            return web.json_response({"error": "rate_limited"}, status=429)
        pending = self._db.execute(
            "SELECT COUNT(*) FROM publications WHERE client=? AND status IN ('pending', 'sending')",
            (client.id,),
        ).fetchone()[0]
        if pending >= _MAX_PENDING:
            return web.json_response({"error": "queue_full"}, status=429)
        with self._db:
            self._db.execute(
                "INSERT INTO publications(client,id,payload_sha,target_alias,canonical_target,text,status,created) "
                "VALUES(?,?,?,?,?,?,'pending',?)",
                (client.id, identifier, digest, alias, canonical, text, now),
            )
        # Quiet intake is still accepted-pending, with normal ACL/queue/rate
        # checks. Only the worker may deliver it after the quiet window ends.
        if client.quiet_hours is None or not client.quiet_hours.is_quiet(self._clock()):
            self._wake.set()
        row = self._db.execute(
            "SELECT * FROM publications WHERE client=? AND id=?", (client.id, identifier)
        ).fetchone()
        return web.json_response(self._view(row), status=202)

    async def _get(self, request: web.Request) -> web.Response:
        identifier = request.match_info["id"]
        if not _PUBLICATION_ID.fullmatch(identifier):
            return web.json_response({"error": "not_found"}, status=404)
        self._expire()
        row = self._db.execute(
            "SELECT * FROM publications WHERE client=? AND id=?", (request[_CLIENT_KEY].id, identifier)
        ).fetchone()
        if row is None:
            return web.json_response({"error": "not_found"}, status=404)
        return web.json_response(self._view(row))

    async def _status(self, request: web.Request) -> web.Response:
        self._expire()
        client = request[_CLIENT_KEY]
        counts = dict.fromkeys(_STATES, 0)
        counts.update(
            dict(
                self._db.execute(
                    "SELECT status, COUNT(*) FROM publications WHERE client=? GROUP BY status", (client.id,)
                ).fetchall()
            )
        )
        return web.json_response({"client": client.id, "counts": counts})

    async def _run(self) -> None:
        while not self._stopping:
            self._wake.clear()
            self._expire()
            now = self._clock()
            rows = self._db.execute(
                "SELECT * FROM publications WHERE status='pending' AND retry_at<=? ORDER BY created, rowid",
                (now,),
            ).fetchall()
            visited = set()
            for queued in rows:
                if queued["client"] in visited:
                    continue
                # Yield even for purely local rejections; large queues must not
                # prevent HTTP progress or shutdown cancellation. Re-read after
                # every callback because another request may have expired rows.
                await asyncio.sleep(0)
                row = self._db.execute(
                    "SELECT * FROM publications WHERE client=? AND id=?",
                    (queued["client"], queued["id"]),
                ).fetchone()
                if row["status"] != "pending":
                    continue
                if row["created"] <= self._clock() - _PAYLOAD_TTL:
                    self._expire()
                    continue
                client = self._clients.get(row["client"])
                if client is None or client.targets.get(row["target_alias"]) != row["canonical_target"]:
                    self._finish(row, "rejected", error="target_not_allowed")
                    continue
                if client.quiet_hours is not None and client.quiet_hours.is_quiet(self._clock()):
                    visited.add(client.id)
                    continue
                if self._attempts(client.id, self._clock()) >= client.max_per_hour:
                    continue
                if self._route_retry.get((client.id, row["canonical_target"]), 0) > time.monotonic():
                    continue
                visited.add(client.id)  # At most one dispatch per client per round.
                await self._dispatch(row)
            # Timed wake also enforces TTL/rate windows without requiring traffic.
            if not self._stopping:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1.0)
                except TimeoutError:
                    pass

    def _finish(
        self,
        row: sqlite3.Row,
        state: str,
        *,
        message_id: str | None = None,
        account: str | None = None,
        error: str | None = None,
    ) -> None:
        with self._db:
            self._db.execute(
                "UPDATE publications SET status=?,message_id=?,account=?,error=? WHERE client=? AND id=?",
                (state, message_id, account, error, row["client"], row["id"]),
            )

    @staticmethod
    def _safe_id(value: object, *, account: bool = False) -> str | None:
        # Callback metadata is not trusted to contain neither exception text nor credentials.
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            return None
        pattern = r"[1-9][0-9]{0,63}" if account else r"-?[0-9]{1,64}"
        result = str(value)
        return result if re.fullmatch(pattern, result) else None

    async def _dispatch(self, row: sqlite3.Row) -> None:
        with self._db:
            row = self._db.execute(
                "SELECT * FROM publications WHERE client=? AND id=?", (row["client"], row["id"])
            ).fetchone()
            if row is None:
                return
            client = self._clients.get(row["client"])
            if client is None:
                return
            quiet_hours = client.quiet_hours
            if quiet_hours is not None and quiet_hours.is_quiet(self._clock()):
                return  # Never claim or consume an attempt during quiet hours.
            claimed = self._db.execute(
                "UPDATE publications SET status='sending',error=NULL "
                "WHERE client=? AND id=? AND status='pending' AND created>? "
                "AND text IS NOT NULL AND canonical_target IS NOT NULL",
                (row["client"], row["id"], self._clock() - _PAYLOAD_TTL),
            ).rowcount
            if claimed != 1:
                return
            attempt = self._db.execute(
                "INSERT INTO attempts(client,id,attempted) VALUES(?,?,?)",
                (row["client"], row["id"], self._clock()),
            ).lastrowid
        timeout = self.DELIVERY_TIMEOUT
        if quiet_hours is not None:
            now = self._clock()
            if quiet_hours.is_quiet(now):
                # The tiny durable claim transaction itself can cross a wall
                # clock boundary. No callback has run, so safely undo that claim.
                with self._db:
                    self._db.execute("DELETE FROM attempts WHERE attempt_id=?", (attempt,))
                    self._db.execute(
                        "UPDATE publications SET status='pending' WHERE client=? AND id=? AND status='sending'",
                        (row["client"], row["id"]),
                    )
                return
            timeout = min(timeout, quiet_hours.seconds_until_start(now))
        try:
            async with asyncio.timeout(timeout):
                created_token = PUBLICATION_CREATED.set(row["created"])
                try:
                    result = await self._deliver(
                        self.stable_key(row["client"], row["id"]), row["canonical_target"], row["text"]
                    )
                finally:
                    PUBLICATION_CREATED.reset(created_token)
            if not isinstance(result, dict) or result.get("state") not in {"sent", "unknown", "rejected"}:
                self._finish(row, "unknown", error="delivery_unknown")
                return
            state = result["state"]
            message_id = self._safe_id(result.get("message_id"))
            if state == "sent" and message_id is None:
                state = "unknown"  # No verifiable receipt is not proof of delivery.
            self._finish(
                row,
                state,
                message_id=message_id,
                account=self._safe_id(result.get("account"), account=True),
                error=None if state == "sent" else "delivery_" + state,
            )
        except NoRoute as exc:
            # This explicit contract is the ONLY exception permitted to undo sending.
            # Share route backoff across queued IDs so a full queue cannot cause
            # a storm of read-only route probes while its destination is offline.
            self._route_retry[(row["client"], row["canonical_target"])] = (
                time.monotonic() + self.ROUTE_BACKOFF
            )
            with self._db:
                self._db.execute("DELETE FROM attempts WHERE attempt_id=?", (attempt,))
                self._db.execute(
                    "UPDATE publications SET status='pending',retry_at=?,error='route_unavailable' "
                    "WHERE client=? AND id=?",
                    (
                        max(self._clock() + self.NO_ROUTE_BACKOFF, exc.next_at)
                        if isinstance(exc, Deferred)
                        else self._clock() + self.NO_ROUTE_BACKOFF,
                        row["client"],
                        row["id"],
                    ),
                )
        except asyncio.CancelledError:
            self._finish(row, "unknown", error="delivery_unknown")
            raise
        except Exception:
            self._finish(row, "unknown", error="delivery_unknown")
