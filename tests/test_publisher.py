"""Offline tests: real loopback HTTP, fake delivery, no QQ or Hermes imports."""

import asyncio
import json
import socket
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from datetime import datetime
from pathlib import Path

import aiohttp
import pytest

from hermes_onebot_fleet.publisher import (
    NoRoute,
    Publisher,
    PublisherClient,
    PublisherQuietHours,
    PublisherSettings,
)

TOKEN_A = "publisher-only-A-" + "a" * 40
TOKEN_B = "publisher-only-B-" + "b" * 40
TARGET = "obf:test:group:2001"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def client_mapping(identifier="source", token_env="PUB_A"):
    return {
        "id": identifier,
        "token_env": token_env,
        "targets": {"news": TARGET, "same-place": TARGET},
    }


def shanghai_timestamp(local_time, day="2026-10-19"):
    return datetime.fromisoformat(f"{day}T{local_time}+08:00").timestamp()


@pytest.mark.parametrize(
    "start,end,at,expected",
    [
        ("22:00", "08:00", "21:59:59.999", False),
        ("22:00", "08:00", "22:00:00", True),
        ("22:00", "08:00", "23:59:59", True),
        ("22:00", "08:00", "00:00:00", True),
        ("22:00", "08:00", "07:59:59.999", True),
        ("22:00", "08:00", "08:00:00", False),
        ("22:00", "08:00", "12:00:00", False),
        ("12:00", "13:00", "11:59:59.999", False),
        ("12:00", "13:00", "12:00:00", True),
        ("12:00", "13:00", "12:59:59.999", True),
        ("12:00", "13:00", "13:00:00", False),
        ("12:00", "13:00", "00:00:00", False),
        ("00:00", "08:00", "00:00:00", True),
        ("22:00", "00:00", "00:00:00", False),
    ],
)
def test_quiet_hours_fixed_shanghai_boundaries(start, end, at, expected):
    policy = PublisherQuietHours(start, end, "Asia/Shanghai")
    assert policy.is_quiet(shanghai_timestamp(at)) is expected


@pytest.mark.parametrize(
    "policy",
    [
        None,
        False,
        [],
        "22:00-08:00",
        {},
        {"start": "22:00", "end": "08:00"},
        {"start": "22:00", "end": "08:00", "time_zone": "Asia/Shanghai", "extra": True},
        *[
            {"start": start, "end": end, "time_zone": zone}
            for start, end, zone in [
                ("22:00", "22:00", "Asia/Shanghai"),
                ("2:00", "08:00", "Asia/Shanghai"),
                ("24:00", "08:00", "Asia/Shanghai"),
                ("22:60", "08:00", "Asia/Shanghai"),
                ("22:00:00", "08:00", "Asia/Shanghai"),
                ("22:00", "8:00", "Asia/Shanghai"),
                ("22:00", "08:00\n", "Asia/Shanghai"),
                (2200, "08:00", "Asia/Shanghai"),
                ("22:00", None, "Asia/Shanghai"),
                ("22:00", "08:00", "UTC"),
                ("22:00", "08:00", "Asia/Chongqing"),
                ("22:00", "08:00", "+08:00"),
                ("22:00", "08:00", None),
                ("22:00", "08:00", []),
            ]
        ],
    ],
)
def test_quiet_hours_reject_invalid_policy_without_timezone_fallback(policy):
    with pytest.raises(ValueError):
        PublisherSettings.from_mapping({"clients": [dict(client_mapping(), quiet_hours=policy)]})


def test_quiet_hours_optional_and_mapping_is_copied():
    assert PublisherSettings.from_mapping({"clients": [client_mapping()]}).clients[0].quiet_hours is None
    policy = {"start": "22:00", "end": "08:00", "time_zone": "Asia/Shanghai"}
    settings = PublisherSettings.from_mapping({"clients": [dict(client_mapping(), quiet_hours=policy)]})
    policy["start"] = "01:00"
    assert settings.clients[0].quiet_hours == PublisherQuietHours("22:00", "08:00", "Asia/Shanghai")
    assert settings.clients[0].quiet_hours.seconds_until_start(shanghai_timestamp("21:59:30")) == 30
    with pytest.raises(ValueError):
        PublisherClient("source", "PUB_A", {"news": TARGET}, quiet_hours={})


def test_settings_defaults_and_secret_capture():
    assert PublisherSettings.from_mapping({}) == PublisherSettings()
    settings = PublisherSettings.from_mapping(
        {
            "enabled": True,
            "port": 18888,
            "clients": [client_mapping()],
        }
    )
    assert settings.clients[0].max_per_hour == 6
    assert settings.clients[0].resolve_token({"PUB_A": TOKEN_A}) == TOKEN_A
    assert TOKEN_A not in repr(settings)
    with pytest.raises(TypeError):
        settings.clients[0].targets["extra"] = TARGET
    for token in [None, "x" * 31, "x" * 32 + " ", "x" * 32 + "\n", "x" * 32 + "\x00"]:
        with pytest.raises(ValueError):
            settings.clients[0].resolve_token({"PUB_A": token})
    duplicate = PublisherSettings(
        clients=(
            PublisherClient("one", "PUB_A", {"news": TARGET}),
            PublisherClient("two", "PUB_B", {"news": TARGET}),
        )
    )
    with pytest.raises(ValueError):
        Publisher(duplicate, "unused.sqlite3", {"PUB_A": TOKEN_A, "PUB_B": TOKEN_A}, None)


@pytest.mark.parametrize(
    "mapping",
    [
        None,
        [],
        {"host": "0.0.0.0"},
        {"enabled": "false"},
        {"enabled": True},
        {"enabled": True, "port": 18888},
        {"port": 0},
        {"port": 1023},
        {"port": 65536},
        {"port": True},
        {"clients": {}},
        {"clients": [None]},
        {"clients": [dict(client_mapping(), extra=True)]},
        {"clients": [dict(client_mapping(), id="source/path")]},
        {"clients": [dict(client_mapping(), id="source?token=x")]},
        {"clients": [dict(client_mapping(), id="")]},
        {"clients": [client_mapping(), client_mapping()]},
        {"clients": [dict(client_mapping(), token_env="PUB=VALUE")]},
        {"clients": [dict(client_mapping(), targets={})]},
        {"clients": [dict(client_mapping(), targets={"news?token=x": TARGET})]},
        {"clients": [dict(client_mapping(), targets={"news": TARGET + "?token=x"})]},
        {"clients": [dict(client_mapping(), targets={"news": "obf:test:group:0"})]},
        {"clients": [dict(client_mapping(), targets={"news": "https://127.0.0.1/"})]},
        {"clients": [dict(client_mapping(), max_per_hour=0)]},
        {"clients": [dict(client_mapping(), max_per_hour=61)]},
        {"clients": [dict(client_mapping(), max_per_hour=True)]},
    ],
)
def test_settings_reject_invalid_capabilities(mapping):
    with pytest.raises(ValueError):
        PublisherSettings.from_mapping(mapping)


class PublisherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "publications.sqlite3"
        self.port = free_port()
        self.settings = PublisherSettings.from_mapping(
            {
                "enabled": True,
                "port": self.port,
                "clients": [client_mapping(), client_mapping("other", "PUB_B")],
            }
        )
        self.secrets = {"PUB_A": TOKEN_A, "PUB_B": TOKEN_B}
        self.calls = []
        self.delivery_impl = None
        self.publisher = Publisher(self.settings, self.path, self.secrets, self.deliver)
        await self.publisher.start()
        self.http = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.publisher.stop()
        await self.http.close()
        self.directory.cleanup()

    async def deliver(self, key, target, text):
        self.calls.append((key, target, text))
        if self.delivery_impl is not None:
            return await self.delivery_impl(key, target, text)
        return {"state": "sent", "message_id": "42", "account": "1001"}

    def payload(self, identifier="item-1", **overrides):
        body = {"publication_id": identifier, "target": "news", "text": "publication secret text"}
        body.update(overrides)
        return body

    async def request(self, method, path, *, token=TOKEN_A, headers=None, **kwargs):
        supplied = {} if headers is None else dict(headers)
        if token is not None:
            supplied["Authorization"] = "Bearer " + token
        async with self.http.request(
            method, f"http://127.0.0.1:{self.port}" + path, headers=supplied, **kwargs
        ) as response:
            body = await response.json()
            self.assertNotIn(TOKEN_A, json.dumps(body))
            self.assertNotIn(TOKEN_B, json.dumps(body))
            self.assertNotIn("publication secret text", json.dumps(body))
            self.assertNotIn("Access-Control-Allow-Origin", response.headers)
            return response.status, body

    async def post(self, identifier="item-1", *, token=TOKEN_A, **overrides):
        return await self.request(
            "POST", "/v1/publications", token=token, json=self.payload(identifier, **overrides)
        )

    async def status(self, identifier="item-1", *, token=TOKEN_A):
        code, body = await self.request("GET", "/v1/publications/" + identifier, token=token)
        self.assertEqual(code, 200)
        return body

    async def wait_state(self, state, identifier="item-1", *, token=TOKEN_A):
        async with asyncio.timeout(4):
            while True:
                body = await self.status(identifier, token=token)
                if body["status"] == state:
                    return body
                await asyncio.sleep(0.01)

    async def restart(self, settings=None, *, clock=None):
        await self.publisher.stop()
        self.settings = settings or self.settings
        self.publisher = Publisher(self.settings, self.path, self.secrets, self.deliver, clock=clock)
        await self.publisher.start()

    async def configure_quiet(self, at, *, start="22:00", end="08:00", max_per_hour=6):
        self.now = shanghai_timestamp(at)
        settings = PublisherSettings.from_mapping(
            {
                "enabled": True,
                "port": self.port,
                "clients": [
                    dict(
                        client_mapping(),
                        max_per_hour=max_per_hour,
                        quiet_hours={
                            "start": start,
                            "end": end,
                            "time_zone": "Asia/Shanghai",
                        },
                    ),
                    client_mapping("other", "PUB_B"),
                ],
            }
        )
        await self.restart(settings, clock=lambda: self.now)

    async def test_quiet_intake_pending_scoped_reconciliation_and_no_starvation(self):
        await self.configure_quiet("22:00:00")
        for index in range(8):
            code, body = await self.post(f"quiet-{index}")
            self.assertEqual((code, body["status"]), (202, "pending"))
        self.assertEqual((await self.post("quiet-0"))[1]["status"], "pending")
        self.assertEqual((await self.post("quiet-0", text="different"))[0], 409)
        self.assertEqual((await self.post("bad-target", target="denied"))[0], 403)
        self.assertEqual((await self.post("awake", token=TOKEN_B))[0], 202)
        await self.wait_state("sent", "awake", token=TOKEN_B)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][0], Publisher.stable_key("other", "awake"))
        code, body = await self.request("GET", "/v1/status")
        self.assertEqual((code, body["counts"]["pending"]), (200, 8))
        self.assertEqual((await self.status("quiet-0"))["status"], "pending")
        self.assertEqual(
            self.publisher._db.execute("SELECT COUNT(*) FROM attempts WHERE client='source'").fetchone()[0], 0
        )
        self.now = shanghai_timestamp("08:00:00", "2026-10-20")
        self.publisher._wake.set()
        await self.wait_state("sent", "quiet-0")

    async def test_no_route_pending_into_quiet_and_restart_never_dispatches(self):
        await self.configure_quiet("21:59:00")
        self.publisher.NO_ROUTE_BACKOFF = self.publisher.ROUTE_BACKOFF = 0

        async def offline(*_):
            raise NoRoute()

        self.delivery_impl = offline
        await self.post()
        async with asyncio.timeout(2):
            while not self.calls:
                await asyncio.sleep(0.01)
        self.assertEqual((await self.status())["status"], "pending")
        self.now = shanghai_timestamp("22:00:00")
        self.delivery_impl = None
        self.publisher._wake.set()
        await self.post("other-before-restart", token=TOKEN_B)
        await self.wait_state("sent", "other-before-restart", token=TOKEN_B)
        self.assertEqual(len(self.calls), 2)  # One safe offline probe, one other-client send.
        await self.restart(clock=lambda: self.now)
        await self.post("other-after-restart", token=TOKEN_B)
        await self.wait_state("sent", "other-after-restart", token=TOKEN_B)
        self.assertEqual((await self.status())["status"], "pending")
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(
            self.publisher._db.execute("SELECT COUNT(*) FROM attempts WHERE client='source'").fetchone()[0], 0
        )
        self.now = shanghai_timestamp("08:00:00", "2026-10-20")
        self.publisher._wake.set()
        await self.wait_state("sent")
        self.assertEqual(len(self.calls), 4)
        self.assertEqual(self.calls[0], self.calls[-1])

    async def test_nonwrapping_quiet_window_drains_at_exact_end(self):
        await self.configure_quiet("12:00:00", start="12:00", end="13:00")
        await self.post()
        await self.post("awake", token=TOKEN_B)
        await self.wait_state("sent", "awake", token=TOKEN_B)
        self.assertEqual((await self.status())["status"], "pending")
        self.assertEqual(len(self.calls), 1)
        self.now = shanghai_timestamp("13:00:00")
        self.publisher._wake.set()
        await self.wait_state("sent")
        self.assertEqual(len(self.calls), 2)

    async def test_quiet_hours_preserve_persisted_rate_cap_and_ttl(self):
        await self.configure_quiet("11:59:00", start="12:00", end="12:30", max_per_hour=1)
        await self.post()
        await self.wait_state("sent")
        self.now = shanghai_timestamp("12:00:00")
        self.assertEqual((await self.post("rate-denied"))[0], 429)
        self.assertEqual((await self.post())[1]["status"], "sent")
        with self.publisher._db:
            self.publisher._db.execute(
                "INSERT INTO publications(client,id,payload_sha,target_alias,canonical_target,text,status,created) "
                "VALUES('source','queued','seed','news',?,'queued','pending',?)",
                (TARGET, self.now),
            )
        await self.restart(clock=lambda: self.now)
        self.assertEqual((await self.status("queued"))["status"], "pending")
        self.assertEqual((await self.post("still-rate-denied"))[0], 429)
        self.now = shanghai_timestamp("12:30:00")
        self.publisher._wake.set()
        await self.post("rate-barrier", token=TOKEN_B)
        await self.wait_state("sent", "rate-barrier", token=TOKEN_B)
        self.assertEqual((await self.status("queued"))["status"], "pending")
        self.now = shanghai_timestamp("13:00:00")
        self.publisher._wake.set()
        await self.wait_state("sent", "queued")
        self.assertEqual(len(self.calls), 3)
        # Payload expiry still runs in quiet hours; no send is needed to expire.
        self.now = shanghai_timestamp("12:00:00", "2026-10-21")
        with self.publisher._db:
            self.publisher._db.execute(
                "UPDATE publications SET status='pending',created=? WHERE id='queued'",
                (self.now - 86401,),
            )
        self.assertEqual((await self.status("queued"))["status"], "expired")
        row = self.publisher._db.execute(
            "SELECT text,canonical_target FROM publications WHERE id='queued'"
        ).fetchone()
        self.assertEqual(tuple(row), (None, None))
        self.assertEqual(len(self.calls), 3)

    async def test_dispatch_rechecks_quiet_before_claim_and_after_commit(self):
        await self.configure_quiet("22:00:00")
        await self.post()
        row = self.publisher._db.execute("SELECT * FROM publications").fetchone()
        await self.publisher._dispatch(row)
        self.assertEqual(self.calls, [])
        outside, inside = shanghai_timestamp("21:59:59"), self.now
        times = iter([outside, outside, outside, inside])
        self.publisher._clock = lambda: next(times, inside)
        await self.publisher._dispatch(row)
        self.publisher._clock = lambda: self.now
        self.assertEqual((await self.status())["status"], "pending")
        self.assertEqual(self.calls, [])
        self.assertEqual(self.publisher._db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0], 0)

    async def test_inflight_callback_is_cancelled_at_next_quiet_boundary(self):
        await self.configure_quiet("21:59:59.95")
        cancelled = asyncio.Event()

        async def preflight_wait(*_):
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        self.delivery_impl = preflight_wait
        await self.post()
        body = await self.wait_state("unknown")
        self.assertEqual(body["error"], "delivery_unknown")
        self.assertTrue(cancelled.is_set())
        self.now = shanghai_timestamp("22:00:00")
        self.assertEqual((await self.post())[1]["status"], "unknown")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.publisher._db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0], 1)

    async def test_disabled_no_resources_and_captured_tokens(self):
        disabled_path = self.path.with_name("disabled.sqlite3")
        disabled = Publisher(PublisherSettings(), disabled_path, {}, self.deliver)
        await disabled.start()
        self.assertFalse(disabled_path.exists())
        self.assertIsNone(disabled._worker)
        await disabled.stop()
        # Mutating the caller mapping cannot change captured server credentials.
        self.secrets["PUB_A"] = "changed-" + "c" * 40
        self.assertEqual((await self.request("GET", "/v1/status"))[0], 200)
        await self.publisher.start()  # Idempotent lifecycle.

    async def test_stable_duplicate_conflict_and_canonical_payload_hash(self):
        code, body = await self.post()
        self.assertEqual(code, 202)
        self.assertEqual(body["status"], "pending")
        sent = await self.wait_state("sent")
        self.assertEqual(sent["message_id"], "42")
        self.assertEqual(sent["account"], "1001")
        for _ in range(3):
            duplicate_code, duplicate = await self.post(target="same-place")
            self.assertEqual((duplicate_code, duplicate), (202, sent))
        self.assertEqual((await self.post(text="different"))[0], 409)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(
            self.calls[0],
            (
                Publisher.stable_key("source", "item-1"),
                TARGET,
                "publication secret text",
            ),
        )
        await self.restart()
        self.assertEqual((await self.post())[1]["status"], "sent")
        self.assertEqual(len(self.calls), 1)

    async def test_client_scoped_identity_status_and_auth_all_paths(self):
        await self.post()
        await self.wait_state("sent")
        code, body = await self.request("GET", "/v1/publications/item-1", token=TOKEN_B)
        self.assertEqual(code, 404)
        self.assertEqual((await self.post(token=TOKEN_B))[0], 202)
        await self.wait_state("sent", token=TOKEN_B)
        self.assertEqual(len(self.calls), 2)
        self.assertNotEqual(self.calls[0][0], self.calls[1][0])
        for token, identifier in [(TOKEN_A, "source"), (TOKEN_B, "other")]:
            code, body = await self.request("GET", "/v1/status", token=token)
            self.assertEqual(code, 200)
            self.assertEqual(body["client"], identifier)
            self.assertEqual(body["counts"]["sent"], 1)
            self.assertEqual(sum(body["counts"].values()), 1)
        for path in ["/v1/status", "/v1/publications/item-1", "/missing", "/client/source", "/"]:
            for method in ["GET", "POST", "OPTIONS"]:
                self.assertEqual((await self.request(method, path, token=None))[0], 401)
        self.assertEqual((await self.request("GET", "/v1/status", token="wrong"))[0], 401)
        self.assertEqual((await self.request("GET", "/v1/status?token=" + TOKEN_A, token=None))[0], 401)
        self.assertEqual((await self.request("GET", "/v1/status?client=other"))[0], 400)
        self.assertEqual((await self.request("GET", "/missing"))[0], 404)

    async def test_host_binding_and_no_cors(self):
        for host in [
            "evil.example",
            f"evil.example:{self.port}",
            "localhost",
            "127.0.0.1",
            "[::1]",
            "0.0.0.0",
        ]:
            self.assertEqual((await self.request("GET", "/v1/status", headers={"Host": host}))[0], 403)
        self.assertEqual(
            (await self.request("GET", "/v1/status", headers={"Host": f"localhost:{self.port}"}))[0], 200
        )
        for origin in ["https://evil.example", f"http://127.0.0.1:{self.port}", "null", ""]:
            self.assertEqual((await self.request("GET", "/v1/status", headers={"Origin": origin}))[0], 403)
        sockets = self.publisher._runner.sites.copy().pop()._server.sockets
        self.assertEqual({s.getsockname()[0] for s in sockets}, {"127.0.0.1"})

    async def test_target_denial_unknown_fields_and_payload_bounds(self):
        self.assertEqual((await self.post(target="not-authorized"))[0], 403)
        self.assertEqual((await self.post(target=TARGET))[0], 400)
        for identifier in ["", "a" * 161, "bad/id", "bad?token=x", "bad id"]:
            self.assertEqual((await self.post(identifier))[0], 400)
        for text in ["", "x" * 1801, None, 123, "\ud800"]:
            self.assertEqual((await self.post(text=text))[0], 400)
        self.assertEqual((await self.post(extra="forbidden"))[0], 400)
        for body in ["null", "[]", "{}", "{", '{"text":"x","text":"y"}', '"x"']:
            self.assertEqual(
                (
                    await self.request(
                        "POST", "/v1/publications", data=body, headers={"Content-Type": "application/json"}
                    )
                )[0],
                400,
            )
        self.assertEqual((await self.request("POST", "/v1/publications", data="{}"))[0], 415)
        self.assertEqual(
            (
                await self.request(
                    "POST",
                    "/v1/publications",
                    data=b"x" * 16385,
                    headers={"Content-Type": "application/json"},
                )
            )[0],
            413,
        )
        exact = json.dumps(self.payload("exact-limit")).encode()
        exact += b" " * (16384 - len(exact))
        self.assertEqual(
            (
                await self.request(
                    "POST", "/v1/publications", data=exact, headers={"Content-Type": "application/json"}
                )
            )[0],
            202,
        )
        self.assertEqual((await self.post("max-id-" + "x" * 153, text="文" * 1800))[0], 202)
        self.assertEqual((await self.post("min-text", text="x"))[0], 202)

    async def test_chunked_payload_limit(self):
        async def chunks():
            for _ in range(5):
                yield b" " * 4096
                await asyncio.sleep(0)

        self.assertEqual(
            (
                await self.request(
                    "POST", "/v1/publications", data=chunks(), headers={"Content-Type": "application/json"}
                )
            )[0],
            413,
        )

    async def test_lost_ack_exception_unknown_never_replayed(self):
        async def lost_ack(*_):
            raise RuntimeError("publication secret text " + TOKEN_A)

        self.delivery_impl = lost_ack
        await self.post()
        body = await self.wait_state("unknown")
        self.assertEqual(body["error"], "delivery_unknown")
        self.assertEqual((await self.post())[1]["status"], "unknown")
        await self.restart()
        self.assertEqual((await self.status())["status"], "unknown")
        self.assertEqual(len(self.calls), 1)

    async def test_callback_rejected_unknown_and_invalid_results_redacted(self):
        results = [
            {"state": "unknown", "error": TOKEN_A, "message_id": TOKEN_A, "account": TOKEN_A},
            {"state": "rejected", "error": "publication secret text"},
            {"state": "made-up"},
            None,
        ]
        for index, result in enumerate(results):

            async def outcome(*_, value=result):
                return value

            self.delivery_impl = outcome
            identifier = f"result-{index}"
            await self.post(identifier)
            state = "rejected" if index == 1 else "unknown"
            body = await self.wait_state(state, identifier)
            self.assertEqual(body["error"], "delivery_" + state)
            self.assertIsNone(body["message_id"])
            self.assertIsNone(body["account"])

    async def test_restart_sending_recovery_without_replay(self):
        await self.post()
        await self.wait_state("sent")
        await self.publisher.stop()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE publications SET status='sending'")
        await self.restart()
        body = await self.status()
        self.assertEqual(body["status"], "unknown")
        self.assertEqual(body["error"], "delivery_unknown")
        self.assertEqual((await self.post())[1]["status"], "unknown")
        self.assertEqual(len(self.calls), 1)

    async def test_stop_cancels_sending_records_unknown_closes_resources(self):
        entered = asyncio.Event()
        cancelled = asyncio.Event()

        async def hang(*_):
            entered.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        self.delivery_impl = hang
        await self.post()
        await asyncio.wait_for(entered.wait(), 2)
        self.assertEqual((await self.status())["status"], "sending")
        task = self.publisher._worker
        await self.publisher.stop()
        self.assertTrue(cancelled.is_set())
        self.assertTrue(task.done())
        self.assertIsNone(self.publisher._db)
        self.assertIsNone(self.publisher._runner)
        await self.publisher.stop()
        await self.restart()
        self.assertEqual((await self.status())["status"], "unknown")
        self.assertEqual(len(self.calls), 1)

    async def test_no_route_reconnect_is_safe_and_refunds_rate_attempt(self):
        settings = PublisherSettings(
            True, self.port, (PublisherClient("source", "PUB_A", {"news": TARGET}, max_per_hour=1),)
        )
        await self.restart(settings)
        unavailable = asyncio.Event()

        async def reconnect(*_):
            if len(self.calls) == 1:
                unavailable.set()
                raise NoRoute(TOKEN_A)
            return {"state": "sent", "message_id": 43, "account": 1001}

        self.delivery_impl = reconnect
        await self.post()
        await asyncio.wait_for(unavailable.wait(), 2)
        self.assertEqual((await self.status())["status"], "pending")
        self.assertEqual(self.publisher._db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0], 0)
        body = await self.wait_state("sent")
        self.assertEqual(body["message_id"], "43")
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0], self.calls[1])
        self.assertEqual(self.publisher._db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0], 1)

    async def test_persisted_rate_cap_counts_unknown_and_gates_queued_work(self):
        settings = PublisherSettings(
            True, self.port, (PublisherClient("source", "PUB_A", {"news": TARGET}, max_per_hour=1),)
        )
        await self.restart(settings)
        entered, release = asyncio.Event(), asyncio.Event()

        async def uncertain(*_):
            entered.set()
            await release.wait()
            return {"state": "unknown", "error": TOKEN_A}

        self.delivery_impl = uncertain
        await self.post()
        await asyncio.wait_for(entered.wait(), 2)
        # Already-marked sending counts against admission, too.
        self.assertEqual((await self.post("rate-denied"))[0], 429)
        # Seed an already-enqueued item to exercise the worker independently of admission.
        with self.publisher._db:
            self.publisher._db.execute(
                "INSERT INTO publications(client,id,payload_sha,target_alias,canonical_target,text,status,created) "
                "VALUES('source','queued','seed','news',?,'queued text','pending',?)",
                (TARGET, time.time()),
            )
        release.set()
        await self.wait_state("unknown")
        self.assertEqual((await self.post())[0], 202)  # Duplicate bypasses rate cap.
        self.assertEqual((await self.post("rate-denied"))[0], 429)
        await self.restart()
        self.assertEqual((await self.status("queued"))["status"], "pending")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual((await self.post("rate-denied"))[0], 429)
        self.delivery_impl = None
        with self.publisher._db:
            self.publisher._db.execute("UPDATE attempts SET attempted=?", (time.time() - 3601,))
        self.publisher._wake.set()
        await self.wait_state("sent", "queued")
        self.assertEqual(len(self.calls), 2)

    async def test_payload_ttl_scrubs_payload_retains_duplicate_tombstone(self):
        async def no_route(*_):
            raise NoRoute()

        self.delivery_impl = no_route
        await self.post()
        await asyncio.sleep(0.02)
        with self.publisher._db:
            self.publisher._db.execute("UPDATE publications SET created=?", (time.time() - 86401,))
        body = await self.status()
        self.assertEqual(body["status"], "expired")
        self.assertEqual(body["error"], "payload_expired")
        row = self.publisher._db.execute("SELECT * FROM publications").fetchone()
        self.assertIsNone(row["text"])
        self.assertIsNone(row["canonical_target"])
        calls = len(self.calls)
        self.assertEqual((await self.post())[1]["status"], "expired")
        self.assertEqual((await self.post(text="changed"))[0], 409)
        await self.restart()
        self.assertEqual((await self.post())[1]["status"], "expired")
        self.assertEqual(len(self.calls), calls)

    async def test_queue_bound_per_client_and_recheck_target_on_restart(self):
        with self.publisher._db:
            self.publisher._db.executemany(
                "INSERT INTO publications(client,id,payload_sha,target_alias,canonical_target,text,status,created,retry_at) "
                "VALUES('source',?,'seed','news',?,'queued','pending',?,?)",
                [(f"queue-{i}", TARGET, time.time(), time.time() + 3600) for i in range(1000)],
            )
        self.assertEqual((await self.post())[0], 429)
        self.assertEqual((await self.post(token=TOKEN_B))[0], 202)
        await self.wait_state("sent", token=TOKEN_B)
        await self.publisher.stop()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE publications SET retry_at=0 WHERE client='source'")
        settings = PublisherSettings(
            True, self.port, (PublisherClient("source", "PUB_A", {"news": "obf:test:group:9999"}),)
        )
        await self.restart(settings)
        await self.wait_state("rejected", "queue-0")
        self.assertEqual(len(self.calls), 1)  # No queued payload retargeting.

    async def test_missing_or_invalid_sent_receipt_is_unknown_without_retry(self):
        for index, receipt in enumerate([None, "", True, "bad receipt", TOKEN_A, []]):

            async def invalid_receipt(*_, value=receipt):
                return {"state": "sent", "message_id": value}

            self.delivery_impl = invalid_receipt
            identifier = f"bad-receipt-{index}"
            await self.post(identifier)
            body = await self.wait_state("unknown", identifier)
            self.assertEqual(body["error"], "delivery_unknown")
            self.assertIsNone(body["message_id"])
            self.assertEqual((await self.post(identifier))[1]["status"], "unknown")
        self.assertEqual(len(self.calls), 6)

    async def test_same_database_exclusivity_does_not_recover_live_sending(self):
        entered = asyncio.Event()

        async def hang(*_):
            entered.set()
            await asyncio.Future()

        self.delivery_impl = hang
        await self.post()
        await asyncio.wait_for(entered.wait(), 2)
        settings = PublisherSettings(True, free_port(), self.settings.clients)
        other = Publisher(settings, self.path, self.secrets, self.deliver)
        try:
            with self.assertRaises(RuntimeError):
                await other.start()
            self.assertIsNone(other._db)
            self.assertIsNone(other._runner)
            self.assertIsNone(other._lock.file)
            self.assertEqual((await self.status())["status"], "sending")
            self.assertEqual(len(self.calls), 1)
        finally:
            await other.stop()
        await self.publisher.stop()
        self.assertIsNone(self.publisher._lock.file)
        await other.start()  # The stopped owner released its lock.
        try:
            row = other._db.execute("SELECT status FROM publications").fetchone()
            self.assertEqual(row["status"], "unknown")
            self.assertEqual(len(self.calls), 1)
        finally:
            await other.stop()

    async def test_dispatch_compare_and_swap_rejects_stale_expired_snapshot(self):
        async def no_route(*_):
            raise NoRoute()

        self.delivery_impl = no_route
        await self.post()
        async with asyncio.timeout(2):
            while not self.calls:
                await asyncio.sleep(0.01)
        stale = self.publisher._db.execute("SELECT * FROM publications").fetchone()
        calls = len(self.calls)
        with self.publisher._db:
            self.publisher._db.execute("UPDATE publications SET created=?", (time.time() - 86401,))
        await self.publisher._dispatch(stale)
        self.assertEqual(len(self.calls), calls)
        self.assertEqual(self.publisher._db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0], 0)
        self.assertEqual((await self.status())["status"], "expired")
        with self.publisher._db:
            self.publisher._db.execute(
                "UPDATE publications SET status='unknown',created=?,canonical_target=?,text='stale'",
                (time.time(), TARGET),
            )
        await self.publisher._dispatch(stale)
        self.assertEqual(len(self.calls), calls)
        self.assertEqual((await self.status())["status"], "unknown")

    async def test_stalled_partial_body_shutdown_is_bounded(self):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        writer.write(
            (
                "POST /v1/publications HTTP/1.1\r\n"
                f"Host: 127.0.0.1:{self.port}\r\nAuthorization: Bearer {TOKEN_A}\r\n"
                "Content-Type: application/json\r\nContent-Length: 1000\r\n\r\n{"
            ).encode()
        )
        await writer.drain()
        await asyncio.sleep(0.02)
        try:
            async with asyncio.timeout(2):
                await self.publisher.stop()
            self.assertIsNone(self.publisher._db)
            self.assertIsNone(self.publisher._lock.file)
            self.assertEqual(self.calls, [])
            self.assertEqual(await reader.read(), b"")
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_stalled_body_times_out_without_enqueue(self):
        self.publisher.BODY_TIMEOUT = 0.05
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        writer.write(
            (
                "POST /v1/publications HTTP/1.1\r\n"
                f"Host: 127.0.0.1:{self.port}\r\nAuthorization: Bearer {TOKEN_A}\r\n"
                "Content-Type: application/json\r\nContent-Length: 1000\r\n\r\n{"
            ).encode()
        )
        await writer.drain()
        try:
            async with asyncio.timeout(2):
                response = await reader.readuntil(b"\r\n\r\n")
            self.assertTrue(response.startswith(b"HTTP/1.1 408 "))
            self.assertEqual(self.calls, [])
            self.assertEqual(self.publisher._db.execute("SELECT COUNT(*) FROM publications").fetchone()[0], 0)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_callback_timeout_becomes_unknown_without_replay(self):
        self.publisher.DELIVERY_TIMEOUT = 0.05
        cancelled = asyncio.Event()

        async def hung(*_):
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        self.delivery_impl = hung
        await self.post()
        body = await self.wait_state("unknown")
        self.assertEqual(body["error"], "delivery_unknown")
        self.assertTrue(cancelled.is_set())
        self.assertEqual((await self.post())[1]["status"], "unknown")
        self.assertEqual(len(self.calls), 1)

    async def test_no_route_backoff_is_shared_without_other_client_starvation(self):
        self.publisher.ROUTE_BACKOFF = 60
        other_key = Publisher.stable_key("other", "other-item")

        async def offline_source(key, *_):
            if key == other_key:
                return {"state": "sent", "message_id": "44"}
            raise NoRoute()

        self.delivery_impl = offline_source
        for index in range(20):
            self.assertEqual((await self.post(f"offline-{index}"))[0], 202)
        self.assertEqual((await self.post("other-item", token=TOKEN_B))[0], 202)
        await self.wait_state("sent", "other-item", token=TOKEN_B)
        self.assertEqual(len(self.calls), 2)
        code, body = await self.request("GET", "/v1/status")
        self.assertEqual(code, 200)
        self.assertEqual(body["counts"]["pending"], 20)

    async def test_start_failure_closes_database_and_http_runner(self):
        other = Publisher(self.settings, self.path.with_name("other.sqlite3"), self.secrets, self.deliver)
        with self.assertRaises(OSError):
            await other.start()
        self.assertIsNone(other._db)
        self.assertIsNone(other._runner)
        self.assertIsNone(other._worker)
        await other.stop()
