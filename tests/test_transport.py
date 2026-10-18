"""Offline integration tests against an authenticated local aiohttp WS server."""

import asyncio
import json
import unittest

from aiohttp import web

from hermes_onebot_fleet.config import Account
from hermes_onebot_fleet.transport import ActionError, ActionUncertain, OneBotConnection

TOKEN = "test-only-token-123456789"


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sockets = []
        self.requests = []
        self.headers = []
        self.events = asyncio.Queue()
        self.login_id = 1001
        self.online = True
        self.early_event = False
        self.emit_during_probe = False
        self.seen = asyncio.Event()
        app = web.Application()
        app.router.add_get("/", self.handle)
        self.server = web.AppRunner(app)
        await self.server.setup()
        self.site = web.TCPSite(self.server, "127.0.0.1", 0)
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]
        self.account = Account("1001", f"ws://127.0.0.1:{port}/", "ONEBOT_FLEET_TEST_TOKEN")
        self.connection = OneBotConnection(self.account, TOKEN, self.events.put)
        self.connection.BACKOFF_INITIAL = 0.01
        self.connection.BACKOFF_MAX = 0.03

    async def asyncTearDown(self):
        await self.connection.stop()
        for ws in self.sockets:
            await ws.close()
        await self.server.cleanup()

    async def handle(self, request):
        auth = request.headers.get("Authorization")
        self.headers.append(auth)
        if auth != "Bearer " + TOKEN:
            raise web.HTTPUnauthorized()
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.sockets.append(ws)
        if self.early_event:
            await ws.send_json(self.message())
        async for frame in ws:
            if frame.type != web.WSMsgType.TEXT:
                continue
            payload = json.loads(frame.data)
            self.requests.append(payload)
            action = payload["action"]
            if action == "get_login_info":
                if self.emit_during_probe:
                    await ws.send_json(self.message(marker="during-probe"))
                data = {"user_id": self.login_id}
            elif action == "get_status":
                data = {"online": self.online}
            elif action in {"hang", "disconnect"}:
                self.seen.set()
                if action == "disconnect":
                    await ws.close()
                continue
            elif action == "fail":
                await ws.send_json(
                    {"echo": payload["echo"], "status": "failed", "retcode": 1404, "message": TOKEN}
                )  # remote secrets must not enter exception text
                continue
            else:
                data = payload["params"]
            await ws.send_json({"echo": payload["echo"], "status": "ok", "retcode": 0, "data": data})
        return ws

    @staticmethod
    def message(**overrides):
        value = {"post_type": "message", "self_id": 1001, "user_id": 2001, "message": []}
        value.update(overrides)
        return value

    async def test_connect_auth_health_rpc_unique_echoes(self):
        await self.connection.connect()
        self.assertTrue(self.connection.healthy)
        self.assertTrue(self.connection.connected)
        self.assertEqual(self.headers, ["Bearer " + TOKEN])
        values = await asyncio.gather(*(self.connection.rpc("test", {"i": i}) for i in range(10)))
        self.assertEqual(values, [{"i": i} for i in range(10)])
        echoes = [request["echo"] for request in self.requests]
        self.assertEqual(len(echoes), len(set(echoes)))
        self.assertNotIn(TOKEN, repr(self.connection))
        self.assertEqual([r["action"] for r in self.requests[:2]], ["get_login_info", "get_status"])

    async def test_periodic_health_probe_does_not_drop_verified_events(self):
        await self.connection.connect()
        self.emit_during_probe = True
        self.assertTrue(await self.connection.check_health())
        event = await asyncio.wait_for(self.events.get(), 1)
        self.assertEqual(event["marker"], "during-probe")
        self.assertTrue(self.connection.healthy)
        self.emit_during_probe = False
        self.online = False
        self.assertFalse(await self.connection.check_health())
        self.assertFalse(self.connection.healthy)
        self.assertFalse(self.connection._ready.is_set())
        await self.sockets[-1].send_json(self.message(marker="unverified"))
        self.online = True
        # RPC responses are processed in socket order, so this completed health
        # check proves the preceding unverified event was examined and rejected.
        self.assertTrue(await self.connection.check_health())
        self.assertTrue(self.events.empty())

    async def test_explicit_failure_and_unknown_echo(self):
        await self.connection.connect()
        await self.sockets[-1].send_json({"echo": "unknown", "status": "ok", "retcode": 0, "data": {}})
        with self.assertRaises(ActionError) as caught:
            await self.connection.rpc("fail", {})
        self.assertEqual(caught.exception.retcode, 1404)
        self.assertNotIn(TOKEN, str(caught.exception))
        self.assertFalse(self.connection._pending)
        self.assertTrue(self.connection.healthy)

    async def test_timeout_is_uncertain_never_replayed(self):
        await self.connection.connect()
        with self.assertRaises(ActionUncertain):
            await self.connection.rpc("hang", {}, timeout=0.03)
        self.assertEqual(sum(r["action"] == "hang" for r in self.requests), 1)
        self.assertFalse(self.connection._pending)

    async def test_disconnect_and_stop_resolve_pending(self):
        await self.connection.connect()
        with self.assertRaises(ActionUncertain):
            await self.connection.rpc("disconnect", {}, timeout=1)
        await self.connection.connect()
        task = asyncio.create_task(self.connection.rpc("hang", {}, timeout=5))
        self.seen.clear()
        await asyncio.wait_for(self.seen.wait(), 1)
        await self.connection.stop()
        with self.assertRaises(ActionUncertain):
            await task
        self.assertFalse(self.connection.connected)
        self.assertFalse(self.connection._pending)
        with self.assertRaises(ConnectionError):
            await self.connection.rpc("send_msg", {})

    async def test_messages_require_verified_identity_and_ignore_selfreport(self):
        self.early_event = True
        await self.connection.connect()
        ws = self.sockets[-1]
        for value in (
            [],
            {"post_type": "message_sent", "self_id": 1001, "user_id": 2001},
            self.message(self_id=1002),
            self.message(user_id=1001),
            self.message(self_id=True),
        ):
            await ws.send_json(value)
        await ws.send_str("not-json")
        await ws.send_json(self.message(marker="accepted"))
        accepted = await asyncio.wait_for(self.events.get(), 1)
        self.assertEqual(accepted["marker"], "accepted")
        self.assertTrue(self.events.empty())

    async def test_wrong_login_and_offline_do_not_become_healthy(self):
        self.login_id = 1002
        with self.assertRaises(TimeoutError):
            await self.connection.connect(timeout=0.08)
        self.assertFalse(self.connection.healthy)
        self.assertTrue(self.events.empty())
        await self.connection.stop()
        self.login_id = 1001
        self.online = False
        with self.assertRaises(TimeoutError):
            await self.connection.connect(timeout=0.08)
        self.assertFalse(self.connection.healthy)

    async def test_reconnect_can_refresh_token_supplier(self):
        calls = []
        self.connection._token = lambda: calls.append(True) or TOKEN
        await self.connection.connect()
        old = self.sockets[-1]
        await old.close()

        # Await actual confirmed new connection rather than polling or fixed sleeps.
        async def reconnected():
            while len(self.sockets) < 2 or not self.connection.healthy:
                await asyncio.sleep(0.005)

        await asyncio.wait_for(reconnected(), 1)
        self.assertGreaterEqual(len(calls), 2)
        self.assertTrue(self.connection.healthy)

    async def test_incoming_size_cap_and_bad_token(self):
        await self.connection.connect()
        self.connection.MAX_MESSAGE_BYTES = 512
        await self.sockets[-1].send_str(json.dumps(self.message(text="x" * 600)))

        async def closed():
            while not self.sockets[0].closed:
                await asyncio.sleep(0.005)

        await asyncio.wait_for(closed(), 1)
        self.assertTrue(self.events.empty())
        await self.connection.stop()
        self.connection._token = "short"
        with self.assertRaises(TimeoutError):
            await self.connection.connect(timeout=0.05)
        self.assertFalse(self.connection.healthy)

    async def test_callback_can_rpc_without_reader_deadlock(self):
        async def callback(event):
            value = await self.connection.rpc("callback_action", {"ok": True})
            await self.events.put(value)

        self.connection._on_event = callback
        await self.connection.connect()
        await self.sockets[-1].send_json(self.message())
        self.assertEqual(await asyncio.wait_for(self.events.get(), 1), {"ok": True})

    async def test_stop_cancels_inflight_callback(self):
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def callback(event):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        self.connection._on_event = callback
        await self.connection.connect()
        await self.sockets[-1].send_json(self.message())
        await asyncio.wait_for(started.wait(), 1)
        await self.connection.stop()
        self.assertTrue(cancelled.is_set())
        self.assertFalse(self.connection.healthy)


if __name__ == "__main__":
    unittest.main()
