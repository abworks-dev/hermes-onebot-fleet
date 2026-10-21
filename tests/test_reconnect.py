"""Deterministic reconnect policy/state-machine checks; no sockets or QQ traffic."""

import asyncio
import json
from contextlib import suppress
from dataclasses import FrozenInstanceError, dataclass
from types import SimpleNamespace

import pytest
from aiohttp import WSMsgType

from hermes_onebot_fleet import transport as transport_module
from hermes_onebot_fleet.config import Account, Settings
from hermes_onebot_fleet.reconnect import ReconnectPolicy
from hermes_onebot_fleet.transport import ActionUncertain, OneBotConnection

TOKEN = "offline-test-token-123456789"
END = object()


class Clock:
    def __init__(self):
        self.value = 1000.0

    def __call__(self):
        return self.value


@dataclass
class Plan:
    connect_failure: bool = False
    login_id: int = 10001
    online: bool = True
    duration: float = 0.25
    auto_health: bool = True
    after_verified: object = None


class FakeSocket:
    def __init__(self, session, plan):
        self.session, self.plan = session, plan
        self.online = plan.online
        self.closed = False
        self.frames = asyncio.Queue()
        self.sent = []
        self.login_sent = asyncio.Event()
        self.mutation_sent = asyncio.Event()
        self.driver = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        frame = await self.frames.get()
        if frame is END:
            self.closed = True
            raise StopAsyncIteration
        return frame

    def respond(self, echo, data):
        self.frames.put_nowait(
            SimpleNamespace(
                type=WSMsgType.TEXT,
                data=json.dumps({"echo": echo, "status": "ok", "retcode": 0, "data": data}),
            )
        )

    async def send_str(self, text):
        request = json.loads(text)
        self.sent.append(request)
        action = request["action"]
        if action == "get_login_info":
            self.login_sent.set()
            if self.plan.auto_health:
                self.respond(request["echo"], {"user_id": self.plan.login_id})
        elif action == "get_status":
            if self.plan.auto_health:
                self.respond(request["echo"], {"online": self.online, "good": True})
        elif action == "send_group_msg":
            self.mutation_sent.set()  # Deliberately no acknowledgement.
        elif action == "local_probe":
            self.respond(request["echo"], {"ok": True})
        else:
            pytest.fail(f"unexpected fake RPC: {action}")

    async def finish_after_verification(self):
        transport = self.session.transport
        await transport._ready.wait()
        if self.plan.after_verified is not None:
            await self.plan.after_verified(transport, self)
        else:
            self.session.clock.value += self.plan.duration
        self.frames.put_nowait(END)


class FakeContext:
    def __init__(self, session, plan):
        self.session, self.plan = session, plan
        self.socket = None

    async def __aenter__(self):
        if self.plan.connect_failure:
            raise OSError("offline simulated connect failure")
        self.socket = FakeSocket(self.session, self.plan)
        self.session.sockets.append(self.socket)
        self.session.socket_created.set()
        self.socket.driver = asyncio.create_task(self.socket.finish_after_verification())
        return self.socket

    async def __aexit__(self, *args):
        self.socket.closed = True
        self.socket.driver.cancel()
        with suppress(asyncio.CancelledError):
            await self.socket.driver


class FakeSession:
    def __init__(self, plans, clock):
        self.plans, self.clock = plans, clock
        self.transport = None
        self.requests, self.sockets = [], []
        self.closed = False
        self.socket_created = asyncio.Event()

    def ws_connect(self, url, **kwargs):
        index = len(self.requests)
        assert index < len(self.plans), "unexpected extra reconnect attempt"
        self.requests.append((url, kwargs))
        return FakeContext(self, self.plans[index])

    async def close(self):
        self.closed = True


@pytest.fixture
def rig(monkeypatch):
    def build(plans, *, policy=None, sample=1, sleep=None, token=TOKEN):
        clock, delays = Clock(), []
        session = FakeSession(plans, clock)
        monkeypatch.setattr(transport_module.aiohttp, "ClientSession", lambda **kwargs: session)

        async def advance(delay):
            delays.append(delay)
            clock.value += delay
            if len(delays) == len(plans):
                session.transport._stopping = True

        transport = OneBotConnection(
            Account("10001", "ws://127.0.0.1:3001", "TOKEN_A"),
            token,
            lambda event: None,
            clock=clock,
            reconnect=policy,
            random_sample=lambda: sample,
            sleep=advance if sleep is None else sleep,
        )
        session.transport = transport
        return transport, session, clock, delays

    return build


def settings_mapping():
    return {"accounts": [{"id": "10001", "ws_url": "ws://127.0.0.1:3001", "token_env": "TOKEN_A"}]}


def test_reconnect_defaults_are_bounded_local_engineering_policy():
    policy = ReconnectPolicy.from_mapping({})
    assert policy.status() == {
        "initial_seconds": 1,
        "max_seconds": 60,
        "stable_seconds": 120,
        "jitter_ratio": 0.25,
    }
    with pytest.raises(FrozenInstanceError):
        policy.initial_seconds = 0
    status = policy.status()
    status["max_seconds"] = 1
    assert policy.max_seconds == 60
    assert Settings.from_mapping(settings_mapping()).reconnect == policy


@pytest.mark.parametrize("field", ["initial_seconds", "max_seconds", "stable_seconds", "jitter_ratio"])
@pytest.mark.parametrize("value", [None, True, False, "1", [], {}, float("nan"), float("inf"), -float("inf")])
def test_reconnect_rejects_nonnumeric_boolean_null_and_nonfinite(field, value):
    with pytest.raises(ValueError):
        ReconnectPolicy.from_mapping({field: value})
    with pytest.raises(ValueError):
        ReconnectPolicy(**{field: value})
    with pytest.raises(ValueError):
        Settings.from_mapping({**settings_mapping(), "reconnect": {field: value}})


@pytest.mark.parametrize(
    "mapping",
    [
        {"initial_seconds": 0},
        {"initial_seconds": 0.999},
        {"initial_seconds": -1},
        {"initial_seconds": 61},
        {"max_seconds": 0},
        {"max_seconds": 3600.001},
        {"initial_seconds": 2, "max_seconds": 1},
        {"stable_seconds": 0},
        {"stable_seconds": 86400.001},
        {"jitter_ratio": -0.001},
        {"jitter_ratio": 0.50001},
        {"unknown": 1},
        {"unknown": None},
    ],
)
def test_reconnect_rejects_bounds_order_and_unknown_fields(mapping):
    with pytest.raises(ValueError):
        ReconnectPolicy.from_mapping(mapping)
    with pytest.raises(ValueError):
        Settings.from_mapping({**settings_mapping(), "reconnect": mapping})


@pytest.mark.parametrize("value", [None, True, False, [], "defaults", 1])
def test_reconnect_mapping_itself_must_be_an_object(value):
    with pytest.raises(ValueError):
        ReconnectPolicy.from_mapping(value)
    with pytest.raises(ValueError):
        Settings.from_mapping({**settings_mapping(), "reconnect": value})


@pytest.mark.parametrize(
    "mapping",
    [
        {"initial_seconds": 1, "max_seconds": 1, "stable_seconds": 1, "jitter_ratio": 0},
        {"initial_seconds": 3600, "max_seconds": 3600, "stable_seconds": 86400, "jitter_ratio": 0.5},
        {"initial_seconds": 1.5, "max_seconds": 7.5, "stable_seconds": 120.5, "jitter_ratio": 0.125},
    ],
)
def test_reconnect_accepts_numeric_boundaries_and_settings_round_trip(mapping):
    policy = ReconnectPolicy.from_mapping(mapping)
    assert policy.status() == mapping
    assert Settings.from_mapping({**settings_mapping(), "reconnect": mapping}).reconnect == policy


@pytest.mark.parametrize("base", [1, 2, 60, 600])
@pytest.mark.parametrize("sample", [0, 0.125, 0.5, 0.999, 1])
def test_default_jitter_is_nonzero_between_three_quarters_and_full_capped_delay(base, sample):
    policy = ReconnectPolicy()
    delay = policy.delay(base, sample)
    cap = min(base, 60)
    assert 0 < 0.75 * cap <= delay <= cap <= 60
    assert delay == pytest.approx(cap * (0.75 + 0.25 * sample))


@pytest.mark.parametrize("sample", [None, True, False, "0.5", -0.001, 1.001, float("nan"), float("inf")])
def test_jitter_rejects_invalid_random_samples(sample):
    with pytest.raises(ValueError):
        ReconnectPolicy().delay(60, sample)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["short_verified", "wrong_identity", "offline", "connect"])
async def test_repeated_flaps_and_failed_authentication_escalate_to_absolute_cap(rig, failure):
    plans = [
        Plan(
            connect_failure=failure == "connect",
            login_id=99999 if failure == "wrong_identity" else 10001,
            online=failure != "offline",
        )
        for _ in range(8)
    ]
    transport, session, _, delays = rig(plans)
    await asyncio.wait_for(transport._run(), 1)
    assert delays == [1, 2, 4, 8, 16, 32, 60, 60]
    assert len(session.requests) == 8
    assert session.closed
    assert not transport.healthy and not transport.connected and not transport._ready.is_set()
    assert transport._verified_since is None and not transport._pending
    if failure == "connect":
        assert not session.sockets
    else:
        assert all(
            [request["action"] for request in socket.sent] == ["get_login_info", "get_status"]
            for socket in session.sockets
        )
        assert all(socket.closed and socket.driver.done() for socket in session.sockets)


@pytest.mark.asyncio
@pytest.mark.parametrize("sample,factor", [(0, 0.75), (0.5, 0.875), (1, 1)])
async def test_run_applies_injected_jitter_without_exceeding_cap(rig, sample, factor):
    transport, _, _, delays = rig([Plan(connect_failure=True) for _ in range(8)], sample=sample)
    await asyncio.wait_for(transport._run(), 1)
    assert delays == pytest.approx([base * factor for base in (1, 2, 4, 8, 16, 32, 60, 60)])
    assert all(0 < delay <= 60 for delay in delays)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stable_for,expected",
    [
        (119.999, [1, 2, 4]),
        (120, [1, 1, 2]),
        (120.001, [1, 1, 2]),
    ],
)
async def test_only_continuous_verified_stability_resets_at_exact_threshold(rig, stable_for, expected):
    transport, _, _, delays = rig([Plan(), Plan(duration=stable_for), Plan()])
    await asyncio.wait_for(transport._run(), 1)
    assert delays == expected


@pytest.mark.asyncio
async def test_failed_mid_socket_health_restarts_stability_clock_not_socket_age(rig):
    verified_intervals = []

    async def interrupted(transport, socket):
        clock = socket.session.clock
        initial = transport._verified_since
        clock.value += 100
        assert await transport.check_health(periodic=True)
        assert transport._verified_since == initial  # Good periodic checks do not restart it.
        clock.value += 1
        socket.online = False
        assert not await transport.check_health()
        assert transport._verified_since is None
        clock.value += 1
        socket.online = True
        assert await transport.check_health()
        renewed = transport._verified_since
        assert renewed == initial + 102
        clock.value += 119.999
        verified_intervals.append((clock.value - initial, clock.value - renewed))

    transport, _, _, delays = rig([Plan(), Plan(), Plan(after_verified=interrupted), Plan(duration=120)])
    await asyncio.wait_for(transport._run(), 1)
    assert delays == [1, 2, 4, 1]
    assert verified_intervals[0] == pytest.approx((221.999, 119.999))


@pytest.mark.asyncio
async def test_injected_reconnect_policy_controls_initial_cap_stability_and_jitter(rig):
    policy = ReconnectPolicy(initial_seconds=2, max_seconds=5, stable_seconds=10, jitter_ratio=0.5)
    transport, _, _, delays = rig(
        [
            Plan(connect_failure=True),
            Plan(connect_failure=True),
            Plan(connect_failure=True),
            Plan(duration=10),
            Plan(),
        ],
        policy=policy,
        sample=0,
    )
    await asyncio.wait_for(transport._run(), 1)
    assert delays == [1, 2, 2.5, 1, 2]


@pytest.mark.asyncio
async def test_instance_backoff_overrides_remain_compatible_for_existing_transport_tests(rig):
    transport, _, _, delays = rig([Plan(connect_failure=True) for _ in range(5)])
    transport.BACKOFF_INITIAL, transport.BACKOFF_MAX = 0.01, 0.03
    await asyncio.wait_for(transport._run(), 1)
    assert delays == pytest.approx([0.01, 0.02, 0.03, 0.03, 0.03])


@pytest.mark.asyncio
async def test_stop_interrupts_reconnect_sleep_without_an_extra_attempt(rig):
    sleeping, cancelled = asyncio.Event(), asyncio.Event()
    delays = []

    async def blocking_sleep(delay):
        delays.append(delay)
        sleeping.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    transport, session, _, _ = rig([Plan(connect_failure=True)], sleep=blocking_sleep)
    await transport.start()
    try:
        await asyncio.wait_for(sleeping.wait(), 1)
        await asyncio.wait_for(transport.stop(), 1)
        assert cancelled.is_set()
        assert delays == [1] and len(session.requests) == 1
        assert session.closed and transport._runner is None
        assert not transport.connected and not transport._pending
    finally:
        await transport.stop()


@pytest.mark.asyncio
async def test_stop_settles_shared_health_read_and_cancels_waiters_without_reconnect(rig):
    transport, session, _, delays = rig([Plan(auto_health=False)])
    waiter = None
    await transport.start()
    try:
        await asyncio.wait_for(session.socket_created.wait(), 1)
        socket = session.sockets[0]
        await asyncio.wait_for(socket.login_sent.wait(), 1)
        health_task = transport._health_task
        waiter = asyncio.create_task(transport.check_health(periodic=True))
        await asyncio.sleep(0)
        assert transport._health_task is health_task and not health_task.done()
        await asyncio.wait_for(transport.stop(), 1)
        result = (await asyncio.gather(waiter, return_exceptions=True))[0]
        assert result is False or isinstance(result, asyncio.CancelledError)
        assert health_task.done() and socket.driver.done() and socket.closed
        assert not transport._pending and transport._runner is None and transport._health_task is None
        assert not transport.healthy and not transport._ready.is_set()
        assert len(session.requests) == 1 and not delays and session.closed
    finally:
        await transport.stop()
        if waiter is not None:
            await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.asyncio
async def test_reconnect_refreshes_token_supplier_for_each_attempt(rig):
    tokens = ["first-offline-token-12345", "rotated-offline-token-12345", "third-offline-token-12345"]
    supplied = []

    def fresh_token():
        supplied.append(tokens[len(supplied)])
        return supplied[-1]

    transport, session, _, delays = rig(
        [Plan(connect_failure=True), Plan(login_id=99999), Plan()], token=fresh_token
    )
    await asyncio.wait_for(transport._run(), 1)
    assert supplied == tokens
    assert [kwargs["headers"]["Authorization"] for _, kwargs in session.requests] == [
        "Bearer " + token for token in tokens
    ]
    assert delays == [1, 2, 4]


@pytest.mark.asyncio
async def test_lost_mutating_rpc_becomes_unknown_and_late_echo_never_replays_after_reconnect(rig):
    second_ready, hold = asyncio.Event(), asyncio.Event()

    async def disconnect_after_dispatch(transport, socket):
        await socket.mutation_sent.wait()

    async def stay_connected(transport, socket):
        second_ready.set()
        await hold.wait()

    transport, session, _, delays = rig(
        [
            Plan(after_verified=disconnect_after_dispatch),
            Plan(after_verified=stay_connected),
        ]
    )
    pending = None
    await transport.start()
    try:
        await asyncio.wait_for(transport._ready.wait(), 1)
        pending = asyncio.create_task(transport.rpc("send_group_msg", {"group_id": 30001, "message": []}))
        with pytest.raises(ActionUncertain, match="connection closed after request dispatch"):
            await asyncio.wait_for(pending, 1)
        await asyncio.wait_for(second_ready.wait(), 1)
        old, current = session.sockets
        mutation = next(request for request in old.sent if request["action"] == "send_group_msg")
        assert mutation["echo"] not in transport._pending
        current.respond(mutation["echo"], {"message_id": 42})
        # Socket-order processing before a fresh completed probe proves the old
        # echo was consumed and ignored, rather than merely left in a queue.
        assert await transport.check_health()
        assert await transport.rpc("local_probe") == {"ok": True}
        assert (
            sum(
                request["action"] == "send_group_msg" for socket in session.sockets for request in socket.sent
            )
            == 1
        )
        assert not any(request["action"] == "send_group_msg" for request in current.sent)
        assert not transport._pending
        assert transport.healthy and delays == [1]
    finally:
        await transport.stop()
        hold.set()
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)
    assert session.closed and all(socket.closed and socket.driver.done() for socket in session.sockets)
