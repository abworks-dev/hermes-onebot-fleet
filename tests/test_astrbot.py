"""Offline pinned-AstrBot API doubles: no host installation, sockets or live profile."""

from __future__ import annotations

import asyncio
import importlib
import sys
from abc import ABC, abstractmethod
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from onebot_fleet.protocol import Incoming


@pytest.fixture
def host(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    names = [
        "astrbot",
        "astrbot.api",
        "astrbot.api.event",
        "astrbot.api.message_components",
        "astrbot.api.platform",
        "astrbot.api.star",
        "astrbot.core",
        "astrbot.core.platform",
        "astrbot.core.platform.astr_message_event",
    ]
    modules = {name: ModuleType(name) for name in names}
    for name, module in modules.items():
        module.__path__ = []
        monkeypatch.setitem(sys.modules, name, module)
    current = ContextVar("test_astrbot_current_event", default=None)
    modules[names[-1]].current_message_event = current

    class MessageType(Enum):
        GROUP_MESSAGE = "GroupMessage"
        FRIEND_MESSAGE = "FriendMessage"

    @dataclass
    class Meta:
        name: str
        description: str
        id: str | None = None
        support_streaming_message: bool = True
        support_proactive_message: bool = True

    @dataclass
    class Member:
        user_id: str
        nickname: str | None = None

    @dataclass
    class Plain:
        text: str

    class Message:
        pass

    class Chain:
        def __init__(self, chain):
            self.chain = chain

    @dataclass
    class Session:
        platform_name: str
        message_type: MessageType
        session_id: str

        def __str__(self):
            return f"{self.platform_name}:{self.message_type.value}:{self.session_id}"

    class Event:
        def __init__(self, message_str, message_obj, platform_meta, session_id):
            self.message_str, self.message_obj, self.platform_meta = message_str, message_obj, platform_meta
            self.session = Session(platform_meta.id, message_obj.type, session_id)
            self._has_send_oper = False
            self._execution_active = False
            self._result = "pending"
            self._force_stopped = False
            self.base_sends = 0

        @property
        def unified_msg_origin(self):
            return str(self.session)

        async def send(self, chain):
            self.base_sends += 1
            self._has_send_oper = True

        async def react(self, emoji):
            await self.send(Chain([Plain(emoji)]))

        async def process_buffer(self, buffer, pattern):
            match = pattern.search(buffer)
            if match:
                await self.send(Chain([Plain(match.group().strip())]))
                return buffer[match.end() :]
            return buffer

        def stop_event(self):
            self._force_stopped = True

        def clear_result(self):
            self._result = None

    class Platform(ABC):
        def __init__(self, config, event_queue):
            self.config, self._event_queue = config, event_queue

        @abstractmethod
        def run(self):
            raise NotImplementedError

        @abstractmethod
        def meta(self):
            raise NotImplementedError

        def commit_event(self, event):
            self._event_queue.put_nowait(event)

    class Star:
        def __init__(self, context):
            self.context = context

    registrations = []

    def register(name, description, default_config_tmpl, support_streaming_message):
        def decorator(cls):
            registrations.append((name, cls, default_config_tmpl, support_streaming_message))
            return cls

        return decorator

    api = modules["astrbot.api.platform"]
    for name, value in {
        "Platform": Platform,
        "PlatformMetadata": Meta,
        "AstrBotMessage": Message,
        "AstrMessageEvent": Event,
        "MessageMember": Member,
        "MessageType": MessageType,
        "register_platform_adapter": register,
    }.items():
        setattr(api, name, value)
    modules["astrbot.api.event"].MessageChain = Chain
    modules["astrbot.api.message_components"].Plain = Plain
    modules["astrbot.api.star"].Context = SimpleNamespace
    modules["astrbot.api.star"].Star = Star

    imported = [
        "onebot_fleet.astrbot",
        "astrbot_plugin_onebot_fleet.main",
        "astrbot_plugin_onebot_fleet.adapter",
        "astrbot_plugin_onebot_fleet",
    ]
    for name in imported:
        monkeypatch.delitem(sys.modules, name, raising=False)
    bridge = importlib.import_module("onebot_fleet.astrbot")

    class Core:
        instances = []

        def __init__(self, settings, **kwargs):
            self.settings, self.kwargs = settings, kwargs
            self.calls = []
            self.connected = self.disconnected = 0
            self.result = SimpleNamespace(success=True, message_id="ack-42", error=None, retryable=False)
            self.error = None
            self.instances.append(self)

        async def connect(self):
            self.connected += 1
            return True

        async def disconnect(self):
            self.disconnected += 1

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            self.calls.append((chat_id, content, reply_to, metadata))
            await asyncio.sleep(0)
            if self.error:
                raise self.error
            return self.result

    monkeypatch.setattr(bridge, "FleetCore", Core)
    yield SimpleNamespace(
        bridge=bridge,
        Core=Core,
        Chain=Chain,
        Plain=Plain,
        Message=Message,
        Session=Session,
        MessageType=MessageType,
        current=current,
        registrations=registrations,
        Star=Star,
    )
    for name in imported:
        sys.modules.pop(name, None)


def config(tmp_path, **overrides):
    result = {
        "id": "fleet-platform",
        "type": "onebot_fleet",
        "enable": True,
        "fleet": {
            "logical_id": "onebot-fleet",
            "state_path": str(tmp_path / "fleet.sqlite3"),
            "accounts": [{"id": "10001", "ws_url": "ws://127.0.0.1:3001", "token_env": "FLEET_A"}],
            "allowed_users": ["20001"],
            "allowed_groups": ["30001"],
        },
    }
    result.update(overrides)
    return result


def make(host, tmp_path, **kwargs):
    platform = host.bridge.OneBotFleetPlatform(
        config(tmp_path), {}, asyncio.Queue(), capability=lambda: True, **kwargs
    )
    platform._core = host.Core(platform.settings)
    return platform


async def incoming(platform, *, group=False, key="stable-inbound"):
    await platform._on_incoming(
        Incoming(
            "physical-42",
            key,
            "20001",
            "30001" if group else "20001",
            "group" if group else "private",
            "question",
            "10001",
            100,
            "onebot-fleet",
            ("10001",),
        )
    )
    return platform._event_queue.get_nowait()


def test_constructor_manager_signature_and_fixed_metadata(host, tmp_path):
    p = make(host, tmp_path)
    assert p.meta().id == "fleet-platform"
    assert p.meta().name == "onebot_fleet"
    assert not p.meta().support_streaming_message
    assert not p.meta().support_proactive_message
    assert not list(tmp_path.iterdir())
    for invalid in ["a:b", "a/b", "a!b", "a b", "", "中文"]:
        with pytest.raises(host.bridge.FleetDeliveryError, match="invalid_platform_id"):
            host.bridge.OneBotFleetPlatform(
                config(tmp_path, id=invalid), {}, asyncio.Queue(), capability=lambda: True
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("group", [False, True])
async def test_incoming_native_umo_real_user_and_minimal_raw(host, tmp_path, group):
    p = make(host, tmp_path)
    e = await incoming(p, group=group)
    expected = "GroupMessage:30001" if group else "FriendMessage:20001"
    assert e.unified_msg_origin == f"fleet-platform:{expected}"
    assert e.message_obj.sender.user_id == "20001"
    assert e.message_obj.sender.nickname is None
    assert e.message_obj.self_id == "onebot-fleet"
    assert e.message_obj.message_id == "stable-inbound"
    assert e.message_obj.raw_message == {"platform": "onebot_fleet", "message_id": "stable-inbound"}
    assert "10001" not in repr(e.message_obj.raw_message)
    await e.send(host.Chain([host.Plain("reply")]))
    assert p._core.calls == [
        (f"obf:onebot-fleet:{'group:30001' if group else 'private:20001'}", "reply", "stable-inbound", None)
    ]
    assert e._has_send_oper and e.base_sends == 1
    assert e.fleet_receipts[0].message_id == "ack-42"


@pytest.mark.asyncio
async def test_concurrent_identical_replies_do_not_repeat_transport(host, tmp_path):
    p = make(host, tmp_path)
    e = await incoming(p)
    chain = host.Chain([host.Plain("same")])
    await asyncio.gather(e.send(chain), e.send(chain), e.send(chain))
    assert len(p._core.calls) == 1
    assert e.base_sends == 1


@pytest.mark.asyncio
async def test_mixed_media_and_plain_subclasses_zero_send_and_latch(host, tmp_path):
    class PretendPlain(host.Plain):
        pass

    for chain in [[host.Plain("text"), object()], [PretendPlain("text")], [], [host.Plain(12)]]:
        p = make(host, tmp_path)
        e = await incoming(p)
        with pytest.raises(host.bridge.FleetDeliveryError):
            await e.send(host.Chain(chain))
        with pytest.raises(host.bridge.FleetDeliveryError):
            await e.send(host.Chain([host.Plain("fallback")]))
        assert not p._core.calls
        assert not e._has_send_oper
        assert e._force_stopped and e._result is None


@pytest.mark.asyncio
async def test_stream_and_raw_client_refused(host, tmp_path):
    p = make(host, tmp_path)
    e = await incoming(p)
    with pytest.raises(host.bridge.FleetDeliveryError, match="streaming_not_supported"):
        await e.send_streaming(None, use_fallback=True)
    for owner in [p, e]:
        with pytest.raises(host.bridge.FleetDeliveryError, match="raw_client_not_supported"):
            owner.get_client()
    assert not e._has_send_oper and not p._core.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["unknown", "missing_id", "exception"])
async def test_failed_or_unknown_not_marked_sent_and_terminal_across_changed_text(host, tmp_path, mode):
    p = make(host, tmp_path)
    e = await incoming(p)
    if mode == "unknown":
        p._core.result = SimpleNamespace(
            success=False, message_id=None, error="opaque-secret-token", retryable=True
        )
    elif mode == "missing_id":
        p._core.result = SimpleNamespace(success=True, message_id=None)
    else:
        p._core.error = RuntimeError("opaque-secret-token")
    with pytest.raises(host.bridge.FleetDeliveryError) as exc:
        await e.send(host.Chain([host.Plain("first")]))
    assert "opaque-secret-token" not in str(exc.value)
    assert not e._has_send_oper and not e.base_sends
    assert len(e.fleet_receipts) == 1 and not e.fleet_receipts[0].success
    p._core.result = SimpleNamespace(success=True, message_id="later")
    for text in ["first", "changed segment"]:
        with pytest.raises(host.bridge.FleetDeliveryError):
            await e.send(host.Chain([host.Plain(text)]))
    with pytest.raises(host.bridge.FleetDeliveryError):
        await e.react("x")
    assert len(p._core.calls) == 1
    assert e._force_stopped and e._result is None


@pytest.mark.asyncio
async def test_public_create_event_cannot_mint_anchor(host, tmp_path):
    p = make(host, tmp_path)
    admitted = await incoming(p)
    forged = p.create_event(admitted.message_obj)
    forged.message_obj.raw_message["reply_to"] = "stable-inbound"
    with pytest.raises(host.bridge.FleetDeliveryError, match="reply_anchor_required"):
        await forged.send(host.Chain([host.Plain("no")]))
    assert not p._core.calls


@pytest.mark.asyncio
async def test_send_by_session_only_active_owned_strict_same_session(host, tmp_path):
    p = make(host, tmp_path)
    e = await incoming(p)
    chain = host.Chain([host.Plain("reply")])
    with pytest.raises(host.bridge.FleetDeliveryError, match="active_same_session"):
        await p.send_by_session(e.session, chain)
    token = host.current.set(e)
    try:
        with pytest.raises(host.bridge.FleetDeliveryError):
            await p.send_by_session(e.session, chain)
        e._execution_active = True
        other = host.Session("fleet-platform", host.MessageType.GROUP_MESSAGE, "30001")
        with pytest.raises(host.bridge.FleetDeliveryError):
            await p.send_by_session(other, chain)
        wrong_platform = make(host, tmp_path)
        with pytest.raises(host.bridge.FleetDeliveryError):
            await wrong_platform.send_by_session(e.session, chain)
        await p.send_by_session(e.session, chain)
        assert e._has_send_oper
    finally:
        host.current.reset(token)
    assert len(p._core.calls) == 1


@pytest.mark.asyncio
async def test_inherited_react_and_process_buffer_use_receipt_send(host, tmp_path):
    import re

    p = make(host, tmp_path)
    e = await incoming(p)
    await e.react("ok")
    assert await e.process_buffer("sentence. remaining", re.compile(r"[^.]+\.")) == " remaining"
    assert [call[1] for call in p._core.calls] == ["ok", "sentence."]
    assert len(e.fleet_receipts) == 2


@pytest.mark.asyncio
async def test_queue_admission_is_handoff_not_host_execution_and_full_queue_raises(host, tmp_path):
    p = make(host, tmp_path)
    p._event_queue = asyncio.Queue(maxsize=1)
    item = Incoming("42", "key", "20001", "20001", "private", "question", "10001", 100, "onebot-fleet", ())
    await asyncio.wait_for(p._on_incoming(item), timeout=1)
    assert p._event_queue.qsize() == 1
    with pytest.raises(host.bridge.FleetDeliveryError, match="event_queue_unavailable"):
        await asyncio.wait_for(p._on_incoming(item), timeout=1)


@pytest.mark.asyncio
async def test_plugin_disabled_import_registration_namespace_and_owned_shutdown(host, tmp_path):
    main = importlib.import_module("astrbot_plugin_onebot_fleet.main")
    assert len(host.registrations) == 1
    name, cls, template, streaming = host.registrations[0]
    assert name == "onebot_fleet" and not streaming and template["enable"] is False
    assert cls.__module__ == "astrbot_plugin_onebot_fleet.adapter"
    subclasses = [
        v
        for v in vars(main).values()
        if isinstance(v, type) and v is not host.Star and issubclass(v, host.Star)
    ]
    assert subclasses == [main.OneBotFleetPlugin]
    with pytest.raises(host.bridge.FleetDeliveryError, match="plugin_not_active"):
        cls(config(tmp_path), {}, asyncio.Queue())
    assert not host.Core.instances

    class Manager:
        platform_insts = []
        stopped = []

        async def terminate_platform(self, identifier):
            # Capability must be revoked before the awaited manager call.
            with pytest.raises(host.bridge.FleetDeliveryError, match="plugin_not_active"):
                cls(config(tmp_path), {}, asyncio.Queue())
            self.stopped.append(identifier)
            for instance in self.platform_insts:
                if instance.meta().id == identifier:
                    await instance.terminate()

    manager = Manager()
    plugin = main.OneBotFleetPlugin(context=SimpleNamespace(platform_manager=manager), config={})
    await plugin.initialize()
    with pytest.raises(host.bridge.FleetDeliveryError, match="platform_not_enabled"):
        cls(config(tmp_path, enable=False), {}, asyncio.Queue())
    p = cls(config(tmp_path), {}, asyncio.Queue())
    manager.platform_insts = [p, SimpleNamespace(meta=lambda: SimpleNamespace(id="unrelated"))]
    await plugin.terminate()
    assert manager.stopped == ["fleet-platform"]
    with pytest.raises(host.bridge.FleetDeliveryError, match="plugin_not_active"):
        await p.run()
    assert not host.Core.instances
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_run_explicit_scoped_secrets_connect_disconnect(host, tmp_path, monkeypatch):
    monkeypatch.setenv("FLEET_A", "scoped-test-token-123456")
    monkeypatch.setenv("UNRELATED_SECRET", "must-not-copy")
    monkeypatch.setenv("PUBLISHER_A", "publisher-test-token-12345678901234567890")
    configured = config(tmp_path)
    configured["fleet"]["publisher"] = {
        "clients": [
            {"id": "test", "token_env": "PUBLISHER_A", "targets": {"room": "obf:onebot-fleet:group:30001"}}
        ]
    }
    p = host.bridge.OneBotFleetPlatform(configured, {}, asyncio.Queue(), capability=lambda: True)
    task = asyncio.create_task(p.run())
    await asyncio.sleep(0)
    core = host.Core.instances[-1]
    try:
        assert core.connected == 1
        assert core.kwargs["secrets"] == {
            "FLEET_A": "scoped-test-token-123456",
            "PUBLISHER_A": "publisher-test-token-12345678901234567890",
        }
        assert core.kwargs["receive_events"] is True
        assert core.kwargs["state_path"] == tmp_path / "fleet.sqlite3"
        assert core.kwargs["message_handler"] == p._on_incoming
        await p.terminate()
        await task
        assert core.disconnected == 1
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_uncertain_send_latches_without_base_sent(host, tmp_path):
    p = make(host, tmp_path)
    e = await incoming(p)
    entered = asyncio.Event()
    calls = []

    async def interrupted(*args, **kwargs):
        calls.append((args, kwargs))
        entered.set()
        await asyncio.Event().wait()

    p._core.send = interrupted
    task = asyncio.create_task(e.send(host.Chain([host.Plain("uncertain")])))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not e._has_send_oper and not e.base_sends
    assert e.fleet_receipts[0].error == "delivery_interrupted"
    with pytest.raises(host.bridge.FleetDeliveryError, match="delivery_interrupted"):
        await e.send(host.Chain([host.Plain("later segment")]))
    assert len(calls) == 1


@pytest.mark.parametrize("broken", [False, True])
def test_bootstrap_prefers_vendor_and_never_hides_internal_import_error(host, monkeypatch, broken):
    original = importlib.import_module
    vendor = "astrbot_plugin_onebot_fleet._vendor"
    imports = []

    def controlled(name, package=None):
        imports.append(name)
        if name == vendor:
            return ModuleType(vendor)
        if name == vendor + ".onebot_fleet.astrbot":
            if broken:
                raise ModuleNotFoundError("internal vendor dependency missing", name="internal_dependency")
            return host.bridge
        return original(name, package)

    monkeypatch.setattr(importlib, "import_module", controlled)
    if broken:
        with pytest.raises(ModuleNotFoundError, match="internal vendor dependency missing"):
            original("astrbot_plugin_onebot_fleet.adapter")
    else:
        module = original("astrbot_plugin_onebot_fleet.adapter")
        assert issubclass(module.OneBotFleetAdapter, host.bridge.OneBotFleetPlatform)
    assert "onebot_fleet.astrbot" not in imports


@pytest.mark.asyncio
async def test_host_after_sent_hook_cannot_be_treated_as_receipt(host, tmp_path):
    p = make(host, tmp_path)
    e = await incoming(p)
    p._core.result = SimpleNamespace(success=False, message_id=None)
    hooks = []
    # Model the pinned RespondStage limitation explicitly: ordinary exceptions
    # are swallowed before the first hook. Adapter cannot promise to fix host.
    for segment in ["first", "second"]:
        try:
            await e.send(host.Chain([host.Plain(segment)]))
        except Exception:
            pass
        hooks.append("after_sent")
    assert hooks == ["after_sent", "after_sent"]
    assert len(p._core.calls) == 1 and not e._has_send_oper
    assert not e.fleet_receipts[0].success
