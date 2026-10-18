"""Pure configuration and trigger-policy tests; no live QQ accounts."""

import os
import unittest
from unittest.mock import patch

from hermes_qq_failover.config import Account, Settings
from hermes_qq_failover.protocol import normalize_event


def config(**overrides):
    result = {
        "accounts": [
            {"id": "1001", "ws_url": "ws://127.0.0.1:12345", "token_env": "QQ_TEST_TOKEN"},
            {"id": "1002", "ws_url": "ws://[::1]:12345", "token_env": "QQ_OTHER_TOKEN", "priority": 1},
        ],
        "allowed_users": ["2001"],
        "allowed_groups": ["3001"],
    }
    result.update(overrides)
    return result


def event(**overrides):
    result = {
        "post_type": "message",
        "message_type": "group",
        "self_id": 1001,
        "user_id": 2001,
        "group_id": 3001,
        "message_id": 555,
        "time": 100000,
        "sender": {"user_id": 2001},
        "message": [{"type": "at", "data": {"qq": "1001"}}, {"type": "text", "data": {"text": " hello "}}],
    }
    result.update(overrides)
    return result


class ConfigTests(unittest.TestCase):
    def test_defaults_and_injected_host_keys(self):
        settings = Settings.from_mapping(
            config(
                allowed_users=[],
                allowed_groups=[],
                group_sessions_per_user=True,
                thread_sessions_per_user=False,
            )
        )
        self.assertTrue(settings.require_mention)
        self.assertEqual(settings.logical_id, "qq-failover")
        self.assertFalse(settings.allowed_users)
        self.assertIsNone(normalize_event(event(), settings.accounts[0], settings))
        with self.assertRaises(ValueError):
            Settings(settings.accounts, require_mention=False)

    def test_unknown_and_invalid_fields(self):
        for mutation in (
            {"typo": 1},
            {"accounts": []},
            {"allowed_users": "2001"},
            {"allowed_users": [True]},
            {"require_mention": "true"},
            {"require_mention": False},
            {"require_mention": 1},
            {"logical_id": "../escape"},
            {"state_path": ""},
        ):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                Settings.from_mapping(config(**mutation))
        for account in (
            {"id": "1001", "ws_url": "ws://127.0.0.1", "token_env": "T", "token": "secret"},
            {"id": True, "ws_url": "ws://127.0.0.1", "token_env": "T"},
        ):
            with self.assertRaises(ValueError):
                Settings.from_mapping(config(accounts=[account]))
        duplicate = config()
        duplicate["accounts"][1]["id"] = "1001"
        with self.assertRaises(ValueError):
            Settings.from_mapping(duplicate)

    def test_endpoint_validation(self):
        for url in (
            "http://127.0.0.1",
            "ws://example.com",
            "ws://192.168.1.1",
            "ws://user:password@127.0.0.1",
            "ws://127.0.0.1?token=abc",
            "ws://localhost:bad",
            "ws://[bad",
            "ws://127.0.0.1#fragment",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                Account("1", url, "TOKEN")
        for url in ("ws://localhost:1234", "ws://127.0.0.2", "wss://[::1]/onebot"):
            self.assertEqual(Account("1", url, "TOKEN").ws_url, url)

    def test_lazy_tokens_never_in_repr_or_error(self):
        account = Settings.from_mapping(config()).accounts[0]
        with patch.dict(os.environ, {"QQ_TEST_TOKEN": "a" * 16}):
            self.assertEqual(account.resolve_token(), "a" * 16)
            os.environ["QQ_TEST_TOKEN"] = "b" * 16
            self.assertEqual(account.resolve_token(), "b" * 16)
        self.assertEqual(account.resolve_token({"QQ_TEST_TOKEN": "x" * 16}), "x" * 16)
        self.assertNotIn("x" * 16, repr(account))
        for value in (None, "short", "a" * 16 + "\n"):
            with self.assertRaises(ValueError) as error:
                account.resolve_token({"QQ_TEST_TOKEN": value})
            if value:
                self.assertNotIn(value, str(error.exception))


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.settings = Settings.from_mapping(config())
        self.account = self.settings.accounts[0]

    def normalize(self, raw):
        return normalize_event(raw, self.account, self.settings)

    def test_normalized_and_cross_account_dedup(self):
        first = self.normalize(event())
        second = normalize_event(
            event(self_id=1002, message_id=999), self.settings.accounts[1], self.settings
        )
        self.assertEqual(first.text, "hello")
        self.assertEqual(first.chat_type, "group")
        self.assertEqual(first.physical_account, "1001")
        self.assertEqual(first.dedup_key, second.dedup_key)
        self.assertNotEqual(first.id, second.id)
        self.assertNotEqual(first.dedup_key, self.normalize(event(time=100001)).dedup_key)
        self.assertEqual(
            first.dedup_key, self.normalize(event(message_id=556)).dedup_key
        )  # documented same-second collapse

    def test_group_needs_user_group_and_structured_mention(self):
        for raw in (
            event(user_id=2002),
            event(group_id=3002),
            event(message=[{"type": "text", "data": {"text": "@1001 hello"}}]),
            event(message=[{"type": "text", "data": {"text": "[CQ:at,qq=1001] hello"}}]),
            event(
                message=[
                    {"type": "reply", "data": {"id": "555"}},
                    {"type": "text", "data": {"text": "hello"}},
                ]
            ),
            event(
                message=[{"type": "at", "data": {"qq": "9999"}}, {"type": "text", "data": {"text": "hello"}}]
            ),
            event(message="[CQ:at,qq=1001] hello"),
        ):
            with self.subTest(raw=raw):
                self.assertIsNone(self.normalize(raw))
        raw = event()
        raw["message"][0]["data"]["qq"] = "1002"
        self.assertIsNotNone(self.normalize(raw))

    def test_private_allowed_without_mention(self):
        self.assertIsNotNone(
            self.normalize(
                event(message_type="private", message=[{"type": "text", "data": {"text": "hello"}}])
            )
        )

    def test_invalid_and_bot_input_is_ignored(self):
        for raw in (
            None,
            [],
            {},
            event(self_id=1002),
            event(post_type="message_sent"),
            event(user_id=1001),
            event(user_id=True),
            event(time=True),
            event(time="100000"),
            event(message_id=[]),
            event(sender={"user_id": "2002"}),
            event(message_type="guild"),
            event(message=[{"type": "image", "data": {"file": "x"}}]),
            event(message=[{"type": "text", "data": {"text": 1}}]),
            event(message=[]),
        ):
            with self.subTest(raw=raw):
                self.assertIsNone(self.normalize(raw))


if __name__ == "__main__":
    unittest.main()
