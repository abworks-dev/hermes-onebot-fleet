from dataclasses import replace

import pytest

from hermes_onebot_fleet.config import Settings
from hermes_onebot_fleet.protocol import currently_admitted, normalize_event


def admitted():
    settings = Settings.from_mapping(
        {
            "accounts": [{"id": "10001", "ws_url": "ws://127.0.0.1:3001", "token_env": "TOKEN_A"}],
            "allowed_users": ["20001"],
            "allowed_groups": ["30001"],
        }
    )
    raw = {
        "post_type": "message",
        "self_id": 10001,
        "user_id": 20001,
        "message_type": "group",
        "group_id": 30001,
        "message_id": 1,
        "time": 100,
        "message": [{"type": "at", "data": {"qq": "10001"}}, {"type": "text", "data": {"text": "hi"}}],
    }
    return settings, normalize_event(raw, settings.accounts[0], settings)


@pytest.mark.parametrize(
    "field,value",
    [
        ("allowed_users", frozenset()),
        ("allowed_groups", frozenset()),
        ("logical_id", "new-namespace"),
    ],
)
def test_current_policy_revokes_queued_admissions(field, value):
    settings, item = admitted()
    assert currently_admitted(item, settings)
    assert not currently_admitted(item, replace(settings, **{field: value}))


def test_removed_mention_account_denied():
    settings, item = admitted()
    assert not currently_admitted(replace(item, mentioned_bots=("99999",)), settings)
