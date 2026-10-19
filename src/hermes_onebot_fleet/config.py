"""Strict, secret-free configuration for the standalone OneBot transport."""

from __future__ import annotations

import ipaddress
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from .delivery_budget import BudgetPolicy
from .publisher import PublisherSettings


def qq_id(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("QQ id must be a positive decimal string or integer")
    text = str(value)
    if not re.fullmatch(r"[1-9][0-9]*", text):
        raise ValueError("QQ id must be a positive decimal string or integer")
    return text


@dataclass(frozen=True)
class Account:
    id: str
    ws_url: str
    token_env: str
    priority: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", qq_id(self.id))
        if not isinstance(self.ws_url, str):
            raise ValueError("ws_url must be a loopback WebSocket URL")
        try:
            url = urlsplit(self.ws_url)
            host = url.hostname
            _ = url.port
            loopback = host == "localhost" or bool(host and ipaddress.ip_address(host).is_loopback)
        except ValueError:
            raise ValueError("ws_url must be a valid loopback WebSocket URL") from None
        if (
            url.scheme not in {"ws", "wss"}
            or not loopback
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
        ):
            raise ValueError("ws_url must be a loopback WebSocket URL without credentials or query")
        if not isinstance(self.token_env, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.token_env):
            raise ValueError("token_env must name an environment variable")
        if type(self.priority) is not int:
            raise ValueError("priority must be an integer")

    def resolve_token(self, environ: Mapping[str, str] | None = None) -> str:
        token = (os.environ if environ is None else environ).get(self.token_env)
        if not isinstance(token, str) or len(token) < 16 or any(c.isspace() or ord(c) < 32 for c in token):
            raise ValueError("token environment variable must contain at least 16 non-whitespace characters")
        return token


@dataclass(frozen=True)
class Settings:
    accounts: tuple[Account, ...]
    allowed_users: frozenset[str] = frozenset()
    allowed_groups: frozenset[str] = frozenset()
    logical_id: str = "onebot-fleet"
    state_path: Path = Path("onebot-fleet.sqlite3")
    require_mention: bool = True
    publisher: PublisherSettings = field(default_factory=PublisherSettings)
    budget: BudgetPolicy = field(default_factory=BudgetPolicy)

    def __post_init__(self) -> None:
        if self.require_mention is not True:
            raise ValueError("require_mention must be true in v0.2")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> Settings:
        if not isinstance(mapping, Mapping):
            raise ValueError("settings must be a mapping")
        allowed = {
            "accounts",
            "allowed_users",
            "allowed_groups",
            "logical_id",
            "state_path",
            "require_mention",
            "publisher",
            "budget",
            "group_sessions_per_user",
            "thread_sessions_per_user",
        }
        if set(mapping) - allowed:
            raise ValueError("unknown settings keys")
        raw = mapping.get("accounts")
        if not isinstance(raw, list) or not raw:
            raise ValueError("accounts must be a nonempty list")
        accounts = []
        for item in raw:
            if (
                not isinstance(item, Mapping)
                or set(item) - {"id", "ws_url", "token_env", "priority"}
                or not {"id", "ws_url", "token_env"} <= set(item)
            ):
                raise ValueError("invalid account fields")
            accounts.append(Account(**item))
        if len({a.id for a in accounts}) != len(accounts):
            raise ValueError("duplicate account ids")

        def ids(key: str) -> frozenset[str]:
            value = mapping.get(key, [])
            if not isinstance(value, list):
                raise ValueError(f"{key} must be a list")
            return frozenset(qq_id(x) for x in value)

        logical_id = mapping.get("logical_id", "onebot-fleet")
        if not isinstance(logical_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", logical_id):
            raise ValueError("invalid logical_id")
        path = mapping.get("state_path", "onebot-fleet.sqlite3")
        if not isinstance(path, (str, Path)) or not str(path).strip():
            raise ValueError("state_path must be a nonempty path")
        mention = mapping.get("require_mention", True)
        if mention is not True:
            raise ValueError("require_mention must be true in v0.2")
        return cls(
            tuple(accounts),
            ids("allowed_users"),
            ids("allowed_groups"),
            logical_id,
            Path(path),
            mention,
            PublisherSettings.from_mapping(mapping.get("publisher", {})),
            BudgetPolicy.from_mapping(mapping.get("budget", {})),
        )
