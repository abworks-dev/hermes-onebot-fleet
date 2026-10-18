"""Pure deny-by-default normalization; no network or framework dependencies.

Cross-account dedup uses sender/chat/Unix-second/text, not account-local message
IDs. Consequently identical messages from one sender in the same chat and second
collapse; OneBot v11 does not expose a portable cross-account message identifier.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass

from .config import Account, Settings, qq_id


@dataclass(frozen=True)
class Incoming:
    id: str
    dedup_key: str
    user_id: str
    chat_id: str
    chat_type: str
    text: str
    physical_account: str
    timestamp: int
    logical_id: str
    mentioned_bots: tuple[str, ...]


def currently_admitted(item: Incoming, settings: Settings) -> bool:
    """Recheck queued work after restart/config changes, before host dispatch."""
    bots = {a.id for a in settings.accounts}
    if (
        item.logical_id != settings.logical_id
        or item.physical_account not in bots
        or item.user_id in bots
        or item.user_id not in settings.allowed_users
    ):
        return False
    if item.chat_type == "private":
        return item.chat_id == item.user_id
    return (
        item.chat_type == "group"
        and item.chat_id in settings.allowed_groups
        and bool(set(item.mentioned_bots) & bots)
    )


def normalize_event(event: object, account: Account, settings: Settings) -> Incoming | None:
    """Accept structured v11 text events only; CQ strings never grant mentions."""
    if not isinstance(event, Mapping) or event.get("post_type") != "message":
        return None
    try:
        if qq_id(event.get("self_id")) != account.id or account.id not in {a.id for a in settings.accounts}:
            return None
        user = qq_id(event.get("user_id"))
        bots = {a.id for a in settings.accounts}
        if user in bots or user not in settings.allowed_users:
            return None
        sender = event.get("sender")
        if sender is not None and (not isinstance(sender, Mapping) or qq_id(sender.get("user_id")) != user):
            return None
        timestamp = event.get("time")
        if type(timestamp) is not int or timestamp < 0:
            return None
        message_id = event.get("message_id")
        if (
            isinstance(message_id, bool)
            or not isinstance(message_id, (str, int))
            or not re.fullmatch(r"-?[0-9]+", str(message_id))
        ):
            return None
        kind = event.get("message_type")
        if kind == "group":
            chat = qq_id(event.get("group_id"))
            if chat not in settings.allowed_groups:
                return None
        elif kind == "private":
            chat = user
        else:
            return None
        segments = event.get("message")
        if not isinstance(segments, list) or not segments:
            return None
        text_parts = []
        mentioned_ids = set()
        for segment in segments:
            if (
                not isinstance(segment, Mapping)
                or set(segment) != {"type", "data"}
                or not isinstance(segment["data"], Mapping)
            ):
                return None
            data = segment["data"]
            if segment["type"] == "text":
                if set(data) != {"text"} or not isinstance(data["text"], str):
                    return None
                text_parts.append(data["text"])
            elif segment["type"] == "at":
                if set(data) != {"qq"}:
                    return None
                target = data["qq"]
                if target != "all":
                    mentioned_id = qq_id(target)
                    if mentioned_id in bots:
                        mentioned_ids.add(mentioned_id)
            elif segment["type"] == "reply":
                if (
                    set(data) != {"id"}
                    or isinstance(data["id"], bool)
                    or not isinstance(data["id"], (str, int))
                ):
                    return None
            else:
                return None  # v1 intentionally rejects attachments and rich content
        if kind == "group" and settings.require_mention and not mentioned_ids:
            return None
        text = "".join(text_parts).strip()
        if not text:
            return None
        canonical = json.dumps(
            [settings.logical_id, kind, chat, user, timestamp, text],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return Incoming(
            str(message_id),
            fingerprint,
            user,
            chat,
            kind,
            text,
            account.id,
            timestamp,
            settings.logical_id,
            tuple(sorted(mentioned_ids)),
        )
    except (ValueError, TypeError, UnicodeError):
        return None
