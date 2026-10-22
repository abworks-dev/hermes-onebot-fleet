"""Plugin-local registration permits AstrBot to unregister this adapter on unload."""

from __future__ import annotations

from importlib import import_module

from astrbot.api.platform import register_platform_adapter

# A staged distribution is self-contained; a source checkout may use the
# installed canonical package. Never alter sys.path or hide an internal import
# failure in a present vendored distribution.
_vendor_name = f"{__package__}._vendor"
try:
    import_module(_vendor_name)
except ModuleNotFoundError as exc:
    if exc.name != _vendor_name:
        raise
    _bridge = import_module("onebot_fleet.astrbot")
else:
    _bridge = import_module(f"{_vendor_name}.onebot_fleet.astrbot")

_active_owner: object | None = None


def activate(owner: object) -> None:
    global _active_owner
    if _active_owner is not None and _active_owner is not owner:
        raise _bridge.FleetDeliveryError("plugin_already_active")
    _active_owner = owner


def revoke(owner: object) -> None:
    global _active_owner
    if _active_owner is owner:
        _active_owner = None


@register_platform_adapter(
    "onebot_fleet",
    "OneBot Fleet conservative text replies (restart required)",
    default_config_tmpl={
        "id": "onebot-fleet",
        "type": "onebot_fleet",
        "enable": False,
        "fleet": {
            "logical_id": "onebot-fleet",
            "state_path": "data/plugin_data/astrbot_plugin_onebot_fleet/fleet.sqlite3",
            "accounts": [],
            "allowed_users": [],
            "allowed_groups": [],
            "require_mention": True,
        },
    },
    support_streaming_message=False,
)
class OneBotFleetAdapter(_bridge.OneBotFleetPlatform):
    def __init__(self, platform_config: dict, platform_settings: dict, event_queue) -> None:
        owner = _active_owner
        self._fleet_owner = owner
        super().__init__(
            platform_config,
            platform_settings,
            event_queue,
            capability=lambda: owner is not None and _active_owner is owner,
        )
