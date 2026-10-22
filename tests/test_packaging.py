"""Exercise the staged AstrBot artifact, not just the source-template imports."""

import asyncio
import importlib
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_astrbot import host as host

from hermes_onebot_fleet.cli import main
from onebot_fleet.core import FleetCore


@pytest.mark.asyncio
async def test_staged_astrbot_uses_only_its_own_vendored_core(tmp_path, monkeypatch, host):
    destination = tmp_path / "staged_fleet_plugin"
    monkeypatch.setattr(sys, "argv", ["onebot-fleet", "stage-astrbot", "--destination", str(destination)])
    main()
    prefix = "staged_fleet_plugin"
    spec = importlib.util.spec_from_file_location(
        prefix, destination / "__init__.py", submodule_search_locations=[str(destination)]
    )
    package = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, prefix, package)
    spec.loader.exec_module(package)
    try:
        entry = importlib.import_module(f"{prefix}.main")
        adapter = importlib.import_module(f"{prefix}.adapter")
        bridge = importlib.import_module(f"{prefix}._vendor.onebot_fleet.astrbot")
        core = importlib.import_module(f"{prefix}._vendor.onebot_fleet.core")
        legacy = importlib.import_module(f"{prefix}._vendor.hermes_onebot_fleet.config")
        assert bridge.FleetCore is core.FleetCore
        assert core.FleetCore is not FleetCore
        assert core.Settings is legacy.Settings
        assert adapter.OneBotFleetAdapter.__module__ == f"{prefix}.adapter"
        assert bridge.__name__ != host.bridge.__name__
        platform_config = {
            "id": "fleet-artifact",
            "enable": True,
            "fleet": {
                "state_path": str(tmp_path / "never-opened.sqlite3"),
                "accounts": [{"id": "10001", "ws_url": "ws://127.0.0.1:1", "token_env": "ABSENT"}],
                "allowed_users": ["20001"],
            },
        }
        with pytest.raises(bridge.FleetDeliveryError, match="plugin_not_active"):
            adapter.OneBotFleetAdapter(platform_config, {}, asyncio.Queue())
        context = SimpleNamespace(platform_manager=SimpleNamespace(platform_insts=[]))
        plugin = entry.OneBotFleetPlugin(context)
        await plugin.initialize()
        platform = adapter.OneBotFleetAdapter(platform_config, {}, asyncio.Queue())
        assert platform.meta().id == "fleet-artifact"
        assert not Path(platform.settings.state_path).exists()
        await plugin.terminate()
        with pytest.raises(bridge.FleetDeliveryError, match="plugin_not_active"):
            platform._require_active()
        assert not (tmp_path / "never-opened.sqlite3").exists()
    finally:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)
