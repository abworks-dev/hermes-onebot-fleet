"""Release metadata and operator-facing identifiers must agree after a rename."""

import tomllib
from pathlib import Path

from hermes_onebot_fleet import __version__ as legacy_version
from hermes_onebot_fleet.config import Settings as LegacySettings
from onebot_fleet import __version__
from onebot_fleet.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def test_release_metadata_and_entrypoint_agree():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["name"] == "onebot-fleet"
    assert project["version"] == __version__ == legacy_version == "0.6.0"
    assert project["scripts"] == {
        "onebot-fleet": "onebot_fleet.cli:main",
        "hermes-onebot-fleet": "hermes_onebot_fleet.cli:main",
    }
    assert Settings is LegacySettings
    manifest = (ROOT / "plugin" / "plugin.yaml").read_text(encoding="utf-8")
    assert "name: onebot-fleet\n" in manifest
    assert f"version: {__version__}\n" in manifest
    assert "ONEBOT_FLEET_ALLOWED_USERS" in manifest
    assert "ONEBOT_FLEET_HOME_CHANNEL" in manifest


def test_default_namespace_and_ledger_are_consistent():
    settings = Settings.from_mapping(
        {"accounts": [{"id": "10001", "ws_url": "ws://127.0.0.1:1", "token_env": "TOKEN"}]}
    )
    assert settings.logical_id == "onebot-fleet"
    assert settings.state_path == Path("onebot-fleet.sqlite3")
