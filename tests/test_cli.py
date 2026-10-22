import sys

import pytest

from hermes_onebot_fleet.cli import main


def test_stage_is_additive_and_refuses_overwrite(tmp_path, monkeypatch):
    profile = tmp_path / "profile"
    profile.mkdir()
    config = profile / "config.yaml"
    config.write_text("plugins: {}\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["onebot-fleet", "stage-plugin", "--hermes-home", str(profile)])
    main()
    destination = profile / "plugins" / "onebot-fleet"
    assert (destination / "plugin.yaml").is_file()
    assert (destination / "src" / "hermes_onebot_fleet" / "adapter.py").is_file()
    assert (destination / "src" / "onebot_fleet" / "core.py").is_file()
    assert "aiohttp" in (destination / "pyproject.toml").read_text(encoding="utf-8")
    assert config.read_text(encoding="utf-8") == "plugins: {}\n"
    assert not (profile / ".env").exists()
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code == 2


def test_stage_refuses_non_profile(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["onebot-fleet", "stage-plugin", "--hermes-home", str(tmp_path)])
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code == 2
    assert not (tmp_path / "plugins").exists()


def test_stage_astrbot_is_self_contained_and_refuses_overwrite(tmp_path, monkeypatch):
    destination = tmp_path / "astrbot_plugin_onebot_fleet"
    monkeypatch.setattr(sys, "argv", ["onebot-fleet", "stage-astrbot", "--destination", str(destination)])
    main()
    assert (destination / "main.py").is_file()
    assert (destination / "metadata.yaml").is_file()
    assert (destination / "_vendor" / "__init__.py").is_file()
    assert (destination / "_vendor" / "onebot_fleet" / "core.py").is_file()
    assert (destination / "_vendor" / "onebot_fleet" / "astrbot.py").is_file()
    assert (destination / "_vendor" / "hermes_onebot_fleet" / "config.py").is_file()
    assert not (destination / "config.yaml").exists()
    assert not (destination / ".env").exists()
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code == 2


def test_stage_astrbot_refuses_existing_directory_without_mutation(tmp_path, monkeypatch):
    destination = tmp_path / "existing"
    destination.mkdir()
    sentinel = destination / "keep.txt"
    sentinel.write_text("original", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["onebot-fleet", "stage-astrbot", "--destination", str(destination)])
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code == 2
    assert sentinel.read_text(encoding="utf-8") == "original"
    assert list(destination.iterdir()) == [sentinel]


def test_status_does_not_create_missing_database(tmp_path, monkeypatch):
    path = tmp_path / "absent.sqlite3"
    monkeypatch.setattr(sys, "argv", ["onebot-fleet", "status", "--state", str(path)])
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code == 2
    assert not path.exists()
