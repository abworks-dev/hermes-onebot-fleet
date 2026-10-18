import sys

import pytest

from hermes_qq_failover.cli import main


def test_stage_is_additive_and_refuses_overwrite(tmp_path, monkeypatch):
    profile = tmp_path / "profile"
    profile.mkdir()
    config = profile / "config.yaml"
    config.write_text("plugins: {}\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["qq-failover", "stage-plugin", "--hermes-home", str(profile)])
    main()
    destination = profile / "plugins" / "qq-failover"
    assert (destination / "plugin.yaml").is_file()
    assert (destination / "src" / "hermes_qq_failover" / "adapter.py").is_file()
    assert "aiohttp" in (destination / "pyproject.toml").read_text(encoding="utf-8")
    assert config.read_text(encoding="utf-8") == "plugins: {}\n"
    assert not (profile / ".env").exists()
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code == 2


def test_stage_refuses_non_profile(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["qq-failover", "stage-plugin", "--hermes-home", str(tmp_path)])
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code == 2
    assert not (tmp_path / "plugins").exists()


def test_status_does_not_create_missing_database(tmp_path, monkeypatch):
    path = tmp_path / "absent.sqlite3"
    monkeypatch.setattr(sys, "argv", ["qq-failover", "status", "--state", str(path)])
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code == 2
    assert not path.exists()
