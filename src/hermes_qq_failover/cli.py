"""Offline operator tooling: no connection or credential reads."""

import argparse
import json
import shutil
from pathlib import Path

from .store import Store


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    status = commands.add_parser(
        "status", help="Inspect ledger state counts and uncertain IDs, not message text"
    )
    status.add_argument("--state", required=True, type=Path)
    stage = commands.add_parser(
        "stage-plugin", help="Stage new plugin source only; does not enable or change profile config"
    )
    stage.add_argument("--hermes-home", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "status":
        if not args.state.is_file():
            parser.error("state database does not exist")
        store = Store(args.state)
        print(
            json.dumps(
                {"states": store.status(), "uncertain": store.uncertain()}, ensure_ascii=False, indent=2
            )
        )
        return
    root = Path(__file__).resolve().parents[2]
    shim = root / "plugin"
    if not (shim / "plugin.yaml").is_file():
        parser.error(
            "stage-plugin requires the source repository checkout (wheel users should use a checkout)"
        )
    home = args.hermes_home.resolve()
    if not home.is_dir() or not (home / "config.yaml").is_file():
        parser.error("hermes-home must be an existing profile with config.yaml")
    destination = home / "plugins" / "qq-failover"
    if destination.exists():
        parser.error("destination exists; back up/review it manually before an upgrade")
    destination.mkdir(parents=True)
    shutil.copy2(shim / "plugin.yaml", destination / "plugin.yaml")
    shutil.copy2(shim / "__init__.py", destination / "__init__.py")
    shutil.copytree(
        Path(__file__).resolve().parent,
        destination / "src" / "hermes_qq_failover",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    # Hermes PM reads plugin-local project dependencies. Never pip into its environment.
    (destination / "pyproject.toml").write_text(
        '[project]\nname="hermes-qq-failover-directory"\nversion="0.1.0"\n'
        'requires-python=">=3.12,<3.15"\ndependencies=["aiohttp>=3.12,<4"]\n',
        encoding="utf-8",
    )
    print("Plugin source staged. Existing adapters and credentials were not changed.")
    print("Configure the profile, then enable qq-failover through Hermes plugin/PM commands.")


if __name__ == "__main__":
    main()
