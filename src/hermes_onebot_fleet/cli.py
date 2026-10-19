"""Offline operator tooling: no connection or credential reads."""

import argparse
import json
import shutil
import sqlite3
from pathlib import Path

from .account_state import AccountState
from .store import Store


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    status = commands.add_parser(
        "status", help="Inspect ledger state counts and uncertain IDs, not message text"
    )
    status.add_argument("--state", required=True, type=Path)
    accounts = commands.add_parser(
        "accounts", help="Inspect or explicitly change local business admission; never logs in to QQ"
    )
    accounts.add_argument("--state", required=True, type=Path, help="Existing Fleet message ledger")
    accounts.add_argument(
        "--action",
        choices=["status", "audit", "pause", "quarantine", "observe", "allow", "promote"],
        default="status",
    )
    accounts.add_argument("--account")
    accounts.add_argument("--reason")
    accounts.add_argument("--confirm", action="store_true")
    stage = commands.add_parser(
        "stage-plugin", help="Stage new plugin source only; does not enable or change profile config"
    )
    stage.add_argument("--hermes-home", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "accounts":
        if not args.state.is_file():
            parser.error("state database does not exist")
        sidecar = args.state.with_name(args.state.name + ".accounts.sqlite3")
        if not sidecar.is_file():
            parser.error("account state does not exist; this command cannot initialize or reset admission")
        readonly = args.action in {"status", "audit"}
        if readonly and (args.account is not None or args.reason is not None or args.confirm):
            parser.error("status/audit do not accept mutation arguments")
        if not readonly and (not args.confirm or not args.account or not args.reason):
            parser.error("account changes require --account, --reason and --confirm")
        try:
            state = AccountState.open_existing(sidecar)
            if args.action == "status":
                result = state.status()
                result["transport_health"] = "not probed by this local CLI"
            elif args.action == "audit":
                result = {"audit": state.audit()}
            elif args.action == "promote":
                result = state.promote(args.account, args.reason)
            else:
                result = state.change(args.account, args.action, args.reason)
        except (ValueError, sqlite3.Error):
            parser.error(
                "invalid account operation or unavailable state; inspect account status before retrying"
            )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
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
    destination = home / "plugins" / "onebot-fleet"
    if destination.exists():
        parser.error("destination exists; back up/review it manually before an upgrade")
    destination.mkdir(parents=True)
    shutil.copy2(shim / "plugin.yaml", destination / "plugin.yaml")
    shutil.copy2(shim / "__init__.py", destination / "__init__.py")
    shutil.copytree(
        Path(__file__).resolve().parent,
        destination / "src" / "hermes_onebot_fleet",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    # Hermes PM reads plugin-local project dependencies. Never pip into its environment.
    (destination / "pyproject.toml").write_text(
        '[project]\nname="hermes-onebot-fleet-directory"\nversion="0.4.0"\n'
        'requires-python=">=3.12,<3.15"\ndependencies=["aiohttp>=3.12,<4"]\n',
        encoding="utf-8",
    )
    print("Plugin source staged. Existing adapters and credentials were not changed.")
    print("Configure the profile, then enable onebot-fleet through Hermes plugin/PM commands.")


if __name__ == "__main__":
    main()
