"""Offline operator tooling: no connection or credential reads."""

import argparse
import json
import math
import shutil
import sqlite3
import time
from pathlib import Path

from .account_state import AccountState
from .delivery_budget import DeliveryBudget
from .health import HealthState
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
    health = commands.add_parser(
        "health", help="Inspect diagnostics or explicitly reset a circuit; no QQ probes"
    )
    health.add_argument("--state", required=True, type=Path)
    health.add_argument("--action", choices=["status", "events", "alerts", "reset"], default="status")
    health.add_argument("--account")
    health.add_argument("--target")
    health.add_argument("--reason")
    health.add_argument("--confirm", action="store_true")
    budget = commands.add_parser("budget", help="Inspect shared budget; never refund or clear reservations")
    budget.add_argument("--state", required=True, type=Path)
    budget.add_argument("--action", choices=["status", "audit"], default="status")
    alerts = commands.add_parser(
        "alerts", help="Export alerts independently of QQ; does not install a service"
    )
    alerts.add_argument("--state", required=True, type=Path)
    alerts.add_argument("--output", required=True, type=Path)
    alerts.add_argument("--follow", action="store_true")
    alerts.add_argument("--interval", type=float, default=10)
    alerts.add_argument("--stale-after", type=float, default=30)
    stage = commands.add_parser(
        "stage-plugin", help="Stage new plugin source only; does not enable or change profile config"
    )
    stage.add_argument("--hermes-home", required=True, type=Path)
    args = parser.parse_args()
    if args.command in {"health", "budget", "alerts"}:
        if not args.state.is_file():
            parser.error("state database does not exist")
        try:
            if args.command == "alerts":
                from .alerts import AlertWatcher

                if not math.isfinite(args.interval) or not 1 <= args.interval <= 3600:
                    parser.error("interval must be finite and in 1..3600 seconds")
                watcher = AlertWatcher(
                    args.state.with_name(args.state.name + ".health.sqlite3"),
                    args.output,
                    stale_after=args.stale_after,
                )
                while True:
                    print(json.dumps(watcher.poll(), ensure_ascii=False), flush=True)
                    if not args.follow:
                        break
                    time.sleep(args.interval)
                return
            if args.command == "budget":
                state = DeliveryBudget.open_existing(
                    args.state.with_name(args.state.name + ".budget.sqlite3")
                )
                result = state.status() if args.action == "status" else {"audit": state.audit()}
            else:
                state = HealthState.open_existing(args.state.with_name(args.state.name + ".health.sqlite3"))
                if args.action != "reset":
                    if (
                        args.account is not None
                        or args.target is not None
                        or args.reason is not None
                        or args.confirm
                    ):
                        parser.error("health inspection does not accept mutation arguments")
                    result = (
                        state.status()
                        if args.action == "status"
                        else {args.action: getattr(state, args.action)()}
                    )
                else:
                    if not args.confirm or not args.account or not args.reason:
                        parser.error("circuit reset requires --account, --reason and --confirm")
                    if args.account not in {entry["account_id"] for entry in state.status()["accounts"]}:
                        parser.error("unknown observed account")
                    result = state.reset(args.account, args.target, reason=args.reason)
        except KeyboardInterrupt:
            return
        except (ValueError, OSError, RuntimeError, sqlite3.Error):
            parser.error("invalid operation or unavailable local state; no QQ operation was performed")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
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
        '[project]\nname="hermes-onebot-fleet-directory"\nversion="0.5.0"\n'
        'requires-python=">=3.12,<3.15"\ndependencies=["aiohttp>=3.12,<4"]\n',
        encoding="utf-8",
    )
    print("Plugin source staged. Existing adapters and credentials were not changed.")
    print("Configure the profile, then enable onebot-fleet through Hermes plugin/PM commands.")


if __name__ == "__main__":
    main()
