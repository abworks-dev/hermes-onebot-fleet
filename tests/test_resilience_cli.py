"""Offline CLI inspections and explicit reset; no host/QQ configuration."""

import json
import sys

import pytest

from hermes_onebot_fleet import cli
from hermes_onebot_fleet.account_state import AccountState
from hermes_onebot_fleet.delivery_budget import BudgetPolicy, DeliveryBudget
from hermes_onebot_fleet.health import HealthState
from hermes_onebot_fleet.store import Store


def state(tmp_path):
    ledger = tmp_path / "fleet.sqlite3"
    Store(ledger)
    health = HealthState(ledger.with_name(ledger.name + ".health.sqlite3"), "onebot-fleet")
    health.observe("10001", connected=True, identity="verified", online=True, good=True)
    return ledger, health


def run(monkeypatch, command, ledger, *args):
    monkeypatch.setattr(sys, "argv", ["fleet", command, "--state", str(ledger), *args])
    cli.main()


def test_health_reset_is_explicit_and_never_admits_or_promotes(tmp_path, monkeypatch, capsys):
    ledger, health = state(tmp_path)
    admission = AccountState(ledger.with_name(ledger.name + ".accounts.sqlite3"), "onebot-fleet", ["10001"])
    admission.change("10001", "quarantine", "manual hold")
    before = admission.status()
    health.report("10001", "account_anomaly")
    for args in [[], ["--account", "10001"], ["--account", "10001", "--reason", "review"]]:
        with pytest.raises(SystemExit):
            run(monkeypatch, "health", ledger, "--action", "reset", *args)
    assert not health.is_eligible("10001")
    run(
        monkeypatch,
        "health",
        ledger,
        "--action",
        "reset",
        "--account",
        "10001",
        "--reason",
        "review",
        "--confirm",
    )
    assert health.is_eligible("10001")
    assert admission.status() == before
    assert "review" not in capsys.readouterr().out


@pytest.mark.parametrize("action", ["status", "events", "alerts"])
def test_health_inspection_rejects_mutation_arguments(tmp_path, monkeypatch, action):
    ledger, _ = state(tmp_path)
    with pytest.raises(SystemExit):
        run(monkeypatch, "health", ledger, "--action", action, "--confirm")


def test_missing_health_cannot_be_initialized(tmp_path, monkeypatch):
    ledger = tmp_path / "fleet.sqlite3"
    Store(ledger)
    with pytest.raises(SystemExit):
        run(monkeypatch, "health", ledger)
    assert not ledger.with_name(ledger.name + ".health.sqlite3").exists()


@pytest.mark.parametrize("action", ["status", "audit"])
def test_budget_cli_inspects_without_refund(tmp_path, monkeypatch, capsys, action):
    ledger, _ = state(tmp_path)
    budget = DeliveryBudget(ledger.with_name(ledger.name + ".budget.sqlite3"), "onebot-fleet", BudgetPolicy())
    budget.reserve("key", "obf:onebot-fleet:group:30001")
    run(monkeypatch, "budget", ledger, "--action", action)
    json.loads(capsys.readouterr().out)
    assert budget.status()["reservations_total"] == 1


def test_alerts_exports_without_a_qq_connection(tmp_path, monkeypatch, capsys):
    ledger, health = state(tmp_path)
    health.report("10001", "account_anomaly")
    output = tmp_path / "alerts.jsonl"
    run(monkeypatch, "alerts", ledger, "--output", str(output))
    assert json.loads(capsys.readouterr().out)["written"] > 0
    assert output.is_file()
    run(monkeypatch, "alerts", ledger, "--output", str(output))
    assert json.loads(capsys.readouterr().out)["written"] == 0


@pytest.mark.parametrize("interval", ["nan", "0", "inf", "4000"])
def test_alert_watch_interval_is_bounded(tmp_path, monkeypatch, interval):
    ledger, _ = state(tmp_path)
    with pytest.raises(SystemExit):
        run(monkeypatch, "alerts", ledger, "--output", str(tmp_path / "alerts.jsonl"), "--interval", interval)
