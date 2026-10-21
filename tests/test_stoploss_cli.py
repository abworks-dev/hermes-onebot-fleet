"""Offline aggregate-hold operator contract; never initializes sidecars or probes QQ."""

import json

import pytest
from test_resilience_cli import run, state

from hermes_onebot_fleet.config import Settings
from hermes_onebot_fleet.stop_loss import StopLoss, StopLossPolicy


def make_stop(tmp_path):
    ledger, health = state(tmp_path)
    stop = StopLoss(
        ledger.with_name(ledger.name + ".stoploss.sqlite3"),
        "onebot-fleet",
        StopLossPolicy(
            account_failures=1, account_targets=1, fleet_failures=1, fleet_targets=1, fleet_accounts=1
        ),
        clock=lambda: 1000,
    )
    stop.record("10001", "obf:onebot-fleet:group:30001", "outcome_unknown", intent="one")
    return ledger, health, stop


@pytest.mark.parametrize("action", ["status", "audit"])
def test_inspection_readonly(tmp_path, monkeypatch, capsys, action):
    ledger, _, stop = make_stop(tmp_path)
    before = stop.path.read_bytes()
    run(monkeypatch, "stoploss", ledger, "--action", action)
    assert json.loads(capsys.readouterr().out)
    assert stop.path.read_bytes() == before
    assert stop.status()["business_held"]


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--scope", "fleet"],
        ["--scope", "account", "--reason", "review", "--confirm"],
        ["--scope", "fleet", "--account", "10001", "--reason", "review", "--confirm"],
        ["--scope", "account", "--account", "99999", "--reason", "review", "--confirm"],
        ["--scope", "fleet", "--reason", "", "--confirm"],
    ],
)
def test_reset_requires_exact_scope_reason_confirmation(tmp_path, monkeypatch, args):
    ledger, _, stop = make_stop(tmp_path)
    before = stop.status()
    with pytest.raises(SystemExit):
        run(monkeypatch, "stoploss", ledger, "--action", "reset", *args)
    assert stop.status() == before


def test_resets_independent_scopes_and_keep_evidence(tmp_path, monkeypatch, capsys):
    ledger, health, stop = make_stop(tmp_path)
    health.report("10001", "outcome_unknown", "obf:onebot-fleet:group:30001")
    for scope, extra in [("account", ["--account", "10001"]), ("fleet", [])]:
        run(
            monkeypatch,
            "stoploss",
            ledger,
            "--action",
            "reset",
            "--scope",
            scope,
            *extra,
            "--reason",
            "private-review-label",
            "--confirm",
        )
        result = json.loads(capsys.readouterr().out)
        assert result["scope"] == scope and result["held"] is False
        assert "private-review-label" not in json.dumps(result)
        if scope == "account":
            assert stop.status()["fleet_held"]
    assert not stop.status()["business_held"]
    assert not health.is_eligible("10001", "obf:onebot-fleet:group:30001")
    assert stop.status()["failures_total"] == 1
    assert not stop.record("10001", "obf:onebot-fleet:group:30001", "outcome_unknown", intent="one")[
        "recorded"
    ]


@pytest.mark.parametrize("action", ["status", "audit"])
@pytest.mark.parametrize(
    "args", [["--confirm"], ["--reason", "label"], ["--account", "10001"], ["--scope", "fleet"]]
)
def test_inspection_rejects_mutation_args(tmp_path, monkeypatch, action, args):
    ledger, _, stop = make_stop(tmp_path)
    before = stop.status()
    with pytest.raises(SystemExit):
        run(monkeypatch, "stoploss", ledger, "--action", action, *args)
    assert stop.status() == before


def test_missing_stoploss_not_initialized(tmp_path, monkeypatch):
    ledger, _ = state(tmp_path)
    with pytest.raises(SystemExit):
        run(monkeypatch, "stoploss", ledger)
    assert not ledger.with_name(ledger.name + ".stoploss.sqlite3").exists()


def test_stoploss_configuration_wired_and_strict():
    base = {"accounts": [{"id": "10001", "ws_url": "ws://127.0.0.1:3001", "token_env": "TOKEN"}]}
    assert Settings.from_mapping(base).stoploss == StopLossPolicy()
    assert Settings.from_mapping({**base, "stoploss": {"account_failures": 4}}).stoploss.account_failures == 4
    for invalid in [None, False, {"enabled": False}, {"window_seconds": 0}, {"account_targets": 4}]:
        with pytest.raises(ValueError):
            Settings.from_mapping({**base, "stoploss": invalid})
