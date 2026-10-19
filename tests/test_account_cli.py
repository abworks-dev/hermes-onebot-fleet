"""Local operator commands must not create/reset admission or touch credentials."""

import json
import sys

import pytest

from hermes_onebot_fleet.account_state import AccountState
from hermes_onebot_fleet.cli import main
from hermes_onebot_fleet.store import Store


def setup_state(tmp_path):
    ledger = tmp_path / "ledger.db"
    Store(ledger)
    sidecar = ledger.with_name(ledger.name + ".accounts.sqlite3")
    state = AccountState(sidecar, "onebot-fleet", ["10001", "10002"])
    return ledger, state


def run(monkeypatch, ledger, *arguments):
    monkeypatch.setattr(sys, "argv", ["fleet", "accounts", "--state", str(ledger), *arguments])
    main()


def test_status_and_audit_do_not_reactivate_blocked_account(tmp_path, monkeypatch, capsys):
    ledger, state = setup_state(tmp_path)
    state.change("10001", "quarantine", "identity verification required")
    run(monkeypatch, ledger)
    output = json.loads(capsys.readouterr().out)
    assert output["accounts"][0]["state"] == "quarantined"
    assert output["transport_health"] == "not probed by this local CLI"
    run(monkeypatch, ledger, "--action", "audit")
    output = json.loads(capsys.readouterr().out)
    assert output["audit"][0]["action"] == "quarantine"
    assert not state.is_allowed("10001")


@pytest.mark.parametrize("action", ["pause", "quarantine", "observe", "allow", "promote"])
@pytest.mark.parametrize("missing", ["--account", "--reason", "--confirm"])
def test_mutations_need_complete_explicit_authorization(tmp_path, monkeypatch, action, missing):
    ledger, state = setup_state(tmp_path)
    arguments = ["--action", action]
    for flag, values in (("--account", ["10001"]), ("--reason", ["review"]), ("--confirm", [])):
        if flag != missing:
            arguments.extend([flag, *values])
    with pytest.raises(SystemExit) as failure:
        run(monkeypatch, ledger, *arguments)
    assert failure.value.code == 2
    assert state.snapshot("10001")["revision"] == 0
    assert not state.audit()


def test_pause_manual_allow_and_promotion_are_separate(tmp_path, monkeypatch, capsys):
    ledger, state = setup_state(tmp_path)
    state.select_primary(["10002", "10001"])
    for action in ("pause", "allow"):
        run(
            monkeypatch,
            ledger,
            "--action",
            action,
            "--account",
            "10001",
            "--reason",
            "manual review",
            "--confirm",
        )
        capsys.readouterr()
    assert state.is_allowed("10001")
    assert state.status()["current_primary"] == "10002"
    run(
        monkeypatch,
        ledger,
        "--action",
        "promote",
        "--account",
        "10001",
        "--reason",
        "manual role change",
        "--confirm",
    )
    assert json.loads(capsys.readouterr().out)["current_primary"] == "10001"


@pytest.mark.parametrize("ledger_exists", [False, True])
def test_cli_refuses_missing_state_without_initializing(tmp_path, monkeypatch, ledger_exists):
    ledger = tmp_path / "missing.db"
    if ledger_exists:
        Store(ledger)
    with pytest.raises(SystemExit) as failure:
        run(monkeypatch, ledger)
    assert failure.value.code == 2
    assert not ledger.with_name(ledger.name + ".accounts.sqlite3").exists()


def test_bad_account_and_reason_fail_closed(tmp_path, monkeypatch):
    ledger, state = setup_state(tmp_path)
    for account, reason in (("99999", "review"), ("10001", ""), ("10001", "bad\nlabel")):
        with pytest.raises(SystemExit) as failure:
            run(
                monkeypatch,
                ledger,
                "--action",
                "allow",
                "--account",
                account,
                "--reason",
                reason,
                "--confirm",
            )
        assert failure.value.code == 2
    assert state.snapshot("10001")["revision"] == 0
    assert not state.audit()
