import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

from hermes_onebot_fleet.account_state import AccountState
from hermes_onebot_fleet.health import DIAGNOSTIC_CODES, REPORT_KINDS, HealthState
from hermes_onebot_fleet.store import Store

TARGET = "obf:fleet:group:123"
OTHER = "obf:fleet:private:456"


@pytest.fixture
def state(tmp_path):
    return HealthState(tmp_path / "health.sqlite3", "fleet", clock=lambda: 100)


def active(state, code):
    return [row for row in state.alerts() if row["code"] == code and row["state"] == "active"]


def test_unseen_is_only_circuit_eligible_and_query_does_not_create_accounts(state):
    assert state.status() == {"logical_id": "fleet", "accounts": [], "circuits": []}
    assert state.is_eligible("a")
    assert state.is_eligible("a", TARGET)
    assert state.status()["accounts"] == []
    assert state.events() == state.alerts() == []
    assert not hasattr(state, "allow")
    assert not hasattr(state, "login")
    assert not hasattr(state, "promote")


def test_unknown_is_not_success_or_confirmed_frozen(state):
    snapshot = state.observe("a", connected=True)
    assert snapshot == {
        "account_id": "a",
        "connected": True,
        "identity": "unknown",
        "online": None,
        "good": None,
        "diagnostics": [],
        "updated_at": 100,
    }
    assert state.is_eligible("a")  # Veto only, caller still needs live identity/readiness.
    assert state.status()["circuits"] == []
    assert state.alerts() == []
    assert {row["code"] for row in state.events()} == {"observation"}


@pytest.mark.parametrize("connected,identity", [(False, "verified"), (False, "unknown"), (True, "unknown")])
@pytest.mark.parametrize("online,good", [(True, True), (False, False), (None, None)])
def test_unverified_or_disconnected_does_not_claim_backend_status(state, connected, identity, online, good):
    row = state.observe("a", connected=connected, identity=identity, online=online, good=good)
    assert row["online"] is row["good"] is None
    assert row["diagnostics"] == ([] if connected else ["network_down"])
    assert state.status()["circuits"] == []
    assert state.is_eligible("a", TARGET)
    assert not active(state, "account_offline")
    assert not active(state, "account_anomaly")


def test_diagnostics_are_distinct_and_not_latched_account_freeze(state):
    network = state.observe("network", connected=False)
    offline = state.observe("offline", connected=True, identity="verified", online=False, good=True)
    degraded = state.observe("backend", connected=True, identity="verified", online=True, good=False)
    mismatch = state.observe("identity", connected=True, identity="mismatch")
    assert network["diagnostics"] == ["network_down"]
    assert offline["diagnostics"] == ["account_offline"]
    assert degraded["diagnostics"] == ["backend_degraded"]
    assert mismatch["diagnostics"] == ["identity_mismatch"]
    assert {row["code"] for row in state.alerts()} == DIAGNOSTIC_CODES
    assert [row["code"] for row in state.status()["circuits"]] == ["identity_mismatch"]
    for account in ("network", "offline", "backend"):
        assert state.is_eligible(account)
    assert not state.is_eligible("identity")
    assert not active(state, "multiple_account_anomalies")


@pytest.mark.parametrize(
    "kind,code",
    [
        ("target_rejected", "target_restricted"),
        ("outcome_unknown", "result_unknown"),
        ("target_unavailable", "target_unavailable"),
    ],
)
def test_target_circuit_is_per_account_and_target_and_latched(state, kind, code):
    state.report("a", kind, TARGET)
    assert state.is_eligible("a")
    assert not state.is_eligible("a", TARGET)
    assert state.is_eligible("a", OTHER)
    assert state.is_eligible("b", TARGET)
    state.report("a", "send_ack", TARGET)
    state.observe("a", connected=True, identity="verified", online=True, good=True)
    assert not state.is_eligible("a", TARGET)
    assert state.status()["circuits"] == [
        {
            "account_id": "a",
            "target": TARGET,
            "code": code,
            "opened_at": 100,
        }
    ]
    assert active(state, code)[0]["scope"] == "target"
    assert not active(state, "account_anomaly")
    assert not active(state, "multiple_account_anomalies")


@pytest.mark.parametrize("origin", ["identity_mismatch", "account_anomaly"])
def test_account_circuits_cover_all_targets_and_ignore_later_good_evidence(state, origin):
    if origin == "identity_mismatch":
        state.observe("a", connected=True, identity="mismatch")
    else:
        state.report("a", "account_anomaly")
    for destination in (None, TARGET, OTHER):
        assert not state.is_eligible("a", destination)
        assert state.is_eligible("b", destination)
    state.observe("a", connected=True, identity="verified", online=True, good=True)
    state.report("a", "send_ack", TARGET)
    assert not state.is_eligible("a", OTHER)
    assert active(state, origin)
    assert state.status()["accounts"][0]["diagnostics"] == []


def test_send_ack_allows_account_or_target_evidence_but_never_clears(state):
    state.report("a", "account_anomaly")
    state.report("a", "send_ack")
    state.report("a", "send_ack", TARGET)
    assert not state.is_eligible("a", TARGET)
    acks = [row for row in state.events() if row["code"] == "send_ack"]
    assert {row["scope"] for row in acks} == {"account", "target"}
    assert state.status()["accounts"][0]["identity"] == "unknown"


def test_all_causes_preserved_without_overwriting_old_latch(state):
    state.observe("a", connected=True, identity="mismatch")
    state.report("a", "account_anomaly")
    for kind in ("target_rejected", "outcome_unknown", "target_unavailable"):
        state.report("a", kind, TARGET)
    assert {row["code"] for row in state.status()["circuits"]} == {
        "identity_mismatch",
        "account_anomaly",
        "target_restricted",
        "result_unknown",
        "target_unavailable",
    }
    assert state.reset("a", reason="reviewed")["cleared"] == 2
    assert state.is_eligible("a")
    assert not state.is_eligible("a", TARGET)
    assert state.reset("a", TARGET, reason="reconciled")["cleared"] == 3
    assert state.is_eligible("a", TARGET)


def test_reset_is_exact_scope_and_never_admission_or_primary(tmp_path, state):
    admission = AccountState(tmp_path / "admission.sqlite3", "fleet", ["a", "b"])
    admission.promote("b", "manual selection")
    admission.change("a", "quarantine", "manual quarantine")
    before = admission.status()
    state.report("a", "account_anomaly")
    state.report("a", "target_rejected", TARGET)
    state.report("a", "outcome_unknown", OTHER)
    state.observe("a", connected=False)
    observed = state.status()["accounts"]
    assert state.reset("a", TARGET, reason="reviewed") == {"account_id": "a", "target": TARGET, "cleared": 1}
    assert not state.is_eligible("a", TARGET)  # Account latch still overrides.
    assert state.reset("a", reason="operator review")["cleared"] == 1
    assert state.is_eligible("a", TARGET)
    assert not state.is_eligible("a", OTHER)
    assert state.reset("a", OTHER, reason="reconciled")["cleared"] == 1
    assert state.is_eligible("a", OTHER)
    assert state.status()["accounts"] == observed  # Still network-down, never repaired by reset.
    assert active(state, "network_down")
    assert admission.status() == before
    assert not admission.is_allowed("a")
    assert state.reset("unseen", reason="reviewed")["cleared"] == 0
    assert "unseen" not in {row["account_id"] for row in state.status()["accounts"]}


def test_restart_and_open_existing_preserve_all_latches_and_journals(state):
    state.report("a", "account_anomaly")
    state.report("a", "outcome_unknown", TARGET)
    state.report("b", "pool_unavailable")
    snapshot, events, alerts = state.status(), state.events(), state.alerts()
    for reopened in (HealthState(state.path, "fleet"), HealthState.open_existing(state.path)):
        assert reopened.status() == snapshot
        assert reopened.events() == events
        assert reopened.alerts() == alerts
        assert not reopened.is_eligible("a", TARGET)
        assert reopened.is_eligible("b", TARGET)  # Pool alert is diagnostic, not admission.


def test_committed_circuit_and_alert_survive_abrupt_process_exit(tmp_path):
    path = tmp_path / "crash.sqlite3"
    script = (
        "import os,sys; from hermes_onebot_fleet.health import HealthState; "
        "h=HealthState(sys.argv[1],'fleet'); h.report('a','outcome_unknown',sys.argv[2]); os._exit(23)"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(path), TARGET], env=os.environ.copy(), check=False
    )
    assert result.returncode == 23
    reopened = HealthState.open_existing(path)
    assert not reopened.is_eligible("a", TARGET)
    assert reopened.events()[0]["code"] == "outcome_unknown"
    assert active(reopened, "result_unknown")[0]["occurrences"] == 1


def test_observation_alert_coalescing_resolution_and_recurrence(state):
    state.observe("a", connected=False)
    first = active(state, "network_down")[0]
    state.observe("a", connected=False)
    repeated = active(state, "network_down")[0]
    assert repeated["alert_id"] == first["alert_id"]
    assert repeated["occurrences"] == 2
    assert len(state.events()) == 1  # Identical passive observation is not a state transition.
    state.observe("a", connected=True, identity="verified", online=True, good=True)
    assert not active(state, "network_down")
    resolved = state.alerts()[0]
    assert resolved["state"] == "resolved"
    assert resolved["resolved_at"] == 100
    state.observe("a", connected=False)
    recurrent = active(state, "network_down")[0]
    assert recurrent["alert_id"] != first["alert_id"]
    assert recurrent["occurrences"] == 1
    assert len(state.alerts()) == 2


def test_latched_alert_coalesces_across_restart_until_manual_reset(state):
    for _ in range(3):
        state.report("a", "outcome_unknown", TARGET)
    old = active(state, "result_unknown")[0]
    assert old["occurrences"] == 3
    reopened = HealthState.open_existing(state.path)
    reopened.report("a", "outcome_unknown", TARGET)
    assert active(reopened, "result_unknown")[0]["alert_id"] == old["alert_id"]
    assert active(reopened, "result_unknown")[0]["occurrences"] == 4
    reopened.reset("a", TARGET, reason="operator reconciled")
    assert not active(state, "result_unknown")
    state.report("a", "outcome_unknown", TARGET)
    assert active(state, "result_unknown")[0]["alert_id"] != old["alert_id"]
    assert len([row for row in state.alerts() if row["code"] == "result_unknown"]) == 2


def test_fleet_alert_requires_multiple_distinct_anomalous_accounts(state):
    state.report("a", "account_anomaly")
    state.observe("a", connected=True, identity="mismatch")
    assert not active(state, "multiple_account_anomalies")
    state.report("b", "target_rejected", TARGET)
    state.observe("b", connected=False)
    assert not active(state, "multiple_account_anomalies")
    state.report("b", "account_anomaly")
    fleet = active(state, "multiple_account_anomalies")[0]
    assert fleet["scope"] == "fleet"
    assert fleet["account_id"] is fleet["target"] is None
    state.report("a", "account_anomaly")
    assert active(state, "multiple_account_anomalies")[0] == fleet
    state.reset("a", reason="reviewed")
    assert not active(state, "multiple_account_anomalies")
    assert state.is_eligible("a")
    assert not state.is_eligible("b")
    state.report("a", "account_anomaly")
    assert active(state, "multiple_account_anomalies")[0]["alert_id"] != fleet["alert_id"]


def test_pool_unavailable_is_explicit_durable_fleet_alert_without_circuit(state):
    state.report("a", "pool_unavailable")
    state.report("b", "pool_unavailable")
    fleet = active(state, "pool_unavailable")[0]
    assert fleet["scope"] == "fleet" and fleet["account_id"] is None
    assert fleet["occurrences"] == 2
    assert state.status()["circuits"] == []
    assert state.is_eligible("a", TARGET)
    assert HealthState.open_existing(state.path).alerts() == state.alerts()


def test_clock_is_finite_and_clamped_durably_across_restart(tmp_path):
    clock = [200.0]
    state = HealthState(tmp_path / "health.sqlite3", "fleet", clock=lambda: clock[0])
    state.report("a", "target_rejected", TARGET)
    clock[0] = 5
    state.report("a", "target_rejected", TARGET)
    state = HealthState(state.path, "fleet", clock=lambda: 1)
    state.reset("a", TARGET, reason="reviewed")
    assert {event["timestamp"] for event in state.events()} == {200}
    assert state.alerts()[0]["first_seen"] == state.alerts()[0]["resolved_at"] == 200
    for invalid in (True, None, "secret", float("nan"), float("inf"), -1, 253402300800):
        bad = HealthState(state.path, "fleet", clock=lambda invalid=invalid: invalid)
        before = bad.events()
        with pytest.raises(ValueError, match="timestamp"):
            bad.report("a", "account_anomaly")
        assert bad.events() == before
        assert bad.is_eligible("a")


def test_no_time_retry_half_open_or_probe(state):
    state.report("a", "account_anomaly")
    state._clock = lambda: 253402300799
    state.observe("a", connected=True, identity="verified", online=True, good=True)
    assert not state.is_eligible("a")
    assert state.status()["circuits"][0]["opened_at"] == 100


def test_fixed_codes_only_no_fake_get_status_interpretation(state):
    assert REPORT_KINDS == {
        "account_anomaly",
        "target_rejected",
        "outcome_unknown",
        "target_unavailable",
        "send_ack",
        "pool_unavailable",
        "pool_available",
    }
    for kind in (
        "get_status",
        "frozen",
        "retcode_100",
        "error",
        "network_down",
        100,
        None,
        [],
        "token=secret",
    ):
        with pytest.raises(ValueError, match="report kind"):
            state.report("a", kind)
    assert state.status()["accounts"] == []
    assert state.events() == state.alerts() == []


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("observe", ("a",), {"connected": 1}),
        ("observe", ("a",), {"connected": None}),
        ("observe", ("a",), {"connected": True, "identity": "healthy"}),
        ("observe", ("a",), {"connected": True, "identity": []}),
        ("observe", ("a",), {"connected": True, "online": 0}),
        ("observe", ("a",), {"connected": True, "good": "false"}),
        ("report", ("a", "target_rejected"), {}),
        ("report", ("a", "outcome_unknown"), {}),
        ("report", ("a", "target_unavailable"), {}),
        ("report", ("a", "account_anomaly", TARGET), {}),
        ("report", ("a", "pool_unavailable", TARGET), {}),
    ],
)
def test_invalid_observations_and_scopes_are_atomic(state, method, args, kwargs):
    with pytest.raises(ValueError):
        getattr(state, method)(*args, **kwargs)
    assert state.status()["accounts"] == []
    assert state.events() == state.alerts() == []


@pytest.mark.parametrize("bad", [None, "", " ", "a b", "a\n", "a/../b", "x" * 129, "令牌", [], 123, True])
def test_bad_account_identifiers_rejected_everywhere(state, bad):
    for call in (
        lambda: state.observe(bad, connected=False),
        lambda: state.report(bad, "account_anomaly"),
        lambda: state.reset(bad, reason="reviewed"),
        lambda: state.is_eligible(bad),
    ):
        with pytest.raises(ValueError):
            call()
    assert state.events() == state.alerts() == []


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "target",
        "obf:other:group:123",
        "obf:fleet:channel:123",
        "obf:fleet:group:0",
        "obf:fleet:group:0123",
        "obf:fleet:group:123\n",
        "obf:fleet:group:" + "1" * 33,
        "obf:fleet:group:token",
        [],
        123,
    ],
)
def test_bad_target_or_cross_namespace_rejected_everywhere(state, bad):
    for call in (
        lambda: state.report("a", "outcome_unknown", bad),
        lambda: state.reset("a", bad, reason="reviewed"),
        lambda: state.is_eligible("a", bad),
    ):
        with pytest.raises(ValueError):
            call()
    assert state.events() == state.alerts() == []


@pytest.mark.parametrize("reason", [None, "", " ", "bad\nreason", "x" * 201, [], 123])
def test_reset_reason_required_before_any_mutation(state, reason):
    state.report("a", "account_anomaly")
    before = state.status(), state.events(), state.alerts()
    with pytest.raises(ValueError):
        state.reset("a", reason=reason)
    assert (state.status(), state.events(), state.alerts()) == before


def test_reason_text_and_payloads_never_persist_or_appear_in_output(state):
    secret = "operator-token=NOT-A-REAL-SECRET-12345"
    state.report("a", "outcome_unknown", TARGET)
    state.reset("a", TARGET, reason=secret)
    event = state.events()[0]
    assert event["code"] == "reset"
    assert event["reason_id"] == hashlib.sha256(secret.encode()).hexdigest()
    serialized = json.dumps([state.status(), state.events(), state.alerts()])
    assert secret not in serialized
    assert secret.encode() not in state.path.read_bytes()
    assert not {"message", "exception", "token", "reason", "error"} & set(event)
    with pytest.raises(TypeError):
        state.report("a", "account_anomaly", message=secret)
    with pytest.raises(TypeError):
        state.observe("a", connected=False, exception=secret)


@pytest.mark.parametrize("limit", [0, -1, 1001, True, 1.0, "5", None])
def test_journal_query_limits_strict(state, limit):
    for method in (state.events, state.alerts):
        with pytest.raises(ValueError, match="limit"):
            method(limit)


def test_journal_queries_are_bounded_newest_first(state):
    for i in range(120):
        state.report("a", "outcome_unknown", f"obf:fleet:group:{i + 1}")
    assert len(state.events()) == len(state.alerts()) == 100
    assert len(state.events(1000)) == len(state.alerts(1000)) == 120
    assert [row["event_id"] for row in state.events(3)] == [120, 119, 118]
    assert [row["alert_id"] for row in state.alerts(3)] == [120, 119, 118]


def test_missing_open_existing_never_creates_files_or_parents(tmp_path):
    missing = tmp_path / "missing" / "health.sqlite3"
    with pytest.raises(sqlite3.OperationalError):
        HealthState.open_existing(missing)
    assert not missing.parent.exists()


def test_wrong_namespace_foreign_schema_and_empty_existing_fail_closed(tmp_path, state):
    state.report("a", "account_anomaly")
    before = state.path.read_bytes()
    with pytest.raises(ValueError, match="namespace"):
        HealthState(state.path, "other")
    assert state.path.read_bytes() == before
    ledger = tmp_path / "ledger.sqlite3"
    Store(ledger).enqueue("test", {"content": "never read this"})
    before = ledger.read_bytes()
    for call in (lambda: HealthState(ledger, "fleet"), lambda: HealthState.open_existing(ledger)):
        with pytest.raises(ValueError, match="schema"):
            call()
    assert ledger.read_bytes() == before
    empty = tmp_path / "empty.sqlite3"
    empty.touch()
    with pytest.raises(ValueError, match="schema"):
        HealthState(empty, "fleet")
    assert empty.read_bytes() == b""


@pytest.mark.parametrize(
    "damage",
    [
        "PRAGMA user_version=999",
        "PRAGMA application_id=0",
        "DROP TABLE health_circuits",
        "ALTER TABLE health_events ADD COLUMN message TEXT",
        "CREATE TABLE foreign_table (token TEXT)",
        "CREATE TABLE sqliteevil (token TEXT)",
        "CREATE TRIGGER unwanted AFTER INSERT ON health_events BEGIN DELETE FROM health_circuits; END",
        "DELETE FROM health_namespace",
        "UPDATE health_namespace SET logical_id='other'",
    ],
)
def test_schema_namespace_drift_fails_closed_on_existing_instances(state, damage):
    state.report("a", "account_anomaly")
    with sqlite3.connect(state.path) as db:
        db.execute(damage)
    for call in (
        state.status,
        state.events,
        state.alerts,
        lambda: state.is_eligible("a"),
        lambda: state.report("a", "send_ack", TARGET),
        lambda: state.reset("a", reason="reviewed"),
        lambda: state.observe("a", connected=True),
        lambda: HealthState(state.path, "fleet"),
    ):
        with pytest.raises(ValueError):
            call()
    # open_existing intentionally discovers the namespace: it cannot compare
    # against an out-of-band expected value that its API does not receive.
    if damage != "UPDATE health_namespace SET logical_id='other'":
        with pytest.raises(ValueError):
            HealthState.open_existing(state.path)


def test_corrupt_data_cannot_escape_as_free_text_enums(state):
    state.observe("a", connected=True, identity="verified")
    state.report("a", "account_anomaly")
    with sqlite3.connect(state.path) as db:
        db.execute("PRAGMA ignore_check_constraints=ON")
        db.execute("UPDATE health_accounts SET identity='SECRET_FREE_TEXT'")
        db.execute("UPDATE health_events SET code='SECRET_FREE_TEXT'")
        db.execute("UPDATE health_alerts SET code='SECRET_FREE_TEXT'")
    for query in (state.status, state.events, state.alerts):
        with pytest.raises(ValueError, match="enum"):
            query()


def test_circuit_event_and_alert_are_one_atomic_transaction(state, monkeypatch):
    before = state.status(), state.events(), state.alerts()

    def fail_alert(*args, **kwargs):
        raise RuntimeError("SECRET simulated interrupted alert write")

    monkeypatch.setattr(state, "_alert", fail_alert)
    with pytest.raises(RuntimeError, match="interrupted"):
        state.report("a", "account_anomaly")
    assert (state.status(), state.events(), state.alerts()) == before
    assert state.is_eligible("a")
    assert b"SECRET" not in state.path.read_bytes()


def test_concurrent_reports_preserve_latches_coalesced_counts_and_fleet_transition(state):
    def report(index):
        HealthState.open_existing(state.path).report("a" if index % 2 else "b", "account_anomaly")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(report, range(40)))
    assert len(state.events()) == 40
    assert len(state.status()["circuits"]) == 2
    assert not state.is_eligible("a") and not state.is_eligible("b")
    assert sorted(row["occurrences"] for row in active(state, "account_anomaly")) == [20, 20]
    assert len(active(state, "multiple_account_anomalies")) == 1
    assert active(state, "multiple_account_anomalies")[0]["occurrences"] == 1
