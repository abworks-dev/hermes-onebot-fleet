import json
import os
import sqlite3

import pytest

from hermes_onebot_fleet.alerts import AlertWatcher
from hermes_onebot_fleet.health import HealthState


class Clock:
    def __init__(self, value=100):
        self.value = value

    def __call__(self):
        return self.value


@pytest.fixture
def local(tmp_path):
    clock = Clock()
    health = HealthState(tmp_path / "health.sqlite3", "fleet", clock=clock)
    output = tmp_path / "alerts.jsonl"
    return clock, health, output


def records(output):
    return [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]


def observe_good(health, account="a"):
    health.observe(account, connected=True, identity="verified", online=True, good=True)


def test_sanitized_alert_fields_and_no_source_mutation(local):
    clock, health, output = local
    health.report("a", "target_rejected", "obf:fleet:group:123")
    health_bytes = health.path.read_bytes()
    watcher = AlertWatcher(health.path, output, clock=clock)
    assert watcher.poll() == {"written": 1, "observer_state": "healthy"}
    [record] = records(output)
    assert record == {
        **health.alerts()[0],
        "version": 1,
        "kind": "health_alert",
        "exported_at": 100.0,
        "logical_id": "fleet",
    }
    assert health.path.read_bytes() == health_bytes
    assert set(record) == {
        "version",
        "kind",
        "exported_at",
        "logical_id",
        "alert_id",
        "code",
        "scope",
        "account_id",
        "target",
        "state",
        "first_seen",
        "last_seen",
        "occurrences",
        "resolved_at",
    }
    assert watcher.cursor_path == output.with_name(output.name + ".cursor.json")


def test_repeat_occurrences_coalesce_and_restart_exports_resolution_once(local):
    clock, health, output = local
    health.report("a", "account_anomaly")
    watcher = AlertWatcher(health.path, output, clock=clock)
    assert watcher.poll()["written"] == 1
    health.report("a", "account_anomaly")
    assert health.alerts()[0]["occurrences"] == 2
    assert watcher.poll()["written"] == 0
    restarted = AlertWatcher(health.path, output, clock=clock)
    assert restarted.poll()["written"] == 0
    clock.value = 105
    health.reset("a", reason="sensitive operator reason never exported")
    assert restarted.poll() == {"written": 1, "observer_state": "healthy"}
    assert [record["state"] for record in records(output)] == ["active", "resolved"]
    assert records(output)[-1]["occurrences"] == 2
    assert "sensitive operator reason" not in output.read_text(encoding="utf-8")
    assert restarted.poll()["written"] == 0


def test_new_incident_after_resolution_is_new_fingerprint(local):
    clock, health, output = local
    health.observe("a", connected=False)
    watcher = AlertWatcher(health.path, output, clock=clock)
    watcher.poll()
    observe_good(health)
    watcher.poll()
    health.observe("a", connected=False)
    assert watcher.poll()["written"] == 1
    exported = records(output)
    assert [record["state"] for record in exported] == ["active", "resolved", "active"]
    assert exported[0]["alert_id"] != exported[-1]["alert_id"]


def test_freshness_exact_boundary_staleness_recovery_and_persistence(local):
    clock, health, output = local
    observe_good(health)
    watcher = AlertWatcher(health.path, output, clock=clock)
    assert watcher.poll() == {"written": 0, "observer_state": "healthy"}
    clock.value = 130
    assert watcher.poll() == {"written": 0, "observer_state": "healthy"}
    clock.value = 130.1
    assert watcher.poll() == {"written": 1, "observer_state": "stale"}
    assert records(output)[-1]["code"] == "observer_stale"
    assert records(output)[-1]["state"] == "active"
    restarted = AlertWatcher(health.path, output, clock=clock)
    assert restarted.poll() == {"written": 0, "observer_state": "stale"}
    observe_good(health)
    assert restarted.poll() == {"written": 1, "observer_state": "healthy"}
    assert records(output)[-1]["code"] == "observer_stale"
    assert records(output)[-1]["state"] == "resolved"


def test_no_account_evidence_is_stale_not_false_healthy(local):
    clock, health, output = local
    watcher = AlertWatcher(health.path, output, clock=clock)
    assert watcher.poll() == {"written": 1, "observer_state": "stale"}
    assert records(output)[0]["code"] == "observer_stale"


def test_latest_account_update_is_journal_heartbeat(local):
    clock, health, output = local
    observe_good(health, "a")
    clock.value = 150
    observe_good(health, "b")
    watcher = AlertWatcher(health.path, output, clock=clock)
    assert watcher.poll()["observer_state"] == "healthy"


def test_missing_health_never_created_unavailable_coalesced_then_recovery(tmp_path):
    clock = Clock()
    path = tmp_path / "health-does-not-exist.sqlite3"
    output = tmp_path / "alerts.jsonl"
    watcher = AlertWatcher(path, output, clock=clock)
    assert watcher.poll() == {"written": 1, "observer_state": "unavailable"}
    assert not path.exists()
    assert watcher.poll() == {"written": 0, "observer_state": "unavailable"}
    health = HealthState(path, "fleet", clock=clock)
    observe_good(health)
    assert watcher.poll() == {"written": 1, "observer_state": "healthy"}
    assert [(row["code"], row["state"]) for row in records(output)] == [
        ("observer_unavailable", "active"),
        ("observer_unavailable", "resolved"),
    ]


def test_invalid_schema_emits_no_sql_or_exception_details(local):
    clock, health, output = local
    observe_good(health)
    watcher = AlertWatcher(health.path, output, clock=clock)
    watcher.poll()
    with sqlite3.connect(health.path) as db:
        db.execute("PRAGMA user_version=999")
    assert watcher.poll() == {"written": 1, "observer_state": "unavailable"}
    assert records(output) == [
        {
            "version": 1,
            "kind": "observer",
            "exported_at": 100.0,
            "code": "observer_unavailable",
            "state": "active",
        }
    ]
    with sqlite3.connect(health.path) as db:
        db.execute("PRAGMA user_version=1")
    assert watcher.poll() == {"written": 1, "observer_state": "healthy"}


def test_unavailable_to_stale_then_healthy_emits_condition_transitions(local):
    clock, health, output = local
    watcher = AlertWatcher(health.path, output, clock=clock)
    assert watcher.poll()["observer_state"] == "stale"
    health.path.unlink()
    assert watcher.poll() == {"written": 2, "observer_state": "unavailable"}
    restored = HealthState(health.path, "fleet", clock=clock)
    assert watcher.poll() == {"written": 2, "observer_state": "stale"}
    observe_good(restored)
    assert watcher.poll() == {"written": 1, "observer_state": "healthy"}


def test_namespace_replacement_is_unavailable_not_reused_alert_ids(local):
    clock, health, output = local
    observe_good(health)
    watcher = AlertWatcher(health.path, output, clock=clock)
    watcher.poll()
    health.path.unlink()
    replacement = HealthState(health.path, "different", clock=clock)
    replacement.report("a", "account_anomaly")
    assert watcher.poll() == {"written": 1, "observer_state": "unavailable"}
    assert all(row["kind"] == "observer" for row in records(output))


def test_output_before_cursor_failure_replays_at_least_once(local, monkeypatch):
    clock, health, output = local
    health.report("a", "account_anomaly")
    watcher = AlertWatcher(health.path, output, clock=clock)

    def crash(_):
        raise OSError("simulated crash after fsynced output")

    monkeypatch.setattr(watcher, "_save_cursor", crash)
    with pytest.raises(OSError):
        watcher.poll()
    assert not watcher.cursor_path.exists()
    assert len(records(output)) == 1
    restarted = AlertWatcher(health.path, output, clock=clock)
    assert restarted.poll()["written"] == 1
    assert records(output)[0] == records(output)[1]
    assert restarted.poll()["written"] == 0


def test_output_write_failure_never_advances_cursor(local, monkeypatch):
    clock, health, output = local
    observe_good(health)
    watcher = AlertWatcher(health.path, output, clock=clock)
    watcher.poll()
    cursor_before = watcher.cursor_path.read_bytes()
    health.report("a", "account_anomaly")
    original = type(output).open

    def deny_append(path, mode="r", *args, **kwargs):
        if path == output and mode == "ab":
            raise OSError("simulated output failure")
        return original(path, mode, *args, **kwargs)

    monkeypatch.setattr(type(output), "open", deny_append)
    with pytest.raises(OSError):
        watcher.poll()
    assert watcher.cursor_path.read_bytes() == cursor_before


def test_atomic_cursor_replace_fsync_and_no_leftover_temp(local, monkeypatch):
    clock, health, output = local
    health.report("a", "account_anomaly")
    watcher = AlertWatcher(health.path, output, clock=clock)
    calls = []
    original_replace, original_fsync = os.replace, os.fsync

    def replace(source, destination):
        assert output.exists() and records(output)
        assert source.parent == destination.parent
        calls.append("replace")
        return original_replace(source, destination)

    def fsync(descriptor):
        calls.append("fsync")
        return original_fsync(descriptor)

    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(os, "fsync", fsync)
    watcher.poll()
    assert calls.count("fsync") >= 2
    assert calls.index("replace") >= 2
    assert list(output.parent.glob("*.tmp")) == []
    assert list(output.parent.glob(".*.tmp")) == []


@pytest.mark.parametrize(
    "bad",
    [
        b"not JSON\n",
        b"{}\n",
        b'{"token":"DO-NOT-READ"}\n',
        b'{"version":1,"kind":"observer","exported_at":100,"code":"observer_stale","state":"active"}',
        b'{"version":1,"kind":"observer","exported_at":NaN,"code":"observer_stale","state":"active"}\n',
        b'{"version":1,"kind":"observer","exported_at":100,"code":"observer_stale","state":"active","token":"secret"}\n',
        b'{"version":1,"version":1,"kind":"observer","exported_at":100,"code":"observer_stale","state":"active"}\n',
        b"x" * 17000 + b"\n",
    ],
    ids=["text", "object", "token", "partial", "nan", "extra", "duplicate", "oversize"],
)
def test_existing_arbitrary_or_incomplete_output_refused_unchanged(local, bad):
    clock, health, output = local
    output.write_bytes(bad)
    with pytest.raises(ValueError, match="existing alert output"):
        AlertWatcher(health.path, output, clock=clock)
    assert output.read_bytes() == bad
    assert not output.with_name(output.name + ".cursor.json").exists()


@pytest.mark.parametrize(
    "bad",
    [b"not JSON", b"{}", b"[]", b'{"version":1}', b"x" * 262145],
    ids=["text", "empty", "list", "partial", "oversize"],
)
def test_malformed_cursor_refused_without_any_write(local, bad):
    clock, health, output = local
    output.touch()
    cursor = output.with_name(output.name + ".cursor.json")
    cursor.write_bytes(bad)
    with pytest.raises(ValueError, match="cursor"):
        AlertWatcher(health.path, output, clock=clock)
    assert cursor.read_bytes() == bad
    assert output.read_bytes() == b""


def test_modified_cursor_at_runtime_refused_and_missing_output_fail_closed(local):
    clock, health, output = local
    watcher = AlertWatcher(health.path, output, clock=clock)
    watcher.poll()
    before = output.read_bytes()
    watcher.cursor_path.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="cursor"):
        watcher.poll()
    assert output.read_bytes() == before
    output.unlink()
    with pytest.raises(ValueError, match="cursor exists without"):
        watcher.poll()
    assert not output.exists()


@pytest.mark.parametrize(
    "name",
    [
        "fleet.db",
        "admission.sqlite3",
        "delivery-budget.sqlite",
        "health.sqlite3-wal",
        "fleet.db-journal",
    ],
)
def test_database_named_output_or_cursor_never_created(local, name):
    clock, health, output = local
    destination = output.parent / name
    with pytest.raises(ValueError, match="database"):
        AlertWatcher(health.path, destination, clock=clock)
    assert not destination.exists() or destination == health.path


def test_health_path_alias_and_hardlink_cannot_be_output(local):
    clock, health, output = local
    before = health.path.read_bytes()
    with pytest.raises(ValueError):
        AlertWatcher(health.path, health.path, clock=clock)
    os.link(health.path, output)
    with pytest.raises(ValueError, match="alias"):
        AlertWatcher(health.path, output, clock=clock)
    assert health.path.read_bytes() == before


def test_other_sqlite_database_disguised_as_jsonl_is_not_overwritten(local):
    clock, health, output = local
    with sqlite3.connect(output) as db:
        db.execute("CREATE TABLE protected(content TEXT)")
        db.execute("INSERT INTO protected VALUES ('private')")
    before = output.read_bytes()
    with pytest.raises(ValueError, match="existing alert output"):
        AlertWatcher(health.path, output, clock=clock)
    assert output.read_bytes() == before


def test_cursor_copied_from_other_source_is_refused(local):
    clock, health, output = local
    watcher = AlertWatcher(health.path, output, clock=clock)
    watcher.poll()
    with pytest.raises(ValueError, match="cursor"):
        AlertWatcher(output.parent / "another-health.sqlite3", output, clock=clock)


def test_cursor_last_poll_time_prevents_clock_rollback_freshness_reset(local):
    clock, health, output = local
    observe_good(health)
    watcher = AlertWatcher(health.path, output, clock=clock)
    clock.value = 200
    assert watcher.poll()["observer_state"] == "stale"
    clock.value = 100
    restarted = AlertWatcher(health.path, output, clock=clock)
    assert restarted.poll() == {"written": 0, "observer_state": "stale"}
    assert json.loads(restarted.cursor_path.read_text(encoding="utf-8"))["last_poll_at"] == 200


@pytest.mark.parametrize("stale_after", [True, False, 0, -1, float("nan"), float("inf"), "30", 86401])
def test_invalid_stale_interval(local, stale_after):
    clock, health, output = local
    with pytest.raises(ValueError):
        AlertWatcher(health.path, output, clock=clock, stale_after=stale_after)
    assert not output.exists()


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), -1, "100"])
def test_invalid_clock_fails_before_any_output(local, value):
    clock, health, output = local
    clock.value = value
    watcher = AlertWatcher(health.path, output, clock=clock)
    with pytest.raises(ValueError):
        watcher.poll()
    assert not output.exists()


def test_full_journal_window_warns_and_cursor_remains_bounded(local):
    clock, health, output = local
    # Generate real journal incidents in one local transaction, not 1001 network calls.
    observe_good(health)
    with sqlite3.connect(health.path) as db:
        db.executemany(
            "INSERT INTO health_alerts(code,scope,account_id,target,state,first_seen,last_seen,occurrences) "
            "VALUES ('account_anomaly','account',?,'','active',100,100,1)",
            [(f"account_{index}",) for index in range(1001)],
        )
    watcher = AlertWatcher(health.path, output, clock=clock)
    assert watcher.poll() == {"written": 1001, "observer_state": "healthy"}
    exported = records(output)
    assert exported[0]["code"] == "journal_window_full" and exported[0]["state"] == "active"
    assert len([row for row in exported if row["kind"] == "health_alert"]) == 1000
    cursor = json.loads(watcher.cursor_path.read_text(encoding="utf-8"))
    assert len(cursor["alerts"]) == 1000
    assert watcher.cursor_path.stat().st_size < 100000
    assert watcher.poll()["written"] == 0
    restarted = AlertWatcher(health.path, output, clock=clock)
    assert restarted.poll()["written"] == 0


@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"observer_state": "unknown"},
        {"logical_id": "private message"},
        {"last_poll_at": True},
        {"journal_window_full": 1},
        {"alerts": {"0": "active"}},
        {"alerts": {"9223372036854775808": "active"}},
        {"alerts": {"1": "arbitrary text"}},
        {"alerts": {str(index + 1): "active" for index in range(1001)}},
    ],
    ids=["version", "state", "namespace", "time", "flag", "zero", "id-overflow", "fingerprint", "overfull"],
)
def test_structurally_invalid_cursor_fails_closed(local, change):
    clock, health, output = local
    watcher = AlertWatcher(health.path, output, clock=clock)
    watcher.poll()
    before = output.read_bytes()
    cursor = json.loads(watcher.cursor_path.read_text(encoding="utf-8")) | change
    watcher.cursor_path.write_text(json.dumps(cursor), encoding="utf-8")
    with pytest.raises(ValueError, match="cursor"):
        watcher.poll()
    assert output.read_bytes() == before


def test_output_revalidated_each_poll_without_truncation(local):
    clock, health, output = local
    watcher = AlertWatcher(health.path, output, clock=clock)
    watcher.poll()
    with output.open("ab") as stream:
        stream.write(b'{"arbitrary":"external-write"}\n')
    before = output.read_bytes()
    cursor_before = watcher.cursor_path.read_bytes()
    with pytest.raises(ValueError, match="existing alert output"):
        watcher.poll()
    assert output.read_bytes() == before
    assert watcher.cursor_path.read_bytes() == cursor_before


def test_watcher_never_calls_health_constructor(local, monkeypatch):
    clock, health, output = local
    observe_good(health)

    def forbidden(*args, **kwargs):
        raise AssertionError("watcher must not initialize or configure health")

    monkeypatch.setattr(HealthState, "__init__", forbidden)
    assert AlertWatcher(health.path, output, clock=clock).poll()["observer_state"] == "healthy"


def test_extra_health_fields_are_whitelisted_out(local, monkeypatch):
    clock, health, output = local
    health.report("a", "account_anomaly")
    original = HealthState.alerts

    def extra_fields(self, limit=100):
        return [row | {"message": "SECRET-MESSAGE", "token": "SECRET-TOKEN"} for row in original(self, limit)]

    monkeypatch.setattr(HealthState, "alerts", extra_fields)
    watcher = AlertWatcher(health.path, output, clock=clock)
    assert watcher.poll()["written"] == 1
    text = output.read_text(encoding="utf-8")
    assert "SECRET" not in text and "token" not in text and "message" not in text
