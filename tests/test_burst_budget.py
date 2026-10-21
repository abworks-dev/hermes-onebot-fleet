"""Local-only rolling burst caps, permanent charges and guarded v2 migration."""

import hashlib
import json
import multiprocessing
import sqlite3
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import FrozenInstanceError, asdict

import pytest

from hermes_onebot_fleet.delivery_budget import BudgetPolicy, DeliveryBudget


class Clock:
    def __init__(self, value=1000):
        self.value = value

    def __call__(self):
        return self.value


def policy(**changes):
    values = dict(
        global_per_hour=3600,
        target_per_hour=3600,
        min_interval_seconds=1,
        target_interval_seconds=1,
    )
    values.update(changes)
    return BudgetPolicy(**values)


def budget(tmp_path, clock, **changes):
    return DeliveryBudget(tmp_path / "budget.sqlite3", "fleet", policy(**changes), clock=clock)


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def sql(path, statement, args=()):
    with closing(sqlite3.connect(path)) as db, db:
        return db.execute(statement, args).fetchall()


def history(path):
    return (
        sql(path, "SELECT * FROM budget_reservations ORDER BY rowid"),
        sql(path, "SELECT * FROM budget_audit ORDER BY id"),
        sql(path, "SELECT last_wallclock FROM budget_meta"),
    )


def legacy(tmp_path, *, count=4, name="legacy.sqlite3"):
    """Independent exact v2 DDL/policy fixture; never use the v3 initializer."""
    path = tmp_path / name
    statements = (
        "CREATE TABLE budget_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "logical_id TEXT NOT NULL, policy TEXT NOT NULL, last_wallclock REAL NOT NULL)",
        "CREATE TABLE budget_reservations (key_hash TEXT PRIMARY KEY, target_hash TEXT NOT NULL, "
        "reserved_at REAL NOT NULL, global_next_at REAL NOT NULL, target_next_at REAL NOT NULL, "
        "paced_at REAL NOT NULL)",
        "CREATE INDEX budget_global_time ON budget_reservations(reserved_at)",
        "CREATE INDEX budget_target_time ON budget_reservations(target_hash, reserved_at)",
        "CREATE TABLE budget_audit (id INTEGER PRIMARY KEY, key_hash TEXT NOT NULL, "
        "target_hash TEXT NOT NULL, at REAL NOT NULL, allowed INTEGER NOT NULL, "
        "reason TEXT NOT NULL, next_at REAL NOT NULL)",
    )
    old_policy = {
        "global_per_hour": 60,
        "target_per_hour": 30,
        "min_interval_seconds": 1,
        "target_interval_seconds": 1,
        "max_pending_age_seconds": 900,
    }
    with closing(sqlite3.connect(path)) as db, db:
        for statement in statements:
            db.execute(statement)
        db.execute("PRAGMA application_id=0x48464231")
        db.execute("PRAGMA user_version=2")
        last = 1000 + max(count - 1, 0)
        db.execute("INSERT INTO budget_meta VALUES (1,'fleet',?,?)", (json.dumps(old_policy), last))
        for index in range(count):
            at = 1000 + index
            key, target = digest(f"old-{index}"), digest("same")
            db.execute(
                "INSERT INTO budget_reservations VALUES (?,?,?,?,?,?)", (key, target, at, at + 1, at + 1, at)
            )
            db.execute(
                "INSERT INTO budget_audit(key_hash,target_hash,at,allowed,reason,next_at) "
                "VALUES (?,?,?,1,'reserved',?)",
                (key, target, at, at),
            )
        if count:
            db.execute(
                "INSERT INTO budget_audit(key_hash,target_hash,at,allowed,reason,next_at) "
                "VALUES (?,?,?,0,'global_interval',?)",
                (digest("old-denial"), digest("different"), last, last + 1),
            )
    return path


def process_attempt(args):
    path, index = args
    handle = DeliveryBudget.open_existing(path, clock=lambda: 1006)
    return handle.reserve(f"process-{index}", f"target-{index}")


def test_new_defaults_are_frozen_and_preserve_old_positional_api():
    default = BudgetPolicy()
    assert default.global_per_minute == 6
    assert default.target_per_minute == 3
    assert BudgetPolicy(1, 2, 3, 4, 5) == BudgetPolicy(
        global_per_hour=1,
        target_per_hour=2,
        min_interval_seconds=3,
        target_interval_seconds=4,
        max_pending_age_seconds=5,
    )
    assert BudgetPolicy(1, 2, 3, 4, 5, 7, 8).global_per_minute == 7
    with pytest.raises(FrozenInstanceError):
        default.global_per_minute = 99


@pytest.mark.parametrize("name", ["global_per_minute", "target_per_minute"])
@pytest.mark.parametrize("value", [None, True, False, 0, -1, 3601, 1.0, "6", float("inf"), float("nan")])
def test_minute_caps_are_strict_direct_and_mapping(name, value):
    with pytest.raises(ValueError):
        BudgetPolicy(**{name: value})
    with pytest.raises(ValueError):
        BudgetPolicy.from_mapping({name: value})


def test_minute_policy_boundaries_no_aliases_or_disable_switch():
    assert policy(global_per_minute=1, target_per_minute=3600).target_per_minute == 3600
    for mapping in ({"disabled": True}, {"enabled": False}, {"account_per_minute": 6}):
        with pytest.raises(ValueError):
            BudgetPolicy.from_mapping(mapping)


def test_default_global_minute_cap_across_clients_routes_types_and_restart(tmp_path):
    clock = Clock()
    first = budget(tmp_path, clock)
    second = DeliveryBudget.open_existing(first.path, clock=clock)
    for index in range(6):
        clock.value = 1000 + index * 5
        handle = first if index % 2 else second
        assert handle.reserve(f"client-{index}:reply-or-publish", f"group-or-private:{index}")["allowed"]
    clock.value = 1030
    result = second.reserve("another-account:another-client", "fresh-route")
    assert result == {"allowed": False, "reason": "global_minute_limit", "next_at": 1060.0}
    assert first.status()["reserved_last_minute"] == 6
    assert first.status()["reserved_last_hour"] == 6
    assert first.status()["next_global_at"] == 1060
    restarted = DeliveryBudget.open_existing(first.path, clock=clock)
    clock.value = 1059.999
    assert not restarted.reserve("not-yet", "fresh-route")["allowed"]
    clock.value = 1060
    assert restarted.reserve("at-boundary", "fresh-route")["allowed"]
    assert restarted.status()["reserved_last_minute"] == 6
    assert restarted.status()["reservations_total"] == 7


def test_default_target_minute_cap_and_other_target_is_independent(tmp_path):
    clock = Clock()
    handle = budget(tmp_path, clock)
    for index in range(3):
        clock.value = 1000 + index * 10
        assert handle.reserve(str(index), "same")["allowed"]
    clock.value = 1030
    assert handle.reserve("blocked", "same") == {
        "allowed": False,
        "reason": "target_minute_limit",
        "next_at": 1060.0,
    }
    assert handle.reserve("other", "other-target")["allowed"]
    clock.value = 1060
    assert handle.reserve("blocked", "same")["allowed"]
    assert handle.audit(1)[0]["reason"] == "reserved"


@pytest.mark.parametrize(
    "changes,reason,deadline",
    [
        ({"global_per_minute": 1}, "global_minute_limit", 1060),
        ({"target_per_minute": 1}, "target_minute_limit", 1060),
        ({"global_per_hour": 1}, "global_hourly_limit", 4600),
        ({"target_per_hour": 1}, "target_hourly_limit", 4600),
        ({"min_interval_seconds": 70}, "global_interval", 1070),
        ({"target_interval_seconds": 80}, "target_interval", 1080),
    ],
)
def test_all_four_caps_and_both_intervals_share_check_and_reserve(tmp_path, changes, reason, deadline):
    clock = Clock()
    handle = budget(tmp_path, clock, **changes)
    assert handle.reserve("first", "same")["allowed"]
    clock.value = 1002
    before = handle.path.read_bytes()
    expected = {"allowed": False, "reason": reason, "next_at": float(deadline)}
    assert handle.check("second", "same") == expected
    assert handle.path.read_bytes() == before
    assert handle.reserve("second", "same") == expected
    assert handle.audit(1)[0]["reason"] == reason
    assert handle.status()["reservations_total"] == 1


def test_rolling_window_is_not_wall_minute_bucket_and_idle_does_not_bank_credits(tmp_path):
    clock = Clock(1019)
    handle = budget(tmp_path, clock, global_per_minute=1)
    handle.reserve("first", "one")
    clock.value = 1020  # A calendar minute boundary changes nothing.
    assert handle.reserve("second", "two")["next_at"] == 1079
    clock.value = 1079
    assert handle.reserve("second", "two")["allowed"]
    clock.value = 100000
    assert handle.reserve("after-idle", "three")["allowed"]
    assert not handle.reserve("no-banked-burst", "four")["allowed"]


def test_completion_recovery_unknown_and_denials_never_refund_or_move_charge_time(tmp_path):
    clock = Clock()
    handle = budget(tmp_path, clock, global_per_minute=1)
    handle.reserve("unknown-or-rejected", "same")
    charge = sql(handle.path, "SELECT key_hash,target_hash,reserved_at FROM budget_reservations")
    clock.value = 1010
    handle.complete("unknown-or-rejected")
    clock.value = 1020
    handle.recover()
    for index in range(5):
        clock.value += 1
        assert handle.reserve(f"new-{index}", "another")["next_at"] == 1060
    assert sql(handle.path, "SELECT key_hash,target_hash,reserved_at FROM budget_reservations") == charge
    assert handle.status()["reservations_total"] == 1
    clock.value = 1060
    assert handle.reserve("fresh", "another")["allowed"]
    clock.value = 100000
    assert handle.reserve("unknown-or-rejected", "different")["reason"] == "prior_reservation"


def test_tighten_then_loosen_without_reservation_commits_interval_fences(tmp_path):
    clock = Clock()
    stale = budget(tmp_path, clock)
    stale.reserve("one", "same")
    before = history(stale.path)
    clock.value = 1005
    DeliveryBudget(
        stale.path, "fleet", policy(min_interval_seconds=70, target_interval_seconds=80), clock=clock
    )
    DeliveryBudget(stale.path, "fleet", policy(), clock=clock)
    assert stale.check("next", "different")["next_at"] == 1070
    assert stale.check("next", "same")["next_at"] == 1080
    after = history(stale.path)
    assert [row[:3] + row[5:] for row in before[0]] == [row[:3] + row[5:] for row in after[0]]
    assert before[1:] == after[1:]


def test_tightened_minute_caps_count_all_existing_charges_and_use_count_minus_cap(tmp_path):
    clock = Clock()
    stale = budget(tmp_path, clock, global_per_minute=10, target_per_minute=10)
    for index in range(4):
        clock.value = 1000 + index
        assert stale.reserve(str(index), "same")["allowed"]
    clock.value = 1004
    DeliveryBudget(stale.path, "fleet", policy(global_per_minute=2, target_per_minute=1), clock=clock)
    assert stale.policy.global_per_minute == 2
    assert stale.check("new", "other")["next_at"] == 1062
    assert stale.check("new", "same")["next_at"] == 1063
    clock.value = 1062
    assert stale.reserve("new", "other")["allowed"]
    assert stale.reserve("same-wait", "same")["next_at"] == 1063
    clock.value = 1063
    assert stale.reserve("same-wait", "same")["allowed"]


def test_clock_rollback_restart_and_read_only_check_do_not_move_committed_clock(tmp_path):
    clock = Clock()
    handle = budget(tmp_path, clock, global_per_minute=1)
    handle.reserve("first", "one")
    clock.value = 1059
    assert not handle.reserve("denied", "two")["allowed"]
    clock.value = 1
    reopened = DeliveryBudget.open_existing(handle.path, clock=clock)
    assert reopened.status()["now"] == 1059
    assert reopened.reserve("later", "two")["next_at"] == 1060
    clock.value = 5000
    before = handle.path.read_bytes()
    assert reopened.check("later", "two")["allowed"]
    assert handle.path.read_bytes() == before
    clock.value = 1
    assert reopened.status()["now"] == 1059
    clock.value = 1060
    assert reopened.reserve("later", "two")["allowed"]


def test_check_is_never_a_permit_or_a_charge_and_final_reserve_rechecks_latest_policy(tmp_path):
    clock = Clock()
    handle = budget(tmp_path, clock)
    before = handle.path.read_bytes()
    assert handle.check("planned", "one")["allowed"]
    assert handle.path.read_bytes() == before
    assert handle.status()["reservations_total"] == 0
    other = DeliveryBudget(handle.path, "fleet", policy(global_per_minute=1), clock=clock)
    assert other.reserve("other-dispatch", "two")["allowed"]
    clock.value += 1
    assert handle.reserve("planned", "one")["reason"] == "global_minute_limit"


def test_thread_handles_race_at_minute_boundary_only_one_available_slot(tmp_path):
    clock = Clock()
    handle = budget(tmp_path, clock)
    for index in range(6):
        clock.value = 1000 + index
        assert handle.reserve(f"seed-{index}", f"seed-{index}")["allowed"]
    clock.value = 1060
    handles = [DeliveryBudget.open_existing(handle.path, clock=clock) for _ in range(12)]
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(
            pool.map(lambda index: handles[index % 12].reserve(f"race-{index}", str(index)), range(24))
        )
    assert sum(result["allowed"] for result in results) == 1
    assert handle.status()["reserved_last_minute"] == 6
    assert handle.status()["reservations_total"] == 7


def test_processes_cannot_spend_past_shared_minute_cap(tmp_path):
    clock = Clock()
    handle = budget(tmp_path, clock)
    for index in range(6):
        clock.value = 1000 + index
        assert handle.reserve(f"seed-{index}", str(index))["allowed"]
    args = [(str(handle.path), index) for index in range(8)]
    with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context("spawn")) as pool:
        results = list(pool.map(process_attempt, args))
    assert not any(result["allowed"] for result in results)
    assert all(result["reason"] == "global_minute_limit" for result in results)


def test_v2_diagnostics_and_stale_handle_never_migrate_or_write(tmp_path):
    path = legacy(tmp_path)
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="schema mismatch"):
        DeliveryBudget.open_existing(path, clock=Clock())
    stale = DeliveryBudget.__new__(DeliveryBudget)
    stale.path, stale.logical_id, stale._clock = path, "fleet", Clock()
    for action in (
        stale.status,
        stale.audit,
        lambda: stale.check("no", "same"),
        lambda: stale.reserve("no", "same"),
        stale.recover,
        lambda: stale.complete("old-0"),
    ):
        with pytest.raises(RuntimeError, match="schema mismatch"):
            action()
        assert path.read_bytes() == before


def test_constructor_migrates_exact_v2_retains_history_and_enforces_requested_caps(tmp_path):
    path = legacy(tmp_path)
    original = history(path)
    clock = Clock(1004)
    requested = policy(global_per_minute=2, target_per_minute=1)
    migrated = DeliveryBudget(path, "fleet", requested, clock=clock)
    assert sql(path, "PRAGMA user_version") == [(3,)]
    assert history(path) == original
    assert migrated.policy == requested
    assert migrated.check("new", "different")["next_at"] == 1062
    assert migrated.check("new", "same")["next_at"] == 1063
    assert migrated.reserve("old-0", "different")["reason"] == "prior_reservation"
    assert migrated.status()["reserved_last_minute"] == 4
    assert migrated.status()["reserved_last_hour"] == 4
    encoded = json.dumps([migrated.status(), migrated.audit()], allow_nan=False)
    assert "old-0" not in encoded and "different" not in encoded


def test_migration_extends_only_deadlines_and_preserves_finish_clock_and_tombstones(tmp_path):
    path = legacy(tmp_path, count=1)
    before = history(path)
    requested = policy(min_interval_seconds=70, target_interval_seconds=80)
    migrated = DeliveryBudget(path, "fleet", requested, clock=Clock(99999))
    after = history(path)
    assert after[0][0] == before[0][0][:3] + (1070.0, 1080.0) + before[0][0][5:]
    assert after[1:] == before[1:]
    # Construction is not crash recovery and does not pretend an RPC finished now.
    assert migrated.status()["last_wallclock"] == 1000
    assert migrated.reserve("old-0", "other")["reason"] == "prior_reservation"


def test_empty_legacy_migration_and_concurrent_constructor_only_once(tmp_path):
    path = legacy(tmp_path, count=0)
    clock = Clock()
    with ThreadPoolExecutor(max_workers=8) as pool:
        handles = list(pool.map(lambda _: DeliveryBudget(path, "fleet", policy(), clock=clock), range(8)))
    assert all(handle.status()["reservations_total"] == 0 for handle in handles)
    assert sql(path, "PRAGMA user_version") == [(3,)]


@pytest.mark.parametrize(
    "change",
    [
        "PRAGMA user_version=1",
        "PRAGMA user_version=999",
        "PRAGMA application_id=1",
        "DROP INDEX budget_global_time",
        "DROP INDEX budget_target_time",
        "CREATE TABLE unrelated(x TEXT)",
        "CREATE VIEW alien AS SELECT 1",
        "CREATE TRIGGER alien AFTER INSERT ON budget_reservations BEGIN SELECT 1; END",
        "DELETE FROM budget_meta",
        "UPDATE budget_meta SET logical_id='different'",
        "UPDATE budget_meta SET policy='{}'",
        "UPDATE budget_meta SET policy='null'",
        "UPDATE budget_meta SET last_wallclock=1e999",
        "UPDATE budget_meta SET last_wallclock=999",
        "UPDATE budget_reservations SET key_hash='invalid' WHERE rowid=(SELECT min(rowid) FROM budget_reservations)",
        "UPDATE budget_reservations SET target_hash='invalid'",
        "UPDATE budget_reservations SET reserved_at='bad'",
        "UPDATE budget_reservations SET reserved_at=-1",
        "UPDATE budget_reservations SET reserved_at=1e999",
        "UPDATE budget_reservations SET paced_at=999",
        "UPDATE budget_reservations SET paced_at=1004",
        "UPDATE budget_reservations SET paced_at=1e999",
        "UPDATE budget_reservations SET global_next_at='bad'",
        "UPDATE budget_reservations SET target_next_at=1e999",
        "UPDATE budget_reservations SET global_next_at=999",
        "UPDATE budget_audit SET key_hash='bad'",
        "UPDATE budget_audit SET target_hash='bad'",
        "UPDATE budget_audit SET at='bad'",
        "UPDATE budget_audit SET at=1004",
        "UPDATE budget_audit SET next_at=1e999",
        "UPDATE budget_audit SET allowed=2",
        "UPDATE budget_audit SET reason='unrecognized'",
        "DELETE FROM budget_audit WHERE allowed=1",
        "DELETE FROM budget_reservations",
        "UPDATE budget_audit SET next_at=999",
    ],
)
def test_bad_legacy_schema_metadata_or_history_rejects_migration_without_writes(tmp_path, change):
    path = legacy(tmp_path)
    sql(path, change)
    before = path.read_bytes()
    with pytest.raises((RuntimeError, sqlite3.DatabaseError)):
        DeliveryBudget(path, "fleet", policy(), clock=Clock(2000))
    assert path.read_bytes() == before
    assert sql(path, "PRAGMA user_version")[0][0] != 3


@pytest.mark.parametrize("version", [2, 3])
@pytest.mark.parametrize(
    "mutation", ["duplicate", "unknown", "missing", "null", "bool", "nonfinite", "float-cap"]
)
def test_persisted_policy_is_exact_duplicate_safe_and_strict(tmp_path, version, mutation):
    if version == 2:
        path = legacy(tmp_path)
    else:
        path = budget(tmp_path, Clock()).path
    mapping = json.loads(sql(path, "SELECT policy FROM budget_meta")[0][0])
    if mutation == "duplicate":
        encoded = json.dumps(mapping)[:-1] + ',"global_per_hour":60}'
    else:
        if mutation == "unknown":
            mapping["unrecognized"] = 1
        elif mutation == "missing":
            mapping.pop("global_per_hour")
        elif mutation == "null":
            mapping["global_per_hour"] = None
        elif mutation == "bool":
            mapping["global_per_hour"] = True
        elif mutation == "nonfinite":
            mapping["min_interval_seconds"] = float("inf")
        else:
            mapping["global_per_hour"] = 60.0
        encoded = json.dumps(mapping)
    sql(path, "UPDATE budget_meta SET policy=?", (encoded,))
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="persisted delivery budget metadata"):
        DeliveryBudget(path, "fleet", policy(), clock=Clock())
    assert path.read_bytes() == before


def test_exact_v2_schema_does_not_adopt_modified_column_affinities(tmp_path):
    path = legacy(tmp_path, count=0)
    sql(path, "ALTER TABLE budget_meta RENAME TO old_meta")
    sql(
        path,
        "CREATE TABLE budget_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "logical_id BLOB NOT NULL, policy TEXT NOT NULL, last_wallclock REAL NOT NULL)",
    )
    sql(path, "INSERT INTO budget_meta SELECT * FROM old_meta")
    sql(path, "DROP TABLE old_meta")
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="schema mismatch"):
        DeliveryBudget(path, "fleet", policy(), clock=Clock())
    assert path.read_bytes() == before


def test_migration_transaction_rolls_back_fences_policy_and_version_on_failed_commit(tmp_path, monkeypatch):
    path = legacy(tmp_path)
    before = path.read_bytes()
    original = DeliveryBudget._transaction

    class FailCommit:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, statement, *args):
            if statement.startswith("PRAGMA user_version="):
                raise sqlite3.OperationalError("simulated migration failure")
            return self.connection.execute(statement, *args)

    @contextmanager
    def transaction(self, **kwargs):
        with original(self, **kwargs) as db:
            yield FailCommit(db)

    monkeypatch.setattr(DeliveryBudget, "_transaction", transaction)
    with pytest.raises(sqlite3.OperationalError, match="simulated migration failure"):
        DeliveryBudget(path, "fleet", policy(min_interval_seconds=100), clock=Clock(2000))
    assert path.read_bytes() == before
    monkeypatch.undo()
    assert DeliveryBudget(path, "fleet", policy(), clock=Clock()).status()["reservations_total"] == 4


def test_unknown_v3_denial_reason_fails_closed_all_paths(tmp_path):
    clock = Clock()
    handle = budget(tmp_path, clock)
    handle.reserve("first", "one")
    handle.reserve("denial", "one")
    sql(handle.path, "UPDATE budget_audit SET reason='unknown-error' WHERE allowed=0")
    before = handle.path.read_bytes()
    for operation in (
        handle.status,
        handle.audit,
        lambda: handle.check("next", "one"),
        lambda: handle.reserve("next", "one"),
        handle.recover,
    ):
        with pytest.raises(RuntimeError, match="persisted delivery budget audit"):
            operation()
        assert handle.path.read_bytes() == before


@pytest.mark.parametrize("value", [1e308, 253402300800, 10**1000])
def test_unrepresentable_clock_rejects_constructor_before_metadata(tmp_path, value):
    path = tmp_path / "bad-clock.sqlite3"
    with pytest.raises(ValueError):
        DeliveryBudget(path, "fleet", policy(), clock=Clock(value))
    assert sql(path, "SELECT name FROM sqlite_master") == []
    assert sql(path, "PRAGMA user_version") == [(0,)]


@pytest.mark.parametrize(
    "column", ["last_wallclock", "reserved_at", "paced_at", "global_next_at", "target_next_at"]
)
def test_huge_persisted_times_fail_closed_without_poisoning_further_history(tmp_path, column):
    clock = Clock()
    handle = budget(tmp_path, clock)
    handle.reserve("first", "same")
    table = "budget_meta" if column == "last_wallclock" else "budget_reservations"
    sql(handle.path, f"UPDATE {table} SET {column}=1e308")
    before = handle.path.read_bytes()
    for operation in (
        handle.status,
        lambda: handle.reserve("next", "different"),
        lambda: DeliveryBudget(handle.path, "fleet", policy(), clock=clock),
    ):
        with pytest.raises(RuntimeError):
            operation()
        assert handle.path.read_bytes() == before


@pytest.mark.parametrize("action", ["check", "reserve"])
def test_near_epoch_end_new_charge_cannot_overflow_window_or_pacing(tmp_path, action):
    clock = Clock(253402300798)
    handle = budget(tmp_path, clock)
    before = handle.path.read_bytes()
    with pytest.raises(ValueError):
        getattr(handle, action)("no-overflow", "same")
    assert handle.path.read_bytes() == before
    assert handle.status()["reservations_total"] == 0


@pytest.mark.parametrize("action", ["complete", "recover"])
def test_near_epoch_end_completion_recovery_overflow_rolls_back(tmp_path, action):
    clock = Clock()
    handle = budget(tmp_path, clock)
    handle.reserve("first", "same")
    before = handle.path.read_bytes()
    clock.value = 253402300799
    with pytest.raises(ValueError):
        if action == "complete":
            handle.complete("first")
        else:
            handle.recover()
    assert handle.path.read_bytes() == before
    clock.value = 1000
    assert handle.status()["last_wallclock"] == 1000


def test_reconfiguration_rejects_unrepresentable_extended_deadline_without_policy_change(tmp_path):
    clock = Clock()
    handle = budget(tmp_path, clock)
    handle.reserve("first", "same")
    clock.value = 253402300797
    handle.complete("first")
    before = handle.path.read_bytes()
    with pytest.raises(ValueError):
        DeliveryBudget(handle.path, "fleet", policy(min_interval_seconds=10), clock=clock)
    assert handle.path.read_bytes() == before
    assert handle.policy.min_interval_seconds == 1


def test_legacy_migration_rejects_unrepresentable_window_expiry_unchanged(tmp_path):
    path = legacy(tmp_path, count=1)
    at = 253402300798
    sql(path, "UPDATE budget_meta SET last_wallclock=?", (at,))
    sql(
        path,
        "UPDATE budget_reservations SET reserved_at=?,paced_at=?,global_next_at=?,target_next_at=?",
        (at, at, at + 1, at + 1),
    )
    sql(path, "UPDATE budget_audit SET at=?,next_at=? WHERE allowed=1", (at, at))
    sql(path, "UPDATE budget_audit SET at=?,next_at=? WHERE allowed=0", (at, at + 1))
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="persisted delivery budget pacing"):
        DeliveryBudget(path, "fleet", policy(), clock=Clock(at))
    assert path.read_bytes() == before


def test_budget_transactions_disable_trusted_schema(tmp_path):
    handle = budget(tmp_path, Clock())
    with handle._transaction() as db:
        assert db.execute("PRAGMA trusted_schema").fetchone()[0] == 0


@pytest.mark.parametrize("version", [2, 3])
def test_persisted_policy_must_be_text_not_accepted_as_json_blob(tmp_path, version):
    path = legacy(tmp_path) if version == 2 else budget(tmp_path, Clock()).path
    encoded = sql(path, "SELECT policy FROM budget_meta")[0][0].encode()
    sql(path, "UPDATE budget_meta SET policy=?", (encoded,))
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="persisted delivery budget metadata"):
        DeliveryBudget(path, "fleet", policy(), clock=Clock())
    assert path.read_bytes() == before


@pytest.mark.parametrize("action", ["status", "audit"])
def test_cli_legacy_diagnostics_never_migrate_but_explicit_runtime_constructor_can(
    tmp_path, monkeypatch, capsys, action
):
    from hermes_onebot_fleet import cli
    from hermes_onebot_fleet.store import Store

    ledger = tmp_path / "fleet.sqlite3"
    Store(ledger)
    path = legacy(tmp_path, name=ledger.name + ".budget.sqlite3")
    before_ledger, before_budget = ledger.read_bytes(), path.read_bytes()
    monkeypatch.setattr(sys, "argv", ["fleet", "budget", "--state", str(ledger), "--action", action])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert "no QQ operation was performed" in capsys.readouterr().err
    assert (ledger.read_bytes(), path.read_bytes()) == (before_ledger, before_budget)
    assert sql(path, "PRAGMA user_version") == [(2,)]

    original = history(path)
    DeliveryBudget(path, "fleet", policy(), clock=Clock())
    assert history(path) == original
    migrated = path.read_bytes()
    cli.main()
    json.loads(capsys.readouterr().out)
    assert ledger.read_bytes() == before_ledger
    assert path.read_bytes() == migrated
    assert sql(path, "PRAGMA user_version") == [(3,)]


def test_fresh_v3_cannot_be_misidentified_as_legacy_or_downgraded_by_diagnostics(tmp_path):
    handle = budget(tmp_path, Clock())
    assert set(asdict(handle.policy)) == set(BudgetPolicy.__dataclass_fields__)
    sql(handle.path, "PRAGMA user_version=2")  # Seven policy keys are not an authentic v2 policy.
    before = handle.path.read_bytes()
    with pytest.raises(RuntimeError, match="persisted delivery budget metadata"):
        DeliveryBudget(handle.path, "fleet", policy(), clock=Clock())
    assert handle.path.read_bytes() == before
