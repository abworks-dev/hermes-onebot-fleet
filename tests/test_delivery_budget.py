import json
import multiprocessing
import sqlite3
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, asdict

import pytest

from hermes_onebot_fleet.delivery_budget import BudgetPolicy, DeliveryBudget


class Clock:
    def __init__(self, value=10000):
        self.value = value

    def __call__(self):
        return self.value


def make_budget(tmp_path, clock=None, **policy):
    return DeliveryBudget(
        tmp_path / "budget.sqlite3", "fleet", BudgetPolicy(**policy), clock=clock or Clock()
    )


def process_reservation(args):
    path, key = args
    budget = DeliveryBudget.open_existing(path, clock=lambda: 10000)
    return budget.reserve(key, "group:123")


def test_policy_defaults_strict_mapping_and_frozen():
    policy = BudgetPolicy.from_mapping({})
    assert asdict(policy) == {
        "global_per_hour": 60,
        "target_per_hour": 30,
        "min_interval_seconds": 5,
        "target_interval_seconds": 10,
        "max_pending_age_seconds": 900,
    }
    with pytest.raises(FrozenInstanceError):
        policy.global_per_hour = 10
    for mapping in (None, [], {"unknown": 1}, {"account_per_hour": 1}):
        with pytest.raises(ValueError):
            BudgetPolicy.from_mapping(mapping)


@pytest.mark.parametrize("field", ["global_per_hour", "target_per_hour"])
@pytest.mark.parametrize("value", [True, False, 0, -1, 3601, 1.0, "60", None, float("nan"), float("inf")])
def test_invalid_caps_direct_and_mapping(field, value):
    with pytest.raises(ValueError):
        BudgetPolicy(**{field: value})
    with pytest.raises(ValueError):
        BudgetPolicy.from_mapping({field: value})


@pytest.mark.parametrize(
    "field", ["min_interval_seconds", "target_interval_seconds", "max_pending_age_seconds"]
)
@pytest.mark.parametrize("value", [True, False, 0, -1, 0.99, "5", None, float("nan"), float("inf")])
def test_invalid_times_direct_and_mapping(field, value):
    with pytest.raises(ValueError):
        BudgetPolicy(**{field: value})
    with pytest.raises(ValueError):
        BudgetPolicy.from_mapping({field: value})


def test_policy_boundaries():
    BudgetPolicy(1, 3600, 1, 3600, 86400)
    BudgetPolicy(min_interval_seconds=1.5, target_interval_seconds=10.5)
    for mapping in ({"max_pending_age_seconds": 86401}, {"min_interval_seconds": 3601}):
        with pytest.raises(ValueError):
            BudgetPolicy.from_mapping(mapping)


def test_restart_duplicate_key_never_authorizes_replay_even_another_target(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    assert budget.reserve("reply-1", "group:1") == {"allowed": True, "reason": "reserved", "next_at": 10000.0}
    clock.value += 100000
    restarted = DeliveryBudget.open_existing(budget.path, clock=clock)
    for target in ("group:1", "private:2"):
        assert restarted.reserve("reply-1", target)["reason"] == "prior_reservation"
    assert restarted.status()["reservations_total"] == 1
    assert restarted.status()["reserved_last_hour"] == 0


def test_pacing_denial_is_not_reservation_and_no_accumulated_burst(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    assert budget.reserve("a", "same")["allowed"]
    assert budget.reserve("b", "different") == {
        "allowed": False,
        "reason": "global_interval",
        "next_at": 10005.0,
    }
    clock.value += 5
    assert budget.reserve("b", "different")["allowed"]
    assert budget.reserve("c", "same") == {"allowed": False, "reason": "global_interval", "next_at": 10010.0}
    clock.value += 5
    assert budget.reserve("c", "same")["allowed"]
    clock.value += 10000
    assert budget.reserve("d", "fresh")["allowed"]
    assert not budget.reserve("e", "another")["allowed"]
    assert budget.status()["reservations_total"] == 4


def test_target_pacing_survives_restart(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    assert budget.reserve("a", "same")["allowed"]
    clock.value += 5
    reopened = DeliveryBudget(budget.path, "fleet", BudgetPolicy(), clock=clock)
    assert reopened.reserve("b", "same") == {
        "allowed": False,
        "reason": "target_interval",
        "next_at": 10010.0,
    }
    clock.value += 5
    assert reopened.reserve("b", "same")["allowed"]


def test_global_hour_cap_cross_target_and_send_types(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock, global_per_hour=2, min_interval_seconds=1)
    # A second handle models another account/send path; neither is an API argument.
    standby = DeliveryBudget.open_existing(budget.path, clock=clock)
    assert budget.reserve("reply:a", "group:1")["allowed"]
    clock.value += 1
    assert standby.reserve("publish:b", "private:2")["allowed"]
    clock.value += 1
    assert budget.reserve("reply:c", "group:3") == {
        "allowed": False,
        "reason": "global_hourly_limit",
        "next_at": 13600.0,
    }
    clock.value = 13600
    assert standby.reserve("reply:c", "group:3")["allowed"]
    assert budget.status()["reserved_last_hour"] == 2


def test_target_rolling_hour_cap_and_exact_expiry(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock, target_per_hour=1)
    assert budget.reserve("a", "same")["allowed"]
    clock.value += 10
    assert budget.reserve("b", "same") == {
        "allowed": False,
        "reason": "target_hourly_limit",
        "next_at": 13600.0,
    }
    assert budget.reserve("other", "different")["allowed"]
    clock.value = 13600
    assert budget.reserve("b", "same")["allowed"]


def test_tightened_caps_use_history_and_stale_handles_use_persisted_policy(tmp_path):
    clock = Clock()
    old = make_budget(tmp_path, clock, min_interval_seconds=1, target_interval_seconds=1)
    for index in range(3):
        assert old.reserve(str(index), "same")["allowed"]
        clock.value += 1
    tightened = BudgetPolicy(global_per_hour=1, target_per_hour=1)
    DeliveryBudget(old.path, "fleet", tightened, clock=clock)
    assert old.policy == tightened
    assert old.reserve("new", "fresh") == {
        "allowed": False,
        "reason": "global_hourly_limit",
        "next_at": 13602.0,
    }
    assert DeliveryBudget.open_existing(old.path, clock=clock).policy == tightened
    assert old.status()["reservations_total"] == 3


def test_config_changes_cannot_shorten_committed_pacing(tmp_path):
    clock = Clock()
    old = make_budget(tmp_path, clock, min_interval_seconds=30, target_interval_seconds=40)
    assert old.reserve("a", "same")["allowed"]
    clock.value += 1
    loosened = DeliveryBudget(
        old.path, "fleet", BudgetPolicy(min_interval_seconds=1, target_interval_seconds=1), clock=clock
    )
    assert loosened.reserve("b", "different")["next_at"] == 10030
    assert loosened.reserve("b", "same")["next_at"] == 10040
    stricter = DeliveryBudget(
        old.path, "fleet", BudgetPolicy(min_interval_seconds=60, target_interval_seconds=70), clock=clock
    )
    assert stricter.reserve("b", "different")["next_at"] == 10060
    assert stricter.reserve("b", "same")["next_at"] == 10070


def test_rollback_clock_clamped_durably_including_denials(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    assert budget.reserve("a", "same")["allowed"]
    clock.value = 10004
    assert not budget.reserve("b", "other")["allowed"]
    clock.value = 1
    reopened = DeliveryBudget.open_existing(budget.path, clock=clock)
    assert reopened.status()["now"] == 10004
    assert reopened.reserve("b", "other")["next_at"] == 10005
    assert reopened.status()["last_wallclock"] == 10004
    assert reopened.status()["reserved_last_hour"] == 1
    clock.value = 10005
    assert reopened.reserve("b", "other")["allowed"]


def test_charges_permanent_no_ack_or_refund_api(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock, global_per_hour=1)
    assert budget.reserve("could-be-rejected-or-unknown", "group")["allowed"]
    clock.value += 20
    assert budget.reserve("different", "different")["reason"] == "global_hourly_limit"
    assert not hasattr(budget, "refund")
    assert not hasattr(budget, "release")


def test_status_audit_json_safe_and_only_hashed_metadata(tmp_path):
    budget = make_budget(tmp_path)
    key = "secret-message-or-token-must-not-be-stored"
    target = "private-sensitive-id"
    budget.reserve(key, target)
    budget.reserve("denied", target)
    budget.reserve(key, "different")
    status = budget.status()
    assert status["reservations_total"] == 1
    assert status["rejections_total"] == 2
    assert len(budget.audit(2)) == 2
    assert budget.audit(1)[0]["reason"] == "prior_reservation"
    assert budget.audit(100)[-1]["allowed"] is True
    encoded = json.dumps([status, budget.audit()], allow_nan=False)
    assert key not in encoded and target not in encoded
    with sqlite3.connect(budget.path) as db:
        dump = "\n".join(db.iterdump())
    assert key not in dump and target not in dump
    for limit in (True, 0, -1, 1001, "1", 1.5):
        with pytest.raises(ValueError):
            budget.audit(limit)


def test_open_existing_never_creates_or_rewrites_config(tmp_path):
    path = tmp_path / "nonexistent-directory" / "budget.sqlite3"
    with pytest.raises(FileNotFoundError):
        DeliveryBudget.open_existing(path)
    assert not path.parent.exists()
    budget = make_budget(tmp_path)
    before = budget.path.read_bytes()
    opened = DeliveryBudget.open_existing(budget.path)
    assert opened.logical_id == "fleet"
    assert opened.policy == BudgetPolicy()
    assert opened.status()["reservations_total"] == 0
    assert budget.path.read_bytes() == before
    budget.path.unlink()
    with pytest.raises(sqlite3.OperationalError):
        budget.reserve("never-recreate", "target")
    assert not budget.path.exists()


def test_fail_closed_namespace_or_schema_and_no_reconfigure_on_failure(tmp_path):
    budget = make_budget(tmp_path)
    with pytest.raises(RuntimeError, match="logical_id mismatch"):
        DeliveryBudget(budget.path, "different-fleet", BudgetPolicy(global_per_hour=1))
    assert budget.policy == BudgetPolicy()
    with sqlite3.connect(budget.path) as db:
        db.execute("PRAGMA user_version=999")
    for operation in (
        budget.status,
        lambda: budget.reserve("a", "b"),
        lambda: DeliveryBudget.open_existing(budget.path),
    ):
        with pytest.raises(RuntimeError, match="schema mismatch"):
            operation()


def test_refuse_transport_ledger_or_corrupt_policy(tmp_path):
    path = tmp_path / "ledger.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE outbound(key TEXT)")
    with pytest.raises(RuntimeError, match="schema mismatch"):
        DeliveryBudget(path, "fleet", BudgetPolicy())
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [("outbound",)]
    budget = make_budget(tmp_path)
    with sqlite3.connect(budget.path) as db:
        db.execute("UPDATE budget_meta SET policy=?", ('{"global_per_hour":true}',))
    with pytest.raises(RuntimeError, match="persisted delivery budget metadata"):
        DeliveryBudget.open_existing(budget.path)


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), -1, "10000"])
def test_invalid_clock_fails_without_reservation(tmp_path, value):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    clock.value = value
    with pytest.raises(ValueError):
        budget.reserve("a", "target")
    clock.value = 10000
    assert budget.status()["reservations_total"] == 0


def test_thread_concurrency_reservation_and_denials_are_atomic(tmp_path):
    budget = make_budget(tmp_path, global_per_hour=1)
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(lambda index: budget.reserve(str(index), str(index)), range(24)))
    assert sum(result["allowed"] for result in results) == 1
    assert budget.status()["reservations_total"] == 1
    assert budget.status()["rejections_total"] == 23


def test_concurrent_processes_never_overspend_or_replay(tmp_path):
    budget = make_budget(tmp_path, global_per_hour=1)
    args = [(str(budget.path), "same-key" if index < 4 else str(index)) for index in range(12)]
    with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context("spawn")) as pool:
        results = list(pool.map(process_reservation, args))
    assert sum(result["allowed"] for result in results) == 1
    assert budget.status()["reservations_total"] == 1
    assert len(budget.audit()) == 12


@pytest.mark.parametrize(
    "change",
    [
        "UPDATE budget_meta SET policy='{}'",
        "CREATE TRIGGER surprise AFTER INSERT ON budget_reservations BEGIN DELETE FROM budget_reservations; END",
        "ALTER TABLE budget_reservations RENAME TO incompatible",
    ],
)
def test_schema_or_partial_policy_changes_fail_closed(tmp_path, change):
    budget = make_budget(tmp_path)
    with sqlite3.connect(budget.path) as db:
        db.execute(change)
    with pytest.raises(RuntimeError):
        budget.reserve("cannot-send", "target")


def test_existing_unrelated_view_is_not_initialized(tmp_path):
    path = tmp_path / "other.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE VIEW unrelated AS SELECT 1")
    with pytest.raises(RuntimeError, match="schema mismatch"):
        DeliveryBudget(path, "fleet", BudgetPolicy())
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT name FROM sqlite_master").fetchall() == [("unrelated",)]


def test_concurrent_initialization_keeps_single_schema(tmp_path):
    path = tmp_path / "budget.sqlite3"
    with ThreadPoolExecutor(max_workers=8) as pool:
        instances = list(
            pool.map(lambda _: DeliveryBudget(path, "fleet", BudgetPolicy(), clock=Clock()), range(8))
        )
    assert all(instance.status()["reservations_total"] == 0 for instance in instances)


def test_delayed_rpc_completion_paces_actual_wire_finish(tmp_path):
    clock = Clock(1000)
    budget = make_budget(tmp_path, clock)
    assert budget.reserve("first", "same")["allowed"]
    clock.value = 1020  # Store.begin_send/RPC consumed the original reservation gap.
    assert budget.complete("first") is None
    assert budget.reserve("next", "different") == {
        "allowed": False,
        "reason": "global_interval",
        "next_at": 1025.0,
    }
    assert budget.reserve("next", "same")["next_at"] == 1030
    assert budget.status()["next_global_at"] == 1025
    clock.value = 1025
    assert budget.reserve("next", "different")["allowed"]
    assert budget.reserve("third", "same")["next_at"] == 1030
    clock.value = 1030
    assert budget.reserve("third", "same")["allowed"]


def test_completion_after_long_idle_does_not_accumulate_burst(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    assert budget.reserve("first", "one")["allowed"]
    clock.value += 100000
    budget.complete("first")
    assert budget.reserve("second", "two")["next_at"] == clock.value + 5
    clock.value += 5
    assert budget.reserve("second", "two")["allowed"]
    budget.complete("second")
    assert not budget.reserve("third", "three")["allowed"]
    assert budget.status()["reservations_total"] == 2


def test_completion_reads_latest_policy_and_later_tightening_uses_finish_anchor(tmp_path):
    clock = Clock()
    old = make_budget(tmp_path, clock)
    old.reserve("first", "same")
    clock.value += 20
    DeliveryBudget(
        old.path, "fleet", BudgetPolicy(min_interval_seconds=20, target_interval_seconds=30), clock=clock
    )
    old.complete("first")
    assert old.reserve("next", "different")["next_at"] == 10040
    assert old.reserve("next", "same")["next_at"] == 10050
    DeliveryBudget(
        old.path, "fleet", BudgetPolicy(min_interval_seconds=40, target_interval_seconds=50), clock=clock
    )
    assert old.reserve("next", "different")["next_at"] == 10060
    assert old.reserve("next", "same")["next_at"] == 10070
    DeliveryBudget(
        old.path, "fleet", BudgetPolicy(min_interval_seconds=1, target_interval_seconds=1), clock=clock
    )
    # Already committed completion deadlines are never shortened by loosening.
    assert old.reserve("next", "different")["next_at"] == 10040
    assert old.reserve("next", "same")["next_at"] == 10050


def test_completion_of_old_key_cannot_be_hidden_by_newer_reservations(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    budget.reserve("first", "same")
    clock.value += 10
    budget.reserve("second", "same")
    clock.value += 100
    budget.complete("first")
    assert budget.reserve("third", "different")["next_at"] == 10115
    assert budget.reserve("third", "same")["next_at"] == 10120


def test_completion_rollback_clock_is_clamped_and_repeated_completion_never_shrinks(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    budget.reserve("first", "same")
    clock.value += 4
    budget.reserve("denied", "other")
    clock.value = 1
    budget.complete("first")
    reopened = DeliveryBudget.open_existing(budget.path, clock=clock)
    assert reopened.status()["last_wallclock"] == 10004
    assert reopened.reserve("next", "other")["next_at"] == 10009
    assert reopened.reserve("next", "same")["next_at"] == 10014
    reopened.complete("first")
    assert reopened.reserve("next", "same")["next_at"] == 10014


def test_explicit_recovery_fences_crash_before_completion_without_replay(tmp_path):
    clock = Clock(1000)
    budget = make_budget(tmp_path, clock)
    budget.reserve("crashed-during-send", "same")
    clock.value = 1020
    restarted = DeliveryBudget.open_existing(budget.path, clock=clock)
    # Diagnostics/opening are not recovery and must not move history deadlines.
    before = budget.path.read_bytes()
    assert restarted.status()["next_global_at"] == 1020
    assert budget.path.read_bytes() == before
    assert restarted.recover() is None  # Caller has acquired exclusive InstanceLock.
    assert restarted.reserve("fresh", "different")["next_at"] == 1025
    assert restarted.reserve("fresh", "same")["next_at"] == 1030
    assert restarted.reserve("crashed-during-send", "other")["reason"] == "prior_reservation"
    clock.value = 1025
    assert restarted.reserve("fresh", "different")["allowed"]
    assert not restarted.reserve("burst", "third")["allowed"]


def test_recovery_updates_only_latest_row_per_target_and_preserves_charges(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    for key, target in (("a", "one"), ("b", "one"), ("c", "two")):
        budget.reserve(key, target)
        clock.value += 10
    with sqlite3.connect(budget.path) as db:
        before = db.execute("SELECT key_hash,reserved_at FROM budget_reservations ORDER BY rowid").fetchall()
    clock.value += 10000
    budget.recover()
    with sqlite3.connect(budget.path) as db:
        assert (
            db.execute("SELECT key_hash,reserved_at FROM budget_reservations ORDER BY rowid").fetchall()
            == before
        )
        assert (
            db.execute(
                "SELECT count(*) FROM budget_reservations WHERE paced_at=?", (clock.value,)
            ).fetchone()[0]
            == 2
        )
    status = budget.status()
    assert status["reservations_total"] == 3 and status["reserved_last_hour"] == 0
    assert status["next_global_at"] == clock.value + 5
    for target in ("one", "two"):
        assert budget.reserve("fresh", target)["next_at"] == clock.value + 10
    for key in ("a", "b", "c"):
        assert budget.reserve(key, "elsewhere")["reason"] == "prior_reservation"


def test_empty_recovery_and_diagnostics_never_create_charges_or_fence(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    clock.value += 100
    budget.recover()
    assert budget.status()["reservations_total"] == 0
    assert budget.reserve("first", "same")["allowed"]
    with sqlite3.connect(budget.path) as db:
        before = db.execute("SELECT * FROM budget_reservations").fetchall()
    clock.value += 1
    diagnostic = DeliveryBudget.open_existing(budget.path, clock=clock)
    diagnostic.status()
    DeliveryBudget(budget.path, "fleet", BudgetPolicy(), clock=clock)
    with sqlite3.connect(budget.path) as db:
        assert db.execute("SELECT * FROM budget_reservations").fetchall() == before


def test_recovery_latest_policy_rollback_and_committed_deadlines(tmp_path):
    clock = Clock()
    old = make_budget(tmp_path, clock, min_interval_seconds=100, target_interval_seconds=200)
    old.reserve("first", "same")
    clock.value += 20
    old.complete("first")
    DeliveryBudget(
        old.path, "fleet", BudgetPolicy(min_interval_seconds=1, target_interval_seconds=1), clock=clock
    )
    clock.value = 1
    old.recover()
    assert old.status()["last_wallclock"] == 10020
    assert old.reserve("next", "different")["next_at"] == 10120
    assert old.reserve("next", "same")["next_at"] == 10220
    clock.value = 10030
    DeliveryBudget(
        old.path, "fleet", BudgetPolicy(min_interval_seconds=300, target_interval_seconds=400), clock=clock
    )
    old.recover()
    assert old.reserve("next", "different")["next_at"] == 10330
    assert old.reserve("next", "same")["next_at"] == 10430
    clock.value = 2
    old.recover()
    assert old.reserve("next", "different")["next_at"] == 10330


def test_completion_and_recovery_never_refund_hourly_charges(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock, global_per_hour=1, target_per_hour=1)
    budget.reserve("first", "same")
    clock.value += 20
    budget.complete("first")
    budget.recover()
    assert budget.status()["reservations_total"] == 1
    assert budget.status()["reserved_last_hour"] == 1
    assert budget.reserve("next", "other")["reason"] == "global_hourly_limit"
    assert budget.reserve("first", "same")["reason"] == "prior_reservation"


def test_unknown_completion_fails_without_modifying_metadata(tmp_path):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    clock.value += 100
    before = budget.path.read_bytes()
    with pytest.raises(ValueError, match="unknown delivery reservation"):
        budget.complete("not-reserved")
    assert budget.path.read_bytes() == before
    assert budget.status()["last_wallclock"] == 10000


@pytest.mark.parametrize("value", ["-1", "9999", "10001", "'private text'", "1e999"])
def test_corrupt_completion_anchor_fails_closed(tmp_path, value):
    budget = make_budget(tmp_path)
    budget.reserve("first", "same")
    with sqlite3.connect(budget.path) as db:
        db.execute(f"UPDATE budget_reservations SET paced_at={value}")
    for operation in (
        budget.status,
        budget.recover,
        lambda: budget.complete("first"),
        lambda: budget.reserve("next", "other"),
    ):
        with pytest.raises(RuntimeError, match="persisted delivery budget pacing"):
            operation()


@pytest.mark.parametrize("operation", ["complete", "recover"])
def test_completion_and_recovery_transactions_roll_back_on_persistence_failure(
    tmp_path, monkeypatch, operation
):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    budget.reserve("first", "same")
    clock.value += 20
    original_transaction = budget._transaction
    before = budget.path.read_bytes()

    class FailedPersistence:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, sql, *args):
            if sql.startswith("UPDATE budget_meta"):
                # The pacing row update already ran; fail before clock metadata.
                raise sqlite3.OperationalError("simulated local persistence failure")
            return self.connection.execute(sql, *args)

    @contextmanager
    def failing_transaction(**kwargs):
        with original_transaction(**kwargs) as db:
            yield FailedPersistence(db)

    monkeypatch.setattr(budget, "_transaction", failing_transaction)
    with pytest.raises(sqlite3.OperationalError, match="simulated local persistence failure"):
        if operation == "complete":
            budget.complete("first")
        else:
            budget.recover()
    monkeypatch.undo()
    assert budget.path.read_bytes() == before
    assert budget.status()["last_wallclock"] == 10000
    # A new exclusive owner explicitly fences the crash/failed-persist uncertainty.
    budget.recover()
    assert budget.reserve("next", "different")["next_at"] == 10025
    assert budget.reserve("next", "same")["next_at"] == 10030


@pytest.mark.parametrize("operation", ["complete", "recover"])
@pytest.mark.parametrize("value", [True, -1, float("nan"), float("inf"), "10020"])
def test_completion_recovery_reject_invalid_clock_without_mutation(tmp_path, operation, value):
    clock = Clock()
    budget = make_budget(tmp_path, clock)
    budget.reserve("first", "same")
    before = budget.path.read_bytes()
    clock.value = value
    with pytest.raises(ValueError):
        if operation == "complete":
            budget.complete("first")
        else:
            budget.recover()
    assert budget.path.read_bytes() == before


def test_completion_recovery_refuse_removed_sidecar_and_old_schema(tmp_path):
    budget = make_budget(tmp_path)
    budget.reserve("first", "same")
    with sqlite3.connect(budget.path) as db:
        db.execute("PRAGMA user_version=1")
    db.close()  # sqlite3's transaction context does not close its Windows handle.
    for operation in (budget.recover, lambda: budget.complete("first")):
        with pytest.raises(RuntimeError, match="schema mismatch"):
            operation()
    budget.path.unlink()
    for operation in (budget.recover, lambda: budget.complete("first")):
        with pytest.raises(sqlite3.OperationalError):
            operation()
        assert not budget.path.exists()
