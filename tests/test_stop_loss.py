import hashlib
import json
import multiprocessing
import sqlite3
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, asdict, fields

import pytest

from hermes_onebot_fleet.stop_loss import StopLoss, StopLossPolicy


class Clock:
    def __init__(self, value=10000):
        self.value = value

    def __call__(self):
        return self.value


def address(identifier=123, kind="group"):
    return f"obf:fleet:{kind}:{identifier}"


def make_stop(tmp_path, clock=None, **policy):
    return StopLoss(tmp_path / "stop.sqlite3", "fleet", StopLossPolicy(**policy), clock=clock or Clock())


def record(stop, index, account=1001, target=None, code="target_rejected"):
    return stop.record(account, target or address(index + 123), code, intent=f"send-{index}")


def digest(value):
    return hashlib.sha256(str(value).encode()).hexdigest()


def record_in_process(args):
    path, index = args
    stop = StopLoss.open_existing(path, clock=lambda: 10000)
    return record(stop, index, account=1001 + index % 2)


def test_policy_defaults_frozen_and_strict_mapping():
    policy = StopLossPolicy.from_mapping({})
    assert asdict(policy) == {
        "window_seconds": 300,
        "account_failures": 3,
        "account_targets": 3,
        "fleet_failures": 4,
        "fleet_targets": 3,
        "fleet_accounts": 2,
    }
    with pytest.raises(FrozenInstanceError):
        policy.window_seconds = 1
    for mapping in (None, True, [], {"enabled": False}, {"unknown": 1}, {"window": 300}):
        with pytest.raises(ValueError):
            StopLossPolicy.from_mapping(mapping)


@pytest.mark.parametrize("field", [field.name for field in fields(StopLossPolicy)])
@pytest.mark.parametrize(
    "value", [None, True, False, "3", 0, -1, 0.99, 3601, float("nan"), float("inf"), -float("inf")]
)
def test_policy_invalid_values(field, value):
    with pytest.raises(ValueError):
        StopLossPolicy(**{field: value})
    with pytest.raises(ValueError):
        StopLossPolicy.from_mapping({field: value})


def test_policy_bounds_count_types_and_relations():
    StopLossPolicy(1, 1, 1, 1, 1, 1)
    StopLossPolicy(3600, 3600, 3600, 3600, 3600, 3600)
    StopLossPolicy(window_seconds=1.25)
    for name in ("account_failures", "account_targets", "fleet_failures", "fleet_targets", "fleet_accounts"):
        with pytest.raises(ValueError):
            StopLossPolicy(**{name: 3.0})
    for values in (
        {"account_failures": 2},
        {"account_targets": 4},
        {"fleet_failures": 2},
        {"fleet_targets": 5},
        {"fleet_accounts": 5},
    ):
        with pytest.raises(ValueError):
            StopLossPolicy(**values)


def test_account_requires_total_and_distinct_targets_and_mixed_codes(tmp_path):
    stop = make_stop(tmp_path)
    for index in range(4):
        assert not record(stop, index, target=address())["account_held"]
    assert not record(stop, 4, target=address(124), code="outcome_unknown")["account_held"]
    result = record(stop, 5, target=address(125), code="outcome_unknown")
    assert result["account_held"] and not result["fleet_held"]
    assert not stop.is_eligible(1001)
    assert stop.is_eligible(1002)
    status = stop.status()
    assert status["business_held"]  # Caller must not use another eligible bot to bypass this risk.
    assert status["held_accounts"] == [digest(1001)]
    incident = next(event for event in stop.audit() if event["kind"] == "account_hold")
    assert (incident["failures"], incident["targets"], incident["accounts"]) == (6, 3, 1)
    assert not any(event["kind"] == "fleet_hold" for event in stop.audit())


def test_total_threshold_still_required_with_distinct_targets(tmp_path):
    stop = make_stop(tmp_path, account_failures=4, account_targets=2)
    for index in range(3):
        assert not record(stop, index)["account_held"]
    assert record(stop, 3)["account_held"]


def test_fleet_requires_failures_targets_and_accounts(tmp_path):
    stop = make_stop(tmp_path, account_failures=10, account_targets=3)
    for index in range(4):
        assert not record(stop, index)["fleet_held"]  # Enough failures/targets, only one account.
    assert record(stop, 4, account=1002, code="outcome_unknown")["fleet_held"]
    assert not stop.is_eligible(1001) and not stop.is_eligible(9999)
    assert stop.status()["held_accounts"] == []
    assert sum(row["kind"] == "fleet_hold" for row in stop.audit()) == 1


def test_fleet_distinct_targets_are_global_not_account_target_pairs(tmp_path):
    stop = make_stop(tmp_path, account_failures=10)
    for index in range(8):
        assert not record(stop, index, account=1001 + index % 2, target=address())["fleet_held"]
    assert not record(stop, 8, account=1002, target=address(124))["fleet_held"]
    assert record(stop, 9, account=1002, target=address(kind="private"))["fleet_held"]


def test_fleet_total_failure_threshold(tmp_path):
    stop = make_stop(tmp_path, account_failures=10)
    for index in range(3):
        assert not record(stop, index, account=1001 + index % 2)["fleet_held"]
    assert record(stop, 3, account=1002)["fleet_held"]


def test_intent_dedup_is_permanent_across_reset_restart_and_conflicting_reuse(tmp_path):
    stop = make_stop(tmp_path, account_failures=1, account_targets=1)
    first = record(stop, 1)
    assert first["recorded"] and not first["duplicate"]
    count = len(stop.audit())
    duplicate = record(stop, 1)
    assert duplicate["duplicate"] and not duplicate["recorded"]
    assert duplicate["evidence_seq"] == first["evidence_seq"]
    assert len(stop.audit()) == count
    stop.reset(1001, reason="review complete")
    stop.reset(reason="fleet review complete")
    reopened = StopLoss.open_existing(stop.path, clock=Clock(99999))
    assert record(reopened, 1)["duplicate"]
    assert reopened.is_eligible(1001)
    assert reopened.status()["failures_total"] == 1
    for kwargs in ({"account": 1002}, {"target": address(999)}, {"code": "outcome_unknown"}):
        with pytest.raises(ValueError, match="intent conflicts"):
            record(reopened, 1, **kwargs)
    assert reopened.status()["failures_total"] == 1


def test_independent_scope_resets_and_fresh_evidence_watermarks(tmp_path):
    stop = make_stop(tmp_path, account_failures=2, account_targets=2, fleet_failures=3)
    record(stop, 0)
    record(stop, 1)
    record(stop, 2, account=1002)
    assert stop.status()["fleet_held"] and not stop.is_eligible(1001)
    fleet_reset = stop.reset(reason="human fleet review")
    assert fleet_reset["scope"] == "fleet" and fleet_reset["was_held"]
    assert not stop.status()["fleet_held"]
    assert not stop.is_eligible(1001) and stop.is_eligible(1002)
    # First new event cannot relatch fleet from the three old events.
    assert not record(stop, 3, account=1002)["fleet_held"]
    assert not stop.is_eligible(1002)  # Its account evidence still includes the old event.
    account_reset = stop.reset(1001, reason="human account review")
    assert account_reset["scope"] == "account" and account_reset["was_held"]
    assert stop.is_eligible(1001) and not stop.is_eligible(1002)
    first = record(stop, 4)
    assert not first["account_held"] and not first["fleet_held"]
    second = record(stop, 5)
    assert second["account_held"] and second["fleet_held"]
    stop.reset(1001, reason="another account review")
    assert stop.status()["fleet_held"] and not stop.is_eligible(1001)
    stop.reset(reason="another fleet review")
    assert stop.is_eligible(1001) and not stop.is_eligible(1002)
    assert stop.status()["failures_total"] == 6
    assert len([event for event in stop.audit() if event["kind"].endswith("_reset")]) == 4


def test_reset_unheld_scope_excludes_history_and_reconfiguration_cannot_resurrect_it(tmp_path):
    clock = Clock()
    stop = make_stop(tmp_path, clock)
    record(stop, 0)
    result = stop.reset(1001, reason="review pre-threshold")
    assert not result["was_held"] and result["watermark"] == 1
    tightened = StopLossPolicy(account_failures=2, account_targets=2)
    StopLoss(stop.path, "fleet", tightened, clock=clock)
    assert not record(stop, 1)["account_held"]
    assert record(stop, 2)["account_held"]
    assert stop.status()["failures_total"] == 3


def test_restart_time_and_clock_rollback_do_not_release_holds(tmp_path):
    clock = Clock()
    stop = make_stop(tmp_path, clock)
    record(stop, 0)
    clock.value += 1
    record(stop, 1)
    clock.value = 1  # Third evidence is clamped, never backdated out of the window.
    assert record(stop, 2)["account_held"]
    assert stop.status()["last_wallclock"] == 10001
    clock.value = 999999
    reopened = StopLoss.open_existing(stop.path, clock=clock)
    assert not reopened.is_eligible(1001)
    assert reopened.status()["fleet_window"]["failures"] == 0
    assert reopened.status()["last_wallclock"] == 10001  # Status cannot advance the anchor.
    assert not hasattr(reopened, "success")
    assert not hasattr(reopened, "refund")


def test_exact_rolling_window_and_no_time_based_latch(tmp_path):
    clock = Clock()
    stop = make_stop(tmp_path, clock, window_seconds=10, account_failures=2, account_targets=2)
    record(stop, 0)
    clock.value += 10
    assert not record(stop, 1)["account_held"]  # Window is (now-window, now].
    clock.value += 9.999
    assert record(stop, 2)["account_held"]
    clock.value += 100
    assert not stop.is_eligible(1001)


def test_constructor_tightens_from_recent_history_and_loosen_never_releases(tmp_path):
    clock = Clock()
    loose = StopLossPolicy(account_failures=10, account_targets=3, fleet_failures=10)
    stop = StopLoss(tmp_path / "stop.sqlite3", "fleet", loose, clock=clock)
    for index in range(3):
        record(stop, index)
    assert stop.is_eligible(1001)
    tight = StopLossPolicy()
    StopLoss(stop.path, "fleet", tight, clock=clock)
    assert stop.policy == tight and not stop.is_eligible(1001)
    StopLoss(stop.path, "fleet", loose, clock=clock)
    assert stop.policy == loose and not stop.is_eligible(1001)
    assert len([event for event in stop.audit() if event["kind"] == "account_hold"]) == 1
    # A stale handle records under the newest policy, not its original loose one.
    StopLoss(stop.path, "fleet", tight, clock=clock)
    assert record(stop, 4, account=1002)["fleet_held"]
    assert StopLoss.open_existing(stop.path, clock=clock).policy == tight


def test_window_tightening_reaches_older_history_without_releasing_existing_hold(tmp_path):
    clock = Clock()
    stop = make_stop(tmp_path, clock, window_seconds=1, account_failures=2, account_targets=2)
    record(stop, 0)
    clock.value += 2
    record(stop, 1)
    assert stop.is_eligible(1001)
    StopLoss(
        stop.path,
        "fleet",
        StopLossPolicy(window_seconds=10, account_failures=2, account_targets=2),
        clock=clock,
    )
    assert not stop.is_eligible(1001)
    StopLoss(stop.path, "fleet", StopLossPolicy(window_seconds=1, account_failures=10), clock=clock)
    assert not stop.is_eligible(1001)


def test_readonly_operations_never_create_state_or_mutate_clock(tmp_path):
    clock = Clock()
    stop = make_stop(tmp_path, clock)
    record(stop, 0)
    before = stop.path.read_bytes()
    clock.value += 1000
    stop.status()
    stop.audit()
    stop.is_eligible(1001)
    stop.policy
    StopLoss.open_existing(stop.path, clock=lambda: pytest.fail("open_existing sampled clock"))
    assert stop.path.read_bytes() == before
    assert stop.status()["last_wallclock"] == 10000
    assert record(stop, 0)["duplicate"]
    assert stop.path.read_bytes() == before  # Duplicate is not a new event or a clock mutation.


def test_thread_concurrency_deduplicates_and_atomically_latches_once(tmp_path):
    stop = make_stop(tmp_path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda index: record_in_process((stop.path, index % 8)), range(32)))
    assert sum(result["recorded"] for result in results) == 8
    assert stop.status()["failures_total"] == 8
    audit = stop.audit()
    assert len([event for event in audit if event["kind"] == "account_hold"]) == 2
    assert len([event for event in audit if event["kind"] == "fleet_hold"]) == 1
    assert not stop.is_eligible(9999)


def test_process_concurrency_deduplicates_and_preserves_all_evidence(tmp_path):
    stop = make_stop(tmp_path)
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=3, mp_context=context) as pool:
        results = list(pool.map(record_in_process, [(stop.path, index % 6) for index in range(12)]))
    assert sum(result["recorded"] for result in results) == 6
    assert stop.status()["failures_total"] == 6
    assert stop.status()["fleet_held"]
    assert len({event["id"] for event in stop.audit()}) == len(stop.audit())


def test_record_reset_and_constructor_failures_roll_back_all_state(tmp_path, monkeypatch):
    stop = make_stop(tmp_path)
    before = stop.path.read_bytes()
    original = stop._latch

    def failed_latch(*args):
        original(*args)
        raise RuntimeError("simulated persistence failure")

    monkeypatch.setattr(stop, "_latch", failed_latch)
    with pytest.raises(RuntimeError, match="simulated"):
        record(stop, 0)
    assert stop.path.read_bytes() == before
    assert stop.status()["failures_total"] == 0
    monkeypatch.setattr(stop, "_latch", original)
    record(stop, 0)
    before = stop.path.read_bytes()
    original_append = StopLoss._append

    def failed_append(self, *args, **kwargs):
        original_append(self, *args, **kwargs)
        raise RuntimeError("simulated persistence failure")

    monkeypatch.setattr(StopLoss, "_append", failed_append)
    with pytest.raises(RuntimeError):
        stop.reset(1001, reason="review")
    with pytest.raises(RuntimeError):
        StopLoss(stop.path, "fleet", StopLossPolicy(account_failures=1, account_targets=1), clock=Clock())
    assert stop.path.read_bytes() == before
    assert stop.policy == StopLossPolicy()


def test_transaction_uses_full_durability_and_immediate_write_lock(tmp_path, monkeypatch):
    stop = make_stop(tmp_path)
    original = sqlite3.connect
    statements = []

    def traced_connect(*args, **kwargs):
        db = original(*args, **kwargs)
        db.set_trace_callback(statements.append)
        return db

    monkeypatch.setattr(sqlite3, "connect", traced_connect)
    record(stop, 0)
    assert "PRAGMA synchronous=FULL" in statements
    assert "PRAGMA trusted_schema=OFF" in statements
    assert "BEGIN IMMEDIATE" in statements
    statements.clear()
    stop.status()
    assert "BEGIN" in statements and "BEGIN IMMEDIATE" not in statements


@pytest.mark.parametrize(
    "account", [None, True, False, 0, -1, 1.5, "", "001", "+1", " 1", "1 ", "a", "1" * 129]
)
def test_invalid_accounts_fail_before_mutation(tmp_path, account):
    stop = make_stop(tmp_path)
    for action in (
        lambda: stop.record(account, address(), "target_rejected", intent="key"),
        lambda: stop.is_eligible(account),
    ):
        with pytest.raises(ValueError):
            action()
    if account is not None:
        with pytest.raises(ValueError):
            stop.reset(account, reason="review")
    assert stop.audit() == []


@pytest.mark.parametrize(
    "target",
    [
        None,
        True,
        123,
        "",
        "123",
        "obf:other:group:123",
        "obf:fleet:group:0123",
        "obf:fleet:group:0",
        "obf:fleet:channel:123",
        "obf:fleet:private:123:extra",
    ],
)
def test_invalid_target_rejected(tmp_path, target):
    stop = make_stop(tmp_path)
    with pytest.raises(ValueError):
        stop.record(1001, target, "target_rejected", intent="key")
    assert stop.status()["failures_total"] == 0


@pytest.mark.parametrize(
    "code",
    [
        None,
        True,
        1,
        "success",
        "health_failed",
        "member_rejected",
        "preflight_failed",
        "account_unhealthy",
        "timeout",
        "banned",
        "message text",
    ],
)
def test_only_dispatched_send_evidence_codes_are_accepted(tmp_path, code):
    stop = make_stop(tmp_path)
    with pytest.raises(ValueError):
        stop.record(1001, address(), code, intent="key")
    assert stop.audit() == []


@pytest.mark.parametrize("value", [None, True, 1, "", "  \t", "x" * 4097, "\ud800"])
def test_intent_and_reason_strict_and_mandatory(tmp_path, value):
    stop = make_stop(tmp_path)
    with pytest.raises(ValueError):
        stop.record(1001, address(), "target_rejected", intent=value)
    with pytest.raises(ValueError):
        stop.reset(reason=value)
    with pytest.raises(TypeError):
        stop.record(1001, address(), "target_rejected")
    with pytest.raises(TypeError):
        stop.reset()
    assert stop.audit() == []


@pytest.mark.parametrize("clock_value", [None, True, -1, "10", float("nan"), float("inf"), 1e300, 10**1000])
def test_invalid_clock_and_inputs_fail_closed(tmp_path, clock_value):
    with pytest.raises(ValueError):
        make_stop(tmp_path, Clock(clock_value))
    # Failed construction leaves no partially committed schema.
    with sqlite3.connect(tmp_path / "stop.sqlite3") as db:
        assert not db.execute("SELECT * FROM sqlite_master").fetchall()


def test_invalid_operational_clock_cannot_mutate_evidence_or_reset(tmp_path):
    clock = Clock()
    stop = make_stop(tmp_path, clock)
    record(stop, 0)
    before = stop.path.read_bytes()
    for value in (-1, float("nan"), float("inf"), 1e300):
        clock.value = value
        for action in (lambda: record(stop, 1), lambda: stop.reset(reason="review"), stop.status):
            with pytest.raises(ValueError):
                action()
        assert stop.path.read_bytes() == before
    clock.value = 10000
    assert stop.status()["failures_total"] == 1


def test_invalid_constructor_and_readonly_audit_limits(tmp_path):
    for path in (None, "", " ", ":memory:"):
        with pytest.raises(ValueError):
            StopLoss(path, "fleet", StopLossPolicy())
    for logical in (None, "", "with space", "a:group:1", "a" * 129):
        with pytest.raises(ValueError):
            StopLoss(tmp_path / "stop.sqlite3", logical, StopLossPolicy())
    with pytest.raises(ValueError):
        make_stop(tmp_path, clock=3)
    with pytest.raises(ValueError):
        StopLoss(tmp_path / "stop.sqlite3", "fleet", {})
    policy = StopLossPolicy()
    object.__setattr__(policy, "window_seconds", 0)
    with pytest.raises(ValueError):
        StopLoss(tmp_path / "stop.sqlite3", "fleet", policy)
    stop = make_stop(tmp_path)
    for limit in (None, True, 0, -1, 1001, 1.5, "2"):
        with pytest.raises(ValueError):
            stop.audit(limit)
    assert stop.audit(1000) == []


def test_missing_removed_and_corrupt_sidecars_fail_closed_without_recreation(tmp_path):
    path = tmp_path / "missing.sqlite3"
    with pytest.raises(FileNotFoundError):
        StopLoss.open_existing(path)
    assert not path.exists()
    stop = make_stop(tmp_path)
    stop.path.unlink()
    for action in (
        stop.status,
        stop.audit,
        lambda: stop.policy,
        lambda: stop.is_eligible(1001),
        lambda: record(stop, 0),
        lambda: stop.reset(reason="review"),
    ):
        with pytest.raises(sqlite3.Error):
            action()
        assert not stop.path.exists()
    stop.path.write_bytes(b"not sqlite")
    with pytest.raises(sqlite3.Error):
        StopLoss.open_existing(stop.path)
    assert stop.path.read_bytes() == b"not sqlite"


@pytest.mark.parametrize(
    "sql",
    [
        "PRAGMA application_id=1",
        "PRAGMA user_version=99",
        "CREATE TABLE extra (id INTEGER)",
        "CREATE VIEW extra AS SELECT * FROM stop_events",
        "CREATE TRIGGER extra AFTER INSERT ON stop_events BEGIN DELETE FROM stop_events; END",
        "DROP INDEX stop_intent",
        "UPDATE stop_meta SET logical_id='other'",
        "UPDATE stop_meta SET last_wallclock=-1",
        "UPDATE stop_meta SET last_wallclock=1e999",
        "UPDATE stop_meta SET last_event=0",
        "UPDATE stop_meta SET policy='{}'",
        "UPDATE stop_meta SET policy='null'",
        "UPDATE stop_meta SET policy='garbage'",
        "UPDATE stop_events SET account_hash='raw-account' WHERE id=1",
        "UPDATE stop_events SET target_hash='raw-target' WHERE id=1",
        "UPDATE stop_events SET intent_hash='raw-key' WHERE id=1",
        "UPDATE stop_events SET code='success' WHERE id=1",
        "UPDATE stop_events SET at=-1 WHERE id=1",
        "UPDATE stop_events SET at=1e999 WHERE id=1",
        "UPDATE stop_events SET evidence_seq=0 WHERE id=1",
        "UPDATE stop_events SET failures=1 WHERE id=1",
        "UPDATE stop_events SET reason_hash='reason' WHERE id=1",
        "UPDATE stop_events SET policy='{}' WHERE id=1",
        "DELETE FROM stop_events WHERE id=1",
    ],
)
def test_tampering_schema_metadata_and_evidence_fails_closed(tmp_path, sql):
    stop = make_stop(tmp_path)
    record(stop, 0)
    with sqlite3.connect(stop.path) as db:
        db.execute(sql)
    with pytest.raises(RuntimeError):
        stop.status()
    with pytest.raises(RuntimeError):
        record(stop, 1)
    with pytest.raises(RuntimeError):
        stop.reset(reason="review")


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE stop_events SET failures=0 WHERE kind='account_hold'",
        "UPDATE stop_events SET targets=2 WHERE kind='account_hold'",
        "UPDATE stop_events SET accounts=2 WHERE kind='account_hold'",
        "UPDATE stop_events SET evidence_seq=0 WHERE kind='account_hold'",
        "UPDATE stop_events SET reason_hash='not-a-hash' WHERE kind='account_reset'",
        "UPDATE stop_events SET evidence_seq=0 WHERE kind='account_reset'",
        "UPDATE stop_events SET kind='fleet_reset' WHERE kind='account_reset'",
        "DELETE FROM stop_events WHERE kind='account_reset'",
    ],
)
def test_tampered_hold_reset_or_watermark_fails_closed(tmp_path, sql):
    stop = make_stop(tmp_path, account_failures=1, account_targets=1)
    record(stop, 0)
    stop.reset(1001, reason="review")
    with sqlite3.connect(stop.path) as db:
        db.execute(sql)
    with pytest.raises(RuntimeError):
        stop.status()
    with pytest.raises(RuntimeError):
        record(stop, 1)


def test_storage_write_failure_does_not_become_an_eligibility_allow(tmp_path, monkeypatch):
    stop = make_stop(tmp_path)

    @contextmanager
    def unavailable(**kwargs):
        raise sqlite3.OperationalError("storage unavailable")
        yield

    monkeypatch.setattr(stop, "_transaction", unavailable)
    for action in (
        lambda: record(stop, 0),
        lambda: stop.reset(reason="review"),
        lambda: stop.is_eligible(1001),
    ):
        with pytest.raises(sqlite3.OperationalError):
            action()


def test_privacy_storage_status_audit_and_no_budget_replay_apis(tmp_path):
    stop = make_stop(tmp_path, account_failures=1, account_targets=1)
    account, target, intent = (
        "76543210987654321",
        address("87654321098765432"),
        "sensitive immutable send key",
    )
    reason = "operator review containing sensitive message text"
    stop.record(account, target, "outcome_unknown", intent=intent)
    stop.reset(account, reason=reason)
    audit = stop.audit()
    assert [event["kind"] for event in audit] == ["account_reset", "account_hold", "evidence"]
    assert audit[0]["reason_hash"] == digest(reason)
    assert audit[-1]["intent_hash"] == digest(intent)
    assert audit[-1]["target_hash"] == digest(target)
    assert audit[-1]["account_hash"] == digest(account)
    serialized = json.dumps({"status": stop.status(), "audit": audit})
    raw = stop.path.read_bytes()
    for secret in (account, target, "87654321098765432", intent, reason):
        assert secret not in serialized and secret.encode() not in raw
    assert stop.audit(1) == audit[:1]
    for method in ("reserve", "admit", "refund", "release", "replay", "recover", "success", "complete"):
        assert not hasattr(stop, method)
