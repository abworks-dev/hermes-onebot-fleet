import json
import multiprocessing
import sqlite3
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, asdict, fields

import pytest

from hermes_onebot_fleet.read_requests import ReadPolicy, ReadRequests


class Clock:
    def __init__(self, value=10000):
        self.value = value

    def __call__(self):
        return self.value


def make_reads(tmp_path, clock=None, **policy):
    return ReadRequests(tmp_path / "reads.sqlite3", "fleet", ReadPolicy(**policy), clock=clock or Clock())


def address(identifier=123, kind="group"):
    return f"obf:fleet:{kind}:{identifier}"


def reserve_in_process(args):
    path, account = args
    return ReadRequests.open_existing(path, clock=lambda: 10000).reserve_member(account, address())


def test_policy_defaults_frozen_and_strict_mapping():
    policy = ReadPolicy.from_mapping({})
    assert asdict(policy) == {
        "health_interval_seconds": 60,
        "idle_health_interval_seconds": 180,
        "health_backoff_max_seconds": 600,
        "member_interval_seconds": 15,
        "member_account_interval_seconds": 2,
        "member_global_per_minute": 30,
    }
    with pytest.raises(FrozenInstanceError):
        policy.member_interval_seconds = 1
    for mapping in (None, [], True, {"unknown": 2}, {"health_interval": 10}):
        with pytest.raises(ValueError):
            ReadPolicy.from_mapping(mapping)


@pytest.mark.parametrize("field", [field.name for field in fields(ReadPolicy)])
@pytest.mark.parametrize(
    "value", [None, True, False, "2", 0, -1, 0.99, 3601, float("nan"), float("inf"), -float("inf")]
)
def test_policy_rejects_invalid_direct_and_mapping(field, value):
    with pytest.raises(ValueError):
        ReadPolicy(**{field: value})
    with pytest.raises(ValueError):
        ReadPolicy.from_mapping({field: value})


def test_policy_bounds_and_cross_field_ordering():
    ReadPolicy(1, 1, 1, 1, 1, 1)
    ReadPolicy(3600, 3600, 3600, 3600, 3600, 3600)
    ReadPolicy(1.5, 2.5, 3.5, 1.25, 1.75, 2)
    for mapping in (
        {"member_global_per_minute": 1.0},
        {"health_interval_seconds": 181},
        {"idle_health_interval_seconds": 601},
        {"health_backoff_max_seconds": 179},
    ):
        with pytest.raises(ValueError):
            ReadPolicy.from_mapping(mapping)


def test_reservation_committed_before_query_no_refund_or_success_cache(tmp_path):
    clock = Clock()
    reads = make_reads(tmp_path, clock)
    assert reads.reserve_member("1001", address()) == {
        "allowed": True,
        "reason": "reserved",
        "next_at": 10000.0,
    }
    # An independently opened handle sees the charge even if no query happened,
    # or the caller died, timed out, got an error, or got an unknown result.
    observer = ReadRequests.open_existing(reads.path, clock=clock)
    assert observer.status()["reservations_total"] == 1
    for outcome in ("success", "error", "unknown", "never issued"):
        assert outcome
        assert not observer.reserve_member(1001, address())["allowed"]
    for method in ("refund", "release", "complete", "success", "cached_result"):
        assert not hasattr(reads, method)
    clock.value = 10015
    assert observer.reserve_member(1001, address())["allowed"]
    assert observer.status()["reservations_total"] == 2


def test_account_spacing_and_pair_cooldown_exact_boundary_no_denial_extension(tmp_path):
    clock = Clock()
    reads = make_reads(tmp_path, clock)
    assert reads.reserve_member(1001, address())["allowed"]
    assert reads.reserve_member(1001, address(124)) == {
        "allowed": False,
        "reason": "member_account_interval",
        "next_at": 10002.0,
    }
    clock.value = 10001.999
    assert reads.reserve_member(1001, address(124))["next_at"] == 10002
    clock.value = 10002
    assert reads.reserve_member(1001, address(124))["allowed"]
    assert reads.reserve_member(1001, address()) == {
        "allowed": False,
        "reason": "member_target_interval",
        "next_at": 10015.0,
    }
    clock.value = 10014.999
    assert reads.reserve_member(1001, address())["next_at"] == 10015
    clock.value = 10015
    assert reads.reserve_member(1001, address())["allowed"]
    assert reads.status()["reservations_total"] == 3


def test_cooldown_is_per_account_and_canonical_target_not_global_target(tmp_path):
    clock = Clock()
    reads = make_reads(tmp_path, clock)
    assert reads.reserve_member(1001, address())["allowed"]
    assert reads.reserve_member(1002, address())["allowed"]
    clock.value += 2
    # Private and group addresses with the same numeric ID are different targets.
    assert reads.reserve_member(1001, address(kind="private"))["allowed"]
    assert reads.reserve_member(1001, address())["next_at"] == 10015


def test_global_rolling_cap_across_accounts_targets_and_exact_boundary(tmp_path):
    clock = Clock()
    reads = make_reads(tmp_path, clock, member_global_per_minute=2)
    other = ReadRequests.open_existing(reads.path, clock=clock)
    assert reads.reserve_member(1001, address())["allowed"]
    clock.value += 1
    assert other.reserve_member(1002, address(124))["allowed"]
    assert reads.reserve_member(1003, address(125)) == {
        "allowed": False,
        "reason": "member_global_limit",
        "next_at": 10060.0,
    }
    clock.value = 10059.999
    assert other.reserve_member(1004, address())["next_at"] == 10060
    clock.value = 10060
    assert other.reserve_member(1004, address())["allowed"]
    assert reads.status()["reserved_last_minute"] == 2


def test_restart_and_cross_handle_use_latest_policy_and_history(tmp_path):
    clock = Clock()
    old = make_reads(tmp_path, clock, member_interval_seconds=1, member_account_interval_seconds=1)
    for index in range(3):
        assert old.reserve_member(1001, address())["allowed"]
        clock.value += 1
    tightened = ReadPolicy(member_global_per_minute=1, member_interval_seconds=30)
    ReadRequests(old.path, "fleet", tightened, clock=clock)
    assert old.policy == tightened
    reopened = ReadRequests.open_existing(old.path, clock=clock)
    assert reopened.policy == tightened
    assert old.reserve_member(1002, address(124)) == {
        "allowed": False,
        "reason": "member_global_limit",
        "next_at": 10062.0,
    }
    assert reopened.status()["reservations_total"] == 3


def test_tightened_existing_deadlines_remain_committed_after_loosening(tmp_path):
    clock = Clock()
    old = make_reads(tmp_path, clock, member_interval_seconds=30, member_account_interval_seconds=20)
    assert old.reserve_member(1001, address())["allowed"]
    loose = ReadPolicy(member_interval_seconds=1, member_account_interval_seconds=1)
    ReadRequests(old.path, "fleet", loose, clock=clock)
    assert old.reserve_member(1001, address(124))["next_at"] == 10020
    assert old.reserve_member(1001, address())["next_at"] == 10030
    # Tighten then loosen without an intervening reservation: fences still hold.
    ReadRequests(
        old.path,
        "fleet",
        ReadPolicy(member_interval_seconds=70, member_account_interval_seconds=60),
        clock=clock,
    )
    ReadRequests(old.path, "fleet", loose, clock=clock)
    assert old.reserve_member(1001, address(124))["next_at"] == 10060
    assert old.reserve_member(1001, address())["next_at"] == 10070


def test_clock_rollback_anchor_persisted_even_on_denial(tmp_path):
    clock = Clock()
    reads = make_reads(tmp_path, clock)
    assert reads.reserve_member(1001, address())["allowed"]
    clock.value = 10001
    assert not reads.reserve_member(1001, address())["allowed"]
    clock.value = 1
    reopened = ReadRequests.open_existing(reads.path, clock=clock)
    assert reopened.status()["now"] == 10001
    assert reopened.reserve_member(1001, address())["next_at"] == 10015
    assert reads.status()["last_wallclock"] == 10001
    clock.value = 10015
    assert reopened.reserve_member(1001, address())["allowed"]


@pytest.mark.parametrize("value", [None, True, "1", -1, float("inf"), float("nan")])
def test_invalid_clock_fails_without_permit(tmp_path, value):
    clock = Clock()
    reads = make_reads(tmp_path, clock)
    clock.value = value
    with pytest.raises(ValueError):
        reads.reserve_member(1001, address())
    clock.value = 10000
    assert reads.status()["reservations_total"] == 0
    assert reads.audit() == []


def test_unrepresentable_deadline_rolls_back_entire_reservation(tmp_path):
    clock = Clock()
    reads = make_reads(tmp_path, clock)
    clock.value = 1e308
    with pytest.raises(ValueError, match="future deadline"):
        reads.reserve_member(1001, address())
    clock.value = 10000
    assert reads.status()["last_wallclock"] == 10000
    assert reads.status()["reservations_total"] == 0
    assert reads.audit() == []


@pytest.mark.parametrize("account", [None, True, False, "", "01", "0", -1, 1.0, " 1", "1:2"])
def test_invalid_account_rejected_before_recording(tmp_path, account):
    reads = make_reads(tmp_path)
    with pytest.raises(ValueError):
        reads.reserve_member(account, address())
    assert reads.audit() == []


@pytest.mark.parametrize(
    "target",
    [
        None,
        "",
        "group:123",
        "obf:other:group:123",
        "obf:fleet:group:0123",
        "obf:fleet:group:0",
        "obf:fleet:friend:123",
        "obf:fleet:group:123:extra",
        " obf:fleet:group:123",
    ],
)
def test_noncanonical_or_other_namespace_target_rejected(tmp_path, target):
    reads = make_reads(tmp_path)
    with pytest.raises(ValueError):
        reads.reserve_member(1001, target)
    assert reads.audit() == []


def test_metadata_only_and_read_only_diagnostics(tmp_path):
    reads = make_reads(tmp_path)
    account, target = "98765432123456789", address(8765432123456789)
    reads.reserve_member(account, target)
    reads.reserve_member(account, target)
    before = reads.path.read_bytes()
    opened = ReadRequests.open_existing(reads.path, clock=Clock(20000))
    status, audit = opened.status(), opened.audit()
    assert reads.path.read_bytes() == before
    assert status["reservations_total"] == 1
    assert status["rejections_total"] == 1
    assert len(audit) == 2 and audit[0]["allowed"] is False
    assert audit[1]["allowed"] is True
    encoded = json.dumps([status, audit], allow_nan=False)
    with sqlite3.connect(reads.path) as db:
        dump = "\n".join(db.iterdump())
    for raw in (account, target, "8765432123456789"):
        assert raw not in encoded and raw not in dump
    for limit in (True, 0, -1, 1001, "1", 1.5):
        with pytest.raises(ValueError):
            reads.audit(limit)


def test_concurrent_handles_serialize_same_account_and_pair(tmp_path):
    reads = make_reads(tmp_path)

    def reserve(_):
        return ReadRequests.open_existing(reads.path, clock=Clock()).reserve_member(1001, address())

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(reserve, range(24)))
    assert sum(result["allowed"] for result in results) == 1
    assert reads.status()["reservations_total"] == 1
    assert len(reads.audit()) == 24


def test_concurrent_processes_serialize_global_cap(tmp_path):
    reads = make_reads(tmp_path, member_global_per_minute=3)
    with ProcessPoolExecutor(max_workers=3, mp_context=multiprocessing.get_context("spawn")) as pool:
        results = list(pool.map(reserve_in_process, [(str(reads.path), 1000 + i) for i in range(12)]))
    assert sum(result["allowed"] for result in results) == 3
    assert reads.status()["reservations_total"] == 3
    assert reads.status()["rejections_total"] == 9


def test_missing_file_never_recreated_by_open_or_stale_handle(tmp_path):
    missing = tmp_path / "absent.sqlite3"
    with pytest.raises(FileNotFoundError):
        ReadRequests.open_existing(missing)
    assert not missing.exists()
    reads = make_reads(tmp_path)
    reads.path.unlink()
    for action in (
        lambda: reads.reserve_member(1001, address()),
        reads.status,
        reads.audit,
        lambda: reads.policy,
    ):
        with pytest.raises(sqlite3.Error):
            action()
        assert not reads.path.exists()


def test_wrong_namespace_fails_without_reconfiguration(tmp_path):
    reads = make_reads(tmp_path)
    before = reads.path.read_bytes()
    with pytest.raises(RuntimeError, match="logical_id mismatch"):
        ReadRequests(reads.path, "other", ReadPolicy(member_interval_seconds=1))
    assert reads.path.read_bytes() == before
    assert ReadRequests.open_existing(reads.path).logical_id == "fleet"


@pytest.mark.parametrize(
    "setup",
    [
        "CREATE TABLE alien (id INTEGER PRIMARY KEY)",
        "CREATE VIEW alien AS SELECT 1",
        "PRAGMA application_id=123",
        "PRAGMA user_version=1",
        "CREATE TABLE read_meta (singleton INTEGER PRIMARY KEY, logical_id TEXT)",
    ],
)
def test_constructor_does_not_adopt_alien_database(tmp_path, setup):
    path = tmp_path / "alien.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute(setup)
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="schema mismatch"):
        ReadRequests(path, "fleet", ReadPolicy(), clock=Clock())
    assert path.read_bytes() == before


def test_open_empty_or_corrupt_database_fails_closed(tmp_path):
    path = tmp_path / "invalid.sqlite3"
    path.write_bytes(b"")
    with pytest.raises(RuntimeError, match="schema mismatch"):
        ReadRequests.open_existing(path)
    path.write_bytes(b"not sqlite")
    with pytest.raises(sqlite3.Error):
        ReadRequests.open_existing(path)
    with pytest.raises(sqlite3.Error):
        ReadRequests(path, "fleet", ReadPolicy())
    assert path.read_bytes() == b"not sqlite"


@pytest.mark.parametrize(
    "tamper",
    [
        "PRAGMA application_id=0",
        "PRAGMA user_version=99",
        "CREATE TABLE extra (id INTEGER)",
        "DROP INDEX read_member_time",
        "CREATE VIEW extra AS SELECT 1",
        "CREATE TRIGGER extra AFTER INSERT ON read_members BEGIN DELETE FROM read_members; END",
        "ALTER TABLE read_members ADD COLUMN extra TEXT",
        "DELETE FROM read_meta",
        "UPDATE read_meta SET policy='{}'",
        "UPDATE read_meta SET policy='null'",
        "UPDATE read_meta SET policy='not-json'",
        "UPDATE read_meta SET last_wallclock=-1",
        "UPDATE read_meta SET logical_id='bad:namespace'",
    ],
)
def test_schema_and_metadata_tampering_blocks_all_paths(tmp_path, tamper):
    reads = make_reads(tmp_path)
    with sqlite3.connect(reads.path) as db:
        db.execute(tamper)
    for action in (
        lambda: ReadRequests.open_existing(reads.path),
        lambda: ReadRequests(reads.path, "fleet", ReadPolicy()),
        lambda: reads.reserve_member(1001, address()),
        reads.status,
        reads.audit,
    ):
        with pytest.raises(RuntimeError):
            action()


@pytest.mark.parametrize(
    "tamper",
    [
        "UPDATE read_members SET reserved_at=-1",
        "UPDATE read_members SET reserved_at=20000",
        "UPDATE read_members SET reserved_at='bad'",
        "UPDATE read_members SET account_next_at=0",
        "UPDATE read_members SET target_next_at=1e999",
        "UPDATE read_members SET account_hash='not-a-hash'",
        "UPDATE read_members SET target_hash=replace(target_hash,'a','G') || 'G'",
        "UPDATE read_audit SET next_at=0",
        "UPDATE read_audit SET allowed=2",
        "UPDATE read_audit SET reason='cached'",
        "UPDATE read_audit SET at=1e999",
    ],
)
def test_corrupt_history_cannot_bypass_admission(tmp_path, tamper):
    reads = make_reads(tmp_path)
    assert reads.reserve_member(1001, address())["allowed"]
    with sqlite3.connect(reads.path) as db:
        db.execute(tamper)
    with pytest.raises(RuntimeError, match="history"):
        reads.reserve_member(1002, address(124))
    with pytest.raises(RuntimeError, match="history"):
        ReadRequests.open_existing(reads.path)


def test_persistence_exception_rolls_back_and_returns_no_permit(tmp_path, monkeypatch):
    clock = Clock()
    reads = make_reads(tmp_path, clock)
    original = reads._transaction

    @contextmanager
    def failing_transaction(*, create=False, write=False):
        with original(create=create, write=write) as db:
            if write:
                # Fail AFTER the permit row was inserted but before commit.
                db.set_authorizer(
                    lambda action, table, _column, _database, _source: (
                        sqlite3.SQLITE_DENY
                        if action == sqlite3.SQLITE_INSERT and table == "read_audit"
                        else sqlite3.SQLITE_OK
                    )
                )
            yield db

    monkeypatch.setattr(reads, "_transaction", failing_transaction)
    clock.value += 1
    with pytest.raises(sqlite3.Error, match="not authorized"):
        reads.reserve_member(1001, address())
    assert reads.status()["last_wallclock"] == 10000
    assert reads.status()["reservations_total"] == 0
    assert reads.audit() == []
