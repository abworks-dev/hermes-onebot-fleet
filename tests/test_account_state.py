import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from hermes_onebot_fleet.account_state import AccountState
from hermes_onebot_fleet.store import Store


@pytest.fixture
def state(tmp_path):
    return AccountState(tmp_path / "ledger.sqlite3.accounts.sqlite3", "fleet", ["a", "b", "c"])


def test_initial_state_and_sidecar_leave_message_ledger_untouched(tmp_path):
    ledger = tmp_path / "ledger.sqlite3"
    store = Store(ledger)
    store.enqueue("pending", {"private": "message content"})
    before = ledger.read_bytes()
    sidecar = ledger.with_name(ledger.name + ".accounts.sqlite3")
    state = AccountState(sidecar, "fleet", ["a", "b"])
    assert state.path == sidecar
    assert state.snapshot("a") == {
        "account_id": "a",
        "state": "allowed",
        "revision": 0,
        "current_primary": None,
    }
    assert state.is_allowed("a")
    assert state.is_allowed("a", revision=0)
    assert state.audit() == []
    assert state.status() == {
        "logical_id": "fleet",
        "current_primary": None,
        "accounts": [state.snapshot("a"), state.snapshot("b")],
    }
    state.change("a", "pause", "operator maintenance")
    assert ledger.read_bytes() == before
    assert store.status() == {"inbound": {"queued": 1}, "outbound": {}}
    with sqlite3.connect(ledger) as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables == {"inbound", "outbound"}


@pytest.mark.parametrize(
    "action,expected",
    [
        ("pause", "paused"),
        ("quarantine", "quarantined"),
        ("observe", "observing"),
        ("allow", "allowed"),
    ],
)
def test_actions_increment_revision_and_block_unless_explicitly_allowed(state, action, expected):
    before = state.snapshot("a")
    result = state.change("a", action, "controlled operator reason")
    assert result["state"] == expected
    assert result["revision"] == 1
    assert state.is_allowed("a") is (action == "allow")
    assert not state.is_allowed("a", before["revision"])
    result = state.change("a", action, "repeat operator action")
    assert result["revision"] == 2
    assert state.audit()[0]["action"] == action
    assert state.audit()[0]["revision"] == 2
    assert state.snapshot("b")["revision"] == 0


def test_quarantine_latches_until_manual_allow_and_survives_reinitialization(state):
    state.change("a", "quarantine", "operator risk review")
    for action in ["observe", "pause", "quarantine"]:
        assert state.change("a", action, "recovery check")["state"] == "quarantined"
    restarted = AccountState(state.path, "fleet", ["a", "b", "c"])
    assert restarted.snapshot("a")["state"] == "quarantined"
    assert restarted.snapshot("a")["revision"] == 4
    admitted = restarted.change("a", "allow", "manual readmission approved")
    assert admitted["state"] == "allowed"
    assert admitted["revision"] == 5
    assert restarted.is_allowed("a", 5)
    assert not restarted.is_allowed("a", 0)
    assert not restarted.is_allowed("a", 4)
    assert state.is_allowed("a", 5)


@pytest.mark.parametrize("action", ["pause", "quarantine", "observe", "allow"])
def test_removal_and_readdition_preserve_admission_and_advance_revision(state, action):
    previous = state.change("b", action, "operator decision")
    removed = AccountState(state.path, "fleet", ["a"])
    assert [row["account_id"] for row in removed.status()["accounts"]] == ["a"]
    # Even an existing adapter/CLI cannot authorize a removed account.
    for old_instance in [state, removed, AccountState.open_existing(state.path)]:
        with pytest.raises(ValueError, match="unknown or unconfigured"):
            old_instance.is_allowed("b")
        with pytest.raises(ValueError, match="unknown or unconfigured"):
            old_instance.change("b", "allow", "stale configuration")
    with sqlite3.connect(state.path) as db:
        assert db.execute(
            "SELECT state, revision, active FROM account_states WHERE account_id = 'b'"
        ).fetchone() == (previous["state"], previous["revision"] + 1, 0)
    readded = AccountState(state.path, "fleet", ["a", "b", "d"])
    assert readded.snapshot("b") == {**previous, "revision": previous["revision"] + 1}
    assert not readded.is_allowed("b", previous["revision"])
    assert readded.snapshot("d")["state"] == "allowed"
    assert readded.snapshot("d")["revision"] == 0
    assert readded.is_allowed("b") is (action == "allow")


def test_empty_configuration_deactivates_without_deleting_or_readmitting(state):
    state.change("a", "pause", "maintenance")
    empty = AccountState(state.path, "fleet", [])
    assert empty.status()["accounts"] == []
    assert empty.select_primary([]) is None
    assert AccountState.open_existing(state.path).status()["accounts"] == []
    readded = AccountState(state.path, "fleet", ["a"])
    assert readded.snapshot("a")["state"] == "paused"
    assert readded.snapshot("a")["revision"] == 2


def test_remove_readd_never_resurrects_old_permits_and_unchanged_config_keeps_revisions(state):
    captured = state.snapshot("b")["revision"]
    unchanged = state.snapshot("a")
    assert state.is_allowed("b", captured)
    AccountState(state.path, "fleet", ["a", "c"])
    # Repeating the reduced configuration must not revoke an already-inactive
    # row again, nor invalidate work belonging to still-configured accounts.
    AccountState(state.path, "fleet", ["a", "c"])
    assert state.snapshot("a") == unchanged
    with sqlite3.connect(state.path) as db:
        assert (
            db.execute("SELECT revision FROM account_states WHERE account_id = 'b'").fetchone()[0]
            == captured + 1
        )
    readded = AccountState(state.path, "fleet", ["a", "b", "c"])
    assert readded.is_allowed("b")
    assert not readded.is_allowed("b", captured)
    assert not state.is_allowed("b", captured)
    assert readded.snapshot("b")["revision"] == captured + 1
    assert readded.snapshot("a") == unchanged
    repeated = AccountState(state.path, "fleet", ["a", "b", "c"])
    assert repeated.snapshot("b")["revision"] == captured + 1
    assert repeated.snapshot("a") == unchanged
    assert repeated.is_allowed("b", captured + 1)


@pytest.mark.parametrize(
    "method",
    [
        lambda state: state.snapshot("missing"),
        lambda state: state.is_allowed("missing"),
        lambda state: state.change("missing", "allow", "manual admission"),
        lambda state: state.promote("missing", "manual promotion"),
        lambda state: state.select_primary(["a", "missing"]),
        lambda state: state.snapshot(None),
        lambda state: state.is_allowed([]),
        lambda state: state.select_primary([[]]),
    ],
)
def test_unknown_identifiers_fail_closed_without_side_effects(state, method):
    state.promote("b", "initial selection")
    before, audit = state.status(), state.audit()
    with pytest.raises(ValueError, match="unknown or unconfigured"):
        method(state)
    assert state.status() == before
    assert state.audit() == audit


@pytest.mark.parametrize("action", ["", "recover", "resume", "allowed", "PAUSE", None, [], 1])
def test_invalid_actions_do_not_change_state_or_audit(state, action):
    with pytest.raises(ValueError, match="invalid account action"):
        state.change("a", action, "operator action")
    assert state.snapshot("a")["revision"] == 0
    assert state.audit() == []


@pytest.mark.parametrize(
    "reason",
    [
        "",
        "   ",
        "x" * 201,
        None,
        42,
        [],
        "invisible\u200btext",
        "separator\u2028text",
        *["before" + chr(code) + "after" for code in [*range(32), *range(127, 160)]],
    ],
)
def test_invalid_reasons_rejected_for_changes_and_promotions(state, reason):
    for operation in [
        lambda: state.change("a", "pause", reason),
        lambda: state.promote("a", reason),
    ]:
        with pytest.raises(ValueError, match="reason must contain"):
            operation()
    assert state.snapshot("a")["revision"] == 0
    assert state.snapshot("a")["current_primary"] is None
    assert state.audit() == []


def test_printable_unicode_and_exact_200_character_reasons_allowed(state):
    reason = "人工审核 " + "x" * 195
    assert len(reason) == 200
    state.change("a", "observe", reason)
    assert state.audit()[0]["reason"] == reason
    assert not state.is_allowed("a")


@pytest.mark.parametrize("revision", [-1, True, 1.0, "0", [], {}])
def test_invalid_revision_rejected(state, revision):
    with pytest.raises(ValueError, match="revision must be"):
        state.is_allowed("a", revision)


def test_sticky_failover_survives_restart_and_never_automatically_fails_back(state):
    assert state.select_primary(["a", "b"]) == "a"
    assert state.select_primary(["b"]) == "b"
    assert state.select_primary(["a", "b"]) == "b"
    restarted = AccountState(state.path, "fleet", ["c", "b", "a"])
    assert restarted.select_primary(["a", "c", "b"]) == "b"
    assert restarted.snapshot("a")["current_primary"] == "b"
    assert restarted.audit() == []  # Selection is not an operator lifecycle action.


def test_total_outage_does_not_erase_saved_primary_or_enable_failback(state):
    assert state.select_primary(["a", "b"]) == "a"
    assert state.select_primary(["b"]) == "b"
    assert state.select_primary([]) is None
    assert state.snapshot("a")["current_primary"] == "b"
    restarted = AccountState.open_existing(state.path)
    assert restarted.select_primary([]) is None
    assert restarted.select_primary(["a", "b"]) == "b"


def test_blocking_primary_preserves_preference_but_selection_never_admits_it(state):
    state.promote("b", "operator choice")
    state.change("b", "quarantine", "operator review")
    assert state.snapshot("b")["current_primary"] == "b"
    assert state.select_primary(["b"]) is None
    assert state.snapshot("b")["current_primary"] == "b"
    assert state.select_primary(["b", "a", "c"]) == "a"
    state.change("b", "allow", "manual readmission")
    assert state.select_primary(["b", "a"]) == "a"


@pytest.mark.parametrize("action", ["pause", "quarantine", "observe"])
def test_promotion_of_blocked_account_rejected_without_side_effects(state, action):
    state.promote("a", "operator choice")
    state.change("b", action, "operator hold")
    before, audit = state.status(), state.audit()
    with pytest.raises(ValueError, match="cannot promote a blocked account"):
        state.promote("b", "attempted promotion")
    assert state.status() == before
    assert state.audit() == audit


def test_explicit_promotion_is_audited_persisted_and_does_not_change_admission_revision(state):
    state.select_primary(["a", "b"])
    result = state.promote("b", "manual cutover approved")
    assert result == {"account_id": "b", "state": "allowed", "revision": 0, "current_primary": "b"}
    restarted = AccountState(state.path, "fleet", ["a", "b", "c"])
    assert restarted.select_primary(["a", "b"]) == "b"
    assert restarted.audit()[0]["action"] == "promote"
    assert restarted.audit()[0]["current_primary"] == "b"
    assert restarted.is_allowed("b", 0)


def test_audit_is_newest_first_bounded_and_contains_only_operator_metadata(state):
    for index in range(105):
        state.change("a", "pause", f"operator decision {index}")
    rows = state.audit()
    assert len(rows) == 100
    assert rows[0]["reason"] == "operator decision 104"
    assert rows[-1]["reason"] == "operator decision 5"
    assert [row["sequence"] for row in rows] == sorted([row["sequence"] for row in rows], reverse=True)
    assert set(rows[0]) == {
        "sequence",
        "timestamp",
        "account_id",
        "action",
        "reason",
        "state",
        "revision",
        "current_primary",
    }
    assert rows[0]["revision"] == 105
    assert isinstance(rows[0]["timestamp"], float)
    encoded = json.dumps(rows)
    assert "message" not in encoded and "token" not in encoded and "payload" not in encoded


def test_two_instances_observe_immediate_committed_changes_and_stale_revisions_fail(state):
    cli = AccountState.open_existing(state.path)
    captured = state.snapshot("a")
    cli.change("a", "pause", "operator maintenance")
    assert not state.is_allowed("a", captured["revision"])
    cli.change("a", "allow", "manual readmission")
    assert state.is_allowed("a")
    assert not state.is_allowed("a", captured["revision"])
    assert state.is_allowed("a", 2)


def test_concurrent_instances_never_lose_revisions_or_audit_actions(state):
    instances = [state, *[AccountState.open_existing(state.path) for _ in range(7)]]
    barrier = Barrier(len(instances))

    def change_repeatedly(instance):
        barrier.wait(timeout=10)
        return [instance.change("a", "pause", "concurrent operator hold")["revision"] for _ in range(8)]

    with ThreadPoolExecutor(max_workers=len(instances)) as executor:
        results = list(executor.map(change_repeatedly, instances))
    assert sorted(revision for batch in results for revision in batch) == list(range(1, 65))
    assert state.snapshot("a")["revision"] == 64
    assert len(state.audit()) == 64


def test_concurrent_snapshots_pair_state_and_revision_transactionally(state):
    reader = AccountState.open_existing(state.path)
    barrier = Barrier(2)

    def write_changes():
        barrier.wait(timeout=10)
        for revision in range(1, 41):
            state.change("a", "pause" if revision % 2 else "allow", "operator transition")

    def read_snapshots():
        barrier.wait(timeout=10)
        for _ in range(80):
            snapshot = reader.snapshot("a")
            assert snapshot["state"] == ("paused" if snapshot["revision"] % 2 else "allowed")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(write_changes), executor.submit(read_snapshots)]
        for future in futures:
            future.result(timeout=20)
    assert state.snapshot("a")["revision"] == 40


def test_concurrent_selection_makes_one_persisted_sticky_decision(state):
    instances = [AccountState.open_existing(state.path) for _ in range(8)]
    barrier = Barrier(len(instances))

    def select(item):
        index, instance = item
        barrier.wait(timeout=10)
        return instance.select_primary(["a", "b"] if index % 2 else ["b", "a"])

    with ThreadPoolExecutor(max_workers=len(instances)) as executor:
        choices = list(executor.map(select, enumerate(instances)))
    assert len(set(choices)) == 1
    assert state.snapshot("a")["current_primary"] == choices[0]


def test_concurrent_initialization_same_namespace_is_safe(tmp_path):
    path = tmp_path / "concurrent.sqlite3"
    barrier = Barrier(6)

    def initialize(_):
        barrier.wait(timeout=10)
        return AccountState(path, "fleet", ["a", "b"]).snapshot("a")

    with ThreadPoolExecutor(max_workers=6) as executor:
        snapshots = list(executor.map(initialize, range(6)))
    assert all(row["revision"] == 0 and row["state"] == "allowed" for row in snapshots)


def test_change_and_promotion_roll_back_when_audit_write_fails(state):
    state.promote("a", "initial choice")
    before, audit = state.status(), state.audit()
    with sqlite3.connect(state.path) as db:
        db.execute("""
            CREATE TRIGGER reject_audit BEFORE INSERT ON account_audit
            BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END
        """)
    for operation in [
        lambda: state.change("a", "quarantine", "operator hold"),
        lambda: state.promote("b", "manual cutover"),
    ]:
        with pytest.raises(sqlite3.IntegrityError, match="audit unavailable"):
            operation()
        assert state.status() == before
        assert state.audit() == audit


def test_failed_initialization_rolls_back_membership_and_new_rows(state):
    state.change("b", "quarantine", "operator hold")
    before = state.status()
    with sqlite3.connect(state.path) as db:
        db.execute("""
            CREATE TRIGGER reject_new_account BEFORE INSERT ON account_states
            WHEN NEW.account_id = 'new'
            BEGIN SELECT RAISE(ABORT, 'configuration rejected'); END
        """)
    with pytest.raises(sqlite3.IntegrityError, match="configuration rejected"):
        AccountState(state.path, "fleet", ["a", "new"])
    assert state.status() == before
    assert AccountState.open_existing(state.path).status() == before


def test_namespace_mismatch_never_reconciles_configuration_or_resets_state(state):
    state.change("b", "quarantine", "operator review")
    state.promote("a", "operator choice")
    before, audit = state.status(), state.audit()
    with pytest.raises(ValueError, match="namespace mismatch"):
        AccountState(state.path, "different-fleet", ["b", "new"])
    assert state.status() == before
    assert state.audit() == audit
    assert AccountState.open_existing(state.path).status() == before


def test_namespace_is_rechecked_on_every_operation(state):
    with sqlite3.connect(state.path) as db:
        db.execute("UPDATE account_namespace SET logical_id = 'other' WHERE singleton = 1")
    for operation in [
        lambda: state.snapshot("a"),
        lambda: state.is_allowed("a"),
        lambda: state.status(),
        lambda: state.change("a", "allow", "manual admission"),
        lambda: state.audit(),
        lambda: state.select_primary(["a"]),
        lambda: state.promote("a", "manual promotion"),
    ]:
        with pytest.raises(ValueError, match="namespace mismatch"):
            operation()


def test_open_existing_does_not_create_missing_sidecar_or_parent(tmp_path):
    missing = tmp_path / "missing-directory" / "accounts.sqlite3"
    with pytest.raises(sqlite3.OperationalError):
        AccountState.open_existing(missing)
    assert not missing.exists()
    assert not missing.parent.exists()


def test_open_existing_does_not_bootstrap_an_unrelated_database(tmp_path):
    path = tmp_path / "not-accounts.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE unrelated (value TEXT)")
    before = path.read_bytes()
    with pytest.raises(sqlite3.OperationalError):
        AccountState.open_existing(path)
    assert path.read_bytes() == before


def test_open_existing_does_not_readmit_or_reconcile_removed_accounts(state):
    state.change("a", "quarantine", "operator review")
    reduced = AccountState(state.path, "fleet", ["a"])
    opened = AccountState.open_existing(state.path)
    assert opened.status() == reduced.status()
    assert opened.snapshot("a")["state"] == "quarantined"
    with pytest.raises(ValueError, match="unknown or unconfigured"):
        opened.snapshot("b")


@pytest.mark.parametrize(
    "logical_id,accounts",
    [
        ("", ["a"]),
        ("fleet\n", ["a"]),
        (None, ["a"]),
        ("fleet", "a"),
        ("fleet", ["a", "a"]),
        ("fleet", [""]),
        ("fleet", [None]),
    ],
)
def test_invalid_initialization_inputs_do_not_create_database(tmp_path, logical_id, accounts):
    path = tmp_path / "new.sqlite3"
    with pytest.raises(ValueError):
        AccountState(path, logical_id, accounts)
    assert not path.exists()


@pytest.mark.parametrize("candidates", ["a", ("a",), None, {}])
def test_invalid_candidate_collections_do_not_mutate_primary(state, candidates):
    with pytest.raises(ValueError, match="candidates must be a list"):
        state.select_primary(candidates)
    assert state.snapshot("a")["current_primary"] is None
