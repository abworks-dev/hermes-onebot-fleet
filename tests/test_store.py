import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from hermes_qq_failover.lock import InstanceLock
from hermes_qq_failover.store import Store


def test_duplicate_enqueue_and_atomic_claim(tmp_path):
    store = Store(tmp_path / "state.db")
    assert store.enqueue("same", {"user": "test"})
    assert not store.enqueue("same", {"user": "test"})
    with ThreadPoolExecutor(max_workers=4) as executor:
        claimed = list(executor.map(lambda _: store.claim(), range(4)))
    assert sum(row is not None for row in claimed) == 1
    store.recover()
    assert store.status()["inbound"] == {"uncertain": 1}
    assert store.claim() is None


def test_only_queued_inputs_survive_for_dispatch(tmp_path):
    store = Store(tmp_path / "state.db")
    for key in ["queued", "handed"]:
        store.enqueue(key, {"key": key})
    first = store.claim()
    store.inbound_result(first["key"], "handed_off")
    restarted = Store(store.path)
    restarted.recover()
    assert restarted.status()["inbound"] == {"handed_off": 1, "queued": 1}
    assert restarted.claim()["key"] != first["key"]


def test_sending_is_uncertain_after_crash_and_never_replayed(tmp_path):
    store = Store(tmp_path / "state.db")
    assert store.begin_send("reply", "target", "private text", "bot")
    store.recover()
    assert not store.begin_send("reply", "target", "private text", "standby")
    assert store.outbound("reply")["state"] == "uncertain"
    assert "private text" not in json.dumps(store.uncertain())


def test_process_lock_exclusive_and_releasable(tmp_path):
    a, b = InstanceLock(tmp_path / "test.lock"), InstanceLock(tmp_path / "test.lock")
    a.acquire()
    with pytest.raises(RuntimeError):
        b.acquire()
    a.release()
    b.acquire()
    b.release()


def test_store_does_not_change_hermes_database(tmp_path):
    path = tmp_path / "plugin" / "ledger.sqlite3"
    Store(path)
    with sqlite3.connect(path) as db:
        assert {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")} == {
            "inbound",
            "outbound",
        }
