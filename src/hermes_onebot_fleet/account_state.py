"""Isolated, durable account admission and sticky-primary state.

Pass the sidecar path, not the message ledger path: callers derive it with
``ledger.with_name(ledger.name + '.accounts.sqlite3')``. This module never opens
Hermes, QQ, or the message ledger. New configured accounts start allowed; removal
deactivates rows and advances revision to invalidate captured permits. Re-adding
never resets admission or revision.

Reasons are controlled, operator-written labels (1-200 printable characters),
NOT message text, credentials, tokens, or other secrets. They are persisted in
the audit trail. This API cannot identify secrets in otherwise printable text.

Each operation owns a connection and an explicit SQLite transaction. Writers
use BEGIN IMMEDIATE so live adapters and local operator processes cannot lose
updates or make a primary decision from a stale admission snapshot.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

_ACTION_STATES = {
    "pause": "paused",
    "quarantine": "quarantined",
    "observe": "observing",
    "allow": "allowed",
}


class AccountState:
    """One logical fleet per sidecar; a different namespace fails closed.

    ``change`` always increments the account's revision, including repeated
    actions. Quarantine is latched: pause/observe do not clear it; only explicit
    manual ``allow`` admits it again. Observing is never admission.

    Primary preference and admission are separate. Blocking an account does not
    erase its preference. Selection only uses allowed configured candidates; an
    empty eligible set returns None without erasing the last sticky preference.
    ``promote`` changes preference, not admission/revision, and cannot test live
    transport health. Callers must separately enforce connection/readiness gates.
    """

    def __init__(self, path: str | Path, logical_id: str, account_ids: Iterable[str]):
        self.path = Path(path)
        self.logical_id = self._identifier(logical_id)
        if isinstance(account_ids, (str, bytes)):
            raise ValueError("account_ids must be an iterable of account identifiers")
        ids = [self._identifier(account_id) for account_id in account_ids]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate account identifiers")
        self.account_ids = frozenset(ids)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db(write=True, create=True) as db:
            # No executescript: it implicitly commits and would defeat rollback.
            db.execute("""
                CREATE TABLE IF NOT EXISTS account_namespace (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    logical_id TEXT NOT NULL,
                    current_primary TEXT)
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS account_states (
                    account_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL CHECK (
                        state IN ('allowed', 'paused', 'quarantined', 'observing')),
                    revision INTEGER NOT NULL CHECK (revision >= 0),
                    active INTEGER NOT NULL CHECK (active IN (0, 1)))
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS account_audit (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    account_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    state TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    current_primary TEXT)
            """)
            db.execute(
                "INSERT OR IGNORE INTO account_namespace VALUES (1, ?, NULL)",
                (self.logical_id,),
            )
            self._namespace(db)
            # Removing an active account revokes captured permits permanently,
            # even if later configuration re-adds the same allowed account.
            # Repeated initialization/removal must not churn other revisions.
            removed = [
                (row["account_id"],)
                for row in db.execute("SELECT account_id FROM account_states WHERE active = 1")
                if row["account_id"] not in self.account_ids
            ]
            db.executemany(
                "UPDATE account_states SET active = 0, revision = revision + 1 "
                "WHERE account_id = ? AND active = 1",
                removed,
            )
            db.executemany(
                "INSERT INTO account_states VALUES (?, 'allowed', 0, 1) "
                "ON CONFLICT(account_id) DO UPDATE SET active = 1",
                [(account_id,) for account_id in sorted(self.account_ids)],
            )

    @classmethod
    def open_existing(cls, path: str | Path) -> AccountState:
        """Open CLI state without creating a file or reconciling configuration.

        Missing files raise sqlite3.OperationalError. Invalid/mismatched schemas
        also fail closed; this method never bootstraps or re-admits accounts.
        """
        instance = cls.__new__(cls)
        instance.path = Path(path)
        with instance._db() as db:
            row = db.execute("SELECT * FROM account_namespace WHERE singleton = 1").fetchone()
            if row is None:
                raise ValueError("missing account namespace")
            instance.logical_id = instance._identifier(row["logical_id"])
            instance.account_ids = frozenset(
                row["account_id"]
                for row in db.execute("SELECT account_id FROM account_states WHERE active = 1")
            )
            instance._namespace(db)
        return instance

    @staticmethod
    def _identifier(value: str) -> str:
        if not isinstance(value, str) or not value.strip() or not value.isprintable():
            raise ValueError("identifier must be a nonempty printable string")
        return value

    @staticmethod
    def _reason(reason: str) -> str:
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 200 or not reason.isprintable():
            raise ValueError("reason must contain 1-200 printable characters and no controls")
        return reason

    @contextmanager
    def _db(self, *, write: bool = False, create: bool = False) -> Iterator[sqlite3.Connection]:
        # mode=rw avoids silently replacing a removed sidecar with an empty file.
        target = self.path if create else self.path.resolve().as_uri() + "?mode=rw"
        db = sqlite3.connect(target, uri=not create, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _namespace(self, db: sqlite3.Connection) -> sqlite3.Row:
        row = db.execute("SELECT * FROM account_namespace WHERE singleton = 1").fetchone()
        if row is None or row["logical_id"] != self.logical_id:
            raise ValueError("account state namespace mismatch")
        return row

    def _account(self, db: sqlite3.Connection, account_id: str) -> sqlite3.Row:
        if not isinstance(account_id, str) or account_id not in self.account_ids:
            raise ValueError("unknown or unconfigured account identifier")
        row = db.execute("SELECT * FROM account_states WHERE account_id = ?", (account_id,)).fetchone()
        if row is None or not row["active"]:
            raise ValueError("unknown or unconfigured account identifier")
        return row

    @staticmethod
    def _snapshot(row: sqlite3.Row, primary: str | None) -> dict:
        return {
            "account_id": row["account_id"],
            "state": row["state"],
            "revision": row["revision"],
            "current_primary": primary,
        }

    def snapshot(self, account_id: str) -> dict:
        """Return admission/revision and saved primary in one read transaction."""
        with self._db() as db:
            namespace = self._namespace(db)
            return self._snapshot(self._account(db, account_id), namespace["current_primary"])

    def is_allowed(self, account_id: str, revision: int | None = None) -> bool:
        """Check current admission, optionally requiring an exact captured revision.

        Unknown/removed IDs raise ValueError rather than being admitted. A stale
        revision returns False, even when an account has since been re-allowed.
        """
        if revision is not None and (type(revision) is not int or revision < 0):
            raise ValueError("revision must be a nonnegative integer or None")
        with self._db() as db:
            self._namespace(db)
            row = self._account(db, account_id)
            return row["state"] == "allowed" and (revision is None or revision == row["revision"])

    def change(self, account_id: str, action: str, reason: str) -> dict:
        """Apply pause/quarantine/observe/allow and audit atomically.

        ``allow`` is explicit manual admission, never automatic recovery. Reasons
        must be controlled operator labels and must not contain secrets or text
        copied from messages.
        """
        if not isinstance(action, str) or action not in _ACTION_STATES:
            raise ValueError("invalid account action")
        self._reason(reason)
        with self._db(write=True) as db:
            namespace = self._namespace(db)
            row = self._account(db, account_id)
            state = _ACTION_STATES[action]
            if row["state"] == "quarantined" and action != "allow":
                state = "quarantined"
            db.execute(
                "UPDATE account_states SET state = ?, revision = revision + 1 WHERE account_id = ?",
                (state, account_id),
            )
            result = self._snapshot(self._account(db, account_id), namespace["current_primary"])
            self._record(db, result, action, reason)
            return result

    @staticmethod
    def _record(db: sqlite3.Connection, snapshot: dict, action: str, reason: str) -> None:
        db.execute(
            "INSERT INTO account_audit "
            "(timestamp, account_id, action, reason, state, revision, current_primary) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                time.time(),
                snapshot["account_id"],
                action,
                reason,
                snapshot["state"],
                snapshot["revision"],
                snapshot["current_primary"],
            ),
        )

    def audit(self) -> list[dict]:
        """Return at most 100 newest operator actions, never message/token data."""
        with self._db() as db:
            self._namespace(db)
            return [
                dict(row)
                for row in db.execute("SELECT * FROM account_audit ORDER BY sequence DESC LIMIT 100")
            ]

    def status(self) -> dict:
        """Return all active account snapshots and preference in one transaction."""
        with self._db() as db:
            namespace = self._namespace(db)
            primary = namespace["current_primary"]
            return {
                "logical_id": self.logical_id,
                "current_primary": primary,
                "accounts": [
                    self._snapshot(row, primary)
                    for row in db.execute("SELECT * FROM account_states WHERE active = 1 ORDER BY account_id")
                ],
            }

    def select_primary(self, candidates: list[str]) -> str | None:
        """Atomically retain a candidate primary, else select the first allowed one.

        The caller supplies globally healthy candidates in its configured order.
        There is no timer, priority failback, or target-specific availability
        check. Empty/all-blocked candidate sets do NOT erase sticky preference.
        """
        if not isinstance(candidates, list):
            raise ValueError("candidates must be a list of account identifiers")
        with self._db(write=True) as db:
            namespace = self._namespace(db)
            rows = [self._account(db, account_id) for account_id in candidates]
            allowed = [row["account_id"] for row in rows if row["state"] == "allowed"]
            if not allowed:
                return None
            saved = namespace["current_primary"]
            selected = saved if saved in allowed else allowed[0]
            db.execute("UPDATE account_namespace SET current_primary = ? WHERE singleton = 1", (selected,))
            return selected

    def promote(self, account_id: str, reason: str) -> dict:
        """Explicitly prefer an allowed account; persist and audit without re-admission."""
        self._reason(reason)
        with self._db(write=True) as db:
            self._namespace(db)
            row = self._account(db, account_id)
            if row["state"] != "allowed":
                raise ValueError("cannot promote a blocked account")
            db.execute("UPDATE account_namespace SET current_primary = ? WHERE singleton = 1", (account_id,))
            result = self._snapshot(row, account_id)
            self._record(db, result, "promote", reason)
            return result
