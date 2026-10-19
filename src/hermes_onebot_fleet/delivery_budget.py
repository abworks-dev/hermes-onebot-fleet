"""Conservative, process-safe delivery reservations in a dedicated SQLite sidecar.

Reservations are permanent charges, not acknowledgements or permission to replay.
After every attempted wire RPC, complete() extends pacing from its actual finish.
On exclusive-owner startup, recover() fences crash-uncertain prior attempts.
There are deliberately no account, send-type, refund, or message-content APIs.
The supplied path is the actual sidecar filename, not the transport ledger path.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import time
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields
from pathlib import Path


@dataclass(frozen=True)
class BudgetPolicy:
    global_per_hour: int = 60
    target_per_hour: int = 30
    min_interval_seconds: float = 5
    target_interval_seconds: float = 10
    max_pending_age_seconds: float = 900

    def __post_init__(self) -> None:
        for name in ("global_per_hour", "target_per_hour"):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 3600:
                raise ValueError(f"{name} must be an integer in 1..3600")
        for name in ("min_interval_seconds", "target_interval_seconds", "max_pending_age_seconds"):
            value = getattr(self, name)
            upper = 86400 if name == "max_pending_age_seconds" else 3600
            if type(value) not in (int, float) or not 1 <= value <= upper or not math.isfinite(value):
                raise ValueError(f"{name} must be finite and in 1..{upper} seconds")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> BudgetPolicy:
        if not isinstance(mapping, Mapping):
            raise ValueError("delivery budget policy must be a mapping")
        if set(mapping) - {field.name for field in fields(cls)}:
            raise ValueError("unknown delivery budget policy keys")
        return cls(**mapping)


class DeliveryBudget:
    """One logical namespace per sidecar; all physical senders share its budget.

    Reconfiguration is explicit through the constructor. Existing handles always
    use the latest persisted policy. Old not-before deadlines are never shortened
    by reconfiguration, and stricter intervals also apply to prior reservations.
    """

    _APPLICATION_ID = 0x48464231
    _VERSION = 2
    _COLUMNS = {
        "budget_meta": ("singleton", "logical_id", "policy", "last_wallclock"),
        "budget_reservations": (
            "key_hash",
            "target_hash",
            "reserved_at",
            "global_next_at",
            "target_next_at",
            "paced_at",
        ),
        "budget_audit": ("id", "key_hash", "target_hash", "at", "allowed", "reason", "next_at"),
    }

    def __init__(
        self,
        path: str | Path,
        logical_id: str,
        policy: BudgetPolicy,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._set_path(path)
        self._validate_logical_id(logical_id)
        if not isinstance(policy, BudgetPolicy):
            raise ValueError("policy must be a BudgetPolicy")
        # Validate even a frozen dataclass manipulated through object.__setattr__.
        policy.__post_init__()
        self.logical_id = logical_id
        self._clock = time.time if clock is None else clock
        if not callable(self._clock):
            raise ValueError("clock must be callable")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._transaction(create=True) as db:
            tables = self._tables(db)
            if not tables:
                if (
                    db.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
                    or db.execute("PRAGMA application_id").fetchone()[0]
                    or db.execute("PRAGMA user_version").fetchone()[0]
                ):
                    raise RuntimeError("delivery budget schema mismatch")
                self._create(db, policy)
            else:
                self._validate(db)
                db.execute(
                    "UPDATE budget_meta SET policy=? WHERE singleton=1",
                    (json.dumps(asdict(policy), sort_keys=True),),
                )

    def _set_path(self, path: str | Path) -> None:
        if not isinstance(path, (str, Path)) or not str(path).strip() or str(path) == ":memory:":
            raise ValueError("delivery budget requires a persistent sidecar path")
        self.path = Path(path).resolve()

    @staticmethod
    def _validate_logical_id(logical_id: str) -> None:
        if not isinstance(logical_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", logical_id):
            raise ValueError("invalid logical_id")

    @classmethod
    def open_existing(cls, path: str | Path, *, clock: Callable[[], float] | None = None) -> DeliveryBudget:
        """Open only a pre-existing valid sidecar, without changing its policy."""
        instance = cls.__new__(cls)
        instance._set_path(path)
        instance._clock = time.time if clock is None else clock
        if not callable(instance._clock):
            raise ValueError("clock must be callable")
        if not instance.path.is_file():
            raise FileNotFoundError(instance.path)
        with instance._transaction() as db:
            meta, _ = instance._validate(db, discover=True)
            instance.logical_id = meta["logical_id"]
        return instance

    @contextmanager
    def _transaction(self, *, create: bool = False, write: bool = False):
        # URI mode=rw prevents reserve/status from silently recreating a removed
        # sidecar. BEGIN IMMEDIATE serializes the complete admission decision.
        uri = self.path.as_uri() + ("?mode=rwc" if create else "?mode=rw")
        db = sqlite3.connect(uri, uri=True, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE" if create or write else "BEGIN")
            yield db
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()

    @staticmethod
    def _tables(db: sqlite3.Connection) -> set[str]:
        return {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    def _create(self, db: sqlite3.Connection, policy: BudgetPolicy) -> None:
        # Do not use executescript: it implicitly commits an existing transaction.
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
        for statement in statements:
            db.execute(statement)
        db.execute(f"PRAGMA application_id={self._APPLICATION_ID}")
        db.execute(f"PRAGMA user_version={self._VERSION}")
        db.execute(
            "INSERT INTO budget_meta VALUES (1,?,?,?)",
            (self.logical_id, json.dumps(asdict(policy), sort_keys=True), self._now()),
        )

    def _validate(self, db: sqlite3.Connection, *, discover: bool = False):
        if (
            db.execute("PRAGMA application_id").fetchone()[0] != self._APPLICATION_ID
            or db.execute("PRAGMA user_version").fetchone()[0] != self._VERSION
            or self._tables(db) != set(self._COLUMNS)
        ):
            raise RuntimeError("delivery budget schema mismatch")
        if db.execute("SELECT 1 FROM sqlite_master WHERE type IN ('view','trigger') LIMIT 1").fetchone():
            raise RuntimeError("delivery budget schema mismatch")
        for table, columns in self._COLUMNS.items():
            info = db.execute(f"PRAGMA table_info({table})").fetchall()
            if (
                tuple(row[1] for row in info) != columns
                or tuple(row[5] for row in info) != (1,) + (0,) * (len(columns) - 1)
                or tuple(row[3] for row in info) != (0,) + (1,) * (len(columns) - 1)
            ):
                raise RuntimeError("delivery budget schema mismatch")
        rows = db.execute("SELECT * FROM budget_meta").fetchall()
        if len(rows) != 1 or rows[0]["singleton"] != 1:
            raise RuntimeError("delivery budget metadata mismatch")
        meta = rows[0]
        try:
            self._validate_logical_id(meta["logical_id"])
            mapping = json.loads(meta["policy"])
            if not isinstance(mapping, dict) or set(mapping) != {
                field.name for field in fields(BudgetPolicy)
            }:
                raise ValueError("persisted policy must contain every field")
            policy = BudgetPolicy.from_mapping(mapping)
            self._valid_time(meta["last_wallclock"])
        except (ValueError, TypeError) as error:
            raise RuntimeError("invalid persisted delivery budget metadata") from error
        if not discover and meta["logical_id"] != self.logical_id:
            raise RuntimeError("delivery budget logical_id mismatch")
        # A malformed anchor must never remove a completion/recovery fence.
        invalid = db.execute(
            "SELECT 1 FROM budget_reservations WHERE "
            "typeof(paced_at) NOT IN ('integer','real') OR paced_at < reserved_at "
            "OR paced_at > ? OR paced_at < 0 OR paced_at > 1.7976931348623157e308 "
            "OR global_next_at < paced_at OR target_next_at < paced_at LIMIT 1",
            (meta["last_wallclock"],),
        ).fetchone()
        if invalid:
            raise RuntimeError("invalid persisted delivery budget pacing")
        return meta, policy

    @staticmethod
    def _valid_time(value: object) -> float:
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError("clock must return finite nonnegative epoch seconds")
        return float(value)

    def _now(self) -> float:
        return self._valid_time(self._clock())

    @staticmethod
    def _digest(value: str, name: str) -> str:
        if not isinstance(value, str) or not value or len(value) > 4096:
            raise ValueError(f"{name} must be a nonempty string of at most 4096 characters")
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @property
    def policy(self) -> BudgetPolicy:
        with self._transaction() as db:
            return self._validate(db)[1]

    @staticmethod
    def _deadline(db, now, cap, *, target_hash=None):
        where = "reserved_at > ?"
        args = [now - 3600]
        if target_hash is not None:
            where += " AND target_hash=?"
            args.append(target_hash)
        count = db.execute(f"SELECT count(*) FROM budget_reservations WHERE {where}", args).fetchone()[0]
        if count < cap:
            return now
        # Tightening below the existing count requires enough old entries to
        # expire, not merely the first one. The window is (now-3600, now].
        row = db.execute(
            f"SELECT reserved_at FROM budget_reservations WHERE {where} "
            "ORDER BY reserved_at LIMIT 1 OFFSET ?",
            (*args, count - cap),
        ).fetchone()
        return float(row[0] + 3600)

    def reserve(self, key: str, target: str) -> dict:
        key_hash = self._digest(key, "key")
        target_hash = self._digest(target, "target")
        with self._transaction(write=True) as db:
            meta, policy = self._validate(db)
            now = max(self._now(), meta["last_wallclock"])
            db.execute("UPDATE budget_meta SET last_wallclock=? WHERE singleton=1", (now,))
            prior = db.execute("SELECT 1 FROM budget_reservations WHERE key_hash=?", (key_hash,)).fetchone()
            if prior:
                result = {"allowed": False, "reason": "prior_reservation", "next_at": now}
            else:
                global_last = db.execute(
                    "SELECT max(global_next_at) AS next_at, max(paced_at) AS paced_at "
                    "FROM budget_reservations"
                ).fetchone()
                target_last = db.execute(
                    "SELECT max(target_next_at) AS next_at, max(paced_at) AS paced_at "
                    "FROM budget_reservations WHERE target_hash=?",
                    (target_hash,),
                ).fetchone()
                deadlines = [
                    ("global_hourly_limit", self._deadline(db, now, policy.global_per_hour)),
                    (
                        "target_hourly_limit",
                        self._deadline(db, now, policy.target_per_hour, target_hash=target_hash),
                    ),
                ]
                if global_last["paced_at"] is not None:
                    deadlines.append(
                        (
                            "global_interval",
                            max(
                                global_last["next_at"],
                                global_last["paced_at"] + policy.min_interval_seconds,
                            ),
                        )
                    )
                if target_last["paced_at"] is not None:
                    deadlines.append(
                        (
                            "target_interval",
                            max(
                                target_last["next_at"],
                                target_last["paced_at"] + policy.target_interval_seconds,
                            ),
                        )
                    )
                reason, next_at = max(deadlines, key=lambda item: item[1])
                allowed = next_at <= now
                result = {
                    "allowed": allowed,
                    "reason": "reserved" if allowed else reason,
                    "next_at": float(next_at),
                }
                if allowed:
                    db.execute(
                        "INSERT INTO budget_reservations VALUES (?,?,?,?,?,?)",
                        (
                            key_hash,
                            target_hash,
                            now,
                            now + policy.min_interval_seconds,
                            now + policy.target_interval_seconds,
                            now,
                        ),
                    )
            db.execute(
                "INSERT INTO budget_audit(key_hash,target_hash,at,allowed,reason,next_at) VALUES (?,?,?,?,?,?)",
                (key_hash, target_hash, now, int(result["allowed"]), result["reason"], result["next_at"]),
            )
            return result

    def complete(self, key: str) -> None:
        """Fence pacing after a wire attempt finishes, without changing its charge.

        The adapter must serialize reserve/RPC/complete under its send lock and
        call this in the RPC's finally block, irrespective of ACK or failure.
        This is not an acknowledgement API and never permits replay. Unknown
        keys fail closed; persistence errors must stop future wire sends.
        """
        key_hash = self._digest(key, "key")
        with self._transaction(write=True) as db:
            meta, policy = self._validate(db)
            now = max(self._now(), meta["last_wallclock"])
            if not db.execute("SELECT 1 FROM budget_reservations WHERE key_hash=?", (key_hash,)).fetchone():
                raise ValueError("cannot complete an unknown delivery reservation")
            db.execute(
                "UPDATE budget_reservations SET paced_at=max(paced_at,?), "
                "global_next_at=max(global_next_at,?), target_next_at=max(target_next_at,?) "
                "WHERE key_hash=?",
                (now, now + policy.min_interval_seconds, now + policy.target_interval_seconds, key_hash),
            )
            db.execute("UPDATE budget_meta SET last_wallclock=? WHERE singleton=1", (now,))

    def recover(self) -> None:
        """Fence crash-uncertain history on explicit exclusive-owner startup.

        Call ONLY after the adapter acquires its exclusive InstanceLock, before
        business sends resume. Read-only diagnostics must never call this. One
        latest row per target carries the new pacing anchor; permanent charges,
        reservation times, and replay protection remain untouched. An empty
        history has nothing to fence. Repeated recovery conservatively extends
        deadlines and is not implicit in construction, opening, or status.
        """
        with self._transaction(write=True) as db:
            meta, policy = self._validate(db)
            now = max(self._now(), meta["last_wallclock"])
            db.execute(
                "UPDATE budget_reservations SET paced_at=max(paced_at,?), "
                "global_next_at=max(global_next_at,?), target_next_at=max(target_next_at,?) "
                "WHERE rowid IN (SELECT max(rowid) FROM budget_reservations GROUP BY target_hash)",
                (now, now + policy.min_interval_seconds, now + policy.target_interval_seconds),
            )
            db.execute("UPDATE budget_meta SET last_wallclock=? WHERE singleton=1", (now,))

    def status(self) -> dict:
        """Return JSON-safe counters/times/policy, never keys or target contents."""
        with self._transaction() as db:
            meta, policy = self._validate(db)
            now = max(self._now(), meta["last_wallclock"])
            counts = db.execute(
                "SELECT count(*), count(DISTINCT target_hash), max(reserved_at), max(global_next_at), "
                "max(paced_at) "
                "FROM budget_reservations"
            ).fetchone()
            recent = db.execute(
                "SELECT count(*) FROM budget_reservations WHERE reserved_at>?", (now - 3600,)
            ).fetchone()[0]
            rejections = db.execute("SELECT count(*) FROM budget_audit WHERE allowed=0").fetchone()[0]
            next_at = self._deadline(db, now, policy.global_per_hour)
            if counts[2] is not None:
                next_at = max(next_at, counts[3], counts[4] + policy.min_interval_seconds)
            return {
                "logical_id": self.logical_id,
                "policy": asdict(policy),
                "now": now,
                "last_wallclock": float(meta["last_wallclock"]),
                "reservations_total": counts[0],
                "reserved_last_hour": recent,
                "targets_total": counts[1],
                "rejections_total": rejections,
                "last_reserved_at": counts[2],
                "next_global_at": float(next_at),
            }

    def audit(self, limit: int = 100) -> list[dict]:
        """Newest-first metadata only; hashes cannot expose messages or tokens."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("audit limit must be an integer in 1..1000")
        with self._transaction() as db:
            self._validate(db)
            rows = db.execute(
                "SELECT id,key_hash,target_hash,at,allowed,reason,next_at "
                "FROM budget_audit ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) | {"allowed": bool(row["allowed"])} for row in rows]
