"""Fail-closed, process-safe admission for member read RPCs.

Reserve BEFORE querying. A reservation is an irrevocable charge, irrespective of
RPC success, rejection, timeout, or process death; this is not a result cache.
Health policy is shared configuration; health scheduling is the caller's job.
The path names a dedicated sidecar, never the transport or delivery ledger.
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
class ReadPolicy:
    health_interval_seconds: float = 60
    idle_health_interval_seconds: float = 180
    health_backoff_max_seconds: float = 600
    member_interval_seconds: float = 15
    member_account_interval_seconds: float = 2
    member_global_per_minute: int = 30

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == "member_global_per_minute":
                if type(value) is not int or not 1 <= value <= 3600:
                    raise ValueError(f"{field.name} must be an integer in 1..3600")
            elif type(value) not in (int, float) or not 1 <= value <= 3600 or not math.isfinite(value):
                raise ValueError(f"{field.name} must be finite and in 1..3600 seconds")
        if self.idle_health_interval_seconds < self.health_interval_seconds:
            raise ValueError("idle_health_interval_seconds must be >= health_interval_seconds")
        if self.health_backoff_max_seconds < self.idle_health_interval_seconds:
            raise ValueError("health_backoff_max_seconds must be >= idle_health_interval_seconds")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> ReadPolicy:
        if not isinstance(mapping, Mapping):
            raise ValueError("read policy must be a mapping")
        if set(mapping) - {field.name for field in fields(cls)}:
            raise ValueError("unknown read policy keys")
        return cls(**mapping)


class ReadRequests:
    """One logical namespace, persisted policy, and atomic decision per sidecar.

    Only construction configures policy. Stale handles use the latest persisted
    policy. Reconfiguration commits stricter deadlines for existing records;
    later loosening cannot erase these or the original reservation deadlines.
    Denials advance the monotonic wallclock anchor but never pacing deadlines.
    """

    _APPLICATION_ID = 0x48465231
    _VERSION = 1
    _SCHEMA = {
        "read_meta": (
            "table",
            "CREATE TABLE read_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
            "logical_id TEXT NOT NULL, policy TEXT NOT NULL, last_wallclock REAL NOT NULL)",
        ),
        "read_members": (
            "table",
            "CREATE TABLE read_members (id INTEGER PRIMARY KEY, account_hash TEXT NOT NULL, "
            "target_hash TEXT NOT NULL, reserved_at REAL NOT NULL, "
            "account_next_at REAL NOT NULL, target_next_at REAL NOT NULL)",
        ),
        "read_member_time": ("index", "CREATE INDEX read_member_time ON read_members(reserved_at)"),
        "read_account_time": (
            "index",
            "CREATE INDEX read_account_time ON read_members(account_hash, reserved_at)",
        ),
        "read_target_time": (
            "index",
            "CREATE INDEX read_target_time ON read_members(account_hash, target_hash, reserved_at)",
        ),
        "read_audit": (
            "table",
            "CREATE TABLE read_audit (id INTEGER PRIMARY KEY, account_hash TEXT NOT NULL, "
            "target_hash TEXT NOT NULL, at REAL NOT NULL, allowed INTEGER NOT NULL, "
            "reason TEXT NOT NULL, next_at REAL NOT NULL)",
        ),
    }

    def __init__(
        self,
        path: str | Path,
        logical_id: str,
        policy: ReadPolicy,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._setup(path, clock)
        self._validate_logical_id(logical_id)
        if not isinstance(policy, ReadPolicy):
            raise ValueError("policy must be a ReadPolicy")
        policy.__post_init__()
        self.logical_id = logical_id
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._transaction(create=True) as db:
            if not db.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone():
                if (
                    db.execute("PRAGMA application_id").fetchone()[0]
                    or db.execute("PRAGMA user_version").fetchone()[0]
                ):
                    raise RuntimeError("read requests schema mismatch")
                for _, statement in self._SCHEMA.values():
                    db.execute(statement)
                db.execute(f"PRAGMA application_id={self._APPLICATION_ID}")
                db.execute(f"PRAGMA user_version={self._VERSION}")
                db.execute(
                    "INSERT INTO read_meta VALUES (1,?,?,?)",
                    (logical_id, json.dumps(asdict(policy), sort_keys=True), self._now()),
                )
            else:
                self._validate(db)
                # Commit tightened fences now, even if no query occurs before
                # another constructor loosens the policy again.
                db.execute(
                    "UPDATE read_members SET "
                    "account_next_at=max(account_next_at,reserved_at+?), "
                    "target_next_at=max(target_next_at,reserved_at+?)",
                    (policy.member_account_interval_seconds, policy.member_interval_seconds),
                )
                db.execute(
                    "UPDATE read_meta SET policy=? WHERE singleton=1",
                    (json.dumps(asdict(policy), sort_keys=True),),
                )
                self._validate(db)

    def _setup(self, path: str | Path, clock: Callable[[], float] | None) -> None:
        if not isinstance(path, (str, Path)) or not str(path).strip() or str(path) == ":memory:":
            raise ValueError("read requests requires a persistent sidecar path")
        self.path = Path(path).resolve()
        self._clock = time.time if clock is None else clock
        if not callable(self._clock):
            raise ValueError("clock must be callable")

    @staticmethod
    def _validate_logical_id(value: object) -> None:
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
            raise ValueError("invalid logical_id")

    @classmethod
    def open_existing(cls, path: str | Path, *, clock: Callable[[], float] | None = None) -> ReadRequests:
        """Verify an existing sidecar; never create or reconfigure one."""
        instance = cls.__new__(cls)
        instance._setup(path, clock)
        if not instance.path.is_file():
            raise FileNotFoundError(instance.path)
        with instance._transaction() as db:
            meta, _ = instance._validate(db, discover=True)
            instance.logical_id = meta["logical_id"]
        return instance

    @contextmanager
    def _transaction(self, *, create: bool = False, write: bool = False):
        uri = self.path.as_uri() + ("?mode=rwc" if create else "?mode=rw")
        db = sqlite3.connect(uri, uri=True, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA trusted_schema=OFF")
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

    def _validate(self, db: sqlite3.Connection, *, discover: bool = False):
        schema = {row["name"]: (row["type"], row["sql"]) for row in db.execute("SELECT * FROM sqlite_master")}
        if (
            db.execute("PRAGMA application_id").fetchone()[0] != self._APPLICATION_ID
            or db.execute("PRAGMA user_version").fetchone()[0] != self._VERSION
            or schema != self._SCHEMA
        ):
            raise RuntimeError("read requests schema mismatch")
        rows = db.execute("SELECT * FROM read_meta").fetchall()
        if len(rows) != 1 or rows[0]["singleton"] != 1:
            raise RuntimeError("read requests metadata mismatch")
        meta = rows[0]
        try:
            self._validate_logical_id(meta["logical_id"])
            mapping = json.loads(meta["policy"])
            if not isinstance(mapping, dict) or set(mapping) != {field.name for field in fields(ReadPolicy)}:
                raise ValueError("persisted policy must contain every field")
            policy = ReadPolicy.from_mapping(mapping)
            self._valid_time(meta["last_wallclock"])
        except (ValueError, TypeError, OverflowError) as error:
            raise RuntimeError("invalid persisted read requests metadata") from error
        if not discover and meta["logical_id"] != self.logical_id:
            raise RuntimeError("read requests logical_id mismatch")
        # Validate history as well as schema: malformed anchors must not bypass
        # a cap or a committed deadline. SQLite accepts infinities and weak types.
        for table, time_column in (("read_members", "reserved_at"), ("read_audit", "at")):
            invalid = (
                "typeof(id) != 'integer' OR id < 1 "
                "OR typeof(account_hash) != 'text' OR length(account_hash) != 64 "
                "OR account_hash GLOB '*[^0-9a-f]*' "
                "OR typeof(target_hash) != 'text' OR length(target_hash) != 64 "
                "OR target_hash GLOB '*[^0-9a-f]*' "
                f"OR typeof({time_column}) NOT IN ('integer','real') "
                f"OR {time_column} < 0 OR {time_column} > ? "
            )
            if table == "read_members":
                invalid += (
                    "OR typeof(account_next_at) NOT IN ('integer','real') "
                    "OR typeof(target_next_at) NOT IN ('integer','real') "
                    "OR account_next_at <= reserved_at OR target_next_at <= reserved_at "
                    "OR account_next_at > 1.7976931348623157e308 "
                    "OR target_next_at > 1.7976931348623157e308"
                )
            else:
                invalid += (
                    "OR typeof(allowed) != 'integer' OR allowed NOT IN (0,1) "
                    "OR typeof(next_at) NOT IN ('integer','real') OR next_at < at "
                    "OR next_at > 1.7976931348623157e308 "
                    "OR (allowed=1 AND reason != 'reserved') "
                    "OR (allowed=0 AND reason NOT IN "
                    "('member_global_limit','member_account_interval','member_target_interval'))"
                )
            if db.execute(
                f"SELECT 1 FROM {table} WHERE {invalid} LIMIT 1", (meta["last_wallclock"],)
            ).fetchone():
                raise RuntimeError("invalid persisted read requests history")
        return meta, policy

    @staticmethod
    def _valid_time(value: object) -> float:
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError("clock must return finite nonnegative epoch seconds")
        return float(value)

    def _now(self) -> float:
        return self._valid_time(self._clock())

    @staticmethod
    def _future(now: float, interval: float) -> float:
        value = now + interval
        if not math.isfinite(value) or value <= now:
            raise ValueError("clock cannot represent a finite future deadline")
        return value

    def _hashes(self, account_id: str | int, target: str) -> tuple[str, str]:
        if type(account_id) not in (str, int) or not re.fullmatch(r"[1-9][0-9]{0,127}", str(account_id)):
            raise ValueError("account_id must be a positive decimal QQ id")
        if not isinstance(target, str):
            raise ValueError("target must be a canonical Fleet target")
        match = re.fullmatch(r"obf:([A-Za-z0-9_-]{1,128}):(private|group):([1-9][0-9]{0,127})", target)
        if not match or match[1] != self.logical_id:
            raise ValueError("target must be canonical and belong to this logical_id")
        return tuple(hashlib.sha256(value.encode("utf-8")).hexdigest() for value in (str(account_id), target))

    @property
    def policy(self) -> ReadPolicy:
        with self._transaction() as db:
            return self._validate(db)[1]

    @staticmethod
    def _global_deadline(db: sqlite3.Connection, now: float, cap: int) -> float:
        count = db.execute("SELECT count(*) FROM read_members WHERE reserved_at>?", (now - 60,)).fetchone()[0]
        if count < cap:
            return now
        # For a tightened cap, enough entries must expire, not just the oldest.
        row = db.execute(
            "SELECT reserved_at FROM read_members WHERE reserved_at>? "
            "ORDER BY reserved_at,id LIMIT 1 OFFSET ?",
            (now - 60, count - cap),
        ).fetchone()
        return ReadRequests._future(row[0], 60)

    def reserve_member(self, account_id: str | int, target: str) -> dict:
        """Commit a permit before RPC; uncertainty never refunds or caches it.

        Storage, namespace, schema and clock errors raise: callers must not query
        after an exception. A denial is retryable no earlier than next_at.
        """
        account_hash, target_hash = self._hashes(account_id, target)
        with self._transaction(write=True) as db:
            meta, policy = self._validate(db)
            now = max(self._now(), meta["last_wallclock"])
            db.execute("UPDATE read_meta SET last_wallclock=? WHERE singleton=1", (now,))
            account_next, target_next = db.execute(
                "SELECT max(account_next_at), "
                "max(CASE WHEN target_hash=? THEN target_next_at END) "
                "FROM read_members WHERE account_hash=?",
                (target_hash, account_hash),
            ).fetchone()
            deadlines = [
                ("member_global_limit", self._global_deadline(db, now, policy.member_global_per_minute))
            ]
            if account_next is not None:
                deadlines.append(("member_account_interval", account_next))
            if target_next is not None:
                deadlines.append(("member_target_interval", target_next))
            reason, next_at = max(deadlines, key=lambda item: item[1])
            allowed = next_at <= now
            result = {
                "allowed": allowed,
                "reason": "reserved" if allowed else reason,
                "next_at": float(next_at),
            }
            if allowed:
                db.execute(
                    "INSERT INTO read_members(account_hash,target_hash,reserved_at,account_next_at,target_next_at) "
                    "VALUES (?,?,?,?,?)",
                    (
                        account_hash,
                        target_hash,
                        now,
                        self._future(now, policy.member_account_interval_seconds),
                        self._future(now, policy.member_interval_seconds),
                    ),
                )
            db.execute(
                "INSERT INTO read_audit(account_hash,target_hash,at,allowed,reason,next_at) VALUES (?,?,?,?,?,?)",
                (account_hash, target_hash, now, int(allowed), result["reason"], result["next_at"]),
            )
            return result

    def status(self) -> dict:
        """Read-only JSON-safe metadata; no account/target identifiers or results."""
        with self._transaction() as db:
            meta, policy = self._validate(db)
            now = max(self._now(), meta["last_wallclock"])
            counts = db.execute(
                "SELECT count(*),count(DISTINCT account_hash),count(DISTINCT target_hash),max(reserved_at) "
                "FROM read_members"
            ).fetchone()
            recent = db.execute(
                "SELECT count(*) FROM read_members WHERE reserved_at>?", (now - 60,)
            ).fetchone()[0]
            return {
                "logical_id": self.logical_id,
                "policy": asdict(policy),
                "now": now,
                "last_wallclock": float(meta["last_wallclock"]),
                "reservations_total": counts[0],
                "accounts_total": counts[1],
                "targets_total": counts[2],
                "last_reserved_at": counts[3],
                "reserved_last_minute": recent,
                "rejections_total": db.execute("SELECT count(*) FROM read_audit WHERE allowed=0").fetchone()[
                    0
                ],
                "next_global_at": self._global_deadline(db, now, policy.member_global_per_minute),
            }

    def audit(self, limit: int = 100) -> list[dict]:
        """Newest-first hashed admission metadata, never RPC results."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("audit limit must be an integer in 1..1000")
        with self._transaction() as db:
            self._validate(db)
            return [
                dict(row) | {"allowed": bool(row["allowed"])}
                for row in db.execute("SELECT * FROM read_audit ORDER BY id DESC LIMIT ?", (limit,))
            ]
