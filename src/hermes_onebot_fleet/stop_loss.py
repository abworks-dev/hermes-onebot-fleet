"""Durable engineering STOPLOSS evidence and independent account/fleet holds.

Call record ONLY for actual dispatched sends, never health/member-preflight
results. This is not diagnosis, a ban label, delivery admission, or a replay API.
An eligible different account is NOT permission to switch bots around a hold;
callers can conservatively stop all business when status()['business_held'].
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
class StopLossPolicy:
    window_seconds: float = 300
    account_failures: int = 3
    account_targets: int = 3
    fleet_failures: int = 4
    fleet_targets: int = 3
    fleet_accounts: int = 2

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == "window_seconds":
                if type(value) not in (int, float) or not 1 <= value <= 3600 or not math.isfinite(value):
                    raise ValueError("window_seconds must be finite and in 1..3600 seconds")
            elif type(value) is not int or not 1 <= value <= 3600:
                raise ValueError(f"{field.name} must be an integer in 1..3600")
        if self.account_targets > self.account_failures:
            raise ValueError("account_targets must be <= account_failures")
        if max(self.fleet_targets, self.fleet_accounts) > self.fleet_failures:
            raise ValueError("fleet_targets and fleet_accounts must be <= fleet_failures")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> StopLossPolicy:
        if not isinstance(mapping, Mapping):
            raise ValueError("stop loss policy must be a mapping")
        if set(mapping) - {field.name for field in fields(cls)}:
            raise ValueError("unknown stop loss policy keys")
        return cls(**mapping)


class StopLoss:
    """A dedicated sidecar, serialized writes, permanent intent deduplication.

    Construction explicitly persists policy and may latch holds from history.
    Stale handles always read the latest policy; no configuration change, clock
    advance, success, or reset of another scope releases a hold. Storage errors
    raise, and callers must fail closed. Only reset releases its exact scope.
    """

    _APPLICATION_ID = 0x48465331
    _VERSION = 1
    _SCHEMA = {
        "stop_meta": (
            "table",
            "CREATE TABLE stop_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
            "logical_id TEXT NOT NULL, policy TEXT NOT NULL, last_wallclock REAL NOT NULL, "
            "last_event INTEGER NOT NULL)",
        ),
        "stop_events": (
            "table",
            "CREATE TABLE stop_events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, "
            "account_hash TEXT NOT NULL, target_hash TEXT NOT NULL, intent_hash TEXT NOT NULL, "
            "code TEXT NOT NULL, at REAL NOT NULL, evidence_seq INTEGER NOT NULL, "
            "reason_hash TEXT NOT NULL, failures INTEGER NOT NULL, targets INTEGER NOT NULL, "
            "accounts INTEGER NOT NULL, policy TEXT NOT NULL)",
        ),
        "stop_intent": (
            "index",
            "CREATE UNIQUE INDEX stop_intent ON stop_events(intent_hash) WHERE kind='evidence'",
        ),
        "stop_time": ("index", "CREATE INDEX stop_time ON stop_events(kind, at)"),
        "stop_account": ("index", "CREATE INDEX stop_account ON stop_events(account_hash, kind, id)"),
    }
    _CODES = frozenset({"target_rejected", "outcome_unknown"})
    _KINDS = frozenset({"evidence", "account_hold", "fleet_hold", "account_reset", "fleet_reset"})

    def __init__(
        self,
        path: str | Path,
        logical_id: str,
        policy: StopLossPolicy,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._setup(path, clock)
        self._validate_logical_id(logical_id)
        if not isinstance(policy, StopLossPolicy):
            raise ValueError("policy must be a StopLossPolicy")
        policy.__post_init__()
        self.logical_id = logical_id
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._transaction(create=True) as db:
            if not db.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone():
                if (
                    db.execute("PRAGMA application_id").fetchone()[0]
                    or db.execute("PRAGMA user_version").fetchone()[0]
                ):
                    raise RuntimeError("stop loss schema mismatch")
                for _, statement in self._SCHEMA.values():
                    db.execute(statement)
                db.execute(f"PRAGMA application_id={self._APPLICATION_ID}")
                db.execute(f"PRAGMA user_version={self._VERSION}")
                now = self._now()
                self._window_start(now, policy.window_seconds)
                db.execute(
                    "INSERT INTO stop_meta VALUES (1,?,?,?,0)",
                    (logical_id, self._serialize(policy), now),
                )
            else:
                meta, _ = self._validate(db)
                now = max(self._now(), meta["last_wallclock"])
                self._window_start(now, policy.window_seconds)
                db.execute(
                    "UPDATE stop_meta SET policy=?,last_wallclock=? WHERE singleton=1",
                    (self._serialize(policy), now),
                )
                self._latch(db, now, policy)

    def _setup(self, path: str | Path, clock: Callable[[], float] | None) -> None:
        if not isinstance(path, (str, Path)) or not str(path).strip() or str(path) == ":memory:":
            raise ValueError("stop loss requires a persistent sidecar path")
        self.path = Path(path).resolve()
        self._clock = time.time if clock is None else clock
        if not callable(self._clock):
            raise ValueError("clock must be callable")

    @staticmethod
    def _validate_logical_id(value: object) -> None:
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
            raise ValueError("invalid logical_id")

    @classmethod
    def open_existing(cls, path: str | Path, *, clock: Callable[[], float] | None = None) -> StopLoss:
        """Read/verify only; never create, sample the clock, configure, or latch."""
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

    @staticmethod
    def _serialize(policy: StopLossPolicy) -> str:
        return json.dumps(asdict(policy), sort_keys=True)

    @staticmethod
    def _parse_policy(value: str) -> StopLossPolicy:
        mapping = json.loads(value)
        if not isinstance(mapping, dict) or set(mapping) != {field.name for field in fields(StopLossPolicy)}:
            raise ValueError("persisted policy must contain every field")
        return StopLossPolicy.from_mapping(mapping)

    @staticmethod
    def _valid_time(value: object) -> float:
        if type(value) not in (int, float) or value < 0:
            raise ValueError("clock must return finite nonnegative epoch seconds")
        try:
            converted = float(value)
        except OverflowError as error:
            raise ValueError("clock must return finite nonnegative epoch seconds") from error
        if not math.isfinite(converted):
            raise ValueError("clock must return finite nonnegative epoch seconds")
        return converted

    def _now(self) -> float:
        return self._valid_time(self._clock())

    @staticmethod
    def _window_start(now: float, window: float) -> float:
        cutoff = now - window
        if not math.isfinite(cutoff) or cutoff >= now:
            raise ValueError("clock cannot represent a finite rolling window")
        return cutoff

    @staticmethod
    def _is_hash(value: object) -> bool:
        return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None

    def _validate(self, db: sqlite3.Connection, *, discover: bool = False):
        schema = {row["name"]: (row["type"], row["sql"]) for row in db.execute("SELECT * FROM sqlite_master")}
        if (
            db.execute("PRAGMA application_id").fetchone()[0] != self._APPLICATION_ID
            or db.execute("PRAGMA user_version").fetchone()[0] != self._VERSION
            or schema != self._SCHEMA
        ):
            raise RuntimeError("stop loss schema mismatch")
        rows = db.execute("SELECT * FROM stop_meta").fetchall()
        if len(rows) != 1 or rows[0]["singleton"] != 1:
            raise RuntimeError("stop loss metadata mismatch")
        meta = rows[0]
        try:
            self._validate_logical_id(meta["logical_id"])
            policy = self._parse_policy(meta["policy"])
            self._valid_time(meta["last_wallclock"])
            self._window_start(meta["last_wallclock"], policy.window_seconds)
            if type(meta["last_event"]) is not int or meta["last_event"] < 0:
                raise ValueError("invalid event anchor")
        except (ValueError, TypeError, OverflowError) as error:
            raise RuntimeError("invalid persisted stop loss metadata") from error
        if not discover and meta["logical_id"] != self.logical_id:
            raise RuntimeError("stop loss logical_id mismatch")
        # An append-only event sequence anchors permanent dedup and resets. Validate
        # payloads too: SQLite affinity alone does not enforce types or finiteness.
        last_id, last_evidence, last_at = 0, 0, 0.0
        held = set()
        try:
            for row in db.execute("SELECT * FROM stop_events ORDER BY id"):
                kind, account = row["kind"], row["account_hash"]
                if type(row["id"]) is not int or row["id"] != last_id + 1 or kind not in self._KINDS:
                    raise ValueError("invalid event sequence")
                at = self._valid_time(row["at"])
                if not last_at <= at <= meta["last_wallclock"]:
                    raise ValueError("invalid event time")
                snapshot = self._parse_policy(row["policy"])
                if type(row["evidence_seq"]) is not int:
                    raise ValueError("invalid evidence sequence")
                counts = (row["failures"], row["targets"], row["accounts"])
                if any(type(value) is not int or value < 0 for value in counts):
                    raise ValueError("invalid event counters")
                if kind == "evidence":
                    if (
                        not all(
                            self._is_hash(row[key]) for key in ("account_hash", "target_hash", "intent_hash")
                        )
                        or row["code"] not in self._CODES
                        or row["reason_hash"] != ""
                        or counts != (0, 0, 0)
                        or row["evidence_seq"] != row["id"]
                    ):
                        raise ValueError("invalid evidence")
                    last_evidence = row["id"]
                else:
                    if (
                        (not self._is_hash(account) if kind.startswith("account_") else account != "")
                        or any(row[key] != "" for key in ("target_hash", "intent_hash", "code"))
                        or row["evidence_seq"] != last_evidence
                    ):
                        raise ValueError("invalid scope event")
                    if kind.endswith("_reset"):
                        if not self._is_hash(row["reason_hash"]) or counts != (0, 0, 0):
                            raise ValueError("invalid reset")
                        held.discard(account)
                    else:
                        if row["reason_hash"] != "" or account in held or not last_evidence:
                            raise ValueError("invalid hold")
                        if (
                            not self._threshold(snapshot, counts, account=bool(account))
                            or not 1 <= counts[1] <= counts[0] <= last_evidence
                            or not 1 <= counts[2] <= counts[0]
                            or (account and counts[2] != 1)
                        ):
                            raise ValueError("invalid hold counters")
                        held.add(account)
                last_id, last_at = row["id"], at
            if last_id != meta["last_event"]:
                raise ValueError("event anchor mismatch")
        except (ValueError, TypeError, OverflowError) as error:
            raise RuntimeError("invalid persisted stop loss history") from error
        return meta, policy

    @staticmethod
    def _digest(value: str, name: str) -> str:
        if not isinstance(value, str) or not value.strip() or len(value) > 4096:
            raise ValueError(f"{name} must be a nonempty string of at most 4096 characters")
        try:
            return hashlib.sha256(value.encode("utf-8")).hexdigest()
        except UnicodeError as error:
            raise ValueError(f"{name} must be valid UTF-8") from error

    @staticmethod
    def _account_hash(account: str | int) -> str:
        if type(account) not in (str, int) or not re.fullmatch(r"[1-9][0-9]{0,127}", str(account)):
            raise ValueError("account must be a positive decimal QQ id")
        return hashlib.sha256(str(account).encode("utf-8")).hexdigest()

    def _target_hash(self, target: str) -> str:
        if not isinstance(target, str):
            raise ValueError("target must be a canonical Fleet target")
        match = re.fullmatch(r"obf:([A-Za-z0-9_-]{1,128}):(private|group):([1-9][0-9]{0,127})", target)
        if not match or match[1] != self.logical_id:
            raise ValueError("target must be canonical and belong to this logical_id")
        return hashlib.sha256(target.encode("utf-8")).hexdigest()

    @property
    def policy(self) -> StopLossPolicy:
        with self._transaction() as db:
            return self._validate(db)[1]

    @staticmethod
    def _scope(db: sqlite3.Connection, account_hash: str) -> tuple[bool, int]:
        rows = db.execute(
            "SELECT kind,evidence_seq FROM stop_events WHERE account_hash=? AND kind != 'evidence' ORDER BY id DESC",
            (account_hash,),
        ).fetchall()
        held = bool(rows and rows[0]["kind"].endswith("_hold"))
        watermark = next((row["evidence_seq"] for row in rows if row["kind"].endswith("_reset")), 0)
        return held, watermark

    @staticmethod
    def _counts(
        db: sqlite3.Connection, now: float, policy: StopLossPolicy, account_hash: str
    ) -> tuple[int, int, int]:
        _, watermark = StopLoss._scope(db, account_hash)
        where = "kind='evidence' AND at>? AND id>?"
        args = [StopLoss._window_start(now, policy.window_seconds), watermark]
        if account_hash:
            where += " AND account_hash=?"
            args.append(account_hash)
        return tuple(
            db.execute(
                f"SELECT count(*),count(DISTINCT target_hash),count(DISTINCT account_hash) FROM stop_events WHERE {where}",
                args,
            ).fetchone()
        )

    @staticmethod
    def _threshold(policy: StopLossPolicy, counts: tuple[int, int, int], *, account: bool) -> bool:
        failures, targets, accounts = counts
        if account:
            return failures >= policy.account_failures and targets >= policy.account_targets
        return (
            failures >= policy.fleet_failures
            and targets >= policy.fleet_targets
            and accounts >= policy.fleet_accounts
        )

    def _append(
        self,
        db,
        now,
        policy,
        kind,
        account_hash="",
        *,
        target_hash="",
        intent_hash="",
        code="",
        reason_hash="",
        counts=(0, 0, 0),
    ) -> int:
        seq = db.execute("SELECT last_event+1 FROM stop_meta WHERE singleton=1").fetchone()[0]
        evidence_seq = (
            seq
            if kind == "evidence"
            else db.execute("SELECT coalesce(max(id),0) FROM stop_events WHERE kind='evidence'").fetchone()[0]
        )
        db.execute(
            "INSERT INTO stop_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                seq,
                kind,
                account_hash,
                target_hash,
                intent_hash,
                code,
                now,
                evidence_seq,
                reason_hash,
                *counts,
                self._serialize(policy),
            ),
        )
        db.execute("UPDATE stop_meta SET last_event=? WHERE singleton=1", (seq,))
        return seq

    def _latch(self, db: sqlite3.Connection, now: float, policy: StopLossPolicy) -> None:
        scopes = [
            row[0]
            for row in db.execute("SELECT DISTINCT account_hash FROM stop_events WHERE kind='evidence'")
        ]
        for account_hash in [*scopes, ""]:
            if self._scope(db, account_hash)[0]:
                continue
            counts = self._counts(db, now, policy, account_hash)
            if self._threshold(policy, counts, account=bool(account_hash)):
                self._append(
                    db,
                    now,
                    policy,
                    "account_hold" if account_hash else "fleet_hold",
                    account_hash,
                    counts=counts,
                )

    def record(self, account: str | int, target: str, code: str, *, intent: str) -> dict:
        """Charge dispatched failure evidence once per immutable send intent.

        Only target_rejected/outcome_unknown are evidence. The caller must know
        that a send was dispatched; preflight failures must never reach here.
        Conflicting reuse of an intent raises and never changes its first charge.
        """
        account_hash, target_hash = self._account_hash(account), self._target_hash(target)
        if not isinstance(code, str) or code not in self._CODES:
            raise ValueError("code must be target_rejected or outcome_unknown from a dispatched send")
        intent_hash = self._digest(intent, "intent")
        with self._transaction(write=True) as db:
            meta, policy = self._validate(db)
            prior = db.execute(
                "SELECT * FROM stop_events WHERE kind='evidence' AND intent_hash=?",
                (intent_hash,),
            ).fetchone()
            if prior and (prior["account_hash"], prior["target_hash"], prior["code"]) != (
                account_hash,
                target_hash,
                code,
            ):
                raise ValueError("intent conflicts with permanently recorded evidence")
            if not prior:
                now = max(self._now(), meta["last_wallclock"])
                self._window_start(now, policy.window_seconds)
                db.execute("UPDATE stop_meta SET last_wallclock=? WHERE singleton=1", (now,))
                seq = self._append(
                    db,
                    now,
                    policy,
                    "evidence",
                    account_hash,
                    target_hash=target_hash,
                    intent_hash=intent_hash,
                    code=code,
                )
                self._latch(db, now, policy)
            else:
                seq = prior["id"]
            account_held = self._scope(db, account_hash)[0]
            fleet_held = self._scope(db, "")[0]
            return {
                "recorded": not bool(prior),
                "duplicate": bool(prior),
                "evidence_seq": seq,
                "account_held": account_held,
                "fleet_held": fleet_held,
                "eligible": not (account_held or fleet_held),
            }

    def is_eligible(self, account: str | int) -> bool:
        """Scope veto only, NOT permission to switch accounts or dispatch a send."""
        account_hash = self._account_hash(account)
        with self._transaction() as db:
            self._validate(db)
            return not (self._scope(db, "")[0] or self._scope(db, account_hash)[0])

    def status(self) -> dict:
        """Read-only metadata/hashes; business_held suggests a conservative global stop."""
        with self._transaction() as db:
            meta, policy = self._validate(db)
            now = max(self._now(), meta["last_wallclock"])
            scopes = [
                row[0]
                for row in db.execute(
                    "SELECT DISTINCT account_hash FROM stop_events WHERE account_hash!='' ORDER BY account_hash"
                )
            ]
            held_accounts = [account for account in scopes if self._scope(db, account)[0]]
            fleet_held, watermark = self._scope(db, "")
            counts = self._counts(db, now, policy, "")
            total = db.execute("SELECT count(*) FROM stop_events WHERE kind='evidence'").fetchone()[0]
            return {
                "logical_id": self.logical_id,
                "policy": asdict(policy),
                "now": now,
                "last_wallclock": float(meta["last_wallclock"]),
                "failures_total": total,
                "fleet_held": fleet_held,
                "held_accounts": held_accounts,
                "business_held": fleet_held or bool(held_accounts),
                "fleet_watermark": watermark,
                "fleet_window": dict(zip(("failures", "targets", "accounts"), counts, strict=True)),
                "account_windows": {
                    account: {
                        "held": self._scope(db, account)[0],
                        "watermark": self._scope(db, account)[1],
                        "failures": (values := self._counts(db, now, policy, account))[0],
                        "targets": values[1],
                    }
                    for account in scopes
                },
            }

    def audit(self, limit: int = 100) -> list[dict]:
        """Newest-first permanent evidence, hold incidents and reason-hash resets."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("audit limit must be an integer in 1..1000")
        with self._transaction() as db:
            self._validate(db)
            return [
                dict(row) | {"policy": asdict(self._parse_policy(row["policy"]))}
                for row in db.execute("SELECT * FROM stop_events ORDER BY id DESC LIMIT ?", (limit,))
            ]

    def reset(self, account: str | int | None = None, *, reason: str) -> dict:
        """Explicitly reset ONLY the named account, or ONLY fleet when None.

        A reset records a global evidence event-sequence watermark for this scope
        only. Old evidence remains permanently charged and deduplicated, but is
        excluded from that scope's future thresholds, including policy tightening.
        """
        account_hash = "" if account is None else self._account_hash(account)
        reason_hash = self._digest(reason, "reason")
        with self._transaction(write=True) as db:
            meta, policy = self._validate(db)
            now = max(self._now(), meta["last_wallclock"])
            self._window_start(now, policy.window_seconds)
            db.execute("UPDATE stop_meta SET last_wallclock=? WHERE singleton=1", (now,))
            was_held = self._scope(db, account_hash)[0]
            self._append(
                db,
                now,
                policy,
                "account_reset" if account_hash else "fleet_reset",
                account_hash,
                reason_hash=reason_hash,
            )
            return {
                "scope": "account" if account_hash else "fleet",
                "account_hash": account_hash,
                "was_held": was_held,
                "held": False,
                "watermark": self._scope(db, account_hash)[1],
                "fleet_held": self._scope(db, "")[0],
            }
