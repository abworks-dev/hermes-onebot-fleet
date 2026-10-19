"""Local, durable diagnostics and latched circuit vetoes; never admission.

Use a dedicated sidecar, not the message/admission database. ``is_eligible``
means only "no matching open circuit". Even an unseen account passes this veto;
callers MUST separately enforce P0 admission, connected/verified live readiness,
and primary policy. Nothing here logs in, probes, retries, or contacts QQ.

Targets are canonical ``obf:<logical_id>:private|group:<positive decimal id>``.
Unknown/unverified transport observations cannot establish account health.
Network loss is not evidence of an account freeze. All circuits stay latched
until explicit reset of their exact scope: target=None resets account circuits
ONLY, not that account's target circuits. Acknowledgments never reset circuits.

Only fixed codes, bounded identifiers, booleans and timestamps are persisted.
Operator reasons are validated then SHA-256 hashed, never stored as text. IDs
must be identifiers, not messages or credentials; no API can detect a secret
that a caller deliberately disguises as an otherwise valid identifier.
"""

from __future__ import annotations

import hashlib
import math
import re
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

REPORT_KINDS = frozenset(
    {
        "account_anomaly",
        "target_rejected",
        "outcome_unknown",
        "target_unavailable",
        "send_ack",
        "pool_unavailable",
        "pool_available",
    }
)
DIAGNOSTIC_CODES = frozenset(
    {
        "network_down",
        "account_offline",
        "identity_mismatch",
        "backend_degraded",
    }
)
_CIRCUIT_CODES = frozenset(
    {
        "account_anomaly",
        "identity_mismatch",
        "target_restricted",
        "result_unknown",
        "target_unavailable",
    }
)
_ALERT_CODES = _CIRCUIT_CODES | DIAGNOSTIC_CODES | {"pool_unavailable", "multiple_account_anomalies"}
_EVENT_CODES = REPORT_KINDS | {"observation", "reset"}
_TARGET_REPORTS = frozenset({"target_rejected", "outcome_unknown", "target_unavailable"})
_REPORT_CIRCUIT = {
    "account_anomaly": "account_anomaly",
    "target_rejected": "target_restricted",
    "outcome_unknown": "result_unknown",
    "target_unavailable": "target_unavailable",
}
_APPLICATION_ID = 0x48464C54  # HFLT; schema version is independent of other sidecars.
_SCHEMA_VERSION = 1
_MAX_TIMESTAMP = 253402300799


def _choices(values) -> str:
    return ",".join("'" + value + "'" for value in sorted(values))


_SCHEMA = {
    "health_namespace": """CREATE TABLE health_namespace (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        logical_id TEXT NOT NULL,
        last_timestamp REAL NOT NULL CHECK (last_timestamp >= 0 AND last_timestamp <= 253402300799))""",
    "health_accounts": """CREATE TABLE health_accounts (
        account_id TEXT PRIMARY KEY NOT NULL,
        connected INTEGER CHECK (connected IN (0,1)),
        identity TEXT NOT NULL CHECK (identity IN ('unknown','verified','mismatch')),
        online INTEGER CHECK (online IN (0,1)),
        good INTEGER CHECK (good IN (0,1)),
        updated_at REAL NOT NULL CHECK (updated_at >= 0 AND updated_at <= 253402300799))""",
    "health_circuits": f"""CREATE TABLE health_circuits (
        account_id TEXT NOT NULL,
        target TEXT NOT NULL,
        code TEXT NOT NULL CHECK (code IN ({_choices(_CIRCUIT_CODES)})),
        opened_at REAL NOT NULL CHECK (opened_at >= 0 AND opened_at <= 253402300799),
        PRIMARY KEY (account_id,target,code))""",
    "health_events": f"""CREATE TABLE health_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL NOT NULL CHECK (timestamp >= 0 AND timestamp <= 253402300799),
        code TEXT NOT NULL CHECK (code IN ({_choices(_EVENT_CODES)})),
        scope TEXT NOT NULL CHECK (scope IN ('account','target','fleet')),
        account_id TEXT NOT NULL,
        target TEXT,
        reason_id TEXT)""",
    "health_alerts": f"""CREATE TABLE health_alerts (
        alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL CHECK (code IN ({_choices(_ALERT_CODES)})),
        scope TEXT NOT NULL CHECK (scope IN ('account','target','fleet')),
        account_id TEXT NOT NULL,
        target TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('active','resolved')),
        first_seen REAL NOT NULL CHECK (first_seen >= 0 AND first_seen <= 253402300799),
        last_seen REAL NOT NULL CHECK (last_seen >= first_seen AND last_seen <= 253402300799),
        occurrences INTEGER NOT NULL CHECK (occurrences >= 1),
        resolved_at REAL CHECK (resolved_at >= last_seen AND resolved_at <= 253402300799))""",
    "health_active_alert": """CREATE UNIQUE INDEX health_active_alert
        ON health_alerts (code,scope,account_id,target) WHERE state = 'active'""",
}


class HealthState:
    """Transactional, namespace-bound SQLite diagnostics and circuit journal.

    ``events``/``alerts`` return newest first, with limits from 1 through 1000.
    Alerts coalesce repeated active evidence into one incident and occurrence
    count. Recovery resolves only nonlatched observation alerts; a recurrence
    creates a new incident. Fleet anomalies count distinct latched accounts.
    """

    def __init__(self, path: str | Path, logical_id: str, *, clock: Callable[[], float] | None = None):
        self.path = Path(path)
        self.logical_id = self._identifier(logical_id)
        if clock is not None and not callable(clock):
            raise ValueError("clock must be callable")
        self._clock = time.time if clock is None else clock
        existed = self.path.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db(write=True, create=not existed, verify=False) as db:
            objects = db.execute("SELECT name FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").fetchall()
            if not existed and not objects:
                if db.execute("PRAGMA application_id").fetchone()[0] != 0:
                    raise ValueError("invalid health schema")
                for sql in _SCHEMA.values():
                    db.execute(sql)
                db.execute(f"PRAGMA application_id={_APPLICATION_ID}")
                db.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")
                db.execute("INSERT INTO health_namespace VALUES (1,?,0)", (self.logical_id,))
            self._verify(db)

    @classmethod
    def open_existing(cls, path: str | Path) -> HealthState:
        """Open an existing journal without creating or reconciling any state."""
        instance = cls.__new__(cls)
        instance.path = Path(path)
        instance._clock = time.time
        with instance._db(verify=False) as db:
            instance._verify_schema(db)
            row = db.execute("SELECT logical_id FROM health_namespace WHERE singleton=1").fetchone()
            if row is None:
                raise ValueError("missing health namespace")
            instance.logical_id = instance._identifier(row["logical_id"])
            instance._verify(db)
        return instance

    @staticmethod
    def _identifier(value: str) -> str:
        if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value) is None:
            raise ValueError("invalid health identifier")
        return value

    def _target(self, target: str | None) -> str | None:
        if target is None:
            return None
        if not isinstance(target, str) or len(target) > 256:
            raise ValueError("invalid health target")
        match = re.fullmatch(r"obf:([A-Za-z0-9_-]{1,128}):(private|group):([1-9][0-9]{0,31})", target)
        if match is None or match[1] != self.logical_id:
            raise ValueError("invalid health target namespace")
        return target

    @staticmethod
    def _reason_id(reason: str) -> str:
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 200 or not reason.isprintable():
            raise ValueError("reason must contain 1-200 printable characters")
        return hashlib.sha256(reason.encode("utf-8")).hexdigest()

    @staticmethod
    def _limit(limit: int) -> int:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 through 1000")
        return limit

    @contextmanager
    def _db(
        self, *, write: bool = False, create: bool = False, verify: bool = True
    ) -> Iterator[sqlite3.Connection]:
        uri = self.path.resolve().as_uri() + ("?mode=rwc" if create else "?mode=rw")
        db = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA trusted_schema=OFF")
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            if verify:
                self._verify(db)
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _verify_schema(db: sqlite3.Connection) -> None:
        if (
            db.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID
            or db.execute("PRAGMA user_version").fetchone()[0] != _SCHEMA_VERSION
        ):
            raise ValueError("invalid health schema version")
        actual = {
            row["name"]: row["sql"]
            for row in db.execute("SELECT name,sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'")
        }
        if actual != _SCHEMA:
            raise ValueError("invalid health schema")

    def _verify(self, db: sqlite3.Connection) -> None:
        self._verify_schema(db)
        rows = db.execute("SELECT * FROM health_namespace").fetchall()
        if len(rows) != 1 or rows[0]["singleton"] != 1 or rows[0]["logical_id"] != self.logical_id:
            raise ValueError("health namespace mismatch")
        self._timestamp(rows[0]["last_timestamp"])

    @staticmethod
    def _timestamp(value) -> float:
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= _MAX_TIMESTAMP:
            raise ValueError("invalid health timestamp")
        return float(value)

    def _now(self, db: sqlite3.Connection) -> float:
        # Clamp against a DURABLE watermark, including after restart/clock rollback.
        value = self._timestamp(self._clock())
        previous = db.execute("SELECT last_timestamp FROM health_namespace WHERE singleton=1").fetchone()[0]
        value = max(value, self._timestamp(previous))
        db.execute("UPDATE health_namespace SET last_timestamp=? WHERE singleton=1", (value,))
        return value

    @staticmethod
    def _ensure_account(db: sqlite3.Connection, account_id: str, now: float) -> None:
        db.execute(
            "INSERT OR IGNORE INTO health_accounts VALUES (?,NULL,'unknown',NULL,NULL,?)", (account_id, now)
        )

    @staticmethod
    def _event(db, now, code, account_id, target=None, reason_id=None, *, scope=None):
        db.execute(
            "INSERT INTO health_events (timestamp,code,scope,account_id,target,reason_id) VALUES (?,?,?,?,?,?)",
            (
                now,
                code,
                scope or ("target" if target is not None else "account"),
                account_id,
                target,
                reason_id,
            ),
        )

    @staticmethod
    def _alert(db, now, code, account_id="", target="", *, active=True, scope=None):
        scope = scope or ("fleet" if not account_id else "target" if target else "account")
        row = db.execute(
            "SELECT alert_id FROM health_alerts WHERE code=? AND scope=? AND account_id=? "
            "AND target=? AND state='active'",
            (code, scope, account_id, target),
        ).fetchone()
        if not active:
            if row is not None:
                db.execute(
                    "UPDATE health_alerts SET state='resolved',resolved_at=? WHERE alert_id=?", (now, row[0])
                )
        elif row is not None:
            db.execute(
                "UPDATE health_alerts SET last_seen=?,occurrences=occurrences+1 WHERE alert_id=?",
                (now, row[0]),
            )
        else:
            db.execute(
                "INSERT INTO health_alerts (code,scope,account_id,target,state,first_seen,last_seen,occurrences) "
                "VALUES (?,?,?,?,'active',?,?,1)",
                (code, scope, account_id, target, now, now),
            )

    def _circuit(self, db, now, account_id, target, code):
        db.execute(
            "INSERT OR IGNORE INTO health_circuits VALUES (?,?,?,?)", (account_id, target or "", code, now)
        )
        self._alert(db, now, code, account_id, target or "")

    def _fleet_alert(self, db, now):
        count = db.execute(
            "SELECT count(DISTINCT account_id) FROM health_circuits WHERE target=''"
        ).fetchone()[0]
        # Only count a new fleet transition, not unrelated observations/reports.
        existing = db.execute(
            "SELECT 1 FROM health_alerts WHERE code='multiple_account_anomalies' AND state='active'"
        ).fetchone()
        if (count >= 2) != bool(existing):
            self._alert(db, now, "multiple_account_anomalies", active=count >= 2)

    @staticmethod
    def _diagnostics(row) -> list[str]:
        result = []
        if row["connected"] == 0:
            result.append("network_down")
        if row["identity"] == "mismatch":
            result.append("identity_mismatch")
        if row["connected"] == 1 and row["identity"] == "verified":
            if row["online"] == 0:
                result.append("account_offline")
            if row["good"] == 0:
                result.append("backend_degraded")
        return sorted(result)

    @staticmethod
    def _enum(value, choices):
        if not isinstance(value, str) or value not in choices:
            raise ValueError("invalid stored health enum")
        return value

    @staticmethod
    def _flag(value):
        if value is not None and (type(value) is not int or value not in (0, 1)):
            raise ValueError("invalid stored health flag")
        return None if value is None else bool(value)

    def _account_snapshot(self, db, account_id):
        row = db.execute("SELECT * FROM health_accounts WHERE account_id=?", (account_id,)).fetchone()
        self._enum(row["identity"], {"unknown", "verified", "mismatch"})
        for field in ("connected", "online", "good"):
            self._flag(row[field])
        if (row["connected"] != 1 or row["identity"] != "verified") and any(
            row[field] is not None for field in ("online", "good")
        ):
            raise ValueError("invalid stored health evidence")
        return {
            "account_id": self._identifier(row["account_id"]),
            "connected": None if row["connected"] is None else bool(row["connected"]),
            "identity": row["identity"],
            "online": None if row["online"] is None else bool(row["online"]),
            "good": None if row["good"] is None else bool(row["good"]),
            "diagnostics": self._diagnostics(row),
            "updated_at": self._timestamp(row["updated_at"]),
        }

    def observe(
        self,
        account_id: str,
        *,
        connected: bool,
        identity: str = "unknown",
        online: bool | None = None,
        good: bool | None = None,
    ) -> dict:
        """Record passive evidence, trusting online/good only if connected+verified.

        Transport loss, offline and backend degradation are diagnostics, not
        account anomalies. Only explicit identity mismatch latches a circuit.
        Missing evidence replaces old live observations with unknown, not success.
        """
        self._identifier(account_id)
        if (
            type(connected) is not bool
            or not isinstance(identity, str)
            or identity not in {"unknown", "verified", "mismatch"}
        ):
            raise ValueError("invalid health observation")
        if any(value is not None and type(value) is not bool for value in (online, good)):
            raise ValueError("online/good must be bool or None")
        if not connected or identity != "verified":
            online = good = None
        with self._db(write=True) as db:
            now = self._now(db)
            self._ensure_account(db, account_id, now)
            before = self._account_snapshot(db, account_id)
            db.execute(
                "UPDATE health_accounts SET connected=?,identity=?,online=?,good=?,updated_at=? WHERE account_id=?",
                (connected, identity, online, good, now, account_id),
            )
            result = self._account_snapshot(db, account_id)
            if any(before[key] != result[key] for key in ("connected", "identity", "online", "good")):
                self._event(db, now, "observation", account_id)
            for code in sorted(DIAGNOSTIC_CODES - {"identity_mismatch"}):
                self._alert(db, now, code, account_id, active=code in result["diagnostics"])
            if identity == "mismatch":
                self._circuit(db, now, account_id, None, "identity_mismatch")
            self._fleet_alert(db, now)
            return result

    def report(self, account_id: str, kind: str, target: str | None = None) -> dict:
        """Record ONLY a fixed report code; no exception/message/status-code parsing."""
        self._identifier(account_id)
        if not isinstance(kind, str) or kind not in REPORT_KINDS:
            raise ValueError("invalid health report kind")
        target = self._target(target)
        if (kind in _TARGET_REPORTS and target is None) or (
            kind in {"account_anomaly", "pool_unavailable", "pool_available"} and target is not None
        ):
            raise ValueError("invalid health report target scope")
        with self._db(write=True) as db:
            now = self._now(db)
            self._ensure_account(db, account_id, now)
            self._event(
                db,
                now,
                kind,
                account_id,
                target,
                scope="fleet" if kind in {"pool_available", "pool_unavailable"} else None,
            )
            if kind in _REPORT_CIRCUIT:
                self._circuit(db, now, account_id, target, _REPORT_CIRCUIT[kind])
            if kind == "pool_unavailable":
                self._alert(db, now, "pool_unavailable")
            elif kind == "pool_available":
                self._alert(db, now, "pool_unavailable", active=False)
            self._fleet_alert(db, now)
            return self._account_snapshot(db, account_id)

    def is_eligible(self, account_id: str, target: str | None = None) -> bool:
        """Circuit veto only. True is NOT verified readiness or P0 admission."""
        self._identifier(account_id)
        target = self._target(target)
        with self._db() as db:
            return (
                db.execute(
                    "SELECT 1 FROM health_circuits WHERE account_id=? AND (target='' OR target=?) LIMIT 1",
                    (account_id, target or ""),
                ).fetchone()
                is None
            )

    def reset(self, account_id: str, target: str | None = None, *, reason: str) -> dict:
        """Explicit exact-scope circuit reset+audit; never health/admission/login.

        Account reset does not reset target circuits. Target reset cannot clear
        an account circuit. Observation diagnostics are not rewritten by reset.
        """
        self._identifier(account_id)
        target = self._target(target)
        reason_id = self._reason_id(reason)
        with self._db(write=True) as db:
            now = self._now(db)
            rows = db.execute(
                "SELECT code FROM health_circuits WHERE account_id=? AND target=?", (account_id, target or "")
            ).fetchall()
            for row in rows:
                self._alert(db, now, row["code"], account_id, target or "", active=False)
            db.execute(
                "DELETE FROM health_circuits WHERE account_id=? AND target=?", (account_id, target or "")
            )
            self._event(db, now, "reset", account_id, target, reason_id)
            self._fleet_alert(db, now)
            return {"account_id": account_id, "target": target, "cleared": len(rows)}

    def status(self) -> dict:
        """Return sanitized observations and open circuits, not admission policy."""
        with self._db() as db:
            accounts = [
                self._account_snapshot(db, row[0])
                for row in db.execute("SELECT account_id FROM health_accounts ORDER BY account_id").fetchall()
            ]
            circuits = [
                {
                    "account_id": self._identifier(row["account_id"]),
                    "target": self._target(row["target"] or None),
                    "code": self._enum(row["code"], _CIRCUIT_CODES),
                    "opened_at": self._timestamp(row["opened_at"]),
                }
                for row in db.execute("SELECT * FROM health_circuits ORDER BY account_id,target,code")
            ]
            return {"logical_id": self.logical_id, "accounts": accounts, "circuits": circuits}

    def events(self, limit: int = 100) -> list[dict]:
        """Newest-first bounded audit; reasons are one-way IDs, never input text."""
        limit = self._limit(limit)
        with self._db() as db:
            result = [
                dict(row)
                for row in db.execute("SELECT * FROM health_events ORDER BY event_id DESC LIMIT ?", (limit,))
            ]
            for row in result:
                self._identifier(row["account_id"])
                self._target(row["target"])
                self._enum(row["code"], _EVENT_CODES)
                self._enum(row["scope"], {"account", "target", "fleet"})
                self._timestamp(row["timestamp"])
                reason_id = row["reason_id"]
                if reason_id is not None and (
                    not isinstance(reason_id, str) or re.fullmatch(r"[0-9a-f]{64}", reason_id) is None
                ):
                    raise ValueError("invalid stored health reason identifier")
            return result

    def alerts(self, limit: int = 100) -> list[dict]:
        """Newest-first bounded local incidents; no network delivery/callbacks."""
        limit = self._limit(limit)
        with self._db() as db:
            result = [
                dict(row)
                for row in db.execute("SELECT * FROM health_alerts ORDER BY alert_id DESC LIMIT ?", (limit,))
            ]
            for row in result:
                row["account_id"] = self._identifier(row["account_id"]) if row["account_id"] else None
                row["target"] = self._target(row["target"] or None)
                self._enum(row["code"], _ALERT_CODES)
                self._enum(row["scope"], {"account", "target", "fleet"})
                self._enum(row["state"], {"active", "resolved"})
                if type(row["occurrences"]) is not int or row["occurrences"] < 1:
                    raise ValueError("invalid stored health occurrence count")
                self._timestamp(row["first_seen"])
                self._timestamp(row["last_seen"])
                if row["resolved_at"] is not None:
                    self._timestamp(row["resolved_at"])
            return result
