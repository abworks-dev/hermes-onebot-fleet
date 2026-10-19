"""Independent local JSONL alert export; never a QQ sender or webhook.

The health journal is opened existing-only. Export is at-least-once: a crash
between the fsynced append and atomic cursor replacement can repeat records.
Only alert ID/state transitions are exported; occurrence increments coalesce.
The bounded journal query is not a complete audit, and a full 1000-row window
explicitly raises a local journal_window_full condition.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

from .health import HealthState

_MAX_TIME = 253402300799
_MAX_LINE = 16384
_HEALTH_CODES = frozenset(
    {
        "account_anomaly",
        "identity_mismatch",
        "target_restricted",
        "result_unknown",
        "target_unavailable",
        "network_down",
        "account_offline",
        "backend_degraded",
        "pool_unavailable",
        "multiple_account_anomalies",
    }
)
_OBSERVER_CODES = frozenset({"observer_stale", "observer_unavailable", "journal_window_full"})
_ALERT_FIELDS = frozenset(
    {
        "alert_id",
        "code",
        "scope",
        "account_id",
        "target",
        "state",
        "first_seen",
        "last_seen",
        "occurrences",
        "resolved_at",
    }
)
_COMMON = frozenset({"version", "kind", "exported_at"})
_CURSOR_FIELDS = frozenset(
    {
        "version",
        "source_hash",
        "output_hash",
        "logical_id",
        "last_poll_at",
        "observer_state",
        "journal_window_full",
        "alerts",
    }
)


def _timestamp(value) -> float:
    if type(value) not in (int, float) or not 0 <= value <= _MAX_TIME or not math.isfinite(value):
        raise ValueError("invalid alert timestamp")
    return float(value)


def _identifier(value) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value) is None:
        raise ValueError("invalid alert identifier")
    return value


def _reject_constant(value):
    raise ValueError("non-finite JSON value")


def _json(text):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    return json.loads(text, parse_constant=_reject_constant, object_pairs_hook=unique)


class AlertWatcher:
    """Observe a pre-existing health sidecar and append sanitized local records.

    ``poll`` returns ``written`` and ``observer_state`` (healthy/stale/unavailable).
    Cursor state is adjacent at ``str(output_path) + '.cursor.json'`` and contains
    at most 1000 alert ID/state fingerprints, never message or exception text.
    Use a single watcher per output; this class does not schedule polling.
    """

    def __init__(
        self,
        health_path: str | Path,
        output_path: str | Path,
        *,
        clock: Callable[[], float] | None = None,
        stale_after: float = 30,
    ) -> None:
        for path in (health_path, output_path):
            if not isinstance(path, (str, Path)) or not str(path).strip():
                raise ValueError("health and output paths must be nonempty")
        if (
            type(stale_after) not in (int, float)
            or not 0 < stale_after <= 86400
            or not math.isfinite(stale_after)
        ):
            raise ValueError("stale_after must be finite and in (0,86400] seconds")
        if clock is not None and not callable(clock):
            raise ValueError("clock must be callable")
        self.health_path = Path(health_path).resolve()
        # Reject aliases rather than following an output/cursor symlink into a DB.
        self.output_path = Path(output_path).absolute()
        self.cursor_path = Path(str(self.output_path) + ".cursor.json")
        self._clock = time.time if clock is None else clock
        self.stale_after = float(stale_after)
        self._check_paths()
        self.output_path = self.output_path.resolve()
        self.cursor_path = self.cursor_path.resolve()
        self._source_hash = self._path_hash(self.health_path)
        self._output_hash = self._path_hash(self.output_path)
        self._load_cursor()
        self._validate_output()

    @staticmethod
    def _path_hash(path: Path) -> str:
        return hashlib.sha256(os.path.normcase(str(path)).encode("utf-8")).hexdigest()

    def _check_paths(self) -> None:
        for path in (self.output_path, self.cursor_path):
            if path.is_symlink() or path.resolve() == self.health_path:
                raise ValueError("alert output/cursor cannot alias a health or database path")
            if re.search(r"\.(?:db|sqlite|sqlite3)(?:$|[-.])", path.name, re.IGNORECASE):
                raise ValueError("alert output/cursor cannot use a database filename")
            if path.exists() and not path.is_file():
                raise ValueError("alert output/cursor must be regular files")
            if path.exists() and self.health_path.is_file() and os.path.samefile(path, self.health_path):
                raise ValueError("alert output/cursor cannot alias the health database")
        if self.output_path.resolve() == self.cursor_path.resolve():
            raise ValueError("alert output/cursor paths must differ")

    def _new_cursor(self) -> dict:
        return {
            "version": 1,
            "source_hash": self._source_hash,
            "output_hash": self._output_hash,
            "logical_id": None,
            "last_poll_at": 0.0,
            "observer_state": "healthy",
            "journal_window_full": False,
            "alerts": {},
        }

    def _load_cursor(self) -> dict:
        if not self.cursor_path.exists():
            return self._new_cursor()
        if not self.output_path.is_file():
            raise ValueError("alert cursor exists without its output")
        if self.cursor_path.stat().st_size > 262144:
            raise ValueError("alert cursor is too large")
        try:
            cursor = _json(self.cursor_path.read_text(encoding="utf-8"))
            if not isinstance(cursor, dict) or set(cursor) != _CURSOR_FIELDS:
                raise ValueError("invalid alert cursor fields")
            if (
                type(cursor["version"]) is not int
                or cursor["version"] != 1
                or cursor["source_hash"] != self._source_hash
                or cursor["output_hash"] != self._output_hash
                or cursor["observer_state"] not in {"healthy", "stale", "unavailable"}
                or type(cursor["journal_window_full"]) is not bool
            ):
                raise ValueError("invalid alert cursor metadata")
            if cursor["logical_id"] is not None:
                _identifier(cursor["logical_id"])
            _timestamp(cursor["last_poll_at"])
            alerts = cursor["alerts"]
            if not isinstance(alerts, dict) or len(alerts) > 1000:
                raise ValueError("invalid alert cursor fingerprints")
            for alert_id, state in alerts.items():
                if (
                    re.fullmatch(r"[1-9][0-9]{0,18}", alert_id) is None
                    or int(alert_id) > 9223372036854775807
                    or state not in {"active", "resolved"}
                ):
                    raise ValueError("invalid alert cursor fingerprint")
            return cursor
        except (ValueError, TypeError, UnicodeError, OSError) as error:
            raise ValueError("invalid alert cursor; refusing export") from error

    @staticmethod
    def _validate_record(record: dict) -> None:
        if not isinstance(record, dict) or not _COMMON <= set(record):
            raise ValueError("invalid alert output record")
        if type(record["version"]) is not int or record["version"] != 1:
            raise ValueError("invalid alert output version")
        _timestamp(record["exported_at"])
        if record["kind"] == "observer":
            if (
                set(record) != _COMMON | {"code", "state"}
                or record["code"] not in _OBSERVER_CODES
                or record["state"] not in {"active", "resolved"}
            ):
                raise ValueError("invalid observer output record")
            return
        if record["kind"] != "health_alert" or set(record) != _COMMON | _ALERT_FIELDS | {"logical_id"}:
            raise ValueError("invalid health alert output fields")
        logical_id = _identifier(record["logical_id"])
        if (
            type(record["alert_id"]) is not int
            or not 1 <= record["alert_id"] <= 9223372036854775807
            or record["code"] not in _HEALTH_CODES
            or record["scope"] not in {"account", "target", "fleet"}
            or record["state"] not in {"active", "resolved"}
            or type(record["occurrences"]) is not int
            or record["occurrences"] < 1
        ):
            raise ValueError("invalid health alert metadata")
        if record["account_id"] is not None:
            _identifier(record["account_id"])
        target = record["target"]
        if target is not None and (
            not isinstance(target, str)
            or re.fullmatch(rf"obf:{re.escape(logical_id)}:(private|group):[1-9][0-9]{{0,31}}", target)
            is None
        ):
            raise ValueError("invalid health alert target")
        if (
            (record["scope"] == "fleet" and (record["account_id"] is not None or target is not None))
            or (record["scope"] == "account" and (record["account_id"] is None or target is not None))
            or (record["scope"] == "target" and (record["account_id"] is None or target is None))
        ):
            raise ValueError("invalid health alert scope")
        first, last = _timestamp(record["first_seen"]), _timestamp(record["last_seen"])
        if last < first:
            raise ValueError("invalid health alert times")
        if record["state"] == "active":
            if record["resolved_at"] is not None:
                raise ValueError("invalid active alert resolution")
        elif record["resolved_at"] is None or _timestamp(record["resolved_at"]) < last:
            raise ValueError("invalid resolved alert time")

    def _validate_output(self) -> None:
        if not self.output_path.exists():
            return
        try:
            with self.output_path.open("rb") as stream:
                while line := stream.readline(_MAX_LINE + 1):
                    if len(line) > _MAX_LINE or not line.endswith(b"\n"):
                        raise ValueError("oversized or incomplete JSONL record")
                    self._validate_record(_json(line.decode("utf-8")))
        except (ValueError, TypeError, UnicodeError) as error:
            raise ValueError("existing alert output is not valid watcher JSONL; refusing append") from error

    @staticmethod
    def _observer(code, state, now):
        return {"version": 1, "kind": "observer", "exported_at": now, "code": code, "state": state}

    def _snapshot(self, cursor: dict, now: float):
        try:
            health = HealthState.open_existing(self.health_path)
            snapshot = health.status()
            logical_id = _identifier(snapshot["logical_id"])
            if cursor["logical_id"] is not None and logical_id != cursor["logical_id"]:
                raise ValueError("health namespace changed")
            alerts = health.alerts(limit=1000)
            if not isinstance(alerts, list) or len(alerts) > 1000:
                raise ValueError("invalid health journal window")
            records = []
            for row in alerts:
                if not isinstance(row, dict) or not _ALERT_FIELDS <= set(row):
                    raise ValueError("invalid health alert fields")
                record = {field: row[field] for field in _ALERT_FIELDS}
                record.update(version=1, kind="health_alert", exported_at=now, logical_id=logical_id)
                self._validate_record(record)
                records.append(record)
            updated = [_timestamp(account["updated_at"]) for account in snapshot["accounts"]]
            state = "stale" if not updated or now - max(updated) > self.stale_after else "healthy"
            return logical_id, state, records, len(alerts) == 1000
        except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, RuntimeError):
            # Never serialize exception strings, SQL, paths, credentials, or inputs.
            return cursor["logical_id"], "unavailable", [], cursor["journal_window_full"]

    @staticmethod
    def _sync_directory(path: Path) -> None:
        if os.name == "nt":
            return  # Windows does not expose portable directory fsync.
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _save_cursor(self, cursor: dict) -> None:
        descriptor, filename = tempfile.mkstemp(
            dir=self.cursor_path.parent, prefix=f".{self.cursor_path.name}.", suffix=".tmp"
        )
        temporary = Path(filename).resolve()
        # Verify the exact replace/cleanup target remains our adjacent temp file.
        if temporary.parent != self.cursor_path.parent or not temporary.name.startswith(
            f".{self.cursor_path.name}."
        ):
            os.close(descriptor)
            raise ValueError("invalid cursor temporary path")
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
                stream.write(json.dumps(cursor, allow_nan=False, sort_keys=True) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            self._check_paths()
            os.replace(temporary, self.cursor_path)
            self._sync_directory(self.cursor_path.parent)
        finally:
            if temporary.exists():
                temporary.unlink()

    def poll(self) -> dict:
        self._check_paths()
        cursor = self._load_cursor()
        self._validate_output()
        now = max(_timestamp(self._clock()), cursor["last_poll_at"])
        logical_id, observer_state, alerts, window_full = self._snapshot(cursor, now)
        records = []
        previous = cursor["observer_state"]
        if previous != observer_state:
            if previous != "healthy":
                records.append(self._observer("observer_" + previous, "resolved", now))
            if observer_state != "healthy":
                records.append(self._observer("observer_" + observer_state, "active", now))
        if window_full != cursor["journal_window_full"]:
            records.append(
                self._observer("journal_window_full", "active" if window_full else "resolved", now)
            )
        fingerprints = cursor["alerts"]
        if observer_state != "unavailable":
            # Oldest first within the bounded snapshot, preserving incident order.
            for record in reversed(alerts):
                if fingerprints.get(str(record["alert_id"])) != record["state"]:
                    records.append(record)
            fingerprints = {str(record["alert_id"]): record["state"] for record in alerts}
        next_cursor = {
            **cursor,
            "logical_id": logical_id,
            "last_poll_at": now,
            "observer_state": observer_state,
            "journal_window_full": window_full,
            "alerts": fingerprints,
        }
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        # Append and fsync BEFORE advancing the cursor. Crash replay is allowed;
        # a failed output write must never acknowledge unseen transitions.
        with self.output_path.open("ab") as stream:
            for record in records:
                self._validate_record(record)
                stream.write((json.dumps(record, allow_nan=False, sort_keys=True) + "\n").encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        self._sync_directory(self.output_path.parent)
        self._save_cursor(next_cursor)
        return {"written": len(records), "observer_state": observer_state}
