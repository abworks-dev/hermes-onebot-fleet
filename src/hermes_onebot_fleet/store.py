"""Durable at-most-once handoff and conservative outbound recovery.

This ledger is NOT a Hermes execution transaction. A crash between handoff and
acknowledgment becomes uncertain and requires operator reconciliation.
"""

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.db() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS inbound (
                    key TEXT PRIMARY KEY, payload TEXT NOT NULL, state TEXT NOT NULL,
                    created REAL NOT NULL, error TEXT);
                CREATE TABLE IF NOT EXISTS outbound (
                    key TEXT PRIMARY KEY, chat_id TEXT NOT NULL, content TEXT NOT NULL,
                    state TEXT NOT NULL, account TEXT, message_id TEXT, created REAL NOT NULL, error TEXT);
            """)

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def enqueue(self, key, payload):
        with self.db() as db:
            return (
                db.execute(
                    "INSERT OR IGNORE INTO inbound VALUES (?,?,?, ?,NULL)",
                    (key, json.dumps(payload, ensure_ascii=False), "queued", time.time()),
                ).rowcount
                == 1
            )

    def inbound(self, key):
        with self.db() as db:
            row = db.execute("SELECT * FROM inbound WHERE key=?", (key,)).fetchone()
            return dict(row) if row else None

    def claim(self):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM inbound WHERE state='queued' ORDER BY created,rowid LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            db.execute("UPDATE inbound SET state='handing_off' WHERE key=?", (row["key"],))
            return dict(row)

    def inbound_result(self, key, state, error=None):
        if state not in {"handed_off", "uncertain", "denied"}:
            raise ValueError("invalid inbound state")
        with self.db() as db:
            db.execute("UPDATE inbound SET state=?,error=? WHERE key=?", (state, error, key))

    def begin_send(self, key, chat_id, content, account):
        with self.db() as db:
            return (
                db.execute(
                    "INSERT OR IGNORE INTO outbound VALUES (?,?,?,?,?,?,?,NULL)",
                    (key, chat_id, content, "sending", account, None, time.time()),
                ).rowcount
                == 1
            )

    def send_result(self, key, state, message_id=None, error=None):
        if state not in {"sent", "failed", "uncertain"}:
            raise ValueError("invalid outbound state")
        with self.db() as db:
            db.execute(
                "UPDATE outbound SET state=?,message_id=?,error=? WHERE key=?",
                (state, str(message_id) if message_id is not None else None, error, key),
            )

    def outbound(self, key):
        with self.db() as db:
            row = db.execute("SELECT * FROM outbound WHERE key=?", (key,)).fetchone()
            return dict(row) if row else None

    def recover(self):
        with self.db() as db:
            db.execute(
                "UPDATE inbound SET state='uncertain',error='process stopped during handoff' WHERE state='handing_off'"
            )
            db.execute(
                "UPDATE outbound SET state='uncertain',error='process stopped during send' WHERE state='sending'"
            )

    def status(self):
        with self.db() as db:
            return {
                table: {
                    row["state"]: row["n"]
                    for row in db.execute(f"SELECT state,count(*) AS n FROM {table} GROUP BY state")
                }
                for table in ("inbound", "outbound")
            }

    def uncertain(self):
        with self.db() as db:
            # Do not return message contents, tokens, or participant identifiers.
            return [
                {"direction": table, "key": row["key"], "error": row["error"]}
                for table in ("inbound", "outbound")
                for row in db.execute(f"SELECT key,error FROM {table} WHERE state='uncertain'")
            ]
