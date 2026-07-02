"""SQLite persistence for rules and notifications.

Two classes — RuleRepo, NotificationRepo — each owning one table, plus a shared
connection helper. No query builder, no migrations framework (single
`CREATE TABLE IF NOT EXISTS` per table is enough at this scope).

"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from typing import Iterator, Optional

from .models import (Condition, EntityType, Notification, NotificationKind,
                     Rule, Severity)

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    entity_type         TEXT NOT NULL,          -- 'queue' | 'agent'
    conditions_json      TEXT NOT NULL,          -- JSON list[Condition]
    severity            TEXT NOT NULL,          -- 'info' | 'warning' | 'critical'
    recipients_json      TEXT NOT NULL,          -- JSON list[str]
    entity_ids_json       TEXT,                   -- JSON list[str] | NULL = all
    sustained_for_sec   INTEGER NOT NULL DEFAULT 0,
    cooldown_sec        INTEGER NOT NULL DEFAULT 900,
    escalate_after_sec  INTEGER,
    escalate_to_json     TEXT NOT NULL DEFAULT '[]',
    enabled             INTEGER NOT NULL DEFAULT 1,
    created_by          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notifications (
    id            TEXT PRIMARY KEY,
    ts            TEXT NOT NULL,
    kind          TEXT NOT NULL,        -- 'fired' | 'resolved' | 'escalated'
    rule_id       TEXT NOT NULL,
    rule_name     TEXT NOT NULL,
    severity      TEXT NOT NULL,
    entity_type   TEXT NOT NULL,
    entity_id     TEXT NOT NULL,
    message       TEXT NOT NULL,
    recipients_json TEXT NOT NULL,       -- JSON list[str]
    suppressed    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_notifications_ts ON notifications(ts);
CREATE INDEX IF NOT EXISTS idx_notifications_entity ON notifications(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_notifications_rule ON notifications(rule_id);
"""


def connect(path: str = "app.db") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Wrap a block in a transaction; commits on success, rolls back on
    exception. sqlite3 auto-starts a transaction on the first write, so this
    just guarantees commit/rollback happens at a clear boundary."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


class RuleRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def create(self, rule: Rule, created_by: Optional[str] = None) -> None:
        now = datetime.utcnow().isoformat()
        with transaction(self.conn):
            self.conn.execute(
                """INSERT INTO rules (id, name, entity_type, conditions_json,
                    severity, recipients_json, entity_ids_json,
                    sustained_for_sec, cooldown_sec, escalate_after_sec,
                    escalate_to_json, enabled, created_by, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (rule.id, rule.name, rule.entity_type.value,
                 json.dumps([asdict(c) for c in rule.conditions]),
                 rule.severity.value, json.dumps(rule.recipients),
                 json.dumps(rule.entity_ids) if rule.entity_ids is not None else None,
                 rule.sustained_for_sec, rule.cooldown_sec,
                 rule.escalate_after_sec, json.dumps(rule.escalate_to),
                 int(rule.enabled), created_by, now, now),
            )

    def get(self, rule_id: str) -> Optional[Rule]:
        row = self.conn.execute("SELECT * FROM rules WHERE id = ?", (rule_id,)).fetchone()
        return _row_to_rule(row) if row else None

    def list(self, enabled_only: bool = False) -> list[Rule]:
        sql = "SELECT * FROM rules"
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY created_at"
        return [_row_to_rule(r) for r in self.conn.execute(sql).fetchall()]

    def update(self, rule: Rule) -> bool:
        """Full replace of an existing rule's fields (id and created_by/at
        unchanged). Returns False if no such rule exists."""
        now = datetime.utcnow().isoformat()
        with transaction(self.conn):
            cur = self.conn.execute(
                """UPDATE rules SET name=?, entity_type=?, conditions_json=?,
                    severity=?, recipients_json=?, entity_ids_json=?,
                    sustained_for_sec=?, cooldown_sec=?, escalate_after_sec=?,
                    escalate_to_json=?, enabled=?, updated_at=?
                   WHERE id=?""",
                (rule.name, rule.entity_type.value,
                 json.dumps([asdict(c) for c in rule.conditions]),
                 rule.severity.value, json.dumps(rule.recipients),
                 json.dumps(rule.entity_ids) if rule.entity_ids is not None else None,
                 rule.sustained_for_sec, rule.cooldown_sec,
                 rule.escalate_after_sec, json.dumps(rule.escalate_to),
                 int(rule.enabled), now, rule.id),
            )
        return cur.rowcount > 0

    def set_enabled(self, rule_id: str, enabled: bool) -> bool:
        now = datetime.utcnow().isoformat()
        with transaction(self.conn):
            cur = self.conn.execute(
                "UPDATE rules SET enabled=?, updated_at=? WHERE id=?",
                (int(enabled), now, rule_id),
            )
        return cur.rowcount > 0

    def delete(self, rule_id: str) -> bool:
        # NOTE: does not touch any FIRING alert for this rule — that decision
        # (emit a closing notification? just let it dangle?) belongs to the
        # server layer per server.py's docstring, not to the repo.
        with transaction(self.conn):
            cur = self.conn.execute("DELETE FROM rules WHERE id=?", (rule_id,))
        return cur.rowcount > 0

    def seed_defaults_if_empty(self, default_rules: list[Rule]) -> int:
        """Convenience for local/demo runs: populate the table from
        rules_default.DEFAULT_RULES the first time it's empty. No-op
        otherwise (never overwrites rules a user has since edited)."""
        if self.conn.execute("SELECT COUNT(*) FROM rules").fetchone()[0] > 0:
            return 0
        for r in default_rules:
            self.create(r, created_by="system")
        return len(default_rules)


class NotificationRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def insert(self, n: Notification) -> None:
        with transaction(self.conn):
            self.conn.execute(
                """INSERT INTO notifications (id, ts, kind, rule_id, rule_name,
                    severity, entity_type, entity_id, message, recipients_json,
                    suppressed)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (n.id, n.ts.isoformat(), n.kind.value, n.rule_id, n.rule_name,
                 n.severity.value, n.entity_type.value, n.entity_id, n.message,
                 json.dumps(n.recipients), int(n.suppressed)),
            )

    def list(self, *, entity_id: Optional[str] = None,
             recipient: Optional[str] = None,
             include_suppressed: bool = True,
             since: Optional[datetime] = None,
             until: Optional[datetime] = None,
             limit: int = 200) -> list[Notification]:
        clauses, params = [], []
        if entity_id:
            clauses.append("entity_id = ?"); params.append(entity_id)
        if not include_suppressed:
            clauses.append("suppressed = 0")
        if since:
            clauses.append("ts >= ?"); params.append(since.isoformat())
        if until:
            clauses.append("ts <= ?"); params.append(until.isoformat())
        sql = "SELECT * FROM notifications"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        out = [_row_to_notification(r) for r in rows]
        if recipient:
            out = [n for n in out if recipient in n.recipients]
        return out

    def clear(self) -> None:
        """Wipe the notifications table. Handy between replay runs during
        development; not something a production endpoint should expose
        without real authz."""
        with transaction(self.conn):
            self.conn.execute("DELETE FROM notifications")


# --------------------------------------------------------------- row <-> dc
def _row_to_rule(row: sqlite3.Row) -> Rule:
    return Rule(
        id=row["id"], name=row["name"],
        entity_type=EntityType(row["entity_type"]),
        conditions=[Condition(**c) for c in json.loads(row["conditions_json"])],
        severity=Severity(row["severity"]),
        recipients=json.loads(row["recipients_json"]),
        entity_ids=(json.loads(row["entity_ids_json"])
                   if row["entity_ids_json"] is not None else None),
        sustained_for_sec=row["sustained_for_sec"],
        cooldown_sec=row["cooldown_sec"],
        escalate_after_sec=row["escalate_after_sec"],
        escalate_to=json.loads(row["escalate_to_json"]),
        enabled=bool(row["enabled"]),
    )


def _row_to_notification(row: sqlite3.Row) -> Notification:
    return Notification(
        id=row["id"], ts=datetime.fromisoformat(row["ts"]),
        kind=NotificationKind(row["kind"]), rule_id=row["rule_id"],
        rule_name=row["rule_name"], severity=Severity(row["severity"]),
        entity_type=EntityType(row["entity_type"]), entity_id=row["entity_id"],
        message=row["message"], recipients=json.loads(row["recipients_json"]),
        suppressed=bool(row["suppressed"]),
    )
