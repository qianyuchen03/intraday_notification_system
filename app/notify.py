"""Delivery stub. Real Slack/email/push is out of scope by the brief; this
makes notifications *visible* three ways:
  - pretty console lines during replay,
  - an in-memory store (what the /notifications endpoint serves),
  - an append-only notifications.log (jsonl) as the durable audit trail.

The Notifier is the module boundary a real channel plugs into: implement
`deliver(notification)` against Slack instead of print() and nothing upstream
changes.
"""
from __future__ import annotations

import json
from dataclasses import asdict

from .models import Notification, NotificationKind, Severity

ICON = {
    Severity.INFO: "\u2139\ufe0f ",
    Severity.WARNING: "\U0001f7e0",
    Severity.CRITICAL: "\U0001f534",
}


class Notifier:
    def __init__(self, log_path: str | None = "notifications.log",
                 console: bool = True) -> None:
        self.store: list[Notification] = []
        self.log_path = log_path
        self.console = console

    def deliver(self, n: Notification) -> None:
        self.store.append(n)
        if self.log_path:
            with open(self.log_path, "a") as f:
                rec = asdict(n)
                rec["ts"] = n.ts.isoformat()
                f.write(json.dumps(rec, default=str) + "\n")
        if self.console:
            print(self.format_line(n))

    @staticmethod
    def format_line(n: Notification) -> str:
        t = n.ts.strftime("%H:%M:%S")
        if n.suppressed:
            return f"{t}  \U0001f507 suppressed ({n.kind.value}) {n.message}"
        if n.kind == NotificationKind.RESOLVED:
            icon = "\u2705"
        elif n.kind == NotificationKind.ESCALATED:
            icon = "\U0001f6a8"
        else:
            icon = ICON[n.severity]
        who = ",".join(n.recipients) or "-"
        return f"{t}  {icon} [{n.severity.value:8}] {n.message}   -> {who}"
