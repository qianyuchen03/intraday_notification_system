"""Replay harness: feed events.jsonl through the full pipeline and watch the
notifications fire. This is the demo AND the backbone of the golden test.

Usage:
    python -m app.replay events.jsonl
    python -m app.replay events.jsonl --quiet-suppressed   # hide noise-control audit lines
"""
from __future__ import annotations

import argparse

from .engine import Engine
from .ingest import Ingestor, read_jsonl
from .models import Notification
from .notify import Notifier
from .routing import Router
from .rules_default import DEFAULT_RULES
from .world import World


def run_replay(path: str, show_suppressed: bool = True,
               log_path: str | None = "notifications.log") -> tuple[Notifier, Ingestor]:
    world = World()
    notifier = Notifier(log_path=log_path, console=False)
    router = Router(world)
    rules = {r.id: r for r in DEFAULT_RULES}

    def sink(n: Notification) -> None:
        router.resolve(n, rules[n.rule_id].recipients if n.kind.value != "escalated"
                       else rules[n.rule_id].escalate_to)
        notifier.deliver(n)

    engine = Engine(world, DEFAULT_RULES, sink)
    ingestor = Ingestor()

    for raw in read_jsonl(path):
        event = ingestor.accept(raw)
        if event is not None:
            engine.process_event(event)

    # print after the fact so console output is one clean block
    for n in notifier.store:
        if n.suppressed and not show_suppressed:
            continue
        print(Notifier.format_line(n))

    s = ingestor.stats
    print(f"\n--- ingest: {s.accepted} accepted, {s.duplicates} duplicate(s) "
          f"dropped, {s.stale_dropped} stale/out-of-order dropped, "
          f"{s.malformed} malformed ({', '.join(s.dropped_events) or 'none'})")
    audible = sum(1 for n in notifier.store if not n.suppressed)
    print(f"--- notifications: {audible} delivered, "
          f"{len(notifier.store) - audible} suppressed by noise controls")
    return notifier, ingestor


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--quiet-suppressed", action="store_true")
    args = ap.parse_args()
    run_replay(args.path, show_suppressed=not args.quiet_suppressed)
