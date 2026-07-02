"""Golden replay test: the whole pipeline against the real events.jsonl.

Deterministic replay in => exact notification log out. This is the single
most valuable test in the project because it locks in the interaction of ALL
the noise controls at once — any change to thresholds, cooldown semantics, or
tick cadence shows up as a diff against a human-verified expectation.

HOW TO BLESS THE GOLDEN FILE:
  1. Run:  python -m app.replay events.jsonl > /tmp/replay.txt
  2. Verify the log BY HAND against the raw data. Do not skip this — it is
     the point. For each line, check the trigger in events.jsonl
  3. Copy the verified notification lines into tests/golden_expected.txt
  4. This test then guards them forever.
"""
import os

import pytest

from app.notify import Notifier
from app.replay import run_replay

HERE = os.path.dirname(__file__)
GOLDEN = os.path.join(HERE, "golden_expected.txt")
EVENTS = os.path.join(HERE, "..", "events.jsonl")


def test_replay_matches_blessed_golden_log():
    notifier, _ = run_replay(EVENTS, log_path=None)
    actual = [Notifier.format_line(n) for n in notifier.store]
    expected = [l.rstrip("\n") for l in open(GOLDEN) if l.strip()]
    assert actual == expected


def test_replay_invariants_hold_even_without_golden_file():
    """Cheap structural checks that don't require the blessed file."""
    notifier, ingestor = run_replay(EVENTS, log_path=None)
    # both planted traps are caught
    assert ingestor.stats.duplicates == 1
    assert ingestor.stats.stale_dropped == 1
    # every fire eventually resolves or is still open at end — never double-fires
    open_alerts: set[tuple[str, str]] = set()
    for n in notifier.store:
        key = (n.rule_id, n.entity_id)
        if n.kind.value == "fired":
            assert key not in open_alerts, f"double fire without resolve: {key}"
            open_alerts.add(key)
        elif n.kind.value == "resolved":
            assert key in open_alerts, f"resolve without fire: {key}"
            open_alerts.discard(key)
    # noise controls actually suppressed something in this data
    assert any(n.suppressed for n in notifier.store)
