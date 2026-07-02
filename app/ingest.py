"""Ingestion: parse raw events, enforce idempotency and ordering guarantees.

The sample feed contains deliberate traps this layer absorbs so nothing
downstream has to think about them:
  1. Duplicate event_id (evt_01HXYZ050 appears twice)      -> dedup set
  2. Events arriving out of order (a 09:49 snapshot after
     the 10:30 events)                                      -> per-entity
     monotonic timestamp check; stale events are dropped and counted
  3. Null / missing fields                                   -> tolerated here,
     handled semantically in world.py

Production note: dedup here is an in-memory set keyed by event_id, which is
fine for a single-process demo. At scale this becomes a keyed idempotency
check in the stream consumer (e.g. Kafka partition by org/entity + a rolling
window of seen ids), and "stale" is defined per entity partition the same way.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator, Optional

logger = logging.getLogger("ingest")

VALID_TYPES = {"queue_snapshot", "agent_state_change", "adherence_check"}


def parse_ts(raw: str) -> datetime:
    # Python 3.11+ handles the trailing Z; keep the replace for 3.10 compat.
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


@dataclass
class IngestStats:
    accepted: int = 0
    duplicates: int = 0
    stale_dropped: int = 0
    malformed: int = 0
    dropped_events: list[str] = field(default_factory=list)  # audit trail


class Ingestor:
    def __init__(self) -> None:
        self._seen_ids: set[str] = set()
        # Latest accepted ts per entity (queue_id or agent_id). An event older
        # than what we've already processed for that entity would regress
        # state, so we drop it. Trade-off (documented in CLAUDE.md): a late
        # event within the same entity is lost rather than merged; acceptable
        # because every event type here is a full-state snapshot, not a delta.
        self._entity_watermark: dict[str, datetime] = {}
        self.stats = IngestStats()

    def accept(self, raw: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Return the event dict (with parsed 'ts') if it should be processed,
        else None."""
        event_id = raw.get("event_id")
        etype = raw.get("type")
        ts_raw = raw.get("ts")
        if not event_id or etype not in VALID_TYPES or not ts_raw:
            self.stats.malformed += 1
            logger.warning("malformed event skipped: %s", raw)
            return None

        if event_id in self._seen_ids:
            self.stats.duplicates += 1
            self.stats.dropped_events.append(event_id)
            logger.info("duplicate event dropped: %s", event_id)
            return None
        self._seen_ids.add(event_id)

        ts = parse_ts(ts_raw)
        entity_key = raw.get("queue_id") or raw.get("agent_id")
        if entity_key:
            wm = self._entity_watermark.get(entity_key)
            if wm is not None and ts < wm:
                self.stats.stale_dropped += 1
                self.stats.dropped_events.append(event_id)
                logger.info(
                    "stale event dropped: %s (%s at %s, entity already at %s)",
                    event_id, entity_key, ts.isoformat(), wm.isoformat(),
                )
                return None
            self._entity_watermark[entity_key] = ts

        self.stats.accepted += 1
        out = dict(raw)
        out["ts"] = ts
        return out


def read_jsonl(path: str) -> Iterator[dict[str, Any]]:
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)
