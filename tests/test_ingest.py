"""Ingestion guards: these encode the traps planted in events.jsonl.

Each test exists because the sample data would break a naive implementation
in exactly this way (see CLAUDE.md "data findings").
"""
from app.ingest import Ingestor


def ev(event_id="evt_1", ts="2026-05-26T09:00:00Z", type="queue_snapshot",
       queue_id="billing", **kw):
    return {"event_id": event_id, "ts": ts, "type": type,
            "queue_id": queue_id, **kw}


def test_duplicate_event_id_dropped():
    ing = Ingestor()
    assert ing.accept(ev()) is not None
    assert ing.accept(ev()) is None            # same event_id => dropped
    assert ing.stats.duplicates == 1


def test_out_of_order_event_for_same_entity_dropped():
    ing = Ingestor()
    assert ing.accept(ev("evt_1", "2026-05-26T10:30:00Z")) is not None
    # the sample's trap: a 09:49 snapshot arriving after 10:30 data
    assert ing.accept(ev("evt_2", "2026-05-26T09:49:00Z")) is None
    assert ing.stats.stale_dropped == 1


def test_out_of_order_across_entities_is_fine():
    # Watermarks are per-entity: vip lagging billing must not drop vip data.
    ing = Ingestor()
    assert ing.accept(ev("evt_1", "2026-05-26T10:30:00Z", queue_id="billing")) is not None
    assert ing.accept(ev("evt_2", "2026-05-26T09:49:00Z", queue_id="vip")) is not None


def test_malformed_events_skipped_not_crashed():
    ing = Ingestor()
    assert ing.accept({"type": "queue_snapshot"}) is None       # no id/ts
    assert ing.accept(ev(type="mystery_event")) is None         # unknown type
    assert ing.stats.malformed == 2


def test_ts_parsed_to_datetime():
    ing = Ingestor()
    out = ing.accept(ev())
    assert out["ts"].tzinfo is not None
