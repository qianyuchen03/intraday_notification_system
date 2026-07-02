# Intraday Notification System

Alerting for contact-center operations: rules evaluated against a live event
stream, with the noise controls (sustained thresholds, hysteresis, cooldowns,
escalation, resolution notices) that make notifications trustworthy instead
of ignorable.

> **STATUS: scaffold + working core.** The engine, ingestion guards, routing,
> tests, and replay demo run today.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m app.replay events.jsonl        # watch the morning's incident unfold
python -m pytest tests/ -q               # 16 unit tests + replay invariants
uvicorn app.server:app --reload          # then: POST /replay, GET /notifications
```

The replay prints every notification with timestamp, severity, and resolved
recipients — plus, in gray, what the noise controls *suppressed* and an
ingest audit line showing the duplicate and out-of-order events it dropped.

## What it does with the sample data

96 events, 90 minutes, one bad morning: billing breaches SLA at 09:30 and
peaks at 2.25× target; an agent takes an unscheduled 35-minute break in the
middle of it; tier_2 follows; everything recovers by 10:30. The system turns
~40 raw threshold crossings into **21 meaningful notifications** routed to
three personas, suppresses 4 as flap/noise (visibly), escalates two lingering
breaches to the head of support, and drops 2 corrupt events at ingest.

## Who it's for

Team leads author rules and receive most alerts; agents get
one opinionated default — their own adherence nudge; head of support gets
escalations + a digest

## Design

```
events.jsonl ─▶ ingest ─▶ world/metrics ─▶ engine ─▶ routing ─▶ notify (stub)
               dedup      derived state    alert      who gets    console /
               ordering   + vocabulary     lifecycle  it, when    log / API
```

- **Rules are data** (see `app/models.py`): AND-ed conditions over a small
  metric vocabulary, plus per-rule noise controls.
- **Alert lifecycle** per (rule, entity): OK → PENDING → FIRING → resolved,
  with cooldown-suppressed events recorded rather than dropped
  (`app/engine.py`).
- **Messy-data policy**: unknown data is skipped, never guessed; silence
  never resolves an alert; nullable fields degrade conservatively
  (`app/world.py`, `app/ingest.py` — every case has a test).

## Tradeoffs

## Scaling



## Data model (persistence sketch)

Rules and notifications are in-memory in this scaffold. Intended schema:

```sql
rules(id PK, name, entity_type, entity_ids JSON, conditions JSON,
      severity, recipients JSON, sustained_for_sec, cooldown_sec,
      escalate_after_sec, escalate_to JSON, enabled, created_by, created_at)
rule_states(rule_id, entity_id, status, pending_since, firing_since,
            last_audible_activity, escalated, PRIMARY KEY (rule_id, entity_id))
notifications(id PK, ts, kind, rule_id, entity_type, entity_id, severity,
              message, recipients JSON, suppressed)  -- append-only
```

## Testing

- `tests/test_ingest.py` — the traps planted in the sample feed (duplicate
  event, out-of-order arrival, malformed records).
- `tests/test_engine.py` — one test per noise-control behavior, plus the
  messy-data semantics (null forecast, null violation start, silence ≠
  recovery, adherence/state reconciliation).
- `tests/test_golden_replay.py` — full pipeline vs. a hand-verified golden
  log.

## Where AI was used

