# Intraday Notification System

Alerting for contact-center operations: rules evaluated against a live event
stream, with the noise controls (sustained thresholds, hysteresis, cooldowns,
escalation, resolution notices) that make notifications trustworthy instead
of ignorable.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m app.replay events.jsonl        # watch the morning's incident unfold
python -m pytest tests/ -q               # 16 unit tests + replay invariants
uvicorn app.server:app --reload          # then: POST /replay, GET /notifications
http://localhost:8000/                   # load UI
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

1. **Alert lifecycle per (rule, entity):** OK → PENDING → FIRING → resolved.
   `sustained_for` kills blips, `clear_threshold` (hysteresis) kills
   hovering, `cooldown` kills flaps, escalation handles "still on fire",
   RESOLVED notifications give closure.
2. **Suppressed ≠ dropped.** Fires inside a cooldown are recorded with
   `suppressed=true` and shown gray. Auditable ("why wasn't I pinged?") and
   it demos the noise controls explicitly.
3. **Rules are data (AND-ed conditions).** Covers every realistic intraday
   rule found, keeps the UI story simple. OR = second rule.
4. **Metrics vocabulary layer.** Rules reference derived metrics
   (`sla_utilization` = wait/target makes one rule portable across queues
   with different SLAs). Adding a metric = one function.
5. **Event-time clock + tick.** Duration rules ("on a call 45m") have *no
   mid-call event*; a_11's 70-minute call is invisible until it ends. The
   engine ticks simulated time every 30s.
6. **None vs 0.0 in metric resolution.** Queue metrics return None when
   unknown (skip, never guess); agent duration metrics return 0.0 when the
   agent isn't in the state.

## Tradeoffs
- Rules are AND-only conditions, no OR/boolean tree. A boolean expression builder is a much harder form to design well.
- No batching/grouping of near-simultaneous alerts. Three long-call alerts landed on leads within minutes of each other during the incident — flagged as a known gap rather than solved, since a real batching window is its own design problem.
- Suppressed ≠ dropped. Every fire/resolve that's inside a cooldown still gets recorded with suppressed=true rather than silently discarded. Costs a bit of storage and one more field on every notification; buys full auditability.
- SQLite, not Postgres, and not in-memory. In-memory loses everything on restart. Postgres is the "correct" production choice but adds setup friction. SQLite is zero-setup and the schema translates to Postgres almost verbatim.
- PATCH replaces list fields wholesale (conditions, recipients, entity_ids) rather than merging item-by-item. Merging arrays gets ambiguous fast with multiple conditions ("add this" vs "replace all").
- Auto-generated rule ids (slugified name + collision suffix) rather than requiring the client to invent one. 
- Vanilla HTML/CSS/JS, no framework, no build step.
- Notification tray as a global overlay, not inline page content — a redesign mid-build, not the first draft. The digest originally lived inline on the head-of-support page; moved once it was clear a config screen isn't a push channel.
- Per-agent adherence rule instances (one DB row per agent) instead of one shared org-wide threshold. Costs more rows in the table; buys genuine per-agent configurability without one agent's change affecting seven others.

## Scaling
- Partitioning = Everything here already partitions cleanly by org → entity: Events for org A never need to touch org B's rule state. Within an org, rules are indexed by (entity_type, entity_id) — an incoming event only evaluates the rules that actually apply to it, not every rule in the system. This means horizontal scaling is "shard by org" (or by entity within very large orgs).
- Ingestion = Now: a Python generator reading a JSONL file. At scale: a real stream (Kafka/Kinesis/similar), partitioned by org or entity so ordering is preserved where it matters without needing a single global ordering. 
- Rule evaluation = AlertState per (rule, entity) is small and short-lived. At real scale this moves from an in-process dict to Redis (or similar) keyed by org:rule_id:entity_id, so any worker handling that org's partition can evaluate against shared state.
- Storage = SQLite → Postgres
- Delivery = Current Notifier is a single pluggable interface (deliver(notification)) — the module boundary a real Slack/email/push integration slots into without touching the engine. At scale this becomes a queue between "notification decided" and "notification delivered," so a slow or rate-limited downstream channel (Slack API limits, email provider throttling) never blocks rule evaluation.
- Fixing the tick sweep: the periodic tick sweep re-checks every rule against every entity on a fixed 30-second clock (needed for duration-based conditions like "on a call 45+ min," which have no triggering event) — so cost scales with rules × entities × orgs rather than actual activity. Replace the sweep with a deadline-indexed scheduler (a min-heap or timer wheel, the same pattern Kafka/Redis use) — compute each duration rule's exact trigger time up front and check it once, instead of polling everything on a clock, turning the cost into O(1) per actual state change instead of O(rules × entities) every tick.


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
- `tests/test_db.py` — dataclass -> row -> dataclass, filtering, updates
- `tests/test_routing.py` — build_digest(). hand-crafted scenarios one 
  behavior each, then a real replay of events.jsonl
- `tests/test_rule_validation.py` - creating new rules
- `test_server_logic.py` - PATCH and DELETE

## Where AI was used
ME: 
- Chose the framing: this is an alerting problem (Prometheus-style alert
  lifecycle), not a "send messages" problem; signal-to-noise is the core.
- Chose the audiences: team lead as rule-authoring primary, agents as
  recipients of one opinionated default, head of support as escalation +
  digest tier.
- Designed the CRUD endpoints, chose what resources to expose, and rule
  valudation process.
- Verifies the golden replay log by hand against raw events before running
  golden test.
- Chose SQLite persistence and schema
- Chose the UI design and format of notifications/digests/controls.
CLAUDE:
- Exploratory data analysis of `events.jsonl` (found the errors and
  the incident narrative).
- Generated the module scaffolding:
  ingest → world/metrics → engine → routing → notify, plus replay harness.
- Generated the unit test suite targeting the traps and noise-control edge
  cases the analysis surfaced.
- All generated code was reviewed; one nontrivial bug was found by running
  the replay against real data.
