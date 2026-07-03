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

96 events, 90 minutes: billing breaches SLA at 09:30 and
peaks at 2.25× target; an agent takes an unscheduled 35-minute break in the
middle of it; tier_2 follows; everything recovers by 10:30. The system turns
~40 raw threshold crossings into **21 meaningful notifications** routed to
three personas, suppresses 4 as noise (visibly), escalates two lingering
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
- No batching/grouping of near-simultaneous alerts. Batching window is separate
  design problem
- PATCH replaces list fields wholesale (conditions, recipients, entity_ids) rather than merging item-by-item. Merging arrays gets ambiguous fast with multiple conditions ("add this" vs "replace all").
- Auto-generated rule ids (slugified name + collision suffix) rather than requiring the client to invent one. 
- Vanilla HTML/CSS/JS, no framework, no build step.
- Notification tray as a global overlay, not inline page content — a redesign mid-build. The digest originally lived inline on the head-of-support page.
- Per-agent adherence rule instances (one DB row per agent) instead of one shared org-wide threshold. Costs more rows in the table; buys per-agent configurability without one agent's change affecting others.

## Scaling
- **Partitioning** = Everything here already partitions cleanly by org → entity: Events for org A never need to touch org B's rule state. Within an org, rules are indexed by (entity_type, entity_id) — an incoming event only evaluates the rules that actually apply to it, not every rule in the system.
- **Ingestion** = Now: a Python generator reading a JSONL file. At scale: a real stream (Kafka/Kinesis/similar), partitioned by org or entity so ordering is preserved where it matters without needing a single global ordering. 
- **Rule evaluation** = AlertState per (rule, entity) is small and short-lived. At real scale this moves from an in-process dict to Redis (or similar) keyed by org:rule_id:entity_id, so any worker handling that org's partition can evaluate against shared state.
- **Storage** = SQLite → Postgres
- **Delivery** = Currently is one simple interface that a real Slack, email, or push integration can plug into without touching the alerting engine. At scale, that call becomes a queue instead of a direct call: rule evaluation drops a notification in the queue and moves on, while a separate worker handles actually sending it. That way a slow or rate-limited channel never blocks the engine from evaluating the next alert.
- **Fixing the tick sweep**: Currently, every 30 seconds, the engine re-checks every rule against every entity, needed for things like "flag a call over 45 minutes," where nothing else would tell it time has passed. The problem is this cost grows with rules × entities × orgs, not with how much is actually happening. The fix is to stop polling and instead schedule each check for the exact moment it's due (deadline queue like in Kafka and Redis)

## Data model

```sql
Rule
  id, name, entity_type ('queue' | 'agent'), entity_ids (list[str] | None = all)
  conditions: list[Condition]        — AND-ed
  severity, recipients (symbolic specs: 'team_lead', 'agent:self', 'head_of_support', or a concrete id)
  sustained_for_sec, cooldown_sec    — noise controls
  escalate_after_sec, escalate_to    — optional

Condition
  metric, comparator ('>','>=','<','<=','=='), threshold
  state_filter (required only for state_duration_sec)
  clear_threshold (optional — hysteresis)

Notification
  id, ts, kind ('fired' | 'resolved' | 'escalated')
  rule_id, rule_name, severity, entity_type, entity_id
  message, recipients (resolved, concrete), suppressed

AlertState  (per rule × entity, engine-internal, not persisted — see below)
  status ('OK' | 'PENDING' | 'FIRING')
  pending_since, firing_since, last_audible_activity, escalated
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
###Me: 
- Chose the framing: this is an alerting problem (Prometheus-style alert
  lifecycle); signal-to-noise is the core.
- Chose the audiences: team lead as rule-authoring primary, agents as
  recipients of one opinionated default, head of support as escalation +
  digest tier.
- Designed the CRUD endpoints, chose what resources to expose, and rule
  valudation process.
- Verifies the golden replay log by hand against raw events before running
  golden test.
- Chose SQLite persistence and schema
- Chose the UI design and format of notifications/digests/controls.

###CLAUDE:
- Exploratory data analysis of `events.jsonl` (found the errors and
  the incident narrative).
- Generated the module scaffolding:
  ingest → world/metrics → engine → routing → notify, plus replay harness.
- Generated the unit test suite targeting the traps and noise-control edge
  cases the analysis surfaced.
- All generated code was reviewed; one nontrivial bug was found by running
  the replay against real data.
