"""HTTP surface: see notifications, manage rules.

STATUS: skeleton. /notifications and /replay work; rule CRUD is spec'd but
TODO(you) — it's the "rule configuration is the heart of the system" part of
the brief and should carry your fingerprints, not generated ones.

Run:
    uvicorn app.server:app --reload
    curl -X POST localhost:8000/replay
    curl localhost:8000/notifications | python -m json.tool

TODO(you) — rule CRUD, in rough priority order:
  1. GET  /rules                -> list rules (serialize the dataclasses)
  2. POST /rules                -> create from JSON; validate:
        - metric exists in QUEUE_METRICS/AGENT_METRICS for the entity_type
        - comparator in {>, >=, <, <=, ==}
        - state_duration_sec requires state_filter
        - sustained_for/cooldown are non-negative
     Reject with 422 + a message a support lead (not an engineer) can read.
  3. PATCH /rules/{id}          -> edit thresholds / enable-disable
  4. DELETE /rules/{id}         -> and decide: what happens to a FIRING alert
     when its rule is deleted? (Recommend: emit a final resolved-by-deletion
     notification so nothing dangles. Your call — defend it in the README.)
  5. Persistence: rules live in memory here. SQLite via a tiny repo class is
     plenty; schema sketch in README. Migrations are out of scope.

TODO(you) — nicer demo (optional): GET /notifications as a small HTML page
with severity colors and gray suppressed rows, auto-refresh. The reviewer
seeing suppressed rows in gray is the noise-control story told visually.
"""
from __future__ import annotations

from dataclasses import asdict

from fastapi import FastAPI, HTTPException

from .notify import Notifier
from .replay import run_replay
from .rules_default import DEFAULT_RULES

app = FastAPI(title="Intraday Notifications")

_last_run: dict = {"notifier": None, "stats": None}


@app.post("/replay")
def trigger_replay():
    """Replay events.jsonl through the engine and store the results."""
    notifier, ingestor = run_replay("events.jsonl", log_path=None)
    _last_run["notifier"] = notifier
    _last_run["stats"] = ingestor.stats
    return {"delivered": sum(1 for n in notifier.store if not n.suppressed),
            "suppressed": sum(1 for n in notifier.store if n.suppressed),
            "ingest": asdict(ingestor.stats)}


@app.get("/notifications")
def list_notifications(include_suppressed: bool = True, recipient: str | None = None):
    notifier: Notifier | None = _last_run["notifier"]
    if notifier is None:
        raise HTTPException(409, "no replay has been run yet — POST /replay first")
    out = []
    for n in notifier.store:
        if n.suppressed and not include_suppressed:
            continue
        if recipient and recipient not in n.recipients:
            continue
        rec = asdict(n)
        rec["ts"] = n.ts.isoformat()
        rec["display"] = Notifier.format_line(n)
        out.append(rec)
    return out


@app.get("/rules")
def list_rules(entity_type: EntityType | None = None,
               enabled_only: bool = False):
    """
    Query params:
      entity_type   'queue' | 'agent' — restrict to rules scoped to that
                    entity type (what a team-lead-facing UI would filter by
                    when showing "rules about my queues" vs "rules about my
                    agents").
      enabled_only  true -> exclude disabled rules (RuleRepo already
                    supports this filter; exposed here for a UI toggle).
    """
    rules = _rule_repo.list(enabled_only=enabled_only)
    if entity_type is not None:
        rules = [r for r in rules if r.entity_type == entity_type]
    return [_serialize_rule(r) for r in rules]
 
 
def _serialize_rule(rule) -> dict:
    """asdict() alone leaves Enum members (EntityType, Severity) as enum
    instances, which json can't encode — FastAPI would 500 on the response,
    not raise a clear error, so this is worth doing explicitly rather than
    relying on default serialization."""
    d = asdict(rule)
    d["entity_type"] = rule.entity_type.value
    d["severity"] = rule.severity.value
    return d

# TODO(you): POST /rules, PATCH /rules/{id}, DELETE /rules/{id} — see module
# docstring for the validation spec and the deletion-semantics decision.
