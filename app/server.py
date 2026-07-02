"""HTTP surface: see notifications, manage rules.

Run:
    uvicorn app.server:app --reload
    curl -X POST localhost:8000/replay
    curl localhost:8000/notifications | python -m json.tool


TODO: GET /notifications as a small HTML page
with severity colors and gray suppressed rows, auto-refresh. The reviewer
seeing suppressed rows in gray is the noise-control story told visually.
"""
from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Iterator, Optional

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from .db import RuleRepo, connect
from .models import (Condition, EntityType, Notification, NotificationKind,
                     Rule, Severity)
from .notify import Notifier
from .replay import run_replay
from .rule_validation import slugify, validate_rule_input
from .rules_default import DEFAULT_RULES

DB_PATH = "app.db"
app = FastAPI(title="Intraday Notifications")

_last_run: dict = {"notifier": None, "stats": None}


def get_db() -> Iterator:
    """FastAPI dependency: one SQLite connection per request, closed when
    the request ends."""
    
    conn = connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


# One-off connection at import time purely to seed default rules on first
# run. Immediately closed — request handling never touches this connection,
# only the per-request ones from get_db().
_seed_conn = connect(DB_PATH)
RuleRepo(_seed_conn).seed_defaults_if_empty(DEFAULT_RULES)
_seed_conn.close()


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
               enabled_only: bool = False,
               db=Depends(get_db)):
    """
    Query params:
      entity_type   'queue' | 'agent' — restrict to rules scoped to that
                    entity type (what a team-lead-facing UI would filter by
                    when showing "rules about my queues" vs "rules about my
                    agents").
      enabled_only  true -> exclude disabled rules (RuleRepo already
                    supports this filter; exposed here for a UI toggle).
    """
    rule_repo = RuleRepo(db)
    rules = rule_repo.list(enabled_only=enabled_only)
    if entity_type is not None:
        rules = [r for r in rules if r.entity_type == entity_type]
    return [_serialize_rule(r) for r in rules]


class ConditionIn(BaseModel):
    metric: str
    comparator: str
    threshold: float
    state_filter: Optional[str] = None
    clear_threshold: Optional[float] = None


class RuleIn(BaseModel):
    """Request body for creating a rule. Deliberately excludes `id` (server-
    generated from the name, see _generate_rule_id) — a human picking a rule
    name shouldn't also have to invent a unique machine id."""
    name: str
    entity_type: EntityType
    conditions: list[ConditionIn]
    severity: Severity
    recipients: list[str]
    entity_ids: Optional[list[str]] = None
    sustained_for_sec: int = 0
    cooldown_sec: int = 900
    escalate_after_sec: Optional[int] = None
    escalate_to: list[str] = Field(default_factory=list)
    enabled: bool = True


def _generate_rule_id(name: str, rule_repo: RuleRepo) -> str:
    """Slugify the name for a readable id ('Queue approaching SLA' ->
    'queue_approaching_sla'); on collision, append a short random suffix
    rather than erroring — a user renaming/duplicating a rule shouldn't hit
    a cryptic 'id already exists' failure over something they didn't type."""
    base = slugify(name)
    if rule_repo.get(base) is None:
        return base
    for _ in range(5):
        candidate = f"{base}_{uuid.uuid4().hex[:6]}"
        if rule_repo.get(candidate) is None:
            return candidate
    raise HTTPException(500, "could not generate a unique rule id, try again")


@app.post("/rules", status_code=201)
def create_rule(payload: RuleIn, db=Depends(get_db)):
    """Create a new rule.

    Validation: every condition's metric must belong to the rule's entity_type's
    metric vocabulary, comparators are whitelisted, state_duration_sec
    requires state_filter, durations are non-negative, recipients can't be
    empty, and escalate_after_sec requires a non-empty escalate_to. On
    failure: 422 with a list of plain-English error strings
    """
    errors = validate_rule_input(payload)
    if errors:
        raise HTTPException(422, detail=errors)

    rule_repo = RuleRepo(db)
    rule = Rule(
        id=_generate_rule_id(payload.name, rule_repo),
        name=payload.name,
        entity_type=payload.entity_type,
        conditions=[Condition(metric=c.metric, comparator=c.comparator,
                              threshold=c.threshold, state_filter=c.state_filter,
                              clear_threshold=c.clear_threshold)
                   for c in payload.conditions],
        severity=payload.severity,
        recipients=payload.recipients,
        entity_ids=payload.entity_ids,
        sustained_for_sec=payload.sustained_for_sec,
        cooldown_sec=payload.cooldown_sec,
        escalate_after_sec=payload.escalate_after_sec,
        escalate_to=payload.escalate_to,
        enabled=payload.enabled,
    )
    rule_repo.create(rule)
    return _serialize_rule(rule)


# --------------------------------------------------------------- PATCH /rules
class RuleUpdate(BaseModel):
    """Partial update. Only fields present in the request JSON are changed —
    including explicit nulls, which is why every field here defaults to
    `None` 
    """
    name: Optional[str] = None
    entity_type: Optional[EntityType] = None
    conditions: Optional[list[ConditionIn]] = None
    severity: Optional[Severity] = None
    recipients: Optional[list[str]] = None
    entity_ids: Optional[list[str]] = None
    sustained_for_sec: Optional[int] = None
    cooldown_sec: Optional[int] = None
    escalate_after_sec: Optional[int] = None
    escalate_to: Optional[list[str]] = None
    enabled: Optional[bool] = None


# Fields where an explicit `null` is a real, meaningful value
_NULLABLE_PATCH_FIELDS = {"entity_ids", "escalate_after_sec"}


def _provided_fields(payload: BaseModel) -> dict:
    """Fields explicitly present in the request body"""
    if hasattr(payload, "model_dump"):
        return payload.model_dump(exclude_unset=True)
    return payload.dict(exclude_unset=True)


@app.patch("/rules/{rule_id}")
def update_rule(rule_id: str, payload: RuleUpdate, db=Depends(get_db)):
    """Partially update a rule. 404 if it doesn't exist; 422 (same rules as
    creation) if the resulting merged rule is invalid"""
    rule_repo = RuleRepo(db)
    existing = rule_repo.get(rule_id)
    if existing is None:
        raise HTTPException(404, f"no rule with id '{rule_id}'")

    provided = _provided_fields(payload)
    bad_nulls = [f for f, v in provided.items()
                if v is None and f not in _NULLABLE_PATCH_FIELDS]
    if bad_nulls:
        raise HTTPException(422, detail=[f"{f} cannot be set to null" for f in bad_nulls])

    merged = Rule(
        id=existing.id,
        name=provided.get("name", existing.name),
        entity_type=provided.get("entity_type", existing.entity_type),
        conditions=([Condition(**c) for c in provided["conditions"]]
                   if "conditions" in provided else existing.conditions),
        severity=provided.get("severity", existing.severity),
        recipients=provided.get("recipients", existing.recipients),
        entity_ids=(provided["entity_ids"] if "entity_ids" in provided
                   else existing.entity_ids),
        sustained_for_sec=provided.get("sustained_for_sec", existing.sustained_for_sec),
        cooldown_sec=provided.get("cooldown_sec", existing.cooldown_sec),
        escalate_after_sec=(provided["escalate_after_sec"] if "escalate_after_sec" in provided
                            else existing.escalate_after_sec),
        escalate_to=provided.get("escalate_to", existing.escalate_to),
        enabled=provided.get("enabled", existing.enabled),
    )

    errors = validate_rule_input(merged)   # Rule/Condition duck-type the same as RuleIn/ConditionIn
    if errors:
        raise HTTPException(422, detail=errors)

    rule_repo.update(merged)
    return _serialize_rule(merged)

def _open_alerts_for_rule(notifier: Notifier, rule_id: str) -> dict[str, Notification]:
    
    open_map: dict[str, Notification] = {}
    for n in notifier.store:
        if n.rule_id != rule_id:
            continue
        if n.kind == NotificationKind.FIRED:
            open_map[n.entity_id] = n
        elif n.kind == NotificationKind.RESOLVED:
            open_map.pop(n.entity_id, None)
    return open_map


@app.delete("/rules/{rule_id}")
def delete_rule(rule_id: str, db=Depends(get_db)):
    """Delete a rule. 404 if it doesn't exist.

    Before deleting: for every entity whose alert was still FIRING as of
    the most recent /replay run, emit a synthetic RESOLVED notification
    ("closed automatically because the rule was deleted") to the same
    recipients the original alert went to, so nothing dangles silently from
    a team lead's perspective.
    """
    rule_repo = RuleRepo(db)
    rule = rule_repo.get(rule_id)
    if rule is None:
        raise HTTPException(404, f"no rule with id '{rule_id}'")

    closed_entities: list[str] = []
    notifier: Notifier | None = _last_run["notifier"]
    if notifier is not None:
        for entity_id, fired in _open_alerts_for_rule(notifier, rule_id).items():
            notifier.deliver(Notification(
                id=f"ntf_del_{uuid.uuid4().hex[:8]}",
                ts=datetime.now(timezone.utc),
                kind=NotificationKind.RESOLVED,
                rule_id=rule.id, rule_name=rule.name, severity=rule.severity,
                entity_type=rule.entity_type, entity_id=entity_id,
                message=(f"{rule.name} — {entity_id}: closed automatically "
                        f"because the rule was deleted while firing"),
                recipients=fired.recipients, suppressed=False,
            ))
            closed_entities.append(entity_id)

    rule_repo.delete(rule_id)
    return {"deleted": rule_id, "closed_open_alerts_for": closed_entities}


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
