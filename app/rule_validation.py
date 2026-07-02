"""Validation for rule create/update requests."""
from __future__ import annotations

import re

from .models import AGENT_METRICS, EntityType, QUEUE_METRICS

VALID_COMPARATORS = {">", ">=", "<", "<=", "=="}


def validate_rule_input(payload) -> list[str]:
    """Return a list of human-readable error strings; empty = valid.

    Checks:
      - metric exists in QUEUE_METRICS/AGENT_METRICS for the rule's entity_type
      - comparator is one of >, >=, <, <=, ==
      - state_duration_sec requires state_filter
      - sustained_for_sec / cooldown_sec are non-negative
      - recipients is non-empty (an alert nobody receives is a bug)
      - escalate_after_sec, if set, is non-negative and has a non-empty
        escalate_to (escalating to nobody defeats the point)
    """
    errors: list[str] = []

    if not payload.name or not payload.name.strip():
        errors.append("name must not be empty")

    if not payload.conditions:
        errors.append("at least one condition is required")
    else:
        metric_set = (QUEUE_METRICS if payload.entity_type == EntityType.QUEUE
                     else AGENT_METRICS)
        for i, c in enumerate(payload.conditions):
            if c.metric not in metric_set:
                errors.append(
                    f"conditions[{i}]: metric '{c.metric}' is not valid for "
                    f"entity_type '{payload.entity_type.value}' (valid: "
                    f"{sorted(metric_set)})")
            if c.comparator not in VALID_COMPARATORS:
                errors.append(
                    f"conditions[{i}]: comparator '{c.comparator}' must be "
                    f"one of {sorted(VALID_COMPARATORS)}")
            if c.metric == "state_duration_sec" and not c.state_filter:
                errors.append(
                    f"conditions[{i}]: metric 'state_duration_sec' requires "
                    f"state_filter (e.g. 'on_call')")

    if not payload.recipients or any(not r.strip() for r in payload.recipients):
        errors.append(
            "recipients must be a non-empty list of non-empty strings — "
            "an alert with nobody to receive it is a bug, not a rule")

    if payload.sustained_for_sec < 0:
        errors.append("sustained_for_sec must be >= 0")
    if payload.cooldown_sec < 0:
        errors.append("cooldown_sec must be >= 0")

    if payload.escalate_after_sec is not None:
        if payload.escalate_after_sec < 0:
            errors.append("escalate_after_sec must be >= 0")
        if not payload.escalate_to:
            errors.append(
                "escalate_to must not be empty when escalate_after_sec is "
                "set — escalating to nobody defeats the point")

    return errors


def slugify(name: str) -> str:
    """'Queue approaching SLA' -> 'queue_approaching_sla'. Used to derive a
    readable rule id from its name; callers are responsible for resolving
    collisions (see server.py's _generate_rule_id)."""
    slug = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    return slug or "rule"