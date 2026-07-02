"""Seeded rules: the presets a support team would start from.

These are the "templates over blank canvas" product decision — a team lead
edits thresholds on these rather than composing metrics from scratch. Every
rule here is exercised (or deliberately NOT exercised) by events.jsonl; see
CLAUDE.md's expected-firings table.

TODO(you): review every threshold and noise setting below — these are product
decisions and you should own them. In particular:
  - sla_at_risk deliberately has NO hysteresis so the 10:16 billing flap is
    handled by cooldown (a visible suppression in the demo). You could instead
    give it clear_threshold=0.7 and let hysteresis absorb the flap silently.
    Pick one and defend it.
  - volume_over_forecast never fires in the sample (ratios stay < 1.0).
    That's an honest negative case — mention it rather than hiding it.
"""
from .models import Condition, EntityType, Rule, Severity

DEFAULT_RULES: list[Rule] = [
    Rule(
        id="sla_at_risk",
        name="Queue approaching SLA",
        entity_type=EntityType.QUEUE,
        conditions=[Condition("sla_utilization", ">=", 0.8)],
        severity=Severity.WARNING,
        recipients=["team_lead"],
        cooldown_sec=900,
    ),
    Rule(
        id="sla_breach",
        name="Queue breaching SLA",
        entity_type=EntityType.QUEUE,
        conditions=[Condition("sla_utilization", ">=", 1.0, clear_threshold=0.95)],
        severity=Severity.CRITICAL,
        recipients=["team_lead"],
        cooldown_sec=600,
        escalate_after_sec=900,
        escalate_to=["head_of_support"],
    ),
    Rule(
        id="queue_understaffed",
        name="Queue understaffed",
        entity_type=EntityType.QUEUE,
        conditions=[
            Condition("agents_available", "<=", 0),
            Condition("tickets_waiting", ">=", 10),
        ],
        severity=Severity.WARNING,
        recipients=["team_lead"],
        cooldown_sec=1800,
    ),
    Rule(
        id="adherence_10m",
        name="Out of adherence 10m+",
        entity_type=EntityType.AGENT,
        conditions=[Condition("adherence_violation_sec", ">=", 600)],
        severity=Severity.WARNING,
        recipients=["agent:self", "team_lead"],
        cooldown_sec=1800,
    ),
    Rule(
        id="long_call_45m",
        name="Single call over 45m",
        entity_type=EntityType.AGENT,
        conditions=[Condition("state_duration_sec", ">=", 2700,
                              state_filter="on_call")],
        severity=Severity.WARNING,
        recipients=["team_lead"],
        cooldown_sec=3600,
    ),
    Rule(
        id="volume_over_forecast",
        name="Volume 50% over forecast",
        entity_type=EntityType.QUEUE,
        conditions=[Condition("volume_vs_forecast", ">=", 1.5)],
        severity=Severity.WARNING,
        recipients=["team_lead"],
        cooldown_sec=1800,
    ),
]
