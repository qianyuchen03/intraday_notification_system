"""Domain models for the intraday notification system.

- A Rule is structured data, not code. This is what makes rules buildable in a
  UI, validatable, indexable, and testable.
- Conditions within a rule are AND-ed. This covers every realistic intraday
  rule ("0 agents available AND >= 10 tickets waiting") without the
  complexity of a full boolean expression tree. OR = create a second rule.
- Noise controls live ON the rule (sustained_for, cooldown, clear_threshold)
  because different rules need different aggressiveness.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class EntityType(str, Enum):
    QUEUE = "queue"
    AGENT = "agent"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class NotificationKind(str, Enum):
    FIRED = "fired"
    RESOLVED = "resolved"
    ESCALATED = "escalated"


# Metrics the engine knows how to compute. Rules reference these by name.
# Adding a metric = one function in world.py; rules pick it up for free.
QUEUE_METRICS = {
    "tickets_waiting",
    "longest_wait_sec",
    "sla_utilization",        # longest_wait_sec / sla_target_sec  (portable across queues!)
    "agents_available",
    "volume_vs_forecast",     # volume_last_15m / volume_forecast_next_15m
}
AGENT_METRICS = {
    "state_duration_sec",         # requires state_filter, e.g. "on_call"
    "adherence_violation_sec",    # 0 when in adherence
}


@dataclass
class Condition:
    metric: str
    comparator: str                       # one of: > >= < <= ==
    threshold: float
    state_filter: Optional[str] = None    # only for state_duration_sec
    # Hysteresis: once FIRING, the alert only resolves when the condition is
    # false against clear_threshold (defaults to threshold). Prevents flapping
    # when a metric hovers at the line.
    clear_threshold: Optional[float] = None

    def effective_clear_threshold(self) -> float:
        return self.threshold if self.clear_threshold is None else self.clear_threshold


@dataclass
class Rule:
    id: str
    name: str
    entity_type: EntityType
    conditions: list[Condition]
    severity: Severity
    # Recipient specs are symbolic; routing.py resolves them to people.
    #   "agent:self"       -> the agent the alert is about
    #   "team_lead"        -> lead(s) responsible for the entity
    #   "head_of_support"  -> org-level recipient
    recipients: list[str]
    entity_ids: Optional[list[str]] = None   # None => all entities of this type
    sustained_for_sec: int = 0                # condition must hold this long before firing
    cooldown_sec: int = 900                   # min gap between audible fires per entity
    escalate_after_sec: Optional[int] = None  # if still FIRING after this, escalate
    escalate_to: list[str] = field(default_factory=list)
    enabled: bool = True

    def applies_to(self, entity_id: str) -> bool:
        return self.entity_ids is None or entity_id in self.entity_ids


@dataclass
class Notification:
    id: str
    ts: datetime
    kind: NotificationKind
    rule_id: str
    rule_name: str
    severity: Severity
    entity_type: EntityType
    entity_id: str
    message: str
    recipients: list[str]          # resolved, concrete recipients
    # True when the fire/resolve happened inside a cooldown window. We record
    # suppressed notifications instead of dropping them: the reviewer (and a
    # real ops team auditing "why didn't I get pinged?") can see exactly what
    # the noise controls swallowed.
    suppressed: bool = False
