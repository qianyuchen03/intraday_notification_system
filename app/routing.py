"""Routing: turn a rule's symbolic recipients into concrete people.

The org map is a hardcoded stand-in for what would be a real org/teams table
(auth & multi-tenancy are explicitly out of scope). The shape is the point:
routing is resolved per-notification from entity -> responsible humans, so the
same rule ("SLA breach on any queue") lands with billing's lead for billing
and tier_2's lead for tier_2 without per-queue configuration.

"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from .models import EntityType, Notification

# --- org map (stand-in for a real teams table) -----------------------------
QUEUE_LEADS: dict[str, str] = {
    "billing": "lead_priya",
    "tier_2": "lead_marcus",
    "vip": "lead_priya",          # priya covers vip too
}
HEAD_OF_SUPPORT = "hos_dana"

# Stand-in agent roster
KNOWN_AGENTS: list[str] = ["a_05", "a_07", "a_11", "a_19", "a_23", "a_31", "a_42", "a_88"]


class Router:
    """Resolves symbolic recipient specs on a notification, using world state
    for agent -> queue -> lead lookups."""

    def __init__(self, world) -> None:
        self.world = world

    def resolve(self, notification: Notification, recipient_specs: list[str]) -> None:
        out: list[str] = []
        for spec in recipient_specs:
            out.extend(self._resolve_one(spec, notification))
        # de-dupe, keep order
        notification.recipients = list(dict.fromkeys(out))

    def _resolve_one(self, spec: str, n: Notification) -> list[str]:
        if spec == "agent:self":
            return [n.entity_id] if n.entity_type == EntityType.AGENT else []
        if spec == "head_of_support":
            return [HEAD_OF_SUPPORT]
        if spec == "team_lead":
            if n.entity_type == EntityType.QUEUE:
                lead = QUEUE_LEADS.get(n.entity_id)
                return [lead] if lead else [HEAD_OF_SUPPORT]   # fallback: someone owns it
            # agent alert -> leads of all queues the agent works
            agent = self.world.agents.get(n.entity_id)
            leads = [QUEUE_LEADS[q] for q in (agent.queue_ids if agent else [])
                     if q in QUEUE_LEADS]
            return leads or [HEAD_OF_SUPPORT]
        return [spec]   # already a concrete recipient id


def build_digest(notifications: list[Notification],
                 since: datetime, until: datetime) -> Optional[str]:
    """TODO(you): implement the head-of-support digest.

    Spec (from the design discussion):
    - Input: all notifications in [since, until], including suppressed ones.
    - Output: a short human summary, e.g.
        "2 SLA breaches (billing 45m peak 2.2x, tier_2 15m); 2 adherence
         violations (a_19 35m break, a_88 ongoing); 2 calls >45m; all queues
         green at 10:30."
    - Group by rule, aggregate durations from FIRED->RESOLVED pairs, flag
      anything still FIRING as 'ongoing'.
    - Decide: does an empty period produce "all quiet" or no digest at all?
      (Product call — make it and defend it in the README.)
    """
    raise NotImplementedError