"""Routing: turn a rule's symbolic recipients into concrete people, and
build the head-of-support digest.
"""
from __future__ import annotations

from datetime import datetime

from .models import EntityType, Notification, NotificationKind

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


_SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2}


def _fmt_dur(sec: float) -> str:
    """Local, deliberately duplicated from engine.py's private `_fmt_dur`
    rather than imported: it's three lines, and reaching into another
    module's underscore-prefixed helper is worse coupling than the
    duplication."""
    m, s = divmod(int(max(sec, 0)), 60)
    return f"{m}m{s:02d}s" if m else f"{s}s"


def _fmt_clock(ts: datetime) -> str:
    return ts.strftime("%H:%M") + " UTC"


def build_digest(notifications: list[Notification],
                 since: datetime, until: datetime) -> str:
    """Build the head-of-support digest: a short summary of what happened
    in [since, until], covering everything unless nothing happened.

    Structure (all sections optional except the closing line, which always
    appears): escalations first (the strongest "something is on fire"
    signal — see routing.py's escalate_after_sec), then one clause per rule
    grouping the entities it fired for with how long each was active (or
    "ongoing" if still open at `until`), then a closing status line.
    """
    if not notifications:
        return f"All quiet — no notifications between {_fmt_clock(since)} and {_fmt_clock(until)}."

    ordered = sorted(notifications, key=lambda n: n.ts)

    # rule_id -> {"name":.., "severity":.., "closed": [(entity_id, dur_sec)],
    #             "ongoing": [entity_id, ...]}
    groups: dict[str, dict] = {}
    open_fired: dict[tuple[str, str], Notification] = {}   # (rule_id, entity_id) -> FIRED notification
    escalations: list[Notification] = []

    for n in ordered:
        if n.kind == NotificationKind.ESCALATED:
            escalations.append(n)
            continue
        g = groups.setdefault(n.rule_id, {"name": n.rule_name, "severity": n.severity.value,
                                          "closed": [], "ongoing": []})
        key = (n.rule_id, n.entity_id)
        if n.kind == NotificationKind.FIRED:
            open_fired[key] = n
        elif n.kind == NotificationKind.RESOLVED:
            fired = open_fired.pop(key, None)
            duration = (n.ts - fired.ts).total_seconds() if fired else 0.0
            g["closed"].append((n.entity_id, duration))

    # Anything still FIRED at `until` with no RESOLVED is ongoing.
    for (rule_id, entity_id), fired in open_fired.items():
        g = groups[rule_id]
        g["ongoing"].append((entity_id, (until - fired.ts).total_seconds()))

    clauses: list[str] = []

    if escalations:
        parts = []
        for n in escalations[:3]:
            parts.append(f"{n.rule_name} on {n.entity_id}")
        more = f", +{len(escalations) - 3} more" if len(escalations) > 3 else ""
        clauses.append(f"{len(escalations)} escalation{'s' if len(escalations) != 1 else ''} "
                       f"({', '.join(parts)}{more})")

    def group_sort_key(item):
        rule_id, g = item
        count = len(g["closed"]) + len(g["ongoing"])
        return (_SEVERITY_RANK.get(g["severity"], 3), -count)

    for rule_id, g in sorted(groups.items(), key=group_sort_key):
        entities = [f"{eid} {_fmt_dur(dur)}" for eid, dur in g["closed"]]
        entities += [f"{eid} ongoing" for eid, _ in g["ongoing"]]
        if not entities:
            continue   # a rule that only ever escalated, never fired directly (shouldn't happen, but don't crash)
        count = len(entities)
        shown = entities[:3]
        more = f", +{count - 3} more" if count > 3 else ""
        clauses.append(f"{count}× {g['name']} ({', '.join(shown)}{more})")

    # Closing status: what's still open as of `until`, across everything.
    still_open = [(rule_id, eid) for rule_id, g in groups.items() for eid, _ in g["ongoing"]]
    if still_open:
        preview = ", ".join(f"{eid} ({groups[rid]['name']})" for rid, eid in still_open[:3])
        more = f", +{len(still_open) - 3} more" if len(still_open) > 3 else ""
        closing = f"{len(still_open)} still open at {_fmt_clock(until)}: {preview}{more}."
    else:
        closing = f"All clear as of {_fmt_clock(until)}."
    clauses.append(closing)

    return "; ".join(clauses)