"""World state: the engine's live picture of queues and agents, built from the
event stream, plus the derived-metric vocabulary rules are written against.

Key semantics (decided against the sample data; see CLAUDE.md):

- Queue metrics hold their last value between snapshots. The feed's cadence is
  irregular (vip goes 75 minutes without a snapshot), and silence must not
  auto-resolve an alert. Unknown-yet metrics return None => rule evaluation
  skips, it never guesses.

- Agent duration metrics (state_duration_sec, adherence_violation_sec) return
  0.0 rather than None when the agent isn't in the filtered state / violation.
  This distinction matters: for queues, "no data" means *unknown*; for agent
  durations, "not in that state" is a definite false, which is what lets a
  FIRING long-call alert resolve when the call ends.

- adherence_check.actual_state is treated as an authoritative observation and
  reconciled into agent state. The sample proves why: a_23 shows up
  in_meeting in an adherence check with no corresponding agent_state_change.
  Without reconciliation we'd think a_23 was still on_call and fire a phantom
  long-call alert.

- in_violation=true with violation_started_at=null (a_23 again) falls back to
  the observation timestamp: strictly later than the truth, so we alert late
  rather than wrongly. Conservative by design.

- agent_state_change with queue_ids=null keeps the agent's last known queues
  (needed to route their alerts to the right team lead).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from .models import Condition


@dataclass
class QueueState:
    queue_id: str
    last_ts: Optional[datetime] = None
    tickets_waiting: Optional[int] = None
    longest_wait_sec: Optional[int] = None
    sla_target_sec: Optional[int] = None
    agents_available: Optional[int] = None
    agents_on_call: Optional[int] = None
    volume_last_15m: Optional[int] = None
    volume_forecast_next_15m: Optional[int] = None


@dataclass
class AgentState:
    agent_id: str
    current_state: Optional[str] = None
    state_since: Optional[datetime] = None
    queue_ids: list[str] = field(default_factory=list)   # last known, for routing
    in_violation: bool = False
    violation_started_at: Optional[datetime] = None
    last_ts: Optional[datetime] = None


class World:
    def __init__(self) -> None:
        self.queues: dict[str, QueueState] = {}
        self.agents: dict[str, AgentState] = {}

    # ------------------------------------------------------------------ apply
    def apply(self, event: dict[str, Any]) -> None:
        etype = event["type"]
        if etype == "queue_snapshot":
            self._apply_snapshot(event)
        elif etype == "agent_state_change":
            self._apply_state_change(event)
        elif etype == "adherence_check":
            self._apply_adherence(event)

    def _apply_snapshot(self, e: dict[str, Any]) -> None:
        q = self.queues.setdefault(e["queue_id"], QueueState(e["queue_id"]))
        q.last_ts = e["ts"]
        for f in ("tickets_waiting", "longest_wait_sec", "sla_target_sec",
                  "agents_available", "agents_on_call", "volume_last_15m",
                  "volume_forecast_next_15m"):
            setattr(q, f, e.get(f))

    def _apply_state_change(self, e: dict[str, Any]) -> None:
        a = self.agents.setdefault(e["agent_id"], AgentState(e["agent_id"]))
        a.current_state = e["new_state"]
        a.state_since = e["ts"]
        a.last_ts = e["ts"]
        if e.get("queue_ids"):        # null / [] => keep last known queues
            a.queue_ids = list(e["queue_ids"])

    def _apply_adherence(self, e: dict[str, Any]) -> None:
        a = self.agents.setdefault(e["agent_id"], AgentState(e["agent_id"]))
        a.last_ts = e["ts"]
        if e.get("queue_ids"):
            a.queue_ids = list(e["queue_ids"])

        # Reconcile observed state (see module docstring: the a_23 case).
        actual = e.get("actual_state")
        if actual and actual != a.current_state:
            a.current_state = actual
            a.state_since = e["ts"]   # best effort; true start is unknown

        if e.get("in_violation"):
            started_raw = e.get("violation_started_at")
            if started_raw:
                from .ingest import parse_ts
                a.violation_started_at = parse_ts(started_raw)
            elif not a.in_violation:
                # New violation with a null start ts: fall back to observation
                # time (conservative: alerts late, never wrongly).
                a.violation_started_at = e["ts"]
            a.in_violation = True
        else:
            a.in_violation = False
            a.violation_started_at = None

    # ---------------------------------------------------------------- metrics
    def resolve_metric(self, cond: Condition, entity_type: str, entity_id: str,
                       now: datetime) -> Optional[float]:
        """Return the current value of a metric, or None for 'unknown, skip'."""
        m = cond.metric
        if entity_type == "queue":
            q = self.queues.get(entity_id)
            if q is None or q.last_ts is None:
                return None
            if m == "tickets_waiting":
                return _f(q.tickets_waiting)
            if m == "longest_wait_sec":
                return _f(q.longest_wait_sec)
            if m == "agents_available":
                return _f(q.agents_available)
            if m == "sla_utilization":
                if q.longest_wait_sec is None or not q.sla_target_sec:
                    return None
                return q.longest_wait_sec / q.sla_target_sec
            if m == "volume_vs_forecast":
                # Null / zero forecast (present in the sample) => unknown.
                if q.volume_last_15m is None or not q.volume_forecast_next_15m:
                    return None
                return q.volume_last_15m / q.volume_forecast_next_15m
            return None

        a = self.agents.get(entity_id)
        if a is None:
            return None
        if m == "state_duration_sec":
            if cond.state_filter and a.current_state == cond.state_filter and a.state_since:
                return (now - a.state_since).total_seconds()
            return 0.0     # definite "not in that state" => condition false
        if m == "adherence_violation_sec":
            if a.in_violation and a.violation_started_at:
                return (now - a.violation_started_at).total_seconds()
            return 0.0
        return None


def _f(v: Optional[int]) -> Optional[float]:
    return None if v is None else float(v)
