"""Rule evaluation engine.

Every (rule, entity) pair is a small state machine:

    OK --cond true--> PENDING --held for sustained_for--> FIRING --cond
    false (vs clear thresholds)--> OK  (+ a RESOLVED notification)

This is the same lifecycle Prometheus/Alertmanager use, and it's what turns a
raw threshold check into a notification a human can trust:

- sustained_for kills one-sample blips,
- clear_threshold (hysteresis) kills flapping at the line,
- cooldown_sec kills rapid re-fires; suppressed fires are *recorded*, not
  dropped, so they're auditable and demoable,
- escalate_after_sec turns a lingering FIRING into a second, louder
  notification to a different audience,
- FIRING -> OK emits a RESOLVED notification (ops people need closure as much
  as they need alarms).

Evaluation triggers:
- on every accepted event, for rules scoped to that entity, and
- on a periodic tick of the *simulated* clock. The tick is not optional:
  duration rules ("on a call 45 min") have no event mid-call to react to, and
  PENDING->FIRING promotion + escalation are pure time passing.

Scale note (production, out of scope to build): rules are indexed by
(entity_type, entity_id | *) so an event touches only its own rules; state is
per (rule, entity) and tiny, so it partitions cleanly by org -> entity and
lives happily in memory/Redis with the stream consumer. Evaluation cost is
O(rules matching the entity), never O(all rules).
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional

from .models import (Condition, EntityType, Notification, NotificationKind,
                     Rule, Severity)
from .world import World

_id_counter = itertools.count(1)


def _next_id() -> str:
    return f"ntf_{next(_id_counter):04d}"


def compare(value: float, comparator: str, threshold: float) -> bool:
    return {
        ">":  value > threshold,
        ">=": value >= threshold,
        "<":  value < threshold,
        "<=": value <= threshold,
        "==": value == threshold,
    }[comparator]


@dataclass
class AlertState:
    status: str = "OK"                      # OK | PENDING | FIRING
    pending_since: Optional[datetime] = None
    firing_since: Optional[datetime] = None
    last_audible_activity: Optional[datetime] = None   # cooldown anchor: last audible fire OR resolve
    fire_was_suppressed: bool = False
    escalated: bool = False


class Engine:
    def __init__(self, world: World, rules: list[Rule],
                 sink: Callable[[Notification], None],
                 tick_interval_sec: int = 30) -> None:
        self.world = world
        self.rules = [r for r in rules if r.enabled]
        self.sink = sink                     # routing/delivery, injected
        self.tick_interval = timedelta(seconds=tick_interval_sec)
        self.states: dict[tuple[str, str], AlertState] = {}
        self._clock: Optional[datetime] = None   # event-time watermark
        self._last_tick: Optional[datetime] = None

    # ---------------------------------------------------------------- intake
    def process_event(self, event: dict) -> None:
        """Advance time, apply an accepted event, evaluate affected rules.

        Order matters: the clock advances (running intermediate ticks against
        the PRE-event world) before the event is applied. Applying first was a
        real bug caught in replay verification — ticks between the previous
        event and this one would evaluate against data "from the future" and
        resolve alerts minutes before the resolving snapshot existed.
        """
        ts: datetime = event["ts"]
        # Event-time clock only moves forward (late events don't rewind time).
        if self._clock is None or ts > self._clock:
            self._advance_clock(ts)
        self.world.apply(event)

        entity_type, entity_id = self._entity_of(event)
        for rule in self.rules:
            if rule.entity_type == entity_type and rule.applies_to(entity_id):
                self._evaluate(rule, entity_id, self._clock)

    def _advance_clock(self, ts: datetime) -> None:
        """Move simulated time to ts, running ticks at tick_interval along
        the way so duration-based rules fire close to when they crossed the
        line, not whenever the next event happens to show up."""
        if self._clock is None:
            self._clock = ts
            self._last_tick = ts
            return
        nxt = self._last_tick + self.tick_interval
        while nxt <= ts:
            self._tick(nxt)
            self._last_tick = nxt
            nxt = self._last_tick + self.tick_interval
        self._clock = ts

    def _tick(self, now: datetime) -> None:
        """Time-driven evaluation: duration metrics, PENDING promotion,
        escalations. At this scale we sweep all known entities; production
        would keep a timer wheel of (rule, entity) deadlines instead."""
        for rule in self.rules:
            entities = (self.world.queues if rule.entity_type == EntityType.QUEUE
                        else self.world.agents)
            for entity_id in list(entities):
                if rule.applies_to(entity_id):
                    self._evaluate(rule, entity_id, now)

    @staticmethod
    def _entity_of(event: dict) -> tuple[EntityType, str]:
        if event["type"] == "queue_snapshot":
            return EntityType.QUEUE, event["queue_id"]
        return EntityType.AGENT, event["agent_id"]

    # ------------------------------------------------------------ evaluation
    def _evaluate(self, rule: Rule, entity_id: str, now: datetime) -> None:
        st = self.states.setdefault((rule.id, entity_id), AlertState())
        values: list[Optional[float]] = [
            self.world.resolve_metric(c, rule.entity_type.value, entity_id, now)
            for c in rule.conditions
        ]
        if any(v is None for v in values):
            return   # unknown data: never guess, never auto-resolve

        active = all(compare(v, c.comparator, c.threshold)
                     for v, c in zip(values, rule.conditions))

        if st.status == "FIRING":
            # Hysteresis: resolution is judged against clear thresholds.
            still = all(compare(v, c.comparator, c.effective_clear_threshold())
                        for v, c in zip(values, rule.conditions))
            if not still:
                self._resolve(rule, entity_id, st, now, values)
            else:
                self._maybe_escalate(rule, entity_id, st, now, values)
            return

        if not active:
            st.status = "OK"
            st.pending_since = None
            return

        if st.status == "OK":
            st.status = "PENDING"
            st.pending_since = now
        if st.status == "PENDING":
            held = (now - st.pending_since).total_seconds()
            if held >= rule.sustained_for_sec:
                self._fire(rule, entity_id, st, now, values)

    def _fire(self, rule: Rule, entity_id: str, st: AlertState,
              now: datetime, values: list[float]) -> None:
        st.status = "FIRING"
        st.firing_since = now
        st.pending_since = None
        st.escalated = False
        # Cooldown anchors on the last *audible activity* (fire or resolve):
        # a rule that resolved 60s ago and re-trips is a flap, and the human
        # was just told things recovered — suppress the whiplash. Verified
        # against billing's 10:15 recover / 10:16 re-trip in the sample.
        in_cooldown = (st.last_audible_activity is not None and
                       (now - st.last_audible_activity).total_seconds() < rule.cooldown_sec)
        st.fire_was_suppressed = in_cooldown
        if not in_cooldown:
            st.last_audible_activity = now
        self.sink(self._notification(NotificationKind.FIRED, rule, entity_id,
                                     now, values, suppressed=in_cooldown))

    def _resolve(self, rule: Rule, entity_id: str, st: AlertState,
                 now: datetime, values: list[float]) -> None:
        duration = (now - st.firing_since).total_seconds() if st.firing_since else 0
        suppressed = st.fire_was_suppressed   # quiet fire => quiet resolve
        st.status = "OK"
        st.firing_since = None
        st.escalated = False
        if not suppressed:
            st.last_audible_activity = now
        n = self._notification(NotificationKind.RESOLVED, rule, entity_id, now,
                               values, suppressed=suppressed)
        n.message += f" (was active {_fmt_dur(duration)})"
        self.sink(n)

    def _maybe_escalate(self, rule: Rule, entity_id: str, st: AlertState,
                        now: datetime, values: list[float]) -> None:
        if (rule.escalate_after_sec is None or st.escalated
                or st.firing_since is None or st.fire_was_suppressed):
            return
        if (now - st.firing_since).total_seconds() >= rule.escalate_after_sec:
            st.escalated = True
            n = self._notification(NotificationKind.ESCALATED, rule, entity_id,
                                   now, values)
            n.message = (f"ESCALATION: '{rule.name}' on {entity_id} unresolved "
                         f"for {_fmt_dur((now - st.firing_since).total_seconds())}. "
                         + n.message)
            self.sink(n)

    # --------------------------------------------------------------- message
    def _notification(self, kind: NotificationKind, rule: Rule, entity_id: str,
                      now: datetime, values: list[float],
                      suppressed: bool = False) -> Notification:
        if kind == NotificationKind.RESOLVED:
            # "recovered: Single call over 45m — a_11: on_call for 0s" reads
            # as nonsense; resolved messages carry the rule name + duration
            # (appended by _resolve), not the post-recovery metric values —
            # except queue metrics, where the recovered value is the news.
            detail = ", ".join(
                _describe(c, v) for c, v in zip(rule.conditions, values)
                if c.metric not in ("state_duration_sec", "adherence_violation_sec")
            )
            msg = f"recovered: {rule.name} — {entity_id}"
            if detail:
                msg += f": {detail}"
        else:
            detail = ", ".join(
                _describe(c, v) for c, v in zip(rule.conditions, values)
            )
            msg = f"{rule.name} — {entity_id}: {detail}"
        return Notification(
            id=_next_id(), ts=now, kind=kind, rule_id=rule.id,
            rule_name=rule.name, severity=rule.severity,
            entity_type=rule.entity_type, entity_id=entity_id,
            message=msg,
            recipients=[],           # filled in by routing
            suppressed=suppressed,
        )


def _describe(c: Condition, v: float) -> str:
    if c.metric in ("state_duration_sec", "adherence_violation_sec"):
        label = f"{c.state_filter or 'violation'} for {_fmt_dur(v)}"
        return label
    if c.metric == "sla_utilization":
        return f"wait at {v:.0%} of SLA"
    if c.metric == "volume_vs_forecast":
        return f"volume at {v:.0%} of forecast"
    return f"{c.metric}={v:g}"


def _fmt_dur(sec: float) -> str:
    m, s = divmod(int(sec), 60)
    return f"{m}m{s:02d}s" if m else f"{s}s"
