"""Engine state-machine tests.

Strategy: drive the engine with tiny synthetic event sequences, assert on the
exact notifications produced. Each test isolates one noise-control behavior;
the end-to-end interaction of all of them is covered by the golden replay
test against events.jsonl.
"""
from datetime import datetime, timezone

from app.engine import Engine
from app.models import (Condition, EntityType, NotificationKind, Rule,
                        Severity)
from app.world import World


def T(minute: int, second: int = 0) -> str:
    return f"2026-05-26T09:{minute:02d}:{second:02d}+00:00"


def snapshot(ts, queue_id="billing", longest=0, sla=120, waiting=0, avail=4,
             vol=10, forecast=20):
    return {"type": "queue_snapshot", "ts": datetime.fromisoformat(ts),
            "queue_id": queue_id, "tickets_waiting": waiting,
            "longest_wait_sec": longest, "sla_target_sec": sla,
            "agents_available": avail, "agents_on_call": 0,
            "volume_last_15m": vol, "volume_forecast_next_15m": forecast}


def state_change(ts, agent_id="a_1", new_state="on_call", queue_ids=("billing",)):
    return {"type": "agent_state_change", "ts": datetime.fromisoformat(ts),
            "agent_id": agent_id, "queue_ids": list(queue_ids),
            "previous_state": "available", "previous_state_duration_sec": 60,
            "new_state": new_state}


def make(rule) -> tuple[Engine, list]:
    out = []
    eng = Engine(World(), [rule], sink=out.append)
    return eng, out


def audible(out):
    return [n for n in out if not n.suppressed]


# ------------------------------------------------------------------ basics
def test_fire_and_resolve_with_messages():
    rule = Rule(id="r", name="breach", entity_type=EntityType.QUEUE,
                conditions=[Condition("sla_utilization", ">=", 1.0)],
                severity=Severity.CRITICAL, recipients=["team_lead"],
                cooldown_sec=0)
    eng, out = make(rule)
    eng.process_event(snapshot(T(0), longest=60))     # 0.5x: quiet
    eng.process_event(snapshot(T(1), longest=130))    # 1.08x: fire
    eng.process_event(snapshot(T(2), longest=30))     # 0.25x: resolve
    kinds = [n.kind for n in out]
    assert kinds == [NotificationKind.FIRED, NotificationKind.RESOLVED]
    assert "was active 1m00s" in out[1].message


def test_sustained_for_swallows_blips():
    rule = Rule(id="r", name="waiting high", entity_type=EntityType.QUEUE,
                conditions=[Condition("tickets_waiting", ">=", 20)],
                severity=Severity.WARNING, recipients=[],
                sustained_for_sec=120, cooldown_sec=0)
    eng, out = make(rule)
    eng.process_event(snapshot(T(0), waiting=25))   # trips -> PENDING
    eng.process_event(snapshot(T(1), waiting=5))    # back down before 120s
    assert out == []                                # blip never fired
    eng.process_event(snapshot(T(2), waiting=25))   # trips again
    eng.process_event(snapshot(T(5), waiting=25))   # held >= 120s
    assert [n.kind for n in audible(out)] == [NotificationKind.FIRED]
    # fired at the tick where the hold time was met, not at the later event
    assert audible(out)[0].ts.isoformat().endswith("09:04:00+00:00")


def test_hysteresis_clear_threshold():
    rule = Rule(id="r", name="breach", entity_type=EntityType.QUEUE,
                conditions=[Condition("sla_utilization", ">=", 1.0,
                                      clear_threshold=0.95)],
                severity=Severity.CRITICAL, recipients=[], cooldown_sec=0)
    eng, out = make(rule)
    eng.process_event(snapshot(T(0), longest=130))   # 1.08 fire
    eng.process_event(snapshot(T(1), longest=118))   # 0.98 >= 0.95: still firing
    assert len(out) == 1
    eng.process_event(snapshot(T(2), longest=110))   # 0.92 < 0.95: resolve
    assert [n.kind for n in out] == [NotificationKind.FIRED,
                                     NotificationKind.RESOLVED]


def test_cooldown_suppresses_flap_and_records_it():
    # Mirrors billing 10:15 recover -> 10:16 re-trip in the sample.
    rule = Rule(id="r", name="at risk", entity_type=EntityType.QUEUE,
                conditions=[Condition("sla_utilization", ">=", 0.8)],
                severity=Severity.WARNING, recipients=[], cooldown_sec=900)
    eng, out = make(rule)
    eng.process_event(snapshot(T(0), longest=100))   # 0.83 fire (audible)
    eng.process_event(snapshot(T(5), longest=80))    # 0.67 resolve (audible)
    eng.process_event(snapshot(T(6), longest=110))   # 0.92 re-trip 60s later
    assert len(out) == 3
    assert out[2].kind == NotificationKind.FIRED and out[2].suppressed
    assert len(audible(out)) == 2
    # ...and the quiet fire resolves quietly
    eng.process_event(snapshot(T(7), longest=10))
    assert out[3].kind == NotificationKind.RESOLVED and out[3].suppressed


def test_and_conditions():
    rule = Rule(id="r", name="understaffed", entity_type=EntityType.QUEUE,
                conditions=[Condition("agents_available", "<=", 0),
                            Condition("tickets_waiting", ">=", 10)],
                severity=Severity.WARNING, recipients=[], cooldown_sec=0)
    eng, out = make(rule)
    eng.process_event(snapshot(T(0), avail=0, waiting=5))    # only one holds
    eng.process_event(snapshot(T(1), avail=2, waiting=15))   # only one holds
    assert out == []
    eng.process_event(snapshot(T(2), avail=0, waiting=15))   # both hold
    assert [n.kind for n in out] == [NotificationKind.FIRED]


# ------------------------------------------------- time-driven evaluation
def test_long_call_fires_from_tick_with_no_events():
    """The 45-minute-call rule is undetectable from events alone: nothing is
    emitted mid-call. The engine's tick must catch it as simulated time
    passes (here: time is advanced by unrelated snapshots)."""
    rule = Rule(id="r", name="long call", entity_type=EntityType.AGENT,
                conditions=[Condition("state_duration_sec", ">=", 2700,
                                      state_filter="on_call")],
                severity=Severity.WARNING, recipients=[], cooldown_sec=0)
    eng, out = make(rule)
    eng.process_event(state_change(T(0), new_state="on_call"))
    eng.process_event(snapshot(T(30)))          # unrelated queue event
    assert out == []                            # 30m: not yet
    eng.process_event(snapshot("2026-05-26T09:50:00+00:00"))
    assert [n.kind for n in out] == [NotificationKind.FIRED]
    assert out[0].ts.isoformat().endswith("09:45:00+00:00")   # tick-accurate
    # call ends -> resolve
    eng.process_event(state_change("2026-05-26T09:52:00+00:00",
                                   new_state="available"))
    assert out[1].kind == NotificationKind.RESOLVED


def test_escalation_after_unresolved_period():
    rule = Rule(id="r", name="breach", entity_type=EntityType.QUEUE,
                conditions=[Condition("sla_utilization", ">=", 1.0)],
                severity=Severity.CRITICAL, recipients=["team_lead"],
                cooldown_sec=0, escalate_after_sec=900,
                escalate_to=["head_of_support"])
    eng, out = make(rule)
    eng.process_event(snapshot(T(0), longest=130))
    eng.process_event(snapshot(T(20), longest=140))   # still breaching at 20m
    kinds = [n.kind for n in out]
    assert kinds == [NotificationKind.FIRED, NotificationKind.ESCALATED]
    assert out[1].ts.isoformat().endswith("09:15:00+00:00")
    eng.process_event(snapshot(T(21), longest=150))   # no repeat escalation
    assert len(out) == 2


# -------------------------------------------------------- messy-data cases
def test_null_forecast_skips_rule_without_crashing():
    rule = Rule(id="r", name="over forecast", entity_type=EntityType.QUEUE,
                conditions=[Condition("volume_vs_forecast", ">=", 1.5)],
                severity=Severity.WARNING, recipients=[], cooldown_sec=0)
    eng, out = make(rule)
    eng.process_event(snapshot(T(0), vol=30, forecast=None))   # the 10:00 trap
    assert out == []


def test_missing_data_does_not_resolve_a_firing_alert():
    """Silence is not recovery: a firing SLA alert must stay firing when the
    queue stops reporting (vip goes 75 minutes without a snapshot)."""
    rule = Rule(id="r", name="breach", entity_type=EntityType.QUEUE,
                conditions=[Condition("sla_utilization", ">=", 1.0)],
                severity=Severity.CRITICAL, recipients=[], cooldown_sec=0)
    eng, out = make(rule)
    eng.process_event(snapshot(T(0), longest=130))
    # 40 minutes of ticks driven by a DIFFERENT queue's events
    eng.process_event(snapshot(T(40), queue_id="vip", longest=0, sla=60))
    assert [n.kind for n in out] == [NotificationKind.FIRED]   # no resolve


def test_adherence_null_start_falls_back_to_observation_time():
    rule = Rule(id="r", name="adherence", entity_type=EntityType.AGENT,
                conditions=[Condition("adherence_violation_sec", ">=", 600)],
                severity=Severity.WARNING, recipients=[], cooldown_sec=0)
    eng, out = make(rule)
    check = {"type": "adherence_check", "ts": datetime.fromisoformat(T(0)),
             "agent_id": "a_23", "queue_ids": ["tier_2"],
             "scheduled_state": "available", "actual_state": "in_meeting",
             "in_violation": True, "violation_started_at": None}   # the trap
    eng.process_event(check)
    eng.process_event(snapshot(T(9)))
    assert out == []                    # 9m since observation: conservative
    eng.process_event(snapshot(T(12)))
    assert [n.kind for n in out] == [NotificationKind.FIRED]


def test_adherence_state_reconciliation_prevents_phantom_long_call():
    """a_23 in the sample: adherence says in_meeting but no state_change event
    ever arrived. Without reconciliation the agent looks on_call forever and
    a phantom 45m-call alert fires."""
    rule = Rule(id="r", name="long call", entity_type=EntityType.AGENT,
                conditions=[Condition("state_duration_sec", ">=", 2700,
                                      state_filter="on_call")],
                severity=Severity.WARNING, recipients=[], cooldown_sec=0)
    eng, out = make(rule)
    eng.process_event(state_change(T(0), agent_id="a_23", new_state="on_call"))
    check = {"type": "adherence_check", "ts": datetime.fromisoformat(T(30)),
             "agent_id": "a_23", "queue_ids": ["tier_2"],
             "scheduled_state": "available", "actual_state": "in_meeting",
             "in_violation": True, "violation_started_at": T(30)}
    eng.process_event(check)
    eng.process_event(snapshot("2026-05-26T09:59:00+00:00"))   # would be 59m "on_call"
    assert out == []
