"""Tests for build_digest(). Two layers: hand-crafted scenarios isolating one
behavior each, then a real replay of events.jsonl
"""
from datetime import datetime, timedelta, timezone

from app.models import EntityType, Notification, NotificationKind, Severity
from app.replay import run_replay
from app.routing import build_digest

BASE = datetime(2026, 5, 26, 9, 0, tzinfo=timezone.utc)


def ntf(rule_id, rule_name, entity_id, kind, minute_offset, severity=Severity.WARNING,
       entity_type=EntityType.QUEUE, suppressed=False):
    return Notification(
        id=f"n_{rule_id}_{entity_id}_{kind.value}_{minute_offset}",
        ts=BASE + timedelta(minutes=minute_offset), kind=kind, rule_id=rule_id,
        rule_name=rule_name, severity=severity, entity_type=entity_type,
        entity_id=entity_id, message="msg", recipients=[], suppressed=suppressed,
    )


SINCE = BASE
UNTIL = BASE + timedelta(minutes=90)


def test_empty_period_is_all_quiet_not_no_digest():
    d = build_digest([], SINCE, UNTIL)
    assert d == "All quiet — no notifications between 09:00 UTC and 10:30 UTC."


def test_closed_alert_reports_duration():
    notifs = [
        ntf("sla_breach", "Queue breaching SLA", "billing", NotificationKind.FIRED, 30, severity=Severity.CRITICAL),
        ntf("sla_breach", "Queue breaching SLA", "billing", NotificationKind.RESOLVED, 75, severity=Severity.CRITICAL),
    ]
    d = build_digest(notifs, SINCE, UNTIL)
    assert "1× Queue breaching SLA (billing 45m00s)" in d
    assert d.endswith("All clear as of 10:30 UTC.")


def test_open_alert_reports_ongoing_and_closing_line():
    notifs = [ntf("adherence_10m", "Out of adherence 10m+", "a_88", NotificationKind.FIRED, 10,
                  entity_type=EntityType.AGENT)]
    d = build_digest(notifs, SINCE, UNTIL)
    assert "1× Out of adherence 10m+ (a_88 ongoing)" in d
    assert "1 still open at 10:30 UTC: a_88" in d


def test_critical_groups_sort_before_warning_groups():
    notifs = [
        ntf("adherence_10m", "Out of adherence 10m+", "a_88", NotificationKind.FIRED, 5,
           entity_type=EntityType.AGENT),  # warning, fires first chronologically
        ntf("sla_breach", "Queue breaching SLA", "billing", NotificationKind.FIRED, 30, severity=Severity.CRITICAL),
    ]
    d = build_digest(notifs, SINCE, UNTIL)
    assert d.index("Queue breaching SLA") < d.index("Out of adherence")


def test_escalations_reported_first_and_separately():
    notifs = [
        ntf("sla_breach", "Queue breaching SLA", "tier_2", NotificationKind.FIRED, 10, severity=Severity.CRITICAL),
        ntf("sla_breach", "Queue breaching SLA", "tier_2", NotificationKind.ESCALATED, 25, severity=Severity.CRITICAL),
        ntf("sla_breach", "Queue breaching SLA", "tier_2", NotificationKind.RESOLVED, 40, severity=Severity.CRITICAL),
    ]
    d = build_digest(notifs, SINCE, UNTIL)
    assert d.startswith("1 escalation (Queue breaching SLA on tier_2)")


def test_suppressed_alerts_still_counted():
    # The whole point of a digest: surface what cooldowns muted in real time.
    notifs = [ntf("sla_at_risk", "Queue approaching SLA", "vip", NotificationKind.FIRED, 20, suppressed=True)]
    d = build_digest(notifs, SINCE, UNTIL)
    assert "1× Queue approaching SLA (vip ongoing)" in d


def test_more_than_three_entities_truncated():
    notifs = [ntf("adherence_10m", "Out of adherence 10m+", f"a_0{i}", NotificationKind.FIRED, 10 + i,
                  entity_type=EntityType.AGENT) for i in range(5)]
    d = build_digest(notifs, SINCE, UNTIL)
    assert "+2 more" in d


def test_resolved_without_matching_fired_does_not_crash():
    # Defensive: shouldn't happen given the engine's own invariants, but a
    # digest built from a partial/windowed slice of notifications could see
    # a RESOLVED whose FIRED fell just before `since`.
    notifs = [ntf("weird_rule", "Weird Rule", "x", NotificationKind.RESOLVED, 5)]
    d = build_digest(notifs, SINCE, UNTIL)
    assert "1× Weird Rule (x 0s)" in d


def test_everything_closed_ends_with_all_clear():
    notifs = [
        ntf("sla_breach", "Queue breaching SLA", "billing", NotificationKind.FIRED, 30, severity=Severity.CRITICAL),
        ntf("sla_breach", "Queue breaching SLA", "billing", NotificationKind.RESOLVED, 75, severity=Severity.CRITICAL),
    ]
    d = build_digest(notifs, SINCE, UNTIL)
    assert d.endswith("All clear as of 10:30 UTC.")


# ---------------------------------------------------- real replay, hand-verified
def test_digest_against_real_replay_of_events_jsonl():
    """Every clause here was traced by hand against the raw notification log
    from a real run_replay('events.jsonl')
    """
    notifier, _ = run_replay("events.jsonl", log_path=None)
    since = datetime(2026, 5, 26, 9, 0, tzinfo=timezone.utc)
    until = datetime(2026, 5, 26, 10, 30, tzinfo=timezone.utc)
    d = build_digest(notifier.store, since, until)

    assert "2 escalations (Queue breaching SLA on billing, Queue breaching SLA on tier_2)" in d
    assert "2× Queue breaching SLA (billing 45m00s, tier_2 15m00s)" in d
    assert "billing 14m00s" in d          # the suppressed at-risk pair, still counted
    assert "a_88 ongoing" in d
    assert "a_23 ongoing" in d
    assert "a_07 ongoing" in d
    assert "3 still open at 10:30 UTC" in d
    assert "a_88" in d and "a_23" in d and "a_07" in d   # in the closing line