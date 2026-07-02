"""Repo-layer tests: round-trip fidelity (dataclass -> row -> dataclass),
filtering, and the seed/update/delete contract. Uses an in-memory SQLite DB
(':memory:') so tests are fast and never touch disk.
"""
from datetime import datetime, timezone

from app.db import NotificationRepo, RuleRepo, connect
from app.models import (Condition, EntityType, Notification,
                        NotificationKind, Rule, Severity)
from app.rules_default import DEFAULT_RULES


def db():
    return connect(":memory:")


def sample_rule(id="r1", **overrides) -> Rule:
    defaults = dict(
        id=id, name="Queue approaching SLA", entity_type=EntityType.QUEUE,
        conditions=[Condition("sla_utilization", ">=", 0.8)],
        severity=Severity.WARNING, recipients=["team_lead"],
        entity_ids=["billing"], sustained_for_sec=60, cooldown_sec=900,
        escalate_after_sec=None, escalate_to=[], enabled=True,
    )
    defaults.update(overrides)
    return Rule(**defaults)


def sample_notification(**overrides) -> Notification:
    defaults = dict(
        id="ntf_1", ts=datetime(2026, 5, 26, 9, 30, tzinfo=timezone.utc),
        kind=NotificationKind.FIRED, rule_id="r1", rule_name="Queue approaching SLA",
        severity=Severity.WARNING, entity_type=EntityType.QUEUE,
        entity_id="billing", message="wait at 92% of SLA",
        recipients=["lead_priya"], suppressed=False,
    )
    defaults.update(overrides)
    return Notification(**defaults)


# -------------------------------------------------------------------- rules
def test_rule_round_trips_exactly():
    conn = db()
    repo = RuleRepo(conn)
    rule = sample_rule()
    repo.create(rule)
    back = repo.get("r1")
    assert back == rule


def test_rule_with_no_entity_scope_and_multi_condition_round_trips():
    conn = db()
    repo = RuleRepo(conn)
    rule = sample_rule(
        id="r2", entity_ids=None,   # "all queues"
        conditions=[Condition("agents_available", "<=", 0),
                    Condition("tickets_waiting", ">=", 10)],
        escalate_after_sec=900, escalate_to=["head_of_support"],
    )
    repo.create(rule)
    back = repo.get("r2")
    assert back.entity_ids is None
    assert len(back.conditions) == 2
    assert back.escalate_after_sec == 900
    assert back.escalate_to == ["head_of_support"]


def test_get_missing_rule_returns_none():
    assert RuleRepo(db()).get("nope") is None


def test_list_orders_by_created_and_filters_enabled():
    conn = db()
    repo = RuleRepo(conn)
    repo.create(sample_rule(id="r1"))
    repo.create(sample_rule(id="r2", enabled=False))
    assert [r.id for r in repo.list()] == ["r1", "r2"]
    assert [r.id for r in repo.list(enabled_only=True)] == ["r1"]


def test_update_replaces_fields_but_keeps_id():
    conn = db()
    repo = RuleRepo(conn)
    repo.create(sample_rule())
    edited = sample_rule(name="Queue REALLY approaching SLA",
                         conditions=[Condition("sla_utilization", ">=", 0.9)])
    assert repo.update(edited) is True
    back = repo.get("r1")
    assert back.name == "Queue REALLY approaching SLA"
    assert back.conditions[0].threshold == 0.9


def test_update_missing_rule_returns_false():
    assert RuleRepo(db()).update(sample_rule()) is False


def test_set_enabled_toggles():
    conn = db()
    repo = RuleRepo(conn)
    repo.create(sample_rule())
    assert repo.set_enabled("r1", False) is True
    assert repo.get("r1").enabled is False
    assert repo.set_enabled("missing", False) is False


def test_delete_removes_row():
    conn = db()
    repo = RuleRepo(conn)
    repo.create(sample_rule())
    assert repo.delete("r1") is True
    assert repo.get("r1") is None
    assert repo.delete("r1") is False   # already gone


def test_seed_defaults_only_populates_empty_table():
    conn = db()
    repo = RuleRepo(conn)
    n = repo.seed_defaults_if_empty(DEFAULT_RULES)
    assert n == len(DEFAULT_RULES)
    assert len(repo.list()) == len(DEFAULT_RULES)
    # editing then re-seeding must not clobber the edit
    edited = repo.get(DEFAULT_RULES[0].id)
    edited.name = "customized by a user"
    repo.update(edited)
    again = repo.seed_defaults_if_empty(DEFAULT_RULES)
    assert again == 0
    assert repo.get(DEFAULT_RULES[0].id).name == "customized by a user"


# ------------------------------------------------------------- notifications
def test_notification_round_trips_exactly():
    conn = db()
    repo = NotificationRepo(conn)
    n = sample_notification()
    repo.insert(n)
    back = repo.list()[0]
    assert back == n


def test_notification_filters():
    conn = db()
    repo = NotificationRepo(conn)
    repo.insert(sample_notification(id="n1", entity_id="billing",
                                    recipients=["lead_priya"]))
    repo.insert(sample_notification(id="n2", entity_id="tier_2",
                                    recipients=["lead_marcus"], suppressed=True))
    assert [n.id for n in repo.list(entity_id="billing")] == ["n1"]
    assert [n.id for n in repo.list(recipient="lead_marcus")] == ["n2"]
    assert [n.id for n in repo.list(include_suppressed=False)] == ["n1"]


def test_notification_time_range_filter():
    conn = db()
    repo = NotificationRepo(conn)
    early = sample_notification(id="n1", ts=datetime(2026, 5, 26, 9, 0, tzinfo=timezone.utc))
    late = sample_notification(id="n2", ts=datetime(2026, 5, 26, 10, 0, tzinfo=timezone.utc))
    repo.insert(early); repo.insert(late)
    only_late = repo.list(since=datetime(2026, 5, 26, 9, 30, tzinfo=timezone.utc))
    assert [n.id for n in only_late] == ["n2"]


def test_notification_ordering_is_newest_first():
    conn = db()
    repo = NotificationRepo(conn)
    repo.insert(sample_notification(id="n1", ts=datetime(2026, 5, 26, 9, 0, tzinfo=timezone.utc)))
    repo.insert(sample_notification(id="n2", ts=datetime(2026, 5, 26, 10, 0, tzinfo=timezone.utc)))
    assert [n.id for n in repo.list()] == ["n2", "n1"]


def test_clear_empties_table():
    conn = db()
    repo = NotificationRepo(conn)
    repo.insert(sample_notification())
    repo.clear()
    assert repo.list() == []
