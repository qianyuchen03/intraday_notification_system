"""Tests for app/rule_validation.py
"""
from types import SimpleNamespace as NS

from app.models import EntityType
from app.rule_validation import slugify, validate_rule_input


def cond(**kw):
    defaults = dict(metric="sla_utilization", comparator=">=", threshold=0.8,
                    state_filter=None, clear_threshold=None)
    defaults.update(kw)
    return NS(**defaults)


def rule_in(**kw):
    defaults = dict(
        name="Queue approaching SLA", entity_type=EntityType.QUEUE,
        conditions=[cond()], recipients=["team_lead"],
        sustained_for_sec=0, cooldown_sec=900,
        escalate_after_sec=None, escalate_to=[],
    )
    defaults.update(kw)
    return NS(**defaults)


def test_valid_rule_has_no_errors():
    assert validate_rule_input(rule_in()) == []


def test_empty_name_rejected():
    errors = validate_rule_input(rule_in(name="   "))
    assert any("name" in e for e in errors)


def test_no_conditions_rejected():
    errors = validate_rule_input(rule_in(conditions=[]))
    assert any("condition is required" in e for e in errors)


def test_metric_not_valid_for_entity_type_rejected():
    # adherence_violation_sec is an AGENT metric, rule is QUEUE-scoped
    bad = rule_in(conditions=[cond(metric="adherence_violation_sec")])
    errors = validate_rule_input(bad)
    assert any("not valid for entity_type 'queue'" in e for e in errors)


def test_agent_metric_valid_for_agent_rule():
    ok = rule_in(entity_type=EntityType.AGENT,
                 conditions=[cond(metric="adherence_violation_sec", threshold=600)])
    assert validate_rule_input(ok) == []


def test_bad_comparator_rejected():
    errors = validate_rule_input(rule_in(conditions=[cond(comparator="!=")]))
    assert any("comparator" in e for e in errors)


def test_state_duration_without_state_filter_rejected():
    bad = rule_in(entity_type=EntityType.AGENT,
                 conditions=[cond(metric="state_duration_sec", threshold=2700,
                                  state_filter=None)])
    errors = validate_rule_input(bad)
    assert any("requires state_filter" in e for e in errors)


def test_state_duration_with_state_filter_ok():
    ok = rule_in(entity_type=EntityType.AGENT,
                conditions=[cond(metric="state_duration_sec", threshold=2700,
                                 state_filter="on_call")])
    assert validate_rule_input(ok) == []


def test_empty_recipients_rejected():
    errors = validate_rule_input(rule_in(recipients=[]))
    assert any("recipients" in e for e in errors)


def test_blank_recipient_string_rejected():
    errors = validate_rule_input(rule_in(recipients=["  "]))
    assert any("recipients" in e for e in errors)


def test_negative_sustained_for_rejected():
    errors = validate_rule_input(rule_in(sustained_for_sec=-1))
    assert any("sustained_for_sec" in e for e in errors)


def test_negative_cooldown_rejected():
    errors = validate_rule_input(rule_in(cooldown_sec=-1))
    assert any("cooldown_sec" in e for e in errors)


def test_escalation_without_escalate_to_rejected():
    errors = validate_rule_input(rule_in(escalate_after_sec=900, escalate_to=[]))
    assert any("escalate_to" in e for e in errors)


def test_escalation_with_escalate_to_ok():
    ok = rule_in(escalate_after_sec=900, escalate_to=["head_of_support"])
    assert validate_rule_input(ok) == []


def test_negative_escalate_after_rejected():
    errors = validate_rule_input(
        rule_in(escalate_after_sec=-5, escalate_to=["head_of_support"]))
    assert any("escalate_after_sec" in e for e in errors)


def test_multiple_conditions_each_validated_independently():
    bad = rule_in(conditions=[cond(), cond(comparator="bogus")])
    errors = validate_rule_input(bad)
    assert len(errors) == 1
    assert "conditions[1]" in errors[0]


def test_errors_accumulate_dont_short_circuit():
    bad = rule_in(name="", recipients=[], sustained_for_sec=-1)
    errors = validate_rule_input(bad)
    assert len(errors) >= 3


# ------------------------------------------------------------------ slugify
def test_slugify_basic():
    assert slugify("Queue approaching SLA") == "queue_approaching_sla"


def test_slugify_strips_punctuation():
    assert slugify("Agent's call > 45 min!!") == "agent_s_call_45_min"


def test_slugify_empty_falls_back():
    assert slugify("   ") == "rule"