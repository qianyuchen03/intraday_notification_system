"""Tests for the PATCH/DELETE-adjacent pure logic in server.py:
  - _open_alerts_for_rule(): the FIRED/RESOLVED bookkeeping DELETE relies on
  - the PATCH merge semantics (explicit-null vs omitted, list replacement)

Imports directly from app.server, which requires fastapi/pydantic to be
installed
"""
from datetime import datetime, timezone

import pytest

fastapi = pytest.importorskip("fastapi", reason="fastapi not installed")

from app.models import EntityType, Notification, NotificationKind, Severity
from app.notify import Notifier
from app.server import _NULLABLE_PATCH_FIELDS, _open_alerts_for_rule


def ntf(entity_id, kind, rule_id="r1", recipients=("someone",), suppressed=False,
       ts=None):
    return Notification(
        id=f"ntf_{entity_id}_{kind.value}", ts=ts or datetime.now(timezone.utc),
        kind=kind, rule_id=rule_id, rule_name="test rule",
        severity=Severity.WARNING, entity_type=EntityType.AGENT,
        entity_id=entity_id, message="msg", recipients=list(recipients),
        suppressed=suppressed,
    )


def notifier_with(*events) -> Notifier:
    n = Notifier(log_path=None, console=False)
    n.store = list(events)
    return n


# ---------------------------------------------------- _open_alerts_for_rule
def test_fired_without_resolved_is_open():
    n = notifier_with(ntf("a_1", NotificationKind.FIRED))
    open_map = _open_alerts_for_rule(n, "r1")
    assert set(open_map) == {"a_1"}


def test_fired_then_resolved_is_closed():
    n = notifier_with(ntf("a_1", NotificationKind.FIRED),
                      ntf("a_1", NotificationKind.RESOLVED))
    assert _open_alerts_for_rule(n, "r1") == {}


def test_multiple_entities_tracked_independently():
    n = notifier_with(
        ntf("a_1", NotificationKind.FIRED),
        ntf("a_2", NotificationKind.FIRED),
        ntf("a_1", NotificationKind.RESOLVED),
    )
    assert set(_open_alerts_for_rule(n, "r1")) == {"a_2"}


def test_other_rule_ids_ignored():
    n = notifier_with(ntf("a_1", NotificationKind.FIRED, rule_id="other_rule"))
    assert _open_alerts_for_rule(n, "r1") == {}


def test_suppressed_fire_still_counts_as_open():
    # Suppression only affects whether a human was pinged, not whether the
    # underlying AlertState was FIRING — a quietly-firing alert still needs
    # closing on delete.
    n = notifier_with(ntf("a_1", NotificationKind.FIRED, suppressed=True))
    assert set(_open_alerts_for_rule(n, "r1")) == {"a_1"}


def test_refire_after_resolve_reopens():
    n = notifier_with(
        ntf("a_1", NotificationKind.FIRED),
        ntf("a_1", NotificationKind.RESOLVED),
        ntf("a_1", NotificationKind.FIRED),
    )
    assert set(_open_alerts_for_rule(n, "r1")) == {"a_1"}


def test_open_map_value_is_the_fired_notification_for_recipients():
    fired = ntf("a_1", NotificationKind.FIRED, recipients=["a_1", "lead_x"])
    n = notifier_with(fired)
    open_map = _open_alerts_for_rule(n, "r1")
    assert open_map["a_1"].recipients == ["a_1", "lead_x"]


# ------------------------------------------------------------- PATCH policy
def test_nullable_patch_fields_are_exactly_entity_ids_and_escalate_after():
    assert _NULLABLE_PATCH_FIELDS == {"entity_ids", "escalate_after_sec"}