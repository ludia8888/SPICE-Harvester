from __future__ import annotations

from datetime import datetime, timezone

from shared.models.event_envelope import EventEnvelope
from shared.services.core.projection_consistency import (
    build_instance_expectations,
    evaluate_projection_document,
    parse_instance_aggregate_id,
)


def _env(
    *,
    event_id: str,
    event_type: str,
    aggregate_id: str,
    sequence_number: int | None,
    occurred_at: datetime,
    data: dict,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id,
        event_type=event_type,
        aggregate_type="Instance",
        aggregate_id=aggregate_id,
        occurred_at=occurred_at,
        actor="tester",
        data=data,
        metadata={"kind": "domain"},
        sequence_number=sequence_number,
    )


def test_parse_instance_aggregate_id_handles_instance_id_with_colon() -> None:
    key = parse_instance_aggregate_id("db1:main:Customer:user:123")
    assert key is not None
    assert key.db_name == "db1"
    assert key.branch == "main"
    assert key.class_id == "Customer"
    assert key.instance_id == "user:123"


def test_build_instance_expectations_prefers_higher_sequence_number() -> None:
    events = [
        _env(
            event_id="e1",
            event_type="INSTANCE_UPDATED",
            aggregate_id="db1:main:Customer:c1",
            sequence_number=10,
            occurred_at=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
            data={"db_name": "db1", "branch": "main", "class_id": "Customer", "instance_id": "c1"},
        ),
        _env(
            event_id="e2",
            event_type="INSTANCE_DELETED",
            aggregate_id="db1:main:Customer:c1",
            sequence_number=11,
            occurred_at=datetime(2026, 2, 15, 10, 1, tzinfo=timezone.utc),
            data={"db_name": "db1", "branch": "main", "class_id": "Customer", "instance_id": "c1"},
        ),
    ]

    result = build_instance_expectations(events)
    assert len(result.expectations) == 1
    expectation = next(iter(result.expectations.values()))
    assert expectation.latest_event_id == "e2"
    assert expectation.latest_event_type == "INSTANCE_DELETED"
    assert expectation.should_exist is False


def test_build_instance_expectations_uses_aggregate_id_fallback() -> None:
    events = [
        _env(
            event_id="e1",
            event_type="INSTANCE_CREATED",
            aggregate_id="db1:main:Customer:c1",
            sequence_number=1,
            occurred_at=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
            data={},
        )
    ]

    result = build_instance_expectations(events)
    expectation = next(iter(result.expectations.values()))
    assert expectation.key.db_name == "db1"
    assert expectation.key.branch == "main"
    assert expectation.key.class_id == "Customer"
    assert expectation.key.instance_id == "c1"


def test_evaluate_projection_document_reports_missing_document() -> None:
    events = [
        _env(
            event_id="e1",
            event_type="INSTANCE_CREATED",
            aggregate_id="db1:main:Customer:c1",
            sequence_number=1,
            occurred_at=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
            data={"db_name": "db1", "branch": "main", "class_id": "Customer", "instance_id": "c1"},
        )
    ]
    expectation = next(iter(build_instance_expectations(events).expectations.values()))
    assert evaluate_projection_document(expectation, None) == "missing_document"


def test_evaluate_projection_document_detects_deleted_document_present() -> None:
    events = [
        _env(
            event_id="e1",
            event_type="INSTANCE_DELETED",
            aggregate_id="db1:main:Customer:c1",
            sequence_number=2,
            occurred_at=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
            data={"db_name": "db1", "branch": "main", "class_id": "Customer", "instance_id": "c1"},
        )
    ]
    expectation = next(iter(build_instance_expectations(events).expectations.values()))
    reason = evaluate_projection_document(
        expectation,
        {"instance_id": "c1", "class_id": "Customer", "db_name": "db1", "branch": "main"},
    )
    assert reason == "deleted_document_present"
