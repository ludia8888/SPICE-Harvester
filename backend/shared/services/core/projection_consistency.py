"""
Projection consistency helpers.

These helpers compare the durable event source (Event Store) with the
Elasticsearch read model for instance projections.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from shared.models.event_envelope import EventEnvelope

INSTANCE_EVENT_TYPES = frozenset(
    {
        "INSTANCE_CREATED",
        "INSTANCE_UPDATED",
        "INSTANCE_DELETED",
    }
)


@dataclass(frozen=True)
class InstanceProjectionKey:
    db_name: str
    branch: str
    class_id: str
    instance_id: str


@dataclass(frozen=True)
class InstanceProjectionExpectation:
    key: InstanceProjectionKey
    latest_event_id: str
    latest_event_type: str
    sequence_number: Optional[int]
    occurred_at: datetime
    should_exist: bool


@dataclass(frozen=True)
class ExpectationBuildResult:
    expectations: Dict[InstanceProjectionKey, InstanceProjectionExpectation]
    skipped_event_ids: list[str]


def _normalize_branch(value: Any) -> str:
    branch = str(value or "").strip()
    return branch or "main"


def _normalize_dt(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_instance_aggregate_id(aggregate_id: str) -> Optional[InstanceProjectionKey]:
    """
    Parse aggregate_id in the canonical Instance format:
      db_name:branch:class_id:instance_id
    """
    raw = str(aggregate_id or "").strip()
    if not raw:
        return None

    parts = raw.split(":", 3)
    if len(parts) != 4:
        return None

    db_name, branch, class_id, instance_id = (p.strip() for p in parts)
    if not db_name or not class_id or not instance_id:
        return None

    return InstanceProjectionKey(
        db_name=db_name,
        branch=_normalize_branch(branch),
        class_id=class_id,
        instance_id=instance_id,
    )


def key_from_instance_event(event: EventEnvelope) -> Optional[InstanceProjectionKey]:
    """
    Build an instance projection key from an EventEnvelope.
    """
    if str(event.aggregate_type or "").strip().lower() != "instance":
        return None
    if str(event.event_type or "").strip() not in INSTANCE_EVENT_TYPES:
        return None

    payload = event.data if isinstance(event.data, dict) else {}
    db_name = str(payload.get("db_name") or "").strip()
    branch = _normalize_branch(payload.get("branch"))
    class_id = str(payload.get("class_id") or "").strip()
    instance_id = str(payload.get("instance_id") or "").strip()

    if db_name and class_id and instance_id:
        return InstanceProjectionKey(
            db_name=db_name,
            branch=branch,
            class_id=class_id,
            instance_id=instance_id,
        )

    return parse_instance_aggregate_id(str(event.aggregate_id or ""))


def _event_order_key(event: EventEnvelope) -> tuple[int, float, str]:
    sequence_number = event.sequence_number
    seq = int(sequence_number) if isinstance(sequence_number, int) else -1
    occurred_at = _normalize_dt(event.occurred_at).timestamp()
    return (seq, occurred_at, str(event.event_id))


def build_instance_expectations(
    events: Iterable[EventEnvelope],
    *,
    db_name: Optional[str] = None,
    branch: Optional[str] = None,
) -> ExpectationBuildResult:
    """
    Reduce instance events to one expected projection state per instance.
    """
    by_key: Dict[InstanceProjectionKey, InstanceProjectionExpectation] = {}
    skipped_event_ids: list[str] = []

    db_filter = str(db_name or "").strip() or None
    branch_filter = str(branch or "").strip() or None

    for event in events:
        key = key_from_instance_event(event)
        if key is None:
            skipped_event_ids.append(str(event.event_id))
            continue

        if db_filter and key.db_name != db_filter:
            continue
        if branch_filter and key.branch != branch_filter:
            continue

        expectation = InstanceProjectionExpectation(
            key=key,
            latest_event_id=str(event.event_id),
            latest_event_type=str(event.event_type),
            sequence_number=event.sequence_number if isinstance(event.sequence_number, int) else None,
            occurred_at=_normalize_dt(event.occurred_at),
            should_exist=str(event.event_type) != "INSTANCE_DELETED",
        )

        current = by_key.get(key)
        if current is None:
            by_key[key] = expectation
            continue

        if _event_order_key(event) >= (
            int(current.sequence_number) if isinstance(current.sequence_number, int) else -1,
            current.occurred_at.timestamp(),
            current.latest_event_id,
        ):
            by_key[key] = expectation

    return ExpectationBuildResult(expectations=by_key, skipped_event_ids=skipped_event_ids)


def evaluate_projection_document(
    expectation: InstanceProjectionExpectation,
    document: Optional[Dict[str, Any]],
) -> Optional[str]:
    """
    Compare expected state against an ES projection document.

    Returns:
        None if consistent, otherwise mismatch reason string.
    """
    if expectation.should_exist:
        if not isinstance(document, dict):
            return "missing_document"

        instance_id = str(document.get("instance_id") or "").strip()
        if instance_id and instance_id != expectation.key.instance_id:
            return "instance_id_mismatch"

        class_id = str(document.get("class_id") or "").strip()
        if class_id and class_id != expectation.key.class_id:
            return "class_id_mismatch"

        db_name = str(document.get("db_name") or "").strip()
        if db_name and db_name != expectation.key.db_name:
            return "db_name_mismatch"

        branch = str(document.get("branch") or "").strip()
        if branch and branch != expectation.key.branch:
            return "branch_mismatch"

        return None

    if isinstance(document, dict):
        return "deleted_document_present"
    return None
