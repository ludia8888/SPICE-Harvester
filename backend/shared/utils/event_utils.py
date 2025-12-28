from __future__ import annotations

from typing import Any, Dict, Optional
from uuid import uuid4

from shared.models.event_envelope import EventEnvelope
from shared.utils.time_utils import utcnow


def build_command_event(
    *,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    data: Dict[str, Any],
    command_type: str,
    actor: Optional[str] = None,
    event_id: Optional[str] = None,
) -> EventEnvelope:
    command_id = event_id or str(uuid4())
    now = utcnow()
    command_payload = {
        "command_id": command_id,
        "command_type": command_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "payload": data,
        "metadata": {},
        "created_at": now.isoformat(),
        "created_by": actor,
        "version": 1,
    }
    return EventEnvelope(
        event_id=command_payload["command_id"],
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        occurred_at=now,
        actor=actor,
        data=command_payload,
        metadata={
            "kind": "command",
            "command_type": command_type,
            "actor": actor,
        },
    )
