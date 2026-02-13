"""
OMS router helpers for Event Sourcing command emission.

Problem: Many OMS endpoints repeat the same boilerplate:
- wrap a command into an EventEnvelope
- append to the S3/MinIO Event Store (SSoT)
- map optimistic concurrency conflicts to HTTP 409
- best-effort persist command status to Redis

This module centralizes that logic to reduce drift and duplication.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import HTTPException, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from shared.models.commands import CommandStatus
from shared.models.event_envelope import EventEnvelope
from shared.services.events.aggregate_sequence_allocator import OptimisticConcurrencyError

logger = logging.getLogger(__name__)


def build_command_status_metadata(*, command: Any, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "command_type": getattr(command, "command_type", None),
        "aggregate_id": getattr(command, "aggregate_id", None),
        "created_at": getattr(getattr(command, "created_at", None), "isoformat", lambda: None)(),
        "created_by": getattr(command, "created_by", None) or "system",
    }
    if extra:
        meta.update(extra)
    return meta


async def append_event_sourcing_command(
    *,
    event_store: Any,
    command: Any,
    actor: Optional[str],
    kafka_topic: str,
    envelope_metadata: Optional[Dict[str, Any]] = None,
    command_status_service: Any = None,
    command_status_metadata: Optional[Dict[str, Any]] = None,
) -> EventEnvelope:
    metadata: Dict[str, Any] = {"service": "oms", "mode": "event_sourcing"}
    if envelope_metadata:
        metadata.update(envelope_metadata)

    envelope = EventEnvelope.from_command(
        command,
        actor=(actor or "system"),
        kafka_topic=str(kafka_topic),
        metadata=metadata,
    )
    try:
        await event_store.append_event(envelope)
    except OptimisticConcurrencyError as exc:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Optimistic concurrency conflict",
            code=ErrorCode.CONFLICT,
            extra={
                "error": "optimistic_concurrency_conflict",
                "aggregate_id": exc.aggregate_id,
                "expected_seq": exc.expected_last_sequence,
                "actual_seq": exc.actual_last_sequence,
            },
        ) from exc

    if command_status_service and command_status_metadata:
        try:
            await command_status_service.set_command_status(
                command_id=str(command.command_id),
                status=CommandStatus.PENDING,
                metadata=command_status_metadata,
            )
        except Exception as exc:
            logger.warning(
                "Failed to persist command status (continuing without Redis): %s",
                exc,
            )

    return envelope
