"""
Event envelope model used across services.

The codebase historically had multiple "event" shapes (S3 event_store.Event,
shared.models.events.BaseEvent, ad-hoc dicts in workers). This envelope provides a
single canonical shape for storage (S3/MinIO) and transport (Kafka).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from .commands import BaseCommand
from .events import BaseEvent


class EventEnvelope(BaseModel):
    """
    Canonical event envelope.

    Notes:
    - `event_type` is a string to support both shared `EventType` enums and
      command-request lifecycle events (e.g. "*_REQUESTED").
    - `occurred_at` is always timezone-aware UTC.
    - `metadata.kafka_topic` can be used by publishers to route events.
    """

    event_id: str = Field(default_factory=lambda: str(uuid4()), description="Unique event id")
    event_type: str = Field(..., description="Event type name (string)")
    aggregate_type: str = Field(..., description="Aggregate type")
    aggregate_id: str = Field(..., description="Aggregate id")
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Event timestamp (UTC)",
    )
    actor: Optional[str] = Field(None, description="Actor/user id")
    data: Dict[str, Any] = Field(default_factory=dict, description="Event payload")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Event metadata")
    schema_version: str = Field("1", description="Envelope schema version")
    sequence_number: Optional[int] = Field(None, description="Per-aggregate sequence/version")

    @staticmethod
    def _normalize_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @classmethod
    def from_command(
        cls,
        command: BaseCommand,
        *,
        actor: Optional[str] = None,
        event_type: Optional[str] = None,
        kafka_topic: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "EventEnvelope":
        # Use JSON mode so Any-typed payloads don't retain UUID/Enum objects.
        command_dict = command.model_dump(mode="json")
        base_metadata: Dict[str, Any] = {
            "kind": "command",
            "command_type": command.command_type.value,
            "command_version": command.version,
            "command_id": str(command.command_id),
            "run_id": os.getenv("PIPELINE_RUN_ID") or os.getenv("RUN_ID") or os.getenv("EXECUTION_ID"),
            "code_sha": os.getenv("CODE_SHA") or os.getenv("GIT_SHA") or os.getenv("COMMIT_SHA"),
        }
        if kafka_topic:
            base_metadata["kafka_topic"] = kafka_topic
        if metadata:
            base_metadata.update(metadata)

        return cls(
            event_id=str(command.command_id),
            event_type=event_type or f"{command.command_type.value}_REQUESTED",
            aggregate_type=command.aggregate_type,
            aggregate_id=command.aggregate_id,
            occurred_at=cls._normalize_datetime(command.created_at),
            actor=actor or command.created_by,
            data=command_dict,
            metadata=base_metadata,
        )

    @classmethod
    def from_base_event(
        cls,
        event: BaseEvent,
        *,
        kafka_topic: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "EventEnvelope":
        base_metadata: Dict[str, Any] = {
            "kind": "domain",
            "run_id": os.getenv("PIPELINE_RUN_ID") or os.getenv("RUN_ID") or os.getenv("EXECUTION_ID"),
            "code_sha": os.getenv("CODE_SHA") or os.getenv("GIT_SHA") or os.getenv("COMMIT_SHA"),
        }
        if kafka_topic:
            base_metadata["kafka_topic"] = kafka_topic
        if event.metadata:
            base_metadata.update(event.metadata)
        if metadata:
            base_metadata.update(metadata)

        event_type = event.event_type.value if hasattr(event.event_type, "value") else str(event.event_type)

        return cls(
            event_id=str(event.event_id),
            event_type=event_type,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            occurred_at=cls._normalize_datetime(event.occurred_at),
            actor=event.occurred_by,
            data=event.data,
            metadata=base_metadata,
            schema_version=event.schema_version or "1",
            sequence_number=event.sequence_number,
        )

    def as_kafka_key(self) -> bytes:
        # Per-aggregate ordering: partition by aggregate_id.
        return (self.aggregate_id or self.event_id).encode("utf-8")

    def as_json(self) -> str:
        return self.model_dump_json()
