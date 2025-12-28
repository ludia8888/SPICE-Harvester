from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

from shared.config.app_config import AppConfig
from shared.services.dataset_registry import DatasetRegistry
from shared.services.event_store import event_store
from shared.services.lineage_store import LineageStore
from shared.models.event_envelope import EventEnvelope


async def flush_dataset_ingest_outbox(
    *,
    dataset_registry: DatasetRegistry,
    lineage_store: Optional[LineageStore],
    batch_size: int = 50,
) -> None:
    await event_store.connect()
    while True:
        batch = await dataset_registry.claim_ingest_outbox_batch(limit=batch_size)
        if not batch:
            break
        for item in batch:
            try:
                if item.kind == "eventstore":
                    payload = item.payload or {}
                    event = EventEnvelope(
                        event_id=str(payload.get("event_id") or ""),
                        event_type=str(payload.get("event_type") or ""),
                        aggregate_type=str(payload.get("aggregate_type") or ""),
                        aggregate_id=str(payload.get("aggregate_id") or ""),
                        occurred_at=payload.get("occurred_at") or datetime.now(timezone.utc),
                        actor=payload.get("actor"),
                        data=payload.get("data") or {},
                        metadata={
                            "kind": "command",
                            "command_type": payload.get("command_type"),
                            "command_id": str(payload.get("event_id") or ""),
                        },
                    )
                    event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
                    await event_store.append_event(event)
                elif item.kind == "lineage":
                    if lineage_store is None:
                        raise RuntimeError("LineageStore unavailable")
                    payload = item.payload or {}
                    await lineage_store.record_link(
                        from_node_id=payload.get("from_node_id"),
                        to_node_id=payload.get("to_node_id"),
                        edge_type=payload.get("edge_type"),
                        occurred_at=payload.get("occurred_at"),
                        from_label=payload.get("from_label"),
                        to_label=payload.get("to_label"),
                        db_name=payload.get("db_name"),
                        edge_metadata=payload.get("edge_metadata") or {},
                    )
                else:
                    raise RuntimeError(f"Unknown ingest outbox kind: {item.kind}")
                await dataset_registry.mark_ingest_outbox_published(outbox_id=item.outbox_id)
            except Exception as exc:
                await dataset_registry.mark_ingest_outbox_failed(outbox_id=item.outbox_id, error=str(exc))


async def run_dataset_ingest_outbox_worker(
    *,
    dataset_registry: DatasetRegistry,
    lineage_store: Optional[LineageStore],
    poll_interval_seconds: int = 5,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    while not stop_event.is_set():
        await flush_dataset_ingest_outbox(
            dataset_registry=dataset_registry,
            lineage_store=lineage_store,
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
        except asyncio.TimeoutError:
            continue


def build_dataset_event_payload(
    *,
    event_id: str,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    command_type: str,
    actor: Optional[str],
    data: dict[str, Any],
) -> dict[str, Any]:
    command_payload = {
        "command_id": event_id,
        "command_type": command_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "payload": data,
        "metadata": {},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": actor,
        "version": 1,
    }
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "command_type": command_type,
        "actor": actor,
        "data": command_payload,
    }
