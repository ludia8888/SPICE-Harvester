from __future__ import annotations

import logging
import os
from typing import Any, Optional

from shared.config.app_config import AppConfig
from shared.models.event_envelope import EventEnvelope
from shared.services.event_store import event_store
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)


def pipeline_control_plane_events_enabled() -> bool:
    raw = str(os.getenv("ENABLE_PIPELINE_CONTROL_PLANE_EVENTS", "")).strip().lower()
    if raw in {"0", "false", "no", "off"}:
        logger.warning(
            "ENABLE_PIPELINE_CONTROL_PLANE_EVENTS is disabled but ignored; control-plane events are always on."
        )
    return True


def sanitize_event_id(value: str) -> str:
    return str(value or "").strip().replace("/", "_")


async def emit_pipeline_control_plane_event(
    *,
    event_type: str,
    pipeline_id: str,
    event_id: str,
    data: dict[str, Any],
    actor: Optional[str] = None,
    kind: str = "pipeline_control_plane",
) -> bool:
    if not pipeline_control_plane_events_enabled():
        return False

    resolved_event_id = sanitize_event_id(event_id)
    if not resolved_event_id:
        logger.warning("pipeline control-plane event skipped: empty event_id (type=%s)", event_type)
        return False

    envelope = EventEnvelope(
        event_id=resolved_event_id,
        event_type=event_type,
        aggregate_type="Pipeline",
        aggregate_id=str(pipeline_id),
        occurred_at=utcnow(),
        actor=actor,
        data=data,
        metadata={
            "kind": kind,
            "kafka_topic": AppConfig.PIPELINE_EVENTS_TOPIC,
        },
    )
    try:
        await event_store.connect()
        await event_store.append_event(envelope)
        return True
    except Exception as exc:
        logger.warning("pipeline control-plane event append failed (%s): %s", event_type, exc)
        return False
