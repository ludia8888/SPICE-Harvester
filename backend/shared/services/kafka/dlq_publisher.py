"""
Shared DLQ publisher for Kafka workers (Facade).

Many workers publish "raw" DLQ messages for terminal failures. This module
centralizes:
- consistent payload shape (standard fields + optional extras)
- context propagation (traceparent/baggage) via Kafka headers/metadata
- tracing span wrapper + metric hook
- non-blocking flush via `asyncio.to_thread`
"""

from __future__ import annotations

import asyncio
import json
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from shared.observability.context_propagation import (
    attach_context_from_kafka,
    kafka_headers_from_current_context,
)
from shared.utils.time_utils import utcnow


def _safe_decode(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    try:
        return str(value)
    except Exception:
        return None


def build_standard_dlq_payload(
    *,
    msg: Any,
    worker: str,
    stage: Optional[str],
    error: str,
    attempt_count: Optional[int],
    payload_text: Optional[str],
    payload_obj: Optional[Mapping[str, Any]] = None,
    error_max_chars: int = 4000,
    extra: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    try:
        original_value = payload_text if payload_text is not None else _safe_decode(msg.value())
    except Exception:
        original_value = payload_text
    if original_value is None:
        original_value = "<unavailable>"

    timestamp_ms: Optional[int] = None
    try:
        ts = msg.timestamp()
        if ts:
            timestamp_ms = int(ts[1])
    except Exception:
        timestamp_ms = None

    payload: dict[str, Any] = {
        "original_topic": getattr(msg, "topic")(),
        "original_partition": int(getattr(msg, "partition")()),
        "original_offset": int(getattr(msg, "offset")()),
        "original_timestamp": timestamp_ms,
        "original_key": _safe_decode(getattr(msg, "key")()),
        "original_value": original_value,
        "error": (error or "").strip()[: max(0, int(error_max_chars))],
        "attempt_count": int(attempt_count) if attempt_count is not None else None,
        "worker": str(worker or "").strip() or worker,
        "timestamp": utcnow().isoformat(),
    }

    stage_final = (stage or "").strip()
    if stage_final:
        payload["stage"] = stage_final

    if payload_obj is not None:
        payload["parsed_payload"] = dict(payload_obj)

    if extra:
        payload.update(dict(extra))

    return payload


@dataclass(frozen=True, slots=True)
class DlqPublishSpec:
    dlq_topic: str
    service_name: str
    span_name: Optional[str] = None
    metric_event_name: Optional[str] = None
    flush_timeout_seconds: Optional[float] = 10.0
    poll_after_produce: bool = False


def default_dlq_span_attributes(*, dlq_topic: str, msg: Any) -> dict[str, Any]:
    return {
        "messaging.system": "kafka",
        "messaging.destination": dlq_topic,
        "messaging.destination_kind": "topic",
        "messaging.kafka.partition": int(getattr(msg, "partition")()),
        "messaging.kafka.offset": int(getattr(msg, "offset")()),
    }


async def publish_dlq_json(
    *,
    producer: Any,
    spec: DlqPublishSpec,
    msg: Any,
    payload: Mapping[str, Any],
    tracing: Optional[Any] = None,
    metrics: Optional[Any] = None,
    kafka_headers: Optional[Any] = None,
    fallback_metadata: Optional[Mapping[str, Any]] = None,
    lock: Optional[asyncio.Lock] = None,
    span_attributes: Optional[Mapping[str, Any]] = None,
) -> None:
    key = f"{getattr(msg, 'topic')()}:{int(getattr(msg, 'partition')())}:{int(getattr(msg, 'offset')())}".encode(
        "utf-8"
    )
    value = json.dumps(dict(payload), ensure_ascii=False, default=str).encode("utf-8")

    with attach_context_from_kafka(
        kafka_headers=kafka_headers,
        fallback_metadata=fallback_metadata,
        service_name=str(spec.service_name or "").strip() or None,
    ):
        headers = kafka_headers_from_current_context()

        span_cm = nullcontext()
        if tracing is not None and getattr(tracing, "span", None) and spec.span_name:
            attrs = dict(span_attributes) if span_attributes is not None else default_dlq_span_attributes(
                dlq_topic=spec.dlq_topic,
                msg=msg,
            )
            span_cm = tracing.span(str(spec.span_name), attributes=attrs)

        with span_cm:
            if lock is not None:
                async with lock:
                    producer.produce(spec.dlq_topic, key=key, value=value, headers=headers or None)
                    if spec.poll_after_produce and getattr(producer, "poll", None):
                        producer.poll(0)
                    if spec.flush_timeout_seconds is None:
                        await asyncio.to_thread(producer.flush)
                    else:
                        await asyncio.to_thread(producer.flush, float(spec.flush_timeout_seconds))
            else:
                producer.produce(spec.dlq_topic, key=key, value=value, headers=headers or None)
                if spec.poll_after_produce and getattr(producer, "poll", None):
                    producer.poll(0)
                if spec.flush_timeout_seconds is None:
                    await asyncio.to_thread(producer.flush)
                else:
                    await asyncio.to_thread(producer.flush, float(spec.flush_timeout_seconds))

    if metrics is not None and spec.metric_event_name:
        try:
            metrics.record_event(str(spec.metric_event_name), action="published")
        except Exception:
            pass
