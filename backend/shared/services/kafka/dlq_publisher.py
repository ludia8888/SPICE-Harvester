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

from shared.models.event_envelope import EventEnvelope
from shared.observability.tracing import trace_kafka_operation
from shared.observability.context_propagation import (
    attach_context_from_kafka,
    kafka_headers_from_current_context,
    kafka_headers_from_envelope_metadata,
)
from shared.utils.time_utils import utcnow


def _safe_decode(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    try:
        return str(value)
    except (TypeError, ValueError, RuntimeError):
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
    enterprise_error: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    try:
        original_value = payload_text if payload_text is not None else _safe_decode(msg.value())
    except (AttributeError, TypeError, ValueError, RuntimeError):
        original_value = payload_text
    if original_value is None:
        original_value = "<unavailable>"

    timestamp_ms: Optional[int] = None
    try:
        ts = msg.timestamp()
        if ts:
            timestamp_ms = int(ts[1])
    except (AttributeError, TypeError, ValueError, RuntimeError):
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

    if enterprise_error:
        payload["enterprise"] = dict(enterprise_error)

    return payload


@dataclass(frozen=True, slots=True)
class DlqPublishSpec:
    dlq_topic: str
    service_name: str
    span_name: Optional[str] = None
    metric_event_name: Optional[str] = None
    flush_timeout_seconds: Optional[float] = 10.0
    poll_after_produce: bool = False


@dataclass(frozen=True, slots=True)
class EnvelopeDlqSpec:
    dlq_topic: str
    service_name: str
    kind: str
    failed_event_type: str
    span_name: Optional[str] = None
    metric_event_name: Optional[str] = None
    flush_timeout_seconds: float = 10.0


@dataclass(frozen=True, slots=True)
class _SyntheticKafkaMessage:
    topic_name: str
    partition_id: int = -1
    offset_id: int = -1

    def topic(self) -> str:
        return str(self.topic_name)

    def partition(self) -> int:
        return int(self.partition_id)

    def offset(self) -> int:
        return int(self.offset_id)


def default_dlq_span_attributes(*, dlq_topic: str, msg: Any) -> dict[str, Any]:
    return {
        "messaging.system": "kafka",
        "messaging.destination": dlq_topic,
        "messaging.destination_kind": "topic",
        "messaging.kafka.partition": int(getattr(msg, "partition")()),
        "messaging.kafka.offset": int(getattr(msg, "offset")()),
    }


def build_envelope_dlq_event(
    *,
    envelope: EventEnvelope,
    spec: EnvelopeDlqSpec,
    error: str,
    attempt_count: int,
    extra_metadata: Optional[Mapping[str, Any]] = None,
) -> EventEnvelope:
    dlq_env = envelope.model_copy(deep=True)
    metadata = dlq_env.metadata if isinstance(dlq_env.metadata, dict) else {}
    dlq_env.metadata = dict(metadata)
    dlq_env.metadata.update(
        {
            "kind": str(spec.kind or "").strip(),
            "dlq_error": (error or "").strip()[:4000],
            "dlq_attempt_count": int(attempt_count),
            "dlq_topic": str(spec.dlq_topic or "").strip(),
        }
    )
    if extra_metadata:
        dlq_env.metadata.update(dict(extra_metadata))
    dlq_env.event_type = str(spec.failed_event_type or "").strip() or dlq_env.event_type
    return dlq_env


def default_envelope_dlq_span_attributes(*, spec: EnvelopeDlqSpec, envelope: EventEnvelope) -> dict[str, Any]:
    return {
        "messaging.system": "kafka",
        "messaging.destination": spec.dlq_topic,
        "messaging.destination_kind": "topic",
        "event.id": str(envelope.event_id),
        "event.aggregate_id": str(envelope.aggregate_id or ""),
        "event.type": str(envelope.event_type),
    }


@trace_kafka_operation("kafka.publish_envelope_dlq")
async def publish_envelope_dlq(
    *,
    producer_ops: Any,
    spec: EnvelopeDlqSpec,
    envelope: EventEnvelope,
    tracing: Optional[Any] = None,
    metrics: Optional[Any] = None,
    key: Optional[bytes] = None,
    span_attributes: Optional[Mapping[str, Any]] = None,
) -> None:
    key_bytes = key
    if key_bytes is None:
        key_text = str(envelope.aggregate_id or envelope.event_id or spec.service_name or "dlq")
        key_bytes = key_text.encode("utf-8")

    value = envelope.model_dump_json().encode("utf-8")
    metadata = envelope.metadata if isinstance(envelope.metadata, dict) else None
    headers = kafka_headers_from_envelope_metadata(metadata or {})

    with attach_context_from_kafka(
        kafka_headers=headers,
        fallback_metadata=metadata,
        service_name=str(spec.service_name or "").strip() or None,
    ):
        span_cm = nullcontext()
        if tracing is not None and getattr(tracing, "span", None) and spec.span_name:
            attrs = (
                dict(span_attributes)
                if span_attributes is not None
                else default_envelope_dlq_span_attributes(spec=spec, envelope=envelope)
            )
            span_cm = tracing.span(str(spec.span_name), attributes=attrs)

        with span_cm:
            await producer_ops.produce(
                topic=spec.dlq_topic,
                key=key_bytes,
                value=value,
                headers=headers or None,
            )
            await producer_ops.flush(float(spec.flush_timeout_seconds))

    if metrics is not None and spec.metric_event_name:
        try:
            metrics.record_event(str(spec.metric_event_name), action="published")
        except Exception as exc:
            logger.warning("Failed to record DLQ metric %s: %s", spec.metric_event_name, exc, exc_info=True)


@trace_kafka_operation("kafka.publish_standard_dlq")
async def publish_standard_dlq(
    *,
    producer: Any,
    msg: Any,
    worker: str,
    dlq_spec: DlqPublishSpec,
    error: str,
    attempt_count: Optional[int],
    stage: Optional[str] = None,
    payload_text: Optional[str] = None,
    payload_obj: Optional[Mapping[str, Any]] = None,
    extra: Optional[Mapping[str, Any]] = None,
    tracing: Optional[Any] = None,
    metrics: Optional[Any] = None,
    kafka_headers: Optional[Any] = None,
    fallback_metadata: Optional[Mapping[str, Any]] = None,
    lock: Optional[asyncio.Lock] = None,
    error_max_chars: int = 4000,
) -> None:
    dlq_payload = build_standard_dlq_payload(
        msg=msg,
        worker=str(worker or "").strip() or worker,
        stage=stage,
        error=error,
        attempt_count=int(attempt_count) if attempt_count is not None else None,
        payload_text=payload_text,
        payload_obj=payload_obj,
        error_max_chars=int(error_max_chars),
        extra=extra,
    )
    await publish_dlq_json(
        producer=producer,
        spec=dlq_spec,
        msg=msg,
        payload=dlq_payload,
        tracing=tracing,
        metrics=metrics,
        kafka_headers=kafka_headers,
        fallback_metadata=dict(fallback_metadata) if isinstance(fallback_metadata, Mapping) else None,
        lock=lock,
    )


@trace_kafka_operation("kafka.publish_dlq_json")
async def publish_dlq_json(
    *,
    producer: Any,
    spec: DlqPublishSpec,
    msg: Any,
    payload: Mapping[str, Any],
    message_key: Optional[bytes] = None,
    tracing: Optional[Any] = None,
    metrics: Optional[Any] = None,
    kafka_headers: Optional[Any] = None,
    fallback_metadata: Optional[Mapping[str, Any]] = None,
    lock: Optional[asyncio.Lock] = None,
    span_attributes: Optional[Mapping[str, Any]] = None,
) -> None:
    key = message_key
    if key is None:
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
        except Exception as exc:
            logger.warning("Failed to record contextual DLQ metric %s: %s", spec.metric_event_name, exc, exc_info=True)


@trace_kafka_operation("kafka.publish_contextual_dlq_json")
async def publish_contextual_dlq_json(
    *,
    producer: Any,
    spec: DlqPublishSpec,
    payload: Mapping[str, Any],
    message_key: Optional[bytes] = None,
    msg: Optional[Any] = None,
    source_topic: Optional[str] = None,
    source_partition: int = -1,
    source_offset: int = -1,
    tracing: Optional[Any] = None,
    metrics: Optional[Any] = None,
    kafka_headers: Optional[Any] = None,
    fallback_metadata: Optional[Mapping[str, Any]] = None,
    lock: Optional[asyncio.Lock] = None,
    span_attributes: Optional[Mapping[str, Any]] = None,
) -> None:
    effective_msg = msg
    if effective_msg is None:
        effective_msg = _SyntheticKafkaMessage(
            topic_name=str(source_topic or "unknown"),
            partition_id=int(source_partition),
            offset_id=int(source_offset),
        )

    attrs = span_attributes
    if attrs is None and msg is None:
        attrs = {
            "messaging.system": "kafka",
            "messaging.destination": spec.dlq_topic,
            "messaging.destination_kind": "topic",
            "messaging.source_name": str(source_topic or "unknown"),
            "messaging.source_partition": int(source_partition),
            "messaging.source_offset": int(source_offset),
        }

    await publish_dlq_json(
        producer=producer,
        spec=spec,
        msg=effective_msg,
        payload=payload,
        message_key=message_key,
        tracing=tracing,
        metrics=metrics,
        kafka_headers=kafka_headers,
        fallback_metadata=fallback_metadata,
        lock=lock,
        span_attributes=attrs,
    )
