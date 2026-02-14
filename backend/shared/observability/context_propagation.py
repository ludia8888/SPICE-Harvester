"""
Trace context propagation helpers.

This module bridges trace context across:
- HTTP (handled by OTel instrumentations)
- Kafka (confluent_kafka headers; manual propagation)
- Stored payload metadata (EventEnvelope.metadata or outbox payloads)

Design goals:
- Safe no-op when OpenTelemetry API isn't available.
- Work even when OpenTelemetry SDK/exporters are not installed (context may still flow).
- Use W3C Trace Context (`traceparent`/`tracestate`) plus W3C baggage (`baggage`).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Mapping, MutableMapping, Optional, Sequence, Tuple
from urllib.parse import quote

from shared.observability.request_context import (
    context_from_metadata,
    parse_baggage_header,
    get_correlation_id,
    get_db_name,
    get_principal,
    get_request_id,
    request_context,
)

_TRACEPARENT = "traceparent"
_TRACESTATE = "tracestate"
_BAGGAGE = "baggage"
_KNOWN_KEYS = (_TRACEPARENT, _TRACESTATE, _BAGGAGE)
logger = logging.getLogger(__name__)


try:  # OpenTelemetry API (no SDK dependency)
    from opentelemetry import context as otel_context
    from opentelemetry import trace as otel_trace
    from opentelemetry import baggage as otel_baggage
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.propagators.composite import CompositePropagator
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    _PROPAGATOR = CompositePropagator(
        [
            TraceContextTextMapPropagator(),
            W3CBaggagePropagator(),
        ]
    )
    _HAS_OTEL_API = True
except ImportError:  # pragma: no cover - env dependent
    otel_context = None
    otel_trace = None
    otel_baggage = None
    _PROPAGATOR = None
    _HAS_OTEL_API = False


def _to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            return None
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except (TypeError, ValueError, RuntimeError):
        return None


def _encode_baggage_value(value: str) -> str:
    return quote(str(value), safe="-._~")


def _merge_baggage(
    existing: Optional[str],
    *,
    request_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    db_name: Optional[str] = None,
) -> Optional[str]:
    extras = {
        "request_id": _to_text(request_id),
        "correlation_id": _to_text(correlation_id),
        "db_name": _to_text(db_name),
    }
    extras = {k: v for k, v in extras.items() if v}
    if not extras:
        return existing

    raw_existing = _to_text(existing)
    if not raw_existing:
        return ",".join(f"{k}={_encode_baggage_value(v)}" for k, v in extras.items())

    parsed = parse_baggage_header(raw_existing)
    missing = {k: v for k, v in extras.items() if k not in parsed}
    if not missing:
        return raw_existing

    suffix = ",".join(f"{k}={_encode_baggage_value(v)}" for k, v in missing.items())
    return f"{raw_existing},{suffix}"


def carrier_from_kafka_headers(kafka_headers: Optional[Sequence[Tuple[str, Any]]]) -> Dict[str, str]:
    """
    Extract a W3C carrier dict from confluent_kafka headers.

    confluent_kafka typically returns: List[Tuple[str, bytes|None]]
    """
    carrier: Dict[str, str] = {}
    for key, raw in kafka_headers or []:
        if not key:
            continue
        lowered = str(key).lower()
        if lowered not in _KNOWN_KEYS:
            continue
        text = _to_text(raw)
        if text:
            carrier[lowered] = text
    return carrier


def carrier_from_envelope_metadata(payload_or_metadata: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    """
    Extract a W3C carrier dict from an EventEnvelope.metadata-like dict.

    Accepts either:
    - the metadata dict itself, OR
    - a parent payload dict that contains a nested `metadata` dict.
    """
    if not isinstance(payload_or_metadata, Mapping):
        return {}

    base: Mapping[str, Any] = payload_or_metadata
    has_any = any(k in base for k in _KNOWN_KEYS)
    nested = base.get("metadata")
    if not has_any and isinstance(nested, Mapping):
        base = nested

    carrier: Dict[str, str] = {}
    for key in _KNOWN_KEYS:
        text = _to_text(base.get(key))
        if text:
            carrier[key] = text

    # Extra context keys (not part of W3C header set).
    request_id = _to_text(base.get("request_id"))
    correlation_id = _to_text(base.get("correlation_id"))
    db_name = _to_text(base.get("db_name") or base.get("project") or base.get("db"))
    principal = _to_text(base.get("principal"))

    if request_id:
        carrier["request_id"] = request_id
    if correlation_id:
        carrier["correlation_id"] = correlation_id
    if db_name:
        carrier["db_name"] = db_name
    if principal:
        carrier["principal"] = principal

    baggage = _merge_baggage(
        carrier.get(_BAGGAGE),
        request_id=request_id,
        correlation_id=correlation_id,
        db_name=db_name,
    )
    if baggage:
        carrier[_BAGGAGE] = baggage
    return carrier


def kafka_headers_from_carrier(carrier: Mapping[str, str]) -> list[Tuple[str, bytes]]:
    headers: list[Tuple[str, bytes]] = []
    for key in _KNOWN_KEYS:
        value = carrier.get(key)
        if not value:
            continue
        try:
            headers.append((key, str(value).encode("utf-8")))
        except (UnicodeEncodeError, TypeError, ValueError, RuntimeError) as exc:
            logger.warning("Failed to encode Kafka header %s: %s", key, exc, exc_info=True)
            continue
    return headers


def kafka_headers_from_envelope_metadata(payload_or_metadata: Optional[Mapping[str, Any]]) -> list[Tuple[str, bytes]]:
    carrier = carrier_from_envelope_metadata(payload_or_metadata)
    return kafka_headers_from_carrier(carrier)


def kafka_headers_with_dedup(
    payload_or_metadata: Optional[Mapping[str, Any]],
    *,
    dedup_id: Optional[str] = None,
    event_id: Optional[str] = None,
    aggregate_id: Optional[str] = None,
) -> list[Tuple[str, bytes]]:
    """
    Build Kafka headers with deduplication ID for idempotent message processing.

    Args:
        payload_or_metadata: Payload or metadata dict for trace context extraction
        dedup_id: Explicit deduplication ID (if not provided, computed from event_id/aggregate_id)
        event_id: Event ID for dedup key computation
        aggregate_id: Aggregate ID for dedup key computation

    Returns:
        List of Kafka header tuples including dedup-id header
    """
    headers = kafka_headers_from_envelope_metadata(payload_or_metadata)

    # Compute or use provided dedup_id
    if not dedup_id:
        if event_id:
            dedup_id = f"{aggregate_id or 'global'}:{event_id}"
        elif payload_or_metadata:
            # Try to extract from payload
            if isinstance(payload_or_metadata, Mapping):
                event_id = payload_or_metadata.get("event_id") or payload_or_metadata.get("job_id")
                aggregate_id = payload_or_metadata.get("aggregate_id") or payload_or_metadata.get("dataset_id") or payload_or_metadata.get("pipeline_id")
                if event_id:
                    dedup_id = f"{aggregate_id or 'global'}:{event_id}"

    if dedup_id:
        headers.append(("dedup-id", dedup_id.encode("utf-8")))

    return headers


def kafka_headers_from_current_context() -> list[Tuple[str, bytes]]:
    """
    Build Kafka headers (W3C Trace Context + baggage) from the current OTel context.
    """
    if not _HAS_OTEL_API or _PROPAGATOR is None:
        return []
    carrier: Dict[str, str] = {}
    try:
        _PROPAGATOR.inject(carrier)
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        logger.warning("Failed to inject OTel context into Kafka headers: %s", exc, exc_info=True)
        return []

    baggage = _merge_baggage(
        carrier.get(_BAGGAGE),
        request_id=get_request_id(),
        correlation_id=get_correlation_id(),
        db_name=get_db_name(),
    )
    if baggage:
        carrier[_BAGGAGE] = baggage
    return kafka_headers_from_carrier(carrier)


def enrich_metadata_with_current_trace(metadata: Any) -> None:
    """
    Mutate a metadata/payload dict by adding W3C trace context keys.

    This is intentionally best-effort and safe to call in any code path.
    """
    if not _HAS_OTEL_API or _PROPAGATOR is None:
        return
    if not isinstance(metadata, MutableMapping):
        return
    carrier: Dict[str, str] = {}
    try:
        _PROPAGATOR.inject(carrier)
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        logger.warning("Failed to inject OTel context into metadata: %s", exc, exc_info=True)
        return

    baggage = _merge_baggage(
        carrier.get(_BAGGAGE),
        request_id=get_request_id(),
        correlation_id=get_correlation_id(),
        db_name=get_db_name(),
    )
    if baggage:
        carrier[_BAGGAGE] = baggage
    for key in _KNOWN_KEYS:
        value = carrier.get(key)
        if value:
            metadata[key] = value

    # Convenience debug fields for operators/audit logs.
    # (These do not replace W3C headers; they complement them.)
    try:
        if otel_trace is not None:
            span = otel_trace.get_current_span()
            ctx = span.get_span_context() if span is not None else None
            is_valid = getattr(ctx, "is_valid", False) if ctx is not None else False
            if callable(is_valid):
                is_valid = is_valid()
            if ctx is not None and is_valid:
                trace_id = getattr(ctx, "trace_id", 0) or 0
                span_id = getattr(ctx, "span_id", 0) or 0
                if trace_id:
                    metadata.setdefault("trace_id", format(int(trace_id), "032x"))
                if span_id:
                    metadata.setdefault("span_id", format(int(span_id), "016x"))
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        logger.warning("Failed to enrich metadata with trace/span ids: %s", exc, exc_info=True)

    try:
        if otel_baggage is not None:
            for key in ("request_id", "correlation_id", "db_name"):
                value = otel_baggage.get_baggage(key)
                if value:
                    metadata.setdefault(key, value)
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        logger.warning("Failed to enrich metadata from baggage: %s", exc, exc_info=True)

    try:
        for key, getter in (
            ("request_id", get_request_id),
            ("correlation_id", get_correlation_id),
            ("db_name", get_db_name),
        ):
            value = getter()
            if value:
                metadata.setdefault(key, value)
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        logger.warning("Failed to enrich metadata from request context getters: %s", exc, exc_info=True)


@contextmanager
def attach_context_from_carrier(
    carrier: Optional[Mapping[str, str]],
    *,
    service_name: Optional[str] = None,
) -> Iterator[None]:
    """
    Attach an extracted context for the duration of the `with` block.

    `service_name` is accepted for future debugging/metrics; currently unused.
    """
    _ = service_name
    if not _HAS_OTEL_API or _PROPAGATOR is None:
        yield
        return
    if not carrier:
        yield
        return

    token = None
    try:
        ctx = _PROPAGATOR.extract(dict(carrier))
        token = otel_context.attach(ctx)
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        logger.warning("Failed to attach extracted OTel context: %s", exc, exc_info=True)
        token = None

    try:
        extracted = context_from_metadata(carrier)
        with request_context(
            request_id=extracted.get("request_id") or get_request_id(),
            correlation_id=extracted.get("correlation_id") or get_correlation_id(),
            db_name=extracted.get("db_name") or get_db_name(),
            principal=extracted.get("principal") or get_principal(),
            inject_baggage=False,
        ):
            yield
    finally:
        if token is not None:
            try:
                otel_context.detach(token)
            except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
                logger.warning("Failed to detach OTel context token: %s", exc, exc_info=True)


@contextmanager
def attach_context_from_kafka(
    *,
    kafka_headers: Optional[Sequence[Tuple[str, Any]]],
    fallback_metadata: Optional[Mapping[str, Any]] = None,
    service_name: Optional[str] = None,
) -> Iterator[None]:
    """
    Attach trace context from Kafka headers (preferred) or fallback metadata.
    """
    carrier: Dict[str, str] = {}
    fallback_ctx = context_from_metadata(fallback_metadata) if fallback_metadata else {}
    if fallback_metadata:
        carrier.update(carrier_from_envelope_metadata(fallback_metadata))
    carrier.update(carrier_from_kafka_headers(kafka_headers))

    # Prefer explicit metadata fields for context vars (correlation_id/request_id),
    # then let baggage/propagators handle cross-hop propagation.
    with request_context(
        request_id=fallback_ctx.get("request_id") or get_request_id(),
        correlation_id=fallback_ctx.get("correlation_id") or get_correlation_id(),
        db_name=fallback_ctx.get("db_name") or get_db_name(),
        principal=fallback_ctx.get("principal") or get_principal(),
        inject_baggage=False,
    ):
        with attach_context_from_carrier(carrier or None, service_name=service_name):
            yield
