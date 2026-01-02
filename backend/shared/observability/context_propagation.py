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

from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, Mapping, MutableMapping, Optional, Sequence, Tuple

_TRACEPARENT = "traceparent"
_TRACESTATE = "tracestate"
_BAGGAGE = "baggage"
_KNOWN_KEYS = (_TRACEPARENT, _TRACESTATE, _BAGGAGE)


try:  # OpenTelemetry API (no SDK dependency)
    from opentelemetry import context as otel_context
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
except Exception:  # pragma: no cover - env dependent
    otel_context = None
    _PROPAGATOR = None
    _HAS_OTEL_API = False


def _to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return None
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except Exception:
        return None


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
    return carrier


def kafka_headers_from_carrier(carrier: Mapping[str, str]) -> list[Tuple[str, bytes]]:
    headers: list[Tuple[str, bytes]] = []
    for key in _KNOWN_KEYS:
        value = carrier.get(key)
        if not value:
            continue
        try:
            headers.append((key, str(value).encode("utf-8")))
        except Exception:
            continue
    return headers


def kafka_headers_from_envelope_metadata(payload_or_metadata: Optional[Mapping[str, Any]]) -> list[Tuple[str, bytes]]:
    carrier = carrier_from_envelope_metadata(payload_or_metadata)
    return kafka_headers_from_carrier(carrier)


def kafka_headers_from_current_context() -> list[Tuple[str, bytes]]:
    """
    Build Kafka headers (W3C Trace Context + baggage) from the current OTel context.
    """
    if not _HAS_OTEL_API or _PROPAGATOR is None:
        return []
    carrier: Dict[str, str] = {}
    try:
        _PROPAGATOR.inject(carrier)
    except Exception:
        return []
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
    except Exception:
        return
    for key in _KNOWN_KEYS:
        value = carrier.get(key)
        if value:
            metadata[key] = value


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
    except Exception:
        token = None

    try:
        yield
    finally:
        if token is not None:
            try:
                otel_context.detach(token)
            except Exception:
                pass


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
    if fallback_metadata:
        carrier.update(carrier_from_envelope_metadata(fallback_metadata))
    carrier.update(carrier_from_kafka_headers(kafka_headers))
    with attach_context_from_carrier(carrier or None, service_name=service_name):
        yield

