"""
OpenTelemetry tracing helpers.

Enterprise requirement:
- This module must never "look enabled" while doing nothing.
- Missing optional exporters/instrumentations must NOT disable tracing entirely.

This implementation:
- Enables tracing when core OpenTelemetry deps are present.
- Degrades gracefully when optional deps (OTLP exporter, Kafka/httpx/asyncpg instrumentors, etc.) are missing.
"""

from __future__ import annotations

import os
from contextlib import contextmanager, nullcontext
from functools import wraps
from typing import Any, Dict, Optional

from shared.config.settings import settings
from shared.utils.app_logger import get_logger

logger = get_logger(__name__)


# -----------------------------
# Optional imports (best-effort)
# -----------------------------

try:
    from opentelemetry import baggage, context, trace as otel_trace
    from opentelemetry.propagate import extract, inject, set_global_textmap
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
    from opentelemetry.trace import Status, StatusCode, get_tracer_provider, set_tracer_provider
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    HAS_OPENTELEMETRY = True
except Exception:  # pragma: no cover - depends on environment
    baggage = None
    context = None
    otel_trace = None
    extract = None
    inject = None
    set_global_textmap = None
    Resource = None
    SERVICE_NAME = None
    SERVICE_VERSION = None
    TracerProvider = None
    BatchSpanProcessor = None
    ConsoleSpanExporter = None
    ParentBased = None
    TraceIdRatioBased = None
    Status = None
    StatusCode = None
    get_tracer_provider = None
    set_tracer_provider = None
    TraceContextTextMapPropagator = None
    HAS_OPENTELEMETRY = False


try:  # optional
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
except Exception:  # pragma: no cover - depends on environment
    OTLPSpanExporter = None

try:  # optional
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
except Exception:  # pragma: no cover - depends on environment
    JaegerExporter = None

try:  # optional
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
except Exception:  # pragma: no cover - depends on environment
    FastAPIInstrumentor = None

try:  # optional
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
except Exception:  # pragma: no cover - depends on environment
    HTTPXClientInstrumentor = None

try:  # optional
    from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
except Exception:  # pragma: no cover - depends on environment
    AsyncPGInstrumentor = None

try:  # optional
    from opentelemetry.instrumentation.redis import RedisInstrumentor
except Exception:  # pragma: no cover - depends on environment
    RedisInstrumentor = None

try:  # optional
    from opentelemetry.instrumentation.kafka import KafkaInstrumentor
except Exception:  # pragma: no cover - depends on environment
    KafkaInstrumentor = None


class OpenTelemetryConfig:
    # Service identification
    SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "spice-harvester")
    SERVICE_VERSION = os.getenv("OTEL_SERVICE_VERSION", "1.0.0")
    ENVIRONMENT = os.getenv("OTEL_ENVIRONMENT", settings.environment.value)

    # Exporter endpoints
    OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    JAEGER_ENDPOINT = os.getenv("JAEGER_ENDPOINT", "localhost:14250")

    # Sampling
    TRACE_SAMPLE_RATE = float(os.getenv("OTEL_TRACE_SAMPLE_RATE", "1.0"))

    # Export configuration
    EXPORT_CONSOLE = os.getenv("OTEL_EXPORT_CONSOLE", "false").lower() == "true"
    _raw_export_otlp = os.getenv("OTEL_EXPORT_OTLP")
    if _raw_export_otlp is None:
        EXPORT_OTLP = bool(OTLP_ENDPOINT)
    else:
        EXPORT_OTLP = _raw_export_otlp.lower() == "true"
    EXPORT_JAEGER = os.getenv("OTEL_EXPORT_JAEGER", "false").lower() == "true"

    # Feature flags
    ENABLE_TRACING = os.getenv("OTEL_ENABLE_TRACING", "true").lower() == "true"


class TracingService:
    """
    Distributed tracing facade.

    - When OpenTelemetry is available, this configures a TracerProvider and provides helpers.
    - When unavailable, it stays explicitly no-op (and logs once).
    """

    def __init__(self, service_name: Optional[str] = None):
        self.service_name = service_name or OpenTelemetryConfig.SERVICE_NAME
        self._initialized = False
        self.tracer = None
        self._no_op_logged = False

        if not HAS_OPENTELEMETRY:
            self._log_no_op_once("OpenTelemetry core not available; tracing disabled")

    def _log_no_op_once(self, reason: str) -> None:
        if self._no_op_logged:
            return
        self._no_op_logged = True
        logger.warning(f"TracingService running in no-op mode: {reason}")

    def initialize(self) -> None:
        if not HAS_OPENTELEMETRY:
            self._log_no_op_once("OpenTelemetry core not available; tracing disabled")
            return
        if not OpenTelemetryConfig.ENABLE_TRACING:
            self._log_no_op_once("OTEL_ENABLE_TRACING=false")
            return
        if self._initialized:
            return

        try:
            resource = Resource.create(
                {
                    SERVICE_NAME: self.service_name,
                    SERVICE_VERSION: OpenTelemetryConfig.SERVICE_VERSION,
                    "environment": OpenTelemetryConfig.ENVIRONMENT,
                    "service.namespace": "spice-harvester",
                }
            )

            ratio = OpenTelemetryConfig.TRACE_SAMPLE_RATE
            if ratio < 0.0:
                ratio = 0.0
            if ratio > 1.0:
                ratio = 1.0

            sampler = ParentBased(TraceIdRatioBased(ratio))
            tracer_provider = TracerProvider(resource=resource, sampler=sampler)

            processors = 0

            if OpenTelemetryConfig.EXPORT_CONSOLE:
                tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
                processors += 1

            if OpenTelemetryConfig.EXPORT_OTLP:
                if OTLPSpanExporter is None:
                    logger.warning("OTLP exporter requested but opentelemetry-exporter-otlp is not installed")
                elif not OpenTelemetryConfig.OTLP_ENDPOINT:
                    logger.warning("OTLP exporter requested but OTEL_EXPORTER_OTLP_ENDPOINT is not set")
                else:
                    try:
                        tracer_provider.add_span_processor(
                            BatchSpanProcessor(
                                OTLPSpanExporter(endpoint=OpenTelemetryConfig.OTLP_ENDPOINT, insecure=True)
                            )
                        )
                        processors += 1
                        logger.info(f"OTLP exporter configured: {OpenTelemetryConfig.OTLP_ENDPOINT}")
                    except Exception as e:
                        logger.error(f"Failed to configure OTLP exporter: {e}")

            if OpenTelemetryConfig.EXPORT_JAEGER:
                if JaegerExporter is None:
                    logger.warning("Jaeger exporter requested but opentelemetry-exporter-jaeger is not installed")
                else:
                    try:
                        host, port_str = (OpenTelemetryConfig.JAEGER_ENDPOINT.split(":") + ["14250"])[:2]
                        tracer_provider.add_span_processor(
                            BatchSpanProcessor(JaegerExporter(agent_host_name=host, agent_port=int(port_str)))
                        )
                        processors += 1
                        logger.info(f"Jaeger exporter configured: {OpenTelemetryConfig.JAEGER_ENDPOINT}")
                    except Exception as e:
                        logger.error(f"Failed to configure Jaeger exporter: {e}")

            if processors == 0:
                logger.warning(
                    "Tracing enabled but no exporters configured/available; spans will not be exported"
                )

            set_tracer_provider(tracer_provider)
            set_global_textmap(TraceContextTextMapPropagator())
            self.tracer = otel_trace.get_tracer(self.service_name, OpenTelemetryConfig.SERVICE_VERSION)
            self._initialized = True
            logger.info(f"OpenTelemetry tracing initialized: service={self.service_name} sample_rate={ratio}")
        except Exception as e:
            logger.error(f"Failed to initialize OpenTelemetry tracing: {e}")
            self._log_no_op_once(f"initialization failed: {e}")

    def instrument_fastapi(self, app) -> None:
        if not HAS_OPENTELEMETRY or not OpenTelemetryConfig.ENABLE_TRACING:
            return
        if FastAPIInstrumentor is None:
            logger.warning("FastAPI instrumentation requested but opentelemetry-instrumentation-fastapi is missing")
            return
        try:
            FastAPIInstrumentor.instrument_app(
                app,
                tracer_provider=get_tracer_provider(),
                excluded_urls="/health,/metrics,/api/v1/health/*",
            )
            logger.info("FastAPI instrumented for tracing")
        except Exception as e:
            logger.error(f"Failed to instrument FastAPI: {e}")

    def instrument_clients(self) -> None:
        if not HAS_OPENTELEMETRY or not OpenTelemetryConfig.ENABLE_TRACING:
            return

        if HTTPXClientInstrumentor is not None:
            try:
                HTTPXClientInstrumentor().instrument()
                logger.info("HTTPX instrumented")
            except Exception as e:
                logger.error(f"Failed to instrument HTTPX: {e}")
        else:
            logger.debug("HTTPX instrumentation not available")

        if AsyncPGInstrumentor is not None:
            try:
                AsyncPGInstrumentor().instrument()
                logger.info("AsyncPG instrumented")
            except Exception as e:
                logger.error(f"Failed to instrument AsyncPG: {e}")
        else:
            logger.debug("AsyncPG instrumentation not available")

        if RedisInstrumentor is not None:
            try:
                RedisInstrumentor().instrument()
                logger.info("Redis instrumented")
            except Exception as e:
                logger.error(f"Failed to instrument Redis: {e}")
        else:
            logger.debug("Redis instrumentation not available")

        if KafkaInstrumentor is not None:
            try:
                KafkaInstrumentor().instrument()
                logger.info("Kafka instrumented")
            except Exception as e:
                logger.error(f"Failed to instrument Kafka: {e}")
        else:
            logger.debug("Kafka instrumentation not available")

    @contextmanager
    def span(self, name: str, kind=None, attributes: Optional[Dict[str, Any]] = None):
        if not HAS_OPENTELEMETRY or not self.tracer:
            yield from nullcontext()
            return

        with self.tracer.start_as_current_span(name, kind=kind, attributes=attributes or {}) as span:
            try:
                yield span
            except Exception as e:
                try:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                finally:
                    raise

    def trace(self, name: Optional[str] = None, kind=None, attributes: Optional[Dict[str, Any]] = None):
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                span_name = name or f"{func.__module__}.{func.__name__}"
                with self.span(span_name, kind=kind, attributes=attributes):
                    return await func(*args, **kwargs)

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                span_name = name or f"{func.__module__}.{func.__name__}"
                with self.span(span_name, kind=kind, attributes=attributes):
                    return func(*args, **kwargs)

            import asyncio

            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

        return decorator

    def get_current_span(self):
        if not HAS_OPENTELEMETRY:
            return None
        return otel_trace.get_current_span()

    def record_exception(self, exception: Exception) -> None:
        span = self.get_current_span()
        if span is None or not getattr(span, "is_recording", lambda: False)():
            return
        span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception)))

    def set_span_attribute(self, key: str, value: Any) -> None:
        span = self.get_current_span()
        if span is None or not getattr(span, "is_recording", lambda: False)():
            return
        span.set_attribute(key, value)

    def get_trace_id(self) -> Optional[str]:
        span = self.get_current_span()
        if span is None:
            return None
        ctx = span.get_span_context()
        if not ctx or not getattr(ctx, "trace_id", 0):
            return None
        return format(ctx.trace_id, "032x")

    def get_span_id(self) -> Optional[str]:
        span = self.get_current_span()
        if span is None:
            return None
        ctx = span.get_span_context()
        if not ctx or not getattr(ctx, "span_id", 0):
            return None
        return format(ctx.span_id, "016x")

    def inject_trace_context(self, headers: Dict[str, str]) -> Dict[str, str]:
        if not HAS_OPENTELEMETRY or inject is None:
            return headers
        try:
            inject(headers)
        except Exception:
            return headers
        return headers

    def extract_trace_context(self, headers: Dict[str, str]):
        if not HAS_OPENTELEMETRY or extract is None:
            return None
        try:
            return extract(headers)
        except Exception:
            return None


# Global tracing instance (per process)
_tracing_service: Optional[TracingService] = None


def get_tracing_service(service_name: Optional[str] = None) -> TracingService:
    global _tracing_service
    if _tracing_service is None:
        _tracing_service = TracingService(service_name)
        _tracing_service.initialize()
        _tracing_service.instrument_clients()
    return _tracing_service


def trace_endpoint(name: Optional[str] = None):
    tracing = get_tracing_service()
    if HAS_OPENTELEMETRY and otel_trace is not None:
        return tracing.trace(name, kind=otel_trace.SpanKind.SERVER)
    return tracing.trace(name, kind=None)


def trace_db_operation(name: Optional[str] = None):
    tracing = get_tracing_service()
    attrs = {"db.system": "postgresql"}
    if HAS_OPENTELEMETRY and otel_trace is not None:
        return tracing.trace(name, kind=otel_trace.SpanKind.CLIENT, attributes=attrs)
    return tracing.trace(name, kind=None, attributes=attrs)


def trace_external_call(name: Optional[str] = None):
    tracing = get_tracing_service()
    if HAS_OPENTELEMETRY and otel_trace is not None:
        return tracing.trace(name, kind=otel_trace.SpanKind.CLIENT)
    return tracing.trace(name, kind=None)
