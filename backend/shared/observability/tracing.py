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

from shared.config.settings import get_settings
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


try:  # optional: baggage propagation for richer cross-service context
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.propagators.composite import CompositePropagator
except Exception:  # pragma: no cover - depends on environment
    W3CBaggagePropagator = None
    CompositePropagator = None


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
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
except Exception:  # pragma: no cover - depends on environment
    AsyncioInstrumentor = None

try:  # optional
    from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
except Exception:  # pragma: no cover - depends on environment
    AioHttpClientInstrumentor = None

try:  # optional
    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
except Exception:  # pragma: no cover - depends on environment
    BotocoreInstrumentor = None

try:  # optional
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
except Exception:  # pragma: no cover - depends on environment
    RequestsInstrumentor = None

try:  # optional
    from opentelemetry.instrumentation.kafka import KafkaInstrumentor
except Exception:  # pragma: no cover - depends on environment
    KafkaInstrumentor = None


class _SettingsValue:
    def __init__(self, getter):  # noqa: ANN001
        self._getter = getter

    def __get__(self, instance: object, owner: type | None = None):  # noqa: ANN001
        return self._getter(get_settings())


class OpenTelemetryConfig:
    # Service identification
    SERVICE_NAME = _SettingsValue(
        lambda s: (s.observability.otel_service_name or "spice-harvester").strip() or "spice-harvester"
    )
    SERVICE_VERSION = _SettingsValue(lambda s: str(s.observability.otel_service_version or "1.0.0").strip() or "1.0.0")
    ENVIRONMENT = _SettingsValue(lambda s: (s.observability.otel_environment or s.environment.value).strip())

    # Exporter endpoints
    OTLP_ENDPOINT = _SettingsValue(lambda s: (s.observability.otel_exporter_otlp_endpoint or "").strip())
    JAEGER_ENDPOINT = _SettingsValue(lambda s: str(s.observability.jaeger_endpoint or "localhost:14250").strip() or "localhost:14250")

    # Sampling
    TRACE_SAMPLE_RATE = _SettingsValue(lambda s: float(s.observability.otel_trace_sample_rate))

    # Export configuration
    EXPORT_CONSOLE = _SettingsValue(lambda s: bool(s.observability.otel_export_console))
    EXPORT_OTLP = _SettingsValue(lambda s: bool(s.observability.otel_export_otlp_effective))
    EXPORT_JAEGER = _SettingsValue(lambda s: bool(s.observability.otel_export_jaeger))

    # Feature flags
    ENABLE_TRACING = _SettingsValue(lambda s: bool(s.observability.otel_enable_tracing))
    ENABLE_ASYNCIO_INSTRUMENTATION = _SettingsValue(lambda s: bool(s.observability.otel_instrument_asyncio))


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

            # Prefer W3C Trace Context + W3C baggage when available. This improves
            # end-to-end correlation (e.g., request_id/correlation_id) across HTTP hops.
            propagator = TraceContextTextMapPropagator()
            if CompositePropagator is not None and W3CBaggagePropagator is not None:
                try:
                    propagator = CompositePropagator(
                        [TraceContextTextMapPropagator(), W3CBaggagePropagator()]
                    )
                except Exception:
                    propagator = TraceContextTextMapPropagator()

            set_global_textmap(propagator)
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

        # NOTE: opentelemetry-instrumentation-asyncio has historically caused runtime
        # issues around `asyncio.to_thread` / partial callables in some versions.
        # Keep this opt-in so observability never breaks control-plane correctness.
        if AsyncioInstrumentor is None:
            logger.debug("Asyncio instrumentation not available")
        elif "PYTEST_CURRENT_TEST" in os.environ:
            logger.debug("Asyncio instrumentation disabled during tests")
        elif not OpenTelemetryConfig.ENABLE_ASYNCIO_INSTRUMENTATION:
            logger.debug("Asyncio instrumentation disabled (OTEL_INSTRUMENT_ASYNCIO=false)")
        else:
            try:
                AsyncioInstrumentor().instrument()
                logger.info("Asyncio instrumented")
            except Exception as e:
                logger.error(f"Failed to instrument Asyncio: {e}")

        if HTTPXClientInstrumentor is not None:
            try:
                HTTPXClientInstrumentor().instrument()
                logger.info("HTTPX instrumented")
            except Exception as e:
                logger.error(f"Failed to instrument HTTPX: {e}")
        else:
            logger.debug("HTTPX instrumentation not available")

        if RequestsInstrumentor is not None:
            try:
                RequestsInstrumentor().instrument()
                logger.info("Requests instrumented")
            except Exception as e:
                logger.error(f"Failed to instrument Requests: {e}")
        else:
            logger.debug("Requests instrumentation not available")

        if AioHttpClientInstrumentor is not None:
            try:
                AioHttpClientInstrumentor().instrument()
                logger.info("AioHttpClient instrumented")
            except Exception as e:
                logger.error(f"Failed to instrument AioHttpClient: {e}")
        else:
            logger.debug("AioHttpClient instrumentation not available")

        if BotocoreInstrumentor is not None:
            try:
                BotocoreInstrumentor().instrument()
                logger.info("Botocore instrumented")
            except Exception as e:
                logger.error(f"Failed to instrument Botocore: {e}")
        else:
            logger.debug("Botocore instrumentation not available")

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
    """
    Lazily create a tracing decorator for request handlers.

    Important: this must NOT call `get_tracing_service()` at import/decorator time.
    Some modules apply this decorator at import time, before the service has had a
    chance to initialize tracing with the correct `service.name`.
    """

    kind = otel_trace.SpanKind.SERVER if HAS_OPENTELEMETRY and otel_trace is not None else None
    return _lazy_trace(name=name, kind=kind, attributes=None)


def trace_db_operation(name: Optional[str] = None):
    attrs = {"db.system": "postgresql"}
    kind = otel_trace.SpanKind.CLIENT if HAS_OPENTELEMETRY and otel_trace is not None else None
    return _lazy_trace(name=name, kind=kind, attributes=attrs)


def trace_external_call(name: Optional[str] = None):
    kind = otel_trace.SpanKind.CLIENT if HAS_OPENTELEMETRY and otel_trace is not None else None
    return _lazy_trace(name=name, kind=kind, attributes=None)


def _lazy_trace(*, name: Optional[str], kind=None, attributes: Optional[Dict[str, Any]] = None):
    def decorator(func):
        import asyncio

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                tracing = get_tracing_service()
                span_name = name or f"{func.__module__}.{func.__name__}"
                with tracing.span(span_name, kind=kind, attributes=attributes):
                    return await func(*args, **kwargs)

            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            tracing = get_tracing_service()
            span_name = name or f"{func.__module__}.{func.__name__}"
            with tracing.span(span_name, kind=kind, attributes=attributes):
                return func(*args, **kwargs)

        return sync_wrapper

    return decorator
