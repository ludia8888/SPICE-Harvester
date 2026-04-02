from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

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
    logger.warning("Failed to import core OpenTelemetry dependencies", exc_info=True)
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

try:
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.propagators.composite import CompositePropagator
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import baggage propagation support", exc_info=True)
    W3CBaggagePropagator = None
    CompositePropagator = None

try:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import OTLP trace exporter", exc_info=True)
    OTLPSpanExporter = None

try:
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import Jaeger trace exporter", exc_info=True)
    JaegerExporter = None

try:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import FastAPI instrumentor", exc_info=True)
    FastAPIInstrumentor = None

try:
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import HTTPX instrumentor", exc_info=True)
    HTTPXClientInstrumentor = None

try:
    from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import AsyncPG instrumentor", exc_info=True)
    AsyncPGInstrumentor = None

try:
    from opentelemetry.instrumentation.redis import RedisInstrumentor
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import Redis instrumentor", exc_info=True)
    RedisInstrumentor = None

try:
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import Asyncio instrumentor", exc_info=True)
    AsyncioInstrumentor = None

try:
    from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import AioHttpClient instrumentor", exc_info=True)
    AioHttpClientInstrumentor = None

try:
    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import Botocore instrumentor", exc_info=True)
    BotocoreInstrumentor = None

try:
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import Requests instrumentor", exc_info=True)
    RequestsInstrumentor = None

try:
    from opentelemetry.instrumentation.kafka import KafkaInstrumentor
except ModuleNotFoundError:
    logger.info("Kafka OpenTelemetry instrumentation module is not installed; continuing without Kafka instrumentation.")
    KafkaInstrumentor = None
except Exception:  # pragma: no cover - depends on environment
    logger.warning("Failed to import Kafka instrumentor", exc_info=True)
    KafkaInstrumentor = None

__all__ = [
    "AioHttpClientInstrumentor",
    "AsyncPGInstrumentor",
    "AsyncioInstrumentor",
    "BatchSpanProcessor",
    "BotocoreInstrumentor",
    "CompositePropagator",
    "ConsoleSpanExporter",
    "FastAPIInstrumentor",
    "HAS_OPENTELEMETRY",
    "HTTPXClientInstrumentor",
    "JaegerExporter",
    "KafkaInstrumentor",
    "OTLPSpanExporter",
    "ParentBased",
    "RedisInstrumentor",
    "RequestsInstrumentor",
    "Resource",
    "SERVICE_NAME",
    "SERVICE_VERSION",
    "Status",
    "StatusCode",
    "TraceContextTextMapPropagator",
    "TraceIdRatioBased",
    "TracerProvider",
    "W3CBaggagePropagator",
    "baggage",
    "context",
    "extract",
    "get_tracer_provider",
    "inject",
    "otel_trace",
    "set_global_textmap",
    "set_tracer_provider",
]
