"""
OpenTelemetry Tracing Configuration
Based on Context7 recommendations for monitoring and observability

This module provides distributed tracing capabilities for SPICE HARVESTER
using OpenTelemetry standards.
"""

import os
import logging
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from functools import wraps

from opentelemetry import trace as otel_trace
from opentelemetry import context, baggage, metrics
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.kafka import KafkaInstrumentor
from opentelemetry.metrics import get_meter_provider, set_meter_provider
from opentelemetry.propagate import set_global_textmap
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Status, StatusCode, get_tracer_provider, set_tracer_provider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from shared.config.settings import settings
from shared.utils.app_logger import get_logger

logger = get_logger(__name__)


class OpenTelemetryConfig:
    """
    Configuration for OpenTelemetry based on Context7 recommendations
    """
    
    # Service identification
    SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "spice-harvester")
    SERVICE_VERSION = os.getenv("OTEL_SERVICE_VERSION", "1.0.0")
    ENVIRONMENT = os.getenv("OTEL_ENVIRONMENT", settings.environment.value)
    
    # Exporter endpoints
    OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
    JAEGER_ENDPOINT = os.getenv("JAEGER_ENDPOINT", "localhost:14250")
    
    # Sampling configuration
    TRACE_SAMPLE_RATE = float(os.getenv("OTEL_TRACE_SAMPLE_RATE", "1.0"))
    
    # Export configuration
    EXPORT_CONSOLE = os.getenv("OTEL_EXPORT_CONSOLE", "false").lower() == "true"
    EXPORT_OTLP = os.getenv("OTEL_EXPORT_OTLP", "true").lower() == "true"
    EXPORT_JAEGER = os.getenv("OTEL_EXPORT_JAEGER", "false").lower() == "true"
    
    # Feature flags
    ENABLE_TRACING = os.getenv("OTEL_ENABLE_TRACING", "true").lower() == "true"
    ENABLE_METRICS = os.getenv("OTEL_ENABLE_METRICS", "true").lower() == "true"
    ENABLE_LOGS = os.getenv("OTEL_ENABLE_LOGS", "true").lower() == "true"


class TracingService:
    """
    Service for managing OpenTelemetry tracing
    Based on Context7 patterns for distributed systems
    """
    
    def __init__(self, service_name: Optional[str] = None):
        """
        Initialize tracing service
        
        Args:
            service_name: Override default service name
        """
        self.service_name = service_name or OpenTelemetryConfig.SERVICE_NAME
        self.tracer: Optional[otel_trace.Tracer] = None
        self.meter: Optional[metrics.Meter] = None
        self._initialized = False
        
    def initialize(self) -> None:
        """
        Initialize OpenTelemetry with all components
        """
        if self._initialized:
            logger.warning("OpenTelemetry already initialized")
            return
            
        try:
            # Create resource
            resource = Resource.create({
                SERVICE_NAME: self.service_name,
                SERVICE_VERSION: OpenTelemetryConfig.SERVICE_VERSION,
                "environment": OpenTelemetryConfig.ENVIRONMENT,
                "service.namespace": "spice-harvester",
            })
            
            # Initialize tracing
            if OpenTelemetryConfig.ENABLE_TRACING:
                self._initialize_tracing(resource)
                
            # Initialize metrics
            if OpenTelemetryConfig.ENABLE_METRICS:
                self._initialize_metrics(resource)
                
            # Set up context propagation
            set_global_textmap(TraceContextTextMapPropagator())
            
            self._initialized = True
            logger.info(f"OpenTelemetry initialized for service: {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize OpenTelemetry: {e}")
            # Continue without tracing - don't break the application
    
    def _initialize_tracing(self, resource: Resource) -> None:
        """
        Initialize tracing provider and exporters
        """
        # Create tracer provider
        tracer_provider = TracerProvider(
            resource=resource,
            active_span_processor=None,
        )
        
        # Add exporters based on configuration
        if OpenTelemetryConfig.EXPORT_CONSOLE:
            tracer_provider.add_span_processor(
                BatchSpanProcessor(ConsoleSpanExporter())
            )
            
        if OpenTelemetryConfig.EXPORT_OTLP:
            try:
                otlp_exporter = OTLPSpanExporter(
                    endpoint=OpenTelemetryConfig.OTLP_ENDPOINT,
                    insecure=True,  # Use secure=False for local development
                )
                tracer_provider.add_span_processor(
                    BatchSpanProcessor(otlp_exporter)
                )
                logger.info(f"OTLP exporter configured: {OpenTelemetryConfig.OTLP_ENDPOINT}")
            except Exception as e:
                logger.error(f"Failed to configure OTLP exporter: {e}")
                
        if OpenTelemetryConfig.EXPORT_JAEGER:
            try:
                jaeger_exporter = JaegerExporter(
                    agent_host_name=OpenTelemetryConfig.JAEGER_ENDPOINT.split(":")[0],
                    agent_port=int(OpenTelemetryConfig.JAEGER_ENDPOINT.split(":")[1]),
                    collector_endpoint=None,  # Use agent, not collector
                )
                tracer_provider.add_span_processor(
                    BatchSpanProcessor(jaeger_exporter)
                )
                logger.info(f"Jaeger exporter configured: {OpenTelemetryConfig.JAEGER_ENDPOINT}")
            except Exception as e:
                logger.error(f"Failed to configure Jaeger exporter: {e}")
        
        # Set global tracer provider
        set_tracer_provider(tracer_provider)
        
        # Get tracer for this service
        self.tracer = otel_trace.get_tracer(
            self.service_name,
            OpenTelemetryConfig.SERVICE_VERSION
        )
    
    def _initialize_metrics(self, resource: Resource) -> None:
        """
        Initialize metrics provider and exporters
        """
        # Create meter provider with Prometheus exporter
        reader = PrometheusMetricReader()
        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[reader]
        )
        
        # Add OTLP metric exporter if configured
        if OpenTelemetryConfig.EXPORT_OTLP:
            try:
                otlp_metric_exporter = OTLPMetricExporter(
                    endpoint=OpenTelemetryConfig.OTLP_ENDPOINT,
                    insecure=True,
                )
                meter_provider.add_metric_reader(otlp_metric_exporter)
                logger.info("OTLP metric exporter configured")
            except Exception as e:
                logger.error(f"Failed to configure OTLP metric exporter: {e}")
        
        # Set global meter provider
        set_meter_provider(meter_provider)
        
        # Get meter for this service
        self.meter = metrics.get_meter(
            self.service_name,
            OpenTelemetryConfig.SERVICE_VERSION
        )
    
    def instrument_fastapi(self, app) -> None:
        """
        Instrument FastAPI application
        
        Args:
            app: FastAPI application instance
        """
        if not OpenTelemetryConfig.ENABLE_TRACING:
            return
            
        try:
            FastAPIInstrumentor.instrument_app(
                app,
                tracer_provider=get_tracer_provider(),
                excluded_urls="/health,/metrics,/api/v1/health/*"
            )
            logger.info("FastAPI instrumented for tracing")
        except Exception as e:
            logger.error(f"Failed to instrument FastAPI: {e}")
    
    def instrument_clients(self) -> None:
        """
        Instrument various client libraries
        """
        if not OpenTelemetryConfig.ENABLE_TRACING:
            return
            
        # Instrument HTTPX
        try:
            HTTPXClientInstrumentor().instrument()
            logger.info("HTTPX client instrumented")
        except Exception as e:
            logger.error(f"Failed to instrument HTTPX: {e}")
        
        # Instrument AsyncPG
        try:
            AsyncPGInstrumentor().instrument()
            logger.info("AsyncPG instrumented")
        except Exception as e:
            logger.error(f"Failed to instrument AsyncPG: {e}")
        
        # Instrument Redis
        try:
            RedisInstrumentor().instrument()
            logger.info("Redis instrumented")
        except Exception as e:
            logger.error(f"Failed to instrument Redis: {e}")
        
        # Instrument Kafka
        try:
            KafkaInstrumentor().instrument()
            logger.info("Kafka instrumented")
        except Exception as e:
            logger.error(f"Failed to instrument Kafka: {e}")
    
    @contextmanager
    def span(
        self,
        name: str,
        kind: otel_trace.SpanKind = otel_trace.SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """
        Create a new span context manager
        
        Args:
            name: Span name
            kind: Span kind (INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER)
            attributes: Span attributes
            
        Example:
            with tracing.span("process_data", attributes={"count": 100}):
                # Your code here
                pass
        """
        if not self.tracer:
            yield
            return
            
        with self.tracer.start_as_current_span(
            name,
            kind=kind,
            attributes=attributes or {}
        ) as span:
            try:
                yield span
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise
    
    def trace(
        self,
        name: Optional[str] = None,
        kind: otel_trace.SpanKind = otel_trace.SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """
        Decorator for tracing functions
        
        Args:
            name: Span name (defaults to function name)
            kind: Span kind
            attributes: Additional attributes
            
        Example:
            @tracing.trace(attributes={"operation": "fetch"})
            async def fetch_data():
                pass
        """
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                span_name = name or f"{func.__module__}.{func.__name__}"
                with self.span(span_name, kind, attributes):
                    return await func(*args, **kwargs)
            
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                span_name = name or f"{func.__module__}.{func.__name__}"
                with self.span(span_name, kind, attributes):
                    return func(*args, **kwargs)
            
            # Return appropriate wrapper based on function type
            import asyncio
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                return sync_wrapper
                
        return decorator
    
    def add_baggage(self, key: str, value: str) -> None:
        """
        Add baggage to context for propagation
        
        Args:
            key: Baggage key
            value: Baggage value
        """
        ctx = baggage.set_baggage(key, value)
        context.attach(ctx)
    
    def get_current_span(self) -> Optional[otel_trace.Span]:
        """
        Get the current active span
        
        Returns:
            Current span or None
        """
        return otel_trace.get_current_span()
    
    def record_metric(
        self,
        name: str,
        value: float,
        unit: str = "",
        description: str = "",
        attributes: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Record a metric value
        
        Args:
            name: Metric name
            value: Metric value
            unit: Unit of measurement
            description: Metric description
            attributes: Additional attributes
        """
        if not self.meter:
            return
            
        try:
            # Create or get counter
            counter = self.meter.create_counter(
                name=name,
                unit=unit,
                description=description
            )
            counter.add(value, attributes or {})
        except Exception as e:
            logger.error(f"Failed to record metric {name}: {e}")
    
    def shutdown(self) -> None:
        """
        Shutdown OpenTelemetry providers
        """
        if not self._initialized:
            return
            
        try:
            # Shutdown tracer provider
            tracer_provider = get_tracer_provider()
            if hasattr(tracer_provider, "shutdown"):
                tracer_provider.shutdown()
                
            # Shutdown meter provider
            meter_provider = get_meter_provider()
            if hasattr(meter_provider, "shutdown"):
                meter_provider.shutdown()
                
            self._initialized = False
            logger.info("OpenTelemetry shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during OpenTelemetry shutdown: {e}")


# Global tracing instance
_tracing_service: Optional[TracingService] = None


def get_tracing_service(service_name: Optional[str] = None) -> TracingService:
    """
    Get or create global tracing service
    
    Args:
        service_name: Optional service name override
        
    Returns:
        TracingService instance
    """
    global _tracing_service
    if _tracing_service is None:
        _tracing_service = TracingService(service_name)
        _tracing_service.initialize()
        _tracing_service.instrument_clients()
    return _tracing_service


# Convenience decorators
def trace_endpoint(name: Optional[str] = None):
    """
    Decorator for tracing API endpoints
    
    Example:
        @trace_endpoint("get_user")
        async def get_user(user_id: str):
            pass
    """
    tracing = get_tracing_service()
    return tracing.trace(name, kind=otel_trace.SpanKind.SERVER)


def trace_db_operation(name: Optional[str] = None):
    """
    Decorator for tracing database operations
    
    Example:
        @trace_db_operation("fetch_ontology")
        async def fetch_ontology(db_name: str):
            pass
    """
    tracing = get_tracing_service()
    return tracing.trace(name, kind=otel_trace.SpanKind.CLIENT, attributes={"db.system": "postgresql"})


def trace_external_call(name: Optional[str] = None):
    """
    Decorator for tracing external API calls
    
    Example:
        @trace_external_call("call_terminus")
        async def call_terminus_api():
            pass
    """
    tracing = get_tracing_service()
    return tracing.trace(name, kind=otel_trace.SpanKind.CLIENT)