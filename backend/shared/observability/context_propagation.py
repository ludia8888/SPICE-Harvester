"""
Context Propagation for Distributed Tracing
Based on Context7 recommendations for microservices observability

This module handles trace context propagation across service boundaries.
"""

import json
from typing import Dict, Optional, Any, Tuple
from fastapi import Request, Response
from httpx import Headers

from opentelemetry import trace as otel_trace
from opentelemetry import context, baggage
from opentelemetry.propagate import extract, inject
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.propagators.composite import CompositePropagator

from shared.utils.app_logger import get_logger

logger = get_logger(__name__)


class ContextPropagator:
    """
    Handles context propagation for distributed tracing
    Based on W3C Trace Context standard
    """
    
    def __init__(self):
        """Initialize context propagator with W3C standards"""
        self.propagator = CompositePropagator([
            TraceContextTextMapPropagator(),
            W3CBaggagePropagator(),
        ])
        
    def extract_from_request(self, request: Request) -> context.Context:
        """
        Extract trace context from incoming request
        
        Args:
            request: FastAPI request object
            
        Returns:
            Extracted context
        """
        # Convert headers to carrier format
        carrier = dict(request.headers)
        
        # Extract context
        ctx = extract(carrier, context=context.get_current())
        
        # Log trace information
        span = otel_trace.get_current_span(ctx)
        if span.is_recording():
            trace_id = format(span.get_span_context().trace_id, '032x')
            span_id = format(span.get_span_context().span_id, '016x')
            logger.debug(f"Extracted trace context - TraceID: {trace_id}, SpanID: {span_id}")
            
        return ctx
    
    def inject_to_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """
        Inject trace context into outgoing headers
        
        Args:
            headers: Existing headers dictionary
            
        Returns:
            Headers with trace context
        """
        if headers is None:
            headers = {}
            
        # Inject current context
        inject(headers)
        
        # Log what was injected
        if "traceparent" in headers:
            logger.debug(f"Injected trace context: {headers['traceparent']}")
            
        return headers
    
    def extract_from_kafka_headers(self, headers: list) -> context.Context:
        """
        Extract trace context from Kafka message headers
        
        Args:
            headers: Kafka message headers
            
        Returns:
            Extracted context
        """
        # Convert Kafka headers to dictionary
        carrier = {}
        for key, value in headers:
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            carrier[key] = value
            
        # Extract context
        return extract(carrier, context=context.get_current())
    
    def inject_to_kafka_headers(self, headers: Optional[list] = None) -> list:
        """
        Inject trace context into Kafka message headers
        
        Args:
            headers: Existing Kafka headers
            
        Returns:
            Headers with trace context
        """
        if headers is None:
            headers = []
            
        # Create carrier for injection
        carrier = {}
        inject(carrier)
        
        # Convert to Kafka header format
        for key, value in carrier.items():
            headers.append((key.encode('utf-8'), value.encode('utf-8')))
            
        return headers
    
    def create_child_span(
        self,
        name: str,
        parent_context: Optional[context.Context] = None,
        kind: otel_trace.SpanKind = otel_trace.SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Tuple[otel_trace.Span, context.Context]:
        """
        Create a child span with proper context
        
        Args:
            name: Span name
            parent_context: Parent context (uses current if None)
            kind: Span kind
            attributes: Span attributes
            
        Returns:
            Tuple of (span, context)
        """
        tracer = otel_trace.get_tracer(__name__)
        
        # Use parent context or current
        ctx = parent_context or context.get_current()
        
        # Create child span
        with context.use(ctx):
            span = tracer.start_span(
                name,
                kind=kind,
                attributes=attributes or {}
            )
            
        # Get new context with this span
        new_ctx = otel_trace.set_span_in_context(span, ctx)
        
        return span, new_ctx
    
    def add_correlation_id(self, correlation_id: str) -> None:
        """
        Add correlation ID to baggage for propagation
        
        Args:
            correlation_id: Correlation ID to propagate
        """
        ctx = baggage.set_baggage("correlation_id", correlation_id)
        context.attach(ctx)
        
    def get_correlation_id(self) -> Optional[str]:
        """
        Get correlation ID from current context
        
        Returns:
            Correlation ID or None
        """
        return baggage.get_baggage("correlation_id")
    
    def add_user_context(self, user_id: str, tenant_id: Optional[str] = None) -> None:
        """
        Add user context to baggage
        
        Args:
            user_id: User identifier
            tenant_id: Optional tenant identifier
        """
        ctx = baggage.set_baggage("user_id", user_id)
        if tenant_id:
            ctx = baggage.set_baggage("tenant_id", tenant_id, ctx)
        context.attach(ctx)
        
    def get_user_context(self) -> Dict[str, Optional[str]]:
        """
        Get user context from baggage
        
        Returns:
            Dictionary with user_id and tenant_id
        """
        return {
            "user_id": baggage.get_baggage("user_id"),
            "tenant_id": baggage.get_baggage("tenant_id")
        }


class TraceContextMiddleware:
    """
    FastAPI middleware for automatic context propagation
    """
    
    def __init__(self, app):
        """
        Initialize middleware
        
        Args:
            app: FastAPI application
        """
        self.app = app
        self.propagator = ContextPropagator()
        
    async def __call__(self, request: Request, call_next):
        """
        Process request with context propagation
        
        Args:
            request: Incoming request
            call_next: Next middleware/handler
            
        Returns:
            Response with trace headers
        """
        # Extract context from incoming request
        ctx = self.propagator.extract_from_request(request)
        
        # Attach context for this request
        token = context.attach(ctx)
        
        try:
            # Get current span and add request attributes
            span = otel_trace.get_current_span()
            if span.is_recording():
                span.set_attributes({
                    "http.method": request.method,
                    "http.url": str(request.url),
                    "http.scheme": request.url.scheme,
                    "http.host": request.url.hostname,
                    "http.target": request.url.path,
                    "http.user_agent": request.headers.get("user-agent", ""),
                })
            
            # Process request
            response = await call_next(request)
            
            # Add trace headers to response
            if span.is_recording():
                trace_id = format(span.get_span_context().trace_id, '032x')
                span_id = format(span.get_span_context().span_id, '016x')
                response.headers["X-Trace-Id"] = trace_id
                response.headers["X-Span-Id"] = span_id
                
                # Add response status to span
                span.set_attribute("http.status_code", response.status_code)
                
            return response
            
        except Exception as e:
            # Record exception in span
            span = otel_trace.get_current_span()
            if span.is_recording():
                span.record_exception(e)
                span.set_status(otel_trace.Status(otel_trace.StatusCode.ERROR, str(e)))
            raise
            
        finally:
            # Detach context
            context.detach(token)


def create_trace_headers(
    service_name: str,
    operation: str,
    correlation_id: Optional[str] = None
) -> Dict[str, str]:
    """
    Create trace headers for service-to-service communication
    
    Args:
        service_name: Calling service name
        operation: Operation being performed
        correlation_id: Optional correlation ID
        
    Returns:
        Dictionary of trace headers
    """
    propagator = ContextPropagator()
    
    # Add service information to current span
    span = otel_trace.get_current_span()
    if span.is_recording():
        span.set_attributes({
            "caller.service": service_name,
            "operation": operation,
        })
        
    # Add correlation ID if provided
    if correlation_id:
        propagator.add_correlation_id(correlation_id)
        
    # Inject and return headers
    return propagator.inject_to_headers()


def extract_trace_info(headers: Dict[str, str]) -> Dict[str, Optional[str]]:
    """
    Extract trace information from headers
    
    Args:
        headers: Request headers
        
    Returns:
        Dictionary with trace_id, span_id, and parent_id
    """
    traceparent = headers.get("traceparent", "")
    
    if not traceparent:
        return {"trace_id": None, "span_id": None, "parent_id": None}
        
    # Parse W3C traceparent header
    # Format: version-trace_id-parent_id-trace_flags
    parts = traceparent.split("-")
    if len(parts) != 4:
        return {"trace_id": None, "span_id": None, "parent_id": None}
        
    return {
        "trace_id": parts[1],
        "span_id": parts[2],
        "parent_id": parts[2],  # Parent ID is the span ID from the header
        "trace_flags": parts[3]
    }


# Global propagator instance
_propagator = ContextPropagator()


def get_propagator() -> ContextPropagator:
    """Get global context propagator instance"""
    return _propagator