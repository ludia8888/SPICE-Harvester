"""
Metrics Collection for OpenTelemetry
Based on Context7 recommendations for observability

This module provides metrics collection for monitoring system performance.
"""

import time
from typing import Dict, Optional, Any, Callable
from functools import wraps
from contextlib import contextmanager

from opentelemetry import metrics
from opentelemetry.metrics import Counter, Histogram, UpDownCounter, ObservableGauge
from prometheus_client import Counter as PrometheusCounter, Histogram as PrometheusHistogram, Gauge

from shared.utils.app_logger import get_logger

logger = get_logger(__name__)


class MetricsCollector:
    """
    Centralized metrics collection based on Context7 patterns
    """
    
    def __init__(self, service_name: str):
        """
        Initialize metrics collector
        
        Args:
            service_name: Name of the service
        """
        self.service_name = service_name
        self.meter = metrics.get_meter(service_name, "1.0.0")
        self._metrics: Dict[str, Any] = {}
        self._initialize_metrics()
        
    def _initialize_metrics(self) -> None:
        """Initialize all metrics"""
        try:
            # Request metrics
            self._metrics["request_count"] = self.meter.create_counter(
                name="http_requests_total",
                description="Total number of HTTP requests",
                unit="1"
            )
            
            self._metrics["request_duration"] = self.meter.create_histogram(
                name="http_request_duration_seconds",
                description="HTTP request duration in seconds",
                unit="s"
            )
            
            self._metrics["request_size"] = self.meter.create_histogram(
                name="http_request_size_bytes",
                description="HTTP request size in bytes",
                unit="By"
            )
            
            self._metrics["response_size"] = self.meter.create_histogram(
                name="http_response_size_bytes",
                description="HTTP response size in bytes",
                unit="By"
            )
            
            # Database metrics
            self._metrics["db_query_count"] = self.meter.create_counter(
                name="db_queries_total",
                description="Total number of database queries",
                unit="1"
            )
            
            self._metrics["db_query_duration"] = self.meter.create_histogram(
                name="db_query_duration_seconds",
                description="Database query duration in seconds",
                unit="s"
            )
            
            self._metrics["db_connection_pool"] = self.meter.create_up_down_counter(
                name="db_connection_pool_size",
                description="Current database connection pool size",
                unit="1"
            )
            
            # Cache metrics
            self._metrics["cache_hits"] = self.meter.create_counter(
                name="cache_hits_total",
                description="Total number of cache hits",
                unit="1"
            )
            
            self._metrics["cache_misses"] = self.meter.create_counter(
                name="cache_misses_total",
                description="Total number of cache misses",
                unit="1"
            )
            
            # Event sourcing metrics
            self._metrics["events_published"] = self.meter.create_counter(
                name="events_published_total",
                description="Total number of events published",
                unit="1"
            )
            
            self._metrics["events_processed"] = self.meter.create_counter(
                name="events_processed_total",
                description="Total number of events processed",
                unit="1"
            )
            
            self._metrics["event_processing_duration"] = self.meter.create_histogram(
                name="event_processing_duration_seconds",
                description="Event processing duration in seconds",
                unit="s"
            )
            
            # Rate limiting metrics
            self._metrics["rate_limit_hits"] = self.meter.create_counter(
                name="rate_limit_hits_total",
                description="Total number of rate limit hits",
                unit="1"
            )
            
            self._metrics["rate_limit_rejections"] = self.meter.create_counter(
                name="rate_limit_rejections_total",
                description="Total number of requests rejected due to rate limiting",
                unit="1"
            )
            
            # Business metrics
            self._metrics["ontology_created"] = self.meter.create_counter(
                name="ontology_created_total",
                description="Total number of ontologies created",
                unit="1"
            )
            
            self._metrics["ontology_updated"] = self.meter.create_counter(
                name="ontology_updated_total",
                description="Total number of ontologies updated",
                unit="1"
            )
            
            self._metrics["active_users"] = self.meter.create_up_down_counter(
                name="active_users",
                description="Current number of active users",
                unit="1"
            )
            
            logger.info(f"Metrics initialized for service: {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize metrics: {e}")
    
    def record_request(
        self,
        method: str,
        endpoint: str,
        status_code: int,
        duration: float,
        request_size: int = 0,
        response_size: int = 0
    ) -> None:
        """
        Record HTTP request metrics
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            status_code: Response status code
            duration: Request duration in seconds
            request_size: Request body size in bytes
            response_size: Response body size in bytes
        """
        attributes = {
            "method": method,
            "endpoint": endpoint,
            "status_code": str(status_code),
            "service": self.service_name
        }
        
        try:
            self._metrics["request_count"].add(1, attributes)
            self._metrics["request_duration"].record(duration, attributes)
            
            if request_size > 0:
                self._metrics["request_size"].record(request_size, attributes)
            if response_size > 0:
                self._metrics["response_size"].record(response_size, attributes)
                
        except Exception as e:
            logger.error(f"Failed to record request metrics: {e}")
    
    def record_db_query(
        self,
        operation: str,
        table: str,
        duration: float,
        success: bool = True
    ) -> None:
        """
        Record database query metrics
        
        Args:
            operation: Database operation (SELECT, INSERT, UPDATE, DELETE)
            table: Table name
            duration: Query duration in seconds
            success: Whether query succeeded
        """
        attributes = {
            "operation": operation,
            "table": table,
            "success": str(success),
            "service": self.service_name
        }
        
        try:
            self._metrics["db_query_count"].add(1, attributes)
            self._metrics["db_query_duration"].record(duration, attributes)
        except Exception as e:
            logger.error(f"Failed to record database metrics: {e}")
    
    def record_cache_access(self, hit: bool, cache_name: str = "default") -> None:
        """
        Record cache access
        
        Args:
            hit: Whether it was a cache hit
            cache_name: Name of the cache
        """
        attributes = {"cache": cache_name, "service": self.service_name}
        
        try:
            if hit:
                self._metrics["cache_hits"].add(1, attributes)
            else:
                self._metrics["cache_misses"].add(1, attributes)
        except Exception as e:
            logger.error(f"Failed to record cache metrics: {e}")
    
    def record_event(
        self,
        event_type: str,
        action: str = "published",
        duration: Optional[float] = None
    ) -> None:
        """
        Record event sourcing metrics
        
        Args:
            event_type: Type of event
            action: Action taken (published/processed)
            duration: Processing duration if applicable
        """
        attributes = {
            "event_type": event_type,
            "service": self.service_name
        }
        
        try:
            if action == "published":
                self._metrics["events_published"].add(1, attributes)
            elif action == "processed":
                self._metrics["events_processed"].add(1, attributes)
                if duration:
                    self._metrics["event_processing_duration"].record(duration, attributes)
        except Exception as e:
            logger.error(f"Failed to record event metrics: {e}")
    
    def record_rate_limit(
        self,
        endpoint: str,
        rejected: bool = False,
        strategy: str = "ip"
    ) -> None:
        """
        Record rate limiting metrics
        
        Args:
            endpoint: API endpoint
            rejected: Whether request was rejected
            strategy: Rate limiting strategy used
        """
        attributes = {
            "endpoint": endpoint,
            "strategy": strategy,
            "service": self.service_name
        }
        
        try:
            self._metrics["rate_limit_hits"].add(1, attributes)
            if rejected:
                self._metrics["rate_limit_rejections"].add(1, attributes)
        except Exception as e:
            logger.error(f"Failed to record rate limit metrics: {e}")
    
    def record_business_metric(
        self,
        metric_name: str,
        value: float = 1,
        attributes: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Record custom business metrics
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            attributes: Additional attributes
        """
        if metric_name not in self._metrics:
            logger.warning(f"Unknown metric: {metric_name}")
            return
            
        attrs = attributes or {}
        attrs["service"] = self.service_name
        
        try:
            metric = self._metrics[metric_name]
            if hasattr(metric, "add"):
                metric.add(value, attrs)
            elif hasattr(metric, "record"):
                metric.record(value, attrs)
        except Exception as e:
            logger.error(f"Failed to record business metric {metric_name}: {e}")
    
    @contextmanager
    def timer(self, metric_name: str, attributes: Optional[Dict[str, str]] = None):
        """
        Context manager for timing operations
        
        Args:
            metric_name: Name of the timing metric
            attributes: Additional attributes
            
        Example:
            with metrics.timer("operation_duration"):
                # Timed operation
                pass
        """
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            if metric_name in self._metrics:
                attrs = attributes or {}
                attrs["service"] = self.service_name
                try:
                    self._metrics[metric_name].record(duration, attrs)
                except Exception as e:
                    logger.error(f"Failed to record timer metric {metric_name}: {e}")


def measure_time(metric_name: str, collector: Optional[MetricsCollector] = None):
    """
    Decorator for measuring function execution time
    
    Args:
        metric_name: Name of the timing metric
        collector: MetricsCollector instance (uses global if None)
        
    Example:
        @measure_time("function_duration")
        async def my_function():
            pass
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                if collector:
                    collector.record_business_metric(
                        metric_name,
                        duration,
                        {"function": func.__name__}
                    )
                    
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                if collector:
                    collector.record_business_metric(
                        metric_name,
                        duration,
                        {"function": func.__name__}
                    )
        
        # Return appropriate wrapper
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator


class RequestMetricsMiddleware:
    """
    FastAPI middleware for automatic request metrics collection
    """
    
    def __init__(self, app, metrics_collector: MetricsCollector):
        """
        Initialize middleware
        
        Args:
            app: FastAPI application
            metrics_collector: MetricsCollector instance
        """
        self.app = app
        self.metrics = metrics_collector
        
    async def __call__(self, request, call_next):
        """
        Process request and collect metrics
        """
        start_time = time.time()
        
        # Get request size
        request_size = int(request.headers.get("content-length", 0))
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate duration
            duration = time.time() - start_time
            
            # Get response size
            response_size = int(response.headers.get("content-length", 0))
            
            # Record metrics
            self.metrics.record_request(
                method=request.method,
                endpoint=request.url.path,
                status_code=response.status_code,
                duration=duration,
                request_size=request_size,
                response_size=response_size
            )
            
            return response
            
        except Exception as e:
            # Record failed request
            duration = time.time() - start_time
            self.metrics.record_request(
                method=request.method,
                endpoint=request.url.path,
                status_code=500,
                duration=duration,
                request_size=request_size
            )
            raise


# Global metrics collector
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector(service_name: str) -> MetricsCollector:
    """
    Get or create global metrics collector
    
    Args:
        service_name: Name of the service
        
    Returns:
        MetricsCollector instance
    """
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(service_name)
    return _metrics_collector