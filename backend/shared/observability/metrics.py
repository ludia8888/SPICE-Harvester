"""
Metrics Collection for OpenTelemetry
Based on Context7 recommendations for observability

This module provides metrics collection for monitoring system performance.
"""

import time
from typing import Any, Dict, Mapping, Optional
from functools import wraps
from contextlib import contextmanager

from opentelemetry import metrics
from opentelemetry.metrics import set_meter_provider
from prometheus_client import Counter as PrometheusCounter, Histogram as PrometheusHistogram, Gauge
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from shared.utils.app_logger import get_logger
from shared.config.settings import get_settings
import logging

logger = get_logger(__name__)

try:  # best-effort: enable OTLP export when SDK is available
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION

    _HAS_OTEL_METRICS_SDK = True
except Exception:  # pragma: no cover - env dependent
    logging.getLogger(__name__).warning("Exception fallback at shared/observability/metrics.py:30", exc_info=True)
    OTLPMetricExporter = None
    MeterProvider = None
    PeriodicExportingMetricReader = None
    Resource = None
    SERVICE_NAME = None
    SERVICE_VERSION = None
    _HAS_OTEL_METRICS_SDK = False


class _SettingsValue:
    def __init__(self, getter):  # noqa: ANN001
        self._getter = getter

    def __get__(self, instance: object, owner: type | None = None):  # noqa: ANN001
        return self._getter(get_settings())


class OpenTelemetryMetricsConfig:
    SERVICE_VERSION = _SettingsValue(lambda s: str(s.observability.otel_service_version or "1.0.0").strip() or "1.0.0")
    ENVIRONMENT = _SettingsValue(lambda s: (s.observability.otel_environment or s.environment.value).strip())

    OTLP_ENDPOINT = _SettingsValue(lambda s: (s.observability.otel_exporter_otlp_endpoint or "").strip())

    ENABLE_METRICS = _SettingsValue(lambda s: bool(s.observability.otel_enable_metrics))

    EXPORT_OTLP = _SettingsValue(lambda s: bool(s.observability.otel_export_otlp_effective))

    EXPORT_INTERVAL_SECONDS = _SettingsValue(lambda s: float(s.observability.otel_metric_export_interval_seconds))


_metrics_provider_initialized = False
_metrics_provider_no_op_logged = False
_metrics_provider_no_op_reason: Optional[str] = None
_metrics_provider_active_exporters: list[str] = []


def _log_no_op_once(reason: str) -> None:
    global _metrics_provider_no_op_logged, _metrics_provider_no_op_reason
    _metrics_provider_no_op_reason = reason
    if _metrics_provider_no_op_logged:
        return
    _metrics_provider_no_op_logged = True
    logger.warning("OpenTelemetry metrics running in no-op mode: %s", reason)


def initialize_metrics_provider(*, service_name: str) -> None:
    """
    Configure a global MeterProvider so OTel metrics are actually exported.

    Safe to call multiple times; first call wins (per-process).
    """

    global _metrics_provider_active_exporters, _metrics_provider_initialized, _metrics_provider_no_op_reason
    if _metrics_provider_initialized:
        return
    _metrics_provider_active_exporters = []

    if not OpenTelemetryMetricsConfig.ENABLE_METRICS:
        _log_no_op_once("OTEL_ENABLE_METRICS=false")
        _metrics_provider_initialized = True
        return

    if not _HAS_OTEL_METRICS_SDK:
        _log_no_op_once("OpenTelemetry metrics SDK not available")
        _metrics_provider_initialized = True
        return

    resource = Resource.create(
        {
            SERVICE_NAME: service_name,
            SERVICE_VERSION: OpenTelemetryMetricsConfig.SERVICE_VERSION,
            "environment": OpenTelemetryMetricsConfig.ENVIRONMENT,
            "service.namespace": "spice-harvester",
        }
    )

    readers = []
    if OpenTelemetryMetricsConfig.EXPORT_OTLP:
        if OTLPMetricExporter is None:
            _log_no_op_once("OTLP exporter requested but opentelemetry-exporter-otlp is not installed")
        elif not OpenTelemetryMetricsConfig.OTLP_ENDPOINT:
            _log_no_op_once("OTLP exporter requested but OTEL_EXPORTER_OTLP_ENDPOINT is not set")
        else:
            try:
                interval_ms = int(max(OpenTelemetryMetricsConfig.EXPORT_INTERVAL_SECONDS, 1.0) * 1000)
                exporter = OTLPMetricExporter(endpoint=OpenTelemetryMetricsConfig.OTLP_ENDPOINT, insecure=True)
                readers.append(PeriodicExportingMetricReader(exporter, export_interval_millis=interval_ms))
                _metrics_provider_active_exporters.append("otlp")
                logger.info(
                    "OTLP metrics exporter configured: endpoint=%s interval_s=%s",
                    OpenTelemetryMetricsConfig.OTLP_ENDPOINT,
                    OpenTelemetryMetricsConfig.EXPORT_INTERVAL_SECONDS,
                )
            except Exception as e:
                logging.getLogger(__name__).warning("Exception fallback at shared/observability/metrics.py:119", exc_info=True)
                _log_no_op_once(f"Failed to configure OTLP metric exporter: {e}")

    if not readers:
        _log_no_op_once("Metrics enabled but no exporters configured/available")
        _metrics_provider_initialized = True
        return

    try:
        provider = MeterProvider(resource=resource, metric_readers=readers)
        set_meter_provider(provider)
        _metrics_provider_initialized = True
        _metrics_provider_no_op_reason = None
        logger.info("OpenTelemetry metrics initialized: service=%s", service_name)
    except Exception as e:
        logging.getLogger(__name__).warning("Exception fallback at shared/observability/metrics.py:132", exc_info=True)
        _log_no_op_once(f"Failed to set meter provider: {e}")
        _metrics_provider_initialized = True

_PROM_COUNTERS: Dict[str, PrometheusCounter] = {}
_PROM_HISTOGRAMS: Dict[str, PrometheusHistogram] = {}
_PROM_GAUGES: Dict[str, Gauge] = {}


def _prom_counter(name: str, description: str, *, labelnames: tuple[str, ...]) -> PrometheusCounter:
    metric = _PROM_COUNTERS.get(name)
    if metric is None:
        metric = PrometheusCounter(name, description, labelnames=labelnames)
        _PROM_COUNTERS[name] = metric
    return metric


def _prom_histogram(name: str, description: str, *, labelnames: tuple[str, ...]) -> PrometheusHistogram:
    metric = _PROM_HISTOGRAMS.get(name)
    if metric is None:
        metric = PrometheusHistogram(name, description, labelnames=labelnames)
        _PROM_HISTOGRAMS[name] = metric
    return metric


def _prom_gauge(name: str, description: str, *, labelnames: tuple[str, ...]) -> Gauge:
    metric = _PROM_GAUGES.get(name)
    if metric is None:
        metric = Gauge(name, description, labelnames=labelnames)
        _PROM_GAUGES[name] = metric
    return metric


def _normalize_label_value(
    value: Any,
    *,
    default: str = "unknown",
    max_len: int = 128,
) -> str:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    return text[:max_len]


def _normalize_bool_label(value: Any) -> str:
    return "true" if bool(value) else "false"


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
        self._prom: Dict[str, Any] = {}
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

            # Error taxonomy metrics
            self._metrics["errors_total"] = self.meter.create_counter(
                name="spice_errors_total",
                description="Total error envelopes emitted by enterprise taxonomy labels",
                unit="1",
            )
            self._metrics["runtime_fallback_total"] = self.meter.create_counter(
                name="spice_runtime_fallback_total",
                description="Total runtime fallback events emitted by runtime exception policy",
                unit="1",
            )

            # Pipeline scheduler metrics
            self._metrics["pipeline_scheduler_ticks"] = self.meter.create_counter(
                name="pipeline_scheduler_ticks_total",
                description="Total pipeline scheduler ticks",
                unit="1",
            )
            self._metrics["pipeline_scheduler_tick_errors"] = self.meter.create_counter(
                name="pipeline_scheduler_tick_errors_total",
                description="Total pipeline scheduler tick errors",
                unit="1",
            )
            self._metrics["pipeline_scheduler_enqueued_jobs"] = self.meter.create_counter(
                name="pipeline_scheduler_enqueued_jobs_total",
                description="Total pipeline jobs enqueued by scheduler",
                unit="1",
            )

            # Ingest reconciler metrics
            self._metrics["ingest_reconciler_runs"] = self.meter.create_counter(
                name="ingest_reconciler_runs_total",
                description="Total ingest reconciler runs",
                unit="1",
            )
            self._metrics["ingest_reconciler_published"] = self.meter.create_counter(
                name="ingest_reconciler_published_total",
                description="Total ingests published by reconciler",
                unit="1",
            )
            self._metrics["ingest_reconciler_aborted"] = self.meter.create_counter(
                name="ingest_reconciler_aborted_total",
                description="Total ingests aborted by reconciler",
                unit="1",
            )
            self._metrics["ingest_reconciler_committed_tx"] = self.meter.create_counter(
                name="ingest_reconciler_committed_tx_total",
                description="Total ingest transactions repaired to COMMITTED",
                unit="1",
            )
            self._metrics["ingest_reconciler_skipped"] = self.meter.create_counter(
                name="ingest_reconciler_skipped_total",
                description="Total reconciler runs skipped due to lock contention",
                unit="1",
            )
            self._metrics["ingest_reconciler_errors"] = self.meter.create_counter(
                name="ingest_reconciler_errors_total",
                description="Total ingest reconciler errors",
                unit="1",
            )
            self._metrics["ingest_reconciler_alerts"] = self.meter.create_counter(
                name="ingest_reconciler_alerts_total",
                description="Total ingest reconciler alerts sent",
                unit="1",
            )
            self._metrics["ingest_reconciler_alert_failures"] = self.meter.create_counter(
                name="ingest_reconciler_alert_failures_total",
                description="Total ingest reconciler alert delivery failures",
                unit="1",
            )
            self._metrics["ingest_reconciler_duration_seconds"] = self.meter.create_histogram(
                name="ingest_reconciler_duration_seconds",
                description="Ingest reconciler run duration in seconds",
                unit="s",
            )
            
            logger.info(f"Metrics initialized for service: {self.service_name}")

            # Prometheus fallback/primary export (used by /metrics).
            # Keep names stable and prefixed to avoid collisions.
            self._prom["http_requests_total"] = _prom_counter(
                "spice_http_requests_total",
                "Total number of HTTP requests",
                labelnames=("service", "method", "endpoint", "status_code"),
            )
            self._prom["http_request_duration_seconds"] = _prom_histogram(
                "spice_http_request_duration_seconds",
                "HTTP request duration in seconds",
                labelnames=("service", "method", "endpoint"),
            )
            self._prom["db_queries_total"] = _prom_counter(
                "spice_db_queries_total",
                "Total number of database queries",
                labelnames=("service", "operation", "table", "success"),
            )
            self._prom["db_query_duration_seconds"] = _prom_histogram(
                "spice_db_query_duration_seconds",
                "Database query duration in seconds",
                labelnames=("service", "operation", "table"),
            )
            self._prom["cache_access_total"] = _prom_counter(
                "spice_cache_access_total",
                "Cache access count (hit/miss)",
                labelnames=("service", "cache", "result"),
            )
            self._prom["events_total"] = _prom_counter(
                "spice_events_total",
                "Events published/processed",
                labelnames=("service", "event_type", "action"),
            )
            self._prom["event_processing_duration_seconds"] = _prom_histogram(
                "spice_event_processing_duration_seconds",
                "Event processing duration in seconds",
                labelnames=("service", "event_type"),
            )
            self._prom["rate_limit_total"] = _prom_counter(
                "spice_rate_limit_total",
                "Rate limit checks/rejections",
                labelnames=("service", "endpoint", "strategy", "result"),
            )
            self._prom["business_total"] = _prom_counter(
                "spice_business_total",
                "Business counters",
                labelnames=("service", "metric"),
            )
            self._prom["errors_total"] = _prom_counter(
                "spice_errors_total",
                "Error envelopes emitted by enterprise taxonomy labels",
                labelnames=(
                    "service",
                    "source",
                    "code",
                    "category",
                    "http_status",
                    "enterprise_code",
                    "enterprise_class",
                    "enterprise_domain",
                    "retryable",
                ),
            )
            self._prom["runtime_fallback_total"] = _prom_counter(
                "spice_runtime_fallback_total",
                "Runtime fallback events emitted by runtime exception policy",
                labelnames=("service", "zone", "operation", "error_code", "error_category"),
            )
            self._prom["observability_signal_enabled"] = _prom_gauge(
                "spice_observability_signal_enabled",
                "Whether an observability signal is configured to be enabled (1=true, 0=false)",
                labelnames=("service", "signal"),
            )
            self._prom["observability_signal_active"] = _prom_gauge(
                "spice_observability_signal_active",
                "Whether an observability signal is active at runtime (1=true, 0=false)",
                labelnames=("service", "signal"),
            )
            self._prom["observability_exporter_enabled"] = _prom_gauge(
                "spice_observability_exporter_enabled",
                "Whether an observability exporter is enabled by config (1=true, 0=false)",
                labelnames=("service", "signal", "exporter"),
            )
            self._prom["observability_exporter_active"] = _prom_gauge(
                "spice_observability_exporter_active",
                "Whether an observability exporter is active at runtime (1=true, 0=false)",
                labelnames=("service", "signal", "exporter"),
            )
            
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

        try:
            self._prom["http_requests_total"].labels(
                self.service_name, method, endpoint, str(status_code)
            ).inc()
            self._prom["http_request_duration_seconds"].labels(
                self.service_name, method, endpoint
            ).observe(duration)
        except Exception as e:
            logger.error(f"Failed to record Prometheus request metrics: {e}")
    
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

        try:
            self._prom["db_queries_total"].labels(
                self.service_name, operation, table, str(bool(success))
            ).inc()
            self._prom["db_query_duration_seconds"].labels(
                self.service_name, operation, table
            ).observe(duration)
        except Exception as e:
            logger.error(f"Failed to record Prometheus DB metrics: {e}")
    
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

        try:
            self._prom["cache_access_total"].labels(
                self.service_name, cache_name, "hit" if hit else "miss"
            ).inc()
        except Exception as e:
            logger.error(f"Failed to record Prometheus cache metrics: {e}")
    
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

        try:
            self._prom["events_total"].labels(self.service_name, event_type, action).inc()
            if action == "processed" and duration:
                self._prom["event_processing_duration_seconds"].labels(self.service_name, event_type).observe(
                    duration
                )
        except Exception as e:
            logger.error(f"Failed to record Prometheus event metrics: {e}")
    
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

        try:
            self._prom["rate_limit_total"].labels(
                self.service_name,
                endpoint,
                strategy,
                "rejected" if rejected else "allowed",
            ).inc()
        except Exception as e:
            logger.error(f"Failed to record Prometheus rate limit metrics: {e}")
    
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

        try:
            self._prom["business_total"].labels(self.service_name, metric_name).inc(float(value) if value else 1.0)
        except Exception as e:
            logger.error(f"Failed to record Prometheus business metric {metric_name}: {e}")

    def record_error_envelope(
        self,
        payload: Mapping[str, Any],
        *,
        source: str = "http",
    ) -> None:
        enterprise = payload.get("enterprise") if isinstance(payload.get("enterprise"), Mapping) else {}
        attributes = {
            "service": _normalize_label_value(self.service_name),
            "source": _normalize_label_value(source),
            "code": _normalize_label_value(payload.get("code"), default="INTERNAL_ERROR"),
            "category": _normalize_label_value(payload.get("category"), default="internal"),
            "http_status": _normalize_label_value(payload.get("http_status"), default="500"),
            "enterprise_code": _normalize_label_value(enterprise.get("code"), default="unknown"),
            "enterprise_class": _normalize_label_value(enterprise.get("class"), default="unknown"),
            "enterprise_domain": _normalize_label_value(enterprise.get("domain"), default="unknown"),
            "retryable": _normalize_bool_label(payload.get("retryable")),
        }

        try:
            if "errors_total" in self._metrics:
                self._metrics["errors_total"].add(1, attributes)
        except Exception as e:
            logger.error(f"Failed to record error taxonomy metrics: {e}")

        try:
            if "errors_total" in self._prom:
                self._prom["errors_total"].labels(
                    attributes["service"],
                    attributes["source"],
                    attributes["code"],
                    attributes["category"],
                    attributes["http_status"],
                    attributes["enterprise_code"],
                    attributes["enterprise_class"],
                    attributes["enterprise_domain"],
                    attributes["retryable"],
                ).inc()
        except Exception as e:
            logger.error(f"Failed to record Prometheus error taxonomy metrics: {e}")

    def record_runtime_fallback(
        self,
        *,
        zone: str,
        operation: str,
        error_code: str,
        error_category: str,
    ) -> None:
        attributes = {
            "service": _normalize_label_value(self.service_name),
            "zone": _normalize_label_value(zone),
            "operation": _normalize_label_value(operation),
            "error_code": _normalize_label_value(error_code),
            "error_category": _normalize_label_value(error_category),
        }

        try:
            if "runtime_fallback_total" in self._metrics:
                self._metrics["runtime_fallback_total"].add(1, attributes)
        except Exception as e:
            logger.error(f"Failed to record runtime fallback metrics: {e}")

        try:
            if "runtime_fallback_total" in self._prom:
                self._prom["runtime_fallback_total"].labels(
                    attributes["service"],
                    attributes["zone"],
                    attributes["operation"],
                    attributes["error_code"],
                    attributes["error_category"],
                ).inc()
        except Exception as e:
            logger.error(f"Failed to record Prometheus runtime fallback metrics: {e}")

    def record_observability_bootstrap(
        self,
        *,
        tracing_status: Optional[Mapping[str, Any]] = None,
        metrics_status: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if "observability_signal_enabled" not in self._prom:
            return

        tracing_enabled = bool((tracing_status or {}).get("enabled"))
        tracing_active = bool((tracing_status or {}).get("active"))
        tracing_exporters_enabled = {
            _normalize_label_value(name)
            for name in ((tracing_status or {}).get("exporters_enabled") or [])
            if str(name or "").strip()
        }
        tracing_exporters_active = {
            _normalize_label_value(name)
            for name in ((tracing_status or {}).get("exporters_active") or [])
            if str(name or "").strip()
        }

        metrics_enabled = bool((metrics_status or {}).get("enabled"))
        metrics_active = bool((metrics_status or {}).get("active"))
        metrics_exporters_enabled = {
            _normalize_label_value(name)
            for name in ((metrics_status or {}).get("exporters_enabled") or [])
            if str(name or "").strip()
        }
        metrics_exporters_active = {
            _normalize_label_value(name)
            for name in ((metrics_status or {}).get("exporters_active") or [])
            if str(name or "").strip()
        }

        try:
            self._prom["observability_signal_enabled"].labels(self.service_name, "tracing").set(1 if tracing_enabled else 0)
            self._prom["observability_signal_active"].labels(self.service_name, "tracing").set(1 if tracing_active else 0)
            self._prom["observability_signal_enabled"].labels(self.service_name, "metrics").set(1 if metrics_enabled else 0)
            self._prom["observability_signal_active"].labels(self.service_name, "metrics").set(1 if metrics_active else 0)

            for exporter in sorted(tracing_exporters_enabled | tracing_exporters_active | {"otlp", "jaeger", "console"}):
                self._prom["observability_exporter_enabled"].labels(
                    self.service_name, "tracing", exporter
                ).set(1 if exporter in tracing_exporters_enabled else 0)
                self._prom["observability_exporter_active"].labels(
                    self.service_name, "tracing", exporter
                ).set(1 if exporter in tracing_exporters_active else 0)

            for exporter in sorted(metrics_exporters_enabled | metrics_exporters_active | {"otlp"}):
                self._prom["observability_exporter_enabled"].labels(
                    self.service_name, "metrics", exporter
                ).set(1 if exporter in metrics_exporters_enabled else 0)
                self._prom["observability_exporter_active"].labels(
                    self.service_name, "metrics", exporter
                ).set(1 if exporter in metrics_exporters_active else 0)
        except Exception as e:
            logger.error(f"Failed to record observability bootstrap metrics: {e}")
    
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
    
    def __init__(self, metrics_collector: MetricsCollector):
        """
        Initialize middleware
        
        Args:
            app: FastAPI application
            metrics_collector: MetricsCollector instance
        """
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
            
        except Exception:
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


def prometheus_latest() -> bytes:
    """
    Render Prometheus metrics for `/metrics`.
    """
    return generate_latest()


PROMETHEUS_CONTENT_TYPE_LATEST = CONTENT_TYPE_LATEST


# Global metrics collector cache (per service in-process)
_metrics_collectors: Dict[str, MetricsCollector] = {}


def get_metrics_collector(service_name: str) -> MetricsCollector:
    """
    Get or create global metrics collector
    
    Args:
        service_name: Name of the service
        
    Returns:
        MetricsCollector instance
    """
    key = _normalize_label_value(service_name, default="spice-harvester")
    collector = _metrics_collectors.get(key)
    if collector is not None:
        return collector

    initialize_metrics_provider(service_name=key)
    collector = MetricsCollector(key)
    _metrics_collectors[key] = collector
    return collector


def get_metrics_runtime_status(service_name: Optional[str] = None) -> Dict[str, Any]:
    settings = get_settings()
    resolved_service = _normalize_label_value(
        service_name or settings.observability.service_name_effective,
        default="spice-harvester",
    )
    enabled = bool(OpenTelemetryMetricsConfig.ENABLE_METRICS)
    exporters_enabled: list[str] = []
    if bool(OpenTelemetryMetricsConfig.EXPORT_OTLP):
        exporters_enabled.append("otlp")
    active_exporters = list(_metrics_provider_active_exporters)
    active = bool(enabled and _metrics_provider_initialized and active_exporters)
    return {
        "service": resolved_service,
        "enabled": enabled,
        "provider_initialized": bool(_metrics_provider_initialized),
        "active": active,
        "no_op_reason": _metrics_provider_no_op_reason,
        "exporters_enabled": exporters_enabled,
        "exporters_active": active_exporters,
        "otlp_endpoint": _normalize_label_value(OpenTelemetryMetricsConfig.OTLP_ENDPOINT, default=""),
        "export_interval_seconds": float(OpenTelemetryMetricsConfig.EXPORT_INTERVAL_SECONDS),
    }
