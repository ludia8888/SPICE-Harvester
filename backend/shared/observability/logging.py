"""
Logging helpers for observability.

Goals:
- Add trace correlation fields (`trace_id`, `span_id`) to all log records safely.
- Be safe when OpenTelemetry isn't installed/enabled (fields still exist with "-").
- Avoid implicitly initializing tracing providers (no side effects).

Additionally (debuggability):
- Provide `request_id` / `correlation_id` / `db_name` fields (from ContextVars),
  so operators can correlate incidents even without a trace backend.
"""

from __future__ import annotations

import logging
from typing import Optional

from shared.observability.request_context import (
    get_correlation_id,
    get_db_name,
    get_principal,
    get_request_id,
)
_module_logger = logging.getLogger(__name__)

try:  # OpenTelemetry API (no SDK dependency)
    from opentelemetry import trace as otel_trace

    _HAS_OTEL_API = True
except ImportError:  # pragma: no cover - env dependent
    otel_trace = None
    _HAS_OTEL_API = False


_record_factory_installed = False
_previous_record_factory = None


def install_trace_context_record_factory() -> None:
    """
    Install a global LogRecord factory that always provides `trace_id`/`span_id`.

    Why this exists:
    - Some services use `logging.basicConfig(..., format="...%(trace_id)s...")`.
    - Filters are not guaranteed to run on every logger/handler (propagate=False, custom handlers).
    - A LogRecord factory guarantees the fields exist, avoiding runtime logging crashes.
    """

    global _record_factory_installed, _previous_record_factory
    if _record_factory_installed:
        return

    _previous_record_factory = logging.getLogRecordFactory()

    def _factory(*args, **kwargs):  # type: ignore[no-untyped-def]
        record = _previous_record_factory(*args, **kwargs)  # type: ignore[misc]

        # Defaults (stable formatter contract)
        record.trace_id = "-"  # type: ignore[attr-defined]
        record.span_id = "-"  # type: ignore[attr-defined]
        record.request_id = "-"  # type: ignore[attr-defined]
        record.correlation_id = "-"  # type: ignore[attr-defined]
        record.db_name = "-"  # type: ignore[attr-defined]
        record.principal = "-"  # type: ignore[attr-defined]

        try:
            request_id = get_request_id()
            correlation_id = get_correlation_id()
            db_name = get_db_name()
            principal = get_principal()
            if request_id:
                record.request_id = request_id  # type: ignore[attr-defined]
            if correlation_id:
                record.correlation_id = correlation_id  # type: ignore[attr-defined]
            if db_name:
                record.db_name = db_name  # type: ignore[attr-defined]
            if principal:
                record.principal = principal  # type: ignore[attr-defined]
        except (LookupError, RuntimeError, ValueError, TypeError):
            pass

        if not _HAS_OTEL_API or otel_trace is None:
            return record

        try:
            span = otel_trace.get_current_span()
            ctx = span.get_span_context() if span is not None else None
            if ctx is None:
                return record

            is_valid = getattr(ctx, "is_valid", False)
            if callable(is_valid):
                is_valid = is_valid()
            if not is_valid:
                return record

            trace_id = getattr(ctx, "trace_id", 0) or 0
            span_id = getattr(ctx, "span_id", 0) or 0
            if trace_id:
                record.trace_id = format(int(trace_id), "032x")  # type: ignore[attr-defined]
            if span_id:
                record.span_id = format(int(span_id), "016x")  # type: ignore[attr-defined]
        except (AttributeError, RuntimeError, ValueError, TypeError):
            return record

        return record

    logging.setLogRecordFactory(_factory)
    _record_factory_installed = True


class TraceContextFilter(logging.Filter):
    """
    Attach `trace_id` and `span_id` fields to every LogRecord.

    This keeps formatters stable and enables log/trace correlation in any sink.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = "-"  # type: ignore[attr-defined]
        record.span_id = "-"  # type: ignore[attr-defined]
        record.request_id = "-"  # type: ignore[attr-defined]
        record.correlation_id = "-"  # type: ignore[attr-defined]
        record.db_name = "-"  # type: ignore[attr-defined]
        record.principal = "-"  # type: ignore[attr-defined]

        try:
            request_id = get_request_id()
            correlation_id = get_correlation_id()
            db_name = get_db_name()
            principal = get_principal()
            if request_id:
                record.request_id = request_id  # type: ignore[attr-defined]
            if correlation_id:
                record.correlation_id = correlation_id  # type: ignore[attr-defined]
            if db_name:
                record.db_name = db_name  # type: ignore[attr-defined]
            if principal:
                record.principal = principal  # type: ignore[attr-defined]
        except (LookupError, RuntimeError, ValueError, TypeError):
            pass

        if not _HAS_OTEL_API or otel_trace is None:
            return True

        try:
            span = otel_trace.get_current_span()
            ctx = span.get_span_context() if span is not None else None
            if ctx is None:
                return True

            is_valid = getattr(ctx, "is_valid", False)
            if callable(is_valid):
                is_valid = is_valid()
            if not is_valid:
                return True

            trace_id = getattr(ctx, "trace_id", 0) or 0
            span_id = getattr(ctx, "span_id", 0) or 0
            if trace_id:
                record.trace_id = format(int(trace_id), "032x")  # type: ignore[attr-defined]
            if span_id:
                record.span_id = format(int(span_id), "016x")  # type: ignore[attr-defined]
        except (AttributeError, RuntimeError, ValueError, TypeError):
            return True

        return True


_installed = False


def install_trace_context_filter(*, logger: Optional[logging.Logger] = None) -> None:
    """
    Install TraceContextFilter on the given logger (default: root logger).

    This is best-effort and safe to call multiple times.
    """

    # Always ensure the formatter contract holds globally (trace_id/span_id fields exist).
    install_trace_context_record_factory()

    global _installed
    if _installed:
        return

    target = logger or logging.getLogger()
    filt = TraceContextFilter()

    # Logger-level filter.
    target.addFilter(filt)

    # Handler-level filters (covers libraries using their own logger instances).
    for handler in list(target.handlers):
        try:
            handler.addFilter(filt)
        except (AttributeError, RuntimeError, ValueError, TypeError) as exc:
            _module_logger.warning("Failed to install trace context filter on handler %r: %s", handler, exc, exc_info=True)
            continue

    _installed = True
