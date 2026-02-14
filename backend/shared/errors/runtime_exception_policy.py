from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Generic, Mapping, Optional, Tuple, TypeVar

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.observability.request_context import get_correlation_id, get_request_id

T = TypeVar("T")
R = TypeVar("R")


class RuntimeZone(str, Enum):
    CORE = "core"
    ADAPTER = "adapter"
    OBSERVABILITY = "observability"
    MCP = "mcp"


DEFAULT_ALLOWED_EXCEPTIONS: Tuple[type[BaseException], ...] = (
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    OSError,
    RuntimeError,
)


@dataclass(frozen=True)
class FallbackPolicy(Generic[T]):
    zone: RuntimeZone
    operation: str
    default_factory: Callable[[], T]
    allowed_exceptions: Tuple[type[BaseException], ...] = DEFAULT_ALLOWED_EXCEPTIONS
    code: ErrorCode = ErrorCode.INTERNAL_ERROR
    category: ErrorCategory = ErrorCategory.INTERNAL
    warn_interval_seconds: float = 60.0


@dataclass
class _RateState:
    last_warning_ts: float
    suppressed_count: int = 0


_RATE_STATE: Dict[str, _RateState] = {}
_RATE_LOCK = threading.Lock()


def _current_trace_id() -> Optional[str]:
    try:
        from opentelemetry import trace as otel_trace

        span = otel_trace.get_current_span()
        ctx = span.get_span_context() if span is not None else None
        if ctx is None:
            return None
        is_valid = getattr(ctx, "is_valid", False)
        if callable(is_valid):
            is_valid = is_valid()
        if not is_valid:
            return None
        trace_id = getattr(ctx, "trace_id", 0) or 0
        if not trace_id:
            return None
        return format(int(trace_id), "032x")
    except (ImportError, AttributeError, TypeError, ValueError):
        return None


def _safe_text(value: Any) -> str:
    try:
        return str(value)
    except (TypeError, ValueError, RuntimeError):
        return "<unprintable>"


def _exception_fingerprint(
    *,
    zone: RuntimeZone,
    operation: str,
    exc: BaseException,
    code: ErrorCode,
) -> str:
    payload = "|".join(
        [
            zone.value,
            operation,
            code.value,
            exc.__class__.__name__,
            _safe_text(exc),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()[:20]


def _record_runtime_fallback_metric(
    *,
    zone: RuntimeZone,
    operation: str,
    code: ErrorCode,
    category: ErrorCategory,
) -> None:
    try:
        from shared.config.settings import get_settings
        from shared.observability.metrics import get_metrics_collector

        service_name = get_settings().observability.service_name_effective
        collector = get_metrics_collector(service_name)
        record = getattr(collector, "record_runtime_fallback", None)
        if callable(record):
            record(
                zone=zone.value,
                operation=operation,
                error_code=code.value,
                error_category=category.value,
            )
    except (ImportError, AttributeError, RuntimeError, TypeError, ValueError):
        # Runtime fallback telemetry must not break primary control flow.
        return


def log_exception_rate_limited(
    logger: logging.Logger,
    *,
    zone: RuntimeZone,
    operation: str,
    exc: BaseException,
    code: ErrorCode,
    category: ErrorCategory,
    warn_interval_seconds: float = 60.0,
    context: Optional[Mapping[str, Any]] = None,
) -> str:
    fingerprint = _exception_fingerprint(zone=zone, operation=operation, exc=exc, code=code)
    now = time.monotonic()
    warning_emitted = False
    suppressed_since_last = 0

    with _RATE_LOCK:
        state = _RATE_STATE.get(fingerprint)
        if state is None:
            _RATE_STATE[fingerprint] = _RateState(last_warning_ts=now, suppressed_count=0)
            warning_emitted = True
        elif now - state.last_warning_ts >= max(1.0, float(warn_interval_seconds)):
            suppressed_since_last = state.suppressed_count
            state.last_warning_ts = now
            state.suppressed_count = 0
            warning_emitted = True
        else:
            state.suppressed_count += 1

    payload: Dict[str, Any] = {
        "error_code": code.value,
        "error_category": category.value,
        "operation": operation,
        "zone": zone.value,
        "fingerprint": fingerprint,
        "request_id": get_request_id(),
        "correlation_id": get_correlation_id(),
        "trace_id": _current_trace_id(),
    }
    if context:
        payload.update({str(k): v for k, v in context.items()})
    if suppressed_since_last:
        payload["suppressed_since_last"] = suppressed_since_last

    if warning_emitted:
        logger.warning(
            "Runtime fallback triggered (%s/%s): %s",
            zone.value,
            operation,
            _safe_text(exc),
            extra={"runtime_fallback": payload},
            exc_info=True,
        )
    else:
        logger.debug(
            "Runtime fallback suppressed duplicate (%s/%s): %s",
            zone.value,
            operation,
            _safe_text(exc),
            extra={"runtime_fallback": payload},
        )

    _record_runtime_fallback_metric(
        zone=zone,
        operation=operation,
        code=code,
        category=category,
    )

    return fingerprint


def fallback_value(
    *,
    policy: FallbackPolicy[T],
    exc: BaseException,
    logger: logging.Logger,
    context: Optional[Mapping[str, Any]] = None,
) -> T:
    if not isinstance(exc, policy.allowed_exceptions):
        raise exc

    log_exception_rate_limited(
        logger,
        zone=policy.zone,
        operation=policy.operation,
        exc=exc,
        code=policy.code,
        category=policy.category,
        warn_interval_seconds=policy.warn_interval_seconds,
        context=context,
    )
    return policy.default_factory()


def preserve_primary_exception(
    *,
    primary_exc: Optional[BaseException],
    cleanup_exc: BaseException,
    logger: logging.Logger,
    operation: str,
    zone: RuntimeZone = RuntimeZone.CORE,
    code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    category: ErrorCategory = ErrorCategory.INTERNAL,
    warn_interval_seconds: float = 60.0,
    context: Optional[Mapping[str, Any]] = None,
) -> None:
    if primary_exc is None:
        raise cleanup_exc

    log_exception_rate_limited(
        logger,
        zone=zone,
        operation=operation,
        exc=cleanup_exc,
        code=code,
        category=category,
        warn_interval_seconds=warn_interval_seconds,
        context=context,
    )


class LineageUnavailableError(RuntimeError):
    """Raised when lineage is required but no lineage store is available."""


class LineageRecordError(RuntimeError):
    """Raised when lineage recording fails in fail-closed mode."""


def assert_lineage_available(
    *,
    lineage_store: Any,
    required: bool,
    logger: logging.Logger,
    operation: str,
    zone: RuntimeZone = RuntimeZone.OBSERVABILITY,
    context: Optional[Mapping[str, Any]] = None,
    warn_interval_seconds: float = 60.0,
) -> bool:
    if lineage_store is not None:
        return True

    unavailable_exc = LineageUnavailableError("LineageStore unavailable")
    if required:
        raise unavailable_exc

    log_exception_rate_limited(
        logger,
        zone=zone,
        operation=operation,
        exc=unavailable_exc,
        code=ErrorCode.LINEAGE_UNAVAILABLE,
        category=ErrorCategory.UPSTREAM,
        warn_interval_seconds=warn_interval_seconds,
        context=context,
    )
    return False


async def record_lineage_or_raise(
    *,
    lineage_store: Any,
    required: bool,
    record_call: Callable[[], Awaitable[R]],
    logger: logging.Logger,
    operation: str,
    zone: RuntimeZone = RuntimeZone.CORE,
    context: Optional[Mapping[str, Any]] = None,
    warn_interval_seconds: float = 60.0,
) -> Optional[R]:
    if not assert_lineage_available(
        lineage_store=lineage_store,
        required=required,
        logger=logger,
        operation=f"{operation}.availability",
        zone=zone,
        context=context,
        warn_interval_seconds=warn_interval_seconds,
    ):
        return None

    try:
        return await record_call()
    except Exception as exc:
        if required:
            raise LineageRecordError("lineage_record_failed") from exc

        log_exception_rate_limited(
            logger,
            zone=zone,
            operation=operation,
            exc=exc,
            code=ErrorCode.LINEAGE_RECORD_FAILED,
            category=ErrorCategory.INTERNAL,
            warn_interval_seconds=warn_interval_seconds,
            context=context,
        )
        return None
