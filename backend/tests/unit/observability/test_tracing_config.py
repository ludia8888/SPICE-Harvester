import importlib
import os
from contextlib import contextmanager

import pytest

import shared.observability.tracing as tracing


@contextmanager
def _set_env(**updates):
    original = {key: os.environ.get(key) for key in updates}
    for key, value in updates.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@pytest.mark.unit
def test_otlp_export_disabled_when_no_endpoint():
    with _set_env(OTEL_EXPORTER_OTLP_ENDPOINT=None, OTEL_EXPORT_OTLP=None):
        importlib.reload(tracing)
        assert tracing.OpenTelemetryConfig.EXPORT_OTLP is False


@pytest.mark.unit
def test_span_omits_kind_when_none() -> None:
    if not tracing.HAS_OPENTELEMETRY:
        pytest.skip("OpenTelemetry not available in this environment")

    called: dict[str, object] = {}

    class _Span:
        def record_exception(self, _exc: Exception) -> None:
            called["record_exception"] = True

        def set_status(self, _status: object) -> None:
            called["set_status"] = True

    class _SpanContext:
        def __enter__(self) -> _Span:
            return _Span()

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

    class _Tracer:
        def start_as_current_span(self, name: str, **kwargs):  # noqa: ANN001
            called["name"] = name
            called["kwargs"] = kwargs
            return _SpanContext()

    service = tracing.TracingService("unit-test")
    service.tracer = _Tracer()

    with service.span("test-span"):
        pass

    kwargs = called.get("kwargs")
    assert isinstance(kwargs, dict)
    assert "kind" not in kwargs


@pytest.mark.unit
def test_span_passes_kind_when_set() -> None:
    if not tracing.HAS_OPENTELEMETRY:
        pytest.skip("OpenTelemetry not available in this environment")

    called: dict[str, object] = {}
    sentinel_kind = object()

    class _Span:
        def record_exception(self, _exc: Exception) -> None:
            called["record_exception"] = True

        def set_status(self, _status: object) -> None:
            called["set_status"] = True

    class _SpanContext:
        def __enter__(self) -> _Span:
            return _Span()

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

    class _Tracer:
        def start_as_current_span(self, name: str, **kwargs):  # noqa: ANN001
            called["name"] = name
            called["kwargs"] = kwargs
            return _SpanContext()

    service = tracing.TracingService("unit-test")
    service.tracer = _Tracer()

    with service.span("test-span", kind=sentinel_kind):
        pass

    kwargs = called.get("kwargs")
    assert isinstance(kwargs, dict)
    assert kwargs.get("kind") is sentinel_kind
