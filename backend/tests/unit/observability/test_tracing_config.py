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
