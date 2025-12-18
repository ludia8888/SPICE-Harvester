"""
Event store TLS guard tests (no mocks).
"""

from __future__ import annotations

import os
from contextlib import contextmanager

import pytest

from shared.services.event_store import EventStore


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_event_store_tls_requirement():
    with _set_env(
        EVENT_STORE_REQUIRE_TLS="true",
        MINIO_ENDPOINT_URL="http://localhost:9000",
    ):
        store = EventStore()
        with pytest.raises(RuntimeError):
            await store.connect()
