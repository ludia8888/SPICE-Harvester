from __future__ import annotations

import asyncio
from typing import Any

import pytest

from shared.errors.runtime_exception_policy import LineageRecordError, LineageUnavailableError
from shared.models.event_envelope import EventEnvelope
from shared.services.storage import event_store as event_store_module
from shared.services.storage.event_store import EventStore


class _DummyS3:
    def __init__(self, counters: dict[str, int]) -> None:
        self._counters = counters

    async def head_bucket(self, **_: Any) -> None:
        self._counters["head_bucket"] += 1

    async def create_bucket(self, **_: Any) -> None:
        self._counters["create_bucket"] += 1

    async def put_bucket_versioning(self, **_: Any) -> None:
        self._counters["put_bucket_versioning"] += 1


class _DummyS3ClientContext:
    def __init__(self, s3: _DummyS3) -> None:
        self._s3 = s3

    async def __aenter__(self) -> _DummyS3:
        return self._s3

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _DummySession:
    def __init__(self, s3: _DummyS3) -> None:
        self._s3 = s3

    def client(self, **_: Any) -> _DummyS3ClientContext:
        return _DummyS3ClientContext(self._s3)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_event_store_connect_is_idempotent_under_concurrency(monkeypatch: pytest.MonkeyPatch) -> None:
    counters = {"head_bucket": 0, "create_bucket": 0, "put_bucket_versioning": 0, "init_lineage_audit": 0}
    dummy_s3 = _DummyS3(counters)

    def _session_factory() -> _DummySession:
        return _DummySession(dummy_s3)

    monkeypatch.setattr(event_store_module.aioboto3, "Session", _session_factory)

    store = EventStore()

    async def _init_noop() -> None:
        counters["init_lineage_audit"] += 1

    monkeypatch.setattr(store, "_initialize_lineage_and_audit", _init_noop)

    await asyncio.gather(*[store.connect() for _ in range(10)])

    assert counters["head_bucket"] == 1
    assert counters["create_bucket"] == 0
    assert counters["put_bucket_versioning"] == 0
    assert counters["init_lineage_audit"] == 1
    assert getattr(store, "_connected", False) is True

    await store.connect()
    assert counters["head_bucket"] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_event_store_connect_fails_when_lineage_required_and_store_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counters = {"head_bucket": 0}
    dummy_s3 = _DummyS3(counters)

    class _FailingLineageStore:
        async def initialize(self) -> None:
            raise RuntimeError("lineage unavailable")

    monkeypatch.setenv("ENABLE_LINEAGE", "true")
    monkeypatch.setenv("LINEAGE_FAIL_CLOSED", "true")
    monkeypatch.setenv("LINEAGE_FAIL_OPEN_OVERRIDE", "false")
    monkeypatch.setattr(event_store_module.aioboto3, "Session", lambda: _DummySession(dummy_s3))
    monkeypatch.setattr(event_store_module, "LineageStore", _FailingLineageStore)

    store = EventStore()
    with pytest.raises(LineageUnavailableError):
        await store.connect()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_event_store_lineage_record_failure_propagates_when_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingLineageStore:
        async def record_event_envelope(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
            raise RuntimeError("lineage write failed")

        async def enqueue_backfill(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
            return None

    monkeypatch.setenv("ENABLE_LINEAGE", "true")
    monkeypatch.setenv("LINEAGE_FAIL_CLOSED", "true")
    monkeypatch.setenv("LINEAGE_FAIL_OPEN_OVERRIDE", "false")

    store = EventStore()
    store._lineage_store = _FailingLineageStore()

    envelope = EventEnvelope(
        event_id="evt-1",
        event_type="TEST_EVENT",
        aggregate_type="TestAggregate",
        aggregate_id="agg-1",
        data={},
        metadata={},
    )

    with pytest.raises(LineageRecordError):
        await store._record_lineage_and_audit(envelope, s3_key="events/test.json")
