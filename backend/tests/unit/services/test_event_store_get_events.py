from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from shared.services.storage import event_store as event_store_module
from shared.services.storage.event_store import EventStore


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    async def read(self) -> bytes:
        return self._payload


class _Paginator:
    def __init__(self, pages_by_prefix, *, raise_on_prefixes=None):
        self._pages_by_prefix = pages_by_prefix
        self._raise_on_prefixes = set(raise_on_prefixes or [])

    async def paginate(self, *, Bucket: str, Prefix: str):  # noqa: N803
        _ = Bucket
        if Prefix in self._raise_on_prefixes:
            raise AssertionError(f"unexpected paginate call for prefix={Prefix}")
        for page in self._pages_by_prefix.get(Prefix, []):
            yield page


class _S3Client:
    def __init__(self, *, pages_by_prefix, objects_by_key, raise_on_prefixes=None) -> None:
        self._pages_by_prefix = pages_by_prefix
        self._objects_by_key = objects_by_key
        self._raise_on_prefixes = set(raise_on_prefixes or [])
        self.put_calls: list[dict[str, object]] = []

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        return _Paginator(self._pages_by_prefix, raise_on_prefixes=self._raise_on_prefixes)

    async def get_object(self, *, Bucket: str, Key: str):  # noqa: N803
        _ = Bucket
        return {"Body": _Body(self._objects_by_key[Key])}

    async def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str):  # noqa: N803
        _ = Bucket, ContentType
        self.put_calls.append({"Key": Key, "Body": Body})
        self._objects_by_key[Key] = Body


class _ClientCtx:
    def __init__(self, client: _S3Client) -> None:
        self._client = client

    async def __aenter__(self) -> _S3Client:
        return self._client

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _Session:
    def __init__(self, client: _S3Client) -> None:
        self._client = client

    def client(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return _ClientCtx(self._client)


def _event_bytes(*, event_id: str, sequence: int) -> bytes:
    return json.dumps(
        {
            "event_id": event_id,
            "event_type": "OrderUpdated",
            "aggregate_type": "Order",
            "aggregate_id": "ord-1",
            "occurred_at": "2026-01-01T00:00:00Z",
            "actor": "tester",
            "data": {"order_id": "ord-1"},
            "metadata": {},
            "schema_version": "1",
            "sequence_number": sequence,
        }
    ).encode("utf-8")


def _event_bytes_at(*, event_id: str, sequence: int, occurred_at: str) -> bytes:
    return json.dumps(
        {
            "event_id": event_id,
            "event_type": "OrderUpdated",
            "aggregate_type": "Order",
            "aggregate_id": "ord-1",
            "occurred_at": occurred_at,
            "actor": "tester",
            "data": {"order_id": "ord-1"},
            "metadata": {},
            "schema_version": "1",
            "sequence_number": sequence,
        }
    ).encode("utf-8")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_events_avoids_full_bucket_scan_when_aggregate_index_is_complete() -> None:
    event_1_key = "events/2026/01/01/Order/ord-1/evt-1.json"
    event_2_key = "events/2026/01/01/Order/ord-1/evt-2.json"
    client = _S3Client(
        pages_by_prefix={
            "indexes/by-aggregate/Order/ord-1/": [
                {
                    "Contents": [
                        {"Key": "indexes/by-aggregate/Order/ord-1/000001_evt-1.json"},
                        {"Key": "indexes/by-aggregate/Order/ord-1/000002_evt-2.json"},
                    ]
                }
            ],
        },
        objects_by_key={
            "indexes/by-aggregate/Order/ord-1/000001_evt-1.json": json.dumps({"s3_key": event_1_key}).encode("utf-8"),
            "indexes/by-aggregate/Order/ord-1/000002_evt-2.json": json.dumps({"s3_key": event_2_key}).encode("utf-8"),
            event_1_key: _event_bytes(event_id="evt-1", sequence=1),
            event_2_key: _event_bytes(event_id="evt-2", sequence=2),
        },
        raise_on_prefixes={"events/"},
    )
    store = EventStore()
    store.bucket_name = "test-bucket"
    store.session = _Session(client)

    events = await store.get_events("Order", "ord-1")

    assert [event.event_id for event in events] == ["evt-1", "evt-2"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_events_falls_back_when_aggregate_index_has_sequence_gap() -> None:
    event_1_key = "events/2026/01/01/Order/ord-1/evt-1.json"
    event_2_key = "events/2026/01/01/Order/ord-1/evt-2.json"
    event_3_key = "events/2026/01/01/Order/ord-1/evt-3.json"
    client = _S3Client(
        pages_by_prefix={
            "indexes/by-aggregate/Order/ord-1/": [
                {
                    "Contents": [
                        {"Key": "indexes/by-aggregate/Order/ord-1/000001_evt-1.json"},
                        {"Key": "indexes/by-aggregate/Order/ord-1/000003_evt-3.json"},
                    ]
                }
            ],
            "events/": [
                {
                    "Contents": [
                        {"Key": event_1_key},
                        {"Key": event_2_key},
                        {"Key": event_3_key},
                    ]
                }
            ],
        },
        objects_by_key={
            "indexes/by-aggregate/Order/ord-1/000001_evt-1.json": json.dumps({"s3_key": event_1_key}).encode("utf-8"),
            "indexes/by-aggregate/Order/ord-1/000003_evt-3.json": json.dumps({"s3_key": event_3_key}).encode("utf-8"),
            event_1_key: _event_bytes(event_id="evt-1", sequence=1),
            event_2_key: _event_bytes(event_id="evt-2", sequence=2),
            event_3_key: _event_bytes(event_id="evt-3", sequence=3),
        },
    )
    store = EventStore()
    store.bucket_name = "test-bucket"
    store.session = _Session(client)

    events = await store.get_events("Order", "ord-1")

    assert [event.event_id for event in events] == ["evt-1", "evt-2", "evt-3"]
    assert [event.sequence_number for event in events] == [1, 2, 3]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_replay_events_fallback_scans_each_year_in_requested_range() -> None:
    event_2025_key = "events/2025/12/31/Order/ord-1/evt-1.json"
    event_2026_key = "events/2026/01/01/Order/ord-1/evt-2.json"
    client = _S3Client(
        pages_by_prefix={
            "events/2025/": [{"Contents": [{"Key": event_2025_key}]}],
            "events/2026/": [{"Contents": [{"Key": event_2026_key}]}],
        },
        objects_by_key={
            event_2025_key: _event_bytes_at(
                event_id="evt-1",
                sequence=1,
                occurred_at="2025-12-31T23:59:59+00:00",
            ),
            event_2026_key: _event_bytes_at(
                event_id="evt-2",
                sequence=2,
                occurred_at="2026-01-01T00:00:01+00:00",
            ),
        },
    )
    store = EventStore()
    store.bucket_name = "test-bucket"
    store.session = _Session(client)

    events = [
        event
        async for event in store.replay_events(
            from_timestamp=datetime(2025, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
            to_timestamp=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
        )
    ]

    assert [event.event_id for event in events] == ["evt-1", "evt-2"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_replay_events_falls_back_when_any_day_in_range_lacks_index() -> None:
    indexed_event_key = "events/2026/01/01/Order/ord-1/evt-1.json"
    fallback_event_key = "events/2026/01/02/Order/ord-1/evt-2.json"
    client = _S3Client(
        pages_by_prefix={
            "indexes/by-date/2026/01/01/": [{"Contents": [{"Key": "indexes/by-date/2026/01/01/1735689600000_evt-1.json"}]}],
            "indexes/by-date/2026/01/02/": [{"Contents": []}],
            "events/2026/": [{"Contents": [{"Key": indexed_event_key}, {"Key": fallback_event_key}]}],
        },
        objects_by_key={
            "indexes/by-date/2026/01/01/1735689600000_evt-1.json": json.dumps({"s3_key": indexed_event_key}).encode("utf-8"),
            indexed_event_key: _event_bytes_at(
                event_id="evt-1",
                sequence=1,
                occurred_at="2026-01-01T00:00:00+00:00",
            ),
            fallback_event_key: _event_bytes_at(
                event_id="evt-2",
                sequence=2,
                occurred_at="2026-01-02T00:00:00+00:00",
            ),
        },
    )
    store = EventStore()
    store.bucket_name = "test-bucket"
    store.session = _Session(client)

    events = [
        event
        async for event in store.replay_events(
            from_timestamp=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_timestamp=datetime(2026, 1, 2, 0, 0, 1, tzinfo=timezone.utc),
        )
    ]

    assert [event.event_id for event in events] == ["evt-1", "evt-2"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_snapshot_operations_initialize_session_lazily(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot_key = "snapshots/Order/ord-1/vlatest.json"
    client = _S3Client(
        pages_by_prefix={},
        objects_by_key={
            snapshot_key: json.dumps({"state": {"status": "OPEN"}}).encode("utf-8"),
        },
    )
    monkeypatch.setattr(event_store_module.aioboto3, "Session", lambda: _Session(client))

    store = EventStore()
    store.bucket_name = "test-bucket"

    snapshot = await store.get_snapshot("Order", "ord-1")
    await store.save_snapshot("Order", "ord-1", 3, {"status": "OPEN"})

    assert snapshot == {"state": {"status": "OPEN"}}
    assert store.session is not None
    assert [call["Key"] for call in client.put_calls] == [
        "snapshots/Order/ord-1/v3.json",
        "snapshots/Order/ord-1/vlatest.json",
    ]
