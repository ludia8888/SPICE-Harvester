from __future__ import annotations

import json

import pytest

from shared.services.storage.event_store import EventStore


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    async def read(self) -> bytes:
        return self._payload


class _Paginator:
    def __init__(self, pages_by_prefix):
        self._pages_by_prefix = pages_by_prefix

    async def paginate(self, *, Bucket: str, Prefix: str):  # noqa: N803
        _ = Bucket
        for page in self._pages_by_prefix.get(Prefix, []):
            yield page


class _S3Client:
    def __init__(self, *, pages_by_prefix, objects_by_key) -> None:
        self._pages_by_prefix = pages_by_prefix
        self._objects_by_key = objects_by_key

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        return _Paginator(self._pages_by_prefix)

    async def get_object(self, *, Bucket: str, Key: str):  # noqa: N803
        _ = Bucket
        return {"Body": _Body(self._objects_by_key[Key])}


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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_events_falls_back_when_aggregate_index_is_partial() -> None:
    event_1_key = "events/2026/01/01/Order/ord-1/evt-1.json"
    event_2_key = "events/2026/01/01/Order/ord-1/evt-2.json"
    client = _S3Client(
        pages_by_prefix={
            "indexes/by-aggregate/Order/ord-1/": [
                {"Contents": [{"Key": "indexes/by-aggregate/Order/ord-1/000001_evt-1.json"}]}
            ],
            "events/": [
                {
                    "Contents": [
                        {"Key": event_1_key},
                        {"Key": event_2_key},
                    ]
                }
            ],
        },
        objects_by_key={
            "indexes/by-aggregate/Order/ord-1/000001_evt-1.json": json.dumps({"s3_key": event_1_key}).encode("utf-8"),
            event_1_key: _event_bytes(event_id="evt-1", sequence=1),
            event_2_key: _event_bytes(event_id="evt-2", sequence=2),
        },
    )
    store = EventStore()
    store.bucket_name = "test-bucket"
    store.session = _Session(client)

    events = await store.get_events("Order", "ord-1")

    assert [event.event_id for event in events] == ["evt-1", "evt-2"]
    assert [event.sequence_number for event in events] == [1, 2]
