from __future__ import annotations

from contextlib import nullcontext

import pytest
from botocore.exceptions import ClientError

from message_relay.main import EventPublisher, _RecentPublishedEventIds


class _AsyncBody:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.closed = False

    async def read(self) -> bytes:
        return self.payload

    def close(self) -> None:
        self.closed = True


class _ClientContext:
    def __init__(self, client) -> None:  # noqa: ANN001
        self._client = client

    async def __aenter__(self):  # noqa: ANN201
        return self._client

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        _ = exc_type, exc, tb
        return False


class _Session:
    def __init__(self, client) -> None:  # noqa: ANN001
        self._client = client

    def client(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        _ = args, kwargs
        return _ClientContext(self._client)


class _Paginator:
    def __init__(self, contents_by_prefix):  # noqa: ANN001
        self._contents_by_prefix = contents_by_prefix

    async def paginate(self, **kwargs):  # noqa: ANN003, ANN201
        prefix = kwargs.get("Prefix")
        yield {"Contents": self._contents_by_prefix.get(prefix, [])}


class _ListClient:
    def __init__(self, contents_by_prefix):  # noqa: ANN001
        self._contents_by_prefix = contents_by_prefix

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        return _Paginator(self._contents_by_prefix)


class _Producer:
    def __init__(self) -> None:
        self.published_keys: list[bytes] = []
        self._callbacks = []

    def produce(self, *, topic, value, key, headers=None, callback=None):  # noqa: ANN001, ANN003
        _ = topic, value, headers
        self.published_keys.append(key)
        if callback is not None:
            self._callbacks.append(callback)

    def poll(self, timeout):  # noqa: ANN001, ANN201
        _ = timeout
        return None

    def flush(self, timeout):  # noqa: ANN001, ANN201
        _ = timeout
        callbacks = list(self._callbacks)
        self._callbacks.clear()
        for callback in callbacks:
            callback(None, None)
        return 0


class _Tracing:
    def span(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        _ = args, kwargs
        return nullcontext()


class _Metrics:
    def record_event(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        _ = args, kwargs


@pytest.mark.asyncio
async def test_load_checkpoint_returns_empty_for_missing_object() -> None:
    body = _AsyncBody(b"{}")

    class _Client:
        async def get_object(self, **kwargs):  # noqa: ANN003, ANN201
            _ = kwargs
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(_Client())
    publisher.bucket_name = "bucket"
    publisher.checkpoint_key = "checkpoint.json"
    publisher._s3_client_kwargs = lambda: {}

    checkpoint = await EventPublisher._load_checkpoint(publisher)

    assert checkpoint == {}
    assert body.closed is False


@pytest.mark.asyncio
async def test_load_checkpoint_raises_for_non_missing_client_error() -> None:
    class _Client:
        async def get_object(self, **kwargs):  # noqa: ANN003, ANN201
            _ = kwargs
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")

    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(_Client())
    publisher.bucket_name = "bucket"
    publisher.checkpoint_key = "checkpoint.json"
    publisher._s3_client_kwargs = lambda: {}

    with pytest.raises(ClientError):
        await EventPublisher._load_checkpoint(publisher)


@pytest.mark.asyncio
async def test_process_events_propagates_checkpoint_load_failures() -> None:
    publisher = EventPublisher.__new__(EventPublisher)

    async def _raise_checkpoint():
        raise RuntimeError("checkpoint unavailable")

    publisher._load_checkpoint = _raise_checkpoint

    with pytest.raises(RuntimeError, match="checkpoint unavailable"):
        await EventPublisher.process_events(publisher)


def test_s3_client_kwargs_derive_tls_from_endpoint_scheme() -> None:
    publisher = EventPublisher.__new__(EventPublisher)
    publisher.endpoint_url = "https://minio.example.com"
    publisher.access_key = "access"
    publisher.secret_key = "secret"

    kwargs = EventPublisher._s3_client_kwargs(publisher)

    assert kwargs["use_ssl"] is True


@pytest.mark.asyncio
async def test_list_next_index_keys_orders_lookback_before_forward() -> None:
    now_ms = 1_735_689_600_000
    prefix = "indexes/by-date/2025/01/01/"
    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(
        _ListClient(
            {
                prefix: [
                    {"Key": f"{prefix}{now_ms - 1_000}_evt-old.json"},
                    {"Key": f"{prefix}{now_ms + 1_000}_evt-new.json"},
                ]
            }
        )
    )
    publisher.bucket_name = "bucket"
    publisher.lookback_seconds = 10
    publisher.lookback_max_keys = 5
    publisher.batch_size = 5
    publisher._s3_client_kwargs = lambda: {}

    checkpoint = {
        "last_timestamp_ms": now_ms,
        "last_index_key": f"{prefix}{now_ms}_evt-mid.json",
    }

    keys = await EventPublisher._list_next_index_keys(publisher, checkpoint)

    assert keys == [
        f"{prefix}{now_ms - 1_000}_evt-old.json",
        f"{prefix}{now_ms + 1_000}_evt-new.json",
    ]


@pytest.mark.asyncio
async def test_process_events_does_not_advance_checkpoint_on_missing_index_entry() -> None:
    missing_key = "indexes/by-date/2025/01/01/100_evt.json"

    class _Client:
        async def get_object(self, **kwargs):  # noqa: ANN003, ANN201
            key = kwargs["Key"]
            if key == missing_key:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            raise AssertionError(f"Unexpected key: {key}")

    saved: list[dict[str, object]] = []
    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(_Client())
    publisher.bucket_name = "bucket"
    publisher._s3_client_kwargs = lambda: {}
    publisher._load_checkpoint = lambda: None  # type: ignore[assignment]
    publisher._log_metrics_if_due = lambda: None  # type: ignore[assignment]
    publisher._recent_loaded_from_checkpoint = True
    publisher._recent_published_event_ids = _RecentPublishedEventIds(10)
    publisher._metrics_total = {
        "index_keys_considered": 0,
        "events_published": 0,
        "events_skipped_dedup": 0,
        "checkpoint_saves": 0,
        "kafka_flush_count": 0,
        "kafka_flush_ms_total": 0,
        "kafka_delivery_failures": 0,
        "kafka_produce_buffer_full": 0,
    }
    publisher.kafka_flush_batch_size = 1
    publisher.batch_size = 1
    publisher.kafka_flush_timeout_s = 1.0
    publisher.producer = object()
    publisher._flush_producer = lambda timeout_s: 0  # type: ignore[assignment]
    publisher._save_checkpoint = lambda checkpoint: saved.append(dict(checkpoint))  # type: ignore[assignment]

    async def _load_checkpoint():  # noqa: ANN202
        return {"last_timestamp_ms": 100, "last_index_key": None}

    async def _save_checkpoint(checkpoint):  # noqa: ANN001, ANN202
        saved.append(dict(checkpoint))

    async def _list_next_index_keys(checkpoint):  # noqa: ANN001, ANN202
        return [missing_key]

    publisher._load_checkpoint = _load_checkpoint  # type: ignore[assignment]
    publisher._save_checkpoint = _save_checkpoint  # type: ignore[assignment]
    publisher._list_next_index_keys = _list_next_index_keys  # type: ignore[assignment]

    published = await EventPublisher.process_events(publisher)

    assert published == 0
    assert saved == []


@pytest.mark.asyncio
async def test_process_events_does_not_advance_checkpoint_on_missing_event_payload() -> None:
    index_key = "indexes/by-date/2025/01/01/100_evt.json"
    payload_key = "events/evt.json"

    class _Client:
        async def get_object(self, **kwargs):  # noqa: ANN003, ANN201
            key = kwargs["Key"]
            if key == index_key:
                return {
                    "Body": _AsyncBody(
                        (
                            "{"
                            f"\"s3_key\":\"{payload_key}\","
                            "\"event_id\":\"evt-1\","
                            "\"aggregate_id\":\"agg-1\","
                            "\"kafka_topic\":\"instance-events\""
                            "}"
                        ).encode("utf-8")
                    )
                }
            if key == payload_key:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            raise AssertionError(f"Unexpected key: {key}")

    saved: list[dict[str, object]] = []
    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(_Client())
    publisher.bucket_name = "bucket"
    publisher._s3_client_kwargs = lambda: {}
    publisher._log_metrics_if_due = lambda: None  # type: ignore[assignment]
    publisher._recent_loaded_from_checkpoint = True
    publisher._recent_published_event_ids = _RecentPublishedEventIds(10)
    publisher._metrics_total = {
        "index_keys_considered": 0,
        "events_published": 0,
        "events_skipped_dedup": 0,
        "checkpoint_saves": 0,
        "kafka_flush_count": 0,
        "kafka_flush_ms_total": 0,
        "kafka_delivery_failures": 0,
        "kafka_produce_buffer_full": 0,
    }
    publisher.kafka_flush_batch_size = 1
    publisher.batch_size = 1
    publisher.kafka_flush_timeout_s = 1.0
    publisher.producer = _Producer()
    publisher.tracing = _Tracing()
    publisher.metrics = _Metrics()
    publisher._flush_producer = lambda timeout_s: publisher.producer.flush(timeout_s)  # type: ignore[assignment]

    async def _load_checkpoint():  # noqa: ANN202
        return {"last_timestamp_ms": 100, "last_index_key": None}

    async def _save_checkpoint(checkpoint):  # noqa: ANN001, ANN202
        saved.append(dict(checkpoint))

    async def _list_next_index_keys(checkpoint):  # noqa: ANN001, ANN202
        return [index_key]

    publisher._load_checkpoint = _load_checkpoint  # type: ignore[assignment]
    publisher._save_checkpoint = _save_checkpoint  # type: ignore[assignment]
    publisher._list_next_index_keys = _list_next_index_keys  # type: ignore[assignment]

    published = await EventPublisher.process_events(publisher)

    assert published == 0
    assert saved == []


@pytest.mark.asyncio
async def test_process_events_advances_checkpoint_past_malformed_index_entry() -> None:
    bad_key = "indexes/by-date/2025/01/01/100_evt.json"

    class _Client:
        async def get_object(self, **kwargs):  # noqa: ANN003, ANN201
            key = kwargs["Key"]
            if key == bad_key:
                return {"Body": _AsyncBody(b"{}")}
            raise AssertionError(f"Unexpected key: {key}")

    saved: list[dict[str, object]] = []
    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(_Client())
    publisher.bucket_name = "bucket"
    publisher._s3_client_kwargs = lambda: {}
    publisher._log_metrics_if_due = lambda: None  # type: ignore[assignment]
    publisher._recent_loaded_from_checkpoint = True
    publisher._recent_published_event_ids = _RecentPublishedEventIds(10)
    publisher._metrics_total = {
        "index_keys_considered": 0,
        "events_published": 0,
        "events_skipped_dedup": 0,
        "checkpoint_saves": 0,
        "kafka_flush_count": 0,
        "kafka_flush_ms_total": 0,
        "kafka_delivery_failures": 0,
        "kafka_produce_buffer_full": 0,
    }
    publisher.kafka_flush_batch_size = 1
    publisher.batch_size = 1
    publisher.kafka_flush_timeout_s = 1.0
    publisher.producer = object()
    publisher._flush_producer = lambda timeout_s: 0  # type: ignore[assignment]

    async def _load_checkpoint():  # noqa: ANN202
        return {"last_timestamp_ms": 50, "last_index_key": None}

    async def _save_checkpoint(checkpoint):  # noqa: ANN001, ANN202
        saved.append(dict(checkpoint))

    async def _list_next_index_keys(checkpoint):  # noqa: ANN001, ANN202
        return [bad_key]

    publisher._load_checkpoint = _load_checkpoint  # type: ignore[assignment]
    publisher._save_checkpoint = _save_checkpoint  # type: ignore[assignment]
    publisher._list_next_index_keys = _list_next_index_keys  # type: ignore[assignment]

    published = await EventPublisher.process_events(publisher)

    assert published == 0
    assert saved
    assert saved[-1]["last_index_key"] == bad_key


@pytest.mark.asyncio
async def test_process_events_advances_checkpoint_past_invalid_json_index_entry() -> None:
    bad_key = "indexes/by-date/2025/01/01/100_evt.json"

    class _Client:
        async def get_object(self, **kwargs):  # noqa: ANN003, ANN201
            key = kwargs["Key"]
            if key == bad_key:
                return {"Body": _AsyncBody(b"{not-json")}
            raise AssertionError(f"Unexpected key: {key}")

    saved: list[dict[str, object]] = []
    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(_Client())
    publisher.bucket_name = "bucket"
    publisher._s3_client_kwargs = lambda: {}
    publisher._log_metrics_if_due = lambda: None  # type: ignore[assignment]
    publisher._recent_loaded_from_checkpoint = True
    publisher._recent_published_event_ids = _RecentPublishedEventIds(10)
    publisher._metrics_total = {
        "index_keys_considered": 0,
        "events_published": 0,
        "events_skipped_dedup": 0,
        "checkpoint_saves": 0,
        "kafka_flush_count": 0,
        "kafka_flush_ms_total": 0,
        "kafka_delivery_failures": 0,
        "kafka_produce_buffer_full": 0,
    }
    publisher.kafka_flush_batch_size = 1
    publisher.batch_size = 1
    publisher.kafka_flush_timeout_s = 1.0
    publisher.producer = object()
    publisher._flush_producer = lambda timeout_s: 0  # type: ignore[assignment]

    async def _load_checkpoint():  # noqa: ANN202
        return {"last_timestamp_ms": 50, "last_index_key": None}

    async def _save_checkpoint(checkpoint):  # noqa: ANN001, ANN202
        saved.append(dict(checkpoint))

    async def _list_next_index_keys(checkpoint):  # noqa: ANN001, ANN202
        return [bad_key]

    publisher._load_checkpoint = _load_checkpoint  # type: ignore[assignment]
    publisher._save_checkpoint = _save_checkpoint  # type: ignore[assignment]
    publisher._list_next_index_keys = _list_next_index_keys  # type: ignore[assignment]

    published = await EventPublisher.process_events(publisher)

    assert published == 0
    assert saved
    assert saved[-1]["last_index_key"] == bad_key


@pytest.mark.asyncio
async def test_process_events_flushes_pending_batch_before_skipping_malformed_index_entry() -> None:
    good_key = "indexes/by-date/2025/01/01/100_evt-good.json"
    bad_key = "indexes/by-date/2025/01/01/101_evt-bad.json"
    payload_key = "events/evt-good.json"

    class _Client:
        async def get_object(self, **kwargs):  # noqa: ANN003, ANN201
            key = kwargs["Key"]
            if key == good_key:
                return {
                    "Body": _AsyncBody(
                        (
                            "{"
                            f"\"s3_key\":\"{payload_key}\","
                            "\"event_id\":\"evt-good\","
                            "\"aggregate_id\":\"agg-1\","
                            "\"ordering_key\":\"agg-1\","
                            "\"kafka_topic\":\"instance-events\""
                            "}"
                        ).encode("utf-8")
                    )
                }
            if key == payload_key:
                return {"Body": _AsyncBody(b"{\"event_id\":\"evt-good\"}")}
            if key == bad_key:
                return {"Body": _AsyncBody(b"{}")}
            raise AssertionError(f"Unexpected key: {key}")

    saved: list[dict[str, object]] = []
    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(_Client())
    publisher.bucket_name = "bucket"
    publisher._s3_client_kwargs = lambda: {}
    publisher._log_metrics_if_due = lambda: None  # type: ignore[assignment]
    publisher._recent_loaded_from_checkpoint = True
    publisher._recent_published_event_ids = _RecentPublishedEventIds(10)
    publisher._metrics_total = {
        "index_keys_considered": 0,
        "events_published": 0,
        "events_skipped_dedup": 0,
        "checkpoint_saves": 0,
        "kafka_flush_count": 0,
        "kafka_flush_ms_total": 0,
        "kafka_delivery_failures": 0,
        "kafka_produce_buffer_full": 0,
    }
    publisher.kafka_flush_batch_size = 10
    publisher.batch_size = 10
    publisher.kafka_flush_timeout_s = 1.0
    publisher.producer = _Producer()
    publisher.tracing = _Tracing()
    publisher.metrics = _Metrics()
    publisher._flush_producer = lambda timeout_s: publisher.producer.flush(timeout_s)  # type: ignore[assignment]

    async def _load_checkpoint():  # noqa: ANN202
        return {"last_timestamp_ms": 50, "last_index_key": None}

    async def _save_checkpoint(checkpoint):  # noqa: ANN001, ANN202
        saved.append(dict(checkpoint))

    async def _list_next_index_keys(checkpoint):  # noqa: ANN001, ANN202
        return [good_key, bad_key]

    publisher._load_checkpoint = _load_checkpoint  # type: ignore[assignment]
    publisher._save_checkpoint = _save_checkpoint  # type: ignore[assignment]
    publisher._list_next_index_keys = _list_next_index_keys  # type: ignore[assignment]

    published = await EventPublisher.process_events(publisher)

    assert published == 1
    assert publisher._metrics_total["events_published"] == 1
    assert saved
    assert saved[-1]["last_index_key"] == bad_key
