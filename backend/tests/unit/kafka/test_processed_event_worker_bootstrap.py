from __future__ import annotations

from typing import Any

from shared.services.kafka.processed_event_worker import ProcessedEventKafkaWorker, RegistryKey


class _StubWorker(ProcessedEventKafkaWorker[dict[str, Any], None]):
    def __init__(self) -> None:
        self.consumer = None
        self.consumer_ops = None
        self.processed = None
        self.handler = "stub_handler"
        self.max_retries = 3
        self.backoff_base = 1
        self.backoff_max = 10

    def _parse_payload(self, payload: Any) -> dict[str, Any]:  # type: ignore[override]
        return dict(payload or {})

    def _registry_key(self, payload: dict[str, Any]) -> RegistryKey:  # type: ignore[override]
        return RegistryKey(event_id=str(payload.get("event_id") or "evt"))

    async def _process_payload(self, payload: dict[str, Any]) -> None:  # type: ignore[override]
        return None

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: dict[str, Any],
        raw_payload: str | None,
        error: str,
        attempt_count: int,
    ) -> None:
        return None


class _FakeConsumerOps:
    def __init__(self, consumer: Any, *, thread_name_prefix: str) -> None:
        self.consumer = consumer
        self.thread_name_prefix = thread_name_prefix


def test_initialize_safe_consumer_runtime_sets_consumer_ops(monkeypatch) -> None:
    created: dict[str, Any] = {}

    def _fake_create_safe_consumer(**kwargs: Any):
        created.update(kwargs)
        return object()

    monkeypatch.setattr(
        "shared.services.kafka.processed_event_worker.create_safe_consumer",
        _fake_create_safe_consumer,
    )
    monkeypatch.setattr(
        "shared.services.kafka.processed_event_worker.ExecutorKafkaConsumerOps",
        _FakeConsumerOps,
    )

    worker = _StubWorker()
    worker._initialize_safe_consumer_runtime(
        group_id="g1",
        topics=["t1"],
        service_name="svc",
        thread_name_prefix="svc-kafka",
        max_poll_interval_ms=321000,
        reset_partition_state=True,
    )

    assert created["group_id"] == "g1"
    assert created["topics"] == ["t1"]
    assert created["service_name"] == "svc"
    assert int(created["max_poll_interval_ms"]) == 321000
    assert callable(created["on_revoke"])
    assert callable(created["on_assign"])

    assert worker.consumer is not None
    assert isinstance(worker.consumer_ops, _FakeConsumerOps)
    assert worker.consumer_ops.thread_name_prefix == "svc-kafka"
    assert worker._rebalance_in_progress is False
    assert hasattr(worker, "_revoked_partitions")
    assert hasattr(worker, "_pending_by_partition")


def test_initialize_safe_consumer_runtime_without_partition_reset(monkeypatch) -> None:
    def _fake_create_safe_consumer(**kwargs: Any):
        return object()

    monkeypatch.setattr(
        "shared.services.kafka.processed_event_worker.create_safe_consumer",
        _fake_create_safe_consumer,
    )
    monkeypatch.setattr(
        "shared.services.kafka.processed_event_worker.ExecutorKafkaConsumerOps",
        _FakeConsumerOps,
    )

    worker = _StubWorker()
    worker._initialize_safe_consumer_runtime(
        group_id="g2",
        topics=["t2"],
        service_name="svc2",
        thread_name_prefix="svc2-kafka",
        reset_partition_state=False,
    )

    assert worker.consumer is not None
    assert isinstance(worker.consumer_ops, _FakeConsumerOps)
    assert not hasattr(worker, "_revoked_partitions")
