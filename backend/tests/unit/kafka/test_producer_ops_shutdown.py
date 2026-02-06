from __future__ import annotations

import logging

import pytest

from shared.services.kafka.producer_ops import close_kafka_producer


class _StubProducerOps:
    def __init__(self) -> None:
        self.close_calls: list[float] = []

    async def close(self, *, timeout_s: float = 5.0) -> None:
        self.close_calls.append(float(timeout_s))


class _StubProducer:
    def __init__(self) -> None:
        self.flush_calls: list[float] = []

    def flush(self, timeout_s: float) -> int:
        self.flush_calls.append(float(timeout_s))
        return 0


class _NoArgFlushProducer:
    def __init__(self) -> None:
        self.flush_calls = 0

    def flush(self) -> int:
        self.flush_calls += 1
        return 0


@pytest.mark.asyncio
async def test_close_kafka_producer_prefers_ops_when_present() -> None:
    ops = _StubProducerOps()
    producer = _StubProducer()

    await close_kafka_producer(
        producer=producer,
        producer_ops=ops,  # type: ignore[arg-type]
        timeout_s=1.25,
    )

    assert ops.close_calls == [1.25]
    assert producer.flush_calls == []


@pytest.mark.asyncio
async def test_close_kafka_producer_flushes_raw_producer() -> None:
    producer = _StubProducer()

    await close_kafka_producer(
        producer=producer,
        timeout_s=2.5,
    )

    assert producer.flush_calls == [2.5]


@pytest.mark.asyncio
async def test_close_kafka_producer_handles_noarg_flush_signature() -> None:
    producer = _NoArgFlushProducer()

    await close_kafka_producer(
        producer=producer,
        timeout_s=3.0,
    )

    assert producer.flush_calls == 1


@pytest.mark.asyncio
async def test_close_kafka_producer_swallows_close_errors(caplog: pytest.LogCaptureFixture) -> None:
    class _RaisingOps:
        async def close(self, *, timeout_s: float = 5.0) -> None:
            raise RuntimeError("close failed")

    caplog.set_level(logging.WARNING)
    await close_kafka_producer(
        producer_ops=_RaisingOps(),  # type: ignore[arg-type]
        warning_logger=logging.getLogger("test.producer.shutdown"),
    )
    assert "close failed" in caplog.text
