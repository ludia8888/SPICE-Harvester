from __future__ import annotations

import asyncio
from typing import Any

import pytest

from shared.services.kafka.consumer_ops import KafkaConsumerOps
from shared.services.kafka.worker_consumer_runtime import WorkerConsumerRuntime


class _StubMsg:
    def __init__(self, *, topic: str, partition: int, offset: int) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class _StubConsumer:
    def __init__(self) -> None:
        self.commits: list[Any] = []
        self.seeks: list[Any] = []

    def poll(self, timeout: float) -> None:
        raise NotImplementedError

    def commit_sync(self, msg: Any) -> None:  # noqa: ANN401
        self.commits.append(msg)

    def seek(self, tp: Any) -> None:  # noqa: ANN401
        self.seeks.append(tp)

    def pause(self, partitions: list[Any]) -> None:  # noqa: ANN401
        return

    def resume(self, partitions: list[Any]) -> None:  # noqa: ANN401
        return

    def close(self) -> None:
        return


class _DeferredOps(KafkaConsumerOps):
    """
    A KafkaConsumerOps stub that defers `call(...)` execution.

    This lets tests mutate shared state (revoked_partitions) between scheduling a
    commit/seek and actually executing the consumer call.
    """

    def __init__(self, consumer: _StubConsumer) -> None:
        self._consumer = consumer
        self.pending: list[tuple[Any, tuple[Any, ...], dict[str, Any], asyncio.Future]] = []

    async def poll(self, *, timeout: float) -> Any:  # noqa: ANN401
        raise NotImplementedError

    async def commit_sync(self, msg: Any) -> None:  # noqa: ANN401
        self._consumer.commit_sync(msg)

    async def seek(self, tp: Any) -> None:  # noqa: ANN401
        self._consumer.seek(tp)

    async def pause(self, partitions: list[Any]) -> None:  # noqa: ANN401
        self._consumer.pause(partitions)

    async def resume(self, partitions: list[Any]) -> None:  # noqa: ANN401
        self._consumer.resume(partitions)

    async def close(self) -> None:
        self._consumer.close()

    async def call(self, fn, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN001,ANN401
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending.append((fn, args, kwargs, fut))
        return await fut

    def flush_next(self) -> None:
        fn, args, kwargs, fut = self.pending.pop(0)
        fut.set_result(fn(self._consumer, *args, **kwargs))


@pytest.mark.asyncio
async def test_commit_checks_revoked_at_execution_time() -> None:
    consumer = _StubConsumer()
    ops = _DeferredOps(consumer)
    revoked: set[tuple[str, int]] = set()
    commit_state: dict[tuple[str, int], bool] = {}

    runtime = WorkerConsumerRuntime(
        ops=ops,
        loop_label=lambda: "test",
        revoked_partitions=revoked,
        uses_commit_state=lambda: True,
        commit_state_by_partition=commit_state,
    )
    msg = _StubMsg(topic="topic", partition=1, offset=10)

    task = asyncio.create_task(runtime.commit(msg))
    await asyncio.sleep(0)
    assert len(ops.pending) == 1

    revoked.add(("topic", 1))
    ops.flush_next()

    did_commit = await task
    assert did_commit is False
    assert consumer.commits == []
    assert commit_state == {}


@pytest.mark.asyncio
async def test_seek_checks_revoked_at_execution_time() -> None:
    consumer = _StubConsumer()
    ops = _DeferredOps(consumer)
    revoked: set[tuple[str, int]] = set()
    commit_state: dict[tuple[str, int], bool] = {}

    runtime = WorkerConsumerRuntime(
        ops=ops,
        loop_label=lambda: "test",
        revoked_partitions=revoked,
        uses_commit_state=lambda: True,
        commit_state_by_partition=commit_state,
    )

    task = asyncio.create_task(runtime.seek(topic="topic", partition=1, offset=10))
    await asyncio.sleep(0)
    assert len(ops.pending) == 1

    revoked.add(("topic", 1))
    ops.flush_next()

    did_seek = await task
    assert did_seek is False
    assert consumer.seeks == []
    assert commit_state == {}


@pytest.mark.asyncio
async def test_commit_updates_commit_state_after_commit() -> None:
    consumer = _StubConsumer()
    ops = _DeferredOps(consumer)
    revoked: set[tuple[str, int]] = set()
    commit_state: dict[tuple[str, int], bool] = {}

    runtime = WorkerConsumerRuntime(
        ops=ops,
        loop_label=lambda: "test",
        revoked_partitions=revoked,
        uses_commit_state=lambda: True,
        commit_state_by_partition=commit_state,
    )
    msg = _StubMsg(topic="topic", partition=1, offset=10)

    task = asyncio.create_task(runtime.commit(msg))
    await asyncio.sleep(0)
    ops.flush_next()

    did_commit = await task
    assert did_commit is True
    assert consumer.commits == [msg]
    assert commit_state == {("topic", 1): True}


@pytest.mark.asyncio
async def test_seek_updates_commit_state_after_seek() -> None:
    consumer = _StubConsumer()
    ops = _DeferredOps(consumer)
    revoked: set[tuple[str, int]] = set()
    commit_state: dict[tuple[str, int], bool] = {("topic", 1): True}

    runtime = WorkerConsumerRuntime(
        ops=ops,
        loop_label=lambda: "test",
        revoked_partitions=revoked,
        uses_commit_state=lambda: True,
        commit_state_by_partition=commit_state,
    )

    task = asyncio.create_task(runtime.seek(topic="topic", partition=1, offset=10))
    await asyncio.sleep(0)
    ops.flush_next()

    did_seek = await task
    assert did_seek is True
    assert len(consumer.seeks) == 1
    assert commit_state == {("topic", 1): False}

