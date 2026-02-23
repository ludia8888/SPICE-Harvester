"""Spark on-demand queue/dispatcher for Foundry-style compute routing."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, Optional
from uuid import uuid4

SparkOnDemandRoute = Literal["search_around", "writeback"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SparkOnDemandJobContext:
    job_id: str
    route: SparkOnDemandRoute
    payload: dict[str, Any]


@dataclass(frozen=True)
class SparkOnDemandDispatchResult:
    job_id: str
    route: SparkOnDemandRoute
    queue_wait_ms: int
    execution_ms: int
    result: Any


@dataclass
class _SparkOnDemandQueueItem:
    job_id: str
    route: SparkOnDemandRoute
    payload: dict[str, Any]
    enqueued_at: float
    execute: Callable[[SparkOnDemandJobContext], Awaitable[Any]]
    result_future: asyncio.Future[SparkOnDemandDispatchResult]


class SparkOnDemandDispatchTimeoutError(TimeoutError):
    pass


class SparkOnDemandDispatcher:
    def __init__(self, *, worker_count: int = 1, queue_maxsize: int = 0) -> None:
        self._worker_count = max(1, int(worker_count))
        self._queue_maxsize = max(0, int(queue_maxsize))
        self._queue: asyncio.Queue[_SparkOnDemandQueueItem] = asyncio.Queue(maxsize=self._queue_maxsize)
        self._workers: list[asyncio.Task[None]] = []
        self._start_lock = asyncio.Lock()
        self._started = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._inflight_jobs = 0

    async def dispatch(
        self,
        *,
        route: SparkOnDemandRoute,
        payload: dict[str, Any],
        execute: Callable[[SparkOnDemandJobContext], Awaitable[Any]],
        timeout_seconds: float = 60.0,
    ) -> SparkOnDemandDispatchResult:
        await self._ensure_started()

        loop = asyncio.get_running_loop()
        result_future: asyncio.Future[SparkOnDemandDispatchResult] = loop.create_future()
        job_id = str(uuid4())
        queue_item = _SparkOnDemandQueueItem(
            job_id=job_id,
            route=route,
            payload=dict(payload or {}),
            enqueued_at=time.monotonic(),
            execute=execute,
            result_future=result_future,
        )
        await self._queue.put(queue_item)

        timeout = max(0.1, float(timeout_seconds))
        try:
            return await asyncio.wait_for(result_future, timeout=timeout)
        except asyncio.TimeoutError as exc:
            if not result_future.done():
                result_future.cancel()
            raise SparkOnDemandDispatchTimeoutError(
                f"Spark on-demand dispatch timed out (route={route}, job_id={job_id})"
            ) from exc
        finally:
            await self._shutdown_if_idle()

    async def _ensure_started(self) -> None:
        current_loop = asyncio.get_running_loop()
        async with self._start_lock:
            if self._loop is not current_loop:
                await self._reset_workers_for_loop_change()
                self._loop = current_loop

            if self._started:
                return

            self._queue = asyncio.Queue(maxsize=self._queue_maxsize)
            self._workers = []
            for index in range(self._worker_count):
                task = current_loop.create_task(
                    self._worker_loop(worker_index=index + 1),
                    name=f"spark-on-demand-dispatcher-{index + 1}",
                )
                self._workers.append(task)
            self._started = True

    async def _reset_workers_for_loop_change(self) -> None:
        if not self._workers:
            self._started = False
            return
        workers = list(self._workers)
        self._workers = []
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        self._started = False
        self._inflight_jobs = 0

    async def _shutdown_if_idle(self) -> None:
        async with self._start_lock:
            if not self._started:
                return
            if not self._queue.empty():
                return
            if self._inflight_jobs > 0:
                return
            workers = list(self._workers)
            self._workers = []
            self._started = False
            self._loop = None
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    async def _worker_loop(self, *, worker_index: int) -> None:
        while True:
            try:
                queue_item = await self._queue.get()
            except asyncio.CancelledError:
                raise

            started_at = time.monotonic()
            queue_wait_ms = int((started_at - queue_item.enqueued_at) * 1000)
            job_context = SparkOnDemandJobContext(
                job_id=queue_item.job_id,
                route=queue_item.route,
                payload=dict(queue_item.payload),
            )
            self._inflight_jobs += 1
            try:
                result_value = await queue_item.execute(job_context)
                finished_at = time.monotonic()
                dispatch_result = SparkOnDemandDispatchResult(
                    job_id=queue_item.job_id,
                    route=queue_item.route,
                    queue_wait_ms=max(0, queue_wait_ms),
                    execution_ms=max(0, int((finished_at - started_at) * 1000)),
                    result=result_value,
                )
                if not queue_item.result_future.cancelled():
                    queue_item.result_future.set_result(dispatch_result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Spark on-demand job failed (worker=%s route=%s job_id=%s): %s",
                    worker_index,
                    queue_item.route,
                    queue_item.job_id,
                    exc,
                    exc_info=True,
                )
                if not queue_item.result_future.cancelled():
                    queue_item.result_future.set_exception(exc)
            finally:
                self._inflight_jobs = max(0, self._inflight_jobs - 1)
                self._queue.task_done()


_DISPATCHER: SparkOnDemandDispatcher | None = None


def get_spark_on_demand_dispatcher() -> SparkOnDemandDispatcher:
    global _DISPATCHER
    if _DISPATCHER is None:
        _DISPATCHER = SparkOnDemandDispatcher(worker_count=1, queue_maxsize=0)
    return _DISPATCHER


def reset_spark_on_demand_dispatcher_for_tests() -> None:
    global _DISPATCHER
    _DISPATCHER = None
