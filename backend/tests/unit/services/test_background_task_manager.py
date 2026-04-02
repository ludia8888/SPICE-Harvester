from __future__ import annotations

import asyncio
import json

import pytest

from shared.models.background_task import TaskStatus
from shared.services.core.background_task_manager import BackgroundTaskManager


class _RedisStub:
    def __init__(self) -> None:
        self._data: dict[str, dict] = {}

    async def set_json(self, key: str, value, ttl: int | None = None) -> None:  # noqa: ANN001
        del ttl
        # Keep behavior close to Redis serialization boundaries.
        self._data[key] = json.loads(json.dumps(value, default=str))

    async def get_json(self, key: str):
        value = self._data.get(key)
        if value is None:
            return None
        return json.loads(json.dumps(value))

    async def scan_keys(self, pattern: str) -> list[str]:
        if pattern == "background_task:*":
            return sorted([key for key in self._data.keys() if key.startswith("background_task:")])
        return sorted(self._data.keys())


async def _wait_until_complete(
    manager: BackgroundTaskManager,
    task_id: str,
    *,
    timeout_seconds: float = 2.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while loop.time() < deadline:
        task = await manager.get_task_status(task_id)
        if task and task.is_complete:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"Task {task_id} did not complete within timeout")


@pytest.mark.asyncio
async def test_retry_attempt_clears_terminal_fields_before_reexecution() -> None:
    manager = BackgroundTaskManager(redis_service=_RedisStub(), websocket_service=None)  # type: ignore[arg-type]
    manager.max_retries = 2
    manager.retry_delay = 0

    attempts = 0
    observed_second_attempt_state: dict[str, object] = {}

    async def _flaky_task(task_id: str) -> dict[str, bool]:
        nonlocal attempts
        attempts += 1
        current = await manager.get_task_status(task_id)
        if attempts == 2:
            assert current is not None
            observed_second_attempt_state["status"] = current.status
            observed_second_attempt_state["completed_at"] = current.completed_at
            observed_second_attempt_state["result"] = current.result
            return {"ok": True}
        raise RuntimeError("first attempt failure")

    task_id = await manager.create_task(_flaky_task, task_name="retry-clear-check")
    await _wait_until_complete(manager, task_id)
    final_task = await manager.get_task_status(task_id)

    assert final_task is not None
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.result is not None and final_task.result.success is True
    assert observed_second_attempt_state["status"] == TaskStatus.PROCESSING
    assert observed_second_attempt_state["completed_at"] is None
    assert observed_second_attempt_state["result"] is None


@pytest.mark.asyncio
async def test_task_stays_terminal_failed_after_retry_exhaustion() -> None:
    manager = BackgroundTaskManager(redis_service=_RedisStub(), websocket_service=None)  # type: ignore[arg-type]
    manager.max_retries = 2
    manager.retry_delay = 0

    async def _always_fail(task_id: str) -> None:  # noqa: ARG001
        raise RuntimeError("permanent failure")

    task_id = await manager.create_task(_always_fail, task_name="retry-exhaustion-check")
    await _wait_until_complete(manager, task_id)
    final_task = await manager.get_task_status(task_id)

    assert final_task is not None
    assert final_task.status == TaskStatus.FAILED
    assert final_task.retry_count == 2
    assert final_task.completed_at is not None
    assert final_task.next_retry_at is None
    assert final_task.result is not None
    assert final_task.result.success is False
    assert "permanent failure" in str(final_task.result.error or "")


@pytest.mark.asyncio
async def test_run_with_tracking_registers_current_task_for_dead_task_cleanup() -> None:
    manager = BackgroundTaskManager(redis_service=_RedisStub(), websocket_service=None)  # type: ignore[arg-type]
    manager.dead_task_threshold = 0

    release = asyncio.Event()
    registered: dict[str, bool] = {}

    async def _task(task_id: str) -> None:
        registered["present"] = task_id in manager._running_tasks
        await release.wait()

    task_id = "tracked-task"
    runner = asyncio.create_task(manager.run_with_tracking(task_id=task_id, func=_task, kwargs={"task_id": task_id}))
    try:
        for _ in range(50):
            if registered:
                break
            await asyncio.sleep(0.01)
        assert registered == {"present": True}
        task = await manager.get_task_status(task_id)
        assert task is not None
        assert task.status == TaskStatus.PROCESSING
    finally:
        release.set()
        await runner
