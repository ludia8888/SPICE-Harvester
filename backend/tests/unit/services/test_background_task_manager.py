from __future__ import annotations

import asyncio
import json

import pytest

from shared.dependencies.providers import get_background_task_manager
from shared.models.background_task import TaskStatus
from shared.services.core.background_task_manager import BackgroundTaskManager
from shared.services.storage.redis_service import RedisService


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


@pytest.mark.asyncio
async def test_create_task_does_not_inject_task_id_when_callable_does_not_accept_it() -> None:
    manager = BackgroundTaskManager(redis_service=_RedisStub(), websocket_service=None)  # type: ignore[arg-type]

    observed: dict[str, str] = {}

    async def _listener(command_id: str) -> str:
        observed["command_id"] = command_id
        return "ok"

    task_id = await manager.create_task(
        _listener,
        "cmd-1",
        task_name="listener-without-task-id",
    )
    await _wait_until_complete(manager, task_id)

    task = await manager.get_task_status(task_id)
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert observed == {"command_id": "cmd-1"}


class _ContainerStub:
    def __init__(self, redis_service: object, existing_manager: object | None = None) -> None:
        self._instances = {RedisService: redis_service}
        if existing_manager is not None:
            self._instances[BackgroundTaskManager] = existing_manager

    def has(self, service_type: object) -> bool:
        return service_type in self._instances

    def is_created(self, service_type: object) -> bool:
        return service_type in self._instances

    def ensure_singleton(self, service_type: object, factory: object) -> bool:  # noqa: ARG002
        return service_type not in self._instances

    async def get(self, service_type: object) -> object:
        return self._instances[service_type]

    def ensure_instance(self, service_type: object, instance: object) -> bool:
        created = service_type not in self._instances
        self._instances[service_type] = instance
        return created


@pytest.mark.asyncio
async def test_provider_starts_new_background_task_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    redis_service = _RedisStub()
    container = _ContainerStub(redis_service)
    observed: dict[str, int] = {"starts": 0}

    class _Manager:
        async def start(self) -> None:
            observed["starts"] += 1

    monkeypatch.setattr(
        "shared.dependencies.providers.create_background_task_manager",
        lambda redis: _Manager(),
    )

    manager = await get_background_task_manager(container=container)  # type: ignore[arg-type]

    assert isinstance(manager, _Manager)
    assert observed == {"starts": 1}


@pytest.mark.asyncio
async def test_provider_starts_existing_background_task_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    redis_service = _RedisStub()
    observed: dict[str, int] = {"starts": 0}

    class _Manager:
        async def start(self) -> None:
            observed["starts"] += 1

    manager = _Manager()
    container = _ContainerStub(redis_service, existing_manager=manager)

    resolved = await get_background_task_manager(container=container)  # type: ignore[arg-type]

    assert resolved is manager
    assert observed == {"starts": 1}
