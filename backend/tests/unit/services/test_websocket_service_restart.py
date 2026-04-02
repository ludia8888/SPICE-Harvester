from __future__ import annotations

import pytest

from shared.services.core.websocket_service import (
    WebSocketConnectionManager,
    WebSocketNotificationService,
    get_notification_service,
)


class _RedisStub:
    client = None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_restart_pubsub_listener_resets_running_before_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    service = WebSocketNotificationService(
        redis_service=_RedisStub(),  # type: ignore[arg-type]
        connection_manager=WebSocketConnectionManager(),
    )
    service.running = True
    observed: dict[str, bool] = {}

    async def _fake_sleep(_seconds: float) -> None:
        return None

    async def _fake_start() -> None:
        observed["running_before_start"] = service.running
        service.running = True

    monkeypatch.setattr("shared.services.core.websocket_service.asyncio.sleep", _fake_sleep)
    monkeypatch.setattr(service, "start", _fake_start)

    await service._restart_pubsub_listener()

    assert observed == {"running_before_start": False}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_stop_resets_notification_singleton_for_new_redis_client() -> None:
    redis1 = _RedisStub()
    redis2 = _RedisStub()

    service1 = get_notification_service(redis1)  # type: ignore[arg-type]
    await service1.stop()
    service2 = get_notification_service(redis2)  # type: ignore[arg-type]

    assert service2 is not service1
    assert service2.redis is redis2
