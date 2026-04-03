from __future__ import annotations

import pytest

from oms.dependencies import OMSDependencyProvider
from shared.config.settings import ApplicationSettings
from shared.dependencies.container import ServiceContainer
from shared.services.core.command_status_service import CommandStatusService
from shared.services.registries.processed_event_registry import ProcessedEventRegistry


class _FakeRedisService:
    async def initialize(self) -> None:
        return None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_command_status_service_creates_and_reuses_cached_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "oms.dependencies.create_redis_service",
        lambda settings: _FakeRedisService(),
    )
    container = ServiceContainer(ApplicationSettings(environment="test"))

    created = await OMSDependencyProvider.get_command_status_service(container=container)
    reused = await OMSDependencyProvider.get_command_status_service(container=container)

    assert isinstance(created, CommandStatusService)
    assert reused is created


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_command_status_service_returns_none_when_redis_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _broken_factory(settings):  # noqa: ANN001
        raise RuntimeError("redis down")

    monkeypatch.setattr("oms.dependencies.create_redis_service", _broken_factory)
    container = ServiceContainer(ApplicationSettings(environment="test"))

    resolved = await OMSDependencyProvider.get_command_status_service(container=container)

    assert resolved is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_command_status_service_raises_for_invalid_existing_registration() -> None:
    container = ServiceContainer(ApplicationSettings(environment="test"))
    container.register_singleton(CommandStatusService, lambda settings: CommandStatusService(_FakeRedisService()))

    with pytest.raises(RuntimeError, match="registered without a created instance"):
        await OMSDependencyProvider.get_command_status_service(container=container)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_processed_event_registry_returns_none_when_connection_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _broken_factory(**kwargs):  # noqa: ANN003
        raise OSError("registry down")

    monkeypatch.setattr("oms.dependencies.create_processed_event_registry", _broken_factory)
    container = ServiceContainer(ApplicationSettings(environment="test"))

    resolved = await OMSDependencyProvider.get_processed_event_registry(container=container)

    assert resolved is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_processed_event_registry_does_not_hide_internal_factory_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _broken_factory(**kwargs):  # noqa: ANN003
        raise RuntimeError("unexpected internal bug")

    monkeypatch.setattr("oms.dependencies.create_processed_event_registry", _broken_factory)
    container = ServiceContainer(ApplicationSettings(environment="test"))

    with pytest.raises(RuntimeError, match="unexpected internal bug"):
        await OMSDependencyProvider.get_processed_event_registry(container=container)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_processed_event_registry_does_not_hide_container_registration_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = object()

    async def _factory(**kwargs):  # noqa: ANN003
        return sentinel

    class _BrokenContainer:
        def has(self, service_type):  # noqa: ANN001, ANN201
            return False

        def is_created(self, service_type):  # noqa: ANN001, ANN201
            return False

        async def get(self, service_type):  # noqa: ANN001, ANN201
            raise AssertionError("get() should not be called")

        def register_instance(self, service_type, instance):  # noqa: ANN001, ANN201
            raise RuntimeError("registration conflict")

    monkeypatch.setattr("oms.dependencies.create_processed_event_registry", _factory)

    with pytest.raises(RuntimeError, match="registration conflict"):
        await OMSDependencyProvider.get_processed_event_registry(container=_BrokenContainer())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_processed_event_registry_raises_for_invalid_existing_registration() -> None:
    container = ServiceContainer(ApplicationSettings(environment="test"))
    container.register_singleton(ProcessedEventRegistry, lambda settings: object())

    with pytest.raises(RuntimeError, match="registered without a created instance"):
        await OMSDependencyProvider.get_processed_event_registry(container=container)
