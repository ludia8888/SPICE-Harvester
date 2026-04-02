from __future__ import annotations

from types import SimpleNamespace

import pytest

from oms import main as oms_main


@pytest.mark.asyncio
async def test_root_reports_postgres_profile_features(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_settings = SimpleNamespace(
        ontology=SimpleNamespace(resource_storage_backend="postgres"),
        environment=SimpleNamespace(value="test"),
    )
    monkeypatch.setattr(oms_main, "get_settings", lambda: stub_settings)

    payload = await oms_main.root()

    assert payload["resource_storage_backend"] == "postgres"
    assert "레거시 브랜치/버전 API 제거" in payload["features"]


@pytest.mark.asyncio
async def test_root_forces_postgres_backend_when_legacy_profile_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub_settings = SimpleNamespace(
        ontology=SimpleNamespace(resource_storage_backend="hybrid"),
        environment=SimpleNamespace(value="test"),
    )
    monkeypatch.setattr(oms_main, "get_settings", lambda: stub_settings)

    payload = await oms_main.root()

    assert payload["resource_storage_backend"] == "postgres"
    assert "레거시 브랜치/버전 API 제거" in payload["features"]


class _FakeContainer:
    def __init__(self) -> None:
        self.calls: list[tuple[object, object, bool]] = []

    def register_instance(self, service_type, instance, *, replace: bool = False) -> None:  # noqa: ANN001
        self.calls.append((service_type, instance, replace))


class _FakeRedisService:
    def __init__(self, *, fail_connect: bool = False) -> None:
        self.fail_connect = fail_connect
        self.connected = False

    async def connect(self) -> None:
        if self.fail_connect:
            raise RuntimeError("redis unavailable")
        self.connected = True


@pytest.mark.asyncio
async def test_initialize_redis_and_command_status_registers_services(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_redis = _FakeRedisService()
    fake_container = _FakeContainer()

    monkeypatch.setattr(oms_main, "create_redis_service", lambda settings: fake_redis)
    monkeypatch.setattr(
        oms_main,
        "CommandStatusService",
        lambda redis: SimpleNamespace(redis=redis, name="command-status"),
    )

    service_container = oms_main.OMSServiceContainer(fake_container, SimpleNamespace())
    await service_container._initialize_redis_and_command_status()

    assert service_container._oms_services["redis_service"] is fake_redis
    assert service_container._oms_services["command_status_service"].redis is fake_redis
    assert [call[2] for call in fake_container.calls] == [True, True]


@pytest.mark.asyncio
async def test_initialize_redis_and_command_status_degrades_when_redis_connect_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_container = _FakeContainer()

    monkeypatch.setattr(oms_main, "create_redis_service", lambda settings: _FakeRedisService(fail_connect=True))

    service_container = oms_main.OMSServiceContainer(fake_container, SimpleNamespace())
    await service_container._initialize_redis_and_command_status()

    assert service_container._oms_services["redis_service"] is None
    assert service_container._oms_services["command_status_service"] is None
    assert fake_container.calls == []
