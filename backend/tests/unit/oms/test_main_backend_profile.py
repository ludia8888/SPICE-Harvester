from __future__ import annotations

from types import SimpleNamespace
import json

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


class _FakeInnerContainer:
    async def health_check_all(self):  # noqa: ANN201
        return {"status": "healthy"}


class _FakeOMSContainer:
    def __init__(
        self,
        *,
        jsonld_ok: bool = True,
        label_mapper_ok: bool = True,
        redis_service: object | None = object(),
        command_status_service: object | None = object(),
        elasticsearch_service: object | None = object(),
    ) -> None:
        self.container = _FakeInnerContainer()
        self._jsonld_ok = jsonld_ok
        self._label_mapper_ok = label_mapper_ok
        self._redis_service = redis_service
        self._command_status_service = command_status_service
        self._elasticsearch_service = elasticsearch_service

    def get_jsonld_converter(self):  # noqa: ANN201
        if not self._jsonld_ok:
            raise RuntimeError("jsonld unavailable")
        return object()

    def get_label_mapper(self):  # noqa: ANN201
        if not self._label_mapper_ok:
            raise RuntimeError("label mapper unavailable")
        return object()

    def get_redis_service(self):  # noqa: ANN201
        return self._redis_service

    def get_command_status_service(self):  # noqa: ANN201
        return self._command_status_service

    def get_elasticsearch_service(self):  # noqa: ANN201
        return self._elasticsearch_service


@pytest.mark.asyncio
async def test_container_health_check_returns_unified_hard_down_surface_when_container_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(oms_main, "_oms_container", None)

    response = await oms_main.container_health_check()

    assert response.status_code == 503
    payload = response.body.decode()
    assert "hard_down" in payload
    assert "OMS container not initialized" in payload


@pytest.mark.asyncio
async def test_container_health_check_returns_degraded_surface_for_optional_dependency_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        oms_main,
        "get_settings",
        lambda: SimpleNamespace(environment=SimpleNamespace(value="test"), debug=False),
    )
    monkeypatch.setattr(
        oms_main,
        "_oms_container",
        _FakeOMSContainer(redis_service=None, command_status_service=None, elasticsearch_service=None),
    )

    response = await oms_main.container_health_check()
    payload = json.loads(response.body)

    assert payload["status"] == "degraded"
    assert payload["dependency_status"]["redis"] == "degraded"
    assert payload["dependency_status"]["command_status"] == "degraded"
    assert payload["dependency_status"]["elasticsearch"] == "degraded"
    assert payload["oms_services"]["redis_service"]["status"] == "degraded"
    assert payload["oms_services"]["redis_service"]["classification"] == "unavailable"
    assert payload["oms_services"]["command_status_service"]["status"] == "degraded"
    assert payload["oms_services"]["elasticsearch_service"]["status"] == "degraded"
    assert "oms.command_status" in payload["affected_features"]


@pytest.mark.asyncio
async def test_container_health_check_returns_canonical_nested_status_for_required_services(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        oms_main,
        "get_settings",
        lambda: SimpleNamespace(environment=SimpleNamespace(value="test"), debug=False),
    )
    monkeypatch.setattr(
        oms_main,
        "_oms_container",
        _FakeOMSContainer(jsonld_ok=False, label_mapper_ok=False),
    )

    response = await oms_main.container_health_check()
    payload = json.loads(response.body)

    assert response.status_code == 503
    assert payload["status"] == "hard_down"
    assert payload["oms_services"]["jsonld_converter"]["status"] == "hard_down"
    assert payload["oms_services"]["jsonld_converter"]["classification"] == "internal"
    assert payload["oms_services"]["label_mapper"]["status"] == "hard_down"
    assert payload["oms_services"]["label_mapper"]["classification"] == "internal"
