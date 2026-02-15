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
