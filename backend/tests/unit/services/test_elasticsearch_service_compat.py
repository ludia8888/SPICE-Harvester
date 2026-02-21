"""Tests for Elasticsearch client compatibility header behavior."""

from __future__ import annotations

import asyncio

import elasticsearch._async.client._base as async_base
import pytest

from shared.services.storage import elasticsearch_service as es_module


def test_resolve_compat_version_uses_explicit_env(monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_COMPAT_VERSION", "7")
    monkeypatch.setattr(es_module, "_ELASTIC_CLIENT_VERSION", "9.1.0")

    assert es_module._resolve_compat_version() == "7"


def test_resolve_compat_version_defaults_to_8_for_v9_client(monkeypatch):
    monkeypatch.delenv("ELASTICSEARCH_COMPAT_VERSION", raising=False)
    monkeypatch.delenv("ES_COMPAT_VERSION", raising=False)
    monkeypatch.setattr(es_module, "_ELASTIC_CLIENT_VERSION", "9.2.1")

    assert es_module._resolve_compat_version() == "8"


def test_service_injects_compat_headers_for_v9_client(monkeypatch):
    monkeypatch.delenv("ELASTICSEARCH_COMPAT_VERSION", raising=False)
    monkeypatch.delenv("ES_COMPAT_VERSION", raising=False)
    monkeypatch.setattr(es_module, "_ELASTIC_CLIENT_VERSION", "9.0.0")

    service = es_module.ElasticsearchService(host="127.0.0.1", port=9200)

    assert service.config["headers"]["accept"] == "application/vnd.elasticsearch+json; compatible-with=8"
    assert service.config["headers"]["content-type"] == "application/vnd.elasticsearch+json; compatible-with=8"


def test_service_patches_elasticsearch_v9_compat_template(monkeypatch):
    monkeypatch.delenv("ELASTICSEARCH_COMPAT_VERSION", raising=False)
    monkeypatch.delenv("ES_COMPAT_VERSION", raising=False)
    monkeypatch.setattr(es_module, "_ELASTIC_CLIENT_VERSION", "9.0.0")

    original_template = async_base._COMPAT_MIMETYPE_TEMPLATE
    original_sub = async_base._COMPAT_MIMETYPE_SUB
    try:
        es_module.ElasticsearchService(host="127.0.0.1", port=9200)
        assert "compatible-with=8" in async_base._COMPAT_MIMETYPE_TEMPLATE
        assert "compatible-with=8" in async_base._COMPAT_MIMETYPE_SUB
    finally:
        async_base._COMPAT_MIMETYPE_TEMPLATE = original_template
        async_base._COMPAT_MIMETYPE_SUB = original_sub


@pytest.mark.asyncio
async def test_connect_is_idempotent_under_concurrency(monkeypatch: pytest.MonkeyPatch) -> None:
    created_clients: list["_FakeClient"] = []

    class _FakeClient:
        def __init__(self, **_: object) -> None:
            self.closed = False
            created_clients.append(self)

        async def info(self) -> dict[str, object]:
            await asyncio.sleep(0)
            return {"version": {"number": "8.12.2"}}

        async def ping(self) -> bool:
            await asyncio.sleep(0)
            return True

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(es_module, "AsyncElasticsearch", _FakeClient)

    service = es_module.ElasticsearchService(host="127.0.0.1", port=9200)
    await asyncio.gather(*[service.connect() for _ in range(10)])

    assert len(created_clients) == 1
    assert service._client is created_clients[0]


@pytest.mark.asyncio
async def test_disconnect_clears_client_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    created_clients: list["_FakeClient"] = []

    class _FakeClient:
        def __init__(self, **_: object) -> None:
            self.closed = False
            created_clients.append(self)

        async def info(self) -> dict[str, object]:
            return {"version": {"number": "8.12.2"}}

        async def ping(self) -> bool:
            return True

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(es_module, "AsyncElasticsearch", _FakeClient)

    service = es_module.ElasticsearchService(host="127.0.0.1", port=9200)
    await service.connect()
    assert service._client is created_clients[0]

    await service.disconnect()

    assert created_clients[0].closed is True
    assert service._client is None
