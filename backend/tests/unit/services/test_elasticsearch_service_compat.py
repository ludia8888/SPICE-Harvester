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


@pytest.mark.asyncio
async def test_search_hides_staged_objectify_documents_from_instances_index() -> None:
    class _FakeClient:
        async def search(self, **_: object) -> dict[str, object]:
            return {
                "hits": {
                    "total": {"value": 1},
                    "hits": [
                        {
                            "_source": {
                                "instance_id": "order-1",
                                "objectify_visibility_state": "staged",
                            }
                        }
                    ],
                }
            }

    service = es_module.ElasticsearchService(host="127.0.0.1", port=9200)
    service._client = _FakeClient()  # type: ignore[assignment]

    result = await service.search(index="demo_instances", query={"term": {"class_id": "Order"}})
    assert result["hits"] == []


@pytest.mark.asyncio
async def test_get_document_hides_staged_objectify_documents_from_instances_index() -> None:
    class _FakeClient:
        async def get(self, **_: object) -> dict[str, object]:
            return {"_source": {"instance_id": "order-1", "objectify_visibility_state": "staged"}}

    service = es_module.ElasticsearchService(host="127.0.0.1", port=9200)
    service._client = _FakeClient()  # type: ignore[assignment]

    assert await service.get_document(index="demo_instances", doc_id="order-1") is None


@pytest.mark.asyncio
async def test_count_adds_visibility_filter_for_instances_index() -> None:
    recorded_body: dict[str, object] = {}

    class _FakeClient:
        async def count(self, *, index: str, body: dict[str, object]) -> dict[str, object]:
            recorded_body["index"] = index
            recorded_body["body"] = body
            return {"count": 3}

    service = es_module.ElasticsearchService(host="127.0.0.1", port=9200)
    service._client = _FakeClient()  # type: ignore[assignment]

    count = await service.count(index="demo_instances", query={"term": {"class_id": "Order"}})
    assert count == 3
    query = recorded_body["body"]["query"]  # type: ignore[index]
    assert query["bool"]["must_not"][0]["term"]["objectify_visibility_state"] == "staged"  # type: ignore[index]
