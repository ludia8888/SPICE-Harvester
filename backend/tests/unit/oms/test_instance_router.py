"""Tests for OMS instance router (ES-backed, Phase 2)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from oms.dependencies import ValidatedDatabaseName
from oms.routers.instance import router
from shared.dependencies.providers import get_elasticsearch_service

app = FastAPI()
app.include_router(router)


def _mock_es():
    es = AsyncMock()
    es.search = AsyncMock(return_value={
        "total": 2,
        "hits": [
            {"instance_id": "Customer/c1", "class_id": "Customer", "data": {"name": "Alice"}},
            {"instance_id": "Customer/c2", "class_id": "Customer", "data": {"name": "Bob"}},
        ],
        "aggregations": {},
    })
    es.get_document = AsyncMock(return_value={
        "instance_id": "cust_c1",
        "class_id": "Customer",
        "data": {"name": "Alice"},
    })
    es.count = AsyncMock(return_value=42)
    return es


@pytest.fixture
def mock_es():
    return _mock_es()


@pytest.fixture(autouse=True)
def override_deps(mock_es):
    """Override FastAPI dependencies with mocks."""

    def fake_db_name():
        return "test_db"

    async def fake_es():
        return mock_es

    app.dependency_overrides[ValidatedDatabaseName] = fake_db_name
    app.dependency_overrides[get_elasticsearch_service] = fake_es
    yield
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_class_instances(mock_es):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/instance/test_db/class/Customer/instances?limit=10&offset=0")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert body["source"] == "elasticsearch"
    assert body["total"] == 2
    assert len(body["instances"]) == 2
    mock_es.search.assert_called_once()


@pytest.mark.asyncio
async def test_get_class_instances_with_search(mock_es):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/instance/test_db/class/Customer/instances?search=Alice")

    assert resp.status_code == 200
    body = resp.json()
    assert body["search"] == "Alice"
    call_kwargs = mock_es.search.call_args
    query = call_kwargs.kwargs.get("query") or call_kwargs[1].get("query")
    assert "bool" in query


@pytest.mark.asyncio
async def test_get_instance(mock_es):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/instance/test_db/instance/cust_c1")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert body["source"] == "elasticsearch"
    assert body["data"]["instance_id"] == "cust_c1"


@pytest.mark.asyncio
async def test_get_instance_not_found(mock_es):
    mock_es.get_document = AsyncMock(return_value=None)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/instance/test_db/instance/cust_missing")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_instance_class_mismatch(mock_es):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/instance/test_db/instance/cust_c1?class_id=Order")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_class_instance_count(mock_es):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/instance/test_db/class/Customer/count")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 42


@pytest.mark.asyncio
async def test_sparql_returns_410():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/instance/test_db/sparql")

    assert resp.status_code == 410
