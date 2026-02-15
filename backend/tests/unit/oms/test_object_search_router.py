"""Tests for OMS Foundry-style object search router."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from oms.dependencies import ValidatedDatabaseName
from oms.routers.query import router
from shared.dependencies.providers import get_elasticsearch_service

app = FastAPI()
app.include_router(router)


@pytest.fixture
def mock_es():
    es = AsyncMock()
    es.search = AsyncMock(
        return_value={
            "total": 2,
            "hits": [
                {
                    "instance_id": "cust_1",
                    "class_id": "Customer",
                    "data": {"customer_id": "cust_1", "status": "ACTIVE"},
                }
            ],
            "aggregations": {},
        }
    )
    return es


@pytest.fixture(autouse=True)
def override_deps(mock_es):
    def fake_db_name():
        return "test_db"

    async def fake_es():
        return mock_es

    app.dependency_overrides[ValidatedDatabaseName] = fake_db_name
    app.dependency_overrides[get_elasticsearch_service] = fake_es
    yield
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_search_objects_v2_returns_foundry_shape(mock_es):
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "pageSize": 1,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body.get("data"), list)
    assert body["totalCount"] == "2"
    assert body["nextPageToken"]
    assert body["data"][0]["status"] == "ACTIVE"

    search_call = mock_es.search.call_args
    search_query = search_call.kwargs["query"]
    must = search_query["bool"]["must"]
    assert {"term": {"class_id": "Customer"}} in must


@pytest.mark.asyncio
async def test_search_objects_v2_allows_missing_where_with_match_all_fallback(mock_es):
    payload = {"pageSize": 5}
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    search_query = search_call.kwargs["query"]
    must = search_query["bool"]["must"]
    assert {"term": {"class_id": "Customer"}} in must
    assert isinstance(must[1], dict)


@pytest.mark.asyncio
async def test_search_objects_v2_supports_select_and_order_by_pushdown(mock_es):
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "select": ["customer_id", "status"],
        "orderBy": {"orderType": "fields", "fields": [{"field": "customer_id", "direction": "desc"}]},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["data"] == [{"customer_id": "cust_1", "status": "ACTIVE"}]

    search_call = mock_es.search.call_args
    sort = search_call.kwargs["sort"]
    assert sort[0] == {"data.customer_id": {"order": "desc"}}
    assert sort[-1] == {"instance_id": {"order": "asc"}}


@pytest.mark.asyncio
async def test_search_objects_v2_rejects_select_and_select_v2_together():
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "select": ["customer_id"],
        "selectV2": [{"propertyApiName": "status"}],
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_search_objects_v2_invalid_page_token_returns_foundry_error():
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "pageToken": "invalid@@token",
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "InvalidArgument"
    assert isinstance(body["errorInstanceId"], str)
    assert isinstance(body["parameters"], dict)


@pytest.mark.asyncio
async def test_search_objects_v2_rejects_deprecated_startswith_operator():
    payload = {
        "where": {"type": "startsWith", "field": "customer_name", "value": "Kim"},
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "InvalidArgument"
    assert isinstance(body["errorInstanceId"], str)
    assert isinstance(body["parameters"], dict)


@pytest.mark.asyncio
async def test_search_objects_v2_accepts_contains_any_term_operator(mock_es):
    payload = {
        "where": {"type": "containsAnyTerm", "field": "customer_name", "value": "Kim"},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    search_query = search_call.kwargs["query"]
    must = search_query["bool"]["must"]
    assert {
        "match": {
            "data.customer_name": {
                "query": "Kim",
                "operator": "or",
            }
        }
    } in must


@pytest.mark.asyncio
async def test_search_objects_v2_accepts_in_operator(mock_es):
    payload = {
        "where": {"type": "in", "field": "status", "value": ["ACTIVE", "PENDING"]},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    search_query = search_call.kwargs["query"]
    must = search_query["bool"]["must"]
    assert {"terms": {"data.status": ["ACTIVE", "PENDING"]}} in must


@pytest.mark.asyncio
async def test_search_objects_v2_rejects_in_operator_with_non_list():
    payload = {
        "where": {"type": "in", "field": "status", "value": "ACTIVE"},
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_search_objects_v2_accepts_non_deprecated_operator(mock_es):
    payload = {
        "where": {
            "type": "containsAllTermsInOrderPrefixLastTerm",
            "field": "customer_name",
            "value": "Kim",
        },
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    search_query = search_call.kwargs["query"]
    must = search_query["bool"]["must"]
    assert {"match_phrase_prefix": {"data.customer_name": "Kim"}} in must


@pytest.mark.asyncio
async def test_search_objects_v2_rejects_non_foundry_anyterm_alias():
    payload = {
        "where": {"type": "anyTerm", "field": "customer_name", "value": "Kim"},
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "InvalidArgument"


@pytest.mark.asyncio
async def test_search_objects_v2_rejects_excessive_nesting_depth():
    payload = {
        "where": {
            "type": "and",
            "value": [
                {
                    "type": "and",
                    "value": [
                        {
                            "type": "and",
                            "value": [
                                {
                                    "type": "and",
                                    "value": [{"type": "eq", "field": "status", "value": "ACTIVE"}],
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/objects/test_db/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
