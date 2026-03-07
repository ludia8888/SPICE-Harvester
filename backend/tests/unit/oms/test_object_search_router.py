"""Tests for OMS Foundry-style object search router."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from oms.routers import query as query_router
from oms.routers.query import foundry_router
from shared.dependencies.providers import get_elasticsearch_service
from shared.utils.foundry_page_token import encode_offset_page_token

app = FastAPI()
app.include_router(foundry_router)


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
                    "data": {"customer_id": "cust_1", "status": "ACTIVE", "nickname": None},
                }
            ],
            "aggregations": {},
        }
    )
    return es


@pytest.fixture(autouse=True)
def override_deps(mock_es):
    async def fake_es():
        return mock_es

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
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body.get("data"), list)
    assert body["totalCount"] == "2"
    assert body["nextPageToken"]
    assert body["data"][0]["status"] == "ACTIVE"
    assert body["data"][0]["__apiName"] == "Customer"
    assert body["data"][0]["__primaryKey"] == "cust_1"
    assert body["data"][0]["properties"] == {"customer_id": "cust_1", "status": "ACTIVE"}
    assert "nickname" not in body["data"][0]

    search_call = mock_es.search.call_args
    search_query = search_call.kwargs["query"]
    must = search_query["bool"]["must"]
    assert {"term": {"class_id": "Customer"}} in must


@pytest.mark.asyncio
async def test_search_objects_v2_prefers_hits_over_missing_object_type_guard(
    mock_es,
    monkeypatch: pytest.MonkeyPatch,
):
    async def fake_missing_guard(**kwargs):  # noqa: ANN001
        _ = kwargs
        return JSONResponse(
            status_code=404,
            content={"errorCode": "NOT_FOUND", "errorName": "ObjectTypeNotFound"},
        )

    monkeypatch.setattr(query_router, "_ensure_object_type_exists", fake_missing_guard)

    payload = {"pageSize": 1}
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["totalCount"] == "2"
    assert body["data"][0]["__apiName"] == "Customer"


@pytest.mark.asyncio
async def test_search_objects_v2_returns_missing_object_type_when_guard_and_search_are_empty(
    mock_es,
    monkeypatch: pytest.MonkeyPatch,
):
    mock_es.search.return_value = {"total": 0, "hits": [], "aggregations": {}}

    async def fake_missing_guard(**kwargs):  # noqa: ANN001
        _ = kwargs
        return JSONResponse(
            status_code=404,
            content={"errorCode": "NOT_FOUND", "errorName": "ObjectTypeNotFound"},
        )

    monkeypatch.setattr(query_router, "_ensure_object_type_exists", fake_missing_guard)

    payload = {"pageSize": 1}
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 404
    body = resp.json()
    assert body["errorName"] == "ObjectTypeNotFound"


@pytest.mark.asyncio
async def test_search_objects_v2_retries_with_properties_when_data_query_misses(mock_es):
    mock_es.search.side_effect = [
        {
            "total": 0,
            "hits": [],
            "aggregations": {},
        },
        {
            "total": 1,
            "hits": [
                {
                    "instance_id": "order-1",
                    "class_id": "Order",
                    "properties": [
                        {"name": "order_id", "value": "order-1"},
                        {"name": "order_status", "value": "PENDING"},
                    ],
                }
            ],
            "aggregations": {},
        },
    ]

    payload = {
        "where": {"type": "eq", "field": "order_id", "value": "order-1"},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Order/search", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["totalCount"] == "1"
    assert body["data"][0]["order_id"] == "order-1"
    assert body["data"][0]["__primaryKey"] == "order-1"
    assert mock_es.search.call_count == 2

    fallback_call = mock_es.search.call_args_list[1]
    fallback_must = fallback_call.kwargs["query"]["bool"]["must"]
    assert {"term": {"class_id": "Order"}} in fallback_must
    assert "nested" in fallback_must[1]
    assert fallback_must[1]["nested"]["path"] == "properties"


@pytest.mark.asyncio
async def test_search_objects_v2_collapses_duplicate_overlay_rows(mock_es):
    mock_es.search.return_value = {
        "total": 2,
        "hits": [
            {
                "instance_id": "order-1",
                "class_id": "Order",
                "lifecycle_id": "lc-0",
                "updated_at": "2026-02-20T09:00:00+00:00",
                "data": {"order_id": "order-1", "order_status": "PENDING"},
            },
            {
                "instance_id": "order-1",
                "class_id": "Order",
                "lifecycle_id": "lc-0",
                "event_sequence": 100,
                "updated_at": "2026-02-20T09:00:10+00:00",
                "patchset_commit_id": "c1",
                "data": {"order_id": "order-1", "order_status": "ON_HOLD"},
            },
        ],
        "aggregations": {},
    }
    payload = {"pageSize": 20}
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Order/search", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert len(body["data"]) == 1
    assert body["data"][0]["order_id"] == "order-1"
    assert body["data"][0]["order_status"] == "ON_HOLD"


@pytest.mark.asyncio
async def test_search_objects_v2_hides_overlay_tombstoned_rows(mock_es):
    mock_es.search.return_value = {
        "total": 2,
        "hits": [
            {
                "instance_id": "order-1",
                "class_id": "Order",
                "lifecycle_id": "lc-0",
                "updated_at": "2026-02-20T09:00:00+00:00",
                "data": {"order_id": "order-1", "order_status": "PENDING"},
            },
            {
                "instance_id": "order-1",
                "class_id": "Order",
                "lifecycle_id": "lc-0",
                "event_sequence": 101,
                "updated_at": "2026-02-20T09:00:20+00:00",
                "overlay_tombstone": True,
                "patchset_commit_id": "c2",
                "data": {"order_id": "order-1"},
            },
        ],
        "aggregations": {},
    }
    payload = {"pageSize": 20}
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Order/search", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["data"] == []


@pytest.mark.asyncio
async def test_search_objects_v2_allows_missing_where_with_match_all_fallback(mock_es):
    payload = {"pageSize": 5}
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

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
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["data"][0]["customer_id"] == "cust_1"
    assert body["data"][0]["status"] == "ACTIVE"
    assert body["data"][0]["__apiName"] == "Customer"
    assert body["data"][0]["__primaryKey"] == "cust_1"
    assert body["data"][0]["instance_id"] == "cust_1"
    assert body["data"][0]["class_id"] == "Customer"
    assert body["data"][0]["properties"] == {"customer_id": "cust_1", "status": "ACTIVE"}

    search_call = mock_es.search.call_args
    sort = search_call.kwargs["sort"]
    assert sort[0] == {"data.customer_id": {"order": "desc"}}
    assert sort[-1] == {"instance_id": {"order": "asc"}}


@pytest.mark.asyncio
async def test_search_objects_v2_preserves_existing_properties_object(mock_es):
    mock_es.search.return_value = {
        "total": 1,
        "hits": [
            {
                "instance_id": "cust_1",
                "class_id": "Customer",
                "properties": {
                    "customer_id": "cust_1",
                    "status": "ACTIVE",
                },
                "data": {
                    "customer_id": "cust_1",
                    "status": "ACTIVE",
                },
            }
        ],
        "aggregations": {},
    }
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["data"][0]["properties"] == {
        "customer_id": "cust_1",
        "status": "ACTIVE",
    }


@pytest.mark.asyncio
async def test_search_objects_v2_rejects_select_and_select_v2_together():
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "select": ["customer_id"],
        "selectV2": [{"propertyApiName": "status"}],
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

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
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "InvalidArgument"
    assert isinstance(body["errorInstanceId"], str)
    assert isinstance(body["parameters"], dict)


@pytest.mark.asyncio
async def test_search_objects_v2_expired_page_token_returns_foundry_error():
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "pageToken": encode_offset_page_token(10, issued_at=0),
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "InvalidArgument"


@pytest.mark.asyncio
async def test_search_objects_v2_rejects_page_token_scope_mismatch():
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "pageToken": encode_offset_page_token(10, scope="v2/searchObjects|other-scope"),
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "InvalidArgument"


@pytest.mark.asyncio
async def test_search_objects_v2_accepts_scope_matched_page_token(mock_es):
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "pageSize": 1,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)
        assert first.status_code == 200
        token = first.json().get("nextPageToken")
        assert isinstance(token, str) and token

        second_payload = dict(payload)
        second_payload["pageToken"] = token
        second = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=second_payload)

    assert second.status_code == 200
    search_call = mock_es.search.call_args
    assert search_call.kwargs["from_"] == 1


@pytest.mark.asyncio
async def test_search_objects_v2_rejects_page_token_when_page_size_changes():
    initial_payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "pageSize": 1,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=initial_payload)
        assert first.status_code == 200
        token = first.json().get("nextPageToken")
        assert isinstance(token, str) and token

        changed_page_size_payload = {
            "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
            "pageSize": 2,
            "pageToken": token,
        }
        second = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=changed_page_size_payload)

    assert second.status_code == 400
    body = second.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "InvalidArgument"


@pytest.mark.asyncio
async def test_search_objects_v2_accepts_deprecated_startswith_alias(mock_es):
    payload = {
        "where": {"type": "startsWith", "field": "customer_name", "value": "Kim"},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    search_query = search_call.kwargs["query"]
    must = search_query["bool"]["must"]
    assert {"match_phrase_prefix": {"data.customer_name": "Kim"}} in must


@pytest.mark.asyncio
async def test_search_objects_v2_accepts_contains_any_term_operator(mock_es):
    payload = {
        "where": {"type": "containsAnyTerm", "field": "customer_name", "value": "Kim"},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

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
async def test_search_objects_v2_in_operator_valid_list_accepted(mock_es):
    """``in`` operator with a non-empty list should be accepted (previously rejected)."""
    payload = {
        "where": {"type": "in", "field": "status", "value": ["ACTIVE", "PENDING"]},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_objects_v2_is_null_false_maps_to_exists_clause(mock_es):
    payload = {
        "where": {"type": "isNull", "field": "status", "value": False},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    search_query = search_call.kwargs["query"]
    must = search_query["bool"]["must"]
    assert {"exists": {"field": "data.status"}} in must


@pytest.mark.asyncio
async def test_search_objects_v2_rejects_is_null_with_non_boolean_value():
    payload = {
        "where": {"type": "isNull", "field": "status", "value": "nope"},
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

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
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

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
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

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
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 400
    body = resp.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_search_objects_v2_accepts_foundry_branch_rid(mock_es):
    payload = {
        "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
        "pageSize": 10,
    }
    branch_rid = "ri.ontology.main.branch.809f45f2-8f80-4f18-ba5e-34725fb85f65"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            f"/v2/ontologies/test_db/objects/Customer/search?branch={branch_rid}",
            json=payload,
        )

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    index_name = search_call.kwargs["index"]
    assert index_name.startswith("test_db_instances__br_")


# ---------------------------------------------------------------------------
# ``in`` operator
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_v2_in_operator_generates_terms_query(mock_es):
    payload = {
        "where": {"type": "in", "field": "status", "value": ["ACTIVE", "PENDING"]},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    must = search_call.kwargs["query"]["bool"]["must"]
    terms_clause = must[1]
    assert "terms" in terms_clause
    assert terms_clause["terms"]["data.status"] == ["ACTIVE", "PENDING"]


@pytest.mark.asyncio
async def test_search_v2_in_operator_rejects_empty_list():
    payload = {
        "where": {"type": "in", "field": "status", "value": []},
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# ``interval`` operator
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_v2_interval_operator_generates_intervals_query(mock_es):
    payload = {
        "where": {
            "type": "interval",
            "field": "description",
            "rule": {"match": {"query": "quick fox", "maxGaps": 0, "ordered": True}},
        },
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    must = search_call.kwargs["query"]["bool"]["must"]
    interval_clause = must[1]
    assert "intervals" in interval_clause
    match_spec = interval_clause["intervals"]["data.description"]["match"]
    assert match_spec["query"] == "quick fox"
    assert match_spec["max_gaps"] == 0
    assert match_spec["ordered"] is True


# ---------------------------------------------------------------------------
# Geo-spatial operators
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_v2_within_distance_of_generates_nested_geo_query(mock_es):
    payload = {
        "where": {
            "type": "withinDistanceOf",
            "field": "location",
            "value": {
                "center": {"type": "Point", "coordinates": [126.978, 37.566]},
                "distance": {"value": 10, "unit": "KILOMETERS"},
            },
        },
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    must = search_call.kwargs["query"]["bool"]["must"]
    geo_clause = must[1]
    # Should be wrapped in nested
    assert "nested" in geo_clause
    assert geo_clause["nested"]["path"] == "properties"
    inner_must = geo_clause["nested"]["query"]["bool"]["must"]
    # Name filter
    assert inner_must[0] == {"term": {"properties.name": "location"}}
    # Geo distance query
    assert "geo_distance" in inner_must[1]
    assert inner_must[1]["geo_distance"]["distance"] == "10km"


@pytest.mark.asyncio
async def test_search_v2_within_bounding_box_generates_nested_geo_query(mock_es):
    payload = {
        "where": {
            "type": "withinBoundingBox",
            "field": "location",
            "value": {
                "topLeft": {"latitude": 37.6, "longitude": 126.9},
                "bottomRight": {"latitude": 37.5, "longitude": 127.1},
            },
        },
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    must = search_call.kwargs["query"]["bool"]["must"]
    geo_clause = must[1]
    assert "nested" in geo_clause
    inner_must = geo_clause["nested"]["query"]["bool"]["must"]
    assert "geo_bounding_box" in inner_must[1]


@pytest.mark.asyncio
async def test_search_v2_within_polygon_generates_nested_geo_query(mock_es):
    polygon_coords = [[[126.9, 37.5], [127.1, 37.5], [127.1, 37.6], [126.9, 37.6], [126.9, 37.5]]]
    payload = {
        "where": {
            "type": "withinPolygon",
            "field": "area",
            "value": {"type": "Polygon", "coordinates": polygon_coords},
        },
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    must = search_call.kwargs["query"]["bool"]["must"]
    geo_clause = must[1]
    assert "nested" in geo_clause
    inner_must = geo_clause["nested"]["query"]["bool"]["must"]
    assert "geo_shape" in inner_must[1]
    assert inner_must[1]["geo_shape"]["properties.geo_point"]["relation"] == "within"


@pytest.mark.asyncio
async def test_search_v2_intersects_bounding_box_generates_geo_shape_query(mock_es):
    payload = {
        "where": {
            "type": "intersectsBoundingBox",
            "field": "coverage",
            "value": {
                "topLeft": {"lat": 37.6, "lon": 126.9},
                "bottomRight": {"lat": 37.5, "lon": 127.1},
            },
        },
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    must = search_call.kwargs["query"]["bool"]["must"]
    geo_clause = must[1]
    assert "nested" in geo_clause
    inner_must = geo_clause["nested"]["query"]["bool"]["must"]
    geo_shape = inner_must[1]
    assert "geo_shape" in geo_shape
    assert geo_shape["geo_shape"]["properties.geo_shape"]["relation"] == "intersects"


@pytest.mark.asyncio
async def test_search_v2_does_not_intersect_polygon_generates_disjoint_query(mock_es):
    polygon_coords = [[[126.9, 37.5], [127.1, 37.5], [127.1, 37.6], [126.9, 37.6], [126.9, 37.5]]]
    payload = {
        "where": {
            "type": "doesNotIntersectPolygon",
            "field": "coverage",
            "value": {"type": "Polygon", "coordinates": polygon_coords},
        },
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 200
    search_call = mock_es.search.call_args
    must = search_call.kwargs["query"]["bool"]["must"]
    geo_clause = must[1]
    assert "nested" in geo_clause
    inner_must = geo_clause["nested"]["query"]["bool"]["must"]
    geo_shape = inner_must[1]
    assert "geo_shape" in geo_shape
    assert geo_shape["geo_shape"]["properties.geo_shape"]["relation"] == "disjoint"


@pytest.mark.asyncio
async def test_search_v2_geo_operator_rejects_non_dict_value():
    payload = {
        "where": {
            "type": "withinDistanceOf",
            "field": "location",
            "value": "invalid",
        },
        "pageSize": 10,
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/v2/ontologies/test_db/objects/Customer/search", json=payload)

    assert resp.status_code == 400
