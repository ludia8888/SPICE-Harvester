from __future__ import annotations

import json

import pytest
from fastapi.responses import JSONResponse
from starlette.requests import Request

from bff.routers import foundry_ontology_v2 as router_v2


class _FakeOMSClient:
    async def list_databases(self):  # noqa: ANN201
        return {
            "data": {
                "databases": [
                    {"name": "test_db", "description": "Test ontology", "rid": "ri.ontology.test_db"},
                    {"name": "sales", "description": "Sales ontology"},
                ]
            }
        }

    async def get_database(self, db_name):  # noqa: ANN001
        if db_name == "missing":
            raise RuntimeError("not found")
        return {"data": {"name": db_name, "description": f"{db_name} ontology"}}

    async def post(self, path, **kwargs):  # noqa: ANN001, ANN003
        _ = kwargs
        if path.endswith("/search"):
            return {
                "data": [{"instance_id": "i-1", "name": "Alice"}],
                "nextPageToken": None,
                "totalCount": "1",
            }
        return {}

    async def list_ontology_resources(
        self,
        db_name,
        *,
        resource_type,
        branch="main",
        limit=200,
        offset=0,
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch, limit, offset
        if resource_type == "object_type":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "Account",
                            "label": {"en": "Account"},
                            "spec": {
                                "status": "ACTIVE",
                                "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
                            },
                        }
                    ]
                }
            }
        if resource_type == "link_type":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "owned_by",
                            "spec": {
                                "from": "Account",
                                "to": "User",
                                "cardinality": "n:1",
                                "relationship_spec": {"fk_column": "owner_id"},
                            },
                        },
                        {
                            "id": "contains",
                            "spec": {
                                "from": "Order",
                                "to": "Product",
                                "cardinality": "1:n",
                            },
                        },
                    ]
                }
            }
        return {"data": {"resources": []}}

    async def get_ontology_resource(
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch
        if resource_type == "object_type":
            return {
                "data": {
                    "id": resource_id,
                    "label": {"en": "Account"},
                    "spec": {
                        "status": "ACTIVE",
                        "visibility": "NORMAL",
                        "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
                    },
                }
            }
        if resource_type == "link_type" and resource_id == "owned_by":
            return {
                "data": {
                    "id": "owned_by",
                    "label": {"en": "Owned By"},
                    "spec": {
                        "from": "Account",
                        "to": "User",
                        "cardinality": "n:1",
                        "status": "ACTIVE",
                        "relationship_spec": {"fk_column": "owner_id"},
                    },
                }
            }
        return {"data": {"id": resource_id, "spec": {"from": "Order", "to": "Product"}}}

    async def get_ontology(self, db_name, class_id, *, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, class_id, branch
        return {
            "data": {
                "properties": [
                    {
                        "name": "account_id",
                        "label": {"en": "Account ID"},
                        "type": "xsd:string",
                        "required": True,
                    }
                ]
            }
        }


async def _noop_require_domain_role(request, *, db_name):  # noqa: ANN001, ANN003
    _ = request, db_name
    return None


@pytest.mark.asyncio
async def test_list_object_types_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_object_types_v2(
            ontology="test_db",
            request=request,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "Account"
    assert response["data"][0]["primaryKey"] == "account_id"
    assert response["data"][0]["properties"]["account_id"]["status"] == "ACTIVE"
    assert "nextPageToken" in response


@pytest.mark.asyncio
async def test_list_ontologies_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    response = await router_v2.list_ontologies_v2(
        request=request,
        oms_client=_FakeOMSClient(),
    )

    assert isinstance(response, dict)
    assert [row["apiName"] for row in response["data"]] == ["test_db", "sales"]
    assert response["data"][0]["displayName"] == "test_db"
    assert "nextPageToken" not in response


@pytest.mark.asyncio
async def test_get_ontology_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_ontology_v2(
            ontology="test_db",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "test_db"
    assert response["displayName"] == "test_db"
    assert response["description"] == "test_db ontology"


@pytest.mark.asyncio
async def test_get_ontology_v2_resolves_rid_identifier():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_ontology_v2(
            ontology="ri.ontology.test_db",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "test_db"


@pytest.mark.asyncio
async def test_get_object_type_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_type_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "Account"
    assert response["titleProperty"] == "account_id"
    assert response["visibility"] == "NORMAL"


@pytest.mark.asyncio
async def test_list_outgoing_link_types_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_outgoing_link_types_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert [row["apiName"] for row in response["data"]] == ["owned_by"]
    assert response["data"][0]["objectTypeApiName"] == "User"
    assert response["data"][0]["cardinality"] == "ONE"


@pytest.mark.asyncio
async def test_get_outgoing_link_type_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_outgoing_link_type_v2(
            ontology="test_db",
            objectType="Account",
            linkType="owned_by",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "owned_by"
    assert response["displayName"] == "Owned By"
    assert response["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_search_objects_v2_passthrough_foundry_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.search_objects_v2(
            ontology="test_db",
            objectType="Account",
            payload={"where": {"type": "eq", "field": "status", "value": "ACTIVE"}, "pageSize": 10},
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["totalCount"] == "1"
    assert response["data"][0]["instance_id"] == "i-1"


@pytest.mark.asyncio
async def test_v2_invalid_ontology_returns_foundry_error_envelope():
    request = Request({"type": "http", "headers": []})
    response = await router_v2.list_object_types_v2(
        ontology="INVALID NAME",
        request=request,
        page_size=10,
        page_token=None,
        branch="main",
        oms_client=_FakeOMSClient(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "InvalidArgument"
    assert isinstance(payload["errorInstanceId"], str)
    assert payload["parameters"]["ontology"] == "INVALID NAME"
