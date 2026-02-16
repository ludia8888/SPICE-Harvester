from __future__ import annotations

import json

import pytest
from fastapi.responses import JSONResponse
from starlette.requests import Request

from bff.routers import foundry_ontology_v2 as router_v2
from shared.utils.foundry_page_token import encode_offset_page_token


class _FakeOMSClient:
    def __init__(self) -> None:
        self.last_post: tuple[str, dict] | None = None

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
        self.last_post = (path, kwargs)
        if path.endswith("/search"):
            payload = kwargs.get("json") if isinstance(kwargs, dict) else None
            where = payload.get("where") if isinstance(payload, dict) else None
            if isinstance(where, dict) and where.get("type") == "eq" and where.get("field") == "account_id":
                value = str(where.get("value") or "")
                if value == "acc-404":
                    return {"data": [], "nextPageToken": None, "totalCount": "0"}
                return {
                    "data": [
                        {
                            "instance_id": value or "acc-1",
                            "account_id": value or "acc-1",
                            "owner_id": "u-1",
                            "owned_by": ["u-1"],
                            "name": "Alice Account",
                        }
                    ],
                    "nextPageToken": None,
                    "totalCount": "1",
                }
            if isinstance(where, dict) and where.get("type") == "eq" and where.get("field") == "user_id":
                value = str(where.get("value") or "")
                if value in {"u-404", "u-missing"}:
                    return {"data": [], "nextPageToken": None, "totalCount": "0"}
                return {
                    "data": [{"instance_id": value or "u-1", "user_id": value or "u-1", "name": "Owner User"}],
                    "nextPageToken": None,
                    "totalCount": "1",
                }
            if isinstance(where, dict) and where.get("type") == "or":
                rows = []
                for clause in where.get("value") or []:
                    if not isinstance(clause, dict):
                        continue
                    if clause.get("type") != "eq" or clause.get("field") != "user_id":
                        continue
                    value = str(clause.get("value") or "").strip()
                    if not value or value in {"u-404", "u-missing"}:
                        continue
                    rows.append({"instance_id": value, "user_id": value, "name": "Owner User"})
                return {"data": rows, "nextPageToken": None, "totalCount": str(len(rows))}
            return {
                "data": [{"instance_id": "i-1", "account_id": "acc-1", "name": "Alice"}],
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
        if resource_type == "object_type" and resource_id == "Account":
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
        if resource_type == "object_type" and resource_id == "User":
            return {
                "data": {
                    "id": resource_id,
                    "label": {"en": "User"},
                    "spec": {
                        "status": "ACTIVE",
                        "visibility": "NORMAL",
                        "pk_spec": {"primary_key": ["user_id"], "title_key": ["user_id"]},
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
        if class_id == "User":
            return {
                "data": {
                    "properties": [
                        {
                            "name": "user_id",
                            "label": {"en": "User ID"},
                            "type": "xsd:string",
                            "required": True,
                        }
                    ]
                }
            }
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


class _ApiNameOnlyOMSClient(_FakeOMSClient):
    async def list_databases(self):  # noqa: ANN201
        return {"data": {"databases": [{"apiName": "finance", "displayName": "Finance"}]}}


class _MissingObjectTypeOMSClient(_FakeOMSClient):
    async def get_ontology_resource(  # noqa: ANN001, ANN003
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):
        if resource_type == "object_type" and resource_id == "MissingType":
            _ = db_name, branch
            return None
        return await super().get_ontology_resource(
            db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )


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
async def test_list_ontologies_v2_accepts_apiname_source_rows():
    request = Request({"type": "http", "headers": []})
    response = await router_v2.list_ontologies_v2(
        request=request,
        oms_client=_ApiNameOnlyOMSClient(),
    )

    assert isinstance(response, dict)
    assert response["data"] == [{"apiName": "finance", "displayName": "Finance"}]


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
async def test_get_ontology_v2_unknown_returns_ontology_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_ontology_v2(
            ontology="unknown_ontology",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "OntologyNotFound"


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
async def test_get_object_type_v2_missing_returns_object_type_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_type_v2(
            ontology="test_db",
            objectType="MissingType",
            request=request,
            branch="main",
            oms_client=_MissingObjectTypeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "ObjectTypeNotFound"


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
async def test_get_outgoing_link_type_v2_missing_returns_link_type_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_outgoing_link_type_v2(
            ontology="test_db",
            objectType="Account",
            linkType="unknown_link",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "LinkTypeNotFound"


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
async def test_list_objects_v2_passthrough_foundry_shape_and_orderby_parse():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _FakeOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_objects_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            page_size=10,
            page_token="MTA",
            select=["account_id", "name"],
            order_by="properties.account_id:desc,p.name:asc",
            exclude_rid=True,
            snapshot=False,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["totalCount"] == "1"
    assert response["data"][0]["account_id"] == "acc-1"
    assert fake.last_post is not None
    _, kwargs = fake.last_post
    assert kwargs["params"]["branch"] == "main"
    assert kwargs["json"]["pageSize"] == 10
    assert kwargs["json"]["pageToken"] == "MTA"
    assert kwargs["json"]["select"] == ["account_id", "name"]
    assert kwargs["json"]["excludeRid"] is True
    assert kwargs["json"]["snapshot"] is False
    assert kwargs["json"]["orderBy"] == {
        "orderType": "fields",
        "fields": [
            {"field": "account_id", "direction": "desc"},
            {"field": "name", "direction": "asc"},
        ],
    }


@pytest.mark.asyncio
async def test_get_object_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            request=request,
            select=["account_id", "name"],
            exclude_rid=True,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["account_id"] == "acc-1"
    assert response["name"] == "Alice Account"


@pytest.mark.asyncio
async def test_get_object_v2_not_found_returns_foundry_error():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-404",
            request=request,
            select=None,
            exclude_rid=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "ObjectNotFound"
    assert payload["parameters"]["primaryKey"] == "acc-404"


@pytest.mark.asyncio
async def test_list_linked_objects_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _FakeOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_linked_objects_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            linkType="owned_by",
            request=request,
            page_size=10,
            page_token=None,
            select=["user_id", "name"],
            order_by="properties.user_id:asc",
            exclude_rid=True,
            snapshot=False,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["user_id"] == "u-1"
    assert response.get("nextPageToken") is None
    assert fake.last_post is not None
    _, kwargs = fake.last_post
    assert kwargs["json"]["where"] == {"type": "eq", "field": "user_id", "value": "u-1"}


@pytest.mark.asyncio
async def test_list_linked_objects_v2_missing_link_type_returns_link_type_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_linked_objects_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            linkType="unknown_link",
            request=request,
            page_size=10,
            page_token=None,
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "LinkTypeNotFound"


@pytest.mark.asyncio
async def test_get_linked_object_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_linked_object_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            linkType="owned_by",
            linkedObjectPrimaryKey="u-1",
            request=request,
            select=["user_id", "name"],
            exclude_rid=True,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["user_id"] == "u-1"
    assert response["name"] == "Owner User"


@pytest.mark.asyncio
async def test_get_linked_object_v2_not_found_returns_foundry_error():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _FakeOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_linked_object_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            linkType="owned_by",
            linkedObjectPrimaryKey="u-404",
            request=request,
            select=None,
            exclude_rid=None,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "LinkedObjectNotFound"
    assert payload["parameters"]["linkedObjectPrimaryKey"] == "u-404"
    assert fake.last_post is not None
    assert fake.last_post[0].endswith("/objects/test_db/Account/search")


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


@pytest.mark.asyncio
async def test_list_objects_v2_rejects_invalid_orderby_expression():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_objects_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            page_size=10,
            page_token=None,
            select=None,
            order_by="account_id:desc",
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_list_objects_v2_rejects_invalid_branch():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _FakeOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_objects_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            page_size=10,
            page_token=None,
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="invalid$branch",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert fake.last_post is None


@pytest.mark.asyncio
async def test_list_object_types_v2_rejects_expired_page_token():
    request = Request({"type": "http", "headers": []})
    issued_at = 0
    expired_token = encode_offset_page_token(10, issued_at=issued_at)

    response = await router_v2.list_object_types_v2(
        ontology="test_db",
        request=request,
        page_size=10,
        page_token=expired_token,
        branch="main",
        oms_client=_FakeOMSClient(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_list_object_types_v2_rejects_page_token_scope_mismatch():
    request = Request({"type": "http", "headers": []})
    mismatched_scope_token = encode_offset_page_token(10, scope="v2/objectTypes|other_db|main")

    response = await router_v2.list_object_types_v2(
        ontology="test_db",
        request=request,
        page_size=10,
        page_token=mismatched_scope_token,
        branch="main",
        oms_client=_FakeOMSClient(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
