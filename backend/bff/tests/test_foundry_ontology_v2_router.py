from __future__ import annotations

import json

import httpx
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
                            "metadata": {
                                "interfaces": ["interface:Auditable"],
                                "implements_interfaces2": {
                                    "Auditable": {"propertyMapping": {"created_at": "created_at"}}
                                },
                                "shared_property_type_mapping": {"TenantScope": "tenant_scope"},
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
        if resource_type == "action_type":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "ApproveAccount",
                            "rid": "ri.action-type.approve-account",
                            "label": {"en": "Approve Account"},
                            "spec": {
                                "status": "ACTIVE",
                                "target_object_type": "Account",
                            },
                        }
                    ]
                }
            }
        if resource_type == "function":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "findAccounts",
                            "label": {"en": "Find Accounts"},
                            "spec": {
                                "status": "ACTIVE",
                                "version": "1.0.0",
                                "parameters": {"status": {"type": "string"}},
                                "output": {"type": "array", "items": {"type": "object"}},
                            },
                        }
                    ]
                }
            }
        if resource_type == "interface":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "Auditable",
                            "label": {"en": "Auditable"},
                            "spec": {"status": "ACTIVE"},
                        }
                    ]
                }
            }
        if resource_type == "shared_property":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "TenantScope",
                            "label": {"en": "Tenant Scope"},
                            "spec": {"status": "ACTIVE"},
                        }
                    ]
                }
            }
        if resource_type == "value_type":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "Money",
                            "label": {"en": "Money"},
                            "spec": {"status": "ACTIVE"},
                        }
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
                    "metadata": {
                        "interfaces": ["interface:Auditable"],
                        "implements_interfaces2": {
                            "Auditable": {"propertyMapping": {"created_at": "created_at"}}
                        },
                        "shared_property_type_mapping": {"TenantScope": "tenant_scope"},
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
        if resource_type == "action_type" and resource_id == "ApproveAccount":
            return {
                "data": {
                    "id": "ApproveAccount",
                    "rid": "ri.action-type.approve-account",
                    "label": {"en": "Approve Account"},
                    "spec": {"status": "ACTIVE", "target_object_type": "Account"},
                }
            }
        if resource_type == "function" and resource_id == "findAccounts":
            return {
                "data": {
                    "id": "findAccounts",
                    "label": {"en": "Find Accounts"},
                    "spec": {
                        "status": "ACTIVE",
                        "version": "1.0.0",
                        "parameters": {"status": {"type": "string"}},
                        "output": {"type": "array", "items": {"type": "object"}},
                    },
                }
            }
        if resource_type == "interface" and resource_id == "Auditable":
            return {
                "data": {
                    "id": "Auditable",
                    "label": {"en": "Auditable"},
                    "spec": {"status": "ACTIVE"},
                }
            }
        if resource_type == "shared_property" and resource_id == "TenantScope":
            return {
                "data": {
                    "id": "TenantScope",
                    "label": {"en": "Tenant Scope"},
                    "spec": {"status": "ACTIVE"},
                }
            }
        if resource_type == "value_type" and resource_id == "Money":
            return {
                "data": {
                    "id": "Money",
                    "label": {"en": "Money"},
                    "spec": {"status": "ACTIVE"},
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
                    },
                    {
                        "name": "tenant_scope",
                        "label": {"en": "Tenant Scope"},
                        "type": "xsd:string",
                        "required": False,
                        "shared_property_ref": "TenantScope",
                    },
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


class _MissingSharedPropertyOMSClient(_FakeOMSClient):
    async def get_ontology_resource(  # noqa: ANN001, ANN003
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):
        if resource_type == "shared_property" and resource_id == "MissingShared":
            _ = db_name, branch
            return None
        return await super().get_ontology_resource(
            db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )


class _ListDatabasesStatusErrorOMSClient(_FakeOMSClient):
    async def list_databases(self):  # noqa: ANN201
        request = httpx.Request("GET", "http://oms/api/v1/database/list")
        response = httpx.Response(
            status_code=503,
            request=request,
            json={"errorCode": "UPSTREAM_UNAVAILABLE"},
        )
        raise httpx.HTTPStatusError("Service unavailable", request=request, response=response)


class _FullMetadataPartialFailureOMSClient(_FakeOMSClient):
    async def list_ontology_resources(
        self,
        db_name,
        *,
        resource_type,
        branch="main",
        limit=200,
        offset=0,
    ):  # noqa: ANN001, ANN003
        if resource_type == "action_type":
            _ = db_name, branch, limit, offset
            raise httpx.ConnectError("action type registry unavailable")
        return await super().list_ontology_resources(
            db_name,
            resource_type=resource_type,
            branch=branch,
            limit=limit,
            offset=offset,
        )


class _PagedLinkTypesOMSClient(_FakeOMSClient):
    def __init__(self) -> None:
        super().__init__()
        self._resources: list[dict] = []
        for index in range(510):
            if index in {498, 501, 504}:
                self._resources.append(
                    {
                        "id": f"rel_{index}",
                        "spec": {
                            "from": "Account",
                            "to": "User",
                            "cardinality": "n:1",
                            "relationship_spec": {"fk_column": "owner_id"},
                        },
                    }
                )
            else:
                self._resources.append(
                    {
                        "id": f"other_{index}",
                        "spec": {
                            "from": "Order",
                            "to": "Product",
                            "cardinality": "1:n",
                        },
                    }
                )

    async def list_ontology_resources(
        self,
        db_name,
        *,
        resource_type,
        branch="main",
        limit=200,
        offset=0,
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch
        if resource_type == "link_type":
            sliced = self._resources[offset : offset + limit]
            return {"data": {"resources": sliced}}
        return await super().list_ontology_resources(
            db_name,
            resource_type=resource_type,
            branch=branch,
            limit=limit,
            offset=offset,
        )


class _LinkedPaginationOMSClient(_FakeOMSClient):
    async def post(self, path, **kwargs):  # noqa: ANN001, ANN003
        self.last_post = (path, kwargs)
        payload = kwargs.get("json") if isinstance(kwargs, dict) else None
        where = payload.get("where") if isinstance(payload, dict) else None

        if path.endswith("/objects/test_db/Account/search"):
            if isinstance(where, dict) and where.get("type") == "eq" and where.get("field") == "account_id":
                return {
                    "data": [
                        {
                            "account_id": "acc-1",
                            "owner_id": "u-1",
                            "owned_by": ["u-1", "u-2", {"id": "u-3"}, "u-4"],
                            "links": {"owned_by": ["u-4", "u-5"]},
                        }
                    ],
                    "nextPageToken": None,
                    "totalCount": "1",
                }

        if path.endswith("/objects/test_db/User/search"):
            if isinstance(where, dict) and where.get("type") == "eq" and where.get("field") == "user_id":
                value = str(where.get("value") or "").strip()
                if not value:
                    return {"data": [], "nextPageToken": None, "totalCount": "0"}
                return {
                    "data": [{"instance_id": value, "user_id": value, "name": f"User {value}"}],
                    "nextPageToken": None,
                    "totalCount": "1",
                }
            if isinstance(where, dict) and where.get("type") == "or":
                rows: list[dict] = []
                for clause in where.get("value") or []:
                    if not isinstance(clause, dict):
                        continue
                    if clause.get("type") != "eq" or clause.get("field") != "user_id":
                        continue
                    value = str(clause.get("value") or "").strip()
                    if not value:
                        continue
                    rows.append({"instance_id": value, "user_id": value, "name": f"User {value}"})
                return {"data": rows, "nextPageToken": None, "totalCount": str(len(rows))}

        return await super().post(path, **kwargs)


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
async def test_get_ontology_v2_permission_denied_returns_foundry_403():
    request = Request({"type": "http", "headers": []})
    original_enforce = router_v2.enforce_database_role

    async def _deny_permission(*args, **kwargs):  # noqa: ANN002, ANN003
        _ = args, kwargs
        raise ValueError("Permission denied")

    router_v2.enforce_database_role = _deny_permission
    try:
        response = await router_v2.get_ontology_v2(
            ontology="test_db",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2.enforce_database_role = original_enforce

    assert isinstance(response, JSONResponse)
    assert response.status_code == 403
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "PERMISSION_DENIED"
    assert payload["errorName"] == "PermissionDenied"


@pytest.mark.asyncio
async def test_get_ontology_v2_non_permission_role_error_returns_invalid_argument():
    request = Request({"type": "http", "headers": []})
    original_enforce = router_v2.enforce_database_role

    async def _invalid_role(*args, **kwargs):  # noqa: ANN002, ANN003
        _ = args, kwargs
        raise ValueError("invalid role policy")

    router_v2.enforce_database_role = _invalid_role
    try:
        response = await router_v2.get_ontology_v2(
            ontology="test_db",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2.enforce_database_role = original_enforce

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "InvalidArgument"


@pytest.mark.asyncio
async def test_get_ontology_v2_preflight_upstream_error_returns_upstream_contract():
    request = Request({"type": "http", "headers": []})
    response = await router_v2.get_ontology_v2(
        ontology="test_db",
        request=request,
        oms_client=_ListDatabasesStatusErrorOMSClient(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 503
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "UPSTREAM_ERROR"
    assert payload["errorName"] == "UpstreamError"


@pytest.mark.asyncio
async def test_get_full_metadata_v2_returns_foundry_full_metadata_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_full_metadata_v2(
            ontology="test_db",
            request=request,
            preview=True,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["ontology"]["apiName"] == "test_db"
    assert response["branch"]["name"] == "main"

    assert "Account" in response["objectTypes"]
    account_full_metadata = response["objectTypes"]["Account"]
    assert account_full_metadata["objectType"]["apiName"] == "Account"
    assert account_full_metadata["linkTypes"][0]["apiName"] == "owned_by"
    assert account_full_metadata["implementsInterfaces"] == ["Auditable"]
    assert account_full_metadata["implementsInterfaces2"]["Auditable"] == {
        "propertyMapping": {"created_at": "created_at"}
    }
    assert account_full_metadata["sharedPropertyTypeMapping"]["TenantScope"] == "tenant_scope"

    assert "ApproveAccount" in response["actionTypes"]
    assert "findAccounts:1.0.0" in response["queryTypes"]
    assert response["queryTypes"]["findAccounts:1.0.0"]["version"] == "1.0.0"
    assert "Auditable" in response["interfaceTypes"]
    assert "TenantScope" in response["sharedPropertyTypes"]
    assert "Money" in response["valueTypes"]


@pytest.mark.asyncio
async def test_get_full_metadata_v2_allows_preview_false():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_full_metadata_v2(
            ontology="test_db",
            request=request,
            preview=False,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["ontology"]["apiName"] == "test_db"


@pytest.mark.asyncio
async def test_get_full_metadata_v2_omits_partial_entities_when_upstream_unavailable():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_full_metadata_v2(
            ontology="test_db",
            request=request,
            preview=True,
            branch="main",
            oms_client=_FullMetadataPartialFailureOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert "Account" in response["objectTypes"]
    assert response["actionTypes"] == {}
    assert "findAccounts:1.0.0" in response["queryTypes"]


@pytest.mark.asyncio
async def test_list_action_types_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_action_types_v2(
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
    assert response["data"][0]["apiName"] == "ApproveAccount"
    assert response["data"][0]["displayName"] == "Approve Account"
    assert "nextPageToken" in response


@pytest.mark.asyncio
async def test_get_action_type_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_action_type_v2(
            ontology="test_db",
            actionType="ApproveAccount",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "ApproveAccount"
    assert response["displayName"] == "Approve Account"


@pytest.mark.asyncio
async def test_get_action_type_by_rid_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_action_type_by_rid_v2(
            ontology="test_db",
            actionTypeRid="ri.action-type.approve-account",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "ApproveAccount"
    assert response["displayName"] == "Approve Account"


@pytest.mark.asyncio
async def test_apply_action_v2_forwards_to_oms_v2_apply_with_foundry_path():
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-user-id", b"alice"),
                (b"x-user-type", b"service"),
            ],
        }
    )
    fake_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.apply_action_v2(
            ontology="test_db",
            action="ApproveAccount",
            body=router_v2.ApplyActionRequestV2(
                parameters={"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
            ),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert response == {}
    assert fake_client.last_post is not None
    assert fake_client.last_post[0] == "/api/v2/ontologies/test_db/actions/ApproveAccount/apply"
    submit_payload = fake_client.last_post[1]["json"]
    assert submit_payload["parameters"]["ticket"]["instance_id"] == "t1"
    assert submit_payload["options"]["mode"] == "VALIDATE_AND_EXECUTE"
    assert submit_payload["metadata"]["user_id"] == "alice"
    assert submit_payload["metadata"]["user_type"] == "service"
    assert fake_client.last_post[1]["params"]["branch"] == "main"
    assert "preview" not in fake_client.last_post[1]["params"]
    assert "validate" not in fake_client.last_post[1]["params"]


@pytest.mark.asyncio
async def test_apply_action_v2_validate_only_maps_to_oms_v2_apply():
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-user-id", b"alice"),
                (b"x-user-type", b"user"),
            ],
        }
    )
    fake_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.apply_action_v2(
            ontology="test_db",
            action="ApproveAccount",
            body=router_v2.ApplyActionRequestV2(
                options=router_v2.ApplyActionRequestOptionsV2(mode="VALIDATE_ONLY"),
                parameters={"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
            ),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert response["validation"]["result"] == "VALID"
    assert fake_client.last_post is not None
    assert fake_client.last_post[0] == "/api/v2/ontologies/test_db/actions/ApproveAccount/apply"
    simulate_payload = fake_client.last_post[1]["json"]
    assert simulate_payload["options"]["mode"] == "VALIDATE_ONLY"
    assert simulate_payload["metadata"]["user_id"] == "alice"
    assert "validate" not in fake_client.last_post[1]["params"]


@pytest.mark.asyncio
async def test_apply_action_batch_v2_forwards_requests_to_submit_batch():
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-user-id", b"alice"),
                (b"x-user-type", b"service"),
            ],
        }
    )
    fake_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.apply_action_batch_v2(
            ontology="test_db",
            action="ApproveAccount",
            body=router_v2.BatchApplyActionRequestV2(
                requests=[
                    router_v2.BatchApplyActionRequestItemV2(parameters={"ticket": {"instance_id": "t1"}}),
                    router_v2.BatchApplyActionRequestItemV2(parameters={"ticket": {"instance_id": "t2"}}),
                ]
            ),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert response == {}
    assert fake_client.last_post is not None
    assert fake_client.last_post[0] == "/api/v2/ontologies/test_db/actions/ApproveAccount/applyBatch"
    submit_payload = fake_client.last_post[1]["json"]
    assert len(submit_payload["requests"]) == 2
    assert submit_payload["requests"][0]["parameters"]["ticket"]["instance_id"] == "t1"
    assert submit_payload["requests"][1]["parameters"]["ticket"]["instance_id"] == "t2"
    assert submit_payload["metadata"]["user_id"] == "alice"
    assert fake_client.last_post[1]["params"]["branch"] == "main"
    assert "preview" not in fake_client.last_post[1]["params"]


@pytest.mark.asyncio
async def test_list_query_types_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_query_types_v2(
            ontology="test_db",
            request=request,
            page_size=10,
            page_token=None,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "findAccounts"
    assert response["data"][0]["displayName"] == "Find Accounts"
    assert response["data"][0]["version"] == "1.0.0"


@pytest.mark.asyncio
async def test_get_query_type_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_query_type_v2(
            ontology="test_db",
            queryApiName="findAccounts",
            request=request,
            version="1.0.0",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "findAccounts"
    assert response["displayName"] == "Find Accounts"
    assert response["version"] == "1.0.0"


@pytest.mark.asyncio
async def test_get_query_type_v2_mismatched_version_returns_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_query_type_v2(
            ontology="test_db",
            queryApiName="findAccounts",
            request=request,
            version="9.9.9",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "QueryTypeNotFound"


@pytest.mark.asyncio
async def test_list_interface_types_v2_allows_preview_false():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_interface_types_v2(
            ontology="test_db",
            request=request,
            preview=False,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "Auditable"


@pytest.mark.asyncio
async def test_list_interface_types_v2_returns_foundry_raw_shape_with_preview():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_interface_types_v2(
            ontology="test_db",
            request=request,
            preview=True,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "Auditable"
    assert response["data"][0]["displayName"] == "Auditable"


@pytest.mark.asyncio
async def test_list_shared_property_types_v2_allows_preview_false():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_shared_property_types_v2(
            ontology="test_db",
            request=request,
            preview=False,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "TenantScope"


@pytest.mark.asyncio
async def test_list_shared_property_types_v2_returns_foundry_raw_shape_with_preview():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_shared_property_types_v2(
            ontology="test_db",
            request=request,
            preview=True,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "TenantScope"
    assert response["data"][0]["displayName"] == "Tenant Scope"
    assert "nextPageToken" in response


@pytest.mark.asyncio
async def test_get_shared_property_type_v2_returns_foundry_raw_shape_with_preview():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_shared_property_type_v2(
            ontology="test_db",
            sharedPropertyType="TenantScope",
            request=request,
            preview=True,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "TenantScope"
    assert response["displayName"] == "Tenant Scope"


@pytest.mark.asyncio
async def test_get_shared_property_type_v2_missing_returns_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_shared_property_type_v2(
            ontology="test_db",
            sharedPropertyType="MissingShared",
            request=request,
            preview=True,
            branch="main",
            oms_client=_MissingSharedPropertyOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "SharedPropertyTypeNotFound"


@pytest.mark.asyncio
async def test_list_value_types_v2_allows_preview_false():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_value_types_v2(
            ontology="test_db",
            request=request,
            preview=False,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "Money"


@pytest.mark.asyncio
async def test_get_value_type_v2_returns_foundry_raw_shape_with_preview():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_value_type_v2(
            ontology="test_db",
            valueType="Money",
            request=request,
            preview=True,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "Money"
    assert response["displayName"] == "Money"


@pytest.mark.asyncio
async def test_list_value_types_v2_returns_foundry_raw_shape_without_pagination_token():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_value_types_v2(
            ontology="test_db",
            request=request,
            preview=True,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "Money"
    assert "nextPageToken" not in response


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
async def test_get_object_type_full_metadata_v2_returns_foundry_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_type_full_metadata_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            branch="main",
            preview=False,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["objectType"]["apiName"] == "Account"
    assert response["linkTypes"][0]["apiName"] == "owned_by"
    assert response["implementsInterfaces"] == ["Auditable"]
    assert response["implementsInterfaces2"]["Auditable"]["propertyMapping"]["created_at"] == "created_at"
    assert response["sharedPropertyTypeMapping"]["TenantScope"] == "tenant_scope"


@pytest.mark.asyncio
async def test_get_object_type_full_metadata_v2_missing_returns_object_type_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_type_full_metadata_v2(
            ontology="test_db",
            objectType="MissingType",
            request=request,
            branch="main",
            preview=False,
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
async def test_list_outgoing_link_types_v2_filters_before_pagination():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    client = _PagedLinkTypesOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_outgoing_link_types_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            page_size=2,
            page_token=None,
            branch="main",
            oms_client=client,
        )
        second = await router_v2.list_outgoing_link_types_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            page_size=2,
            page_token=first.get("nextPageToken"),
            branch="main",
            oms_client=client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert [row["apiName"] for row in first["data"]] == ["rel_498", "rel_501"]
    assert first.get("nextPageToken")
    assert [row["apiName"] for row in second["data"]] == ["rel_504"]
    assert second.get("nextPageToken") is None


@pytest.mark.asyncio
async def test_list_outgoing_link_types_v2_rejects_page_token_when_page_size_changes():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    client = _PagedLinkTypesOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_outgoing_link_types_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            page_size=1,
            page_token=None,
            branch="main",
            oms_client=client,
        )
        second = await router_v2.list_outgoing_link_types_v2(
            ontology="test_db",
            objectType="Account",
            request=request,
            page_size=2,
            page_token=first.get("nextPageToken"),
            branch="main",
            oms_client=client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(second, JSONResponse)
    assert second.status_code == 400
    payload = json.loads(second.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


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
async def test_list_linked_objects_v2_paginates_after_dedup_link_filter():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _LinkedPaginationOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_linked_objects_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            linkType="owned_by",
            request=request,
            page_size=2,
            page_token=None,
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
        second = await router_v2.list_linked_objects_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            linkType="owned_by",
            request=request,
            page_size=2,
            page_token=first.get("nextPageToken"),
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
        third = await router_v2.list_linked_objects_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            linkType="owned_by",
            request=request,
            page_size=2,
            page_token=second.get("nextPageToken"),
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert [row["user_id"] for row in first["data"]] == ["u-1", "u-2"]
    assert [row["user_id"] for row in second["data"]] == ["u-3", "u-4"]
    assert [row["user_id"] for row in third["data"]] == ["u-5"]
    assert first.get("nextPageToken")
    assert second.get("nextPageToken")
    assert third.get("nextPageToken") is None


@pytest.mark.asyncio
async def test_list_linked_objects_v2_rejects_page_token_when_page_size_changes():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _LinkedPaginationOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_linked_objects_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            linkType="owned_by",
            request=request,
            page_size=2,
            page_token=None,
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
        second = await router_v2.list_linked_objects_v2(
            ontology="test_db",
            objectType="Account",
            primaryKey="acc-1",
            linkType="owned_by",
            request=request,
            page_size=3,
            page_token=first.get("nextPageToken"),
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(second, JSONResponse)
    assert second.status_code == 400
    payload = json.loads(second.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


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


@pytest.mark.asyncio
async def test_list_object_types_v2_rejects_page_token_when_page_size_changes():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_object_types_v2(
            ontology="test_db",
            request=request,
            page_size=1,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
        second = await router_v2.list_object_types_v2(
            ontology="test_db",
            request=request,
            page_size=2,
            page_token=first.get("nextPageToken"),
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(second, JSONResponse)
    assert second.status_code == 400
    payload = json.loads(second.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
