import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock

from bff.dependencies import BFFDependencyProvider
from bff.routers import foundry_ontology_v2
from shared.foundry.errors import FoundryAPIError, foundry_exception_handler
from shared.security.user_context import UserPrincipal


def _param_names(schema: dict, *, path: str, method: str) -> list[str]:
    path_item = schema.get("paths", {}).get(path, {}).get(method, {})
    return [param.get("name") for param in path_item.get("parameters", [])]


def _build_router_test_app(*, oms_client: object) -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(FoundryAPIError, foundry_exception_handler)

    @app.middleware("http")
    async def _inject_test_principal(request: Request, call_next):  # noqa: ANN001
        request.state.user = UserPrincipal(
            id="test-user",
            claims={"scope": "api:ontologies-read api:ontologies-write"},
        )
        return await call_next(request)

    app.include_router(foundry_ontology_v2.router, prefix="/api")

    async def _fake_oms_client():
        return oms_client

    app.dependency_overrides[BFFDependencyProvider.get_oms_client] = _fake_oms_client
    return app


@pytest.mark.unit
def test_foundry_v2_ontology_list_has_no_pagination_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")

    schema = app.openapi()
    params = _param_names(schema, path="/api/v2/ontologies", method="get")

    assert "pageSize" not in params
    assert "pageToken" not in params


@pytest.mark.unit
def test_foundry_v2_object_type_list_keeps_pagination_and_branch_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")

    schema = app.openapi()
    params = _param_names(schema, path="/api/v2/ontologies/{ontologyRid}/objectTypes", method="get")

    assert "pageSize" in params
    assert "pageToken" in params
    assert "branch" in params


@pytest.mark.unit
def test_foundry_v2_ontology_read_paths_include_branch_when_supported():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    object_type_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objectTypes/{objectTypeApiName}",
        method="get",
    )
    outgoing_list_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objectTypes/{objectTypeApiName}/outgoingLinkTypes",
        method="get",
    )
    outgoing_get_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objectTypes/{objectTypeApiName}/outgoingLinkTypes/{linkTypeApiName}",
        method="get",
    )
    search_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}/search",
        method="post",
    )
    list_objects_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}",
        method="get",
    )
    get_object_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}",
        method="get",
    )
    list_linked_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/links/{linkTypeApiName}",
        method="get",
    )
    get_linked_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/links/{linkTypeApiName}/{linkedObjectPrimaryKey}",
        method="get",
    )

    assert "branch" in object_type_params
    assert "branch" in outgoing_list_params
    assert "branch" in outgoing_get_params
    assert "sdkPackageRid" not in object_type_params
    assert "sdkVersion" not in object_type_params
    assert "sdkPackageRid" not in outgoing_list_params
    assert "sdkVersion" not in outgoing_list_params
    assert "sdkPackageRid" not in outgoing_get_params
    assert "sdkVersion" not in outgoing_get_params
    assert "branch" in search_params
    assert "sdkPackageRid" in search_params
    assert "sdkVersion" in search_params
    assert "branch" in list_objects_params
    assert "sdkPackageRid" in list_objects_params
    assert "sdkVersion" in list_objects_params
    assert "branch" in get_object_params
    assert "sdkPackageRid" in get_object_params
    assert "sdkVersion" in get_object_params
    assert "branch" in list_linked_params
    assert "sdkPackageRid" in list_linked_params
    assert "sdkVersion" in list_linked_params
    assert "branch" in get_linked_params
    assert "sdkPackageRid" in get_linked_params
    assert "sdkVersion" in get_linked_params


@pytest.mark.unit
def test_foundry_v2_load_object_set_objects_keeps_foundry_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/loadObjects",
        method="post",
    )

    assert "branch" in params
    assert "transactionId" in params
    assert "sdkPackageRid" in params
    assert "sdkVersion" in params
    assert "pageSize" not in params
    assert "pageToken" not in params
    assert "$select" not in params
    assert "preview" not in params


@pytest.mark.unit
def test_foundry_v2_object_set_preview_endpoints_keep_preview_query_param():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    load_links_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/loadLinks",
        method="post",
    )
    load_multiple_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/loadObjectsMultipleObjectTypes",
        method="post",
    )
    load_or_interfaces_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/loadObjectsOrInterfaces",
        method="post",
    )

    assert {"branch", "preview", "sdkPackageRid", "sdkVersion"} <= set(load_links_params)
    assert "transactionId" not in load_links_params
    assert {"branch", "preview", "transactionId", "sdkPackageRid", "sdkVersion"} <= set(load_multiple_params)
    assert {"branch", "preview", "sdkPackageRid", "sdkVersion"} <= set(load_or_interfaces_params)
    assert "transactionId" not in load_or_interfaces_params


@pytest.mark.unit
def test_foundry_v2_object_set_aggregate_and_temporary_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    aggregate_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/aggregate",
        method="post",
    )
    create_temporary_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/createTemporary",
        method="post",
    )

    assert {"branch", "transactionId", "sdkPackageRid", "sdkVersion"} <= set(aggregate_params)
    assert {"sdkPackageRid", "sdkVersion"} <= set(create_temporary_params)


@pytest.mark.unit
def test_foundry_v2_execute_query_keeps_foundry_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/queries/{queryApiName}/execute",
        method="post",
    )

    assert {"version", "sdkPackageRid", "sdkVersion", "transactionId"} <= set(params)
    assert "branch" not in params
    assert "preview" not in params


@pytest.mark.unit
def test_foundry_v2_list_objects_includes_foundry_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}",
        method="get",
    )

    assert "pageSize" in params
    assert "pageToken" in params
    assert "select" in params
    assert "orderBy" in params
    assert "excludeRid" in params
    assert "snapshot" in params
    assert "sdkPackageRid" in params
    assert "sdkVersion" in params


@pytest.mark.unit
def test_foundry_v2_count_objects_includes_foundry_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}/count",
        method="post",
    )

    assert "branch" in params
    assert "sdkPackageRid" in params
    assert "sdkVersion" in params
    assert "pageSize" not in params
    assert "pageToken" not in params
    assert "select" not in params


@pytest.mark.unit
def test_foundry_v2_list_linked_objects_includes_foundry_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/links/{linkTypeApiName}",
        method="get",
    )

    assert "pageSize" in params
    assert "pageToken" in params
    assert "select" in params
    assert "orderBy" in params
    assert "excludeRid" in params
    assert "snapshot" in params
    assert "sdkPackageRid" in params
    assert "sdkVersion" in params


@pytest.mark.unit
def test_foundry_v2_aggregate_objects_keeps_foundry_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}/aggregate",
        method="post",
    )

    assert {"branch", "transactionId", "sdkPackageRid", "sdkVersion"} <= set(params)


@pytest.mark.unit
def test_foundry_v2_timeseries_and_attachment_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    timeseries_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/timeseries/{property}/firstPoint",
        method="get",
    )
    attachment_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/attachments/{property}",
        method="get",
    )

    assert {"branch", "sdkPackageRid", "sdkVersion"} <= set(timeseries_params)
    assert {"branch", "sdkPackageRid", "sdkVersion"} <= set(attachment_params)


@pytest.mark.unit
def test_foundry_v2_attachment_upload_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    params = _param_names(
        schema,
        path="/api/v2/ontologies/attachments/upload",
        method="post",
    )

    assert {"filename", "sdkPackageRid", "sdkVersion"} <= set(params)


@pytest.mark.unit
def test_foundry_v2_strict_compat_env_gate(monkeypatch: pytest.MonkeyPatch):
    assert foundry_ontology_v2._is_foundry_v2_strict_compat_enabled(db_name="any_db") is True

    assert foundry_ontology_v2._is_foundry_v2_strict_compat_enabled(db_name="sales_db") is True
    assert foundry_ontology_v2._is_foundry_v2_strict_compat_enabled(db_name="other_db") is True


@pytest.mark.unit
def test_foundry_v2_full_metadata_branch_contract():
    assert foundry_ontology_v2._full_metadata_branch_contract(branch="main") == {"rid": "main"}


@pytest.mark.unit
def test_foundry_v2_strict_object_type_normalization_adds_required_fields():
    payload = {"apiName": "Order", "status": "ACTIVE"}
    normalized, fixes = foundry_ontology_v2._strictify_foundry_object_type(
        payload,
        db_name="commerce",
        object_type_hint="Order",
    )

    assert fixes > 0
    assert normalized["apiName"] == "Order"
    assert normalized["displayName"] == "Order"
    assert normalized["pluralDisplayName"] == "Order"
    assert normalized["icon"] == {"type": "blueprint", "name": "table", "color": "#4C6A9A"}
    assert normalized["rid"] == "ri.foundry.main.object-type.commerce.Order"
    assert normalized["primaryKey"] in normalized["properties"]
    assert normalized["titleProperty"] in normalized["properties"]
    for prop in normalized["properties"].values():
        assert isinstance(prop.get("dataType"), dict)
        assert str(prop["dataType"].get("type") or "").strip()
        assert str(prop.get("rid") or "").strip()


@pytest.mark.unit
def test_foundry_v2_strict_link_type_normalization_and_resolution():
    unresolved_payload = {"apiName": "orderedBy"}
    unresolved, unresolved_fixes, is_resolved = foundry_ontology_v2._strictify_outgoing_link_type(
        unresolved_payload,
        db_name="commerce",
        source_object_type="Order",
    )
    assert unresolved_fixes > 0
    assert is_resolved is False
    assert unresolved["displayName"] == "orderedBy"
    assert unresolved["cardinality"] == "MANY"
    assert unresolved["linkTypeRid"] == "ri.foundry.main.link-type.commerce.Order.orderedBy"

    resolved_payload = {"apiName": "orderedBy", "objectTypeApiName": "User"}
    resolved, _, is_resolved = foundry_ontology_v2._strictify_outgoing_link_type(
        resolved_payload,
        db_name="commerce",
        source_object_type="Order",
    )
    assert is_resolved is True
    assert resolved["objectTypeApiName"] == "User"


@pytest.mark.unit
def test_foundry_v2_strict_full_metadata_drops_unresolved_links():
    payload = {
        "objectType": {"apiName": "Order"},
        "linkTypes": [
            {"apiName": "orderedBy"},
            {"apiName": "contains", "objectTypeApiName": "Product"},
        ],
    }
    normalized, fixes, dropped = foundry_ontology_v2._strictify_object_type_full_metadata(
        payload,
        db_name="commerce",
        object_type_hint="Order",
    )
    assert fixes > 0
    assert dropped == 1
    assert len(normalized["linkTypes"]) == 1
    assert normalized["linkTypes"][0]["apiName"] == "contains"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_full_metadata_applies_required_fields(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    async def _fake_list_resources_best_effort(*, db_name: str, branch: str, resource_type: str, oms_client):  # noqa: ANN001
        _ = db_name, branch, oms_client
        if resource_type == "object_type":
            return [{"id": "Order"}]
        return []

    async def _fake_get_ontology_payload_best_effort(*, db_name: str, branch: str, object_type: str, oms_client):  # noqa: ANN001
        _ = db_name, branch, object_type, oms_client
        return None

    def _fake_to_foundry_object_type_full_metadata(resource, *, ontology_payload, link_types):  # noqa: ANN001
        _ = resource, ontology_payload, link_types
        return {
            "objectType": {"apiName": "Order"},
            "linkTypes": [{"apiName": "orderedBy"}],
        }

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)
    monkeypatch.setattr(foundry_ontology_v2, "_list_resources_best_effort", _fake_list_resources_best_effort)
    monkeypatch.setattr(foundry_ontology_v2, "_get_ontology_payload_best_effort", _fake_get_ontology_payload_best_effort)
    monkeypatch.setattr(foundry_ontology_v2, "_to_foundry_object_type_full_metadata", _fake_to_foundry_object_type_full_metadata)

    oms_client = AsyncMock()
    oms_client.get_database = AsyncMock(return_value={"data": {"name": "sales_db"}})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v2/ontologies/sales_db/fullMetadata?branch=main&preview=true")

    assert response.status_code == 200
    body = response.json()
    assert body["branch"] == {"rid": "main"}
    object_contract = body["objectTypes"]["Order"]["objectType"]
    assert object_contract["apiName"] == "Order"
    assert object_contract["displayName"] == "Order"
    assert object_contract["pluralDisplayName"] == "Order"
    assert object_contract["icon"] == {"type": "blueprint", "name": "table", "color": "#4C6A9A"}
    assert object_contract["rid"] == "ri.foundry.main.object-type.sales_db.Order"
    assert object_contract["primaryKey"] == "id"
    assert object_contract["titleProperty"] == "id"
    assert object_contract["properties"]["id"]["dataType"] == {"type": "string"}
    assert object_contract["properties"]["id"]["rid"] == "ri.foundry.main.property.sales_db.Order.id"
    assert body["objectTypes"]["Order"]["linkTypes"] == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_load_object_set_objects_routes_to_object_search(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    oms_client.post = AsyncMock(return_value={"data": [{"id": "order-1"}], "nextPageToken": None})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/ontologies/sales_db/objectSets/loadObjects?branch=main&transactionId=txn_1",
            json={
                "objectSet": {"objectType": "Order"},
                "select": ["id", "status"],
                "orderBy": {"orderType": "fields", "fields": [{"field": "id", "direction": "asc"}]},
                "pageSize": 25,
            },
        )

    assert response.status_code == 200
    assert response.json()["data"][0]["id"] == "order-1"
    oms_client.post.assert_awaited_once()
    called_path = oms_client.post.await_args.args[0]
    called_params = oms_client.post.await_args.kwargs["params"]
    called_payload = oms_client.post.await_args.kwargs["json"]
    assert called_path == "/api/v2/ontologies/sales_db/objects/Order/search"
    assert called_params == {"branch": "main"}
    assert called_payload["pageSize"] == 25
    assert called_payload["select"] == ["id", "status"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_load_object_set_objects_requires_object_set(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/ontologies/sales_db/objectSets/loadObjects?branch=main",
            json={},
        )

    assert response.status_code == 400
    body = response.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "InvalidArgument"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_count_objects_routes_to_oms_count(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    oms_client.post = AsyncMock(return_value={"count": 123})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/ontologies/sales_db/objects/Order/count?branch=main&sdkPackageRid=ri.pkg.1&sdkVersion=2.0.0",
        )

    assert response.status_code == 200
    assert response.json() == {"count": 123}
    oms_client.post.assert_awaited_once()
    called_path = oms_client.post.await_args.args[0]
    called_params = oms_client.post.await_args.kwargs["params"]
    assert called_path == "/api/v2/ontologies/sales_db/objects/Order/count"
    assert called_params == {"branch": "main", "sdkPackageRid": "ri.pkg.1", "sdkVersion": "2.0.0"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_load_object_set_multiple_object_types_requires_preview(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/ontologies/sales_db/objectSets/loadObjectsMultipleObjectTypes?branch=main",
            json={
                "objectSet": {"objectType": "Order"},
                "select": ["id"],
            },
        )

    assert response.status_code == 400
    body = response.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "ApiFeaturePreviewUsageOnly"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_load_object_set_links_requires_preview(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/ontologies/sales_db/objectSets/loadLinks?branch=main",
            json={
                "objectSet": {"objectType": "Order"},
                "links": ["owned_by"],
            },
        )

    assert response.status_code == 400
    body = response.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "ApiFeaturePreviewUsageOnly"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_load_object_set_links_returns_locator_payload(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    oms_client.post = AsyncMock(
        return_value={
            "data": [
                {
                    "__apiName": "Order",
                    "__primaryKey": "Order/order-1",
                    "owner_id": "User/user-1",
                    "owned_by": ["User/user-1", "user-2"],
                    "links": {"owned_by": ["User/user-2", "user-3"]},
                }
            ],
            "nextPageToken": "next_source_page",
            "totalCount": "1",
        }
    )
    oms_client.get_ontology_resource = AsyncMock(
        return_value={
            "data": {
                "id": "owned_by",
                "spec": {
                    "from": "Order",
                    "to": "User",
                    "cardinality": "n:1",
                    "relationship_spec": {"fk_column": "owner_id"},
                },
            }
        }
    )
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/ontologies/sales_db/objectSets/loadLinks?branch=main&preview=true",
            json={
                "objectSet": {"objectType": "Order"},
                "links": ["owned_by"],
                "includeComputeUsage": True,
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["nextPageToken"] == "next_source_page"
    assert body["computeUsage"] == 0
    assert len(body["data"]) == 1
    assert body["data"][0]["sourceObject"] == {"__apiName": "Order", "__primaryKey": "order-1"}
    linked_objects = body["data"][0]["linkedObjects"]
    assert [item["targetObject"]["__primaryKey"] for item in linked_objects] == ["user-1", "user-2", "user-3"]
    assert all(item["targetObject"]["__apiName"] == "User" for item in linked_objects)
    assert all(item["linkType"] == "owned_by" for item in linked_objects)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_aggregate_object_set_returns_metrics(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    objectSets/aggregate now delegates to OMS aggregate_objects_v2.
    Mock the aggregate method to return a pre-computed Foundry response.
    """

    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    oms_client.aggregate_objects_v2 = AsyncMock(
        return_value={
            "accuracy": "ACCURATE",
            "data": [
                {
                    "group": {"status": "ACTIVE"},
                    "metrics": [
                        {"name": "rowCount", "value": 2},
                        {"name": "amountSum", "value": 30.0},
                    ],
                },
                {
                    "group": {"status": "INACTIVE"},
                    "metrics": [
                        {"name": "rowCount", "value": 1},
                        {"name": "amountSum", "value": 5.0},
                    ],
                },
            ],
            "excludedItems": 0,
            "computeUsage": 0,
        }
    )
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/ontologies/sales_db/objectSets/aggregate?branch=main",
            json={
                "objectSet": {"objectType": "Order"},
                "aggregation": [
                    {"type": "count", "name": "rowCount"},
                    {"type": "sum", "field": "amount", "name": "amountSum"},
                ],
                "groupBy": [{"type": "exact", "field": "status"}],
                "includeComputeUsage": True,
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["accuracy"] == "ACCURATE"
    assert body["excludedItems"] == 0
    assert "computeUsage" in body
    groups = {item["group"]["status"]: item for item in body["data"]}
    assert groups["ACTIVE"]["metrics"][0]["name"] == "rowCount"
    assert groups["ACTIVE"]["metrics"][0]["value"] == 2
    assert groups["ACTIVE"]["metrics"][1]["name"] == "amountSum"
    assert groups["ACTIVE"]["metrics"][1]["value"] == 30.0
    oms_client.aggregate_objects_v2.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_aggregate_object_set_delegates_to_oms(
    monkeypatch: pytest.MonkeyPatch,
):
    """Verify objectSets/aggregate delegates to OMS (not multi-page Python loop)."""

    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    oms_client.aggregate_objects_v2 = AsyncMock(
        return_value={
            "accuracy": "ACCURATE",
            "data": [
                {
                    "group": {"status": "ACTIVE"},
                    "metrics": [
                        {"name": "rowCount", "value": 2},
                        {"name": "amountSum", "value": 30.0},
                    ],
                },
            ],
            "excludedItems": 0,
        }
    )
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/ontologies/sales_db/objectSets/aggregate?branch=main",
            json={
                "objectSet": {"objectType": "Order"},
                "aggregation": [
                    {"type": "count", "name": "rowCount"},
                    {"type": "sum", "field": "amount", "name": "amountSum"},
                ],
                "groupBy": [{"type": "exact", "field": "status"}],
            },
        )

    assert response.status_code == 200
    body = response.json()
    groups = {item["group"]["status"]: item for item in body["data"]}
    assert groups["ACTIVE"]["metrics"][0]["value"] == 2
    assert groups["ACTIVE"]["metrics"][1]["value"] == 30.0
    # Only ONE call to OMS aggregate (no multi-page search loop)
    oms_client.aggregate_objects_v2.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_aggregate_objects_route_returns_metrics(
    monkeypatch: pytest.MonkeyPatch,
):
    """per-objectType aggregate now delegates to OMS aggregate_objects_v2."""

    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    oms_client.aggregate_objects_v2 = AsyncMock(
        return_value={
            "accuracy": "ACCURATE",
            "data": [
                {
                    "group": {"status": "ACTIVE"},
                    "metrics": [{"name": "amountSum", "value": 30.0}],
                },
            ],
            "excludedItems": 0,
        }
    )
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/ontologies/sales_db/objects/Order/aggregate?branch=main",
            json={
                "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
                "aggregation": [{"type": "sum", "field": "amount", "name": "amountSum"}],
                "groupBy": [{"type": "exact", "field": "status"}],
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["data"][0]["metrics"][0]["name"] == "amountSum"
    assert body["data"][0]["metrics"][0]["value"] == 30.0
    oms_client.aggregate_objects_v2.assert_awaited_once()
    # Verify the correct path was called: db_name, object_type, payload
    call_args = oms_client.aggregate_objects_v2.await_args
    assert call_args.args[0] == "sales_db"
    assert call_args.args[1] == "Order"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_create_temporary_and_get_object_set_roundtrip(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(
            "/api/v2/ontologies/sales_db/objectSets/createTemporary",
            json={"objectSet": {"objectType": "Order", "where": {"type": "eq", "field": "status", "value": "ACTIVE"}}},
        )
        assert created.status_code == 200
        object_set_rid = created.json()["objectSetRid"]

        loaded = await client.get(f"/api/v2/ontologies/sales_db/objectSets/{object_set_rid}")

    assert loaded.status_code == 200
    body = loaded.json()
    assert body["objectType"] == "Order"
    assert body["where"]["field"] == "status"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_full_metadata_keeps_branch_rid_contract(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    async def _fake_list_resources_best_effort(*, db_name: str, branch: str, resource_type: str, oms_client):  # noqa: ANN001
        _ = db_name, branch, oms_client
        if resource_type == "object_type":
            return [{"id": "Order"}]
        return []

    async def _fake_get_ontology_payload_best_effort(*, db_name: str, branch: str, object_type: str, oms_client):  # noqa: ANN001
        _ = db_name, branch, object_type, oms_client
        return None

    def _fake_to_foundry_object_type_full_metadata(resource, *, ontology_payload, link_types):  # noqa: ANN001
        _ = resource, ontology_payload, link_types
        return {
            "objectType": {"apiName": "Order"},
            "linkTypes": [{"apiName": "orderedBy"}],
        }

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)
    monkeypatch.setattr(foundry_ontology_v2, "_list_resources_best_effort", _fake_list_resources_best_effort)
    monkeypatch.setattr(foundry_ontology_v2, "_get_ontology_payload_best_effort", _fake_get_ontology_payload_best_effort)
    monkeypatch.setattr(foundry_ontology_v2, "_to_foundry_object_type_full_metadata", _fake_to_foundry_object_type_full_metadata)

    oms_client = AsyncMock()
    oms_client.get_database = AsyncMock(return_value={"data": {"name": "sales_db"}})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v2/ontologies/sales_db/fullMetadata?branch=main&preview=true")

    assert response.status_code == 200
    body = response.json()
    assert body["branch"] == {"rid": "main"}
    object_contract = body["objectTypes"]["Order"]["objectType"]
    assert object_contract["apiName"] == "Order"
    assert object_contract["displayName"] == "Order"
    assert object_contract["pluralDisplayName"] == "Order"
    assert object_contract["icon"] == {"type": "blueprint", "name": "table", "color": "#4C6A9A"}
    assert object_contract["rid"] == "ri.foundry.main.object-type.sales_db.Order"
    assert object_contract["primaryKey"] == "id"
    assert object_contract["titleProperty"] == "id"
    assert object_contract["properties"]["id"]["dataType"] == {"type": "string"}
    assert object_contract["properties"]["id"]["rid"] == "ri.foundry.main.property.sales_db.Order.id"
    assert body["objectTypes"]["Order"]["linkTypes"] == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_full_metadata_requires_preview_flag(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    async def _fake_list_resources_best_effort(*, db_name: str, branch: str, resource_type: str, oms_client):  # noqa: ANN001
        _ = db_name, branch, resource_type, oms_client
        return []

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)
    monkeypatch.setattr(foundry_ontology_v2, "_list_resources_best_effort", _fake_list_resources_best_effort)

    oms_client = AsyncMock()
    oms_client.get_database = AsyncMock(return_value={"data": {"name": "sales_db"}})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v2/ontologies/sales_db/fullMetadata?branch=main")

    assert response.status_code == 400
    body = response.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "ApiFeaturePreviewUsageOnly"


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/api/v2/ontologies/sales_db/interfaceTypes?branch=main",
        "/api/v2/ontologies/sales_db/interfaceTypes/BaseInterface?branch=main",
        "/api/v2/ontologies/sales_db/sharedPropertyTypes?branch=main",
        "/api/v2/ontologies/sales_db/sharedPropertyTypes/sharedName?branch=main",
        "/api/v2/ontologies/sales_db/valueTypes",
        "/api/v2/ontologies/sales_db/valueTypes/string",
        "/api/v2/ontologies/sales_db/objectTypes/Order/fullMetadata?branch=main",
    ],
)
async def test_foundry_v2_preview_routes_require_preview_flag(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)

    oms_client = AsyncMock()
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(path)

    assert response.status_code == 400
    body = response.json()
    assert body["errorCode"] == "INVALID_ARGUMENT"
    assert body["errorName"] == "ApiFeaturePreviewUsageOnly"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_list_object_types_strict_on_applies_required_fields(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    def _fake_to_foundry_object_type(resource, *, ontology_payload):  # noqa: ANN001
        _ = resource, ontology_payload
        return {"apiName": "Order"}

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)
    monkeypatch.setattr(foundry_ontology_v2, "_to_foundry_object_type", _fake_to_foundry_object_type)

    oms_client = AsyncMock()
    oms_client.list_ontology_resources = AsyncMock(return_value={"data": {"resources": [{"id": "Order"}]}})
    oms_client.get_ontology = AsyncMock(return_value={})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v2/ontologies/sales_db/objectTypes?branch=main&pageSize=100")

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body.get("data"), list)
    assert len(body["data"]) == 1
    object_contract = body["data"][0]
    assert object_contract["apiName"] == "Order"
    assert object_contract["displayName"] == "Order"
    assert object_contract["pluralDisplayName"] == "Order"
    assert object_contract["icon"] == {"type": "blueprint", "name": "table", "color": "#4C6A9A"}
    assert object_contract["rid"] == "ri.foundry.main.object-type.sales_db.Order"
    assert object_contract["primaryKey"] == "id"
    assert object_contract["titleProperty"] == "id"
    assert object_contract["properties"]["id"]["dataType"] == {"type": "string"}
    assert object_contract["properties"]["id"]["rid"] == "ri.foundry.main.property.sales_db.Order.id"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_get_object_type_applies_required_fields(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    def _fake_to_foundry_object_type(resource, *, ontology_payload):  # noqa: ANN001
        _ = resource, ontology_payload
        return {"apiName": "Order"}

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)
    monkeypatch.setattr(foundry_ontology_v2, "_to_foundry_object_type", _fake_to_foundry_object_type)

    oms_client = AsyncMock()
    oms_client.get_ontology_resource = AsyncMock(return_value={"data": {"id": "Order"}})
    oms_client.get_ontology = AsyncMock(return_value={})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v2/ontologies/sales_db/objectTypes/Order?branch=main")

    assert response.status_code == 200
    object_contract = response.json()
    assert object_contract["apiName"] == "Order"
    assert object_contract["displayName"] == "Order"
    assert object_contract["pluralDisplayName"] == "Order"
    assert object_contract["icon"] == {"type": "blueprint", "name": "table", "color": "#4C6A9A"}
    assert object_contract["rid"] == "ri.foundry.main.object-type.sales_db.Order"
    assert object_contract["primaryKey"] == "id"
    assert object_contract["titleProperty"] == "id"
    assert object_contract["properties"]["id"]["dataType"] == {"type": "string"}
    assert object_contract["properties"]["id"]["rid"] == "ri.foundry.main.property.sales_db.Order.id"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_get_object_type_strict_on_applies_required_fields(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    def _fake_to_foundry_object_type(resource, *, ontology_payload):  # noqa: ANN001
        _ = resource, ontology_payload
        return {"apiName": "Order"}

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)
    monkeypatch.setattr(foundry_ontology_v2, "_to_foundry_object_type", _fake_to_foundry_object_type)

    oms_client = AsyncMock()
    oms_client.get_ontology_resource = AsyncMock(return_value={"data": {"id": "Order"}})
    oms_client.get_ontology = AsyncMock(return_value={})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v2/ontologies/sales_db/objectTypes/Order?branch=main")

    assert response.status_code == 200
    object_contract = response.json()
    assert object_contract["apiName"] == "Order"
    assert object_contract["displayName"] == "Order"
    assert object_contract["pluralDisplayName"] == "Order"
    assert object_contract["icon"] == {"type": "blueprint", "name": "table", "color": "#4C6A9A"}
    assert object_contract["rid"] == "ri.foundry.main.object-type.sales_db.Order"
    assert object_contract["primaryKey"] == "id"
    assert object_contract["titleProperty"] == "id"
    assert object_contract["properties"]["id"]["dataType"] == {"type": "string"}
    assert object_contract["properties"]["id"]["rid"] == "ri.foundry.main.property.sales_db.Order.id"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_get_outgoing_link_type_strict_on_unresolved_returns_not_found(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    def _fake_to_foundry_outgoing_link_type(resource, *, source_object_type: str):  # noqa: ANN001
        _ = resource, source_object_type
        return {"apiName": "orderedBy"}

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)
    monkeypatch.setattr(foundry_ontology_v2, "_to_foundry_outgoing_link_type", _fake_to_foundry_outgoing_link_type)

    oms_client = AsyncMock()
    oms_client.get_ontology_resource = AsyncMock(return_value={"data": {"id": "orderedBy"}})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v2/ontologies/sales_db/objectTypes/Order/outgoingLinkTypes/orderedBy?branch=main"
        )

    assert response.status_code == 404
    body = response.json()
    assert body["errorCode"] == "NOT_FOUND"
    assert body["errorName"] == "LinkTypeNotFound"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_get_outgoing_link_type_unresolved_returns_not_found(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    def _fake_to_foundry_outgoing_link_type(resource, *, source_object_type: str):  # noqa: ANN001
        _ = resource, source_object_type
        return {"apiName": "orderedBy"}

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)
    monkeypatch.setattr(foundry_ontology_v2, "_to_foundry_outgoing_link_type", _fake_to_foundry_outgoing_link_type)

    oms_client = AsyncMock()
    oms_client.get_ontology_resource = AsyncMock(return_value={"data": {"id": "orderedBy"}})
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v2/ontologies/sales_db/objectTypes/Order/outgoingLinkTypes/orderedBy?branch=main"
        )

    assert response.status_code == 404
    body = response.json()
    assert body["errorCode"] == "NOT_FOUND"
    assert body["errorName"] == "LinkTypeNotFound"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_list_outgoing_link_types_strict_on_drops_unresolved(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _fake_resolve_ontology_db_name(*, ontology: str, oms_client):  # noqa: ANN001
        _ = ontology, oms_client
        return "sales_db"

    async def _fake_require_domain_role(request, *, db_name: str):  # noqa: ANN001
        _ = request, db_name
        return None

    def _fake_to_foundry_outgoing_link_type(resource, *, source_object_type: str):  # noqa: ANN001
        _ = source_object_type
        resource_id = str(resource.get("id") or "")
        if resource_id == "contains":
            return {"apiName": "contains", "objectTypeApiName": "Product"}
        return {"apiName": resource_id}

    monkeypatch.setattr(foundry_ontology_v2, "_resolve_ontology_db_name", _fake_resolve_ontology_db_name)
    monkeypatch.setattr(foundry_ontology_v2, "_require_domain_role", _fake_require_domain_role)
    monkeypatch.setattr(foundry_ontology_v2, "_to_foundry_outgoing_link_type", _fake_to_foundry_outgoing_link_type)

    oms_client = AsyncMock()
    oms_client.list_ontology_resources = AsyncMock(
        side_effect=[
            {"data": {"resources": [{"id": "orderedBy"}, {"id": "contains"}]}},
            {"data": {"resources": []}},
        ]
    )
    app = _build_router_test_app(oms_client=oms_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v2/ontologies/sales_db/objectTypes/Order/outgoingLinkTypes?branch=main&pageSize=100"
        )

    assert response.status_code == 200
    body = response.json()
    assert body["data"] == [
        {
            "apiName": "contains",
            "objectTypeApiName": "Product",
            "displayName": "contains",
            "status": "ACTIVE",
            "cardinality": "MANY",
            "linkTypeRid": "ri.foundry.main.link-type.sales_db.Order.contains",
        }
    ]
