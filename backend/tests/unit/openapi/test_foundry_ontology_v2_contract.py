import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock

from bff.dependencies import BFFDependencyProvider
from bff.routers import foundry_ontology_v2


def _param_names(schema: dict, *, path: str, method: str) -> list[str]:
    path_item = schema.get("paths", {}).get(path, {}).get(method, {})
    return [param.get("name") for param in path_item.get("parameters", [])]


def _build_router_test_app(*, oms_client: object) -> FastAPI:
    app = FastAPI()
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
    params = _param_names(schema, path="/api/v2/ontologies/{ontology}/objectTypes", method="get")

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
        path="/api/v2/ontologies/{ontology}/objectTypes/{objectType}",
        method="get",
    )
    outgoing_list_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes",
        method="get",
    )
    outgoing_get_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}",
        method="get",
    )
    search_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontology}/objects/{objectType}/search",
        method="post",
    )
    list_objects_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontology}/objects/{objectType}",
        method="get",
    )
    get_object_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}",
        method="get",
    )
    list_linked_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}",
        method="get",
    )
    get_linked_params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}/{linkedObjectPrimaryKey}",
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
def test_foundry_v2_list_objects_includes_foundry_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontology}/objects/{objectType}",
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
def test_foundry_v2_list_linked_objects_includes_foundry_query_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")
    schema = app.openapi()

    params = _param_names(
        schema,
        path="/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}",
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
def test_foundry_v2_strict_compat_env_gate(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "false")
    monkeypatch.setenv("FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST", "sales_db, core")
    assert foundry_ontology_v2._is_foundry_v2_strict_compat_enabled(db_name="sales_db") is True
    assert foundry_ontology_v2._is_foundry_v2_strict_compat_enabled(db_name="Sales_DB") is True
    assert foundry_ontology_v2._is_foundry_v2_strict_compat_enabled(db_name="other_db") is False

    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "true")
    assert foundry_ontology_v2._is_foundry_v2_strict_compat_enabled(db_name="other_db") is True


@pytest.mark.unit
def test_foundry_v2_full_metadata_branch_contract():
    assert foundry_ontology_v2._full_metadata_branch_contract(branch="main", strict_compat=False) == {"name": "main"}
    assert foundry_ontology_v2._full_metadata_branch_contract(branch="main", strict_compat=True) == {"rid": "main"}


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
    assert normalized["rid"] == "ri.spice.main.object-type.commerce.Order"
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
    assert unresolved["linkTypeRid"] == "ri.spice.main.link-type.commerce.Order.orderedBy"

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
async def test_foundry_v2_route_full_metadata_strict_off_keeps_legacy_branch_and_payload(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "false")
    monkeypatch.setenv("FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST", "")

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
        response = await client.get("/api/v2/ontologies/sales_db/fullMetadata?branch=main")

    assert response.status_code == 200
    body = response.json()
    assert body["branch"] == {"name": "main"}
    assert body["objectTypes"]["Order"]["objectType"] == {"apiName": "Order"}
    assert body["objectTypes"]["Order"]["linkTypes"] == [{"apiName": "orderedBy"}]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_full_metadata_strict_on_applies_branch_and_required_fields(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "false")
    monkeypatch.setenv("FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST", "sales_db")

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
        response = await client.get("/api/v2/ontologies/sales_db/fullMetadata?branch=main")

    assert response.status_code == 200
    body = response.json()
    assert body["branch"] == {"rid": "main"}
    object_contract = body["objectTypes"]["Order"]["objectType"]
    assert object_contract["apiName"] == "Order"
    assert object_contract["displayName"] == "Order"
    assert object_contract["pluralDisplayName"] == "Order"
    assert object_contract["icon"] == {"type": "blueprint", "name": "table", "color": "#4C6A9A"}
    assert object_contract["rid"] == "ri.spice.main.object-type.sales_db.Order"
    assert object_contract["primaryKey"] == "id"
    assert object_contract["titleProperty"] == "id"
    assert object_contract["properties"]["id"]["dataType"] == {"type": "string"}
    assert object_contract["properties"]["id"]["rid"] == "ri.spice.main.property.sales_db.Order.id"
    assert body["objectTypes"]["Order"]["linkTypes"] == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_list_object_types_strict_on_applies_required_fields(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "false")
    monkeypatch.setenv("FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST", "sales_db")

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
    assert object_contract["rid"] == "ri.spice.main.object-type.sales_db.Order"
    assert object_contract["primaryKey"] == "id"
    assert object_contract["titleProperty"] == "id"
    assert object_contract["properties"]["id"]["dataType"] == {"type": "string"}
    assert object_contract["properties"]["id"]["rid"] == "ri.spice.main.property.sales_db.Order.id"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_get_object_type_strict_off_keeps_legacy_payload(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "false")
    monkeypatch.setenv("FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST", "")

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
    assert response.json() == {"apiName": "Order"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_get_object_type_strict_on_applies_required_fields(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "true")
    monkeypatch.setenv("FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST", "")

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
    assert object_contract["rid"] == "ri.spice.main.object-type.sales_db.Order"
    assert object_contract["primaryKey"] == "id"
    assert object_contract["titleProperty"] == "id"
    assert object_contract["properties"]["id"]["dataType"] == {"type": "string"}
    assert object_contract["properties"]["id"]["rid"] == "ri.spice.main.property.sales_db.Order.id"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_get_outgoing_link_type_strict_on_unresolved_returns_not_found(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "true")
    monkeypatch.setenv("FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST", "")

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
async def test_foundry_v2_route_get_outgoing_link_type_strict_off_unresolved_returns_payload(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "false")
    monkeypatch.setenv("FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST", "")

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

    assert response.status_code == 200
    assert response.json() == {"apiName": "orderedBy"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_v2_route_list_outgoing_link_types_strict_on_drops_unresolved(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ENABLE_FOUNDRY_V2_STRICT_COMPAT", "true")
    monkeypatch.setenv("FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST", "")

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
            "linkTypeRid": "ri.spice.main.link-type.sales_db.Order.contains",
        }
    ]
