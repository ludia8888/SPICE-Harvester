import pytest
from fastapi import FastAPI

from bff.routers import foundry_ontology_v2


def _param_names(schema: dict, *, path: str, method: str) -> list[str]:
    path_item = schema.get("paths", {}).get(path, {}).get(method, {})
    return [param.get("name") for param in path_item.get("parameters", [])]


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
