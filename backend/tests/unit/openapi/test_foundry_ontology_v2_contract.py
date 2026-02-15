import pytest
from fastapi import FastAPI

from bff.routers import foundry_ontology_v2


@pytest.mark.unit
def test_foundry_v2_ontology_list_has_no_pagination_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")

    schema = app.openapi()
    path_item = schema.get("paths", {}).get("/api/v2/ontologies", {}).get("get", {})
    params = [param.get("name") for param in path_item.get("parameters", [])]

    assert "pageSize" not in params
    assert "pageToken" not in params


@pytest.mark.unit
def test_foundry_v2_object_type_list_keeps_pagination_params():
    app = FastAPI()
    app.include_router(foundry_ontology_v2.router, prefix="/api")

    schema = app.openapi()
    path_item = schema.get("paths", {}).get("/api/v2/ontologies/{ontology}/objectTypes", {}).get("get", {})
    params = [param.get("name") for param in path_item.get("parameters", [])]

    assert "pageSize" in params
    assert "pageToken" in params

