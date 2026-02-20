from __future__ import annotations

from fastapi import FastAPI

from bff.routers import ontology_extensions


def test_ontology_resource_crud_routes_are_exposed() -> None:
    app = FastAPI()
    app.include_router(ontology_extensions.router, prefix="/api/v1")
    paths = (app.openapi() or {}).get("paths", {})

    assert "get" in (paths.get("/api/v1/databases/{db_name}/ontology/resources") or {})
    assert "post" in (paths.get("/api/v1/databases/{db_name}/ontology/records/deployments") or {})
    assert "get" in (paths.get("/api/v1/databases/{db_name}/ontology/resources/{resource_type}") or {})
    assert "post" in (paths.get("/api/v1/databases/{db_name}/ontology/resources/{resource_type}") or {})
    assert "get" in (
        paths.get("/api/v1/databases/{db_name}/ontology/resources/{resource_type}/{resource_id}") or {}
    )
    assert "put" in (
        paths.get("/api/v1/databases/{db_name}/ontology/resources/{resource_type}/{resource_id}") or {}
    )
    assert "delete" in (
        paths.get("/api/v1/databases/{db_name}/ontology/resources/{resource_type}/{resource_id}") or {}
    )
