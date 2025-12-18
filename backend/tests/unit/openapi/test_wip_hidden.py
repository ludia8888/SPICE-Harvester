import pytest
from fastapi import FastAPI

from bff.routers import graph


@pytest.mark.unit
def test_wip_projection_endpoints_hidden_from_openapi():
    app = FastAPI()
    app.include_router(graph.router)

    schema = app.openapi()
    paths = schema.get("paths", {})

    assert "/api/v1/projections/{db_name}/register" not in paths
    assert "/api/v1/projections/{db_name}/query" not in paths
    assert "/api/v1/projections/{db_name}/list" not in paths
