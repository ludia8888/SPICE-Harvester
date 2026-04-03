from __future__ import annotations

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
import pytest

from bff.routers import pipeline_datasets_catalog
from bff.routers.pipeline_deps import get_pipeline_registry
from shared.services.storage.lakefs_client import LakeFSError


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(pipeline_datasets_catalog.router, prefix="/api/v1/pipelines")
    return app


@pytest.mark.unit
@pytest.mark.asyncio
async def test_list_pipeline_branches_returns_503_when_lakefs_is_unavailable() -> None:
    class _LakeFSClient:
        async def list_branches(self, *, repository: str):  # noqa: ANN001
            _ = repository
            raise LakeFSError("lakefs unavailable")

    class _PipelineRegistry:
        async def get_lakefs_client(self, *, user_id=None):  # noqa: ANN001
            _ = user_id
            return _LakeFSClient()

    app = _build_app()
    app.dependency_overrides[get_pipeline_registry] = lambda: _PipelineRegistry()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/pipelines/branches", params={"db_name": "demo"})

    assert resp.status_code == 503
    assert resp.json()["detail"]["code"] == "UPSTREAM_UNAVAILABLE"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_list_pipeline_branches_does_not_fallback_to_main_on_internal_error() -> None:
    class _PipelineRegistry:
        async def get_lakefs_client(self, *, user_id=None):  # noqa: ANN001
            _ = user_id
            raise RuntimeError("broken dependency")

    app = _build_app()
    app.dependency_overrides[get_pipeline_registry] = lambda: _PipelineRegistry()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/pipelines/branches", params={"db_name": "demo"})

    assert resp.status_code == 500
    assert resp.json()["detail"]["code"] == "INTERNAL_ERROR"
