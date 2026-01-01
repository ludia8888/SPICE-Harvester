from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest
from fastapi import HTTPException, status

from bff.routers.pipeline import get_pipeline

PIPELINE_ID = "00000000-0000-0000-0000-000000000003"


@dataclass
class _Request:
    headers: dict[str, str]


@dataclass
class _Pipeline:
    pipeline_id: str
    db_name: str
    name: str = "test-pipeline"
    pipeline_type: str = "batch"
    branch: str = "main"


class _Version:
    version_id = "v1"
    lakefs_commit_id = "c1"
    definition_json: dict[str, Any] = {"nodes": [], "edges": []}


class _PipelineRegistryDenied:
    async def has_any_permissions(self, *, pipeline_id: str) -> bool:
        return True

    async def has_permission(self, *, pipeline_id: str, principal_type: str, principal_id: str, required_role: str) -> bool:
        return False


class _PipelineRegistryBootstrap:
    def __init__(self) -> None:
        self.granted = False
        self.pipeline = _Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb")

    async def has_any_permissions(self, *, pipeline_id: str) -> bool:
        return False

    async def grant_permission(
        self,
        *,
        pipeline_id: str,
        principal_type: str,
        principal_id: str,
        role: str,
    ) -> None:
        self.granted = True

    async def get_pipeline(self, *, pipeline_id: str) -> Optional[_Pipeline]:
        if pipeline_id == self.pipeline.pipeline_id:
            return self.pipeline
        return None

    async def get_latest_version(self, *, pipeline_id: str, branch: Optional[str] = None) -> _Version:
        return _Version()

    async def list_dependencies(self, *, pipeline_id: str) -> list[dict[str, str]]:
        return []


@pytest.mark.asyncio
async def test_get_pipeline_requires_read_permission() -> None:
    registry = _PipelineRegistryDenied()
    with pytest.raises(HTTPException) as exc_info:
        await get_pipeline(
            pipeline_id=PIPELINE_ID,
            pipeline_registry=registry,
            branch=None,
            preview_node_id=None,
            request=_Request(headers={"X-Principal-Id": "viewer"}),
        )

    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
    assert exc_info.value.detail == "Permission denied"


@pytest.mark.asyncio
async def test_get_pipeline_bootstraps_permissions_when_missing() -> None:
    registry = _PipelineRegistryBootstrap()
    response = await get_pipeline(
        pipeline_id=PIPELINE_ID,
        pipeline_registry=registry,
        branch=None,
        preview_node_id=None,
        request=_Request(headers={"X-Principal-Id": "owner"}),
    )

    assert response["status"] == "success"
    assert registry.granted is True
