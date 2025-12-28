from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest
from fastapi import HTTPException, status

from bff.routers.pipeline import deploy_pipeline


@dataclass
class _Request:
    headers: dict[str, str]


@dataclass
class _Pipeline:
    pipeline_id: str
    db_name: str
    pipeline_type: str = "batch"
    branch: str = "main"


class _PipelineRegistry:
    def __init__(self, *, pipeline: _Pipeline, build_run: dict[str, Any]) -> None:
        self._pipeline = pipeline
        self._build_run = build_run

    async def get_pipeline(self, *, pipeline_id: str) -> Optional[_Pipeline]:
        if pipeline_id == self._pipeline.pipeline_id:
            return self._pipeline
        return None

    async def get_run(self, *, pipeline_id: str, job_id: str) -> Optional[dict[str, Any]]:
        if pipeline_id == self._pipeline.pipeline_id and job_id == self._build_run.get("job_id"):
            return self._build_run
        return None

    async def has_any_permissions(self, *, pipeline_id: str) -> bool:
        return True

    async def has_permission(self, *, pipeline_id: str, principal_type: str, principal_id: str, required_role: str) -> bool:
        return True

    async def get_latest_version(self, *, pipeline_id: str, branch: Optional[str] = None) -> Any:
        class _Version:
            version = "v1"
            definition_json: dict[str, Any] = {"nodes": [], "edges": []}

        return _Version()


class _PipelineJobQueue:
    async def publish(self, job: Any) -> None:  # pragma: no cover - not expected in gate tests
        return None


class _DatasetRegistry:
    async def get_dataset_by_name(self, *, db_name: str, name: str, branch: str) -> Any:  # pragma: no cover
        return None


class _AuditStore:
    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []

    async def log(self, **kwargs: Any) -> None:
        self.entries.append(dict(kwargs))


class _OMSClient:
    def __init__(self, *, head_commit_id: str) -> None:
        self._head_commit_id = head_commit_id

    async def get_version_head(self, db_name: str, *, branch: str = "main") -> dict[str, Any]:
        return {"status": "success", "data": {"branch": branch, "head_commit_id": self._head_commit_id}}


class _FailingOMSClient:
    async def get_version_head(self, db_name: str, *, branch: str = "main") -> dict[str, Any]:
        raise RuntimeError("OMS down")


@pytest.mark.asyncio
async def test_promote_build_rejects_missing_ontology_commit() -> None:
    pipeline = _Pipeline(pipeline_id="p", db_name="testdb", branch="main")
    build_run = {
        "job_id": "build-job",
        "mode": "build",
        "status": "SUCCESS",
        "output_json": {"branch": "main"},
    }
    registry = _PipelineRegistry(pipeline=pipeline, build_run=build_run)

    with pytest.raises(HTTPException) as exc_info:
        await deploy_pipeline(
            pipeline_id="p",
            payload={
                "promote_build": True,
                "build_job_id": "build-job",
                "node_id": "node-1",
                "definition_json": {"nodes": [], "edges": []},
            },
            request=_Request(headers={}),
            pipeline_registry=registry,
            dataset_registry=_DatasetRegistry(),
            lineage_store=None,
            oms_client=_OMSClient(head_commit_id="c-main"),
            audit_store=_AuditStore(),
        )

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail["code"] == "ONTOLOGY_VERSION_UNKNOWN"


@pytest.mark.asyncio
async def test_promote_build_rejects_ontology_commit_mismatch() -> None:
    pipeline = _Pipeline(pipeline_id="p", db_name="testdb", branch="main")
    build_run = {
        "job_id": "build-job",
        "mode": "build",
        "status": "SUCCESS",
        "output_json": {"branch": "main", "ontology": {"branch": "main", "commit": "c-build"}},
    }
    registry = _PipelineRegistry(pipeline=pipeline, build_run=build_run)

    with pytest.raises(HTTPException) as exc_info:
        await deploy_pipeline(
            pipeline_id="p",
            payload={
                "promote_build": True,
                "build_job_id": "build-job",
                "node_id": "node-1",
                "definition_json": {"nodes": [], "edges": []},
            },
            request=_Request(headers={}),
            pipeline_registry=registry,
            dataset_registry=_DatasetRegistry(),
            lineage_store=None,
            oms_client=_OMSClient(head_commit_id="c-main"),
            audit_store=_AuditStore(),
        )

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail["code"] == "ONTOLOGY_VERSION_MISMATCH"
    assert exc_info.value.detail["bundle_terminus_commit_id"] == "c-build"
    assert exc_info.value.detail["prod_head_commit_id"] == "c-main"
    assert exc_info.value.detail["target_branch"] == "main"


@pytest.mark.asyncio
async def test_promote_build_returns_503_when_ontology_gate_unavailable() -> None:
    pipeline = _Pipeline(pipeline_id="p", db_name="testdb", branch="main")
    build_run = {
        "job_id": "build-job",
        "mode": "build",
        "status": "SUCCESS",
        "output_json": {"branch": "main", "ontology": {"branch": "main", "commit": "c-main"}},
    }
    registry = _PipelineRegistry(pipeline=pipeline, build_run=build_run)

    with pytest.raises(HTTPException) as exc_info:
        await deploy_pipeline(
            pipeline_id="p",
            payload={
                "promote_build": True,
                "build_job_id": "build-job",
                "node_id": "node-1",
                "definition_json": {"nodes": [], "edges": []},
            },
            request=_Request(headers={}),
            pipeline_registry=registry,
            dataset_registry=_DatasetRegistry(),
            lineage_store=None,
            oms_client=_FailingOMSClient(),
            audit_store=_AuditStore(),
        )

    assert exc_info.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert exc_info.value.detail["code"] == "ONTOLOGY_GATE_UNAVAILABLE"
