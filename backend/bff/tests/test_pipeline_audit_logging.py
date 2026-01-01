from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest

from bff.routers import pipeline as pipeline_router
from bff.routers.pipeline import update_pipeline

PIPELINE_ID = "00000000-0000-0000-0000-000000000001"


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
    definition_json: dict[str, Any] = {"nodes": [{"id": "a"}], "edges": []}


class _PipelineRegistry:
    def __init__(self, pipeline: _Pipeline) -> None:
        self._pipeline = pipeline

    async def has_any_permissions(self, *, pipeline_id: str) -> bool:
        return True

    async def has_permission(self, *, pipeline_id: str, principal_type: str, principal_id: str, required_role: str) -> bool:
        return True

    async def get_pipeline(self, *, pipeline_id: str) -> Optional[_Pipeline]:
        if pipeline_id == self._pipeline.pipeline_id:
            return self._pipeline
        return None

    async def get_latest_version(self, *, pipeline_id: str, branch: Optional[str] = None) -> _Version:
        return _Version()

    async def get_pipeline_branch(self, *, db_name: str, branch: str) -> dict[str, Any]:
        return {"db_name": db_name, "branch": branch, "archived": False}

    async def update_pipeline(self, **kwargs: Any) -> _Pipeline:
        return self._pipeline

    async def list_dependencies(self, *, pipeline_id: str) -> list[dict[str, str]]:
        return []


class _EventStore:
    async def connect(self) -> None:
        return None

    async def append_event(self, event: Any) -> None:
        return None


class _AuditStore:
    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []

    async def log(self, **kwargs: Any) -> None:
        self.entries.append(dict(kwargs))


@pytest.mark.asyncio
async def test_pipeline_update_writes_audit_log(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_router, "event_store", _EventStore())

    audit_store = _AuditStore()
    registry = _PipelineRegistry(_Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb"))
    response = await update_pipeline(
        pipeline_id=PIPELINE_ID,
        payload={"status": "draft"},
        pipeline_registry=registry,
        audit_store=audit_store,
        request=_Request(headers={"X-Principal-Id": "auditor"}),
    )

    assert response["status"] == "success"

    assert any(entry.get("action") == "PIPELINE_UPDATED" for entry in audit_store.entries)
