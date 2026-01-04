from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
from uuid import uuid4

import pytest
from fastapi import HTTPException, status

from bff.routers.pipeline import approve_pipeline_proposal, submit_pipeline_proposal


@dataclass
class _Request:
    headers: dict[str, str]


@dataclass
class _ProposalRecord:
    pipeline_id: str
    status: str
    title: Optional[str] = None
    description: Optional[str] = None
    review_comment: Optional[str] = None


@dataclass
class _PipelineRecord:
    pipeline_id: str
    db_name: str
    name: str
    description: Any
    pipeline_type: str
    location: str
    status: str
    branch: str
    proposal_status: Optional[str] = None
    proposal_title: Optional[str] = None
    proposal_description: Optional[str] = None
    proposal_review_comment: Optional[str] = None


class _FakeAuditStore:
    def __init__(self) -> None:
        self.logs: list[dict[str, Any]] = []

    async def log(self, **kwargs: Any) -> None:
        self.logs.append(dict(kwargs))


class _FakePipelineRegistry:
    def __init__(self) -> None:
        self._pipelines: dict[str, _PipelineRecord] = {}
        self._permissions: dict[str, list[tuple[str, str, str]]] = {}

    async def create_pipeline(
        self,
        *,
        db_name: str,
        name: str,
        description: Any,
        pipeline_type: str,
        location: str,
        status: str,
        branch: str,
        proposal_status: Optional[str] = None,
        proposal_title: Optional[str] = None,
    ) -> _PipelineRecord:
        pipeline_id = str(uuid4())
        pipeline = _PipelineRecord(
            pipeline_id=pipeline_id,
            db_name=db_name,
            name=name,
            description=description,
            pipeline_type=pipeline_type,
            location=location,
            status=status,
            branch=branch,
            proposal_status=proposal_status,
            proposal_title=proposal_title,
        )
        self._pipelines[pipeline_id] = pipeline
        return pipeline

    async def get_pipeline(self, *, pipeline_id: str) -> Optional[_PipelineRecord]:
        return self._pipelines.get(pipeline_id)

    async def grant_permission(
        self,
        *,
        pipeline_id: str,
        principal_type: str,
        principal_id: str,
        role: str,
    ) -> None:
        self._permissions.setdefault(pipeline_id, []).append((principal_type, principal_id, role))

    async def has_any_permissions(self, *, pipeline_id: str) -> bool:
        return bool(self._permissions.get(pipeline_id))

    async def has_permission(
        self,
        *,
        pipeline_id: str,
        principal_type: str,
        principal_id: str,
        required_role: str,
    ) -> bool:
        for entry in self._permissions.get(pipeline_id, []):
            entry_type, entry_id, entry_role = entry
            if entry_type == principal_type and entry_id == principal_id and entry_role == required_role:
                return True
        return False

    async def submit_proposal(
        self,
        *,
        pipeline_id: str,
        title: str,
        description: Optional[str],
        proposal_bundle: Optional[dict[str, Any]],
    ) -> _ProposalRecord:
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            raise RuntimeError("Pipeline not found")
        pipeline.proposal_status = "pending"
        pipeline.proposal_title = title
        pipeline.proposal_description = description
        return _ProposalRecord(
            pipeline_id=pipeline_id,
            status="pending",
            title=title,
            description=description,
        )

    async def review_proposal(
        self,
        *,
        pipeline_id: str,
        status: str,
        review_comment: Optional[str] = None,
    ) -> _ProposalRecord:
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            raise RuntimeError("Pipeline not found")
        pipeline.proposal_status = status
        pipeline.proposal_review_comment = review_comment
        return _ProposalRecord(
            pipeline_id=pipeline_id,
            status=status,
            title=pipeline.proposal_title,
            description=pipeline.proposal_description,
            review_comment=review_comment,
        )

    async def merge_branch(self, *, pipeline_id: str, from_branch: str, to_branch: str) -> None:
        pipeline = self._pipelines.get(pipeline_id)
        if pipeline:
            pipeline.branch = to_branch


class _FakeDatasetRegistry:
    pass


class _FakeObjectifyRegistry:
    pass


@pytest.mark.asyncio
async def test_pipeline_proposal_submit_and_approve_flow() -> None:
    registry = _FakePipelineRegistry()
    dataset_registry = _FakeDatasetRegistry()
    objectify_registry = _FakeObjectifyRegistry()
    audit_store = _FakeAuditStore()

    db_name = f"test_proposal_{uuid4().hex}"
    pipeline = await registry.create_pipeline(
        db_name=db_name,
        name=f"pipeline_{uuid4().hex}",
        description=None,
        pipeline_type="batch",
        location="/pipelines",
        status="draft",
        branch="feature",
    )

    await registry.grant_permission(
        pipeline_id=pipeline.pipeline_id,
        principal_type="user",
        principal_id="editor",
        role="edit",
    )
    await registry.grant_permission(
        pipeline_id=pipeline.pipeline_id,
        principal_type="user",
        principal_id="approver",
        role="approve",
    )

    response = await submit_pipeline_proposal(
        pipeline_id=pipeline.pipeline_id,
        payload={"title": "Proposal", "description": "test"},
        audit_store=audit_store,
        pipeline_registry=registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        request=_Request(headers={"X-Principal-Id": "editor"}),
    )
    assert response["status"] == "success"

    updated = await registry.get_pipeline(pipeline_id=pipeline.pipeline_id)
    assert updated is not None
    assert updated.proposal_status == "pending"
    assert updated.proposal_title == "Proposal"

    response = await approve_pipeline_proposal(
        pipeline_id=pipeline.pipeline_id,
        payload={"merge_into": "main", "review_comment": "ok"},
        audit_store=audit_store,
        pipeline_registry=registry,
        request=_Request(headers={"X-Principal-Id": "approver"}),
    )
    assert response["status"] == "success"

    reviewed = await registry.get_pipeline(pipeline_id=pipeline.pipeline_id)
    assert reviewed is not None
    assert reviewed.proposal_status == "approved"


@pytest.mark.asyncio
async def test_pipeline_proposal_requires_approve_role() -> None:
    registry = _FakePipelineRegistry()
    audit_store = _FakeAuditStore()

    db_name = f"test_approve_perm_{uuid4().hex}"
    pipeline = await registry.create_pipeline(
        db_name=db_name,
        name=f"pipeline_{uuid4().hex}",
        description=None,
        pipeline_type="batch",
        location="/pipelines",
        status="draft",
        branch="feature",
        proposal_status="pending",
        proposal_title="Pending",
    )

    await registry.grant_permission(
        pipeline_id=pipeline.pipeline_id,
        principal_type="user",
        principal_id="editor",
        role="edit",
    )

    with pytest.raises(HTTPException) as exc_info:
        await approve_pipeline_proposal(
            pipeline_id=pipeline.pipeline_id,
            payload={"merge_into": "main"},
            audit_store=audit_store,
            pipeline_registry=registry,
            request=_Request(headers={"X-Principal-Id": "editor"}),
        )

    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
    assert exc_info.value.detail == "Permission denied"


@pytest.mark.asyncio
async def test_pipeline_proposal_requires_pending_status() -> None:
    registry = _FakePipelineRegistry()
    audit_store = _FakeAuditStore()

    db_name = f"test_proposal_status_{uuid4().hex}"
    pipeline = await registry.create_pipeline(
        db_name=db_name,
        name=f"pipeline_{uuid4().hex}",
        description=None,
        pipeline_type="batch",
        location="/pipelines",
        status="draft",
        branch="feature",
    )

    await registry.grant_permission(
        pipeline_id=pipeline.pipeline_id,
        principal_type="user",
        principal_id="approver",
        role="approve",
    )

    with pytest.raises(HTTPException) as exc_info:
        await approve_pipeline_proposal(
            pipeline_id=pipeline.pipeline_id,
            payload={"merge_into": "main"},
            audit_store=audit_store,
            pipeline_registry=registry,
            request=_Request(headers={"X-Principal-Id": "approver"}),
        )

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail == "No pending proposal to approve"
