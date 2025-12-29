from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import pytest
from fastapi import HTTPException, status

from bff.routers.pipeline import approve_pipeline_proposal, submit_pipeline_proposal
from shared.config.service_config import ServiceConfig
from shared.services.audit_log_store import AuditLogStore
from shared.services.dataset_registry import DatasetRegistry
from shared.services.objectify_registry import ObjectifyRegistry
from shared.services.pipeline_registry import PipelineRegistry


@dataclass
class _Request:
    headers: dict[str, str]


class _LakeFSClient:
    async def merge(self, **kwargs):  # noqa: ANN003
        return "commit-merge"


class _LakeFSStorage:
    async def load_json(self, **kwargs):  # noqa: ANN003
        return {}


@pytest.mark.asyncio
async def test_pipeline_proposal_submit_and_approve_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = PipelineRegistry(dsn=ServiceConfig.get_postgres_url())
    dataset_registry = DatasetRegistry(dsn=ServiceConfig.get_postgres_url())
    objectify_registry = ObjectifyRegistry(dsn=ServiceConfig.get_postgres_url())
    audit_store = AuditLogStore(dsn=ServiceConfig.get_postgres_url())
    await registry.connect()
    await dataset_registry.connect()
    await objectify_registry.connect()
    await audit_store.connect()

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

    async def _get_lakefs_client(**kwargs):  # noqa: ANN003
        return _LakeFSClient()

    async def _get_lakefs_storage(**kwargs):  # noqa: ANN003
        return _LakeFSStorage()

    monkeypatch.setattr(registry, "get_lakefs_client", _get_lakefs_client)
    monkeypatch.setattr(registry, "get_lakefs_storage", _get_lakefs_storage)

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

    await audit_store.close()
    await objectify_registry.close()
    await dataset_registry.close()
    await registry.close()


@pytest.mark.asyncio
async def test_pipeline_proposal_requires_approve_role() -> None:
    registry = PipelineRegistry(dsn=ServiceConfig.get_postgres_url())
    audit_store = AuditLogStore(dsn=ServiceConfig.get_postgres_url())
    await registry.connect()
    await audit_store.connect()

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

    await audit_store.close()
    await registry.close()


@pytest.mark.asyncio
async def test_pipeline_proposal_requires_pending_status(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = PipelineRegistry(dsn=ServiceConfig.get_postgres_url())
    audit_store = AuditLogStore(dsn=ServiceConfig.get_postgres_url())
    await registry.connect()
    await audit_store.connect()

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

    async def _get_lakefs_client(**kwargs):  # noqa: ANN003
        return _LakeFSClient()

    async def _get_lakefs_storage(**kwargs):  # noqa: ANN003
        return _LakeFSStorage()

    monkeypatch.setattr(registry, "get_lakefs_client", _get_lakefs_client)
    monkeypatch.setattr(registry, "get_lakefs_storage", _get_lakefs_storage)

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

    await audit_store.close()
    await registry.close()
