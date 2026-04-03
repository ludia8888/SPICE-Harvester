from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest
from fastapi import HTTPException

from bff.services import pipeline_catalog_service
from shared.services.registries.pipeline_registry import PipelineAlreadyExistsError


@dataclass
class _Request:
    headers: dict[str, str]


@dataclass
class _PipelineRecord:
    pipeline_id: str
    db_name: str
    name: str
    description: Optional[str]
    pipeline_type: str
    location: str
    status: str
    branch: str
    lakefs_repository: str
    proposal_status: Optional[str] = None
    proposal_title: Optional[str] = None
    proposal_description: Optional[str] = None
    proposal_submitted_at: Optional[str] = None
    proposal_reviewed_at: Optional[str] = None
    proposal_review_comment: Optional[str] = None
    schedule_interval_seconds: Optional[int] = None
    schedule_cron: Optional[str] = None


@dataclass
class _PipelineVersion:
    version_id: str
    lakefs_commit_id: str
    definition_json: dict[str, Any]


class _PipelineRegistry:
    def __init__(self) -> None:
        self.records: dict[str, _PipelineRecord] = {}
        self.versions: dict[str, _PipelineVersion] = {}
        self.dependencies: dict[str, list[dict[str, str]]] = {}
        self.create_calls = 0
        self.add_version_calls = 0
        self.grant_permission_calls = 0
        self.replace_dependencies_calls = 0
        self.fail_after_create_on_grant = False
        self._grant_failures_remaining = 0
        self._add_version_failures_remaining = 0
        self._replace_dependencies_failures_remaining = 0

    async def create_pipeline(self, **kwargs: Any) -> _PipelineRecord:
        self.create_calls += 1
        pipeline_id = str(kwargs["pipeline_id"])
        if pipeline_id in self.records:
            raise PipelineAlreadyExistsError(
                db_name=str(kwargs["db_name"]),
                name=str(kwargs["name"]),
                branch=str(kwargs["branch"]),
            )
        record = _PipelineRecord(
            pipeline_id=pipeline_id,
            db_name=str(kwargs["db_name"]),
            name=str(kwargs["name"]),
            description=kwargs.get("description"),
            pipeline_type=str(kwargs["pipeline_type"]),
            location=str(kwargs["location"]),
            status="draft",
            branch=str(kwargs["branch"]),
            lakefs_repository="pipeline-artifacts",
            proposal_status=kwargs.get("proposal_status"),
            proposal_title=kwargs.get("proposal_title"),
            proposal_description=kwargs.get("proposal_description"),
            proposal_submitted_at=kwargs.get("proposal_submitted_at"),
            proposal_reviewed_at=kwargs.get("proposal_reviewed_at"),
            proposal_review_comment=kwargs.get("proposal_review_comment"),
            schedule_interval_seconds=kwargs.get("schedule_interval_seconds"),
            schedule_cron=kwargs.get("schedule_cron"),
        )
        self.records[pipeline_id] = record
        return record

    async def get_pipeline(self, *, pipeline_id: str) -> Optional[_PipelineRecord]:
        return self.records.get(pipeline_id)

    async def grant_permission(self, **kwargs: Any) -> None:
        _ = kwargs
        if self._grant_failures_remaining > 0:
            self._grant_failures_remaining -= 1
            raise RuntimeError("grant failed")
        if self.fail_after_create_on_grant:
            raise RuntimeError("grant failed")
        self.grant_permission_calls += 1

    async def add_version(self, *, pipeline_id: str, branch: Optional[str] = None, definition_json: Optional[dict[str, Any]] = None) -> _PipelineVersion:
        _ = branch
        if self._add_version_failures_remaining > 0:
            self._add_version_failures_remaining -= 1
            raise RuntimeError("add_version failed")
        self.add_version_calls += 1
        version = _PipelineVersion(
            version_id=f"version-{self.add_version_calls}",
            lakefs_commit_id=f"commit-{self.add_version_calls}",
            definition_json=dict(definition_json or {}),
        )
        self.versions[pipeline_id] = version
        return version

    async def get_latest_version(self, *, pipeline_id: str, branch: Optional[str] = None) -> Optional[_PipelineVersion]:
        _ = branch
        return self.versions.get(pipeline_id)

    async def replace_dependencies(self, *, pipeline_id: str, dependencies: list[dict[str, str]]) -> None:
        if self._replace_dependencies_failures_remaining > 0:
            self._replace_dependencies_failures_remaining -= 1
            raise RuntimeError("replace_dependencies failed")
        self.replace_dependencies_calls += 1
        self.dependencies[pipeline_id] = list(dependencies)

    async def list_dependencies(self, *, pipeline_id: str) -> list[dict[str, str]]:
        return list(self.dependencies.get(pipeline_id, []))


class _AuditStore:
    def __init__(self) -> None:
        self.logged: list[dict[str, Any]] = []

    async def log(self, **kwargs: Any) -> None:
        self.logged.append(kwargs)


class _EventStore:
    def __init__(self) -> None:
        self.append_calls = 0

    async def connect(self) -> None:
        return None

    async def append_event(self, event: Any) -> None:
        _ = event
        self.append_calls += 1


async def _noop_async(*args: Any, **kwargs: Any) -> None:
    _ = args, kwargs
    return None


def _noop_sync(*args: Any, **kwargs: Any) -> None:
    _ = args, kwargs
    return None


@pytest.mark.asyncio
async def test_create_pipeline_reuses_existing_record_for_same_idempotency_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_catalog_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(pipeline_catalog_service, "enforce_database_role_or_http_error", _noop_async)

    registry = _PipelineRegistry()
    audit_store = _AuditStore()
    event_store = _EventStore()
    request = _Request(headers={"Idempotency-Key": "idem-1", "X-Principal-Id": "user-1"})
    payload = {
        "db_name": "test_db",
        "name": "Orders Pipeline",
        "location": "warehouse/orders",
    }

    first = await pipeline_catalog_service.create_pipeline(
        payload=payload,
        audit_store=audit_store,
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=event_store,
        request=request,
    )
    second = await pipeline_catalog_service.create_pipeline(
        payload=payload,
        audit_store=audit_store,
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=event_store,
        request=request,
    )

    assert first["status"] == "success"
    assert second == first
    assert registry.create_calls == 1
    assert registry.add_version_calls == 1
    assert event_store.append_calls == 1


@pytest.mark.asyncio
async def test_create_pipeline_rejects_same_idempotency_key_with_different_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_catalog_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(pipeline_catalog_service, "enforce_database_role_or_http_error", _noop_async)

    registry = _PipelineRegistry()
    request = _Request(headers={"Idempotency-Key": "idem-2", "X-Principal-Id": "user-1"})
    payload = {
        "db_name": "test_db",
        "name": "Orders Pipeline",
        "location": "warehouse/orders",
    }

    await pipeline_catalog_service.create_pipeline(
        payload=payload,
        audit_store=_AuditStore(),
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=_EventStore(),
        request=request,
    )

    with pytest.raises(HTTPException) as exc_info:
        await pipeline_catalog_service.create_pipeline(
            payload={**payload, "name": "Different Name"},
            audit_store=_AuditStore(),
            pipeline_registry=registry,
            dataset_registry=object(),
            event_store=_EventStore(),
            request=request,
        )

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_idempotent_resume_does_not_grant_admin_before_equivalence_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_catalog_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(pipeline_catalog_service, "enforce_database_role_or_http_error", _noop_async)
    monkeypatch.setattr(pipeline_catalog_service, "_validate_dependency_targets", _noop_async)

    registry = _PipelineRegistry()
    request = _Request(headers={"Idempotency-Key": "idem-3", "X-Principal-Id": "user-1"})
    dependency_a = "11111111-1111-1111-1111-111111111111"
    dependency_b = "22222222-2222-2222-2222-222222222222"
    payload = {
        "db_name": "test_db",
        "name": "Orders Pipeline",
        "location": "warehouse/orders",
        "dependencies": [{"pipeline_id": dependency_a, "behavior": "required"}],
    }

    await pipeline_catalog_service.create_pipeline(
        payload=payload,
        audit_store=_AuditStore(),
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=_EventStore(),
        request=request,
    )

    with pytest.raises(HTTPException) as exc_info:
        await pipeline_catalog_service.create_pipeline(
            payload={**payload, "dependencies": [{"pipeline_id": dependency_b, "behavior": "required"}]},
            audit_store=_AuditStore(),
            pipeline_registry=registry,
            dataset_registry=object(),
            event_store=_EventStore(),
            request=request,
        )

    assert exc_info.value.status_code == 409
    assert registry.grant_permission_calls == 1


@pytest.mark.asyncio
async def test_create_pipeline_recovers_when_permission_grant_fails_after_row_create(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_catalog_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(pipeline_catalog_service, "enforce_database_role_or_http_error", _noop_async)

    registry = _PipelineRegistry()
    registry._grant_failures_remaining = 1
    request = _Request(headers={"Idempotency-Key": "idem-4", "X-Principal-Id": "user-1"})
    payload = {
        "db_name": "test_db",
        "name": "Orders Pipeline",
        "location": "warehouse/orders",
    }

    response = await pipeline_catalog_service.create_pipeline(
        payload=payload,
        audit_store=_AuditStore(),
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=_EventStore(),
        request=request,
    )

    assert response["status"] == "success"
    pipeline = response["data"]["pipeline"]
    assert pipeline["pipeline_id"] in registry.records
    assert registry.create_calls == 1
    assert registry.add_version_calls == 1


@pytest.mark.asyncio
async def test_create_pipeline_recovers_when_add_version_fails_after_row_create(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_catalog_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(pipeline_catalog_service, "enforce_database_role_or_http_error", _noop_async)

    registry = _PipelineRegistry()
    registry._add_version_failures_remaining = 1
    request = _Request(headers={"Idempotency-Key": "idem-5", "X-Principal-Id": "user-1"})
    payload = {
        "db_name": "test_db",
        "name": "Orders Pipeline",
        "location": "warehouse/orders",
    }

    response = await pipeline_catalog_service.create_pipeline(
        payload=payload,
        audit_store=_AuditStore(),
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=_EventStore(),
        request=request,
    )

    assert response["status"] == "success"
    pipeline = response["data"]["pipeline"]
    assert pipeline["pipeline_id"] in registry.records
    assert registry.create_calls == 1
    assert registry.add_version_calls == 1


@pytest.mark.asyncio
async def test_create_pipeline_recovers_when_dependencies_fail_after_row_create(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_catalog_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(pipeline_catalog_service, "enforce_database_role_or_http_error", _noop_async)
    monkeypatch.setattr(pipeline_catalog_service, "_validate_dependency_targets", _noop_async)

    registry = _PipelineRegistry()
    registry._replace_dependencies_failures_remaining = 1
    request = _Request(headers={"Idempotency-Key": "idem-6", "X-Principal-Id": "user-1"})
    dependency_id = "33333333-3333-3333-3333-333333333333"
    payload = {
        "db_name": "test_db",
        "name": "Orders Pipeline",
        "location": "warehouse/orders",
        "dependencies": [{"pipeline_id": dependency_id, "behavior": "required"}],
    }

    response = await pipeline_catalog_service.create_pipeline(
        payload=payload,
        audit_store=_AuditStore(),
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=_EventStore(),
        request=request,
    )

    assert response["status"] == "success"
    pipeline = response["data"]["pipeline"]
    assert pipeline["pipeline_id"] in registry.records
    assert registry.create_calls == 1
    assert registry.replace_dependencies_calls == 1
    assert pipeline["dependencies"] == [{"pipelineId": dependency_id, "status": "DEPLOYED"}]


@pytest.mark.asyncio
async def test_create_pipeline_returns_success_when_admin_grant_keeps_failing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_catalog_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(pipeline_catalog_service, "enforce_database_role_or_http_error", _noop_async)

    registry = _PipelineRegistry()
    registry.fail_after_create_on_grant = True
    request = _Request(headers={"Idempotency-Key": "idem-7", "X-Principal-Id": "user-1"})
    payload = {
        "db_name": "test_db",
        "name": "Orders Pipeline",
        "location": "warehouse/orders",
    }

    response = await pipeline_catalog_service.create_pipeline(
        payload=payload,
        audit_store=_AuditStore(),
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=_EventStore(),
        request=request,
    )

    assert response["status"] == "success"
    pipeline = response["data"]["pipeline"]
    assert pipeline["pipeline_id"] in registry.records
    assert registry.create_calls == 1
    assert registry.add_version_calls == 1
    assert registry.grant_permission_calls == 0


@pytest.mark.asyncio
async def test_create_pipeline_records_write_path_contract_in_audit_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_catalog_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(pipeline_catalog_service, "enforce_database_role_or_http_error", _noop_async)

    registry = _PipelineRegistry()
    audit_store = _AuditStore()
    request = _Request(headers={"Idempotency-Key": "idem-8", "X-Principal-Id": "user-1"})

    await pipeline_catalog_service.create_pipeline(
        payload={
            "db_name": "test_db",
            "name": "Orders Pipeline",
            "location": "warehouse/orders",
        },
        audit_store=audit_store,
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=_EventStore(),
        request=request,
    )

    contract = audit_store.logged[-1]["metadata"]["write_path_contract"]
    assert contract["authoritative_state"] == "committed"
    assert contract["authoritative_write"] == "pipeline_create"
    derived = {item["name"]: item for item in contract["derived_side_effects"]}
    assert derived["pipeline_dependencies"]["status"] == "skipped"
    assert derived["pipeline_admin_grant"]["status"] == "completed"
    assert derived["pipeline_created_event"]["status"] == "completed"
    assert derived["pipeline_audit"]["status"] == "completed"


@pytest.mark.asyncio
async def test_create_pipeline_records_degraded_admin_followup_in_write_path_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_catalog_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(pipeline_catalog_service, "enforce_database_role_or_http_error", _noop_async)

    registry = _PipelineRegistry()
    registry.fail_after_create_on_grant = True
    audit_store = _AuditStore()
    request = _Request(headers={"Idempotency-Key": "idem-9", "X-Principal-Id": "user-1"})

    response = await pipeline_catalog_service.create_pipeline(
        payload={
            "db_name": "test_db",
            "name": "Orders Pipeline",
            "location": "warehouse/orders",
        },
        audit_store=audit_store,
        pipeline_registry=registry,
        dataset_registry=object(),
        event_store=_EventStore(),
        request=request,
    )

    assert response["status"] == "success"
    contract = audit_store.logged[-1]["metadata"]["write_path_contract"]
    derived = {item["name"]: item for item in contract["derived_side_effects"]}
    assert derived["pipeline_admin_grant"]["status"] == "degraded"
