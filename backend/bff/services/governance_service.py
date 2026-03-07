"""Governance service (BFF).

Extracted from `bff.routers.governance` to keep routers thin and to centralize
authorization + registry workflows (Facade pattern).
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from fastapi import HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.role_deps import enforce_required_database_role
from bff.services.input_validation_service import enforce_db_scope_or_403
from bff.schemas.governance_requests import (
    AccessPolicyRequest,
    CreateBackingDatasourceRequest,
    CreateBackingDatasourceVersionRequest,
    CreateKeySpecRequest,
    GatePolicyRequest,
)
from shared.models.requests import ApiResponse
from shared.security.database_access import (
    DATA_ENGINEER_ROLES,
    DOMAIN_MODEL_ROLES,
    READ_ROLES,
    SECURITY_ROLES,
)
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input, validate_db_name
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.dataset_registry_get_or_create import get_or_create_record
from shared.utils.key_spec import normalize_key_spec
from shared.observability.tracing import trace_db_operation

logger = logging.getLogger(__name__)


@trace_db_operation("bff.governance.create_backing_datasource")
async def create_backing_datasource(
    *,
    body: CreateBackingDatasourceRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    payload = sanitize_input(body.model_dump())
    dataset_id = str(payload.get("dataset_id") or "").strip()
    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)

    enforce_db_scope_or_403(request, db_name=dataset.db_name)
    await enforce_required_database_role(request, db_name=dataset.db_name, roles=DATA_ENGINEER_ROLES)

    name = str(payload.get("name") or dataset.name or "").strip() or dataset.name
    description = str(payload.get("description") or dataset.description or "").strip() or None
    record, created = await get_or_create_record(
        lookup=lambda: dataset_registry.get_backing_datasource_by_dataset(
            dataset_id=dataset.dataset_id,
            branch=dataset.branch,
        ),
        create=lambda: dataset_registry.create_backing_datasource(
            dataset_id=dataset.dataset_id,
            db_name=dataset.db_name,
            name=name,
            description=description,
            source_type=dataset.source_type,
            source_ref=dataset.source_ref,
            branch=dataset.branch,
        ),
        conflict_context=f"governance-backing-datasource:{dataset.db_name}/{name}@{dataset.branch}",
    )
    return ApiResponse.success(
        message="Backing datasource created" if created else "Backing datasource already exists",
        data={"backing_datasource": record.__dict__},
    )


@trace_db_operation("bff.governance.list_backing_datasources")
async def list_backing_datasources(
    *,
    request: Request,
    dataset_id: Optional[str],
    db_name: Optional[str],
    branch: Optional[str],
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    records: List[Any] = []
    if dataset_id:
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        enforce_db_scope_or_403(request, db_name=dataset.db_name)
        await enforce_required_database_role(request, db_name=dataset.db_name, roles=READ_ROLES)
        record = await dataset_registry.get_backing_datasource_by_dataset(
            dataset_id=dataset.dataset_id,
            branch=branch or dataset.branch,
        )
        if record:
            records = [record.__dict__]
    elif db_name:
        enforce_db_scope_or_403(request, db_name=db_name)
        await enforce_required_database_role(request, db_name=db_name, roles=READ_ROLES)
        records = [item.__dict__ for item in await dataset_registry.list_backing_datasources(db_name=db_name, branch=branch)]
    else:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "dataset_id or db_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    return ApiResponse.success(message="Backing datasources retrieved", data={"backing_datasources": records})


@trace_db_operation("bff.governance.get_backing_datasource")
async def get_backing_datasource(
    *,
    backing_id: str,
    request: Request,
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    record = await dataset_registry.get_backing_datasource(backing_id=backing_id)
    if not record:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    enforce_db_scope_or_403(request, db_name=record.db_name)
    await enforce_required_database_role(request, db_name=record.db_name, roles=READ_ROLES)
    return ApiResponse.success(message="Backing datasource retrieved", data={"backing_datasource": record.__dict__})


@trace_db_operation("bff.governance.create_backing_datasource_version")
async def create_backing_datasource_version(
    *,
    backing_id: str,
    body: CreateBackingDatasourceVersionRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    payload = sanitize_input(body.model_dump())
    backing = await dataset_registry.get_backing_datasource(backing_id=backing_id)
    if not backing:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    enforce_db_scope_or_403(request, db_name=backing.db_name)
    await enforce_required_database_role(request, db_name=backing.db_name, roles=DATA_ENGINEER_ROLES)

    dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
    if not dataset_version_id:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "dataset_version_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    version = await dataset_registry.get_version(version_id=dataset_version_id)
    if not version or version.dataset_id != backing.dataset_id:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset version not found", code=ErrorCode.RESOURCE_NOT_FOUND)

    record = await dataset_registry.get_or_create_backing_datasource_version(
        backing_id=backing.backing_id,
        dataset_version_id=dataset_version_id,
        schema_hash=str(payload.get("schema_hash") or "").strip() or None,
        metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else None,
    )
    return ApiResponse.success(
        message="Backing datasource version created",
        data={"backing_datasource_version": record.__dict__},
    )


@trace_db_operation("bff.governance.list_backing_datasource_versions")
async def list_backing_datasource_versions(
    *,
    backing_id: str,
    request: Request,
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    backing = await dataset_registry.get_backing_datasource(backing_id=backing_id)
    if not backing:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    enforce_db_scope_or_403(request, db_name=backing.db_name)
    await enforce_required_database_role(request, db_name=backing.db_name, roles=READ_ROLES)
    records = await dataset_registry.list_backing_datasource_versions(backing_id=backing_id)
    return ApiResponse.success(
        message="Backing datasource versions retrieved",
        data={"backing_datasource_versions": [item.__dict__ for item in records]},
    )


@trace_db_operation("bff.governance.get_backing_datasource_version")
async def get_backing_datasource_version(
    *,
    version_id: str,
    request: Request,
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    record = await dataset_registry.get_backing_datasource_version(version_id=version_id)
    if not record:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource version not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    backing = await dataset_registry.get_backing_datasource(backing_id=record.backing_id)
    if backing:
        enforce_db_scope_or_403(request, db_name=backing.db_name)
        await enforce_required_database_role(request, db_name=backing.db_name, roles=READ_ROLES)
    return ApiResponse.success(
        message="Backing datasource version retrieved",
        data={"backing_datasource_version": record.__dict__},
    )


@trace_db_operation("bff.governance.create_key_spec")
async def create_key_spec(
    *,
    body: CreateKeySpecRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    payload = sanitize_input(body.model_dump())
    dataset_id = str(payload.get("dataset_id") or "").strip()
    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    enforce_db_scope_or_403(request, db_name=dataset.db_name)
    await enforce_required_database_role(request, db_name=dataset.db_name, roles=DOMAIN_MODEL_ROLES)

    dataset_version_id = str(payload.get("dataset_version_id") or "").strip() or None
    if dataset_version_id:
        version = await dataset_registry.get_version(version_id=dataset_version_id)
        if not version or version.dataset_id != dataset_id:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset version not found", code=ErrorCode.RESOURCE_NOT_FOUND)

    normalized = normalize_key_spec(
        {
            "primary_key": payload.get("primary_key"),
            "title_key": payload.get("title_key"),
            "unique_keys": payload.get("unique_keys"),
            "nullable_fields": payload.get("nullable_fields"),
            "required_fields": payload.get("required_fields"),
        }
    )
    if not normalized.get("primary_key"):
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "primary_key is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    record, created = await dataset_registry.get_or_create_key_spec(
        dataset_id=dataset_id,
        dataset_version_id=dataset_version_id,
        spec=normalized,
    )
    existing_spec = normalize_key_spec(record.spec)
    if not created:
        if existing_spec == normalized:
            return ApiResponse.success(message="Key spec already exists", data={"key_spec": record.__dict__})
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Key spec already exists with different spec",
            code=ErrorCode.CONFLICT,
        )

    return ApiResponse.success(message="Key spec created", data={"key_spec": record.__dict__})


@trace_db_operation("bff.governance.list_key_specs")
async def list_key_specs(
    *,
    request: Request,
    dataset_id: Optional[str],
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    if dataset_id:
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        enforce_db_scope_or_403(request, db_name=dataset.db_name)
        await enforce_required_database_role(request, db_name=dataset.db_name, roles=READ_ROLES)
    records = await dataset_registry.list_key_specs(dataset_id=dataset_id)
    return ApiResponse.success(message="Key specs retrieved", data={"key_specs": [r.__dict__ for r in records]})


@trace_db_operation("bff.governance.get_key_spec")
async def get_key_spec(
    *,
    key_spec_id: str,
    request: Request,
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    record = await dataset_registry.get_key_spec(key_spec_id=key_spec_id)
    if not record:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Key spec not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    dataset = await dataset_registry.get_dataset(dataset_id=record.dataset_id)
    if dataset:
        enforce_db_scope_or_403(request, db_name=dataset.db_name)
        await enforce_required_database_role(request, db_name=dataset.db_name, roles=READ_ROLES)
    return ApiResponse.success(message="Key spec retrieved", data={"key_spec": record.__dict__})


@trace_db_operation("bff.governance.list_schema_migration_plans")
async def list_schema_migration_plans(
    *,
    request: Request,
    db_name: Optional[str],
    subject_type: Optional[str],
    subject_id: Optional[str],
    status_value: Optional[str],
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    if db_name:
        enforce_db_scope_or_403(request, db_name=db_name)
        await enforce_required_database_role(request, db_name=db_name, roles=READ_ROLES)
    records = await dataset_registry.list_schema_migration_plans(
        db_name=db_name,
        subject_type=subject_type,
        subject_id=subject_id,
        status=status_value,
    )
    return ApiResponse.success(
        message="Schema migration plans retrieved",
        data={"schema_migration_plans": [r.__dict__ for r in records]},
    )


@trace_db_operation("bff.governance.upsert_gate_policy")
async def upsert_gate_policy(
    *,
    body: GatePolicyRequest,
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    payload = sanitize_input(body.model_dump())
    scope = str(payload.get("scope") or "").strip()
    name = str(payload.get("name") or "").strip()
    if not scope or not name:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "scope and name are required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    record = await dataset_registry.upsert_gate_policy(
        scope=scope,
        name=name,
        description=str(payload.get("description") or "").strip() or None,
        rules=payload.get("rules") if isinstance(payload.get("rules"), dict) else {},
        status=str(payload.get("status") or "ACTIVE").strip().upper() or "ACTIVE",
    )
    return ApiResponse.success(message="Gate policy saved", data={"gate_policy": record.__dict__})


@trace_db_operation("bff.governance.list_gate_policies")
async def list_gate_policies(
    *,
    scope: Optional[str],
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    records = await dataset_registry.list_gate_policies(scope=scope)
    return ApiResponse.success(message="Gate policies retrieved", data={"gate_policies": [r.__dict__ for r in records]})


@trace_db_operation("bff.governance.list_gate_results")
async def list_gate_results(
    *,
    scope: Optional[str],
    subject_type: Optional[str],
    subject_id: Optional[str],
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    records = await dataset_registry.list_gate_results(scope=scope, subject_type=subject_type, subject_id=subject_id)
    return ApiResponse.success(message="Gate results retrieved", data={"gate_results": [r.__dict__ for r in records]})


@trace_db_operation("bff.governance.upsert_access_policy")
async def upsert_access_policy(
    *,
    body: AccessPolicyRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    payload = sanitize_input(body.model_dump())
    db_name = validate_db_name(payload.get("db_name") or "")
    enforce_db_scope_or_403(request, db_name=db_name)
    await enforce_required_database_role(request, db_name=db_name, roles=SECURITY_ROLES)

    subject_type = str(payload.get("subject_type") or "").strip()
    subject_id = str(payload.get("subject_id") or "").strip()
    if not subject_type or not subject_id:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "subject_type/subject_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    if subject_type == "dataset":
        dataset = await dataset_registry.get_dataset(dataset_id=subject_id)
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        if dataset.db_name != db_name:
            raise classified_http_exception(status.HTTP_409_CONFLICT, "Dataset db_name mismatch", code=ErrorCode.CONFLICT)
    elif subject_type == "backing_datasource":
        backing = await dataset_registry.get_backing_datasource(backing_id=subject_id)
        if not backing:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        if backing.db_name != db_name:
            raise classified_http_exception(status.HTTP_409_CONFLICT, "Backing datasource db_name mismatch", code=ErrorCode.CONFLICT)

    record = await dataset_registry.upsert_access_policy(
        db_name=db_name,
        scope=str(payload.get("scope") or "data_access").strip() or "data_access",
        subject_type=subject_type,
        subject_id=subject_id,
        policy=payload.get("policy") if isinstance(payload.get("policy"), dict) else {},
        status=str(payload.get("status") or "ACTIVE").strip().upper(),
    )
    return ApiResponse.success(message="Access policy saved", data={"access_policy": record.__dict__})


@trace_db_operation("bff.governance.list_access_policies")
async def list_access_policies(
    *,
    request: Request,
    db_name: Optional[str],
    scope: Optional[str],
    subject_type: Optional[str],
    subject_id: Optional[str],
    policy_status: Optional[str],
    dataset_registry: DatasetRegistry,
) -> ApiResponse:
    if db_name:
        enforce_db_scope_or_403(request, db_name=db_name)
        await enforce_required_database_role(request, db_name=db_name, roles=SECURITY_ROLES)

    records = await dataset_registry.list_access_policies(
        db_name=db_name,
        scope=scope,
        subject_type=subject_type,
        subject_id=subject_id,
        status=policy_status,
    )
    return ApiResponse.success(message="Access policies retrieved", data={"access_policies": [r.__dict__ for r in records]})


@trace_db_operation("bff.governance.handle_request_errors")
async def handle_request_errors(fn, *args, **kwargs):  # noqa: ANN001, D401
    """Helper to wrap governance service calls with consistent error mapping."""
    try:
        return await fn(*args, **kwargs)
    except HTTPException:
        raise
    except (SecurityViolationError, ValueError) as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except Exception as exc:
        logger.error("Governance service failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
