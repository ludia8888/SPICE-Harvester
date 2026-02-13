"""Governance endpoints (BFF).

This router is intentionally thin: business logic lives in
`bff.services.governance_service` (Facade), and request schemas live in
`bff.schemas.governance_requests`.
"""

from shared.observability.tracing import trace_endpoint

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from bff.routers.registry_deps import get_dataset_registry
from bff.schemas.governance_requests import (
    AccessPolicyRequest,
    CreateBackingDatasourceRequest,
    CreateBackingDatasourceVersionRequest,
    CreateKeySpecRequest,
    GatePolicyRequest,
)
from bff.services import governance_service
from shared.models.requests import ApiResponse
from shared.services.registries.dataset_registry import DatasetRegistry

router = APIRouter(tags=["Governance"])


@router.post("/backing-datasources", response_model=ApiResponse)
@trace_endpoint("bff.governance.create_backing_datasource")
async def create_backing_datasource(
    body: CreateBackingDatasourceRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.create_backing_datasource,
        body=body,
        request=request,
        dataset_registry=dataset_registry,
    )


@router.get("/backing-datasources", response_model=ApiResponse)
@trace_endpoint("bff.governance.list_backing_datasources")
async def list_backing_datasources(
    request: Request,
    dataset_id: Optional[str] = Query(default=None),
    db_name: Optional[str] = Query(default=None),
    branch: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.list_backing_datasources,
        request=request,
        dataset_id=dataset_id,
        db_name=db_name,
        branch=branch,
        dataset_registry=dataset_registry,
    )


@router.get("/backing-datasources/{backing_id}", response_model=ApiResponse)
@trace_endpoint("bff.governance.get_backing_datasource")
async def get_backing_datasource(
    backing_id: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.get_backing_datasource,
        backing_id=backing_id,
        request=request,
        dataset_registry=dataset_registry,
    )


@router.post("/backing-datasources/{backing_id}/versions", response_model=ApiResponse)
@trace_endpoint("bff.governance.create_backing_datasource_version")
async def create_backing_datasource_version(
    backing_id: str,
    body: CreateBackingDatasourceVersionRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.create_backing_datasource_version,
        backing_id=backing_id,
        body=body,
        request=request,
        dataset_registry=dataset_registry,
    )


@router.get("/backing-datasources/{backing_id}/versions", response_model=ApiResponse)
@trace_endpoint("bff.governance.list_backing_datasource_versions")
async def list_backing_datasource_versions(
    backing_id: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.list_backing_datasource_versions,
        backing_id=backing_id,
        request=request,
        dataset_registry=dataset_registry,
    )


@router.get("/backing-datasource-versions/{version_id}", response_model=ApiResponse)
@trace_endpoint("bff.governance.get_backing_datasource_version")
async def get_backing_datasource_version(
    version_id: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.get_backing_datasource_version,
        version_id=version_id,
        request=request,
        dataset_registry=dataset_registry,
    )


@router.post("/key-specs", response_model=ApiResponse)
@trace_endpoint("bff.governance.create_key_spec")
async def create_key_spec(
    body: CreateKeySpecRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.create_key_spec,
        body=body,
        request=request,
        dataset_registry=dataset_registry,
    )


@router.get("/key-specs", response_model=ApiResponse)
@trace_endpoint("bff.governance.list_key_specs")
async def list_key_specs(
    request: Request,
    dataset_id: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.list_key_specs,
        request=request,
        dataset_id=dataset_id,
        dataset_registry=dataset_registry,
    )


@router.get("/key-specs/{key_spec_id}", response_model=ApiResponse)
@trace_endpoint("bff.governance.get_key_spec")
async def get_key_spec(
    key_spec_id: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.get_key_spec,
        key_spec_id=key_spec_id,
        request=request,
        dataset_registry=dataset_registry,
    )


@router.get("/schema-migration-plans", response_model=ApiResponse)
@trace_endpoint("bff.governance.list_schema_migration_plans")
async def list_schema_migration_plans(
    request: Request,
    db_name: Optional[str] = Query(default=None),
    subject_type: Optional[str] = Query(default=None),
    subject_id: Optional[str] = Query(default=None),
    status_value: Optional[str] = Query(default=None, alias="status"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.list_schema_migration_plans,
        request=request,
        db_name=db_name,
        subject_type=subject_type,
        subject_id=subject_id,
        status_value=status_value,
        dataset_registry=dataset_registry,
    )


@router.post("/gate-policies", response_model=ApiResponse)
@trace_endpoint("bff.governance.upsert_gate_policy")
async def upsert_gate_policy(
    body: GatePolicyRequest,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.upsert_gate_policy,
        body=body,
        dataset_registry=dataset_registry,
    )


@router.get("/gate-policies", response_model=ApiResponse)
@trace_endpoint("bff.governance.list_gate_policies")
async def list_gate_policies(
    scope: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.list_gate_policies,
        scope=scope,
        dataset_registry=dataset_registry,
    )


@router.get("/gate-results", response_model=ApiResponse)
@trace_endpoint("bff.governance.list_gate_results")
async def list_gate_results(
    scope: Optional[str] = Query(default=None),
    subject_type: Optional[str] = Query(default=None),
    subject_id: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.list_gate_results,
        scope=scope,
        subject_type=subject_type,
        subject_id=subject_id,
        dataset_registry=dataset_registry,
    )


@router.post("/access-policies", response_model=ApiResponse)
@trace_endpoint("bff.governance.upsert_access_policy")
async def upsert_access_policy(
    body: AccessPolicyRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.upsert_access_policy,
        body=body,
        request=request,
        dataset_registry=dataset_registry,
    )


@router.get("/access-policies", response_model=ApiResponse)
@trace_endpoint("bff.governance.list_access_policies")
async def list_access_policies(
    request: Request,
    db_name: Optional[str] = Query(default=None),
    scope: Optional[str] = Query(default=None),
    subject_type: Optional[str] = Query(default=None),
    subject_id: Optional[str] = Query(default=None),
    policy_status: Optional[str] = Query(default=None, alias="status"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    return await governance_service.handle_request_errors(
        governance_service.list_access_policies,
        request=request,
        db_name=db_name,
        scope=scope,
        subject_type=subject_type,
        subject_id=subject_id,
        policy_status=policy_status,
        dataset_registry=dataset_registry,
    )
