"""
Governance resources: backing datasources, key specs, gate policies/results.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from shared.models.requests import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.dataset_registry import DatasetRegistry
from shared.utils.key_spec import normalize_key_spec

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Governance"])


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


class CreateBackingDatasourceRequest(BaseModel):
    dataset_id: str
    name: Optional[str] = None
    description: Optional[str] = None


class CreateBackingDatasourceVersionRequest(BaseModel):
    dataset_version_id: str
    schema_hash: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class CreateKeySpecRequest(BaseModel):
    dataset_id: str
    dataset_version_id: Optional[str] = None
    primary_key: List[str] = Field(default_factory=list)
    title_key: List[str] = Field(default_factory=list)
    unique_keys: List[List[str]] = Field(default_factory=list)
    nullable_fields: List[str] = Field(default_factory=list)
    required_fields: List[str] = Field(default_factory=list)


class GatePolicyRequest(BaseModel):
    scope: str
    name: str
    description: Optional[str] = None
    rules: Dict[str, Any] = Field(default_factory=dict)
    status: str = Field(default="ACTIVE")


class AccessPolicyRequest(BaseModel):
    db_name: str
    scope: str = Field(default="data_access")
    subject_type: str
    subject_id: str
    policy: Dict[str, Any] = Field(default_factory=dict)
    status: str = Field(default="ACTIVE")


@router.post("/backing-datasources", response_model=ApiResponse)
async def create_backing_datasource(
    body: CreateBackingDatasourceRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        payload = sanitize_input(body.model_dump())
        dataset_id = str(payload.get("dataset_id") or "").strip()
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))

        existing = await dataset_registry.get_backing_datasource_by_dataset(
            dataset_id=dataset.dataset_id,
            branch=dataset.branch,
        )
        if existing:
            return ApiResponse.success(
                message="Backing datasource already exists",
                data={"backing_datasource": existing.__dict__},
            )

        name = str(payload.get("name") or dataset.name or "").strip() or dataset.name
        description = str(payload.get("description") or dataset.description or "").strip() or None
        record = await dataset_registry.create_backing_datasource(
            dataset_id=dataset.dataset_id,
            db_name=dataset.db_name,
            name=name,
            description=description,
            source_type=dataset.source_type,
            source_ref=dataset.source_ref,
            branch=dataset.branch,
        )
        return ApiResponse.success(
            message="Backing datasource created",
            data={"backing_datasource": record.__dict__},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create backing datasource: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/backing-datasources", response_model=ApiResponse)
async def list_backing_datasources(
    request: Request,
    dataset_id: Optional[str] = Query(default=None),
    db_name: Optional[str] = Query(default=None),
    branch: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        records: List[Any] = []
        if dataset_id:
            dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
            if not dataset:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
            try:
                enforce_db_scope(request.headers, db_name=dataset.db_name)
            except ValueError as exc:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
            record = await dataset_registry.get_backing_datasource_by_dataset(
                dataset_id=dataset.dataset_id,
                branch=branch or dataset.branch,
            )
            if record:
                records = [record.__dict__]
        elif db_name:
            enforce_db_scope(request.headers, db_name=db_name)
            records = [
                item.__dict__
                for item in await dataset_registry.list_backing_datasources(db_name=db_name, branch=branch)
            ]
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dataset_id or db_name is required")
        return ApiResponse.success(message="Backing datasources retrieved", data={"backing_datasources": records})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list backing datasources: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/backing-datasources/{backing_id}", response_model=ApiResponse)
async def get_backing_datasource(
    backing_id: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        record = await dataset_registry.get_backing_datasource(backing_id=backing_id)
        if not record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
        enforce_db_scope(request.headers, db_name=record.db_name)
        return ApiResponse.success(message="Backing datasource retrieved", data={"backing_datasource": record.__dict__})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get backing datasource: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/backing-datasources/{backing_id}/versions", response_model=ApiResponse)
async def create_backing_datasource_version(
    backing_id: str,
    body: CreateBackingDatasourceVersionRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        payload = sanitize_input(body.model_dump())
        backing = await dataset_registry.get_backing_datasource(backing_id=backing_id)
        if not backing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
        enforce_db_scope(request.headers, db_name=backing.db_name)
        dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
        if not dataset_version_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dataset_version_id is required")
        version = await dataset_registry.get_version(version_id=dataset_version_id)
        if not version or version.dataset_id != backing.dataset_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset version not found")
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
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create backing datasource version: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/backing-datasources/{backing_id}/versions", response_model=ApiResponse)
async def list_backing_datasource_versions(
    backing_id: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        backing = await dataset_registry.get_backing_datasource(backing_id=backing_id)
        if not backing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
        enforce_db_scope(request.headers, db_name=backing.db_name)
        records = await dataset_registry.list_backing_datasource_versions(backing_id=backing_id)
        return ApiResponse.success(
            message="Backing datasource versions retrieved",
            data={"backing_datasource_versions": [item.__dict__ for item in records]},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list backing datasource versions: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/backing-datasource-versions/{version_id}", response_model=ApiResponse)
async def get_backing_datasource_version(
    version_id: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        record = await dataset_registry.get_backing_datasource_version(version_id=version_id)
        if not record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource version not found")
        backing = await dataset_registry.get_backing_datasource(backing_id=record.backing_id)
        if backing:
            enforce_db_scope(request.headers, db_name=backing.db_name)
        return ApiResponse.success(
            message="Backing datasource version retrieved",
            data={"backing_datasource_version": record.__dict__},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get backing datasource version: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/key-specs", response_model=ApiResponse)
async def create_key_spec(
    body: CreateKeySpecRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        payload = sanitize_input(body.model_dump())
        dataset_id = str(payload.get("dataset_id") or "").strip()
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
        enforce_db_scope(request.headers, db_name=dataset.db_name)

        dataset_version_id = str(payload.get("dataset_version_id") or "").strip() or None
        if dataset_version_id:
            version = await dataset_registry.get_version(version_id=dataset_version_id)
            if not version or version.dataset_id != dataset_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset version not found")

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
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="primary_key is required")

        existing = await dataset_registry.get_key_spec_for_dataset(
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
        )
        if existing:
            existing_spec = normalize_key_spec(existing.spec)
            if existing_spec == normalized:
                return ApiResponse.success(
                    message="Key spec already exists",
                    data={"key_spec": existing.__dict__},
                )
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Key spec already exists with different spec")

        record = await dataset_registry.create_key_spec(
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
            spec=normalized,
        )
        return ApiResponse.success(message="Key spec created", data={"key_spec": record.__dict__})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create key spec: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/key-specs", response_model=ApiResponse)
async def list_key_specs(
    request: Request,
    dataset_id: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        if dataset_id:
            dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
            if not dataset:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        records = await dataset_registry.list_key_specs(dataset_id=dataset_id)
        return ApiResponse.success(message="Key specs retrieved", data={"key_specs": [r.__dict__ for r in records]})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list key specs: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/key-specs/{key_spec_id}", response_model=ApiResponse)
async def get_key_spec(
    key_spec_id: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        record = await dataset_registry.get_key_spec(key_spec_id=key_spec_id)
        if not record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Key spec not found")
        dataset = await dataset_registry.get_dataset(dataset_id=record.dataset_id)
        if dataset:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        return ApiResponse.success(message="Key spec retrieved", data={"key_spec": record.__dict__})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get key spec: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/gate-policies", response_model=ApiResponse)
async def upsert_gate_policy(
    body: GatePolicyRequest,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        payload = sanitize_input(body.model_dump())
        scope = str(payload.get("scope") or "").strip()
        name = str(payload.get("name") or "").strip()
        if not scope or not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="scope and name are required")
        record = await dataset_registry.upsert_gate_policy(
            scope=scope,
            name=name,
            description=str(payload.get("description") or "").strip() or None,
            rules=payload.get("rules") if isinstance(payload.get("rules"), dict) else {},
            status=str(payload.get("status") or "ACTIVE").strip().upper() or "ACTIVE",
        )
        return ApiResponse.success(message="Gate policy saved", data={"gate_policy": record.__dict__})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to upsert gate policy: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/gate-policies", response_model=ApiResponse)
async def list_gate_policies(
    scope: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        records = await dataset_registry.list_gate_policies(scope=scope)
        return ApiResponse.success(message="Gate policies retrieved", data={"gate_policies": [r.__dict__ for r in records]})
    except Exception as exc:
        logger.error("Failed to list gate policies: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/gate-results", response_model=ApiResponse)
async def list_gate_results(
    scope: Optional[str] = Query(default=None),
    subject_type: Optional[str] = Query(default=None),
    subject_id: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        records = await dataset_registry.list_gate_results(
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
        )
        return ApiResponse.success(message="Gate results retrieved", data={"gate_results": [r.__dict__ for r in records]})
    except Exception as exc:
        logger.error("Failed to list gate results: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/access-policies", response_model=ApiResponse)
async def upsert_access_policy(
    body: AccessPolicyRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        payload = sanitize_input(body.model_dump())
        db_name = validate_db_name(payload.get("db_name") or "")
        enforce_db_scope(request.headers, db_name=db_name)

        subject_type = str(payload.get("subject_type") or "").strip()
        subject_id = str(payload.get("subject_id") or "").strip()
        if not subject_type or not subject_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="subject_type/subject_id is required")

        if subject_type == "dataset":
            dataset = await dataset_registry.get_dataset(dataset_id=subject_id)
            if not dataset:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
            if dataset.db_name != db_name:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Dataset db_name mismatch")
        elif subject_type == "backing_datasource":
            backing = await dataset_registry.get_backing_datasource(backing_id=subject_id)
            if not backing:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
            if backing.db_name != db_name:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Backing datasource db_name mismatch")

        record = await dataset_registry.upsert_access_policy(
            db_name=db_name,
            scope=str(payload.get("scope") or "data_access").strip() or "data_access",
            subject_type=subject_type,
            subject_id=subject_id,
            policy=payload.get("policy") if isinstance(payload.get("policy"), dict) else {},
            status=str(payload.get("status") or "ACTIVE").strip().upper(),
        )
        return ApiResponse.success(message="Access policy saved", data={"access_policy": record.__dict__})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to upsert access policy: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/access-policies", response_model=ApiResponse)
async def list_access_policies(
    request: Request,
    db_name: Optional[str] = Query(default=None),
    scope: Optional[str] = Query(default=None),
    subject_type: Optional[str] = Query(default=None),
    subject_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        if db_name:
            enforce_db_scope(request.headers, db_name=db_name)
        records = await dataset_registry.list_access_policies(
            db_name=db_name,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            status=status,
        )
        return ApiResponse.success(
            message="Access policies retrieved",
            data={"access_policies": [r.__dict__ for r in records]},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list access policies: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
