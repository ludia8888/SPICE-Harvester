"""
Ontology extensions router (resource CRUD + validation helpers).
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, status

from oms.exceptions import DatabaseError, OntologyNotFoundError
from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2
from oms.services.ontology_resources import (
    OntologyResourceService,
    normalize_resource_type,
)
from oms.services.ontology_resource_validator import (
    ResourceReferenceError,
    ResourceSpecError,
    validate_resource,
)
from shared.models.requests import ApiResponse
from shared.schemas.ontology_extension_requests import (
    OntologyDeploymentRecordRequest,
    OntologyResourceRequest,
)
from shared.security.input_sanitizer import (
    sanitize_input,
    validate_branch_name,
    validate_db_name,
    validate_instance_id,
)
from shared.utils.branch_utils import get_protected_branches, protected_branch_write_message
from shared.config.settings import get_settings
from shared.utils.ontology_type_normalization import normalize_ontology_base_type
from shared.utils.id_generator import generate_simple_id
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.observability.tracing import trace_endpoint

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}/ontology", tags=["Ontology Extensions"])


async def _count_list_resources_total(
    *,
    service: OntologyResourceService,
    db_name: str,
    branch: str,
    resource_type: Optional[str],
) -> int:
    total = 0
    offset = 0
    page_size = 1000
    while True:
        page = await service.list_resources(
            db_name,
            branch=branch,
            resource_type=resource_type,
            limit=page_size,
            offset=offset,
        )
        page_count = len(page)
        total += page_count
        if page_count < page_size:
            return total
        offset += page_count


def _normalize_resource_payload(payload: OntologyResourceRequest) -> Dict[str, Any]:
    data = payload.model_dump(exclude_unset=True)
    spec = data.pop("spec", {}) or {}
    metadata = data.pop("metadata", {}) or {}
    resource_id = data.pop("id", None)
    label = data.pop("label", None)
    description = data.pop("description", None)

    if data:
        spec = {**data, **spec}

    return {
        "id": resource_id,
        "label": label,
        "description": description,
        "spec": spec,
        "metadata": metadata,
    }


def _extract_value_type_spec(payload: Dict[str, Any]) -> Dict[str, Any]:
    spec = payload.get("spec") if isinstance(payload, dict) else None
    spec = spec if isinstance(spec, dict) else {}
    base_type = spec.get("base_type") or spec.get("baseType")
    constraints = spec.get("constraints") or {}
    return {"base_type": base_type, "constraints": constraints}


def _validate_value_type_immutability(existing: Dict[str, Any], incoming: Dict[str, Any]) -> None:
    existing_spec = _extract_value_type_spec(existing)
    incoming_spec = _extract_value_type_spec(incoming)
    if normalize_ontology_base_type(existing_spec.get("base_type")) != normalize_ontology_base_type(
        incoming_spec.get("base_type")
    ):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "value_type base_type is immutable; create a new value type for changes",
            code=ErrorCode.ONTOLOGY_ATOMIC_UPDATE_FAILED,
        )
    if (existing_spec.get("constraints") or {}) != (incoming_spec.get("constraints") or {}):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "value_type constraints are immutable; create a new value type for changes",
            code=ErrorCode.ONTOLOGY_ATOMIC_UPDATE_FAILED,
        )


def _resource_validation_strict() -> bool:
    return bool(get_settings().ontology.resource_strict)


def _ensure_branch_writable(branch: str) -> None:
    protected = get_protected_branches()
    if branch not in protected:
        return
    if bool(get_settings().ontology.require_proposals):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            protected_branch_write_message(),
            code=ErrorCode.CONFLICT,
        )


def _is_foundry_v2_strict_compat_enabled(*, db_name: str) -> bool:
    _ = db_name
    return True


def _normalize_occ_branch_token(branch: str) -> str:
    raw = str(branch or "").strip()
    if raw.lower().startswith("branch:"):
        raw = raw.split(":", 1)[1].strip()
    return raw or "main"


async def _assert_expected_head_commit(
    *,
    db_name: str,
    branch: str,
    expected_head_commit: Optional[str],
    strict_mode: bool,
) -> Optional[str]:
    expected = str(expected_head_commit or "").strip()
    if not expected:
        if strict_mode:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "expected_head_commit is required for strict compat mode",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                external_code="INVALID_ARGUMENT",
            )
        return None

    normalized_branch = _normalize_occ_branch_token(branch)
    allowed_tokens: set[str] = {
        str(branch or "").strip(),
        normalized_branch,
        f"branch:{normalized_branch}",
    }
    try:
        deployment_registry = OntologyDeploymentRegistryV2()
        latest = await deployment_registry.get_latest_deployed_commit(
            db_name=db_name,
            target_branch=normalized_branch,
        )
        deployed_commit = str((latest or {}).get("ontology_commit_id") or "").strip()
        if deployed_commit:
            allowed_tokens.add(deployed_commit)
    except Exception as exc:
        logger.warning(
            "Failed to resolve deployed ontology commit for OCC guard (%s/%s): %s",
            db_name,
            normalized_branch,
            exc,
        )

    if expected not in allowed_tokens:
        logger.warning(
            "OCC mismatch for ontology resource write: db=%s branch=%s expected=%s allowed_count=%d",
            db_name,
            branch,
            expected,
            len(allowed_tokens),
        )
        if strict_mode:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "expected_head_commit does not match allowed branch token/commit",
                code=ErrorCode.CONFLICT,
                external_code="CONFLICT",
                extra={
                    "db_name": db_name,
                    "branch": branch,
                    "expected_head_commit": expected,
                    "allowed_count": len(allowed_tokens),
                },
            )
        return None

    return expected


# Internal: BFF proxies via OMSClient. Public contract: /api/v1/databases/*/ontology (plural).
@router.post("/records/deployments", include_in_schema=False)
@trace_endpoint("oms.ontology_ext.record_deployment")
async def record_deployment(
    db_name: str,
    payload: OntologyDeploymentRecordRequest,
):
    try:
        db_name = validate_db_name(db_name)
        target_branch = validate_branch_name(payload.target_branch or "main")
        ontology_commit_id = str(payload.ontology_commit_id or "").strip() or f"branch:{target_branch}"
        deployed_by = str(payload.deployed_by or "").strip() or "system"

        registry = OntologyDeploymentRegistryV2()
        recorded = await registry.record_deployment(
            db_name=db_name,
            target_branch=target_branch,
            ontology_commit_id=ontology_commit_id,
            snapshot_rid=(str(payload.snapshot_rid).strip() or None) if payload.snapshot_rid is not None else None,
            deployed_by=deployed_by,
            metadata=sanitize_input(payload.metadata or {}),
        )
        return ApiResponse.success(
            message="Ontology deployment recorded",
            data=recorded,
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except Exception as e:
        logger.error("Failed to record ontology deployment: %s", e)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR,
        )


# Internal: BFF proxies via OMSClient. Public contract: /api/v1/databases/*/ontology (plural).
@router.get("/resources", include_in_schema=False)
@trace_endpoint("oms.ontology_ext.list_resources")
async def list_resources(
    db_name: str,
    resource_type: Optional[str] = Query(None, description="Resource type filter"),
    branch: str = Query("main", description="Target branch"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type) if resource_type else None

        service = OntologyResourceService()
        resources = await service.list_resources(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            limit=limit,
            offset=offset,
        )
        total = await _count_list_resources_total(
            service=service,
            db_name=db_name,
            branch=branch,
            resource_type=normalized_type,
        )

        return ApiResponse.success(
            message="Ontology resources retrieved",
            data={"resources": resources, "total": total},
        ).to_dict()
    except RuntimeError as e:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE, str(e), code=ErrorCode.UPSTREAM_UNAVAILABLE,
        )
    except ValueError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except Exception as e:
        logger.error("Failed to list resources: %s", e)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR,
        )


# Internal: BFF proxies via OMSClient. Public contract: /api/v1/databases/*/ontology (plural).
@router.get("/resources/{resource_type}", include_in_schema=False)
@trace_endpoint("oms.ontology_ext.list_resources_by_type")
async def list_resources_by_type(
    db_name: str,
    resource_type: str,
    branch: str = Query("main", description="Target branch"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    return await list_resources(
        db_name=db_name,
        resource_type=resource_type,
        branch=branch,
        limit=limit,
        offset=offset,
    )


# Internal: BFF proxies via OMSClient. Public contract: /api/v1/databases/*/ontology (plural).
@router.post("/resources/{resource_type}", status_code=status.HTTP_201_CREATED, include_in_schema=False)
@trace_endpoint("oms.ontology_ext.create_resource")
async def create_resource(
    db_name: str,
    resource_type: str,
    payload: OntologyResourceRequest,
    branch: str = Query(..., description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)

        _ensure_branch_writable(branch)
        resolved_expected_head = await _assert_expected_head_commit(
            db_name=db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
            strict_mode=strict_compat,
        )

        sanitized = sanitize_input(_normalize_resource_payload(payload))
        await validate_resource(
            db_name=db_name,
            resource_type=normalized_type,
            payload=sanitized,
            branch=branch,
            expected_head_commit=resolved_expected_head,
            strict=_resource_validation_strict(),
        )
        resource_id = sanitized.get("id")
        if resource_id:
            resource_id = validate_instance_id(resource_id)
        else:
            resource_id = generate_simple_id(sanitized.get("label"), default_fallback="resource")

        service = OntologyResourceService()
        created = await service.create_resource(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
            payload=sanitized,
        )

        return ApiResponse.created(
            message="Ontology resource created",
            data=created,
        ).to_dict()
    except ResourceSpecError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.ONTOLOGY_VALIDATION_FAILED,
        )
    except ResourceReferenceError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.ONTOLOGY_RELATIONSHIP_ERROR,
        )
    except RuntimeError as e:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE, str(e), code=ErrorCode.UPSTREAM_UNAVAILABLE,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except DatabaseError as e:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT, str(e), code=ErrorCode.DB_CONSTRAINT_VIOLATION,
        )
    except Exception as e:
        logger.error("Failed to create resource: %s", e)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR,
        )


# Internal: BFF proxies via OMSClient. Public contract: /api/v1/databases/*/ontology (plural).
@router.get("/resources/{resource_type}/{resource_id}", include_in_schema=False)
@trace_endpoint("oms.ontology_ext.get_resource")
async def get_resource(
    db_name: str,
    resource_type: str,
    resource_id: str,
    branch: str = Query("main", description="Target branch"),
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type)
        resource_id = validate_instance_id(resource_id)

        service = OntologyResourceService()
        resource = await service.get_resource(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
        )
        if not resource:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND, "Resource not found",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )

        return ApiResponse.success(message="Ontology resource retrieved", data=resource).to_dict()
    except RuntimeError as e:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE, str(e), code=ErrorCode.UPSTREAM_UNAVAILABLE,
        )
    except ValueError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get resource: %s", e)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR,
        )


# Internal: BFF proxies via OMSClient. Public contract: /api/v1/databases/*/ontology (plural).
@router.put("/resources/{resource_type}/{resource_id}", include_in_schema=False)
@trace_endpoint("oms.ontology_ext.update_resource")
async def update_resource(
    db_name: str,
    resource_type: str,
    resource_id: str,
    payload: OntologyResourceRequest,
    branch: str = Query(..., description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type)
        resource_id = validate_instance_id(resource_id)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)

        _ensure_branch_writable(branch)
        resolved_expected_head = await _assert_expected_head_commit(
            db_name=db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
            strict_mode=strict_compat,
        )

        sanitized = sanitize_input(_normalize_resource_payload(payload))
        sanitized["id"] = resource_id
        await validate_resource(
            db_name=db_name,
            resource_type=normalized_type,
            payload=sanitized,
            branch=branch,
            expected_head_commit=resolved_expected_head,
            strict=_resource_validation_strict(),
        )

        service = OntologyResourceService()
        if normalized_type == "value_type":
            existing = await service.get_resource(
                db_name,
                branch=branch,
                resource_type=normalized_type,
                resource_id=resource_id,
            )
            if existing:
                _validate_value_type_immutability(existing, sanitized)
        updated = await service.update_resource(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
            payload=sanitized,
        )

        return ApiResponse.success(message="Ontology resource updated", data=updated).to_dict()
    except ResourceSpecError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.ONTOLOGY_VALIDATION_FAILED,
        )
    except ResourceReferenceError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.ONTOLOGY_RELATIONSHIP_ERROR,
        )
    except RuntimeError as e:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE, str(e), code=ErrorCode.UPSTREAM_UNAVAILABLE,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except OntologyNotFoundError as e:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND, str(e), code=ErrorCode.ONTOLOGY_NOT_FOUND,
        )
    except DatabaseError as e:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT, str(e), code=ErrorCode.DB_CONSTRAINT_VIOLATION,
        )
    except Exception as e:
        logger.error("Failed to update resource: %s", e)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR,
        )


# Internal: BFF proxies via OMSClient. Public contract: /api/v1/databases/*/ontology (plural).
@router.delete("/resources/{resource_type}/{resource_id}", include_in_schema=False)
@trace_endpoint("oms.ontology_ext.delete_resource")
async def delete_resource(
    db_name: str,
    resource_type: str,
    resource_id: str,
    branch: str = Query(..., description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type)
        resource_id = validate_instance_id(resource_id)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)

        _ensure_branch_writable(branch)
        await _assert_expected_head_commit(
            db_name=db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
            strict_mode=strict_compat,
        )

        service = OntologyResourceService()
        await service.delete_resource(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
        )

        return ApiResponse.success(message="Ontology resource deleted").to_dict()
    except ValueError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except OntologyNotFoundError as e:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND, str(e), code=ErrorCode.ONTOLOGY_NOT_FOUND,
        )
    except RuntimeError as e:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE, str(e), code=ErrorCode.UPSTREAM_UNAVAILABLE,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete resource: %s", e)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR,
        )
