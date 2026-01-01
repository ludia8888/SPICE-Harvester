"""
Ontology extensions router (resources, governance, health).
"""

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field

from oms.database.postgres import db as postgres_db
from oms.dependencies import TerminusServiceDep
from oms.exceptions import DatabaseError, OntologyNotFoundError
from oms.services.async_terminus import AsyncTerminusService
from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2
from oms.services.ontology_resources import (
    OntologyResourceService,
    normalize_resource_type,
)
from oms.services.ontology_health_issue_registry import (
    ISSUE_CATALOG_VERSION,
    build_link_type_ref,
    build_object_type_ref,
    build_ontology_resource_ref,
    normalize_issue,
    normalize_issue_code,
    normalize_severity,
    REL_CLASS_LEVEL_CODES,
)
from oms.services.ontology_resource_validator import (
    ResourceReferenceError,
    ResourceSpecError,
    check_required_fields,
    collect_reference_values,
    find_missing_references,
    validate_resource,
)
from oms.services.ontology_interface_contract import (
    collect_interface_contract_issues,
)
from oms.services.pull_request_service import PullRequestService
from oms.validators.relationship_validator import RelationshipValidator, ValidationResult
from shared.models.requests import ApiResponse, BranchCreateRequest
from shared.security.input_sanitizer import (
    sanitize_input,
    validate_branch_name,
    validate_db_name,
    validate_instance_id,
)
from shared.utils.branch_utils import get_protected_branches, protected_branch_write_message
from shared.services.ontology_linter import (
    OntologyLinterConfig,
    compute_risk_score,
    lint_ontology_create,
    risk_level,
)
from shared.utils.commit_utils import coerce_commit_id
from shared.utils.id_generator import generate_simple_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}/ontology", tags=["Ontology Extensions"])


class OntologyResourceRequest(BaseModel):
    id: Optional[str] = Field(None, description="Resource id (optional; auto-generated from label)")
    label: Any = Field(..., description="Resource label")
    description: Optional[Any] = Field(None, description="Resource description")
    spec: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class OntologyProposalRequest(BaseModel):
    source_branch: str = Field(..., description="Source branch name")
    target_branch: str = Field("main", description="Target branch name")
    title: str = Field(..., description="Proposal title")
    description: Optional[str] = Field(None, description="Proposal description")
    author: str = Field("system", description="Proposal author")


class OntologyDeployRequest(BaseModel):
    proposal_id: str = Field(..., description="Proposal (pull request) id")
    ontology_commit_id: str = Field(..., description="Approved ontology commit id")
    merge_message: Optional[str] = Field(None, description="Merge message override")
    author: str = Field("system", description="Deploy author")
    definition_hash: Optional[str] = Field(None, description="Ontology definition hash")


class OntologyApproveRequest(BaseModel):
    merge_message: Optional[str] = Field(None, description="Merge message override")
    author: str = Field("system", description="Approval author")


async def _get_pr_service() -> PullRequestService:
    if not postgres_db.mvcc_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not initialized",
        )
    return PullRequestService(postgres_db.mvcc_manager)


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


def _resource_validation_strict() -> bool:
    return os.getenv("ONTOLOGY_RESOURCE_STRICT", "true").strip().lower() in {"1", "true", "yes", "on"}


def _require_health_gate(branch: str) -> bool:
    if branch not in get_protected_branches():
        return False
    return os.getenv("ONTOLOGY_REQUIRE_HEALTH_GATE", "true").strip().lower() in {"1", "true", "yes", "on"}


def _ensure_branch_writable(branch: str) -> None:
    protected = get_protected_branches()
    if branch in protected:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=protected_branch_write_message(),
        )


async def _assert_expected_head_commit(
    terminus: AsyncTerminusService,
    *,
    db_name: str,
    branch: str,
    expected_head_commit: Optional[str],
) -> Optional[str]:
    if not expected_head_commit:
        return None

    head_commit: Optional[str] = None
    branches = await terminus.version_control_service.list_branches(db_name)
    for item in branches or []:
        if isinstance(item, dict) and item.get("name") == branch:
            head_commit = coerce_commit_id(item.get("head"))
            break

    if not head_commit:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Branch '{branch}' not found",
        )

    expected = str(expected_head_commit).strip()
    if head_commit != expected:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Branch head commit mismatch",
                "branch": branch,
                "expected_head_commit": expected,
                "actual_head_commit": head_commit,
            },
        )

    return head_commit


async def _compute_ontology_health(
    *,
    db_name: str,
    branch: str,
    terminus: AsyncTerminusService,
) -> Dict[str, Any]:
    ontologies = await terminus.get_ontology(
        db_name, class_id=None, raise_if_missing=False, branch=branch
    )
    if not isinstance(ontologies, list):
        ontologies = []

    linter_config = OntologyLinterConfig.from_env(branch=branch)
    lint_reports: List[Dict[str, Any]] = []
    errors = []
    warnings = []
    infos = []

    for ontology in ontologies:
        label_value = ontology.label
        label_text = label_value if isinstance(label_value, str) else (
            label_value.get("en") if isinstance(label_value, dict) else str(label_value)
        )
        report = lint_ontology_create(
            class_id=ontology.id,
            label=label_text or ontology.id,
            abstract=bool(ontology.abstract),
            properties=list(ontology.properties or []),
            relationships=list(ontology.relationships or []),
            config=linter_config,
        )
        lint_reports.append(
            {
                "class_id": ontology.id,
                "report": report.model_dump(mode="json"),
            }
        )
        errors.extend(report.errors or [])
        warnings.extend(report.warnings or [])
        infos.extend(report.infos or [])

    validator = RelationshipValidator(existing_ontologies=ontologies)
    rel_results = validator.validate_multiple_ontologies(ontologies)
    rel_issues = [_validation_result_to_issue(r) for r in rel_results]

    resource_service = OntologyResourceService(terminus)
    resources = await resource_service.list_resources(db_name, branch=branch)

    issue_items: List[Dict[str, Any]] = []

    resource_issues = []
    referenced_ids: set[str] = set()
    for resource in resources:
        spec = resource.get("spec") or {}
        referenced_ids.update(collect_reference_values(spec))

    object_type_contracts = {
        res.get("id") for res in resources if res.get("resource_type") == "object_type"
    }
    for ontology in ontologies:
        metadata = ontology.metadata if isinstance(ontology.metadata, dict) else {}
        if metadata.get("internal") or ontology.id == "__ontology_resource":
            continue
        if ontology.id in object_type_contracts:
            continue
        resource_issues.append(
            {
                "resource_type": "object_type",
                "resource_id": ontology.id,
                "severity": "error",
                "code": "RESOURCE_OBJECT_TYPE_CONTRACT_MISSING",
                "message": f"Missing object_type contract for '{ontology.id}'",
            }
        )
        issue_items.append(
            _build_issue(
                code="RESOURCE_OBJECT_TYPE_CONTRACT_MISSING",
                severity="ERROR",
                resource_ref=build_object_type_ref(ontology.id),
                details={"object_type_id": ontology.id},
                suggested_fix=None,
                source="resource_validation",
            )
        )

    interface_index = {
        res.get("id"): res for res in resources if res.get("resource_type") == "interface"
    }

    interface_contract_issues = []
    for ontology in ontologies:
        interface_contract_issues.extend(
            collect_interface_contract_issues(
                ontology_id=ontology.id,
                metadata=ontology.metadata if isinstance(ontology.metadata, dict) else {},
                properties=list(ontology.properties or []),
                relationships=list(ontology.relationships or []),
                interface_index=interface_index,
            )
        )

    resource_issues.extend(interface_contract_issues)
    for issue in interface_contract_issues:
        issue_items.append(_normalize_issue(issue, source="interface_contract"))

    for resource in resources:
        resource_type = resource.get("resource_type")
        resource_id = resource.get("id")
        spec = resource.get("spec") or {}

        for issue in check_required_fields(resource_type, spec):
            message = issue.get("message")
            details = issue.get("details") or {}
            resource_issues.append(
                {
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "severity": "error",
                    "code": "RES001",
                    "message": message,
                }
            )
            issue_items.append(
                _build_issue(
                    code=issue.get("code"),
                    severity=None,
                    resource_ref=_resource_ref(resource_type, resource_id),
                    details=details,
                    suggested_fix=None,
                    source="resource_validation",
                )
            )

        missing = await find_missing_references(
            db_name=db_name,
            resource_type=resource_type,
            payload=resource,
            terminus=terminus,
            branch=branch,
        )
        if missing:
            resource_issues.append(
                {
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "severity": "error",
                    "code": "RES002",
                    "message": f"Missing references: {', '.join(missing)}",
                    "missing": missing,
                }
            )
            issue_items.append(
                _build_issue(
                    code="RESOURCE_MISSING_REFERENCE",
                    severity=None,
                    resource_ref=_resource_ref(resource_type, resource_id),
                    details={"missing_refs": missing},
                    suggested_fix=None,
                    source="resource_validation",
                )
            )

    unused_types = {"value_type", "interface", "shared_property"}
    for resource in resources:
        resource_type = resource.get("resource_type")
        resource_id = resource.get("id")
        if resource_type not in unused_types:
            continue
        if _resource_is_referenced(resource_type, resource_id, referenced_ids):
            continue
        issue_items.append(
            _build_issue(
                code="RESOURCE_UNUSED",
                severity=None,
                resource_ref=_resource_ref(resource_type, resource_id),
                details={},
                suggested_fix=None,
                source="resource_validation",
            )
        )

    class_ids = {ontology.id for ontology in ontologies}
    for result in rel_results:
        issue_items.append(
            _build_issue(
                code=result.code,
                severity=result.severity.value,
                resource_ref=_resolve_relationship_resource_ref(result, class_ids),
                details={"field": result.field, "related_objects": result.related_objects or []},
                suggested_fix=None,
                source="relationship_validation",
            )
        )

    for entry in lint_reports:
        report = entry.get("report") or {}
        for issue in report.get("errors") or []:
            issue_items.append(
                _build_issue(
                    code=issue.get("rule_id"),
                    severity="ERROR",
                    resource_ref=build_object_type_ref(entry.get("class_id")),
                    details=issue.get("metadata") or {},
                    suggested_fix=issue.get("suggestion"),
                    message=issue.get("message"),
                    source="lint",
                )
            )
        for issue in report.get("warnings") or []:
            issue_items.append(
                _build_issue(
                    code=issue.get("rule_id"),
                    severity="WARN",
                    resource_ref=build_object_type_ref(entry.get("class_id")),
                    details=issue.get("metadata") or {},
                    suggested_fix=issue.get("suggestion"),
                    message=issue.get("message"),
                    source="lint",
                )
            )
        for issue in report.get("infos") or []:
            issue_items.append(
                _build_issue(
                    code=issue.get("rule_id"),
                    severity="INFO",
                    resource_ref=build_object_type_ref(entry.get("class_id")),
                    details=issue.get("metadata") or {},
                    suggested_fix=issue.get("suggestion"),
                    message=issue.get("message"),
                    source="lint",
                )
            )

    summary = {
        "lint_errors": len(errors),
        "lint_warnings": len(warnings),
        "lint_infos": len(infos),
        "relationship_issues": len(rel_issues),
        "resource_issues": len(resource_issues),
        "issues": len(issue_items),
    }

    return {
        "lint_reports": lint_reports,
        "relationship_issues": rel_issues,
        "resource_issues": resource_issues,
        "issues": issue_items,
        "summary": summary,
    }


@router.get("/resources")
async def list_resources(
    db_name: str,
    resource_type: Optional[str] = Query(None, description="Resource type filter"),
    branch: str = Query("main", description="Target branch"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type) if resource_type else None

        service = OntologyResourceService(terminus)
        resources = await service.list_resources(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            limit=limit,
            offset=offset,
        )

        return ApiResponse.success(
            message="Ontology resources retrieved",
            data={"resources": resources, "total": len(resources)},
        ).to_dict()
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error("Failed to list resources: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/resources/{resource_type}")
async def list_resources_by_type(
    db_name: str,
    resource_type: str,
    branch: str = Query("main", description="Target branch"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    return await list_resources(
        db_name=db_name,
        resource_type=resource_type,
        branch=branch,
        limit=limit,
        offset=offset,
        terminus=terminus,
    )


@router.post("/resources/{resource_type}", status_code=status.HTTP_201_CREATED)
async def create_resource(
    db_name: str,
    resource_type: str,
    payload: OntologyResourceRequest,
    branch: str = Query(..., description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type)

        _ensure_branch_writable(branch)
        await _assert_expected_head_commit(
            terminus, db_name=db_name, branch=branch, expected_head_commit=expected_head_commit
        )

        sanitized = sanitize_input(_normalize_resource_payload(payload))
        await validate_resource(
            db_name=db_name,
            resource_type=normalized_type,
            payload=sanitized,
            terminus=terminus,
            branch=branch,
            expected_head_commit=expected_head_commit,
            strict=_resource_validation_strict(),
        )
        resource_id = sanitized.get("id")
        if resource_id:
            resource_id = validate_instance_id(resource_id)
        else:
            resource_id = generate_simple_id(sanitized.get("label"), default_fallback="resource")

        service = OntologyResourceService(terminus)
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except ResourceReferenceError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except DatabaseError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except Exception as e:
        logger.error("Failed to create resource: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/resources/{resource_type}/{resource_id}")
async def get_resource(
    db_name: str,
    resource_type: str,
    resource_id: str,
    branch: str = Query("main", description="Target branch"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type)
        resource_id = validate_instance_id(resource_id)

        service = OntologyResourceService(terminus)
        resource = await service.get_resource(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
        )
        if not resource:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Resource not found"
            )

        return ApiResponse.success(message="Ontology resource retrieved", data=resource).to_dict()
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get resource: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.put("/resources/{resource_type}/{resource_id}")
async def update_resource(
    db_name: str,
    resource_type: str,
    resource_id: str,
    payload: OntologyResourceRequest,
    branch: str = Query(..., description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type)
        resource_id = validate_instance_id(resource_id)

        _ensure_branch_writable(branch)
        await _assert_expected_head_commit(
            terminus, db_name=db_name, branch=branch, expected_head_commit=expected_head_commit
        )

        sanitized = sanitize_input(_normalize_resource_payload(payload))
        sanitized["id"] = resource_id
        await validate_resource(
            db_name=db_name,
            resource_type=normalized_type,
            payload=sanitized,
            terminus=terminus,
            branch=branch,
            expected_head_commit=expected_head_commit,
            strict=_resource_validation_strict(),
        )

        service = OntologyResourceService(terminus)
        updated = await service.update_resource(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
            payload=sanitized,
        )

        return ApiResponse.success(message="Ontology resource updated", data=updated).to_dict()
    except ResourceSpecError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except ResourceReferenceError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except (DatabaseError, OntologyNotFoundError) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error("Failed to update resource: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.delete("/resources/{resource_type}/{resource_id}")
async def delete_resource(
    db_name: str,
    resource_type: str,
    resource_id: str,
    branch: str = Query(..., description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        normalized_type = normalize_resource_type(resource_type)
        resource_id = validate_instance_id(resource_id)

        _ensure_branch_writable(branch)
        await _assert_expected_head_commit(
            terminus, db_name=db_name, branch=branch, expected_head_commit=expected_head_commit
        )

        service = OntologyResourceService(terminus)
        await service.delete_resource(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
        )

        return ApiResponse.success(message="Ontology resource deleted").to_dict()
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except OntologyNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete resource: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/branches")
async def list_ontology_branches(
    db_name: str,
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        branches = await terminus.list_branches(db_name)
        return ApiResponse.success(
            message="Ontology branches retrieved",
            data={"branches": branches, "total": len(branches)},
        ).to_dict()
    except Exception as e:
        logger.error("Failed to list ontology branches: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/branches", status_code=status.HTTP_201_CREATED)
async def create_ontology_branch(
    db_name: str,
    request: BranchCreateRequest,
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        sanitized = sanitize_input(request.model_dump(mode="json"))
        branch_name = validate_branch_name(sanitized.get("branch_name"))
        from_branch = sanitized.get("from_branch") or "main"
        from_branch = validate_branch_name(from_branch)

        result = await terminus.create_branch(db_name, branch_name, from_branch=from_branch)
        return ApiResponse.created(
            message="Ontology branch created",
            data=result,
        ).to_dict()
    except Exception as e:
        logger.error("Failed to create ontology branch: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/proposals")
async def list_ontology_proposals(
    db_name: str,
    status_filter: Optional[str] = Query(None, alias="status"),
    limit: int = Query(100, ge=1, le=1000),
    pr_service: PullRequestService = Depends(_get_pr_service),
):
    try:
        db_name = validate_db_name(db_name)
        proposals = await pr_service.list_pull_requests(
            db_name=db_name, status=status_filter, limit=limit
        )
        return ApiResponse.success(
            message="Ontology proposals retrieved",
            data={"proposals": proposals, "total": len(proposals)},
        ).to_dict()
    except Exception as e:
        logger.error("Failed to list ontology proposals: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/proposals", status_code=status.HTTP_201_CREATED)
async def create_ontology_proposal(
    db_name: str,
    request: OntologyProposalRequest,
    pr_service: PullRequestService = Depends(_get_pr_service),
):
    try:
        db_name = validate_db_name(db_name)
        payload = sanitize_input(request.model_dump(mode="json"))
        result = await pr_service.create_pull_request(
            db_name=db_name,
            source_branch=validate_branch_name(payload["source_branch"]),
            target_branch=validate_branch_name(payload.get("target_branch") or "main"),
            title=payload["title"],
            description=payload.get("description"),
            author=payload.get("author", "system"),
        )
        return ApiResponse.created(
            message="Ontology proposal created",
            data=result,
        ).to_dict()
    except DatabaseError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error("Failed to create ontology proposal: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/proposals/{proposal_id}/approve")
async def approve_ontology_proposal(
    db_name: str,
    proposal_id: str,
    request: OntologyApproveRequest,
    pr_service: PullRequestService = Depends(_get_pr_service),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        pr_data = await pr_service.get_pull_request(proposal_id)
        if not pr_data or pr_data.get("db_name") != db_name:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proposal not found")

        source_branch = pr_data.get("source_branch") or "main"
        target_branch = pr_data.get("target_branch") or "main"

        source_commit_id = pr_data.get("source_commit_id")
        if source_commit_id:
            head_commit = None
            branches = await terminus.version_control_service.list_branches(db_name)
            for item in branches or []:
                if isinstance(item, dict) and item.get("name") == source_branch:
                    head_commit = coerce_commit_id(item.get("head"))
                    break
            if head_commit and str(head_commit) != str(source_commit_id):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "message": "Source branch head moved since proposal creation; re-propose required",
                        "source_branch": source_branch,
                        "captured_commit_id": source_commit_id,
                        "current_head_commit": head_commit,
                    },
                )

        if _require_health_gate(target_branch):
            health = await _compute_ontology_health(
                db_name=db_name,
                branch=source_branch,
                terminus=terminus,
            )
            errors = [
                issue
                for issue in health.get("issues") or []
                if str(issue.get("severity") or "").upper() == "ERROR"
            ]
            if errors:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "message": "Ontology health gate failed",
                        "summary": health.get("summary"),
                        "errors": errors,
                    },
                )

        result = await pr_service.merge_pull_request(
            pr_id=proposal_id,
            merge_message=request.merge_message,
            author=request.author,
        )
        return ApiResponse.success(
            message="Ontology proposal approved",
            data=result,
        ).to_dict()
    except DatabaseError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to approve ontology proposal: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/deploy")
async def deploy_ontology(
    db_name: str,
    request: OntologyDeployRequest,
    pr_service: PullRequestService = Depends(_get_pr_service),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        pr_data = await pr_service.get_pull_request(request.proposal_id)
        if not pr_data or pr_data.get("db_name") != db_name:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proposal not found")

        if pr_data.get("status") != "merged":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Proposal is not approved/merged",
            )

        merge_commit_id = pr_data.get("merge_commit_id")
        if not merge_commit_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Proposal has no merge commit id",
            )

        if str(merge_commit_id) != str(request.ontology_commit_id):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Ontology commit id mismatch",
                    "proposal_merge_commit_id": merge_commit_id,
                    "requested_commit_id": request.ontology_commit_id,
                },
            )

        target_branch = pr_data.get("target_branch") or "main"
        branches = await terminus.version_control_service.list_branches(db_name)
        head_commit = None
        for item in branches or []:
            if isinstance(item, dict) and item.get("name") == target_branch:
                head_commit = coerce_commit_id(item.get("head"))
                break

        if head_commit and str(head_commit) != str(request.ontology_commit_id):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Target branch head does not match approved commit",
                    "target_branch": target_branch,
                    "target_head_commit": head_commit,
                    "approved_commit_id": request.ontology_commit_id,
                },
            )

        health_summary = None
        try:
            health = await _compute_ontology_health(
                db_name=db_name,
                branch=target_branch,
                terminus=terminus,
            )
            if isinstance(health, dict):
                health_summary = health.get("summary")
        except Exception as e:
            logger.error("Failed to compute ontology health for deploy: %s", e)

        registry = OntologyDeploymentRegistryV2()
        deployment = await registry.record_deployment(
            db_name=db_name,
            target_branch=target_branch,
            ontology_commit_id=request.ontology_commit_id,
            snapshot_rid=None,
            proposal_id=request.proposal_id,
            status="succeeded",
            gate_policy=None,
            health_summary=health_summary,
            deployed_by=request.author,
            error=None,
            metadata={
                "proposal_status": pr_data.get("status"),
                "definition_hash": request.definition_hash,
                "merge_commit_id": str(merge_commit_id),
                "source_branch": pr_data.get("source_branch") or "",
            },
        )
        return ApiResponse.success(
            message="Ontology deploy completed",
            data={
                **deployment,
                "proposal_id": request.proposal_id,
                "target_branch": target_branch,
                "ontology_commit_id": request.ontology_commit_id,
            },
        ).to_dict()
    except DatabaseError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error("Failed to deploy ontology: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/health")
async def ontology_health(
    db_name: str,
    branch: str = Query("main", description="Target branch"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        data = await _compute_ontology_health(db_name=db_name, branch=branch, terminus=terminus)

        return ApiResponse.success(
            message="Ontology health check complete",
            data={
                "summary": data["summary"],
                "lint_reports": data["lint_reports"],
                "relationship_issues": data["relationship_issues"],
                "resource_issues": data["resource_issues"],
                "issues": data["issues"],
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to compute ontology health: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


def _validation_result_to_issue(result: ValidationResult) -> Dict[str, Any]:
    return {
        "severity": result.severity.value,
        "code": result.code,
        "message": result.message,
        "field": result.field,
        "related_objects": result.related_objects or [],
    }


def _resource_is_referenced(resource_type: str, resource_id: str, references: set[str]) -> bool:
    if not resource_id:
        return False
    normalized = str(resource_id)
    prefixes = {
        "value_type": ("value_type:", "value:"),
        "interface": ("interface:",),
        "shared_property": ("shared_property:", "shared:"),
    }.get(resource_type, ())
    if normalized in references:
        return True
    for prefix in prefixes:
        if f"{prefix}{normalized}" in references:
            return True
    return False


def _resource_ref(resource_type: Optional[str], resource_id: Optional[str]) -> str:
    return build_ontology_resource_ref(resource_type, resource_id)


def _build_issue(
    *,
    code: str,
    severity: Optional[str],
    resource_ref: str,
    details: Optional[Dict[str, Any]] = None,
    suggested_fix: Optional[str] = None,
    message: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    return normalize_issue(
        code=code,
        severity=severity,
        resource_ref=resource_ref,
        details=details,
        suggested_fix=suggested_fix,
        message=message,
        source=source,
    )


def _normalize_issue(issue: Dict[str, Any], *, source: Optional[str] = None) -> Dict[str, Any]:
    code = issue.get("code") or "UNKNOWN"
    severity = issue.get("severity")
    resource_ref = (
        issue.get("resource_ref")
        or _resource_ref(issue.get("resource_type"), issue.get("resource_id"))
    )
    details = issue.get("details") or {}
    suggested_fix = issue.get("suggested_fix")
    message = issue.get("message")
    return _build_issue(
        code=code,
        severity=severity,
        resource_ref=resource_ref,
        details=details,
        suggested_fix=suggested_fix,
        message=message,
        source=source,
    )


def _resolve_relationship_resource_ref(result: ValidationResult, class_ids: set[str]) -> str:
    normalized_code = normalize_issue_code(result.code, "relationship_validation")
    if normalized_code in REL_CLASS_LEVEL_CODES:
        for value in result.related_objects or []:
            if value in class_ids:
                return build_object_type_ref(value)
    predicate = None
    for value in result.related_objects or []:
        if value and value not in class_ids:
            predicate = value
            break
    if predicate:
        return build_link_type_ref(predicate)
    return build_link_type_ref("unknown")
