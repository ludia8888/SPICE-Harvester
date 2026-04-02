"""
Pipeline execution domain logic (BFF).

Extracted from `bff.routers.pipeline_execution` to keep routers thin.
"""

from dataclasses import dataclass
import logging
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

import httpx
from fastapi import HTTPException, Request, status
from bff.routers.pipeline_ops import (
    _detect_breaking_schema_changes,
    _normalize_dependencies_payload,
    _pipeline_requires_proposal,
    _resolve_definition_commit_id,
    _resolve_output_pk_columns,
    _run_pipeline_preflight,
    _stable_definition_hash,
    _validate_dependency_targets,
)
from bff.routers.pipeline_shared import (
    _ensure_pipeline_permission,
    _log_pipeline_audit,
    _require_pipeline_idempotency_key,
    _resolve_principal,
)
from bff.services.database_role_guard import enforce_database_role_or_http_error
from bff.services.oms_client import OMSClient
from bff.services.pipeline_execution_queue import publish_build_pipeline_job, publish_preview_pipeline_job
from bff.services.ontology_occ_guard_service import (
    resolve_branch_head_commit_with_bootstrap,
)
from bff.services.input_validation_service import enforce_db_scope_or_403
from shared.dependencies.providers import AuditLogStoreDep, LineageStoreDep
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.models.lineage_edge_types import EDGE_PIPELINE_OUTPUT_STORED
from shared.models.pipeline_job import PipelineJob
from shared.models.requests import ApiResponse
from shared.security.database_access import DATA_ENGINEER_ROLES
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.pipeline.pipeline_job_queue import PipelineJobQueue
from shared.services.pipeline.pipeline_profiler import compute_column_stats
from shared.services.pipeline.pipeline_scheduler import _is_valid_cron_expression
from shared.services.pipeline.dataset_output_semantics import resolve_dataset_write_policy
from shared.services.pipeline.output_plugins import OUTPUT_KIND_DATASET, normalize_output_kind
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.dataset_registry_get_or_create import get_or_create_dataset_record
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.lakefs_client import LakeFSConflictError, LakeFSError
from shared.utils.branch_utils import protected_branch_write_message
from shared.utils.event_utils import build_command_event
from shared.utils.key_spec import normalize_key_spec
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri
from shared.utils.schema_hash import compute_schema_hash
from shared.utils.time_utils import utcnow
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _DeployRequestPayload:
    definition_json: Optional[Dict[str, Any]]
    promote_build: bool
    build_job_id: Optional[str]
    artifact_id: Optional[str]
    replay_on_deploy: bool
    dependencies_raw: Any
    node_id: Optional[str]
    db_name: str
    outputs: Optional[list[dict[str, Any]]]
    expectations: Optional[list[Any]]
    schema_contract: Optional[list[Any]]
    schedule_interval_seconds: Optional[int]
    schedule_cron: Optional[str]
    branch: Optional[str]
    proposal_status: Optional[str]
    proposal_title: Optional[str]
    proposal_description: Optional[str]


@dataclass(frozen=True)
class _DeployPipelineContext:
    pipeline: Any
    resolved_branch: str
    proposal_required: bool
    proposal_bundle: Dict[str, Any]
    latest: Any
    dependencies: Optional[list[dict[str, str]]]


@dataclass(frozen=True)
class _PromoteBuildSourceContext:
    artifact_record: Any
    build_job_id: str
    output_json: Dict[str, Any]
    build_ontology: Dict[str, Any]
    build_ontology_commit: str


@dataclass(frozen=True)
class _PromoteOutputSelection:
    execution_semantics: str
    is_streaming_promotion: bool
    selected_outputs: list[dict[str, Any]]
    staged_bucket: str
    build_ref: str


@dataclass(frozen=True)
class _PreparedPromoteOutputs:
    normalized_outputs: list[dict[str, Any]]
    staged_bucket: str
    build_ref: str


@dataclass(frozen=True)
class _PromotedOutputMaterializationContext:
    merge_commit_id: str
    staged_bucket: str
    db_name: str
    resolved_branch: str
    pipeline_id: str
    build_job_id: str
    definition_hash: str
    build_ontology_commit: str
    principal_id: str
    replay_on_deploy: bool
    artifact_id: Optional[str]
    definition_json: Dict[str, Any]


def _build_ontology_ref(branch: str) -> str:
    resolved = str(branch or "").strip() or "main"
    return f"branch:{resolved}"


def _parse_optional_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _resolve_output_contract_from_definition(
    *,
    definition_json: Dict[str, Any],
    node_id: Optional[str],
    output_name: Optional[str],
) -> Dict[str, Any]:
    resolved_node_id = str(node_id or "").strip()
    resolved_output_name = str(output_name or "").strip()
    metadata: Dict[str, Any] = {}
    raw_kind = ""

    nodes = definition_json.get("nodes") if isinstance(definition_json.get("nodes"), list) else []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "").strip().lower() != "output":
            continue
        candidate_node_id = str(node.get("id") or "").strip()
        node_meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        node_name = str(
            node_meta.get("outputName")
            or node_meta.get("datasetName")
            or node.get("title")
            or ""
        ).strip()
        if (resolved_node_id and candidate_node_id == resolved_node_id) or (
            resolved_output_name and node_name and node_name == resolved_output_name
        ):
            metadata = dict(node_meta)
            raw_kind = str(node_meta.get("outputKind") or node_meta.get("output_kind") or "").strip()
            if not resolved_output_name and node_name:
                resolved_output_name = node_name
            break

    declared_outputs = definition_json.get("outputs") if isinstance(definition_json.get("outputs"), list) else []
    for item in declared_outputs:
        if not isinstance(item, dict):
            continue
        declared_node_id = str(item.get("node_id") or item.get("nodeId") or "").strip()
        declared_name = str(
            item.get("output_name")
            or item.get("outputName")
            or item.get("dataset_name")
            or item.get("datasetName")
            or ""
        ).strip()
        if (resolved_node_id and declared_node_id and declared_node_id == resolved_node_id) or (
            resolved_output_name and declared_name and declared_name == resolved_output_name
        ):
            declared_meta = item.get("output_metadata")
            if isinstance(declared_meta, dict):
                metadata = {**declared_meta, **metadata}
            if not raw_kind:
                raw_kind = str(item.get("output_kind") or item.get("outputKind") or "").strip()
            break

    normalized_kind = normalize_output_kind(raw_kind or OUTPUT_KIND_DATASET)
    return {
        "output_kind": normalized_kind,
        "output_metadata": metadata,
        "output_name": resolved_output_name,
        "node_id": resolved_node_id,
    }


def _extract_deploy_dependencies_raw(
    *,
    sanitized: Dict[str, Any],
    definition_json: Optional[Dict[str, Any]],
) -> Any:
    if "dependencies" in sanitized:
        return sanitized.get("dependencies")
    if isinstance(definition_json, dict) and "dependencies" in definition_json:
        return definition_json.get("dependencies")
    return None


def _parse_deploy_schedule_fields(
    *,
    sanitized: Dict[str, Any],
    output: Dict[str, Any],
) -> Tuple[Optional[int], Optional[str]]:
    schedule_interval_seconds = None
    schedule_cron = None
    schedule = sanitized.get("schedule") or output.get("schedule")
    if isinstance(schedule, dict):
        schedule_interval_seconds = schedule.get("interval_seconds")
        schedule_cron = schedule.get("cron")
    elif isinstance(schedule, (int, float, str)):
        try:
            schedule_interval_seconds = int(schedule)
        except (TypeError, ValueError):
            schedule_interval_seconds = None
    if schedule_cron:
        schedule_cron = str(schedule_cron).strip()
    if schedule_interval_seconds is not None:
        try:
            schedule_interval_seconds = int(schedule_interval_seconds)
        except Exception:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "schedule_interval_seconds must be integer",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if schedule_interval_seconds <= 0:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "schedule_interval_seconds must be > 0",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
    if schedule_interval_seconds and schedule_cron:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "Provide either schedule_interval_seconds or schedule_cron (not both)",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    if schedule_cron and not _is_valid_cron_expression(str(schedule_cron)):
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "schedule_cron must be a supported 5-field cron expression",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    return schedule_interval_seconds, schedule_cron


def _parse_deploy_request_payload(*, sanitized: Dict[str, Any]) -> _DeployRequestPayload:
    output = sanitized.get("output") if isinstance(sanitized.get("output"), dict) else {}
    definition_json = (
        sanitized.get("definition_json")
        if isinstance(sanitized.get("definition_json"), dict)
        else None
    )
    schedule_interval_seconds, schedule_cron = _parse_deploy_schedule_fields(
        sanitized=sanitized,
        output=output,
    )
    return _DeployRequestPayload(
        definition_json=definition_json,
        promote_build=bool(sanitized.get("promote_build") or sanitized.get("promoteBuild") or False),
        build_job_id=str(sanitized.get("build_job_id") or sanitized.get("buildJobId") or "").strip() or None,
        artifact_id=str(sanitized.get("artifact_id") or sanitized.get("artifactId") or "").strip() or None,
        replay_on_deploy=bool(
            sanitized.get("replay")
            or sanitized.get("replay_on_deploy")
            or sanitized.get("replayOnDeploy")
            or sanitized.get("replay_on_deploy")
        ),
        dependencies_raw=_extract_deploy_dependencies_raw(
            sanitized=sanitized,
            definition_json=definition_json,
        ),
        node_id=str(sanitized.get("node_id") or "").strip() or None,
        db_name=str(output.get("db_name") or "").strip(),
        outputs=sanitized.get("outputs") if isinstance(sanitized.get("outputs"), list) else None,
        expectations=sanitized.get("expectations") if isinstance(sanitized.get("expectations"), list) else None,
        schema_contract=sanitized.get("schema_contract") if isinstance(sanitized.get("schema_contract"), list) else None,
        schedule_interval_seconds=schedule_interval_seconds,
        schedule_cron=schedule_cron,
        branch=str(sanitized.get("branch") or "").strip() or None,
        proposal_status=str(sanitized.get("proposal_status") or "").strip() or None,
        proposal_title=str(sanitized.get("proposal_title") or "").strip() or None,
        proposal_description=str(sanitized.get("proposal_description") or "").strip() or None,
    )


async def _resolve_deploy_pipeline_context(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    db_name_hint: str,
    branch: Optional[str],
    dependencies_raw: Any,
) -> _DeployPipelineContext:
    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if not pipeline:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            "Pipeline not found",
            code=ErrorCode.PIPELINE_NOT_FOUND,
        )
    if branch and branch != pipeline.branch:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "branch does not match pipeline branch",
            code=ErrorCode.CONFLICT,
        )

    dependencies: Optional[list[dict[str, str]]] = None
    if dependencies_raw is not None:
        dependencies = _normalize_dependencies_payload(dependencies_raw)
        await _validate_dependency_targets(
            pipeline_registry,
            db_name=str(db_name_hint or pipeline.db_name),
            pipeline_id=pipeline_id,
            dependencies=dependencies,
        )

    resolved_branch = branch or (pipeline.branch if pipeline else None) or "main"
    proposal_required = _pipeline_requires_proposal(resolved_branch)
    proposal_bundle = getattr(pipeline, "proposal_bundle", {}) if pipeline else {}
    if proposal_required:
        if getattr(pipeline, "proposal_status", None) != "approved":
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                protected_branch_write_message(),
                code=ErrorCode.CONFLICT,
            )
        if not proposal_bundle:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Approved proposal is missing bundle metadata",
                code=ErrorCode.CONFLICT,
            )
    latest = await pipeline_registry.get_latest_version(
        pipeline_id=pipeline_id,
        branch=resolved_branch,
    )
    return _DeployPipelineContext(
        pipeline=pipeline,
        resolved_branch=resolved_branch,
        proposal_required=proposal_required,
        proposal_bundle=proposal_bundle,
        latest=latest,
        dependencies=dependencies,
    )


def _resolve_deploy_definition_and_db(
    *,
    definition_json: Optional[Dict[str, Any]],
    latest: Any,
    expectations: Optional[list[Any]],
    schema_contract: Optional[list[Any]],
    outputs: Optional[list[dict[str, Any]]],
    db_name: str,
    pipeline: Any,
) -> Tuple[Dict[str, Any], str]:
    resolved_definition = definition_json
    if not resolved_definition:
        resolved_definition = latest.definition_json if latest else {}
    if expectations is not None:
        resolved_definition = {**(resolved_definition or {}), "expectations": expectations}
    if schema_contract is not None:
        resolved_definition = {**(resolved_definition or {}), "schemaContract": schema_contract}
    if outputs:
        resolved_definition = {**(resolved_definition or {}), "outputs": outputs}

    resolved_db_name = str(db_name or "").strip()
    if not resolved_db_name:
        resolved_db_name = str(getattr(pipeline, "db_name", "") or "").strip()
    if not resolved_db_name:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "db_name is required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    return resolved_definition or {}, validate_db_name(resolved_db_name)


def _extract_build_run_errors(output_json: Any) -> list[str]:
    errors: list[str] = []
    if isinstance(output_json, dict):
        raw_errors = output_json.get("errors")
        if isinstance(raw_errors, list):
            errors = [str(item) for item in raw_errors if str(item).strip()]
    return errors


async def _resolve_promote_artifact_record_and_job_id(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    artifact_id: Optional[str],
    build_job_id: Optional[str],
) -> Tuple[Any, str]:
    artifact_record = None
    resolved_build_job_id = str(build_job_id or "").strip() or None
    if artifact_id:
        artifact_record = await pipeline_registry.get_artifact(artifact_id=artifact_id)
        if not artifact_record:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                "Build artifact not found",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )
        if artifact_record.pipeline_id != pipeline_id:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "artifact_id does not belong to this pipeline",
                code=ErrorCode.CONFLICT,
            )
        if str(artifact_record.mode or "").lower() != "build":
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "artifact_id is not a build artifact",
                code=ErrorCode.CONFLICT,
            )
        if str(artifact_record.status or "").upper() != "SUCCESS":
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "artifact_id is not a successful build artifact",
                code=ErrorCode.CONFLICT,
            )
        if resolved_build_job_id and artifact_record.job_id != resolved_build_job_id:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "build_job_id does not match artifact_id",
                code=ErrorCode.CONFLICT,
            )
        resolved_build_job_id = resolved_build_job_id or artifact_record.job_id

    if not resolved_build_job_id:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "build_job_id is required for promote_build",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    return artifact_record, str(resolved_build_job_id)


async def _resolve_successful_build_output_json(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    build_job_id: str,
    definition_hash: str,
    artifact_record: Any,
) -> Dict[str, Any]:
    build_run = await pipeline_registry.get_run(pipeline_id=pipeline_id, job_id=build_job_id)
    if not build_run:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            "Build run not found",
            code=ErrorCode.RESOURCE_NOT_FOUND,
        )
    if str(build_run.get("mode") or "").lower() != "build":
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "build_job_id is not a build run",
            code=ErrorCode.CONFLICT,
        )
    build_status = str(build_run.get("status") or "").upper()
    if build_status != "SUCCESS":
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build is not successful yet",
            code=ErrorCode.PIPELINE_BUILD_FAILED,
            category=ErrorCategory.CONFLICT,
            extra={
                "build_status": build_status or None,
                "errors": _extract_build_run_errors(build_run.get("output_json")),
                "build_job_id": build_job_id,
            },
        )
    output_json = build_run.get("output_json") or {}
    if not isinstance(output_json, dict):
        output_json = {}
    build_hash = str(output_json.get("definition_hash") or "").strip()
    if build_hash and build_hash != definition_hash:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build definition does not match deploy definition",
            code=ErrorCode.CONFLICT,
        )
    if artifact_record and artifact_record.definition_hash and artifact_record.definition_hash != definition_hash:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build artifact definition does not match deploy definition",
            code=ErrorCode.CONFLICT,
        )
    return output_json


async def _resolve_promote_artifact_record_fallback(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    build_job_id: str,
    artifact_record: Any,
) -> Any:
    if artifact_record is not None:
        return artifact_record
    return await pipeline_registry.get_artifact_by_job(
        pipeline_id=pipeline_id,
        job_id=build_job_id,
        mode="build",
    )


def _resolve_build_ontology_from_output_json(
    *,
    output_json: Dict[str, Any],
    resolved_branch: str,
) -> Tuple[Dict[str, Any], str]:
    build_branch = str(output_json.get("branch") or "").strip() or "main"
    if build_branch != resolved_branch:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build branch does not match deploy branch",
            code=ErrorCode.CONFLICT,
        )
    build_ontology = output_json.get("ontology") if isinstance(output_json.get("ontology"), dict) else {}
    build_ontology_ref = str(build_ontology.get("ref") or "").strip() or _build_ontology_ref(build_branch)
    build_ontology_commit = str(build_ontology.get("commit") or "").strip() or build_ontology_ref
    if not build_ontology_commit:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build output is missing ontology commit id; re-run build after ontology is ready",
            code=ErrorCode.CONFLICT,
            category=ErrorCategory.CONFLICT,
        )
    return build_ontology, build_ontology_commit


async def _collect_proposal_mapping_spec_mismatches(
    *,
    objectify_registry: ObjectifyRegistry,
    proposal_bundle: Dict[str, Any],
    resolved_branch: str,
) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    mapping_specs_payload = proposal_bundle.get("mapping_specs")
    if not isinstance(mapping_specs_payload, list):
        return mismatches
    for item in mapping_specs_payload:
        if not isinstance(item, dict):
            continue
        dataset_id = str(item.get("dataset_id") or "").strip()
        dataset_branch = str(item.get("dataset_branch") or resolved_branch).strip() or resolved_branch
        target_class_id = str(item.get("target_class_id") or "").strip() or None
        expected_spec_id = str(item.get("mapping_spec_id") or "").strip()
        expected_version = item.get("mapping_spec_version")
        if not dataset_id:
            mismatches.append({"reason": "missing_dataset_id", "mapping_spec_id": expected_spec_id or None})
            continue
        current = await objectify_registry.get_active_mapping_spec(
            dataset_id=dataset_id,
            dataset_branch=dataset_branch,
            target_class_id=target_class_id,
        )
        if not current:
            mismatches.append(
                {
                    "reason": "mapping_spec_inactive",
                    "dataset_id": dataset_id,
                    "dataset_branch": dataset_branch,
                    "target_class_id": target_class_id,
                    "mapping_spec_id": expected_spec_id or None,
                }
            )
            continue
        if expected_spec_id and current.mapping_spec_id != expected_spec_id:
            mismatches.append(
                {
                    "reason": "mapping_spec_id_mismatch",
                    "dataset_id": dataset_id,
                    "dataset_branch": dataset_branch,
                    "target_class_id": target_class_id,
                    "proposal_mapping_spec_id": expected_spec_id,
                    "current_mapping_spec_id": current.mapping_spec_id,
                }
            )
        if expected_version is not None:
            try:
                expected_version_int = int(expected_version)
            except Exception:
                logging.getLogger(__name__).warning(
                    "Exception fallback at bff/services/pipeline_execution_service.py:844",
                    exc_info=True,
                )
                expected_version_int = None
            if expected_version_int is not None and expected_version_int != current.version:
                mismatches.append(
                    {
                        "reason": "mapping_spec_version_mismatch",
                        "dataset_id": dataset_id,
                        "dataset_branch": dataset_branch,
                        "target_class_id": target_class_id,
                        "proposal_version": expected_version_int,
                        "current_version": current.version,
                    }
                )
    return mismatches


def _assert_proposal_bundle_core_alignment(
    *,
    proposal_bundle: Dict[str, Any],
    resolved_build_job_id: str,
    definition_hash: str,
    artifact_record: Any,
    build_ontology_commit: str,
    output_json: Dict[str, Any],
) -> None:
    bundle_build_job_id = str(proposal_bundle.get("build_job_id") or "").strip()
    if not bundle_build_job_id:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Approved proposal is missing build_job_id",
            code=ErrorCode.CONFLICT,
        )
    if bundle_build_job_id != resolved_build_job_id:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Approved proposal build does not match deploy build",
            code=ErrorCode.CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={
                "proposal_build_job_id": bundle_build_job_id,
                "deploy_build_job_id": resolved_build_job_id,
            },
        )
    bundle_artifact_id = str(proposal_bundle.get("artifact_id") or "").strip()
    if bundle_artifact_id and artifact_record and bundle_artifact_id != artifact_record.artifact_id:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Approved proposal artifact does not match deploy artifact",
            code=ErrorCode.CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={
                "proposal_artifact_id": bundle_artifact_id,
                "deploy_artifact_id": artifact_record.artifact_id,
            },
        )
    bundle_definition_hash = str(proposal_bundle.get("definition_hash") or "").strip()
    if bundle_definition_hash and bundle_definition_hash != definition_hash:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Approved proposal definition hash does not match deploy definition",
            code=ErrorCode.CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={
                "proposal_definition_hash": bundle_definition_hash,
                "deploy_definition_hash": definition_hash,
            },
        )
    bundle_ontology_commit = str((proposal_bundle.get("ontology") or {}).get("commit") or "").strip()
    if bundle_ontology_commit and bundle_ontology_commit != build_ontology_commit:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Approved proposal ontology commit does not match build",
            code=ErrorCode.CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={
                "proposal_ontology_commit": bundle_ontology_commit,
                "build_ontology_commit": build_ontology_commit,
            },
        )
    bundle_lakefs_commit = str((proposal_bundle.get("lakefs") or {}).get("commit_id") or "").strip()
    build_lakefs_commit = str((output_json.get("lakefs") or {}).get("commit_id") or "").strip()
    if bundle_lakefs_commit and build_lakefs_commit and bundle_lakefs_commit != build_lakefs_commit:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Approved proposal lakeFS commit does not match build",
            code=ErrorCode.LAKEFS_CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={
                "proposal_commit_id": bundle_lakefs_commit,
                "build_commit_id": build_lakefs_commit,
            },
        )


async def _assert_proposal_mapping_spec_alignment(
    *,
    objectify_registry: ObjectifyRegistry,
    proposal_bundle: Dict[str, Any],
    resolved_branch: str,
) -> None:
    mismatches = await _collect_proposal_mapping_spec_mismatches(
        objectify_registry=objectify_registry,
        proposal_bundle=proposal_bundle,
        resolved_branch=resolved_branch,
    )
    if mismatches:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Approved proposal mapping specs no longer match active specs",
            code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
            category=ErrorCategory.CONFLICT,
            extra={"mismatches": mismatches},
        )


async def _validate_proposal_bundle_alignment(
    *,
    proposal_required: bool,
    proposal_bundle: Dict[str, Any],
    resolved_build_job_id: str,
    definition_hash: str,
    artifact_record: Any,
    build_ontology_commit: str,
    output_json: Dict[str, Any],
    resolved_branch: str,
    objectify_registry: ObjectifyRegistry,
) -> None:
    if not proposal_required:
        return
    _assert_proposal_bundle_core_alignment(
        proposal_bundle=proposal_bundle,
        resolved_build_job_id=resolved_build_job_id,
        definition_hash=definition_hash,
        artifact_record=artifact_record,
        build_ontology_commit=build_ontology_commit,
        output_json=output_json,
    )
    await _assert_proposal_mapping_spec_alignment(
        objectify_registry=objectify_registry,
        proposal_bundle=proposal_bundle,
        resolved_branch=resolved_branch,
    )


async def _resolve_promote_build_source_context(
    *,
    pipeline_registry: PipelineRegistry,
    objectify_registry: ObjectifyRegistry,
    pipeline_id: str,
    artifact_id: Optional[str],
    build_job_id: Optional[str],
    definition_hash: str,
    resolved_branch: str,
    proposal_required: bool,
    proposal_bundle: Dict[str, Any],
) -> _PromoteBuildSourceContext:
    artifact_record, resolved_build_job_id = await _resolve_promote_artifact_record_and_job_id(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        artifact_id=artifact_id,
        build_job_id=build_job_id,
    )
    output_json = await _resolve_successful_build_output_json(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        build_job_id=resolved_build_job_id,
        definition_hash=definition_hash,
        artifact_record=artifact_record,
    )
    artifact_record = await _resolve_promote_artifact_record_fallback(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        build_job_id=resolved_build_job_id,
        artifact_record=artifact_record,
    )
    build_ontology, build_ontology_commit = _resolve_build_ontology_from_output_json(
        output_json=output_json,
        resolved_branch=resolved_branch,
    )
    await _validate_proposal_bundle_alignment(
        proposal_required=proposal_required,
        proposal_bundle=proposal_bundle,
        resolved_build_job_id=resolved_build_job_id,
        definition_hash=definition_hash,
        artifact_record=artifact_record,
        build_ontology_commit=build_ontology_commit,
        output_json=output_json,
        resolved_branch=resolved_branch,
        objectify_registry=objectify_registry,
    )
    return _PromoteBuildSourceContext(
        artifact_record=artifact_record,
        build_job_id=str(resolved_build_job_id),
        output_json=output_json,
        build_ontology=build_ontology,
        build_ontology_commit=build_ontology_commit,
    )


@dataclass(frozen=True)
class _PipelineRunRequestPayload:
    limit: int
    definition_json: Optional[Dict[str, Any]]
    node_id: Optional[str]
    db_name: str
    expectations: Optional[list[Any]]
    schema_contract: Optional[list[Any]]
    branch: Optional[str]
    sampling_strategy: Optional[dict[str, Any]]


@dataclass(frozen=True)
class _PipelineRunContext:
    pipeline: Any
    latest: Any
    definition_json: Dict[str, Any]
    db_name: str
    node_id: Optional[str]
    requested_branch: Optional[str]


def _parse_pipeline_run_request_payload(
    *,
    sanitized: Dict[str, Any],
    allow_sampling_strategy: bool,
    default_limit: int = 200,
) -> _PipelineRunRequestPayload:
    limit = int(sanitized.get("limit") or default_limit)
    limit = max(1, min(limit, 500))
    definition_json = (
        sanitized.get("definition_json")
        if isinstance(sanitized.get("definition_json"), dict)
        else None
    )
    node_id = str(sanitized.get("node_id") or "").strip() or None
    db_name = str(sanitized.get("db_name") or "").strip()
    expectations = (
        sanitized.get("expectations")
        if isinstance(sanitized.get("expectations"), list)
        else None
    )
    schema_contract = (
        sanitized.get("schema_contract")
        if isinstance(sanitized.get("schema_contract"), list)
        else None
    )
    sampling_strategy: Optional[dict[str, Any]] = None
    if allow_sampling_strategy:
        raw_sampling_strategy = sanitized.get("sampling_strategy") or sanitized.get("samplingStrategy")
        if raw_sampling_strategy is not None and not isinstance(raw_sampling_strategy, dict):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "sampling_strategy must be an object",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if isinstance(raw_sampling_strategy, dict):
            sampling_strategy = raw_sampling_strategy
    return _PipelineRunRequestPayload(
        limit=limit,
        definition_json=definition_json,
        node_id=node_id,
        db_name=db_name,
        expectations=expectations,
        schema_contract=schema_contract,
        branch=str(sanitized.get("branch") or "").strip() or None,
        sampling_strategy=sampling_strategy,
    )


def _merge_definition_contract_overrides(
    *,
    definition_json: Optional[Dict[str, Any]],
    expectations: Optional[list[Any]],
    schema_contract: Optional[list[Any]],
) -> Dict[str, Any]:
    merged_definition = definition_json or {}
    if expectations is not None:
        merged_definition = {**merged_definition, "expectations": expectations}
    if schema_contract is not None:
        merged_definition = {**merged_definition, "schemaContract": schema_contract}
    return merged_definition


async def _resolve_pipeline_run_context(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    request_payload: _PipelineRunRequestPayload,
    validate_db: bool,
    clear_node_for_streaming_pipeline: bool,
    archived_branch_error: str,
) -> _PipelineRunContext:
    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if not pipeline:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            "Pipeline not found",
            code=ErrorCode.PIPELINE_NOT_FOUND,
        )
    node_id = request_payload.node_id
    if clear_node_for_streaming_pipeline and str(pipeline.pipeline_type or "").strip().lower() in {
        "stream",
        "streaming",
    }:
        node_id = None
    branch_state = await pipeline_registry.get_pipeline_branch(
        db_name=pipeline.db_name,
        branch=pipeline.branch,
    )
    if branch_state and branch_state.get("archived"):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            archived_branch_error,
            code=ErrorCode.CONFLICT,
        )
    if request_payload.branch and request_payload.branch != pipeline.branch:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "branch does not match pipeline branch",
            code=ErrorCode.CONFLICT,
        )
    latest = await pipeline_registry.get_latest_version(
        pipeline_id=pipeline_id,
        branch=pipeline.branch,
    )
    base_definition = request_payload.definition_json or (latest.definition_json if latest else {}) or {}
    definition_json = _merge_definition_contract_overrides(
        definition_json=base_definition,
        expectations=request_payload.expectations,
        schema_contract=request_payload.schema_contract,
    )
    db_name = request_payload.db_name or (pipeline.db_name if pipeline else "")
    if not db_name:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "db_name is required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    if validate_db:
        db_name = validate_db_name(db_name)
    return _PipelineRunContext(
        pipeline=pipeline,
        latest=latest,
        definition_json=definition_json,
        db_name=db_name,
        node_id=node_id,
        requested_branch=request_payload.branch,
    )


def _build_preview_job_id(
    *,
    pipeline_id: str,
    node_id: Optional[str],
    definition_hash: str,
) -> str:
    node_key = (node_id or "pipeline").replace("node-", "").replace("/", "-").replace(" ", "-")
    node_key = node_key[:16] if node_key else "pipeline"
    hash_key = str(definition_hash or uuid4().hex)[:12]
    return f"preview-{pipeline_id}-{hash_key}-{node_key}"


def _build_preview_existing_run_response(
    *,
    pipeline_id: str,
    job_id: str,
    limit: int,
    preflight: Dict[str, Any],
    existing_run: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not existing_run:
        return None
    status_value = str(existing_run.get("status") or "").upper()
    sample_json_existing = (
        existing_run.get("sample_json")
        if isinstance(existing_run.get("sample_json"), dict)
        else {}
    )
    if status_value in {"SUCCESS", "FAILED"} and sample_json_existing:
        return ApiResponse.success(
            message="Pipeline preview",
            data={
                "pipeline_id": pipeline_id,
                "job_id": job_id,
                "limit": limit,
                "preflight": preflight,
                "sample": sample_json_existing,
            },
        ).to_dict()
    return ApiResponse.success(
        message="Pipeline preview",
        data={
            "pipeline_id": pipeline_id,
            "job_id": job_id,
            "limit": limit,
            "preflight": preflight,
            "sample": {"queued": True, "limit": limit, "job_id": job_id},
        },
    ).to_dict()


async def _record_preview_run_queued(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    job_id: str,
    node_id: Optional[str],
    limit: int,
) -> None:
    await pipeline_registry.record_preview(
        pipeline_id=pipeline_id,
        status="QUEUED",
        row_count=0,
        sample_json={"queued": True, "limit": limit, "job_id": job_id},
        job_id=job_id,
        node_id=node_id,
    )
    await pipeline_registry.record_run(
        pipeline_id=pipeline_id,
        job_id=job_id,
        mode="preview",
        status="QUEUED",
        node_id=node_id,
        sample_json={"queued": True, "limit": limit},
    )


def _build_preview_definition(
    *,
    definition_json: Dict[str, Any],
    branch: Optional[str],
    preview_limit: int,
    sampling_strategy: Optional[dict[str, Any]],
) -> Dict[str, Any]:
    preview_definition = dict(definition_json)
    preview_meta = dict(preview_definition.get("__preview_meta__") or {})
    preview_meta["branch"] = branch or "main"
    preview_meta["sample_limit"] = preview_limit
    preview_meta["max_output_rows"] = preview_limit
    preview_meta["sample_based_execution"] = True
    preview_meta["skip_production_checks"] = True
    preview_meta["skip_output_recording"] = True
    if sampling_strategy:
        preview_meta["sampling_strategy"] = sampling_strategy
    preview_definition["__preview_meta__"] = preview_meta
    return preview_definition


async def _append_preview_command_event(
    *,
    event_store: Any,
    pipeline_id: str,
    limit: int,
) -> None:
    event = build_command_event(
        event_type="PIPELINE_PREVIEWED",
        aggregate_type="Pipeline",
        aggregate_id=pipeline_id,
        data={"pipeline_id": pipeline_id, "row_limit": limit},
        command_type="RUN_PIPELINE_PREVIEW",
    )
    try:
        await event_store.connect()
        await event_store.append_event(event)
    except Exception as exc:
        logger.warning("Failed to append pipeline preview event: %s", exc)


def _assert_build_preflight_passes(preflight: Dict[str, Any]) -> None:
    if preflight.get("has_blocking_errors"):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            {
                "message": "Pipeline preflight checks failed",
                "category": ErrorCategory.CONFLICT.value,
                "preflight": preflight,
            },
            code=ErrorCode.PIPELINE_PREFLIGHT_FAILED,
        )


async def _resolve_build_ontology_head_commit(
    *,
    oms_client: OMSClient,
    db_name: str,
    ontology_branch: str,
) -> str:
    ontology_head_commit_id: Optional[str] = None
    try:
        ontology_head_commit_id = await resolve_branch_head_commit_with_bootstrap(
            oms_client=oms_client,
            db_name=db_name,
            branch=ontology_branch,
            source_branch="main",
            max_attempts=5,
            initial_backoff_seconds=0.15,
            max_backoff_seconds=1.0,
            warning_logger=logger,
        )
    except httpx.HTTPStatusError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        logger.warning(
            "Failed to resolve ontology head commit (db=%s branch=%s http=%s): %s",
            db_name,
            ontology_branch,
            status_code,
            exc,
        )
    except Exception as exc:
        logger.warning(
            "Failed to resolve ontology head commit (db=%s branch=%s): %s",
            db_name,
            ontology_branch,
            exc,
        )
    return ontology_head_commit_id or _build_ontology_ref(ontology_branch)


def _build_definition_with_ontology_meta(
    *,
    definition_json: Dict[str, Any],
    ontology_branch: str,
    ontology_head_commit_id: str,
) -> Dict[str, Any]:
    build_definition = dict(definition_json)
    build_definition["__build_meta__"] = {
        "branch": ontology_branch,
        "ontology": {
            "ref": _build_ontology_ref(ontology_branch),
            "branch": ontology_branch,
            "commit": ontology_head_commit_id,
        },
    }
    return build_definition


async def _emit_build_requested_event(
    *,
    emit_pipeline_control_plane_event: Any,
    pipeline_id: str,
    job_id: str,
    db_name: str,
    branch: str,
    node_id: Optional[str],
    definition_hash: str,
    limit: int,
    principal_type: str,
    principal_id: str,
) -> None:
    await emit_pipeline_control_plane_event(
        event_type="PIPELINE_BUILD_REQUESTED",
        pipeline_id=pipeline_id,
        event_id=f"build-requested-{job_id}",
        actor=principal_id,
        data={
            "pipeline_id": pipeline_id,
            "job_id": job_id,
            "db_name": db_name,
            "branch": branch,
            "node_id": node_id,
            "definition_hash": definition_hash,
            "limit": limit,
            "principal_type": principal_type,
            "principal_id": principal_id,
        },
    )


@dataclass(frozen=True)
class _PreparedPreviewExecution:
    request_payload: _PipelineRunRequestPayload
    run_context: _PipelineRunContext
    preflight: Dict[str, Any]
    definition_hash: str
    definition_commit_id: str
    job_id: str


@dataclass(frozen=True)
class _PreparedBuildExecution:
    request_payload: _PipelineRunRequestPayload
    run_context: _PipelineRunContext
    preflight: Dict[str, Any]
    definition_hash: str
    definition_commit_id: str
    job_id: str
    ontology_branch: str
    ontology_head_commit_id: str


async def _prepare_preview_execution(
    *,
    pipeline_id: str,
    sanitized: Dict[str, Any],
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
) -> _PreparedPreviewExecution:
    request_payload = _parse_pipeline_run_request_payload(
        sanitized=sanitized,
        allow_sampling_strategy=True,
        default_limit=500,
    )
    run_context = await _resolve_pipeline_run_context(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        request_payload=request_payload,
        validate_db=False,
        clear_node_for_streaming_pipeline=False,
        archived_branch_error="Archived branch cannot be built; restore the branch first",
    )
    preflight = await _run_pipeline_preflight(
        definition_json=run_context.definition_json,
        db_name=run_context.db_name,
        branch=request_payload.branch or run_context.pipeline.branch,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
    )
    definition_hash = _stable_definition_hash(run_context.definition_json)
    definition_commit_id = _resolve_definition_commit_id(
        run_context.definition_json,
        run_context.latest,
        definition_hash,
    )
    job_id = _build_preview_job_id(
        pipeline_id=pipeline_id,
        node_id=run_context.node_id,
        definition_hash=definition_hash,
    )
    return _PreparedPreviewExecution(
        request_payload=request_payload,
        run_context=run_context,
        preflight=preflight,
        definition_hash=definition_hash,
        definition_commit_id=definition_commit_id,
        job_id=job_id,
    )


async def _dispatch_preview_execution(
    *,
    pipeline_id: str,
    prepared: _PreparedPreviewExecution,
    pipeline_registry: PipelineRegistry,
    pipeline_job_queue: PipelineJobQueue,
    event_store: Any,
) -> Dict[str, Any]:
    existing_run = await pipeline_registry.get_run(
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
    )
    existing_response = _build_preview_existing_run_response(
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
        limit=prepared.request_payload.limit,
        preflight=prepared.preflight,
        existing_run=existing_run,
    )
    if existing_response is not None:
        return existing_response
    await _record_preview_run_queued(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
        node_id=prepared.run_context.node_id,
        limit=prepared.request_payload.limit,
    )
    preview_definition = _build_preview_definition(
        definition_json=prepared.run_context.definition_json,
        branch=prepared.request_payload.branch,
        preview_limit=prepared.request_payload.limit,
        sampling_strategy=prepared.request_payload.sampling_strategy,
    )
    job = PipelineJob(
        job_id=prepared.job_id,
        pipeline_id=pipeline_id,
        db_name=prepared.run_context.db_name,
        pipeline_type=prepared.run_context.pipeline.pipeline_type if prepared.run_context.pipeline else "batch",
        definition_json=preview_definition,
        definition_hash=prepared.definition_hash,
        definition_commit_id=prepared.definition_commit_id,
        node_id=prepared.run_context.node_id,
        output_dataset_name="preview_output",
        mode="preview",
        preview_limit=prepared.request_payload.limit,
        branch=prepared.run_context.pipeline.branch,
    )
    await publish_preview_pipeline_job(
        pipeline_job_queue=pipeline_job_queue,
        pipeline_registry=pipeline_registry,
        job=job,
        pipeline_id=pipeline_id,
        node_id=prepared.run_context.node_id,
    )
    await _append_preview_command_event(
        event_store=event_store,
        pipeline_id=pipeline_id,
        limit=prepared.request_payload.limit,
    )
    return ApiResponse.success(
        message="Preview queued",
        data={
            "pipeline_id": pipeline_id,
            "job_id": prepared.job_id,
            "limit": prepared.request_payload.limit,
            "preflight": prepared.preflight,
            "sample": {"queued": True, "job_id": prepared.job_id},
        },
    ).to_dict()


async def _prepare_build_execution(
    *,
    pipeline_id: str,
    sanitized: Dict[str, Any],
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    oms_client: OMSClient,
) -> _PreparedBuildExecution:
    request_payload = _parse_pipeline_run_request_payload(
        sanitized=sanitized,
        allow_sampling_strategy=False,
    )
    run_context = await _resolve_pipeline_run_context(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        request_payload=request_payload,
        validate_db=True,
        clear_node_for_streaming_pipeline=True,
        archived_branch_error="Archived branch cannot be deployed; restore the branch first",
    )
    preflight = await _run_pipeline_preflight(
        definition_json=run_context.definition_json,
        db_name=run_context.db_name,
        branch=request_payload.branch or run_context.pipeline.branch,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
    )
    _assert_build_preflight_passes(preflight)
    definition_hash = _stable_definition_hash(run_context.definition_json)
    definition_commit_id = _resolve_definition_commit_id(
        run_context.definition_json,
        run_context.latest,
        definition_hash,
    )
    ontology_branch = request_payload.branch or run_context.pipeline.branch or "main"
    ontology_head_commit_id = await _resolve_build_ontology_head_commit(
        oms_client=oms_client,
        db_name=run_context.db_name,
        ontology_branch=ontology_branch,
    )
    return _PreparedBuildExecution(
        request_payload=request_payload,
        run_context=run_context,
        preflight=preflight,
        definition_hash=definition_hash,
        definition_commit_id=definition_commit_id,
        job_id=f"build-{pipeline_id}-{uuid4()}",
        ontology_branch=ontology_branch,
        ontology_head_commit_id=ontology_head_commit_id,
    )


async def _dispatch_build_execution(
    *,
    pipeline_id: str,
    prepared: _PreparedBuildExecution,
    pipeline_registry: PipelineRegistry,
    pipeline_job_queue: PipelineJobQueue,
    emit_pipeline_control_plane_event: Any,
    request: Request | Any,
) -> Dict[str, Any]:
    await pipeline_registry.record_run(
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
        mode="build",
        status="QUEUED",
        node_id=prepared.run_context.node_id,
        output_json={"queued": True, "limit": prepared.request_payload.limit},
    )
    build_definition = _build_definition_with_ontology_meta(
        definition_json=prepared.run_context.definition_json,
        ontology_branch=prepared.ontology_branch,
        ontology_head_commit_id=prepared.ontology_head_commit_id,
    )
    job = PipelineJob(
        job_id=prepared.job_id,
        pipeline_id=pipeline_id,
        db_name=prepared.run_context.db_name,
        pipeline_type=prepared.run_context.pipeline.pipeline_type if prepared.run_context.pipeline else "batch",
        definition_json=build_definition,
        definition_hash=prepared.definition_hash,
        definition_commit_id=prepared.definition_commit_id,
        node_id=prepared.run_context.node_id,
        output_dataset_name="build_output",
        mode="build",
        preview_limit=prepared.request_payload.limit,
        branch=prepared.run_context.pipeline.branch,
    )
    await publish_build_pipeline_job(
        pipeline_job_queue=pipeline_job_queue,
        pipeline_registry=pipeline_registry,
        job=job,
        pipeline_id=pipeline_id,
        node_id=prepared.run_context.node_id,
    )
    principal_type, principal_id = _resolve_principal(request)
    await _emit_build_requested_event(
        emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
        db_name=prepared.run_context.db_name,
        branch=prepared.run_context.pipeline.branch,
        node_id=prepared.run_context.node_id,
        definition_hash=prepared.definition_hash,
        limit=prepared.request_payload.limit,
        principal_type=principal_type,
        principal_id=principal_id,
    )
    return ApiResponse.success(
        message="Build queued",
        data={
            "pipeline_id": pipeline_id,
            "job_id": prepared.job_id,
            "limit": prepared.request_payload.limit,
            "preflight": prepared.preflight,
        },
    ).to_dict()


async def _record_preview_validation_failure(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    node_id: Optional[str],
    error_detail: str,
) -> None:
    validation_payload = build_error_envelope(
        service_name="bff",
        message="Pipeline preview validation failed",
        detail=error_detail,
        code=ErrorCode.REQUEST_VALIDATION_FAILED,
        category=ErrorCategory.INPUT,
        status_code=422,
        errors=[error_detail],
        context={"pipeline_id": pipeline_id, "node_id": node_id, "mode": "preview"},
        external_code="PIPELINE_DEFINITION_INVALID",
    )
    await pipeline_registry.record_preview(
        pipeline_id=pipeline_id,
        status="FAILED",
        row_count=0,
        sample_json=validation_payload,
        node_id=node_id,
    )
    await pipeline_registry.record_run(
        pipeline_id=pipeline_id,
        job_id=f"preview-{pipeline_id}-invalid",
        mode="preview",
        status="FAILED",
        node_id=node_id,
        sample_json=validation_payload,
        finished_at=utcnow(),
    )


async def _log_pipeline_preview_failure(
    *,
    audit_store: AuditLogStoreDep,
    request: Request | Any,
    pipeline_id: str,
    error_detail: str,
) -> None:
    await _log_pipeline_audit(
        audit_store,
        request=request,
        action="PIPELINE_PREVIEW_REQUESTED",
        status="failure",
        pipeline_id=pipeline_id,
        error=error_detail,
    )
    logger.error("Failed to preview pipeline: %s", error_detail)


@trace_external_call("bff.pipeline_execution.preview_pipeline")
async def preview_pipeline(
    *,
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request | Any = None,
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry,
    pipeline_job_queue: PipelineJobQueue,
    dataset_registry: DatasetRegistry,
    event_store: Any,
) -> ApiResponse:
    node_id: Optional[str] = None
    prepared: Optional[_PreparedPreviewExecution] = None
    try:
        _require_pipeline_idempotency_key(request, operation="pipeline preview")
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        prepared = await _prepare_preview_execution(
            pipeline_id=pipeline_id,
            sanitized=sanitize_input(payload),
            pipeline_registry=pipeline_registry,
            dataset_registry=dataset_registry,
        )
        if request is not None:
            enforce_db_scope_or_403(request, db_name=prepared.run_context.db_name)
            await enforce_database_role_or_http_error(
                headers=request.headers,
                db_name=prepared.run_context.db_name,
                required_roles=DATA_ENGINEER_ROLES,
                allow_if_registry_unavailable=True,
            )
        node_id = prepared.run_context.node_id
        response = await _dispatch_preview_execution(
            pipeline_id=pipeline_id,
            prepared=prepared,
            pipeline_registry=pipeline_registry,
            pipeline_job_queue=pipeline_job_queue,
            event_store=event_store,
        )
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PREVIEW_REQUESTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={
                "job_id": prepared.job_id,
                "node_id": node_id,
                "limit": prepared.request_payload.limit,
            },
        )
        return response
    except ValueError as e:
        await _record_preview_validation_failure(
            pipeline_id=pipeline_id,
            pipeline_registry=pipeline_registry,
            node_id=node_id,
            error_detail=str(e),
        )
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            str(e),
            code=ErrorCode.PIPELINE_VALIDATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_preview_failure(
            audit_store=audit_store,
            request=request,
            pipeline_id=pipeline_id,
            error_detail=str(e),
        )
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            str(e),
            code=ErrorCode.INTERNAL_ERROR,
        )


@trace_external_call("bff.pipeline_execution.build_pipeline")
async def build_pipeline(
    *,
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request | Any = None,
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry,
    pipeline_job_queue: PipelineJobQueue,
    dataset_registry: DatasetRegistry,
    oms_client: OMSClient,
    emit_pipeline_control_plane_event: Any,
) -> ApiResponse:
    prepared: Optional[_PreparedBuildExecution] = None
    try:
        _require_pipeline_idempotency_key(request, operation="pipeline build")
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        prepared = await _prepare_build_execution(
            pipeline_id=pipeline_id,
            sanitized=sanitize_input(payload),
            pipeline_registry=pipeline_registry,
            dataset_registry=dataset_registry,
            oms_client=oms_client,
        )
        if request is not None:
            enforce_db_scope_or_403(request, db_name=prepared.run_context.db_name)
            await enforce_database_role_or_http_error(
                headers=request.headers,
                db_name=prepared.run_context.db_name,
                required_roles=DATA_ENGINEER_ROLES,
                allow_if_registry_unavailable=True,
            )
        response = await _dispatch_build_execution(
            pipeline_id=pipeline_id,
            prepared=prepared,
            pipeline_registry=pipeline_registry,
            pipeline_job_queue=pipeline_job_queue,
            emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
            request=request,
        )
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BUILD_REQUESTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={
                "job_id": prepared.job_id,
                "node_id": prepared.run_context.node_id,
                "limit": prepared.request_payload.limit,
            },
        )
        return response
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BUILD_REQUESTED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to build pipeline: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


def _extract_promote_outputs_list(
    *,
    artifact_record: Any,
    output_json: Dict[str, Any],
) -> list[dict[str, Any]]:
    outputs_payload = None
    if artifact_record and artifact_record.outputs:
        outputs_payload = artifact_record.outputs
    if outputs_payload is None:
        outputs_payload = output_json.get("outputs")
    if not isinstance(outputs_payload, list):
        return []
    return [item for item in outputs_payload if isinstance(item, dict)]


def _select_outputs_for_promotion(
    *,
    outputs_list: list[dict[str, Any]],
    node_id: Optional[str],
    is_streaming_promotion: bool,
) -> list[dict[str, Any]]:
    if is_streaming_promotion:
        if not outputs_list:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Build outputs are missing",
                code=ErrorCode.PIPELINE_BUILD_FAILED,
            )
        return outputs_list

    selected_output = None
    for item in outputs_list:
        if str(item.get("node_id") or "").strip() == str(node_id).strip():
            selected_output = item
            break
    if not selected_output:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build output for node_id not found",
            code=ErrorCode.PIPELINE_BUILD_FAILED,
        )
    return [selected_output]


def _resolve_build_artifact_context(
    *,
    selected_outputs: list[dict[str, Any]],
    output_json: Dict[str, Any],
    artifact_record: Any,
) -> tuple[str, str]:
    first_artifact_key = str(selected_outputs[0].get("artifact_key") or "").strip()
    if not first_artifact_key:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build staged artifact_key is missing",
            code=ErrorCode.PIPELINE_BUILD_FAILED,
        )
    parsed_first = parse_s3_uri(first_artifact_key)
    if not parsed_first:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build artifact_key is not a valid s3:// URI",
            code=ErrorCode.PIPELINE_BUILD_FAILED,
        )
    staged_bucket, _ = parsed_first

    lakefs_meta = output_json.get("lakefs") if isinstance(output_json.get("lakefs"), dict) else {}
    if artifact_record:
        if artifact_record.lakefs_repository:
            lakefs_meta.setdefault("repository", artifact_record.lakefs_repository)
        if artifact_record.lakefs_branch:
            lakefs_meta.setdefault("build_branch", artifact_record.lakefs_branch)
        if artifact_record.lakefs_commit_id:
            lakefs_meta.setdefault("commit_id", artifact_record.lakefs_commit_id)

    expected_repo = str(lakefs_meta.get("repository") or "").strip()
    expected_build_branch = str(lakefs_meta.get("build_branch") or "").strip()
    if expected_repo and expected_repo != staged_bucket:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build artifact_key repository does not match build metadata",
            code=ErrorCode.CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={
                "artifact_bucket": staged_bucket,
                "expected_repository": expected_repo,
            },
        )
    if not expected_build_branch:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build output is missing lakefs.build_branch",
            code=ErrorCode.PIPELINE_BUILD_FAILED,
            category=ErrorCategory.CONFLICT,
        )
    return staged_bucket, expected_build_branch


def _resolve_promote_output_selection(
    *,
    output_json: Dict[str, Any],
    artifact_record: Any,
    pipeline: Any,
    node_id: Optional[str],
) -> _PromoteOutputSelection:
    outputs_list = _extract_promote_outputs_list(
        artifact_record=artifact_record,
        output_json=output_json,
    )
    execution_semantics = str(output_json.get("execution_semantics") or "").strip().lower()
    is_streaming_promotion = execution_semantics == "streaming" or str(pipeline.pipeline_type or "").strip().lower() in {
        "stream",
        "streaming",
    }
    if not node_id and not is_streaming_promotion:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "node_id is required for promote_build",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    selected_outputs = _select_outputs_for_promotion(
        outputs_list=outputs_list,
        node_id=node_id,
        is_streaming_promotion=is_streaming_promotion,
    )
    staged_bucket, build_ref = _resolve_build_artifact_context(
        selected_outputs=selected_outputs,
        output_json=output_json,
        artifact_record=artifact_record,
    )
    return _PromoteOutputSelection(
        execution_semantics=execution_semantics,
        is_streaming_promotion=is_streaming_promotion,
        selected_outputs=selected_outputs,
        staged_bucket=staged_bucket,
        build_ref=build_ref,
    )


def _coerce_optional_int(value: Any, *, field_name: str) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except Exception:
        logger.warning("Failed to parse %s as integer in promote output payload", field_name, exc_info=True)
        return None


def _extract_staged_dataset_name(item: dict[str, Any]) -> str:
    staged_dataset_name = str(item.get("dataset_name") or item.get("datasetName") or "").strip()
    if staged_dataset_name:
        return staged_dataset_name
    raise classified_http_exception(
        status.HTTP_409_CONFLICT,
        "Build dataset_name is missing",
        code=ErrorCode.PIPELINE_BUILD_FAILED,
    )


def _extract_artifact_path_from_output(
    *,
    item: dict[str, Any],
    staged_bucket: str,
    staged_prefix: str,
) -> tuple[str, str]:
    staged_artifact_key = str(item.get("artifact_key") or "").strip()
    if not staged_artifact_key:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build staged artifact_key is missing",
            code=ErrorCode.PIPELINE_BUILD_FAILED,
        )
    parsed = parse_s3_uri(staged_artifact_key)
    if not parsed:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build artifact_key is not a valid s3:// URI",
            code=ErrorCode.PIPELINE_BUILD_FAILED,
        )
    bucket, key = parsed
    if bucket != staged_bucket:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build outputs must use a single lakeFS repository",
            code=ErrorCode.CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={"artifact_bucket": bucket, "expected_repository": staged_bucket},
        )
    if not key.startswith(staged_prefix):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build artifact_key does not match build branch metadata",
            code=ErrorCode.PIPELINE_BUILD_FAILED,
            category=ErrorCategory.CONFLICT,
            extra={
                "artifact_key": staged_artifact_key,
                "artifact_path": key,
                "expected_build_branch": staged_prefix[:-1],
            },
        )
    artifact_path = key[len(staged_prefix) :]
    if artifact_path:
        return staged_artifact_key, artifact_path
    raise classified_http_exception(
        status.HTTP_409_CONFLICT,
        "Build artifact_key is missing artifact path after build ref",
        code=ErrorCode.PIPELINE_BUILD_FAILED,
        category=ErrorCategory.CONFLICT,
        extra={
            "artifact_key": staged_artifact_key,
            "expected_build_branch": staged_prefix[:-1],
        },
    )


def _extract_promote_output_runtime_fields(item: dict[str, Any]) -> dict[str, Any]:
    schema_columns = item.get("columns") if isinstance(item.get("columns"), list) else []
    sample_rows = item.get("rows") if isinstance(item.get("rows"), list) else []
    normalized_sample_rows = [row for row in sample_rows if isinstance(row, dict)]
    row_count_int = _coerce_optional_int(item.get("row_count"), field_name="row_count")
    delta_row_count_raw = item.get("delta_row_count") if "delta_row_count" in item else item.get("deltaRowCount")
    delta_row_count_int = _coerce_optional_int(delta_row_count_raw, field_name="delta_row_count")
    try:
        output_kind = normalize_output_kind(
            item.get("output_kind") or item.get("outputKind") or OUTPUT_KIND_DATASET
        )
    except ValueError as exc:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build output has invalid output_kind",
            code=ErrorCode.PIPELINE_BUILD_FAILED,
            category=ErrorCategory.CONFLICT,
            extra={"output_kind": item.get("output_kind"), "reason": str(exc)},
        ) from exc
    pk_columns_raw = item.get("pk_columns") if isinstance(item.get("pk_columns"), list) else item.get("pkColumns")
    return {
        "output_kind": output_kind,
        "output_metadata": item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
        "columns": schema_columns,
        "rows": normalized_sample_rows,
        "row_count": row_count_int,
        "delta_row_count": delta_row_count_int,
        "write_mode_requested": str(item.get("write_mode_requested") or item.get("writeModeRequested") or "").strip() or None,
        "write_mode_resolved": str(item.get("write_mode_resolved") or item.get("writeModeResolved") or "").strip() or None,
        "runtime_write_mode": str(item.get("runtime_write_mode") or item.get("runtimeWriteMode") or "").strip() or None,
        "write_policy_hash": str(item.get("write_policy_hash") or item.get("writePolicyHash") or "").strip() or None,
        "pk_columns": [str(col).strip() for col in (pk_columns_raw or []) if str(col).strip()],
        "post_filtering_column": str(item.get("post_filtering_column") or item.get("postFilteringColumn") or "").strip() or None,
        "has_incremental_input": _parse_optional_bool(
            item.get("has_incremental_input") if "has_incremental_input" in item else item.get("hasIncrementalInput")
        ),
        "incremental_inputs_have_additive_updates": _parse_optional_bool(
            item.get("incremental_inputs_have_additive_updates")
            if "incremental_inputs_have_additive_updates" in item
            else item.get("incrementalInputsHaveAdditiveUpdates")
        ),
    }


def _normalize_promote_output_item(
    *,
    item: dict[str, Any],
    staged_bucket: str,
    staged_prefix: str,
) -> dict[str, Any]:
    staged_dataset_name = _extract_staged_dataset_name(item)
    staged_artifact_key, artifact_path = _extract_artifact_path_from_output(
        item=item,
        staged_bucket=staged_bucket,
        staged_prefix=staged_prefix,
    )
    runtime_fields = _extract_promote_output_runtime_fields(item)
    return {
        "node_id": item.get("node_id"),
        "output_name": item.get("output_name") or item.get("outputName") or staged_dataset_name,
        "output_kind": runtime_fields.get("output_kind"),
        "output_metadata": runtime_fields.get("output_metadata"),
        "dataset_name": staged_dataset_name,
        "build_artifact_key": staged_artifact_key,
        "artifact_path": artifact_path,
        "columns": runtime_fields.get("columns") or [],
        "rows": runtime_fields.get("rows") or [],
        "row_count": runtime_fields.get("row_count"),
        "delta_row_count": runtime_fields.get("delta_row_count"),
        "write_mode_requested": runtime_fields.get("write_mode_requested"),
        "write_mode_resolved": runtime_fields.get("write_mode_resolved"),
        "runtime_write_mode": runtime_fields.get("runtime_write_mode"),
        "pk_columns": runtime_fields.get("pk_columns") or [],
        "post_filtering_column": runtime_fields.get("post_filtering_column"),
        "write_policy_hash": runtime_fields.get("write_policy_hash"),
        "has_incremental_input": runtime_fields.get("has_incremental_input"),
        "incremental_inputs_have_additive_updates": runtime_fields.get("incremental_inputs_have_additive_updates"),
    }


async def _collect_output_breaking_changes(
    *,
    dataset_registry: DatasetRegistry,
    db_name: str,
    resolved_branch: str,
    normalized_output: dict[str, Any],
) -> list[dict[str, str]]:
    dataset = await dataset_registry.get_dataset_by_name(
        db_name=db_name,
        name=str(normalized_output.get("dataset_name") or ""),
        branch=resolved_branch,
    )
    if not dataset:
        return []
    return _detect_breaking_schema_changes(
        previous_schema=dataset.schema_json,
        next_columns=normalized_output.get("columns") if isinstance(normalized_output.get("columns"), list) else [],
    )


async def _normalize_promote_outputs(
    *,
    selected_outputs: list[dict[str, Any]],
    staged_bucket: str,
    build_ref: str,
    dataset_registry: DatasetRegistry,
    db_name: str,
    resolved_branch: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    normalized_outputs: list[dict[str, Any]] = []
    any_breaking_changes: list[dict[str, Any]] = []
    staged_prefix = f"{build_ref}/"
    for item in selected_outputs:
        normalized_output = _normalize_promote_output_item(
            item=item,
            staged_bucket=staged_bucket,
            staged_prefix=staged_prefix,
        )
        breaking_changes = await _collect_output_breaking_changes(
            dataset_registry=dataset_registry,
            db_name=db_name,
            resolved_branch=resolved_branch,
            normalized_output=normalized_output,
        )
        if breaking_changes:
            any_breaking_changes.append(
                {
                    "dataset_name": normalized_output.get("dataset_name"),
                    "node_id": normalized_output.get("node_id"),
                    "breaking_changes": breaking_changes,
                }
            )
        normalized_output["breaking_changes"] = breaking_changes
        normalized_outputs.append(normalized_output)
    return normalized_outputs, any_breaking_changes


def _collect_policy_mismatches(
    *,
    normalized_outputs: list[dict[str, Any]],
    definition_json: Dict[str, Any],
    execution_semantics: str,
) -> list[dict[str, Any]]:
    policy_mismatches: list[dict[str, Any]] = []
    for item in normalized_outputs:
        if str(item.get("output_kind") or "").strip().lower() != OUTPUT_KIND_DATASET:
            continue
        try:
            output_contract = _resolve_output_contract_from_definition(
                definition_json=definition_json or {},
                node_id=str(item.get("node_id") or "").strip() or None,
                output_name=str(item.get("output_name") or "").strip() or None,
            )
        except ValueError as exc:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Deploy definition has invalid output kind",
                code=ErrorCode.CONFLICT,
                category=ErrorCategory.CONFLICT,
                extra={"reason": str(exc), "node_id": item.get("node_id")},
            ) from exc
        if str(output_contract.get("output_kind") or "").strip().lower() != OUTPUT_KIND_DATASET:
            continue
        expected_policy = resolve_dataset_write_policy(
            definition=definition_json or {},
            output_metadata=output_contract.get("output_metadata") if isinstance(output_contract.get("output_metadata"), dict) else {},
            execution_semantics=execution_semantics,
            has_incremental_input=_parse_optional_bool(item.get("has_incremental_input")),
            incremental_inputs_have_additive_updates=_parse_optional_bool(
                item.get("incremental_inputs_have_additive_updates")
            ),
        )
        observed_hash = str(item.get("write_policy_hash") or "").strip()
        observed_resolved = str(item.get("write_mode_resolved") or "").strip()
        observed_runtime = str(item.get("runtime_write_mode") or "").strip()
        observed_pk = [str(col).strip() for col in (item.get("pk_columns") or []) if str(col).strip()]

        mismatch: Dict[str, Any] = {}
        if observed_hash and observed_hash != expected_policy.policy_hash:
            mismatch["write_policy_hash"] = {"observed": observed_hash, "expected": expected_policy.policy_hash}
        if observed_resolved and observed_resolved != expected_policy.resolved_write_mode.value:
            mismatch["write_mode_resolved"] = {
                "observed": observed_resolved,
                "expected": expected_policy.resolved_write_mode.value,
            }
        if observed_runtime and observed_runtime != expected_policy.runtime_write_mode:
            mismatch["runtime_write_mode"] = {
                "observed": observed_runtime,
                "expected": expected_policy.runtime_write_mode,
            }
        if observed_pk and observed_pk != list(expected_policy.primary_key_columns):
            mismatch["pk_columns"] = {
                "observed": observed_pk,
                "expected": list(expected_policy.primary_key_columns),
            }
        if mismatch:
            policy_mismatches.append(
                {
                    "dataset_name": item.get("dataset_name"),
                    "node_id": item.get("node_id"),
                    "mismatch": mismatch,
                }
            )
    return policy_mismatches


def _assert_breaking_changes_replay_policy(
    *,
    any_breaking_changes: list[dict[str, Any]],
    replay_on_deploy: bool,
    is_streaming_promotion: bool,
    normalized_outputs: list[dict[str, Any]],
) -> None:
    if not any_breaking_changes or replay_on_deploy:
        return
    breaking_payload: Any = any_breaking_changes
    if not is_streaming_promotion and normalized_outputs:
        breaking_payload = normalized_outputs[0].get("breaking_changes") or []
    raise classified_http_exception(
        status.HTTP_409_CONFLICT,
        "Breaking schema change detected; replay_on_deploy is required to proceed",
        code=ErrorCode.CONFLICT,
        category=ErrorCategory.CONFLICT,
        extra={"breaking_changes": breaking_payload},
    )


async def _prepare_promote_outputs_for_deploy(
    *,
    output_json: Dict[str, Any],
    artifact_record: Any,
    pipeline: Any,
    node_id: Optional[str],
    dataset_registry: DatasetRegistry,
    db_name: str,
    resolved_branch: str,
    definition_json: Dict[str, Any],
    replay_on_deploy: bool,
) -> _PreparedPromoteOutputs:
    selection = _resolve_promote_output_selection(
        output_json=output_json,
        artifact_record=artifact_record,
        pipeline=pipeline,
        node_id=node_id,
    )
    normalized_outputs, any_breaking_changes = await _normalize_promote_outputs(
        selected_outputs=selection.selected_outputs,
        staged_bucket=selection.staged_bucket,
        build_ref=selection.build_ref,
        dataset_registry=dataset_registry,
        db_name=db_name,
        resolved_branch=resolved_branch,
    )
    policy_mismatches = _collect_policy_mismatches(
        normalized_outputs=normalized_outputs,
        definition_json=definition_json,
        execution_semantics=selection.execution_semantics,
    )
    if policy_mismatches:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Build output write policy does not match deploy definition",
            code=ErrorCode.CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={"policy_mismatches": policy_mismatches},
        )
    _assert_breaking_changes_replay_policy(
        any_breaking_changes=any_breaking_changes,
        replay_on_deploy=replay_on_deploy,
        is_streaming_promotion=selection.is_streaming_promotion,
        normalized_outputs=normalized_outputs,
    )
    return _PreparedPromoteOutputs(
        normalized_outputs=normalized_outputs,
        staged_bucket=selection.staged_bucket,
        build_ref=selection.build_ref,
    )


async def _merge_promote_build_branch(
    *,
    request: Request | Any,
    pipeline_registry: PipelineRegistry,
    acquire_pipeline_publish_lock: Any,
    release_pipeline_publish_lock: Any,
    pipeline_id: str,
    resolved_branch: str,
    promote_job_id: str,
    staged_bucket: str,
    build_ref: str,
    build_job_id: str,
    node_id: Optional[str],
    pipeline: Any,
    db_name: str,
) -> str:
    actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
    lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)
    publish_lock = await acquire_pipeline_publish_lock(
        pipeline_id=str(pipeline_id),
        branch=resolved_branch,
        job_id=promote_job_id,
    )
    try:
        return await lakefs_client.merge(
            repository=staged_bucket,
            source_ref=build_ref,
            destination_branch=resolved_branch,
            message=f"Promote build {build_job_id} -> {resolved_branch} (pipeline {pipeline.db_name}/{pipeline.name})",
            metadata={
                "pipeline_id": str(pipeline_id),
                "db_name": db_name,
                "pipeline_name": str(pipeline.name),
                "build_job_id": str(build_job_id),
                "node_id": str(node_id) if node_id else "*",
            },
            allow_empty=True,
        )
    except LakeFSConflictError as exc:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "lakeFS merge conflict during promotion",
            code=ErrorCode.LAKEFS_CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={
                "repository": staged_bucket,
                "source_ref": build_ref,
                "destination_branch": resolved_branch,
                "error": str(exc),
            },
        ) from exc
    except LakeFSError as exc:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "lakeFS merge failed during promotion",
            code=ErrorCode.LAKEFS_ERROR,
            category=ErrorCategory.UPSTREAM,
            extra={
                "repository": staged_bucket,
                "source_ref": build_ref,
                "destination_branch": resolved_branch,
                "error": str(exc),
            },
        ) from exc
    finally:
        if publish_lock:
            await release_pipeline_publish_lock(*publish_lock)


def _build_dataset_sample_json(
    *,
    columns: list[Any],
    rows: list[dict[str, Any]],
    row_count: Optional[int],
    delta_row_count: Optional[int],
    write_mode_requested: Optional[str],
    write_mode_resolved: Optional[str],
    runtime_write_mode: Optional[str],
    pk_columns: list[str],
    post_filtering_column: Optional[str],
    write_policy_hash: Optional[str],
) -> dict[str, Any]:
    sample_json: dict[str, Any] = {
        "columns": columns,
        "rows": rows,
        "row_count": row_count,
        "sample_row_count": len(rows),
        "column_stats": compute_column_stats(rows=rows, columns=columns),
        "write_mode_requested": write_mode_requested,
        "write_mode_resolved": write_mode_resolved,
        "runtime_write_mode": runtime_write_mode,
        "pk_columns": pk_columns,
        "post_filtering_column": post_filtering_column,
        "write_policy_hash": write_policy_hash,
    }
    if delta_row_count is not None:
        sample_json["delta_row_count"] = delta_row_count
    return sample_json


async def _ensure_dataset_key_spec_alignment(
    *,
    dataset_registry: DatasetRegistry,
    dataset_id: str,
    pk_columns: list[str],
    node_id: Optional[str],
) -> None:
    existing_key_spec = await dataset_registry.get_key_spec_for_dataset(dataset_id=dataset_id)
    if not pk_columns and not existing_key_spec:
        return
    if existing_key_spec:
        normalized_spec = normalize_key_spec(existing_key_spec.spec)
        existing_pk = normalized_spec.get("primary_key") or []
        if existing_pk and not pk_columns:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Pipeline output is missing declared primary key columns",
                code=ErrorCode.CONFLICT,
                category=ErrorCategory.CONFLICT,
                extra={"dataset_id": dataset_id, "expected_primary_key": existing_pk},
            )
        if pk_columns and set(existing_pk) and set(existing_pk) != set(pk_columns):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Pipeline output primary key does not match key spec",
                code=ErrorCode.CONFLICT,
                category=ErrorCategory.CONFLICT,
                extra={
                    "dataset_id": dataset_id,
                    "expected_primary_key": existing_pk,
                    "observed_primary_key": pk_columns,
                },
            )
        if pk_columns and not existing_pk:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Pipeline output declares primary key but key spec is empty",
                code=ErrorCode.CONFLICT,
                category=ErrorCategory.CONFLICT,
                extra={"dataset_id": dataset_id, "observed_primary_key": pk_columns},
            )
        return
    record, created = await dataset_registry.get_or_create_key_spec(
        dataset_id=dataset_id,
        spec={
            "primary_key": pk_columns,
            "source": "pipeline",
            "node_id": node_id,
        },
    )
    if created:
        return

    raced_spec = normalize_key_spec(record.spec)
    raced_pk = raced_spec.get("primary_key") or []
    if set(raced_pk) != set(pk_columns):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Pipeline output primary key does not match key spec",
            code=ErrorCode.CONFLICT,
            category=ErrorCategory.CONFLICT,
            extra={
                "dataset_id": dataset_id,
                "expected_primary_key": raced_pk,
                "observed_primary_key": pk_columns,
            },
        )


def _resolve_mapping_spec_from_bundle(
    *,
    proposal_bundle: Dict[str, Any],
    dataset_id: str,
    dataset_branch: str,
) -> tuple[Optional[str], Any, Optional[str]]:
    mapping_specs_payload = proposal_bundle.get("mapping_specs") if isinstance(proposal_bundle, dict) else None
    if not isinstance(mapping_specs_payload, list):
        return None, None, None
    for spec in mapping_specs_payload:
        if not isinstance(spec, dict):
            continue
        spec_dataset_id = str(spec.get("dataset_id") or "").strip()
        if spec_dataset_id != dataset_id:
            continue
        spec_branch = str(spec.get("dataset_branch") or dataset_branch).strip() or dataset_branch
        if spec_branch != dataset_branch:
            continue
        mapping_spec_id = str(spec.get("mapping_spec_id") or "").strip() or None
        mapping_spec_version = spec.get("mapping_spec_version")
        mapping_spec_target_class_id = str(spec.get("target_class_id") or "").strip() or None
        return mapping_spec_id, mapping_spec_version, mapping_spec_target_class_id
    return None, None, None


async def _resolve_mapping_spec_reference(
    *,
    objectify_registry: ObjectifyRegistry,
    proposal_bundle: Dict[str, Any],
    dataset_id: str,
    dataset_branch: str,
    staged_dataset_name: str,
    schema_hash: str,
) -> tuple[Optional[str], Any, Optional[str]]:
    mapping_spec_id, mapping_spec_version, mapping_spec_target_class_id = _resolve_mapping_spec_from_bundle(
        proposal_bundle=proposal_bundle,
        dataset_id=dataset_id,
        dataset_branch=dataset_branch,
    )
    if mapping_spec_id is not None:
        return mapping_spec_id, mapping_spec_version, mapping_spec_target_class_id
    mapping_spec = await objectify_registry.get_active_mapping_spec(
        dataset_id=dataset_id,
        dataset_branch=dataset_branch,
        artifact_output_name=staged_dataset_name,
        schema_hash=schema_hash,
    )
    if not mapping_spec:
        return None, None, None
    return mapping_spec.mapping_spec_id, mapping_spec.version, mapping_spec.target_class_id


async def _upsert_promoted_dataset_version(
    *,
    dataset_registry: DatasetRegistry,
    db_name: str,
    resolved_branch: str,
    staged_dataset_name: str,
    schema_columns: list[Any],
    sample_rows: list[dict[str, Any]],
    row_count: Optional[int],
    delta_row_count: Optional[int],
    write_mode_requested: Optional[str],
    write_mode_resolved: Optional[str],
    runtime_write_mode: Optional[str],
    pk_columns: list[str],
    post_filtering_column: Optional[str],
    write_policy_hash: Optional[str],
    source_ref: Optional[str],
    merge_commit_id: str,
    promoted_artifact_key: str,
    promoted_from_artifact_id: Optional[str],
) -> tuple[Any, Any, str]:
    dataset, _ = await get_or_create_dataset_record(
        lookup=lambda: dataset_registry.get_dataset_by_name(
            db_name=db_name,
            name=staged_dataset_name,
            branch=resolved_branch,
        ),
        create=lambda: dataset_registry.create_dataset(
            db_name=db_name,
            name=staged_dataset_name,
            description=None,
            source_type="pipeline",
            source_ref=source_ref,
            schema_json={"columns": schema_columns},
            branch=resolved_branch,
        ),
        conflict_context=f"{db_name}/{staged_dataset_name}@{resolved_branch}",
    )
    schema_hash = compute_schema_hash(schema_columns)
    sample_json = _build_dataset_sample_json(
        columns=schema_columns,
        rows=sample_rows,
        row_count=row_count,
        delta_row_count=delta_row_count,
        write_mode_requested=write_mode_requested,
        write_mode_resolved=write_mode_resolved,
        runtime_write_mode=runtime_write_mode,
        pk_columns=pk_columns,
        post_filtering_column=post_filtering_column,
        write_policy_hash=write_policy_hash,
    )
    dataset_version = await dataset_registry.add_version(
        dataset_id=dataset.dataset_id,
        lakefs_commit_id=merge_commit_id,
        artifact_key=promoted_artifact_key,
        row_count=row_count,
        sample_json=sample_json,
        schema_json={"columns": schema_columns},
        promoted_from_artifact_id=promoted_from_artifact_id,
    )
    return dataset, dataset_version, schema_hash


def _extract_materialization_fields(
    *,
    item: dict[str, Any],
    staged_bucket: str,
    merge_commit_id: str,
) -> tuple[str, str, str, list[Any], list[dict[str, Any]], Optional[int], Optional[int]]:
    staged_dataset_name = str(item.get("dataset_name") or "")
    artifact_path = str(item.get("artifact_path") or "")
    promoted_artifact_key = build_s3_uri(staged_bucket, f"{merge_commit_id}/{artifact_path}")
    schema_columns = item.get("columns") if isinstance(item.get("columns"), list) else []
    sample_rows = item.get("rows") if isinstance(item.get("rows"), list) else []
    row_count = item.get("row_count")
    delta_row_count = item.get("delta_row_count")
    return (
        staged_dataset_name,
        artifact_path,
        promoted_artifact_key,
        schema_columns,
        sample_rows,
        row_count,
        delta_row_count,
    )


async def _record_promoted_output_manifest(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    db_name: str,
    build_job_id: str,
    artifact_id: Optional[str],
    definition_hash: str,
    staged_bucket: str,
    merge_commit_id: str,
    build_ontology_commit: str,
    mapping_spec_id: Optional[str],
    mapping_spec_version: Any,
    mapping_spec_target_class_id: Optional[str],
    dataset_version_id: str,
    staged_dataset_name: str,
    resolved_branch: str,
    principal_id: str,
    item: dict[str, Any],
) -> None:
    await pipeline_registry.record_promotion_manifest(
        pipeline_id=pipeline_id,
        db_name=db_name,
        build_job_id=build_job_id,
        artifact_id=artifact_id,
        definition_hash=definition_hash,
        lakefs_repository=staged_bucket,
        lakefs_commit_id=merge_commit_id,
        ontology_commit_id=build_ontology_commit,
        mapping_spec_id=mapping_spec_id,
        mapping_spec_version=mapping_spec_version,
        mapping_spec_target_class_id=mapping_spec_target_class_id,
        promoted_dataset_version_id=dataset_version_id,
        promoted_dataset_name=staged_dataset_name,
        target_branch=resolved_branch,
        promoted_by=principal_id,
        metadata={
            "node_id": item.get("node_id"),
            "build_artifact_key": item.get("build_artifact_key"),
            "write_mode_requested": item.get("write_mode_requested"),
            "write_mode_resolved": item.get("write_mode_resolved"),
            "runtime_write_mode": item.get("runtime_write_mode"),
            "pk_columns": item.get("pk_columns") or [],
            "post_filtering_column": item.get("post_filtering_column"),
            "write_policy_hash": item.get("write_policy_hash"),
        },
    )


async def _finalize_promoted_output_registration(
    *,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    pipeline_registry: PipelineRegistry,
    proposal_bundle: Dict[str, Any],
    definition_json: Dict[str, Any],
    item: dict[str, Any],
    dataset: Any,
    schema_hash: str,
    pipeline_id: str,
    db_name: str,
    build_job_id: str,
    artifact_id: Optional[str],
    definition_hash: str,
    staged_bucket: str,
    merge_commit_id: str,
    build_ontology_commit: str,
    resolved_branch: str,
    principal_id: str,
    staged_dataset_name: str,
    dataset_version_id: str,
) -> None:
    pk_columns = _resolve_output_pk_columns(
        definition_json=definition_json or {},
        node_id=str(item.get("node_id") or "").strip() or None,
        output_name=staged_dataset_name,
    )
    await _ensure_dataset_key_spec_alignment(
        dataset_registry=dataset_registry,
        dataset_id=dataset.dataset_id,
        pk_columns=pk_columns,
        node_id=str(item.get("node_id") or "").strip() or None,
    )
    mapping_spec_id, mapping_spec_version, mapping_spec_target_class_id = await _resolve_mapping_spec_reference(
        objectify_registry=objectify_registry,
        proposal_bundle=proposal_bundle,
        dataset_id=dataset.dataset_id,
        dataset_branch=dataset.branch,
        staged_dataset_name=staged_dataset_name,
        schema_hash=schema_hash,
    )
    await _record_promoted_output_manifest(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        db_name=db_name,
        build_job_id=build_job_id,
        artifact_id=artifact_id,
        definition_hash=definition_hash,
        staged_bucket=staged_bucket,
        merge_commit_id=merge_commit_id,
        build_ontology_commit=build_ontology_commit,
        mapping_spec_id=mapping_spec_id,
        mapping_spec_version=mapping_spec_version,
        mapping_spec_target_class_id=mapping_spec_target_class_id,
        dataset_version_id=dataset_version_id,
        staged_dataset_name=staged_dataset_name,
        resolved_branch=resolved_branch,
        principal_id=principal_id,
        item=item,
    )


def _build_promoted_output_payload(
    *,
    item: dict[str, Any],
    staged_dataset_name: str,
    dataset_id: str,
    dataset_version_id: str,
    promoted_artifact_key: str,
    artifact_path: str,
    merge_commit_id: str,
    replay_on_deploy: bool,
    artifact_id: Optional[str],
) -> dict[str, Any]:
    return {
        "node_id": item.get("node_id"),
        "dataset_name": staged_dataset_name,
        "dataset_id": dataset_id,
        "dataset_version_id": dataset_version_id,
        "artifact_key": promoted_artifact_key,
        "row_count": item.get("row_count"),
        "delta_row_count": item.get("delta_row_count"),
        "write_mode_requested": item.get("write_mode_requested"),
        "write_mode_resolved": item.get("write_mode_resolved"),
        "runtime_write_mode": item.get("runtime_write_mode"),
        "pk_columns": item.get("pk_columns") or [],
        "post_filtering_column": item.get("post_filtering_column"),
        "write_policy_hash": item.get("write_policy_hash"),
        "build_artifact_key": item.get("build_artifact_key"),
        "build_ref": item.get("build_ref"),
        "artifact_path": artifact_path,
        "merge_commit_id": merge_commit_id,
        "breaking_changes": item.get("breaking_changes") or [],
        "replay_on_deploy": replay_on_deploy,
        "promoted_from_artifact_id": artifact_id,
    }


async def _materialize_single_promoted_output(
    *,
    item: dict[str, Any],
    context: _PromotedOutputMaterializationContext,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    proposal_bundle: Dict[str, Any],
    pipeline_registry: PipelineRegistry,
) -> dict[str, Any]:
    (
        staged_dataset_name,
        artifact_path,
        promoted_artifact_key,
        schema_columns,
        sample_rows,
        row_count,
        delta_row_count,
    ) = _extract_materialization_fields(
        item=item,
        staged_bucket=context.staged_bucket,
        merge_commit_id=context.merge_commit_id,
    )

    dataset, dataset_version, schema_hash = await _upsert_promoted_dataset_version(
        dataset_registry=dataset_registry,
        db_name=context.db_name,
        resolved_branch=context.resolved_branch,
        staged_dataset_name=staged_dataset_name,
        schema_columns=schema_columns,
        sample_rows=sample_rows,
        row_count=row_count,
        delta_row_count=delta_row_count,
        write_mode_requested=item.get("write_mode_requested"),
        write_mode_resolved=item.get("write_mode_resolved"),
        runtime_write_mode=item.get("runtime_write_mode"),
        pk_columns=[str(col).strip() for col in (item.get("pk_columns") or []) if str(col).strip()],
        post_filtering_column=item.get("post_filtering_column"),
        write_policy_hash=item.get("write_policy_hash"),
        source_ref=context.pipeline_id,
        merge_commit_id=context.merge_commit_id,
        promoted_artifact_key=promoted_artifact_key,
        promoted_from_artifact_id=context.artifact_id,
    )
    await _finalize_promoted_output_registration(
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        pipeline_registry=pipeline_registry,
        proposal_bundle=proposal_bundle,
        definition_json=context.definition_json,
        item=item,
        dataset=dataset,
        schema_hash=schema_hash,
        pipeline_id=context.pipeline_id,
        db_name=context.db_name,
        build_job_id=context.build_job_id,
        artifact_id=context.artifact_id,
        definition_hash=context.definition_hash,
        staged_bucket=context.staged_bucket,
        merge_commit_id=context.merge_commit_id,
        build_ontology_commit=context.build_ontology_commit,
        resolved_branch=context.resolved_branch,
        principal_id=context.principal_id,
        staged_dataset_name=staged_dataset_name,
        dataset_version_id=dataset_version.version_id,
    )
    return _build_promoted_output_payload(
        item=item,
        staged_dataset_name=staged_dataset_name,
        dataset_id=dataset.dataset_id,
        dataset_version_id=dataset_version.version_id,
        promoted_artifact_key=promoted_artifact_key,
        artifact_path=artifact_path,
        merge_commit_id=context.merge_commit_id,
        replay_on_deploy=context.replay_on_deploy,
        artifact_id=context.artifact_id,
    )


async def _materialize_promoted_build_outputs(
    *,
    normalized_outputs: list[dict[str, Any]],
    merge_commit_id: str,
    staged_bucket: str,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    proposal_bundle: Dict[str, Any],
    db_name: str,
    resolved_branch: str,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    build_job_id: str,
    definition_hash: str,
    build_ontology_commit: str,
    principal_id: str,
    replay_on_deploy: bool,
    artifact_id: Optional[str],
    definition_json: Dict[str, Any],
    build_ref: str,
) -> list[dict[str, Any]]:
    context = _PromotedOutputMaterializationContext(
        merge_commit_id=merge_commit_id,
        staged_bucket=staged_bucket,
        db_name=db_name,
        resolved_branch=resolved_branch,
        pipeline_id=pipeline_id,
        build_job_id=build_job_id,
        definition_hash=definition_hash,
        build_ontology_commit=build_ontology_commit,
        principal_id=principal_id,
        replay_on_deploy=replay_on_deploy,
        artifact_id=artifact_id,
        definition_json=definition_json,
    )
    build_outputs: list[dict[str, Any]] = []
    for item in normalized_outputs:
        normalized_item = dict(item)
        normalized_item["build_ref"] = build_ref
        build_outputs.append(
            await _materialize_single_promoted_output(
                item=normalized_item,
                context=context,
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
                proposal_bundle=proposal_bundle,
                pipeline_registry=pipeline_registry,
            )
        )
    return build_outputs


def _build_promoted_run_output_json(
    *,
    build_outputs: list[dict[str, Any]],
    build_ontology: Dict[str, Any],
    build_job_id: str,
    artifact_id: Optional[str],
    staged_bucket: str,
    build_ref: str,
    resolved_branch: str,
    merge_commit_id: str,
    promote_job_id: Optional[str] = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "outputs": build_outputs,
        "ontology": build_ontology,
        "promoted_from_build_job_id": build_job_id,
        "promoted_from_artifact_id": artifact_id,
        "lakefs": {
            "repository": staged_bucket,
            "source_ref": build_ref,
            "destination_branch": resolved_branch,
            "merge_commit_id": merge_commit_id,
        },
    }
    if promote_job_id:
        payload["promote_job_id"] = promote_job_id
    return payload


async def _record_deploy_build_and_run(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    build_outputs: list[dict[str, Any]],
    build_ontology: Dict[str, Any],
    build_job_id: str,
    artifact_id: Optional[str],
    staged_bucket: str,
    build_ref: str,
    resolved_branch: str,
    merge_commit_id: str,
    promote_job_id: str,
    node_id: Optional[str],
    definition_hash: str,
    definition_commit_id: str,
) -> None:
    await pipeline_registry.record_build(
        pipeline_id=pipeline_id,
        status="DEPLOYED",
        output_json=_build_promoted_run_output_json(
            build_outputs=build_outputs,
            build_ontology=build_ontology,
            build_job_id=build_job_id,
            artifact_id=artifact_id,
            staged_bucket=staged_bucket,
            build_ref=build_ref,
            resolved_branch=resolved_branch,
            merge_commit_id=merge_commit_id,
            promote_job_id=promote_job_id,
        ),
        deployed_commit_id=merge_commit_id,
    )
    await pipeline_registry.record_run(
        pipeline_id=pipeline_id,
        job_id=promote_job_id,
        mode="deploy",
        status="DEPLOYED",
        node_id=node_id,
        row_count=(build_outputs[0].get("row_count") if build_outputs else None),
        pipeline_spec_hash=definition_hash,
        pipeline_spec_commit_id=definition_commit_id,
        output_lakefs_commit_id=merge_commit_id,
        output_json=_build_promoted_run_output_json(
            build_outputs=build_outputs,
            build_ontology=build_ontology,
            build_job_id=build_job_id,
            artifact_id=artifact_id,
            staged_bucket=staged_bucket,
            build_ref=build_ref,
            resolved_branch=resolved_branch,
            merge_commit_id=merge_commit_id,
        ),
        finished_at=utcnow(),
    )


async def _emit_pipeline_deploy_promoted_event(
    *,
    emit_pipeline_control_plane_event: Any,
    pipeline_id: str,
    promote_job_id: str,
    principal_id: str,
    build_job_id: str,
    artifact_id: Optional[str],
    db_name: str,
    resolved_branch: str,
    node_id: Optional[str],
    merge_commit_id: str,
    definition_hash: str,
    replay_on_deploy: bool,
    principal_type: str,
) -> None:
    await emit_pipeline_control_plane_event(
        event_type="PIPELINE_DEPLOY_PROMOTED",
        pipeline_id=pipeline_id,
        event_id=promote_job_id,
        actor=principal_id,
        data={
            "pipeline_id": pipeline_id,
            "promote_job_id": promote_job_id,
            "build_job_id": build_job_id,
            "artifact_id": artifact_id,
            "db_name": db_name,
            "branch": resolved_branch,
            "node_id": node_id,
            "merge_commit_id": merge_commit_id,
            "definition_hash": definition_hash,
            "replay_on_deploy": replay_on_deploy,
            "principal_type": principal_type,
            "principal_id": principal_id,
        },
    )


async def _record_promoted_build_output_lineage(
    *,
    lineage_store: LineageStoreDep,
    build_outputs: list[dict[str, Any]],
    db_name: str,
    resolved_branch: str,
    pipeline_id: str,
    build_job_id: str,
    artifact_id: Optional[str],
) -> None:
    for item in build_outputs:
        promoted_artifact_key = str(item.get("artifact_key") or "")
        parsed = parse_s3_uri(promoted_artifact_key)
        if not parsed:
            continue
        bucket, key = parsed
        await lineage_store.record_link(
            from_node_id=lineage_store.node_aggregate("Pipeline", str(pipeline_id)),
            to_node_id=lineage_store.node_artifact("s3", bucket, key),
            edge_type=EDGE_PIPELINE_OUTPUT_STORED,
            occurred_at=utcnow(),
            db_name=db_name,
            branch=resolved_branch,
            edge_metadata={
                "db_name": db_name,
                "branch": resolved_branch,
                "pipeline_id": str(pipeline_id),
                "artifact_key": promoted_artifact_key,
                "dataset_name": item.get("dataset_name"),
                "node_id": item.get("node_id"),
                "promoted_from_build_job_id": build_job_id,
                "promoted_from_artifact_id": artifact_id,
                "build_artifact_key": item.get("build_artifact_key"),
                "build_ref": item.get("build_ref"),
                "artifact_path": item.get("artifact_path"),
                "merge_commit_id": item.get("merge_commit_id"),
            },
        )


async def _apply_deploy_pipeline_updates(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    schedule_interval_seconds: Optional[int],
    schedule_cron: Optional[str],
    branch: Optional[str],
    proposal_status: Optional[str],
    proposal_title: Optional[str],
    proposal_description: Optional[str],
    dependencies: Optional[list[dict[str, str]]],
) -> None:
    if schedule_interval_seconds or schedule_cron or branch or proposal_status or proposal_title or proposal_description:
        await pipeline_registry.update_pipeline(
            pipeline_id=pipeline_id,
            schedule_interval_seconds=schedule_interval_seconds,
            schedule_cron=schedule_cron,
            branch=branch,
            proposal_status=proposal_status,
            proposal_title=proposal_title,
            proposal_description=proposal_description,
        )
    if dependencies is not None:
        await pipeline_registry.replace_dependencies(pipeline_id=pipeline_id, dependencies=dependencies)


@dataclass(frozen=True)
class _PreparedDeployExecution:
    request_input: _DeployRequestPayload
    deploy_context: _DeployPipelineContext
    definition_json: Dict[str, Any]
    db_name: str
    definition_hash: str
    definition_commit_id: str


@dataclass(frozen=True)
class _DeployPromotionPayload:
    artifact_ref: Optional[str]
    build_job_id: str
    build_ontology: Dict[str, Any]
    build_ontology_commit: str
    normalized_outputs: list[dict[str, Any]]
    staged_bucket: str
    build_ref: str


@dataclass(frozen=True)
class _DeployExecutionResult:
    promote_build: bool
    build_job_id: str
    artifact_ref: Optional[str]
    node_id: Optional[str]
    resolved_branch: str
    replay_on_deploy: bool
    promote_job_id: str
    deployed_commit_id: str
    build_outputs: list[dict[str, Any]]


async def _prepare_deploy_execution(
    *,
    pipeline_id: str,
    sanitized: Dict[str, Any],
    pipeline_registry: PipelineRegistry,
) -> _PreparedDeployExecution:
    request_input = _parse_deploy_request_payload(sanitized=sanitized)
    deploy_context = await _resolve_deploy_pipeline_context(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        db_name_hint=request_input.db_name,
        branch=request_input.branch,
        dependencies_raw=request_input.dependencies_raw,
    )
    definition_json, db_name = _resolve_deploy_definition_and_db(
        definition_json=request_input.definition_json,
        latest=deploy_context.latest,
        expectations=request_input.expectations,
        schema_contract=request_input.schema_contract,
        outputs=request_input.outputs,
        db_name=request_input.db_name,
        pipeline=deploy_context.pipeline,
    )
    definition_hash = _stable_definition_hash(definition_json or {})
    definition_commit_id = _resolve_definition_commit_id(
        definition_json or {},
        deploy_context.latest,
        definition_hash,
    )
    if not request_input.promote_build:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "deploy requires promote_build; run /build then /deploy with promote_build",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    return _PreparedDeployExecution(
        request_input=request_input,
        deploy_context=deploy_context,
        definition_json=definition_json,
        db_name=db_name,
        definition_hash=definition_hash,
        definition_commit_id=definition_commit_id,
    )


async def _resolve_deploy_promotion_payload(
    *,
    prepared: _PreparedDeployExecution,
    pipeline_id: str,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
) -> _DeployPromotionPayload:
    request_input = prepared.request_input
    deploy_context = prepared.deploy_context
    source_context = await _resolve_promote_build_source_context(
        pipeline_registry=pipeline_registry,
        objectify_registry=objectify_registry,
        pipeline_id=pipeline_id,
        artifact_id=request_input.artifact_id,
        build_job_id=request_input.build_job_id,
        definition_hash=prepared.definition_hash,
        resolved_branch=deploy_context.resolved_branch,
        proposal_required=deploy_context.proposal_required,
        proposal_bundle=deploy_context.proposal_bundle,
    )
    artifact_record = source_context.artifact_record
    prepared_outputs = await _prepare_promote_outputs_for_deploy(
        output_json=source_context.output_json,
        artifact_record=artifact_record,
        pipeline=deploy_context.pipeline,
        node_id=request_input.node_id,
        dataset_registry=dataset_registry,
        db_name=prepared.db_name,
        resolved_branch=deploy_context.resolved_branch,
        definition_json=prepared.definition_json or {},
        replay_on_deploy=request_input.replay_on_deploy,
    )
    return _DeployPromotionPayload(
        artifact_ref=(artifact_record.artifact_id if artifact_record else None),
        build_job_id=source_context.build_job_id,
        build_ontology=source_context.build_ontology,
        build_ontology_commit=source_context.build_ontology_commit,
        normalized_outputs=prepared_outputs.normalized_outputs,
        staged_bucket=prepared_outputs.staged_bucket,
        build_ref=prepared_outputs.build_ref,
    )


async def _emit_pipeline_deploy_requested_event(
    *,
    emit_pipeline_control_plane_event: Any,
    pipeline_id: str,
    promote_job_id: str,
    principal_id: str,
    build_job_id: str,
    artifact_id: Optional[str],
    db_name: str,
    resolved_branch: str,
    node_id: Optional[str],
    definition_hash: str,
    replay_on_deploy: bool,
    principal_type: str,
) -> None:
    await emit_pipeline_control_plane_event(
        event_type="PIPELINE_DEPLOY_REQUESTED",
        pipeline_id=pipeline_id,
        event_id=f"deploy-requested-{promote_job_id}",
        actor=principal_id,
        data={
            "pipeline_id": pipeline_id,
            "promote_job_id": promote_job_id,
            "build_job_id": build_job_id,
            "artifact_id": artifact_id,
            "db_name": db_name,
            "branch": resolved_branch,
            "node_id": node_id,
            "definition_hash": definition_hash,
            "replay_on_deploy": replay_on_deploy,
            "principal_type": principal_type,
            "principal_id": principal_id,
        },
    )


async def _merge_and_materialize_deploy_outputs(
    *,
    prepared: _PreparedDeployExecution,
    promotion_payload: _DeployPromotionPayload,
    request_input: _DeployRequestPayload,
    pipeline_context: _DeployPipelineContext,
    pipeline_id: str,
    request: Request | Any,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    acquire_pipeline_publish_lock: Any,
    release_pipeline_publish_lock: Any,
    principal_id: str,
    promote_job_id: str,
) -> tuple[str, list[dict[str, Any]]]:
    merge_commit_id = await _merge_promote_build_branch(
        request=request,
        pipeline_registry=pipeline_registry,
        acquire_pipeline_publish_lock=acquire_pipeline_publish_lock,
        release_pipeline_publish_lock=release_pipeline_publish_lock,
        pipeline_id=pipeline_id,
        resolved_branch=pipeline_context.resolved_branch,
        promote_job_id=promote_job_id,
        staged_bucket=promotion_payload.staged_bucket,
        build_ref=promotion_payload.build_ref,
        build_job_id=promotion_payload.build_job_id,
        node_id=request_input.node_id,
        pipeline=pipeline_context.pipeline,
        db_name=prepared.db_name,
    )
    build_outputs = await _materialize_promoted_build_outputs(
        normalized_outputs=promotion_payload.normalized_outputs,
        merge_commit_id=merge_commit_id,
        staged_bucket=promotion_payload.staged_bucket,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        proposal_bundle=pipeline_context.proposal_bundle,
        db_name=prepared.db_name,
        resolved_branch=pipeline_context.resolved_branch,
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        build_job_id=promotion_payload.build_job_id,
        definition_hash=prepared.definition_hash,
        build_ontology_commit=promotion_payload.build_ontology_commit,
        principal_id=principal_id,
        replay_on_deploy=request_input.replay_on_deploy,
        artifact_id=promotion_payload.artifact_ref,
        definition_json=prepared.definition_json or {},
        build_ref=promotion_payload.build_ref,
    )
    return merge_commit_id, build_outputs


async def _record_deploy_execution_side_effects(
    *,
    prepared: _PreparedDeployExecution,
    promotion_payload: _DeployPromotionPayload,
    pipeline_context: _DeployPipelineContext,
    pipeline_id: str,
    request_input: _DeployRequestPayload,
    build_outputs: list[dict[str, Any]],
    merge_commit_id: str,
    promote_job_id: str,
    principal_id: str,
    principal_type: str,
    pipeline_registry: PipelineRegistry,
    lineage_store: LineageStoreDep,
    emit_pipeline_control_plane_event: Any,
) -> None:
    await _record_deploy_build_and_run(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        build_outputs=build_outputs,
        build_ontology=promotion_payload.build_ontology,
        build_job_id=promotion_payload.build_job_id,
        artifact_id=promotion_payload.artifact_ref,
        staged_bucket=promotion_payload.staged_bucket,
        build_ref=promotion_payload.build_ref,
        resolved_branch=pipeline_context.resolved_branch,
        merge_commit_id=merge_commit_id,
        promote_job_id=promote_job_id,
        node_id=request_input.node_id,
        definition_hash=prepared.definition_hash,
        definition_commit_id=prepared.definition_commit_id,
    )
    await _emit_pipeline_deploy_promoted_event(
        emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
        pipeline_id=pipeline_id,
        promote_job_id=promote_job_id,
        principal_id=principal_id,
        build_job_id=promotion_payload.build_job_id,
        artifact_id=promotion_payload.artifact_ref,
        db_name=prepared.db_name,
        resolved_branch=pipeline_context.resolved_branch,
        node_id=request_input.node_id,
        merge_commit_id=merge_commit_id,
        definition_hash=prepared.definition_hash,
        replay_on_deploy=request_input.replay_on_deploy,
        principal_type=principal_type,
    )
    await _record_promoted_build_output_lineage(
        lineage_store=lineage_store,
        build_outputs=build_outputs,
        db_name=prepared.db_name,
        resolved_branch=pipeline_context.resolved_branch,
        pipeline_id=pipeline_id,
        build_job_id=promotion_payload.build_job_id,
        artifact_id=promotion_payload.artifact_ref,
    )
    await _apply_deploy_pipeline_updates(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        schedule_interval_seconds=request_input.schedule_interval_seconds,
        schedule_cron=request_input.schedule_cron,
        branch=request_input.branch,
        proposal_status=request_input.proposal_status,
        proposal_title=request_input.proposal_title,
        proposal_description=request_input.proposal_description,
        dependencies=pipeline_context.dependencies,
    )


async def _dispatch_deploy_execution(
    *,
    prepared: _PreparedDeployExecution,
    pipeline_id: str,
    request: Request | Any,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    lineage_store: LineageStoreDep,
    emit_pipeline_control_plane_event: Any,
    acquire_pipeline_publish_lock: Any,
    release_pipeline_publish_lock: Any,
) -> _DeployExecutionResult:
    request_input = prepared.request_input
    pipeline_context = prepared.deploy_context
    promotion_payload = await _resolve_deploy_promotion_payload(
        prepared=prepared,
        pipeline_id=pipeline_id,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
    )
    promote_job_id = f"promote-{uuid4().hex}"
    principal_type, principal_id = _resolve_principal(request)
    await _emit_pipeline_deploy_requested_event(
        emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
        pipeline_id=pipeline_id,
        promote_job_id=promote_job_id,
        principal_id=principal_id,
        build_job_id=promotion_payload.build_job_id,
        artifact_id=promotion_payload.artifact_ref,
        db_name=prepared.db_name,
        resolved_branch=pipeline_context.resolved_branch,
        node_id=request_input.node_id,
        definition_hash=prepared.definition_hash,
        replay_on_deploy=request_input.replay_on_deploy,
        principal_type=principal_type,
    )
    merge_commit_id, build_outputs = await _merge_and_materialize_deploy_outputs(
        prepared=prepared,
        promotion_payload=promotion_payload,
        request_input=request_input,
        pipeline_context=pipeline_context,
        pipeline_id=pipeline_id,
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        acquire_pipeline_publish_lock=acquire_pipeline_publish_lock,
        release_pipeline_publish_lock=release_pipeline_publish_lock,
        principal_id=principal_id,
        promote_job_id=promote_job_id,
    )
    await _record_deploy_execution_side_effects(
        prepared=prepared,
        promotion_payload=promotion_payload,
        pipeline_context=pipeline_context,
        pipeline_id=pipeline_id,
        request_input=request_input,
        build_outputs=build_outputs,
        merge_commit_id=merge_commit_id,
        promote_job_id=promote_job_id,
        principal_id=principal_id,
        principal_type=principal_type,
        pipeline_registry=pipeline_registry,
        lineage_store=lineage_store,
        emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
    )
    return _DeployExecutionResult(
        promote_build=request_input.promote_build,
        build_job_id=promotion_payload.build_job_id,
        artifact_ref=promotion_payload.artifact_ref,
        node_id=request_input.node_id,
        resolved_branch=pipeline_context.resolved_branch,
        replay_on_deploy=request_input.replay_on_deploy,
        promote_job_id=promote_job_id,
        deployed_commit_id=merge_commit_id,
        build_outputs=build_outputs,
    )


def _build_deploy_audit_metadata(result: _DeployExecutionResult) -> Dict[str, Any]:
    return {
        "promote_build": result.promote_build,
        "build_job_id": result.build_job_id,
        "artifact_id": result.artifact_ref,
        "node_id": result.node_id,
        "merge_commit_id": result.deployed_commit_id,
        "branch": result.resolved_branch,
        "replay_on_deploy": result.replay_on_deploy,
    }


def _build_deploy_success_response(
    *,
    pipeline_id: str,
    result: _DeployExecutionResult,
) -> Dict[str, Any]:
    return ApiResponse.success(
        message="Pipeline deployed (promoted from build)",
        data={
            "pipeline_id": pipeline_id,
            "job_id": result.promote_job_id,
            "deployed_commit_id": result.deployed_commit_id,
            "outputs": result.build_outputs,
            "artifact_id": result.artifact_ref,
        },
    ).to_dict()


@trace_external_call("bff.pipeline_execution.deploy_pipeline")
async def deploy_pipeline(
    *,
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request | Any,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    oms_client: OMSClient,
    lineage_store: LineageStoreDep,
    audit_store: AuditLogStoreDep,
    emit_pipeline_control_plane_event: Any,
    _acquire_pipeline_publish_lock: Any,
    _release_pipeline_publish_lock: Any,
) -> ApiResponse:
    try:
        _require_pipeline_idempotency_key(request, operation="pipeline deploy")
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="approve",
        )
        prepared = await _prepare_deploy_execution(
            pipeline_id=pipeline_id,
            sanitized=sanitize_input(payload),
            pipeline_registry=pipeline_registry,
        )
        enforce_db_scope_or_403(request, db_name=prepared.db_name)
        await enforce_database_role_or_http_error(
            headers=request.headers,
            db_name=prepared.db_name,
            required_roles=DATA_ENGINEER_ROLES,
            allow_if_registry_unavailable=True,
        )
        result = await _dispatch_deploy_execution(
            prepared=prepared,
            pipeline_id=pipeline_id,
            request=request,
            pipeline_registry=pipeline_registry,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            lineage_store=lineage_store,
            emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
            acquire_pipeline_publish_lock=_acquire_pipeline_publish_lock,
            release_pipeline_publish_lock=_release_pipeline_publish_lock,
        )
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_DEPLOYED",
            status="success",
            pipeline_id=pipeline_id,
            metadata=_build_deploy_audit_metadata(result),
        )
        return _build_deploy_success_response(
            pipeline_id=pipeline_id,
            result=result,
        )

    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_DEPLOYED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to deploy pipeline: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)
