"""
Pipeline execution domain logic (BFF).

Extracted from `bff.routers.pipeline_execution` to keep routers thin.
"""

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
from bff.services.pipeline_execution_requests import (
    _DeployExecutionResult,
    _DeployPipelineContext,
    _DeployPromotionPayload,
    _DeployRequestPayload,
    _PipelineRunContext,
    _PipelineRunRequestPayload,
    _PreparedBuildExecution,
    _PreparedDeployExecution,
    _PreparedPreviewExecution,
    _PreparedPromoteOutputs,
    _PromoteBuildSourceContext,
    _PromotedOutputMaterializationContext,
    _PromoteOutputSelection,
)
from bff.services import pipeline_execution_deploy as _deploy_exec
from bff.services.pipeline_execution_queue import publish_build_pipeline_job, publish_preview_pipeline_job
from bff.services.ontology_occ_guard_service import (
    resolve_branch_head_commit_with_bootstrap,
)
from bff.services.input_validation_service import enforce_db_scope_or_403
from bff.services import pipeline_execution_preview_build as _preview_build
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

_ensure_dataset_key_spec_alignment = _deploy_exec._ensure_dataset_key_spec_alignment


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


async def _prepare_preview_execution(
    *,
    pipeline_id: str,
    sanitized: Dict[str, Any],
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
) -> _PreparedPreviewExecution:
    return await _preview_build.prepare_preview_execution(
        pipeline_id=pipeline_id,
        sanitized=sanitized,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
    )


async def _dispatch_preview_execution(
    *,
    pipeline_id: str,
    prepared: _PreparedPreviewExecution,
    pipeline_registry: PipelineRegistry,
    pipeline_job_queue: PipelineJobQueue,
    event_store: Any,
) -> Dict[str, Any]:
    return await _preview_build.dispatch_preview_execution(
        pipeline_id=pipeline_id,
        prepared=prepared,
        pipeline_registry=pipeline_registry,
        pipeline_job_queue=pipeline_job_queue,
        event_store=event_store,
    )


async def _prepare_build_execution(
    *,
    pipeline_id: str,
    sanitized: Dict[str, Any],
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    oms_client: OMSClient,
) -> _PreparedBuildExecution:
    return await _preview_build.prepare_build_execution(
        pipeline_id=pipeline_id,
        sanitized=sanitized,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        oms_client=oms_client,
        validate_db_name=validate_db_name,
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
    return await _preview_build.dispatch_build_execution(
        pipeline_id=pipeline_id,
        prepared=prepared,
        pipeline_registry=pipeline_registry,
        pipeline_job_queue=pipeline_job_queue,
        emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
        request=request,
    )


async def _record_preview_validation_failure(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    node_id: Optional[str],
    error_detail: str,
) -> None:
    await _preview_build.record_preview_validation_failure(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        node_id=node_id,
        error_detail=error_detail,
    )


async def _log_pipeline_preview_failure(
    *,
    audit_store: AuditLogStoreDep,
    request: Request | Any,
    pipeline_id: str,
    error_detail: str,
) -> None:
    await _preview_build.log_pipeline_preview_failure(
        audit_store=audit_store,
        request=request,
        pipeline_id=pipeline_id,
        error_detail=error_detail,
    )


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
                allow_if_registry_unavailable=False,
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
                allow_if_registry_unavailable=False,
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
    return await _deploy_exec.dispatch_deploy_execution(
        prepared=prepared,
        pipeline_id=pipeline_id,
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        lineage_store=lineage_store,
        emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
        acquire_pipeline_publish_lock=acquire_pipeline_publish_lock,
        release_pipeline_publish_lock=release_pipeline_publish_lock,
        resolve_promote_build_source_context=_resolve_promote_build_source_context,
        resolve_output_contract_from_definition=_resolve_output_contract_from_definition,
    )


def _build_deploy_audit_metadata(result: _DeployExecutionResult) -> Dict[str, Any]:
    return _deploy_exec.build_deploy_audit_metadata(result)


def _build_deploy_success_response(
    *,
    pipeline_id: str,
    result: _DeployExecutionResult,
) -> Dict[str, Any]:
    return _deploy_exec.build_deploy_success_response(
        pipeline_id=pipeline_id,
        result=result,
    )


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
            allow_if_registry_unavailable=False,
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
