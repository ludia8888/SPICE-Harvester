"""
Deploy/promotion execution helpers for pipeline execution.

This module owns output normalization, materialization, and deploy side-effect
recording so the main service facade can stay focused on public entrypoints.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import status

from bff.routers.pipeline_ops import _detect_breaking_schema_changes, _resolve_output_pk_columns
from bff.routers.pipeline_shared import _resolve_principal
from bff.services.pipeline_execution_requests import (
    _DeployExecutionResult,
    _DeployPipelineContext,
    _DeployPromotionPayload,
    _DeployRequestPayload,
    _PreparedDeployExecution,
    _PreparedPromoteOutputs,
    _PromotedOutputMaterializationContext,
    _PromoteOutputSelection,
)
from shared.dependencies.providers import LineageStoreDep
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.models.lineage_edge_types import EDGE_PIPELINE_OUTPUT_STORED
from shared.models.requests import ApiResponse
from shared.services.pipeline.dataset_output_semantics import resolve_dataset_write_policy
from shared.services.pipeline.output_plugins import OUTPUT_KIND_DATASET, normalize_output_kind
from shared.services.pipeline.pipeline_profiler import compute_column_stats
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.dataset_registry_get_or_create import get_or_create_dataset_record
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.lakefs_client import LakeFSConflictError, LakeFSError
from shared.utils.key_spec import normalize_key_spec
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri
from shared.utils.schema_hash import compute_schema_hash
from shared.utils.time_utils import utcnow


logger = logging.getLogger(__name__)


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
    resolve_output_contract_from_definition: Any,
) -> list[dict[str, Any]]:
    policy_mismatches: list[dict[str, Any]] = []
    for item in normalized_outputs:
        if str(item.get("output_kind") or "").strip().lower() != OUTPUT_KIND_DATASET:
            continue
        try:
            output_contract = resolve_output_contract_from_definition(
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


async def prepare_promote_outputs_for_deploy(
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
    resolve_output_contract_from_definition: Any,
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
        resolve_output_contract_from_definition=resolve_output_contract_from_definition,
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


async def merge_promote_build_branch(
    *,
    request: Any,
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


async def materialize_promoted_build_outputs(
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


async def record_deploy_build_and_run(
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


async def emit_pipeline_deploy_promoted_event(
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


async def record_promoted_build_output_lineage(
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


async def apply_deploy_pipeline_updates(
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


async def resolve_deploy_promotion_payload(
    *,
    prepared: _PreparedDeployExecution,
    pipeline_id: str,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    resolve_promote_build_source_context: Any,
    resolve_output_contract_from_definition: Any,
) -> _DeployPromotionPayload:
    request_input = prepared.request_input
    deploy_context = prepared.deploy_context
    source_context = await resolve_promote_build_source_context(
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
    prepared_outputs = await prepare_promote_outputs_for_deploy(
        output_json=source_context.output_json,
        artifact_record=artifact_record,
        pipeline=deploy_context.pipeline,
        node_id=request_input.node_id,
        dataset_registry=dataset_registry,
        db_name=prepared.db_name,
        resolved_branch=deploy_context.resolved_branch,
        definition_json=prepared.definition_json or {},
        replay_on_deploy=request_input.replay_on_deploy,
        resolve_output_contract_from_definition=resolve_output_contract_from_definition,
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


async def emit_pipeline_deploy_requested_event(
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


async def merge_and_materialize_deploy_outputs(
    *,
    prepared: _PreparedDeployExecution,
    promotion_payload: _DeployPromotionPayload,
    request_input: _DeployRequestPayload,
    pipeline_context: _DeployPipelineContext,
    pipeline_id: str,
    request: Any,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    acquire_pipeline_publish_lock: Any,
    release_pipeline_publish_lock: Any,
    principal_id: str,
    promote_job_id: str,
) -> tuple[str, list[dict[str, Any]]]:
    merge_commit_id = await merge_promote_build_branch(
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
    build_outputs = await materialize_promoted_build_outputs(
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


async def record_deploy_execution_side_effects(
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
    await record_deploy_build_and_run(
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
    await emit_pipeline_deploy_promoted_event(
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
    await record_promoted_build_output_lineage(
        lineage_store=lineage_store,
        build_outputs=build_outputs,
        db_name=prepared.db_name,
        resolved_branch=pipeline_context.resolved_branch,
        pipeline_id=pipeline_id,
        build_job_id=promotion_payload.build_job_id,
        artifact_id=promotion_payload.artifact_ref,
    )
    await apply_deploy_pipeline_updates(
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


async def dispatch_deploy_execution(
    *,
    prepared: _PreparedDeployExecution,
    pipeline_id: str,
    request: Any,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    lineage_store: LineageStoreDep,
    emit_pipeline_control_plane_event: Any,
    acquire_pipeline_publish_lock: Any,
    release_pipeline_publish_lock: Any,
    resolve_promote_build_source_context: Any,
    resolve_output_contract_from_definition: Any,
) -> _DeployExecutionResult:
    request_input = prepared.request_input
    pipeline_context = prepared.deploy_context
    promotion_payload = await resolve_deploy_promotion_payload(
        prepared=prepared,
        pipeline_id=pipeline_id,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        resolve_promote_build_source_context=resolve_promote_build_source_context,
        resolve_output_contract_from_definition=resolve_output_contract_from_definition,
    )
    promote_job_id = f"promote-{uuid4().hex}"
    principal_type, principal_id = _resolve_principal(request)
    await emit_pipeline_deploy_requested_event(
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
    merge_commit_id, build_outputs = await merge_and_materialize_deploy_outputs(
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
    await record_deploy_execution_side_effects(
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


def build_deploy_audit_metadata(result: _DeployExecutionResult) -> Dict[str, Any]:
    return {
        "promote_build": result.promote_build,
        "build_job_id": result.build_job_id,
        "artifact_id": result.artifact_ref,
        "node_id": result.node_id,
        "merge_commit_id": result.deployed_commit_id,
        "branch": result.resolved_branch,
        "replay_on_deploy": result.replay_on_deploy,
    }


def build_deploy_success_response(
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
