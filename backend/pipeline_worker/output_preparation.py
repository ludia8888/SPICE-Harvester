from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.models.pipeline_job import PipelineJob
from shared.services.pipeline.dataset_output_semantics import (
    resolve_dataset_write_policy,
    validate_dataset_output_metadata,
)
from shared.services.pipeline.output_plugins import (
    OUTPUT_KIND_DATASET,
    resolve_output_kind,
    validate_output_payload,
)
from shared.services.pipeline.pipeline_definition_utils import (
    build_expectations_with_pk,
    resolve_delete_column,
    resolve_pk_columns,
    resolve_pk_semantics,
    validate_pk_semantics,
)
from shared.services.pipeline.pipeline_profiler import compute_column_stats
from shared.services.pipeline.pipeline_validation_utils import (
    validate_expectations,
    validate_schema_contract,
)
from shared.utils.time_utils import utcnow

from pipeline_worker.spark_schema_helpers import _hash_schema_columns, _schema_from_dataframe

try:  # pragma: no cover - import guard for unit tests without Spark
    from pyspark import StorageLevel  # type: ignore
    from pyspark.sql import DataFrame  # type: ignore

    _PYSPARK_AVAILABLE = True
except ImportError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment,misc]
    StorageLevel = Any  # type: ignore[assignment,misc]
    _PYSPARK_AVAILABLE = False


logger = logging.getLogger(__name__)


class _OutputNodeValidationError(ValueError):
    def __init__(
        self,
        *,
        stage: str,
        node_id: str,
        errors: List[str],
        row_count: int,
        payload: Dict[str, Any],
    ) -> None:
        super().__init__(f"{stage}:{node_id}")
        self.stage = str(stage)
        self.node_id = str(node_id)
        self.errors = list(errors)
        self.row_count = int(row_count)
        self.payload = payload


def _schema_column_names(columns: Optional[List[Any]]) -> set[str]:
    names: set[str] = set()
    for item in columns or []:
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("column") or "").strip()
        else:
            name = str(item or "").strip()
        if name:
            names.add(name)
    return names


def _resolve_declared_output_kind(
    *,
    declared_outputs: List[Dict[str, Any]],
    output_node_id: Optional[str],
    output_name: Optional[str],
) -> str:
    def _normalize_declared_kind(
        *,
        item: Dict[str, Any],
        resolved_node_id: str,
        resolved_output_name: str,
    ) -> str:
        raw_kind = str(item.get("output_kind") or item.get("outputKind") or OUTPUT_KIND_DATASET).strip().lower()
        try:
            resolved = resolve_output_kind(raw_kind)
        except ValueError:
            return OUTPUT_KIND_DATASET
        if resolved.used_alias:
            logger.warning(
                "Output kind alias normalized: raw=%s normalized=%s node_id=%s output_name=%s",
                resolved.raw_kind,
                resolved.normalized_kind,
                resolved_node_id,
                resolved_output_name,
            )
        return resolved.normalized_kind

    node_id = str(output_node_id or "").strip()
    name = str(output_name or "").strip()
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
        if node_id and declared_node_id == node_id:
            return _normalize_declared_kind(
                item=item,
                resolved_node_id=node_id,
                resolved_output_name=name or declared_name,
            )
        if name and declared_name and declared_name == name:
            return _normalize_declared_kind(
                item=item,
                resolved_node_id=node_id or declared_node_id,
                resolved_output_name=name,
            )
    return OUTPUT_KIND_DATASET


def _validate_output_kind_metadata(
    *,
    output_kind: str,
    output_metadata: Dict[str, Any],
    node_id: str,
) -> None:
    errors = validate_output_payload(kind=output_kind, payload=output_metadata)
    if errors:
        details = "; ".join(str(item) for item in errors)
        raise ValueError(f"Invalid output metadata for node {node_id} ({output_kind}): {details}")


def build_output_validation_payload(
    worker: Any,
    *,
    stage: str,
    errors: List[str],
    job: PipelineJob,
    node_id: str,
    execution_semantics: str,
    output_name: Optional[str] = None,
) -> Dict[str, Any]:
    stage_config = {
        "schema_contract": (
            "Pipeline schema contract failed",
            "PIPELINE_SCHEMA_CONTRACT_FAILED",
        ),
        "expectations": (
            "Pipeline expectations failed",
            "PIPELINE_EXPECTATIONS_FAILED",
        ),
        "output_metadata": (
            "Dataset output metadata invalid",
            "PIPELINE_DATASET_OUTPUT_METADATA_INVALID",
        ),
    }
    message, external_code = stage_config.get(
        stage,
        ("Pipeline output validation failed", "PIPELINE_OUTPUT_VALIDATION_FAILED"),
    )
    context: Dict[str, Any] = {"execution_semantics": execution_semantics}
    if output_name:
        context["output_name"] = output_name
    return worker._build_error_payload(
        message=message,
        errors=errors,
        code=ErrorCode.REQUEST_VALIDATION_FAILED,
        category=ErrorCategory.INPUT,
        status_code=422,
        external_code=external_code,
        stage=stage,
        job=job,
        node_id=node_id,
        context=context,
    )


async def prepare_output_work_item(
    worker: Any,
    *,
    mode_label: str,
    job: PipelineJob,
    definition: Dict[str, Any],
    declared_outputs: List[Dict[str, Any]],
    output_nodes: Dict[str, Dict[str, Any]],
    node_id: str,
    output_df: DataFrame,
    pipeline_ref: str,
    output_write_mode: str,
    execution_semantics: str,
    has_incremental_input: bool,
    incremental_inputs_have_additive_updates: Optional[bool],
    preview_limit: int,
    temp_dirs: List[str],
    persist_output: bool,
    persisted_dfs: List[DataFrame],
) -> Dict[str, Any]:
    if persist_output and _PYSPARK_AVAILABLE:
        try:
            output_df = output_df.persist(StorageLevel.DISK_ONLY)
            persisted_dfs.append(output_df)
        except Exception as exc:
            logger.warning(
                "Failed to persist output dataframe for node %s: %s",
                node_id,
                exc,
                exc_info=True,
            )

    schema_columns = _schema_from_dataframe(output_df)
    delta_row_count = int(
        await worker._run_spark(lambda: output_df.count(), label=f"count:{mode_label}:{node_id}")
    )
    schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
    output_ops = worker._build_table_ops(output_df)
    contract_errors = await worker._run_spark(
        lambda: validate_schema_contract(output_ops, schema_contract),
        label=f"schema_contract:{mode_label}:{node_id}",
    )
    if contract_errors:
        raise _OutputNodeValidationError(
            stage="schema_contract",
            node_id=node_id,
            errors=contract_errors,
            row_count=delta_row_count,
            payload=build_output_validation_payload(
                worker,
                stage="schema_contract",
                errors=contract_errors,
                job=job,
                node_id=node_id,
                execution_semantics=execution_semantics,
            ),
        )

    output_meta = output_nodes.get(node_id) or {}
    metadata = output_meta.get("metadata") or {}
    output_name = (
        metadata.get("outputName")
        or metadata.get("datasetName")
        or output_meta.get("title")
        or job.output_dataset_name
    )
    output_kind = _resolve_declared_output_kind(
        declared_outputs=declared_outputs,
        output_node_id=node_id,
        output_name=str(output_name or ""),
    )
    _validate_output_kind_metadata(
        output_kind=output_kind,
        output_metadata=metadata,
        node_id=str(node_id),
    )
    pk_semantics = resolve_pk_semantics(
        execution_semantics=execution_semantics,
        definition=definition,
        output_metadata=metadata,
    )
    delete_column = resolve_delete_column(
        definition=definition,
        output_metadata=metadata,
    )
    pk_columns = resolve_pk_columns(
        definition=definition,
        output_metadata=metadata,
        output_name=output_name,
        output_node_id=node_id,
        declared_outputs=declared_outputs,
    )
    available_columns = output_ops.columns
    pk_semantic_errors = validate_pk_semantics(
        available_columns=available_columns,
        pk_semantics=pk_semantics,
        pk_columns=pk_columns,
        delete_column=delete_column,
    )
    expectations = build_expectations_with_pk(
        definition=definition,
        output_metadata=metadata,
        output_name=output_name,
        output_node_id=node_id,
        declared_outputs=declared_outputs,
        pk_semantics=pk_semantics,
        delete_column=delete_column,
        pk_columns=pk_columns,
        available_columns=available_columns,
    )
    expectation_errors = pk_semantic_errors + await worker._run_spark(
        lambda: validate_expectations(output_ops, expectations),
        label=f"expectations:{mode_label}:{node_id}",
    )
    fk_errors = await worker._evaluate_fk_expectations(
        expectations=expectations,
        output_df=output_df,
        db_name=job.db_name,
        branch=job.branch or "main",
        temp_dirs=temp_dirs,
    )
    expectation_errors = expectation_errors + fk_errors
    if expectation_errors:
        raise _OutputNodeValidationError(
            stage="expectations",
            node_id=node_id,
            errors=expectation_errors,
            row_count=delta_row_count,
            payload=build_output_validation_payload(
                worker,
                stage="expectations",
                errors=expectation_errors,
                job=job,
                node_id=node_id,
                execution_semantics=execution_semantics,
            ),
        )

    dataset_name = str(output_name or job.output_dataset_name)
    write_mode_requested = output_write_mode
    write_mode_resolved = output_write_mode
    runtime_write_mode = output_write_mode
    write_policy_hash: Optional[str] = None
    pk_columns_policy: List[str] = []
    post_filtering_column: Optional[str] = None
    if output_kind == OUTPUT_KIND_DATASET:
        dataset_policy = resolve_dataset_write_policy(
            definition=definition,
            output_metadata=metadata,
            execution_semantics=execution_semantics,
            has_incremental_input=has_incremental_input,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        )
        metadata = dict(dataset_policy.normalized_metadata)
        write_mode_requested = dataset_policy.requested_write_mode
        write_mode_resolved = dataset_policy.resolved_write_mode.value
        runtime_write_mode = dataset_policy.runtime_write_mode
        write_policy_hash = dataset_policy.policy_hash
        pk_columns_policy = list(dataset_policy.primary_key_columns)
        post_filtering_column = dataset_policy.post_filtering_column
        dataset_output_errors = validate_dataset_output_metadata(
            definition=definition,
            output_metadata=metadata,
            execution_semantics=execution_semantics,
            has_incremental_input=has_incremental_input,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            available_columns=_schema_column_names(schema_columns),
        )
        if dataset_output_errors:
            raise _OutputNodeValidationError(
                stage="output_metadata",
                node_id=node_id,
                errors=dataset_output_errors,
                row_count=delta_row_count,
                payload=build_output_validation_payload(
                    worker,
                    stage="output_metadata",
                    errors=dataset_output_errors,
                    job=job,
                    node_id=node_id,
                    execution_semantics=execution_semantics,
                    output_name=dataset_name,
                ),
            )
        output_format = dataset_policy.output_format
        partition_cols = list(dataset_policy.partition_by)
    else:
        output_format = worker._resolve_output_format(definition=definition, output_metadata=metadata)
        partition_cols = worker._resolve_partition_columns(
            definition=definition,
            output_metadata=metadata,
        )

    schema_hash = _hash_schema_columns(schema_columns)
    sample_rows = await worker._run_spark(
        lambda: output_df.limit(preview_limit).collect(),
        label=f"collect:{mode_label}:{node_id}",
    )
    output_sample = [row.asDict(recursive=True) for row in sample_rows]
    sample_row_count = len(output_sample)
    column_stats = compute_column_stats(rows=output_sample, columns=schema_columns)
    safe_name = dataset_name.replace(" ", "_")
    artifact_prefix = f"pipelines/{job.db_name}/{pipeline_ref}/{safe_name}"
    return {
        "node_id": node_id,
        "output_name": output_name,
        "output_kind": output_kind,
        "output_metadata": dict(metadata),
        "dataset_name": dataset_name,
        "output_df": output_df,
        "artifact_prefix": artifact_prefix,
        "output_format": output_format,
        "partition_columns": partition_cols,
        "write_mode_requested": write_mode_requested,
        "write_mode_resolved": write_mode_resolved,
        "runtime_write_mode": runtime_write_mode,
        "write_policy_hash": write_policy_hash,
        "pk_columns": pk_columns_policy,
        "post_filtering_column": post_filtering_column,
        "has_incremental_input": has_incremental_input,
        "incremental_inputs_have_additive_updates": incremental_inputs_have_additive_updates,
        "delta_row_count": delta_row_count,
        "row_count": delta_row_count,
        "schema_columns": schema_columns,
        "columns": schema_columns,
        "schema_hash": schema_hash,
        "schema_json": {"columns": schema_columns},
        "output_sample": output_sample,
        "rows": output_sample,
        "sample_row_count": sample_row_count,
        "column_stats": column_stats,
    }


def log_output_validation_failure(
    *,
    mode_label: str,
    failure: _OutputNodeValidationError,
) -> None:
    stage_messages = {
        "schema_contract": "Pipeline schema contract failed",
        "expectations": "Pipeline expectations failed",
        "output_metadata": "Pipeline dataset output metadata invalid",
    }
    suffix = " (build)" if mode_label == "build" else ""
    logger.error(
        "%s%s: %s",
        stage_messages.get(failure.stage, "Pipeline output validation failed"),
        suffix,
        failure.errors,
    )


async def record_build_output_failure(
    worker: Any,
    *,
    job: PipelineJob,
    failure: _OutputNodeValidationError,
    input_commit_payload: Optional[List[Dict[str, Any]]],
    inputs_payload: Dict[str, Any],
    record_run: Callable[..., Any],
    record_artifact: Callable[..., Any],
    emit_job_event: Callable[..., Any],
) -> None:
    await record_run(
        job_id=job.job_id,
        mode="build",
        status="FAILED",
        node_id=failure.node_id,
        row_count=failure.row_count,
        output_json=failure.payload,
        input_lakefs_commits=input_commit_payload,
        finished_at=utcnow(),
    )
    await record_artifact(
        status="FAILED",
        errors=failure.errors,
        inputs=inputs_payload,
    )
    await emit_job_event(status="FAILED", errors=failure.errors)
    worker._log_output_validation_failure(mode_label="build", failure=failure)


async def record_deploy_output_failure(
    worker: Any,
    *,
    job: PipelineJob,
    failure: _OutputNodeValidationError,
    input_commit_payload: Optional[List[Dict[str, Any]]],
    inputs_payload: Dict[str, Any],
    record_build: Callable[..., Any],
    record_run: Callable[..., Any],
    record_artifact: Callable[..., Any],
    emit_job_event: Callable[..., Any],
) -> None:
    await record_build(
        status="FAILED",
        output_json=failure.payload,
    )
    await record_run(
        job_id=job.job_id,
        mode="deploy",
        status="FAILED",
        node_id=failure.node_id,
        row_count=failure.row_count,
        output_json=failure.payload,
        input_lakefs_commits=input_commit_payload,
        finished_at=utcnow(),
    )
    await record_artifact(
        status="FAILED",
        errors=failure.errors,
        inputs=inputs_payload,
    )
    await emit_job_event(status="FAILED", errors=failure.errors)
    worker._log_output_validation_failure(mode_label="deploy", failure=failure)


async def prepare_output_work(
    worker: Any,
    *,
    mode_label: str,
    job: PipelineJob,
    lock: Any,
    tables: Dict[str, DataFrame],
    target_node_ids: List[str],
    output_nodes: Dict[str, Dict[str, Any]],
    definition: Dict[str, Any],
    declared_outputs: List[Dict[str, Any]],
    pipeline_ref: str,
    output_write_mode: str,
    execution_semantics: str,
    has_incremental_input: bool,
    incremental_inputs_have_additive_updates: Optional[bool],
    preview_limit: int,
    temp_dirs: List[str],
    persisted_dfs: List[DataFrame],
    input_commit_payload: Optional[List[Dict[str, Any]]],
    inputs_payload: Dict[str, Any],
    record_run: Callable[..., Any],
    record_artifact: Callable[..., Any],
    emit_job_event: Callable[..., Any],
    record_build: Optional[Callable[..., Any]] = None,
    persist_output: bool,
) -> Optional[Tuple[List[Dict[str, Any]], bool]]:
    output_work: List[Dict[str, Any]] = []
    has_output_rows = False
    for node_id in target_node_ids:
        if lock:
            lock.raise_if_lost()
        output_df = tables.get(node_id, worker._empty_dataframe())
        try:
            output_item = await worker._prepare_output_work_item(
                mode_label=mode_label,
                job=job,
                definition=definition,
                declared_outputs=declared_outputs,
                output_nodes=output_nodes,
                node_id=node_id,
                output_df=output_df,
                pipeline_ref=pipeline_ref,
                output_write_mode=output_write_mode,
                execution_semantics=execution_semantics,
                has_incremental_input=has_incremental_input,
                incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
                preview_limit=preview_limit,
                temp_dirs=temp_dirs,
                persist_output=persist_output,
                persisted_dfs=persisted_dfs,
            )
        except _OutputNodeValidationError as failure:
            if mode_label == "deploy":
                if record_build is None:
                    raise RuntimeError("record_build callback is required for deploy output failures")
                await worker._record_deploy_output_failure(
                    job=job,
                    failure=failure,
                    input_commit_payload=input_commit_payload,
                    inputs_payload=inputs_payload,
                    record_build=record_build,
                    record_run=record_run,
                    record_artifact=record_artifact,
                    emit_job_event=emit_job_event,
                )
            else:
                await worker._record_build_output_failure(
                    job=job,
                    failure=failure,
                    input_commit_payload=input_commit_payload,
                    inputs_payload=inputs_payload,
                    record_run=record_run,
                    record_artifact=record_artifact,
                    emit_job_event=emit_job_event,
                )
            return None
        if int(output_item.get("delta_row_count") or 0) > 0:
            has_output_rows = True
        output_work.append(output_item)
    return output_work, has_output_rows
