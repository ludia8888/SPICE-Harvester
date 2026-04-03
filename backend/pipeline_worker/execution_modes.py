from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.services.pipeline.pipeline_profiler import compute_column_stats
from shared.services.pipeline.pipeline_validation_utils import validate_schema_contract
from shared.services.core.write_path_contract import (
    build_write_path_contract,
    followup_completed,
    followup_degraded,
)

logger = logging.getLogger(__name__)


async def run_build_mode(
    worker: Any,
    *,
    job: Any,
    lock: Any,
    tables: Dict[str, Any],
    target_node_ids: List[str],
    output_nodes: Dict[str, Dict[str, Any]],
    definition: Dict[str, Any],
    declared_outputs: List[Dict[str, Any]],
    pipeline_ref: str,
    run_ref: str,
    execution_semantics: str,
    diff_empty_inputs: bool,
    has_incremental_input: bool,
    incremental_inputs_have_additive_updates: Optional[bool],
    preview_limit: int,
    temp_dirs: List[str],
    persisted_dfs: List[Any],
    input_snapshots: List[Dict[str, Any]],
    input_commit_payload: Optional[List[Dict[str, Any]]],
    inputs_payload: Dict[str, Any],
    pipeline_spec_hash: Optional[str],
    pipeline_spec_commit_id: Optional[str],
    code_version: Optional[str],
    spark_conf: Dict[str, Any],
    record_build: Any,
    record_run: Any,
    record_artifact: Any,
    emit_job_event: Any,
    resolve_lakefs_repository: Any,
    safe_lakefs_ref: Any,
    build_s3_uri: Any,
    utcnow_fn: Any,
) -> None:
    if not worker.storage or not worker.lakefs_client:
        raise RuntimeError("lakeFS services are not configured")
    if lock:
        lock.raise_if_lost()

    artifact_repo = resolve_lakefs_repository()
    await worker.storage.create_bucket(artifact_repo)
    output_write_mode = "append" if execution_semantics in {"incremental", "streaming"} else "overwrite"
    base_branch = safe_lakefs_ref(job.branch or "main")
    prepared = await worker._prepare_build_output_work(
        job=job,
        lock=lock,
        tables=tables,
        target_node_ids=target_node_ids,
        output_nodes=output_nodes,
        definition=definition,
        declared_outputs=declared_outputs,
        pipeline_ref=pipeline_ref,
        output_write_mode=output_write_mode,
        execution_semantics=execution_semantics,
        has_incremental_input=has_incremental_input,
        incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        preview_limit=preview_limit,
        temp_dirs=temp_dirs,
        persisted_dfs=persisted_dfs,
        input_commit_payload=input_commit_payload,
        inputs_payload=inputs_payload,
        record_run=record_run,
        record_artifact=record_artifact,
        emit_job_event=emit_job_event,
    )
    if not prepared:
        return
    output_work, has_output_rows = prepared

    no_op_candidate = execution_semantics in {"incremental", "streaming"} and diff_empty_inputs
    if no_op_candidate and not has_output_rows:
        no_op_payload = {
            "outputs": [],
            "definition_hash": job.definition_hash,
            "branch": job.branch or "main",
            "execution_semantics": execution_semantics,
            "pipeline_spec_hash": pipeline_spec_hash,
            "pipeline_spec_commit_id": pipeline_spec_commit_id,
            "code_version": code_version,
            "spark_conf": spark_conf,
            "no_op": True,
            "input_snapshots": input_snapshots,
        }
        await record_build(status="SUCCESS", output_json=no_op_payload)
        await record_run(
            job_id=job.job_id,
            mode="build",
            status="SUCCESS",
            node_id=job.node_id,
            input_lakefs_commits=input_commit_payload,
            output_json=no_op_payload,
            finished_at=utcnow_fn(),
        )
        await record_artifact(
            status="SUCCESS",
            outputs=[],
            inputs=inputs_payload,
        )
        await emit_job_event(status="SUCCESS", output={"no_op": True, "outputs": []})
        return

    build_branch = await worker._create_build_branch(
        artifact_repo=artifact_repo,
        pipeline_ref=pipeline_ref,
        run_ref=run_ref,
        base_branch=base_branch,
    )
    build_outputs = await worker._materialize_build_outputs(
        job=job,
        artifact_repo=artifact_repo,
        build_branch=build_branch,
        base_branch=base_branch,
        output_work=output_work,
        output_write_mode=output_write_mode,
        execution_semantics=execution_semantics,
        incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
    )

    if lock:
        lock.raise_if_lost()
    commit_id = await worker.lakefs_client.commit(
        repository=artifact_repo,
        branch=build_branch,
        message=f"Build pipeline {job.db_name}/{pipeline_ref} ({job.job_id})",
        metadata={
            "pipeline_id": pipeline_ref,
            "pipeline_job_id": job.job_id,
            "db_name": job.db_name,
            "mode": "build",
        },
    )
    for item in build_outputs:
        artifact_prefix = str(item.get("artifact_prefix") or "").lstrip("/")
        if artifact_prefix:
            item["artifact_commit_key"] = build_s3_uri(artifact_repo, f"{commit_id}/{artifact_prefix}")

    artifact_id = await record_artifact(
        status="SUCCESS",
        outputs=build_outputs,
        inputs=inputs_payload,
        lakefs={
            "repository": artifact_repo,
            "branch": build_branch,
            "commit_id": commit_id,
        },
    )

    output_json = {
        "outputs": build_outputs,
        "definition_hash": job.definition_hash,
        "branch": job.branch or "main",
        "execution_semantics": execution_semantics,
        "pipeline_spec_hash": pipeline_spec_hash,
        "pipeline_spec_commit_id": pipeline_spec_commit_id,
        "code_version": code_version,
        "spark_conf": spark_conf,
        "lakefs": {
            "repository": artifact_repo,
            "base_branch": base_branch,
            "build_branch": build_branch,
            "commit_id": commit_id,
        },
        "ontology": (
            (definition.get("__build_meta__") or {}).get("ontology")
            if isinstance(definition.get("__build_meta__"), dict)
            else None
        ),
        "input_snapshots": input_snapshots,
    }
    if artifact_id:
        output_json["artifact_id"] = artifact_id

    await record_run(
        job_id=job.job_id,
        mode="build",
        status="SUCCESS",
        node_id=job.node_id,
        input_lakefs_commits=input_commit_payload,
        output_lakefs_commit_id=commit_id,
        output_json=output_json,
        finished_at=utcnow_fn(),
    )
    output_payload = {"outputs": build_outputs}
    if artifact_id:
        output_payload["artifact_id"] = artifact_id
    await emit_job_event(
        status="SUCCESS",
        lakefs={
            "repository": artifact_repo,
            "base_branch": base_branch,
            "build_branch": build_branch,
            "commit_id": commit_id,
        },
        output=output_payload,
    )


async def run_preview_mode(
    worker: Any,
    *,
    job: Any,
    definition: Dict[str, Any],
    preview_meta: Dict[str, Any],
    nodes: Dict[str, Dict[str, Any]],
    target_node_ids: List[str],
    tables: Dict[str, Any],
    input_snapshots: List[Dict[str, Any]],
    input_sampling: Dict[str, Any],
    temp_dirs: List[str],
    preview_limit: int,
    execution_semantics: str,
    declared_outputs: List[Dict[str, Any]],
    pipeline_spec_hash: Optional[str],
    pipeline_spec_commit_id: Optional[str],
    code_version: Optional[str],
    spark_conf: Dict[str, Any],
    input_commit_payload: Optional[List[Dict[str, Any]]],
    inputs_payload: Dict[str, Any],
    record_preview: Any,
    record_run: Any,
    record_artifact: Any,
    schema_from_dataframe: Any,
    resolve_declared_output_kind: Any,
    validate_output_kind_metadata: Any,
    resolve_pk_semantics: Any,
    resolve_delete_column: Any,
    resolve_pk_columns: Any,
    validate_pk_semantics: Any,
    build_expectations_with_pk: Any,
    validate_expectations: Any,
    hash_schema_columns: Any,
    utcnow_fn: Any,
) -> None:
    skip_production_checks = worker._resolve_preview_flag(
        preview_meta,
        snake_case_key="skip_production_checks",
        camel_case_key="skipProductionChecks",
        default=True,
    )
    skip_output_recording = worker._resolve_preview_flag(
        preview_meta,
        snake_case_key="skip_output_recording",
        camel_case_key="skipOutputRecording",
        default=True,
    )
    primary_id = target_node_ids[0] if target_node_ids else None
    output_df = tables.get(primary_id) if primary_id else worker._empty_dataframe()
    schema_columns = schema_from_dataframe(output_df)
    row_count = int(await worker._run_spark(lambda: output_df.count(), label=f"count:preview:{primary_id}"))
    output_ops = worker._build_table_ops(output_df)
    if skip_production_checks:
        contract_errors: List[str] = []
    else:
        schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
        contract_errors = await worker._run_spark(
            lambda: validate_schema_contract(output_ops, schema_contract),
            label=f"schema_contract:preview:{primary_id}",
        )
    if contract_errors:
        contract_payload = worker._build_error_payload(
            message="Pipeline schema contract failed",
            errors=contract_errors,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            status_code=422,
            external_code="PIPELINE_SCHEMA_CONTRACT_FAILED",
            stage="schema_contract",
            job=job,
            node_id=primary_id,
            context={"execution_semantics": execution_semantics},
        )
        await record_preview(
            status="FAILED",
            row_count=row_count,
            sample_json=contract_payload,
            job_id=job.job_id,
            node_id=job.node_id,
        )
        await record_run(
            job_id=job.job_id,
            mode="preview",
            status="FAILED",
            node_id=job.node_id,
            row_count=row_count,
            sample_json=contract_payload,
            input_lakefs_commits=input_commit_payload,
            finished_at=utcnow_fn(),
        )
        if not skip_output_recording:
            await record_artifact(
                status="FAILED",
                errors=contract_errors,
                inputs=inputs_payload,
            )
        logger.error("Pipeline schema contract failed: %s", contract_errors)
        return

    output_meta = nodes.get(primary_id, {}) if primary_id else {}
    output_metadata = output_meta.get("metadata") or {}
    output_name = (
        output_metadata.get("outputName")
        or output_metadata.get("datasetName")
        or output_meta.get("title")
        or (primary_id or "preview_output")
    )
    output_kind = resolve_declared_output_kind(
        declared_outputs=declared_outputs,
        output_node_id=primary_id,
        output_name=str(output_name or ""),
    )
    validate_output_kind_metadata(
        output_kind=output_kind,
        output_metadata=output_metadata,
        node_id=str(primary_id or "preview_output"),
    )
    expectation_errors: List[str] = []
    if not skip_production_checks:
        pk_semantics = resolve_pk_semantics(
            execution_semantics=execution_semantics,
            definition=definition,
            output_metadata=output_metadata,
        )
        delete_column = resolve_delete_column(
            definition=definition,
            output_metadata=output_metadata,
        )
        pk_columns = resolve_pk_columns(
            definition=definition,
            output_metadata=output_metadata,
            output_name=output_name,
            output_node_id=primary_id,
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
            output_metadata=output_metadata,
            output_name=output_name,
            output_node_id=primary_id,
            declared_outputs=declared_outputs,
            pk_semantics=pk_semantics,
            delete_column=delete_column,
            pk_columns=pk_columns,
            available_columns=available_columns,
        )
        expectation_errors = pk_semantic_errors + await worker._run_spark(
            lambda: validate_expectations(output_ops, expectations),
            label=f"expectations:preview:{primary_id}",
        )
        fk_errors = await worker._evaluate_fk_expectations(
            expectations=expectations,
            output_df=output_df,
            db_name=job.db_name,
            branch=job.branch or "main",
            temp_dirs=temp_dirs,
        )
        expectation_errors = expectation_errors + fk_errors
    sample_rows = await worker._run_spark(
        lambda: output_df.limit(preview_limit).collect(),
        label=f"collect:preview:{primary_id}",
    )
    output_sample = [row.asDict(recursive=True) for row in sample_rows]
    column_stats = compute_column_stats(rows=output_sample, columns=schema_columns)
    sample_payload: Dict[str, Any] = {
        "columns": schema_columns,
        "rows": output_sample,
        "job_id": job.job_id,
        "row_count": row_count,
        "sample_row_count": len(output_sample),
        "sampling_strategy": {
            "output": {"type": "limit", "limit": preview_limit},
            **({"input": input_sampling} if input_sampling else {}),
        },
        "column_stats": column_stats,
        "definition_hash": job.definition_hash,
        "branch": job.branch or "main",
        "input_snapshots": input_snapshots,
        "execution_semantics": execution_semantics,
        "pipeline_spec_hash": pipeline_spec_hash,
        "pipeline_spec_commit_id": pipeline_spec_commit_id,
        "code_version": code_version,
        "spark_conf": spark_conf,
        "production_checks_skipped": skip_production_checks,
        "output_recording_skipped": skip_output_recording,
    }
    if primary_id:
        sample_payload["node_id"] = primary_id
    if expectation_errors:
        sample_payload["expectations"] = expectation_errors
    preview_outputs = [
        {
            "node_id": primary_id,
            "output_name": output_name,
            "output_kind": output_kind,
            "dataset_name": output_name,
            "row_count": row_count,
            "columns": schema_columns,
            "schema_hash": hash_schema_columns(schema_columns),
            "schema_json": {"columns": schema_columns},
            "rows": output_sample,
            "sample_row_count": len(output_sample),
            "column_stats": column_stats,
        }
    ]
    artifact_status = "FAILED" if expectation_errors else "SUCCESS"
    if not skip_output_recording:
        artifact_id = await record_artifact(
            status=artifact_status,
            outputs=preview_outputs,
            inputs=inputs_payload,
            sampling_strategy={
                "output": {"type": "limit", "limit": preview_limit},
                **({"input": input_sampling} if input_sampling else {}),
            },
            errors=expectation_errors if expectation_errors else None,
        )
        if artifact_id:
            sample_payload["artifact_id"] = artifact_id

    await record_preview(
        status="FAILED" if expectation_errors else "SUCCESS",
        row_count=row_count,
        sample_json=sample_payload,
        job_id=job.job_id,
        node_id=job.node_id,
    )
    await record_run(
        job_id=job.job_id,
        mode="preview",
        status="FAILED" if expectation_errors else "SUCCESS",
        node_id=job.node_id,
        row_count=row_count,
        sample_json=sample_payload,
        input_lakefs_commits=input_commit_payload,
        finished_at=utcnow_fn(),
    )
    if expectation_errors:
        logger.error("Pipeline expectations failed: %s", expectation_errors)


async def run_deploy_mode(
    worker: Any,
    *,
    job: Any,
    lock: Any,
    tables: Dict[str, Any],
    target_node_ids: List[str],
    output_nodes: Dict[str, Dict[str, Any]],
    definition: Dict[str, Any],
    declared_outputs: List[Dict[str, Any]],
    pipeline_ref: str,
    run_ref: str,
    execution_semantics: str,
    diff_empty_inputs: bool,
    has_incremental_input: bool,
    incremental_inputs_have_additive_updates: Optional[bool],
    preview_limit: int,
    temp_dirs: List[str],
    persisted_dfs: List[Any],
    input_snapshots: List[Dict[str, Any]],
    input_commit_payload: Optional[List[Dict[str, Any]]],
    inputs_payload: Dict[str, Any],
    pipeline_spec_hash: Optional[str],
    pipeline_spec_commit_id: Optional[str],
    code_version: Optional[str],
    spark_conf: Dict[str, Any],
    resolved_pipeline_id: Optional[str],
    previous_watermark: Optional[Any],
    previous_watermark_keys: List[str],
    watermark_column: Optional[str],
    record_build: Any,
    record_run: Any,
    record_artifact: Any,
    emit_job_event: Any,
    resolve_lakefs_repository: Any,
    safe_lakefs_ref: Any,
    utcnow_fn: Any,
) -> None:
    if not worker.storage or not worker.lakefs_client:
        raise RuntimeError("lakeFS services are not configured")
    if lock:
        lock.raise_if_lost()

    artifact_repo = resolve_lakefs_repository()
    await worker.storage.create_bucket(artifact_repo)
    base_branch = safe_lakefs_ref(job.branch or "main")
    output_write_mode = "append" if execution_semantics in {"incremental", "streaming"} else "overwrite"
    prepared = await worker._prepare_deploy_output_work(
        job=job,
        lock=lock,
        tables=tables,
        target_node_ids=target_node_ids,
        output_nodes=output_nodes,
        definition=definition,
        declared_outputs=declared_outputs,
        pipeline_ref=pipeline_ref,
        output_write_mode=output_write_mode,
        execution_semantics=execution_semantics,
        has_incremental_input=has_incremental_input,
        incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        preview_limit=preview_limit,
        temp_dirs=temp_dirs,
        persisted_dfs=persisted_dfs,
        input_commit_payload=input_commit_payload,
        inputs_payload=inputs_payload,
        record_build=record_build,
        record_run=record_run,
        record_artifact=record_artifact,
        emit_job_event=emit_job_event,
    )
    if not prepared:
        return

    output_work, has_output_rows = prepared
    no_op_candidate = execution_semantics in {"incremental", "streaming"} and diff_empty_inputs
    if no_op_candidate and not has_output_rows:
        no_op_payload = {
            "outputs": [],
            "definition_hash": job.definition_hash,
            "branch": base_branch,
            "execution_semantics": execution_semantics,
            "pipeline_spec_hash": pipeline_spec_hash,
            "pipeline_spec_commit_id": pipeline_spec_commit_id,
            "code_version": code_version,
            "spark_conf": spark_conf,
            "no_op": True,
            "input_snapshots": input_snapshots,
        }
        await record_build(
            status="DEPLOYED",
            output_json=no_op_payload,
        )
        await record_run(
            job_id=job.job_id,
            mode="deploy",
            status="DEPLOYED",
            node_id=job.node_id,
            input_lakefs_commits=input_commit_payload,
            output_json=no_op_payload,
            finished_at=utcnow_fn(),
        )
        await emit_job_event(status="DEPLOYED", output={"no_op": True, "outputs": []})
        return

    run_branch, commit_id, merge_commit_id, staged_outputs = await worker._stage_deploy_outputs(
        job=job,
        lock=lock,
        pipeline_ref=pipeline_ref,
        run_ref=run_ref,
        artifact_repo=artifact_repo,
        base_branch=base_branch,
        output_work=output_work,
        output_write_mode=output_write_mode,
        execution_semantics=execution_semantics,
        incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
    )
    build_outputs = await worker._publish_deploy_outputs(
        job=job,
        pipeline_ref=pipeline_ref,
        artifact_repo=artifact_repo,
        base_branch=base_branch,
        merge_commit_id=merge_commit_id,
        output_write_mode=output_write_mode,
        staged_outputs=staged_outputs,
    )

    await record_build(
        status="DEPLOYED",
        output_json={
            "outputs": build_outputs,
            "definition_hash": job.definition_hash,
            "branch": base_branch,
            "execution_semantics": execution_semantics,
            "input_snapshots": input_snapshots,
            "lakefs": {
                "repository": artifact_repo,
                "base_branch": base_branch,
                "run_branch": run_branch,
                "commit_id": commit_id,
                "merge_commit_id": merge_commit_id,
            },
        },
        deployed_commit_id=merge_commit_id,
    )

    try:
        watermark_update = await worker._persist_deploy_watermarks(
            resolved_pipeline_id=resolved_pipeline_id,
            branch=job.branch or "main",
            execution_semantics=execution_semantics,
            input_snapshots=input_snapshots,
            watermark_column=watermark_column,
            previous_watermark=previous_watermark,
            previous_watermark_keys=previous_watermark_keys,
        )
    except Exception as exc:
        logger.warning(
            "Persisting deploy watermarks failed after deploy outputs were already published "
            "(job_id=%s, pipeline_id=%s): %s",
            job.job_id,
            resolved_pipeline_id,
            exc,
            exc_info=True,
        )
        watermark_update = {
            "status": "degraded",
            "error": str(exc),
        }
    write_path_contract = build_write_path_contract(
        authoritative_write="dataset_version_publish",
        followups=[
            followup_completed("deploy_watermarks", details=watermark_update)
            if watermark_update and str(watermark_update.get("status") or "").strip().lower() != "degraded"
            else followup_degraded(
                "deploy_watermarks",
                error=str((watermark_update or {}).get("error") or "deploy watermark persistence degraded"),
                details=watermark_update,
            )
        ],
    )

    await record_run(
        job_id=job.job_id,
        mode="deploy",
        status="DEPLOYED",
        node_id=job.node_id,
        input_lakefs_commits=input_commit_payload,
        output_lakefs_commit_id=merge_commit_id,
        output_json={
            "outputs": build_outputs,
            "definition_hash": job.definition_hash,
            "branch": base_branch,
            "execution_semantics": execution_semantics,
            "pipeline_spec_hash": pipeline_spec_hash,
            "pipeline_spec_commit_id": pipeline_spec_commit_id,
            "code_version": code_version,
            "spark_conf": spark_conf,
            "input_snapshots": input_snapshots,
            "watermarks": watermark_update,
            "write_path_contract": write_path_contract,
            "lakefs": {
                "repository": artifact_repo,
                "base_branch": base_branch,
                "run_branch": run_branch,
                "commit_id": commit_id,
                "merge_commit_id": merge_commit_id,
            },
        },
        finished_at=utcnow_fn(),
    )
    await emit_job_event(
        status="DEPLOYED",
        lakefs={
            "repository": artifact_repo,
            "base_branch": base_branch,
            "run_branch": run_branch,
            "commit_id": commit_id,
            "merge_commit_id": merge_commit_id,
        },
        output={"outputs": build_outputs},
    )
