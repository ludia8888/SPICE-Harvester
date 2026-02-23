from __future__ import annotations

import logging
import shutil
from typing import Any, Callable, Dict, List, Optional

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.models.pipeline_job import PipelineJob
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)


class PipelineRunDomain:
    def __init__(self, worker: Any) -> None:
        self._worker = worker

    async def execute_job(self, job: PipelineJob) -> None:
        worker = self._worker
        worker._validate_execution_prerequisites()

        definition = job.definition_json or {}
        prev_spark_conf, prev_cast_mode = worker._apply_job_overrides(definition)
        tables: Dict[str, Any] = {}
        input_snapshots: List[Dict[str, Any]] = []
        input_sampling: Dict[str, Any] = {}
        input_commit_payload: Optional[List[Dict[str, Any]]] = None
        temp_dirs: List[str] = []
        persisted_dfs: List[Any] = []
        lock: Optional[Any] = None

        run_mode, is_preview, is_build = worker._resolve_run_mode(job=job)

        async def _noop(**kwargs: Any) -> None:
            _ = kwargs
            return None

        record_preview: Callable[..., Any] = _noop
        record_build: Callable[..., Any] = _noop
        record_run: Callable[..., Any] = _noop
        record_artifact: Callable[..., Any] = _noop
        emit_job_event: Callable[..., Any] = _noop

        try:
            (
                preview_meta,
                nodes,
                incoming,
                parameters,
                output_nodes,
                order,
            ) = worker._build_execution_graph(definition=definition)
            execution_semantics = worker._resolve_execution_semantics(job=job, definition=definition)

            (
                target_node_ids,
                required_node_ids,
                order_run,
                requested_node_error,
            ) = worker._plan_execution_scope(
                job=job,
                nodes=nodes,
                order=order,
                incoming=incoming,
                output_nodes=output_nodes,
                execution_semantics=execution_semantics,
            )

            pipeline_context = await worker._resolve_execution_pipeline_context(
                job=job,
                definition=definition,
                execution_semantics=execution_semantics,
            )
            if not pipeline_context:
                return
            (
                resolved_pipeline_id,
                pipeline_ref,
                watermark_column,
                previous_watermark,
                previous_watermark_keys,
                previous_input_commits,
            ) = pipeline_context

            spark_conf = worker._collect_spark_conf()
            code_version = worker._resolve_code_version()
            pipeline_spec_commit_id = (
                str(job.definition_commit_id).strip() if job.definition_commit_id else None
            )
            pipeline_spec_hash = str(job.definition_hash).strip() if job.definition_hash else None
            declared_outputs = definition.get("outputs") if isinstance(definition.get("outputs"), list) else []
            preview_sampling_seed = worker._preview_sampling_seed(job.job_id)
            use_lakefs_diff = worker.use_lakefs_diff

            (
                record_preview,
                record_build,
                record_run,
                record_artifact,
                emit_job_event,
                artifact_state,
            ) = worker._build_execution_recorders(
                job=job,
                resolved_pipeline_id=resolved_pipeline_id,
                pipeline_ref=pipeline_ref,
                run_mode=run_mode,
                execution_semantics=execution_semantics,
                declared_outputs=declared_outputs,
                pipeline_spec_hash=pipeline_spec_hash,
                pipeline_spec_commit_id=pipeline_spec_commit_id,
                spark_conf=spark_conf,
                code_version=code_version,
            )

            run_ref = await worker._start_execution_tracking(
                job=job,
                resolved_pipeline_id=resolved_pipeline_id,
                run_mode=run_mode,
                is_preview=is_preview,
                is_build=is_build,
                artifact_state=artifact_state,
                record_preview=record_preview,
                record_build=record_build,
                record_run=record_run,
                record_artifact=record_artifact,
            )

            validation_errors = worker._validate_definition(definition, require_output=not is_preview)
            if requested_node_error:
                validation_errors.append(requested_node_error)
            if execution_semantics in {"incremental", "streaming"} and not watermark_column:
                validation_errors.append(
                    f"{execution_semantics} pipelines require settings.watermarkColumn to be configured"
                )
            validation_errors.extend(worker._validate_required_subgraph(nodes, incoming, required_node_ids))
            if validation_errors:
                await worker._record_validation_failure(
                    job=job,
                    validation_errors=validation_errors,
                    execution_semantics=execution_semantics,
                    pipeline_spec_hash=pipeline_spec_hash,
                    pipeline_spec_commit_id=pipeline_spec_commit_id,
                    is_preview=is_preview,
                    is_build=is_build,
                    run_mode=run_mode,
                    record_preview=record_preview,
                    record_build=record_build,
                    record_run=record_run,
                    record_artifact=record_artifact,
                    emit_job_event=emit_job_event,
                )
                return

            if run_mode in {"build", "deploy"}:
                lock = await worker._acquire_pipeline_lock(job)

            nodes_executed = await worker._execute_ordered_nodes(
                job=job,
                order_run=order_run,
                nodes=nodes,
                tables=tables,
                incoming=incoming,
                parameters=parameters,
                preview_meta=preview_meta,
                preview_sampling_seed=preview_sampling_seed,
                input_snapshots=input_snapshots,
                input_sampling=input_sampling,
                temp_dirs=temp_dirs,
                previous_input_commits=previous_input_commits,
                use_lakefs_diff=use_lakefs_diff,
                execution_semantics=execution_semantics,
                watermark_column=watermark_column,
                previous_watermark=previous_watermark,
                previous_watermark_keys=previous_watermark_keys,
                is_preview=is_preview,
                is_build=is_build,
                run_mode=run_mode,
                input_commit_payload=input_commit_payload,
                record_preview=record_preview,
                record_build=record_build,
                record_run=record_run,
                record_artifact=record_artifact,
                emit_job_event=emit_job_event,
            )
            if not nodes_executed:
                return

            (
                input_commit_payload,
                inputs_payload,
                diff_empty_inputs,
                has_incremental_input,
                incremental_inputs_have_additive_updates,
                preview_limit,
            ) = worker._derive_post_node_execution_state(
                input_snapshots=input_snapshots,
                execution_semantics=execution_semantics,
                preview_limit=job.preview_limit,
            )

            if is_preview:
                await worker._run_preview_mode(
                    job=job,
                    definition=definition,
                    preview_meta=preview_meta,
                    nodes=nodes,
                    target_node_ids=target_node_ids,
                    tables=tables,
                    input_snapshots=input_snapshots,
                    input_sampling=input_sampling,
                    temp_dirs=temp_dirs,
                    preview_limit=preview_limit,
                    execution_semantics=execution_semantics,
                    declared_outputs=declared_outputs,
                    pipeline_spec_hash=pipeline_spec_hash,
                    pipeline_spec_commit_id=pipeline_spec_commit_id,
                    code_version=code_version,
                    spark_conf=spark_conf,
                    input_commit_payload=input_commit_payload,
                    inputs_payload=inputs_payload,
                    record_preview=record_preview,
                    record_run=record_run,
                    record_artifact=record_artifact,
                )
                return

            if is_build:
                await worker._run_build_mode(
                    job=job,
                    lock=lock,
                    tables=tables,
                    target_node_ids=target_node_ids,
                    output_nodes=output_nodes,
                    definition=definition,
                    declared_outputs=declared_outputs,
                    pipeline_ref=pipeline_ref,
                    run_ref=run_ref,
                    execution_semantics=execution_semantics,
                    diff_empty_inputs=diff_empty_inputs,
                    has_incremental_input=has_incremental_input,
                    incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
                    preview_limit=preview_limit,
                    temp_dirs=temp_dirs,
                    persisted_dfs=persisted_dfs,
                    input_snapshots=input_snapshots,
                    input_commit_payload=input_commit_payload,
                    inputs_payload=inputs_payload,
                    pipeline_spec_hash=pipeline_spec_hash,
                    pipeline_spec_commit_id=pipeline_spec_commit_id,
                    code_version=code_version,
                    spark_conf=spark_conf,
                    record_build=record_build,
                    record_run=record_run,
                    record_artifact=record_artifact,
                    emit_job_event=emit_job_event,
                )
                return

            await worker._run_deploy_mode(
                job=job,
                lock=lock,
                tables=tables,
                target_node_ids=target_node_ids,
                output_nodes=output_nodes,
                definition=definition,
                declared_outputs=declared_outputs,
                pipeline_ref=pipeline_ref,
                run_ref=run_ref,
                execution_semantics=execution_semantics,
                diff_empty_inputs=diff_empty_inputs,
                has_incremental_input=has_incremental_input,
                incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
                preview_limit=preview_limit,
                temp_dirs=temp_dirs,
                persisted_dfs=persisted_dfs,
                input_snapshots=input_snapshots,
                input_commit_payload=input_commit_payload,
                inputs_payload=inputs_payload,
                pipeline_spec_hash=pipeline_spec_hash,
                pipeline_spec_commit_id=pipeline_spec_commit_id,
                code_version=code_version,
                spark_conf=spark_conf,
                resolved_pipeline_id=resolved_pipeline_id,
                previous_watermark=previous_watermark,
                previous_watermark_keys=previous_watermark_keys,
                watermark_column=watermark_column,
                record_build=record_build,
                record_run=record_run,
                record_artifact=record_artifact,
                emit_job_event=emit_job_event,
            )
        except Exception as exc:
            await self.record_execution_failure(
                job=job,
                exc=exc,
                is_preview=is_preview,
                is_build=is_build,
                run_mode=run_mode,
                input_commit_payload=input_commit_payload,
                record_preview=record_preview,
                record_build=record_build,
                record_run=record_run,
                emit_job_event=emit_job_event,
            )
            raise
        finally:
            await self.cleanup_execution_resources(
                lock=lock,
                temp_dirs=temp_dirs,
                persisted_dfs=persisted_dfs,
                prev_spark_conf=prev_spark_conf,
                prev_cast_mode=prev_cast_mode,
            )

    async def record_execution_failure(
        self,
        *,
        job: PipelineJob,
        exc: Exception,
        is_preview: bool,
        is_build: bool,
        run_mode: str,
        input_commit_payload: Optional[List[Dict[str, Any]]],
        record_preview: Callable[..., Any],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        worker = self._worker
        execution_payload = worker._build_error_payload(
            message="Pipeline execution failed",
            errors=[str(exc)],
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=500,
            external_code="PIPELINE_EXECUTION_FAILED",
            stage="execution",
            job=job,
            context={"exception_type": exc.__class__.__name__},
        )
        if is_preview:
            await record_preview(
                status="FAILED",
                row_count=0,
                sample_json=execution_payload,
                job_id=job.job_id,
                node_id=job.node_id,
            )
        elif not is_build:
            await record_build(
                status="FAILED",
                output_json=execution_payload,
            )
        await record_run(
            job_id=job.job_id,
            mode=run_mode,
            status="FAILED",
            node_id=job.node_id,
            sample_json=execution_payload if is_preview else None,
            output_json=execution_payload if not is_preview else None,
            input_lakefs_commits=input_commit_payload,
            finished_at=utcnow(),
        )
        await emit_job_event(status="FAILED", errors=[str(exc)])
        logger.exception("Pipeline execution failed: %s", exc)

    async def cleanup_execution_resources(
        self,
        *,
        lock: Optional[Any],
        temp_dirs: List[str],
        persisted_dfs: List[Any],
        prev_spark_conf: Dict[str, Optional[str]],
        prev_cast_mode: str,
    ) -> None:
        worker = self._worker
        if lock:
            await lock.release()
        for path in temp_dirs:
            shutil.rmtree(path, ignore_errors=True)
        for df in persisted_dfs:
            try:
                df.unpersist(blocking=False)
            except Exception as exc:
                logger.warning("Failed to unpersist persisted dataframe: %s", exc, exc_info=True)
        if worker.spark and prev_spark_conf:
            for key, value in prev_spark_conf.items():
                try:
                    if value is None:
                        worker.spark.conf.unset(key)
                    else:
                        worker.spark.conf.set(key, value)
                except Exception as exc:
                    logger.warning(
                        "Failed to restore Spark conf key %s: %s",
                        key,
                        exc,
                        exc_info=True,
                    )
        worker.cast_mode = prev_cast_mode
