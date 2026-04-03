from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from shared.errors.runtime_exception_policy import RuntimeZone, record_lineage_or_raise
from shared.models.lineage_edge_types import EDGE_PIPELINE_OUTPUT_STORED
from shared.models.pipeline_job import PipelineJob
from shared.services.pipeline.output_plugins import OUTPUT_KIND_DATASET
from shared.services.registries.lineage_store import LineageStore
from shared.services.storage.lakefs_client import LakeFSConflictError
from shared.utils.path_utils import safe_lakefs_ref
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)


async def create_build_branch(
    worker: Any,
    *,
    artifact_repo: str,
    pipeline_ref: str,
    run_ref: str,
    base_branch: str,
) -> str:
    build_branch = safe_lakefs_ref(f"build/{pipeline_ref}/{run_ref}")
    try:
        await worker.lakefs_client.create_branch(  # type: ignore[union-attr]
            repository=artifact_repo,
            name=build_branch,
            source=base_branch,
        )
    except LakeFSConflictError:
        build_branch = safe_lakefs_ref(f"build/{pipeline_ref}/{run_ref}/{uuid4().hex[:8]}")
        await worker.lakefs_client.create_branch(  # type: ignore[union-attr]
            repository=artifact_repo,
            name=build_branch,
            source=base_branch,
        )
    return build_branch


async def materialize_build_outputs(
    worker: Any,
    *,
    job: PipelineJob,
    artifact_repo: str,
    build_branch: str,
    base_branch: str,
    output_work: List[Dict[str, Any]],
    output_write_mode: str,
    execution_semantics: str,
    incremental_inputs_have_additive_updates: Optional[bool],
) -> List[Dict[str, Any]]:
    build_outputs: List[Dict[str, Any]] = []
    for item in output_work:
        output_df = item["output_df"]
        materialized = await worker._materialize_output_by_kind(
            output_kind=str(item.get("output_kind") or OUTPUT_KIND_DATASET),
            output_metadata=item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
            df=output_df,
            artifact_bucket=artifact_repo,
            prefix=f"{build_branch}/{item['artifact_prefix']}",
            db_name=job.db_name,
            branch=base_branch,
            dataset_name=str(item.get("dataset_name") or ""),
            execution_semantics=execution_semantics,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            write_mode=str(item.get("runtime_write_mode") or output_write_mode),
            file_prefix=job.job_id,
            file_format=item["output_format"],
            partition_cols=item["partition_columns"],
            base_row_count=int(item.get("delta_row_count") or 0),
        )
        if getattr(worker, "_spark_executor", None):
            try:
                output_df.unpersist(blocking=False)
            except Exception as exc:
                logger.warning("Failed to unpersist output dataframe: %s", exc, exc_info=True)
        resolved_runtime_write_mode = str(
            materialized.get("runtime_write_mode")
            or item.get("runtime_write_mode")
            or output_write_mode
        )
        delta_row_count = int(
            materialized.get("delta_row_count")
            if materialized.get("delta_row_count") is not None
            else int(item.get("delta_row_count") or 0)
        )
        row_count = delta_row_count
        if resolved_runtime_write_mode == "append":
            try:
                existing_dataset = await worker.dataset_registry.get_dataset_by_name(
                    db_name=job.db_name,
                    name=item["dataset_name"],
                    branch=base_branch,
                )
                if existing_dataset:
                    existing_version = await worker.dataset_registry.get_latest_version(
                        dataset_id=existing_dataset.dataset_id
                    )
                    if existing_version and existing_version.row_count is not None:
                        row_count = int(existing_version.row_count) + delta_row_count
            except Exception as exc:
                logger.debug("Failed to resolve previous row_count for incremental build: %s", exc)
        build_outputs.append(
            {
                "node_id": item["node_id"],
                "output_name": item["output_name"],
                "output_kind": item.get("output_kind", OUTPUT_KIND_DATASET),
                "output_metadata": item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
                "dataset_name": item["dataset_name"],
                "artifact_key": materialized["artifact_key"],
                "artifact_prefix": item["artifact_prefix"],
                "output_format": materialized.get("output_format") or item["output_format"],
                "partition_columns": materialized.get("partition_by") or item["partition_columns"],
                "row_count": row_count,
                "delta_row_count": delta_row_count if resolved_runtime_write_mode == "append" else None,
                "write_mode_requested": materialized.get("write_mode_requested") or item.get("write_mode_requested"),
                "write_mode_resolved": materialized.get("write_mode_resolved") or item.get("write_mode_resolved"),
                "runtime_write_mode": resolved_runtime_write_mode,
                "pk_columns": materialized.get("pk_columns") or item.get("pk_columns") or [],
                "post_filtering_column": materialized.get("post_filtering_column") or item.get("post_filtering_column"),
                "write_policy_hash": materialized.get("write_policy_hash") or item.get("write_policy_hash"),
                "has_incremental_input": (
                    materialized.get("has_incremental_input")
                    if materialized.get("has_incremental_input") is not None
                    else item.get("has_incremental_input")
                ),
                "incremental_inputs_have_additive_updates": (
                    materialized.get("incremental_inputs_have_additive_updates")
                    if materialized.get("incremental_inputs_have_additive_updates") is not None
                    else item.get("incremental_inputs_have_additive_updates")
                ),
                "columns": item["schema_columns"],
                "schema_hash": item["schema_hash"],
                "schema_json": {"columns": item["schema_columns"]},
                "rows": item["output_sample"],
                "sample_row_count": len(item["output_sample"]),
                "column_stats": item["column_stats"],
            }
        )
    return build_outputs


async def create_deploy_run_branch(
    worker: Any,
    *,
    artifact_repo: str,
    base_branch: str,
    pipeline_ref: str,
    run_ref: str,
) -> str:
    run_branch = safe_lakefs_ref(f"run/{pipeline_ref}/{run_ref}")
    try:
        await worker.lakefs_client.create_branch(  # type: ignore[union-attr]
            repository=artifact_repo,
            name=run_branch,
            source=base_branch,
        )
    except LakeFSConflictError:
        run_branch = safe_lakefs_ref(f"run/{pipeline_ref}/{run_ref}/{uuid4().hex[:8]}")
        await worker.lakefs_client.create_branch(  # type: ignore[union-attr]
            repository=artifact_repo,
            name=run_branch,
            source=base_branch,
        )
    return run_branch


async def materialize_deploy_staged_outputs(
    worker: Any,
    *,
    job: PipelineJob,
    run_branch: str,
    artifact_repo: str,
    base_branch: str,
    output_work: List[Dict[str, Any]],
    output_write_mode: str,
    execution_semantics: str,
    incremental_inputs_have_additive_updates: Optional[bool],
) -> List[Dict[str, Any]]:
    staged_outputs: List[Dict[str, Any]] = []
    for item in output_work:
        branch_prefix = f"{run_branch}/{item['artifact_prefix']}"
        materialized = await worker._materialize_output_by_kind(
            output_kind=str(item.get("output_kind") or OUTPUT_KIND_DATASET),
            output_metadata=item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
            df=item["output_df"],
            artifact_bucket=artifact_repo,
            prefix=branch_prefix,
            db_name=job.db_name,
            branch=base_branch,
            dataset_name=str(item.get("dataset_name") or ""),
            execution_semantics=execution_semantics,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            write_mode=str(item.get("runtime_write_mode") or output_write_mode),
            file_prefix=job.job_id,
            file_format=item["output_format"],
            partition_cols=item["partition_columns"],
            base_row_count=int(item.get("row_count") or 0),
        )
        resolved_runtime_write_mode = str(
            materialized.get("runtime_write_mode")
            or item.get("runtime_write_mode")
            or output_write_mode
        )
        delta_row_count = int(
            materialized.get("delta_row_count")
            if materialized.get("delta_row_count") is not None
            else int(item.get("row_count") or 0)
        )
        staged_outputs.append(
            {
                "node_id": item["node_id"],
                "output_name": item.get("output_name"),
                "output_kind": item.get("output_kind", OUTPUT_KIND_DATASET),
                "output_metadata": item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
                "dataset_name": item["dataset_name"],
                "artifact_prefix": item["artifact_prefix"],
                "output_format": materialized.get("output_format") or item["output_format"],
                "partition_columns": materialized.get("partition_by") or item["partition_columns"],
                "row_count": delta_row_count,
                "write_mode_requested": materialized.get("write_mode_requested") or item.get("write_mode_requested"),
                "write_mode_resolved": materialized.get("write_mode_resolved") or item.get("write_mode_resolved"),
                "runtime_write_mode": resolved_runtime_write_mode,
                "pk_columns": materialized.get("pk_columns") or item.get("pk_columns") or [],
                "post_filtering_column": materialized.get("post_filtering_column") or item.get("post_filtering_column"),
                "write_policy_hash": materialized.get("write_policy_hash") or item.get("write_policy_hash"),
                "has_incremental_input": (
                    materialized.get("has_incremental_input")
                    if materialized.get("has_incremental_input") is not None
                    else item.get("has_incremental_input")
                ),
                "incremental_inputs_have_additive_updates": (
                    materialized.get("incremental_inputs_have_additive_updates")
                    if materialized.get("incremental_inputs_have_additive_updates") is not None
                    else item.get("incremental_inputs_have_additive_updates")
                ),
                "columns": item["columns"],
                "rows": item["rows"],
                "sample_row_count": item["sample_row_count"],
                "column_stats": item["column_stats"],
            }
        )
    return staged_outputs


async def commit_and_merge_deploy_branch(
    worker: Any,
    *,
    job: PipelineJob,
    lock: Any,
    pipeline_ref: str,
    artifact_repo: str,
    run_branch: str,
    base_branch: str,
) -> Tuple[str, str]:
    if lock:
        lock.raise_if_lost()
    commit_id = await worker.lakefs_client.commit(  # type: ignore[union-attr]
        repository=artifact_repo,
        branch=run_branch,
        message=f"Build pipeline outputs {job.db_name}/{pipeline_ref} ({job.job_id})",
        metadata={
            "pipeline_id": pipeline_ref,
            "pipeline_job_id": job.job_id,
            "db_name": job.db_name,
            "mode": "deploy",
        },
    )
    if lock:
        lock.raise_if_lost()
    merge_commit_id = await worker.lakefs_client.merge(  # type: ignore[union-attr]
        repository=artifact_repo,
        source_ref=run_branch,
        destination_branch=base_branch,
        message=f"Publish pipeline outputs {job.db_name}/{pipeline_ref} ({job.job_id})",
        metadata={
            "pipeline_id": pipeline_ref,
            "pipeline_job_id": job.job_id,
            "db_name": job.db_name,
            "mode": "deploy",
            "run_branch": run_branch,
            "run_commit_id": commit_id,
        },
        allow_empty=False,
    )
    try:
        await worker.lakefs_client.delete_branch(repository=artifact_repo, name=run_branch)  # type: ignore[union-attr]
    except Exception as exc:
        logger.info("Failed to delete run branch %s after merge: %s", run_branch, exc)
    return commit_id, merge_commit_id


async def stage_deploy_outputs(
    worker: Any,
    *,
    job: PipelineJob,
    lock: Any,
    pipeline_ref: str,
    run_ref: str,
    artifact_repo: str,
    base_branch: str,
    output_work: List[Dict[str, Any]],
    output_write_mode: str,
    execution_semantics: str,
    incremental_inputs_have_additive_updates: Optional[bool],
) -> Tuple[str, str, str, List[Dict[str, Any]]]:
    run_branch = await create_deploy_run_branch(
        worker,
        artifact_repo=artifact_repo,
        base_branch=base_branch,
        pipeline_ref=pipeline_ref,
        run_ref=run_ref,
    )
    staged_outputs = await materialize_deploy_staged_outputs(
        worker,
        job=job,
        run_branch=run_branch,
        artifact_repo=artifact_repo,
        base_branch=base_branch,
        output_work=output_work,
        output_write_mode=output_write_mode,
        execution_semantics=execution_semantics,
        incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
    )
    commit_id, merge_commit_id = await commit_and_merge_deploy_branch(
        worker,
        job=job,
        lock=lock,
        pipeline_ref=pipeline_ref,
        artifact_repo=artifact_repo,
        run_branch=run_branch,
        base_branch=base_branch,
    )
    return run_branch, commit_id, merge_commit_id, staged_outputs


def build_deploy_version_sample_payload(
    *,
    item: Dict[str, Any],
    schema_columns: List[Dict[str, Any]],
    output_sample: List[Dict[str, Any]],
    total_row_count: int,
    delta_row_count: int,
    resolved_runtime_write_mode: str,
) -> Dict[str, Any]:
    return {
        "columns": schema_columns,
        "rows": output_sample,
        "row_count": total_row_count,
        "delta_row_count": delta_row_count if resolved_runtime_write_mode == "append" else None,
        "write_mode_requested": item.get("write_mode_requested"),
        "write_mode_resolved": item.get("write_mode_resolved"),
        "runtime_write_mode": resolved_runtime_write_mode,
        "pk_columns": item.get("pk_columns") or [],
        "write_policy_hash": item.get("write_policy_hash"),
        "has_incremental_input": item.get("has_incremental_input"),
        "incremental_inputs_have_additive_updates": item.get("incremental_inputs_have_additive_updates"),
        "sample_row_count": len(output_sample),
        "column_stats": item.get("column_stats") or {},
    }


async def resolve_deploy_output_publication(
    worker: Any,
    *,
    job: PipelineJob,
    pipeline_ref: str,
    artifact_repo: str,
    base_branch: str,
    merge_commit_id: str,
    output_write_mode: str,
    item: Dict[str, Any],
) -> Dict[str, Any]:
    artifact_prefix = str(item.get("artifact_prefix") or "").lstrip("/")
    artifact_key = build_s3_uri(artifact_repo, f"{merge_commit_id}/{artifact_prefix}")
    dataset_name = str(item.get("dataset_name") or "")
    schema_columns = item.get("columns") if isinstance(item.get("columns"), list) else []
    output_sample = item.get("rows") if isinstance(item.get("rows"), list) else []
    delta_row_count = int(item.get("row_count") or 0)

    dataset = await worker.dataset_registry.get_dataset_by_name(
        db_name=job.db_name,
        name=dataset_name,
        branch=base_branch,
    )
    resolved_runtime_write_mode = str(item.get("runtime_write_mode") or output_write_mode)
    total_row_count = delta_row_count
    if resolved_runtime_write_mode == "append" and dataset:
        previous = await worker.dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
        if previous and previous.row_count is not None:
            total_row_count = int(previous.row_count) + delta_row_count
    if not dataset:
        dataset = await worker.dataset_registry.create_dataset(
            db_name=job.db_name,
            name=dataset_name,
            description=None,
            source_type="pipeline",
            source_ref=pipeline_ref,
            schema_json={"columns": schema_columns},
            branch=base_branch,
        )
    version = await worker.dataset_registry.add_version(
        dataset_id=dataset.dataset_id,
        lakefs_commit_id=merge_commit_id,
        artifact_key=artifact_key,
        row_count=total_row_count,
        sample_json=build_deploy_version_sample_payload(
            item=item,
            schema_columns=schema_columns,
            output_sample=output_sample,
            total_row_count=total_row_count,
            delta_row_count=delta_row_count,
            resolved_runtime_write_mode=resolved_runtime_write_mode,
        ),
        schema_json={"columns": schema_columns},
    )
    objectify_job_id = await worker._maybe_enqueue_objectify_job(dataset=dataset, version=version)
    relationship_job_ids = await worker._maybe_enqueue_relationship_jobs(dataset=dataset, version=version)
    return {
        "dataset_name": dataset_name,
        "artifact_key": artifact_key,
        "total_row_count": total_row_count,
        "delta_row_count": delta_row_count,
        "resolved_runtime_write_mode": resolved_runtime_write_mode,
        "objectify_job_id": objectify_job_id,
        "relationship_job_ids": relationship_job_ids,
    }


def build_deploy_output_record(
    *,
    item: Dict[str, Any],
    publication: Dict[str, Any],
    merge_commit_id: str,
    base_branch: str,
) -> Dict[str, Any]:
    return {
        "node_id": item.get("node_id"),
        "output_name": item.get("output_name"),
        "output_kind": item.get("output_kind", OUTPUT_KIND_DATASET),
        "output_metadata": item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
        "dataset_name": publication["dataset_name"],
        "artifact_key": publication["artifact_key"],
        "row_count": publication["total_row_count"],
        "delta_row_count": (
            publication["delta_row_count"]
            if publication["resolved_runtime_write_mode"] == "append"
            else None
        ),
        "write_mode_requested": item.get("write_mode_requested"),
        "write_mode_resolved": item.get("write_mode_resolved"),
        "runtime_write_mode": publication["resolved_runtime_write_mode"],
        "pk_columns": item.get("pk_columns") or [],
        "post_filtering_column": item.get("post_filtering_column"),
        "write_policy_hash": item.get("write_policy_hash"),
        "has_incremental_input": item.get("has_incremental_input"),
        "incremental_inputs_have_additive_updates": item.get("incremental_inputs_have_additive_updates"),
        "lakefs_commit_id": merge_commit_id,
        "lakefs_branch": base_branch,
        "objectify_job_id": publication["objectify_job_id"],
        "relationship_job_ids": publication["relationship_job_ids"],
    }


async def record_deploy_output_lineage(
    worker: Any,
    *,
    job: PipelineJob,
    pipeline_ref: str,
    artifact_key: str,
    dataset_name: str,
    node_id: Optional[str],
    merge_commit_id: str,
    base_branch: str,
) -> None:
    parsed = parse_s3_uri(artifact_key)
    if not parsed:
        return
    bucket, key = parsed

    async def _record_pipeline_output_lineage() -> None:
        await worker.lineage.record_link(  # type: ignore[union-attr]
            from_node_id=LineageStore.node_aggregate("Pipeline", pipeline_ref),
            to_node_id=LineageStore.node_artifact("s3", bucket, key),
            edge_type=EDGE_PIPELINE_OUTPUT_STORED,
            occurred_at=utcnow(),
            db_name=job.db_name,
            edge_metadata={
                "db_name": job.db_name,
                "pipeline_id": pipeline_ref,
                "artifact_key": artifact_key,
                "dataset_name": dataset_name,
                "node_id": node_id,
                "lakefs_commit_id": merge_commit_id,
                "lakefs_branch": base_branch,
            },
        )

    await record_lineage_or_raise(
        lineage_store=worker.lineage,
        required=worker.lineage_required,
        record_call=_record_pipeline_output_lineage,
        logger=logger,
        operation="pipeline_worker.deploy.pipeline_output_stored",
        zone=RuntimeZone.CORE,
        context={
            "db_name": job.db_name,
            "pipeline_id": pipeline_ref,
            "artifact_key": artifact_key,
            "dataset_name": dataset_name,
            "node_id": node_id,
        },
    )


async def publish_deploy_outputs(
    worker: Any,
    *,
    job: PipelineJob,
    pipeline_ref: str,
    artifact_repo: str,
    base_branch: str,
    merge_commit_id: str,
    output_write_mode: str,
    staged_outputs: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    build_outputs: List[Dict[str, Any]] = []
    for item in staged_outputs:
        publication = await resolve_deploy_output_publication(
            worker,
            job=job,
            pipeline_ref=pipeline_ref,
            artifact_repo=artifact_repo,
            base_branch=base_branch,
            merge_commit_id=merge_commit_id,
            output_write_mode=output_write_mode,
            item=item,
        )
        build_outputs.append(
            build_deploy_output_record(
                item=item,
                publication=publication,
                merge_commit_id=merge_commit_id,
                base_branch=base_branch,
            )
        )
        await record_deploy_output_lineage(
            worker,
            job=job,
            pipeline_ref=pipeline_ref,
            artifact_key=str(publication["artifact_key"]),
            dataset_name=str(publication["dataset_name"]),
            node_id=str(item.get("node_id") or "") or None,
            merge_commit_id=merge_commit_id,
            base_branch=base_branch,
        )
    return build_outputs


def merge_watermark_keys(
    *,
    next_watermark: Optional[Any],
    previous_watermark: Optional[Any],
    previous_watermark_keys: List[str],
    next_watermark_keys: List[str],
) -> List[str]:
    merged_keys: List[str] = []
    if next_watermark is not None:
        if (
            previous_watermark is not None
            and previous_watermark_keys
            and worker_helpers_watermark_values_match(previous_watermark, next_watermark)
        ):
            merged_keys = [*previous_watermark_keys, *next_watermark_keys]
        else:
            merged_keys = list(next_watermark_keys)
    elif previous_watermark is not None and previous_watermark_keys:
        merged_keys = list(previous_watermark_keys)
    if not merged_keys:
        return []
    deduped_keys: List[str] = []
    seen_keys: set[str] = set()
    for item in merged_keys:
        if item in seen_keys:
            continue
        seen_keys.add(item)
        deduped_keys.append(item)
    return deduped_keys


async def persist_deploy_watermarks(
    worker: Any,
    *,
    resolved_pipeline_id: Optional[str],
    branch: str,
    execution_semantics: str,
    input_snapshots: List[Dict[str, Any]],
    watermark_column: Optional[str],
    previous_watermark: Optional[Any],
    previous_watermark_keys: List[str],
) -> Optional[Dict[str, Any]]:
    from pipeline_worker.worker_helpers import (
        _collect_input_commit_map,
        _collect_watermark_keys_from_snapshots,
        _max_watermark_from_snapshots,
    )

    watermark_update: Optional[Dict[str, Any]] = None
    if (
        execution_semantics not in {"incremental", "streaming"}
        or not resolved_pipeline_id
        or not input_snapshots
    ):
        return watermark_update

    next_watermark = (
        _max_watermark_from_snapshots(input_snapshots, watermark_column=watermark_column)
        if watermark_column
        else None
    )
    next_watermark_keys = (
        _collect_watermark_keys_from_snapshots(
            input_snapshots,
            watermark_column=watermark_column,
            watermark_value=next_watermark,
        )
        if watermark_column and next_watermark is not None
        else []
    )
    input_commit_map = _collect_input_commit_map(input_snapshots)
    if (next_watermark is None and not input_commit_map) or not worker.pipeline_registry:
        return watermark_update

    watermarks_payload: Dict[str, Any] = {}
    if watermark_column:
        watermarks_payload["watermark_column"] = watermark_column
        if next_watermark is not None:
            watermarks_payload["watermark_value"] = next_watermark
        elif previous_watermark is not None:
            watermarks_payload["watermark_value"] = previous_watermark
        merged_keys = merge_watermark_keys(
            next_watermark=next_watermark,
            previous_watermark=previous_watermark,
            previous_watermark_keys=previous_watermark_keys,
            next_watermark_keys=next_watermark_keys,
        )
        if merged_keys:
            watermarks_payload["watermark_keys"] = merged_keys
    if input_commit_map:
        watermarks_payload["input_commits"] = input_commit_map

    try:
        await worker.pipeline_registry.upsert_watermarks(
            pipeline_id=resolved_pipeline_id,
            branch=branch,
            watermarks=watermarks_payload,
        )
        watermark_update = watermarks_payload
    except Exception as exc:
        logger.warning(
            "Failed to persist pipeline watermarks (pipeline_id=%s): %s",
            resolved_pipeline_id,
            exc,
        )
    return watermark_update


def worker_helpers_watermark_values_match(left: Any, right: Any) -> bool:
    from pipeline_worker.worker_helpers import _watermark_values_match

    return _watermark_values_match(left, right)
