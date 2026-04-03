from __future__ import annotations

from typing import Any, Dict, List, Optional


async def resolve_execution_node_dataframe(
    worker: Any,
    *,
    job: Any,
    node_id: str,
    node: Dict[str, Any],
    tables: Dict[str, Any],
    incoming: Dict[str, List[str]],
    parameters: Dict[str, Any],
    preview_meta: Dict[str, Any],
    preview_sampling_seed: int,
    input_snapshots: List[Dict[str, Any]],
    input_sampling: Dict[str, Any],
    temp_dirs: List[str],
    previous_input_commits: Dict[str, str],
    use_lakefs_diff: bool,
    execution_semantics: str,
    watermark_column: Optional[str],
    previous_watermark: Optional[Any],
    previous_watermark_keys: List[str],
    is_preview: bool,
    preview_input_sample_limit: int,
    normalize_operation: Any,
    resolve_udf_reference: Any,
) -> Any:
    metadata = node.get("metadata") or {}
    node_type = str(node.get("type") or "transform")
    inputs = [tables[src] for src in incoming.get(node_id, []) if src in tables]

    if node_type == "input":
        df = await worker._load_input_dataframe(
            job.db_name,
            metadata,
            temp_dirs,
            job.branch,
            node_id=node_id,
            input_snapshots=input_snapshots,
            previous_commit_id=previous_input_commits.get(node_id),
            use_lakefs_diff=use_lakefs_diff and execution_semantics in {"incremental", "streaming"},
            watermark_column=watermark_column if execution_semantics in {"incremental", "streaming"} else None,
            watermark_after=previous_watermark if execution_semantics in {"incremental", "streaming"} else None,
            watermark_keys=previous_watermark_keys if execution_semantics in {"incremental", "streaming"} else None,
        )
        if is_preview:
            sampling_strategy = worker._resolve_sampling_strategy(
                metadata,
                preview_meta,
                preview_limit=preview_input_sample_limit,
            )
            if sampling_strategy:
                df = worker._apply_sampling_strategy(
                    df,
                    sampling_strategy,
                    node_id=node_id,
                    seed=preview_sampling_seed,
                )
                input_sampling[node_id] = sampling_strategy
                worker._attach_sampling_snapshot(
                    input_snapshots,
                    node_id=node_id,
                    sampling_strategy=sampling_strategy,
                )
        return df

    if node_type == "output":
        return inputs[0] if inputs else worker._empty_dataframe()

    transform_metadata = dict(metadata)
    operation = normalize_operation(transform_metadata.get("operation"))
    if operation == "udf" and worker._udf_spark_parity_enabled:
        resolved = await resolve_udf_reference(
            metadata=transform_metadata,
            pipeline_registry=worker.pipeline_registry,
            require_reference=worker._udf_require_reference,
            require_version_pinning=worker._udf_require_version_pinning,
            code_cache=worker._udf_code_cache,
        )
        transform_metadata["__resolved_udf_code"] = resolved.code
    return worker._apply_transform(transform_metadata, inputs, parameters)
