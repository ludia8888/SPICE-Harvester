from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from typing import Any, Dict, List, Optional
from uuid import uuid4

from shared.services.pipeline.dataset_output_semantics import (
    DatasetWriteMode,
    resolve_dataset_write_policy,
    validate_dataset_output_format_constraints,
    validate_dataset_output_metadata,
)
from shared.services.pipeline.output_plugins import (
    OUTPUT_KIND_DATASET,
    OUTPUT_KIND_GEOTEMPORAL,
    OUTPUT_KIND_MEDIA,
    OUTPUT_KIND_ONTOLOGY,
    OUTPUT_KIND_VIRTUAL,
    normalize_output_kind,
    resolve_ontology_output_semantics,
)
from shared.utils.s3_uri import build_s3_uri
from shared.utils.time_utils import utcnow

from pipeline_worker.spark_schema_helpers import _list_part_files

logger = logging.getLogger(__name__)


class PipelineOutputDomain:
    def __init__(self, worker: Any) -> None:
        self._worker = worker

    async def materialize_output_by_kind(
        self,
        *,
        output_kind: str,
        output_metadata: Dict[str, Any],
        df: Any,
        artifact_bucket: str,
        prefix: str,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        dataset_name: Optional[str] = None,
        execution_semantics: str = "snapshot",
        incremental_inputs_have_additive_updates: Optional[bool] = None,
        write_mode: str = "overwrite",
        file_prefix: Optional[str] = None,
        file_format: str = "parquet",
        partition_cols: Optional[List[str]] = None,
        base_row_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_kind = normalize_output_kind(output_kind)
        if normalized_kind == OUTPUT_KIND_DATASET:
            return await self.materialize_dataset_output(
                output_metadata=output_metadata,
                df=df,
                artifact_bucket=artifact_bucket,
                prefix=prefix,
                db_name=db_name,
                branch=branch,
                dataset_name=dataset_name,
                execution_semantics=execution_semantics,
                incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
                write_mode=write_mode,
                file_prefix=file_prefix,
                file_format=file_format,
                partition_cols=partition_cols,
                base_row_count=base_row_count,
            )

        if normalized_kind == OUTPUT_KIND_GEOTEMPORAL:
            artifact_key = await self.materialize_geotemporal_output(
                output_metadata=output_metadata,
                df=df,
                artifact_bucket=artifact_bucket,
                prefix=prefix,
                write_mode=write_mode,
                file_prefix=file_prefix,
                file_format=file_format,
                partition_cols=partition_cols,
            )
        elif normalized_kind == OUTPUT_KIND_MEDIA:
            artifact_key = await self.materialize_media_output(
                output_metadata=output_metadata,
                df=df,
                artifact_bucket=artifact_bucket,
                prefix=prefix,
                write_mode=write_mode,
                file_prefix=file_prefix,
                file_format=file_format,
                partition_cols=partition_cols,
            )
        elif normalized_kind == OUTPUT_KIND_VIRTUAL:
            artifact_key = await self.materialize_virtual_output(
                output_metadata=output_metadata,
                df=df,
                artifact_bucket=artifact_bucket,
                prefix=prefix,
                write_mode=write_mode,
                file_prefix=file_prefix,
                file_format=file_format,
                partition_cols=partition_cols,
                row_count_hint=base_row_count,
            )
        elif normalized_kind == OUTPUT_KIND_ONTOLOGY:
            artifact_key = await self.materialize_ontology_output(
                output_metadata=output_metadata,
                df=df,
                artifact_bucket=artifact_bucket,
                prefix=prefix,
                write_mode=write_mode,
                file_prefix=file_prefix,
                file_format=file_format,
                partition_cols=partition_cols,
            )
        else:
            raise ValueError(f"Unsupported output_kind for materialization: {normalized_kind}")

        return {
            "artifact_key": artifact_key,
            "delta_row_count": int(base_row_count or 0),
            "write_mode_requested": write_mode,
            "write_mode_resolved": write_mode,
            "runtime_write_mode": write_mode,
            "pk_columns": [],
            "write_policy_hash": None,
            "has_incremental_input": None,
            "incremental_inputs_have_additive_updates": None,
        }

    async def materialize_dataset_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: Any,
        artifact_bucket: str,
        prefix: str,
        db_name: Optional[str],
        branch: Optional[str],
        dataset_name: Optional[str],
        execution_semantics: str,
        incremental_inputs_have_additive_updates: Optional[bool],
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
        base_row_count: Optional[int],
    ) -> Dict[str, Any]:
        worker = self._worker
        policy = resolve_dataset_write_policy(
            definition={},
            output_metadata=output_metadata,
            execution_semantics=execution_semantics,
            has_incremental_input=execution_semantics in {"incremental", "streaming"},
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        )
        for warning in policy.warnings:
            logger.warning("Dataset output policy normalized for %s: %s", dataset_name or prefix, warning)
        output_metadata.clear()
        output_metadata.update(policy.normalized_metadata)

        validation_errors = validate_dataset_output_metadata(
            definition={},
            output_metadata=output_metadata,
            execution_semantics=execution_semantics,
            has_incremental_input=execution_semantics in {"incremental", "streaming"},
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            available_columns=set(df.columns or []),
        )
        if validation_errors:
            raise ValueError("Invalid dataset output metadata: " + "; ".join(validation_errors))

        pk_columns = list(policy.primary_key_columns)

        existing_df = worker._empty_dataframe()
        if policy.resolved_write_mode in {
            DatasetWriteMode.APPEND_ONLY_NEW_ROWS,
            DatasetWriteMode.CHANGELOG,
            DatasetWriteMode.SNAPSHOT_DIFFERENCE,
            DatasetWriteMode.SNAPSHOT_REPLACE,
            DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE,
        }:
            existing_df = await worker._load_existing_output_dataset(
                db_name=db_name,
                branch=branch,
                dataset_name=dataset_name,
            )

        if existing_df.columns and pk_columns:
            missing_existing = [column for column in pk_columns if column not in set(existing_df.columns)]
            if missing_existing:
                raise ValueError(
                    "Existing dataset missing primary_key_columns: " + ", ".join(missing_existing)
                )

        aligned_columns = list(df.columns or [])
        if policy.resolved_write_mode in {
            DatasetWriteMode.SNAPSHOT_REPLACE,
            DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE,
        } and existing_df.columns:
            existing_only = [column for column in (existing_df.columns or []) if column not in set(aligned_columns)]
            aligned_columns = [*aligned_columns, *existing_only]
        input_aligned = worker._align_columns(df, aligned_columns) if aligned_columns else df
        existing_aligned = worker._align_columns(existing_df, aligned_columns) if aligned_columns else existing_df

        materialized_df = input_aligned
        deduped_input = (
            input_aligned.dropDuplicates(pk_columns)
            if pk_columns
            and policy.resolved_write_mode
            in {
                DatasetWriteMode.APPEND_ONLY_NEW_ROWS,
                DatasetWriteMode.SNAPSHOT_REPLACE,
                DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE,
            }
            else input_aligned
        )

        if policy.resolved_write_mode == DatasetWriteMode.DEFAULT:
            materialized_df = input_aligned
        elif policy.resolved_write_mode == DatasetWriteMode.ALWAYS_APPEND:
            materialized_df = input_aligned
        elif policy.resolved_write_mode == DatasetWriteMode.CHANGELOG:
            materialized_df = worker._select_new_or_changed_rows(
                input_df=input_aligned,
                existing_df=existing_aligned,
                pk_columns=pk_columns,
                dedupe_input=False,
            )
        elif policy.resolved_write_mode == DatasetWriteMode.APPEND_ONLY_NEW_ROWS:
            if existing_aligned.columns and pk_columns:
                existing_keys = existing_aligned.select(*pk_columns).distinct()
                materialized_df = deduped_input.join(existing_keys, on=pk_columns, how="left_anti")
            else:
                materialized_df = deduped_input
        elif policy.resolved_write_mode == DatasetWriteMode.SNAPSHOT_DIFFERENCE:
            if existing_aligned.columns and pk_columns:
                existing_keys = existing_aligned.select(*pk_columns).distinct()
                materialized_df = input_aligned.join(existing_keys, on=pk_columns, how="left_anti")
            else:
                materialized_df = input_aligned
        elif policy.resolved_write_mode == DatasetWriteMode.SNAPSHOT_REPLACE:
            upsert_df = deduped_input
            if existing_aligned.columns and pk_columns:
                existing_dedup = existing_aligned.dropDuplicates(pk_columns)
                upsert_keys = upsert_df.select(*pk_columns).distinct()
                preserved_existing = existing_dedup.join(upsert_keys, on=pk_columns, how="left_anti")
                materialized_df = preserved_existing.unionByName(upsert_df, allowMissingColumns=True)
            else:
                materialized_df = upsert_df
        elif policy.resolved_write_mode == DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE:
            post_col = str(policy.post_filtering_column or "").strip()
            if not post_col:
                raise ValueError("post_filtering_column is required for snapshot_replace_and_remove")
            is_false_expr = worker._post_filter_false_expr(post_filtering_column=post_col)
            upsert_input = input_aligned.filter(~is_false_expr)
            upsert_df = upsert_input.dropDuplicates(pk_columns) if pk_columns else upsert_input
            false_keys = input_aligned.filter(is_false_expr).select(*pk_columns).distinct()
            if existing_aligned.columns and pk_columns:
                existing_dedup = existing_aligned.dropDuplicates(pk_columns)
                upsert_keys = upsert_df.select(*pk_columns).distinct()
                preserved_existing = existing_dedup.join(upsert_keys, on=pk_columns, how="left_anti")
                preserved_existing = preserved_existing.join(false_keys, on=pk_columns, how="left_anti")
                materialized_df = preserved_existing.unionByName(upsert_df, allowMissingColumns=True)
            else:
                materialized_df = upsert_df

        delta_row_count = int(
            await worker._run_spark(
                lambda: materialized_df.count(),
                label=f"count:materialized:{dataset_name or prefix}",
            )
        )
        artifact_key = await self.materialize_output_dataframe(
            materialized_df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=policy.runtime_write_mode,
            file_prefix=file_prefix,
            file_format=policy.output_format or file_format,
            partition_cols=policy.partition_by or partition_cols,
        )
        return {
            "artifact_key": artifact_key,
            "delta_row_count": delta_row_count,
            "write_mode_requested": policy.requested_write_mode,
            "write_mode_resolved": policy.resolved_write_mode.value,
            "runtime_write_mode": policy.runtime_write_mode,
            "pk_columns": list(policy.primary_key_columns),
            "post_filtering_column": policy.post_filtering_column,
            "output_format": policy.output_format,
            "partition_by": list(policy.partition_by),
            "write_policy_hash": policy.policy_hash,
            "input_row_count": int(base_row_count or delta_row_count),
            "has_incremental_input": policy.has_incremental_input,
            "incremental_inputs_have_additive_updates": policy.incremental_inputs_have_additive_updates,
        }

    async def materialize_geotemporal_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: Any,
        artifact_bucket: str,
        prefix: str,
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
    ) -> str:
        time_column = str(output_metadata.get("time_column") or output_metadata.get("timeColumn") or "").strip()
        geometry_column = str(output_metadata.get("geometry_column") or output_metadata.get("geometryColumn") or "").strip()
        geometry_format = str(output_metadata.get("geometry_format") or output_metadata.get("geometryFormat") or "").strip().lower()
        if not time_column or not geometry_column:
            raise ValueError("geotemporal output requires time_column and geometry_column")
        if geometry_format not in {"wkt", "geojson"}:
            raise ValueError("geotemporal geometry_format must be one of: wkt|geojson")
        self.ensure_output_columns_present(
            df=df,
            required_columns=[time_column, geometry_column],
            output_kind=OUTPUT_KIND_GEOTEMPORAL,
        )
        return await self.materialize_output_dataframe(
            df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
        )

    async def materialize_media_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: Any,
        artifact_bucket: str,
        prefix: str,
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
    ) -> str:
        media_uri_column = str(output_metadata.get("media_uri_column") or output_metadata.get("mediaUriColumn") or "").strip()
        media_type = str(output_metadata.get("media_type") or output_metadata.get("mediaType") or "").strip().lower()
        if not media_uri_column:
            raise ValueError("media output requires media_uri_column")
        if media_type not in {"image", "video", "audio", "document"}:
            raise ValueError("media_type must be one of: image|video|audio|document")
        self.ensure_output_columns_present(
            df=df,
            required_columns=[media_uri_column],
            output_kind=OUTPUT_KIND_MEDIA,
        )
        return await self.materialize_output_dataframe(
            df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
        )

    async def materialize_virtual_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: Any,
        artifact_bucket: str,
        prefix: str,
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
        row_count_hint: Optional[int],
    ) -> str:
        worker = self._worker
        query_sql = str(output_metadata.get("query_sql") or output_metadata.get("querySql") or "").strip()
        refresh_mode = str(output_metadata.get("refresh_mode") or output_metadata.get("refreshMode") or "").strip().lower()
        if not query_sql:
            raise ValueError("virtual output requires query_sql")
        if refresh_mode not in {"on_read", "scheduled"}:
            raise ValueError("virtual output refresh_mode must be one of: on_read|scheduled")
        dataset_style_keys = [
            key
            for key in (
                "write_mode",
                "writeMode",
                "primary_key_columns",
                "primaryKeyColumns",
                "post_filtering_column",
                "postFilteringColumn",
                "output_format",
                "outputFormat",
                "partition_by",
                "partitionBy",
            )
            if output_metadata.get(key) not in (None, "", [])
        ]
        if dataset_style_keys:
            raise ValueError(
                "virtual output does not support dataset write settings: "
                + ", ".join(sorted(set(dataset_style_keys)))
            )
        if not worker.storage:
            raise RuntimeError("Storage service not available")
        await worker.storage.create_bucket(artifact_bucket)
        normalized_prefix = (prefix or "").lstrip("/").rstrip("/")
        if not normalized_prefix:
            raise ValueError("prefix is required")
        resolved_write_mode = str(write_mode or "overwrite").strip().lower() or "overwrite"
        if resolved_write_mode not in {"overwrite", "append"}:
            raise ValueError("write_mode must be overwrite or append")
        if resolved_write_mode == "overwrite":
            await worker.storage.delete_prefix(artifact_bucket, normalized_prefix)
        manifest_name = "virtual_manifest.json"
        if resolved_write_mode == "append":
            unique_prefix = str(file_prefix or "").strip().replace("/", "_") or uuid4().hex[:12]
            manifest_name = f"{unique_prefix}_virtual_manifest.json"
        manifest_key = f"{normalized_prefix}/{manifest_name}"
        resolved_row_count_hint = int(row_count_hint) if row_count_hint is not None else int(
            await worker._run_spark(lambda: df.count(), label=f"count:virtual:{normalized_prefix}")
        )
        manifest = {
            "output_kind": OUTPUT_KIND_VIRTUAL,
            "query_sql": query_sql,
            "refresh_mode": refresh_mode,
            "generated_at": utcnow().isoformat(),
            "input_columns": list(df.columns or []),
            "row_count_hint": resolved_row_count_hint,
            "file_format_ignored": str(file_format or "parquet").strip().lower() or "parquet",
            "partition_by_ignored": [str(col).strip() for col in (partition_cols or []) if str(col).strip()],
        }
        payload = json.dumps(manifest, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
        await worker.storage.save_bytes(
            artifact_bucket,
            manifest_key,
            payload,
            content_type="application/json",
        )
        return build_s3_uri(artifact_bucket, manifest_key)

    async def materialize_ontology_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: Any,
        artifact_bucket: str,
        prefix: str,
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
    ) -> str:
        ontology_semantics = resolve_ontology_output_semantics(output_metadata)
        required_columns = list(ontology_semantics.required_columns)
        if required_columns:
            self.ensure_output_columns_present(
                df=df,
                required_columns=required_columns,
                output_kind=OUTPUT_KIND_ONTOLOGY,
            )
        return await self.materialize_output_dataframe(
            df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
        )

    def ensure_output_columns_present(
        self,
        *,
        df: Any,
        required_columns: List[str],
        output_kind: str,
    ) -> None:
        if not required_columns:
            return
        available_columns = set(df.columns or [])
        missing_columns = [column for column in required_columns if column not in available_columns]
        if missing_columns:
            raise ValueError(
                f"{output_kind} output columns missing from dataframe: {', '.join(missing_columns)}"
            )

    async def materialize_output_dataframe(
        self,
        df: Any,
        *,
        artifact_bucket: str,
        prefix: str,
        write_mode: str = "overwrite",
        file_prefix: Optional[str] = None,
        file_format: str = "parquet",
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        worker = self._worker
        if not worker.storage:
            raise RuntimeError("Storage service not available")
        await worker.storage.create_bucket(artifact_bucket)
        normalized_prefix = (prefix or "").lstrip("/").rstrip("/")
        if not normalized_prefix:
            raise ValueError("prefix is required")
        resolved_write_mode = str(write_mode or "overwrite").strip().lower() or "overwrite"
        if resolved_write_mode not in {"overwrite", "append"}:
            raise ValueError("write_mode must be overwrite or append")
        resolved_format = str(file_format or "parquet").strip().lower() or "parquet"
        if resolved_format not in {"parquet", "json", "csv", "avro", "orc"}:
            raise ValueError("file_format must be parquet|json|csv|avro|orc")
        resolved_partition_cols = [col for col in (partition_cols or []) if str(col).strip()]
        format_constraint_errors = validate_dataset_output_format_constraints(
            output_format=resolved_format,
            partition_by=resolved_partition_cols,
        )
        if format_constraint_errors:
            raise ValueError("; ".join(format_constraint_errors))
        if resolved_partition_cols:
            missing = [col for col in resolved_partition_cols if col not in df.columns]
            if missing:
                raise ValueError(f"Partition columns missing from output: {', '.join(missing)}")
        if resolved_write_mode == "overwrite":
            await worker.storage.delete_prefix(artifact_bucket, normalized_prefix)
        temp_dir = tempfile.mkdtemp(prefix="pipeline-output-")
        try:
            output_path = os.path.join(temp_dir, "data")
            writer = df.write.mode("overwrite")
            if resolved_partition_cols:
                writer = writer.partitionBy(*resolved_partition_cols)
            if resolved_format == "parquet":
                await worker._run_spark(
                    lambda: writer.parquet(output_path),
                    label=f"write_parquet:{normalized_prefix}",
                )
            elif resolved_format == "json":
                await worker._run_spark(
                    lambda: writer.json(output_path),
                    label=f"write_json:{normalized_prefix}",
                )
            elif resolved_format == "csv":
                await worker._run_spark(
                    lambda: writer.option("header", "true").csv(output_path),
                    label=f"write_csv:{normalized_prefix}",
                )
            elif resolved_format == "avro":
                await worker._run_spark(
                    lambda: writer.format("avro").save(output_path),
                    label=f"write_avro:{normalized_prefix}",
                )
            else:
                await worker._run_spark(
                    lambda: writer.orc(output_path),
                    label=f"write_orc:{normalized_prefix}",
                )
            extension_map = {
                "parquet": {".parquet"},
                "json": {".json"},
                "csv": {".csv"},
                "avro": {".avro"},
                "orc": {".orc"},
            }
            part_files = _list_part_files(
                output_path,
                extensions=extension_map.get(resolved_format, {f".{resolved_format}"}),
            )
            if not part_files:
                raise FileNotFoundError("Spark output part files not found")
            unique_prefix = str(file_prefix or "").strip().replace("/", "_")
            if not unique_prefix:
                unique_prefix = uuid4().hex[:12]
            for part_file in part_files:
                rel_path = os.path.relpath(part_file, output_path)
                base_name = os.path.basename(rel_path)
                dir_name = os.path.dirname(rel_path)
                if resolved_write_mode == "append":
                    base_name = f"{unique_prefix}_{base_name}"
                rel_path = os.path.join(dir_name, base_name) if dir_name else base_name
                target_key = f"{normalized_prefix}/{rel_path.replace(os.sep, '/')}"
                content_type = {
                    "parquet": "application/x-parquet",
                    "json": "application/json",
                    "csv": "text/csv",
                    "avro": "application/avro",
                    "orc": "application/orc",
                }.get(resolved_format, "application/octet-stream")
                with open(part_file, "rb") as handle:
                    await worker.storage.save_bytes(
                        artifact_bucket,
                        target_key,
                        handle.read(),
                        content_type=content_type,
                    )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
        return build_s3_uri(artifact_bucket, normalized_prefix)
