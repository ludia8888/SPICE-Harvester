"""
Pipeline Executor - Minimal transform engine for Pipeline Builder.

Executes a pipeline definition against dataset samples to produce preview/output data.
"""

from __future__ import annotations

import ast
import csv
import difflib
import io
import json
import logging
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from shared.config.settings import get_settings
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.pipeline.pipeline_profiler import compute_column_stats
from shared.services.pipeline.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes, topological_sort
from shared.services.pipeline.pipeline_parameter_utils import apply_parameters, normalize_parameters
from shared.services.storage.storage_service import StorageService
from shared.services.pipeline.pipeline_udf_runtime import compile_udf, resolve_udf_reference
from shared.services.pipeline.pipeline_definition_utils import (
    build_expectations_with_pk,
    resolve_delete_column,
    resolve_execution_semantics,
    resolve_pk_columns,
    resolve_pk_semantics,
    validate_pk_semantics,
    split_expectation_columns,
)
from shared.services.pipeline.pipeline_schema_utils import normalize_number
from shared.services.pipeline.pipeline_dataset_utils import normalize_dataset_selection, resolve_dataset_version
from shared.services.pipeline.pipeline_type_utils import (
    infer_xsd_type_from_values,
    normalize_cast_mode,
    normalize_cast_target,
    parse_decimal_text,
    parse_datetime_text,
    parse_int_text,
)
from shared.services.pipeline.pipeline_transform_spec import (
    normalize_operation,
    normalize_union_mode,
    resolve_join_spec,
    resolve_stream_join_effective_join_type,
    resolve_stream_join_spec,
)
from shared.services.pipeline.pipeline_validation_utils import (
    TableOps,
    validate_expectations,
    validate_schema_checks,
    validate_schema_contract,
)
from shared.utils.s3_uri import parse_s3_uri
from shared.services.pipeline.pipeline_join_keys import normalize_join_key_list

logger = logging.getLogger(__name__)


class PipelineExpectationError(ValueError):
    pass


@dataclass
class PipelineTable:
    columns: List[str]
    rows: List[Dict[str, Any]]

    def limited_rows(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if limit is None:
            return self.rows
        return self.rows[: max(0, int(limit))]


@dataclass
class PipelineRunResult:
    tables: Dict[str, PipelineTable]
    output_nodes: List[str]


class PipelineArtifactStore:
    def __init__(self, base_path: Optional[str] = None) -> None:
        root = base_path or get_settings().pipeline.artifact_path
        self.base_path = Path(root)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save_table(self, table: PipelineTable, *, dataset_name: str) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        filename = f"{dataset_name}_{timestamp}.json"
        path = self.base_path / filename
        payload = {
            "columns": [{"name": name, "type": "String"} for name in table.columns],
            "rows": table.rows,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        return str(path)


class PipelineExecutor:
    def __init__(
        self,
        dataset_registry: DatasetRegistry,
        pipeline_registry: Optional[PipelineRegistry] = None,
        artifact_store: Optional[PipelineArtifactStore] = None,
        storage_service: Optional[StorageService] = None,
    ) -> None:
        self._dataset_registry = dataset_registry
        self._pipeline_registry = pipeline_registry
        self._artifact_store = artifact_store
        self._storage_service = storage_service
        self._cast_mode = normalize_cast_mode(get_settings().pipeline.cast_mode)
        self._udf_require_reference = bool(get_settings().pipeline.udf_require_reference)
        self._udf_require_version_pinning = bool(get_settings().pipeline.udf_require_version_pinning)
        self._cast_stats: Dict[str, Dict[str, int]] = {}
        self._udf_code_cache: Dict[str, str] = {}
        self._udf_callable_cache: Dict[str, Any] = {}
        # Preview settings are derived from definition["__preview_meta__"] per-run.
        self._preview_mode: bool = False
        self._preview_max_output_rows: Optional[int] = None

    async def preview(
        self,
        *,
        definition: Dict[str, Any],
        db_name: str,
        node_id: Optional[str] = None,
        limit: Optional[int] = None,
        input_overrides: Optional[Dict[str, PipelineTable]] = None,
    ) -> Dict[str, Any]:
        result = await self.run(definition=definition, db_name=db_name, input_overrides=input_overrides)
        table = self._select_table(result, node_id)
        return self._table_to_sample(table, limit=limit)

    async def deploy(
        self,
        *,
        definition: Dict[str, Any],
        db_name: str,
        node_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        store_local: bool = False,
        input_overrides: Optional[Dict[str, PipelineTable]] = None,
    ) -> Tuple[PipelineTable, Dict[str, Any]]:
        result = await self.run(definition=definition, db_name=db_name, input_overrides=input_overrides)
        table = self._select_table(result, node_id)
        meta: Dict[str, Any] = {"row_count": len(table.rows)}
        if store_local and self._artifact_store:
            meta["artifact_key"] = self._artifact_store.save_table(
                table,
                dataset_name=(dataset_name or "pipeline_output"),
            )
        return table, meta

    async def run(
        self,
        *,
        definition: Dict[str, Any],
        db_name: str,
        input_overrides: Optional[Dict[str, PipelineTable]] = None,
    ) -> PipelineRunResult:
        self._cast_stats = {}
        nodes = normalize_nodes(definition.get("nodes"))
        edges = normalize_edges(definition.get("edges"))
        order = topological_sort(nodes, edges, include_unordered=True)
        parameters = normalize_parameters(definition.get("parameters"))
        preview_meta = definition.get("__preview_meta__") or {}
        preview_branch = preview_meta.get("branch")
        self._preview_mode = bool(definition.get("__preview_meta__"))
        self._preview_max_output_rows = None
        if self._preview_mode and "max_output_rows" in preview_meta:
            try:
                max_rows = int(preview_meta.get("max_output_rows") or 0)
            except (TypeError, ValueError):
                max_rows = 0
            if max_rows > 0:
                self._preview_max_output_rows = max_rows
        sample_limit = None
        if "sample_limit" in preview_meta:
            try:
                sample_limit = int(preview_meta.get("sample_limit") or 0)
            except (TypeError, ValueError):
                sample_limit = None
        if sample_limit is not None and sample_limit <= 0:
            sample_limit = None

        tables: Dict[str, PipelineTable] = {}
        incoming_map = build_incoming(edges)

        schema_errors: Dict[str, List[str]] = {}
        for node_id in order:
            node = nodes[node_id]
            incoming_tables = [tables[src] for src in incoming_map.get(node_id, []) if src in tables]
            node_type = str(node.get("type") or "transform")
            metadata = node.get("metadata") or {}

            if node_type in {"input", "read_dataset"}:
                if input_overrides and node_id in input_overrides:
                    table = input_overrides[node_id]
                else:
                    table = await self._load_input(
                        node,
                        db_name,
                        preview_branch,
                        sample_limit=sample_limit,
                    )
            elif node_type == "output":
                table = incoming_tables[0] if incoming_tables else PipelineTable([], [])
            else:
                table = await self._apply_transform(metadata, incoming_tables, parameters)

            table_ops = _build_table_ops(table)
            check_errors = validate_schema_checks(table_ops, metadata.get("schemaChecks") or [])
            if check_errors:
                schema_errors[node_id] = check_errors

            tables[node_id] = table

        if schema_errors:
            raise ValueError(f"Schema checks failed: {schema_errors}")

        output_nodes = [node_id for node_id, node in nodes.items() if node.get("type") == "output"]
        output_table = self._select_table(PipelineRunResult(tables=tables, output_nodes=output_nodes), None)
        output_node_id = output_nodes[-1] if output_nodes else None
        output_node = nodes.get(output_node_id) if output_node_id else {}
        output_metadata = output_node.get("metadata") or {}
        output_name = (
            output_metadata.get("outputName")
            or output_metadata.get("datasetName")
            or output_node.get("title")
            or output_node_id
        )
        declared_outputs = definition.get("outputs") if isinstance(definition.get("outputs"), list) else []
        execution_semantics = resolve_execution_semantics(definition)
        pk_semantics = resolve_pk_semantics(
            execution_semantics=execution_semantics,
            definition=definition,
            output_metadata=output_metadata,
        )
        delete_column = resolve_delete_column(definition=definition, output_metadata=output_metadata)
        pk_columns = resolve_pk_columns(
            definition=definition,
            output_metadata=output_metadata,
            output_name=output_name,
            output_node_id=output_node_id,
            declared_outputs=declared_outputs,
        )
        output_ops = _build_table_ops(output_table)
        available_columns = output_ops.columns
        preview_mode = bool(definition.get("__preview_meta__"))

        contract_errors = validate_schema_contract(
            output_ops,
            definition.get("schemaContract") or definition.get("schema_contract"),
        )
        if contract_errors:
            raise PipelineExpectationError(f"Schema contract failed: {contract_errors}")

        pk_semantic_errors = validate_pk_semantics(
            available_columns=available_columns,
            pk_semantics=pk_semantics,
            pk_columns=pk_columns,
            delete_column=delete_column,
        )
        if preview_mode:
            pk_columns = [col for col in pk_columns if col in available_columns]
            pk_semantic_errors = []
        expectations = build_expectations_with_pk(
            definition=definition,
            output_metadata=output_metadata,
            output_name=output_name,
            output_node_id=output_node_id,
            declared_outputs=declared_outputs,
            pk_semantics=pk_semantics,
            delete_column=delete_column,
            pk_columns=pk_columns,
            available_columns=available_columns,
        )
        expectation_errors = pk_semantic_errors + validate_expectations(output_ops, expectations)
        fk_errors = await self._evaluate_fk_expectations(
            expectations=expectations,
            output_table=output_table,
            db_name=db_name,
            branch=preview_branch,
        )
        expectation_errors = expectation_errors + fk_errors
        if expectation_errors:
            raise PipelineExpectationError(f"Expectations failed: {expectation_errors}")
        return PipelineRunResult(tables=tables, output_nodes=output_nodes)

    async def _load_input(
        self,
        node: Dict[str, Any],
        db_name: str,
        branch: Optional[str] = None,
        sample_limit: Optional[int] = None,
    ) -> PipelineTable:
        metadata = node.get("metadata") or {}
        selection = normalize_dataset_selection(metadata, default_branch=branch or "main")
        resolution = await resolve_dataset_version(
            self._dataset_registry,
            db_name=db_name,
            selection=selection,
        )
        dataset = resolution.dataset
        version = resolution.version

        columns = _extract_schema_columns(dataset.schema_json if dataset else {})
        rows: List[Dict[str, Any]] = []
        if dataset:
            if version:
                rows = (
                    await self._load_rows_from_artifact(version.artifact_key, max_rows=sample_limit)
                    if version.artifact_key
                    else []
                )
                if not rows:
                    rows = _extract_sample_rows(version.sample_json)
                if sample_limit and rows:
                    rows = rows[:sample_limit]
                if not columns:
                    columns = _extract_schema_columns(version.sample_json)
        if not columns:
            columns = _fallback_columns(node)
        if not rows:
            rows = _build_sample_rows(columns, 8)
        schema_types = _extract_schema_types(dataset.schema_json if dataset else {})
        if not schema_types and version is not None:
            schema_types = _extract_schema_types(version.sample_json)
        if schema_types:
            rows = _apply_schema_casts(
                rows,
                schema_types=schema_types,
                cast_mode=self._cast_mode,
                cast_stats=self._cast_stats,
            )
        return PipelineTable(columns=columns, rows=rows)

    async def _load_rows_from_artifact(
        self,
        artifact_key: Optional[str],
        *,
        max_rows: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if not artifact_key:
            return []
        parsed = parse_s3_uri(artifact_key)
        if not parsed or not self._storage_service:
            return []
        bucket, key = parsed
        resolved_limit = 200
        if max_rows is not None:
            try:
                resolved_limit = max(1, int(max_rows))
            except (TypeError, ValueError):
                resolved_limit = 200
        try:
            extension = os.path.splitext(key)[1].lower()
            if extension == ".csv":
                # Avoid loading the full CSV into memory; only read enough lines for the requested sample.
                estimated_bytes = max(64 * 1024, resolved_limit * 512)
                raw_bytes = await self._storage_service.load_bytes_lines(
                    bucket,
                    key,
                    max_lines=resolved_limit + 1,  # header + rows
                    max_bytes=estimated_bytes,
                )
            else:
                raw_bytes = await self._storage_service.load_bytes(bucket, key)
        except Exception as exc:
            logger.warning("Failed to load artifact bytes from storage (bucket=%s key=%s): %s", bucket, key, exc, exc_info=True)
            return []
        if extension == ".csv":
            return _parse_csv_bytes(raw_bytes, max_rows=resolved_limit)
        if extension in {".xlsx", ".xlsm"}:
            return _parse_excel_bytes(raw_bytes, max_rows=resolved_limit)
        if extension == ".json":
            return _parse_json_bytes(raw_bytes, max_rows=resolved_limit)
        prefix = key.rstrip("/")
        if not prefix:
            return []
        prefix = f"{prefix}/"
        try:
            objects = await self._storage_service.list_objects(bucket, prefix=prefix)
        except Exception as exc:
            logger.warning("Failed to list artifact objects (bucket=%s prefix=%s): %s", bucket, prefix, exc, exc_info=True)
            return []
        keys = [obj.get("Key") for obj in objects or [] if obj.get("Key")]
        for candidate in keys:
            ext = os.path.splitext(candidate)[1].lower()
            if ext not in {".json", ".csv", ".xlsx", ".xlsm"}:
                continue
            try:
                candidate_bytes = await self._storage_service.load_bytes(bucket, candidate)
            except Exception as exc:
                logger.warning(
                    "Failed to load artifact candidate bytes (bucket=%s key=%s): %s",
                    bucket,
                    candidate,
                    exc,
                    exc_info=True,
                )
                continue
            if ext == ".csv":
                return _parse_csv_bytes(candidate_bytes, max_rows=resolved_limit)
            if ext in {".xlsx", ".xlsm"}:
                return _parse_excel_bytes(candidate_bytes, max_rows=resolved_limit)
            if ext == ".json":
                return _parse_json_bytes(candidate_bytes, max_rows=resolved_limit)
        return []

    async def _load_fk_reference_rows(
        self,
        *,
        db_name: str,
        dataset_id: Optional[str],
        dataset_name: Optional[str],
        branch: Optional[str],
    ) -> List[Dict[str, Any]]:
        metadata = {}
        if dataset_id:
            metadata["datasetId"] = dataset_id
        if dataset_name:
            metadata["datasetName"] = dataset_name
        if branch:
            metadata["datasetBranch"] = branch
        selection = normalize_dataset_selection(metadata, default_branch=branch or "main")
        resolution = await resolve_dataset_version(
            self._dataset_registry,
            db_name=db_name,
            selection=selection,
        )
        dataset = resolution.dataset
        version = resolution.version
        if not dataset or not version:
            return []
        rows = await self._load_rows_from_artifact(version.artifact_key) if version.artifact_key else []
        if not rows:
            rows = _extract_sample_rows(version.sample_json)
        return rows

    async def _evaluate_fk_expectations(
        self,
        *,
        expectations: List[Dict[str, Any]],
        output_table: PipelineTable,
        db_name: str,
        branch: Optional[str],
    ) -> List[str]:
        errors: List[str] = []
        for exp in expectations or []:
            if not isinstance(exp, dict):
                continue
            rule = str(exp.get("rule") or "").strip().lower()
            if rule != "fk_exists":
                continue
            columns_value = exp.get("columns") or exp.get("column")
            if isinstance(columns_value, list):
                columns = [str(item).strip() for item in columns_value if str(item).strip()]
            else:
                columns = split_expectation_columns(str(columns_value or ""))
            reference = exp.get("reference") if isinstance(exp.get("reference"), dict) else {}
            ref_columns_value = (
                reference.get("columns")
                or reference.get("column")
                or exp.get("ref_columns")
                or exp.get("ref_column")
            )
            if isinstance(ref_columns_value, list):
                ref_columns = [str(item).strip() for item in ref_columns_value if str(item).strip()]
            else:
                ref_columns = split_expectation_columns(str(ref_columns_value or ""))
            if not ref_columns and columns:
                ref_columns = list(columns)

            dataset_id = (
                reference.get("datasetId")
                or reference.get("dataset_id")
                or exp.get("datasetId")
                or exp.get("dataset_id")
            )
            dataset_name = (
                reference.get("datasetName")
                or reference.get("dataset_name")
                or exp.get("datasetName")
                or exp.get("dataset_name")
            )
            ref_branch = (
                reference.get("branch")
                or exp.get("branch")
                or branch
                or "main"
            )
            allow_nulls = exp.get("allow_nulls") if "allow_nulls" in exp else exp.get("allowNulls")
            allow_nulls = True if allow_nulls is None else bool(allow_nulls)

            if not columns or not ref_columns:
                errors.append("fk_exists missing columns or reference columns")
                continue
            if len(columns) != len(ref_columns):
                errors.append(
                    f"fk_exists column count mismatch: {columns} vs {ref_columns}"
                )
                continue
            missing_cols = [col for col in columns if col not in output_table.columns]
            if missing_cols:
                errors.append(f"fk_exists missing column(s): {', '.join(missing_cols)}")
                continue
            ref_rows = await self._load_fk_reference_rows(
                db_name=db_name,
                dataset_id=str(dataset_id) if dataset_id else None,
                dataset_name=str(dataset_name) if dataset_name else None,
                branch=str(ref_branch) if ref_branch else None,
            )
            if not ref_rows:
                errors.append("fk_exists reference dataset has no rows")
                continue
            ref_available_cols: set[str] = set()
            for row in ref_rows:
                ref_available_cols.update(str(key) for key in row.keys())
            missing_ref_cols = [col for col in ref_columns if col not in ref_available_cols]
            if missing_ref_cols:
                errors.append(f"fk_exists reference missing column(s): {', '.join(missing_ref_cols)}")
                continue

            ref_keys: set[tuple[Any, ...]] = set()
            for row in ref_rows:
                key = tuple(row.get(col) for col in ref_columns)
                if any(value is None for value in key):
                    continue
                ref_keys.add(key)

            missing_count = 0
            for row in output_table.rows:
                key = tuple(row.get(col) for col in columns)
                if allow_nulls and any(value is None for value in key):
                    continue
                if key not in ref_keys:
                    missing_count += 1
            if missing_count:
                ref_label = dataset_name or dataset_id or "reference"
                errors.append(
                    f"fk_exists failed: {','.join(columns)} -> {ref_label}({','.join(ref_columns)}) missing={missing_count}"
                )
        return errors

    async def _apply_transform(
        self,
        metadata: Dict[str, Any],
        inputs: List[PipelineTable],
        parameters: Dict[str, Any],
    ) -> PipelineTable:
        if not inputs:
            return PipelineTable([], [])
        operation = normalize_operation(metadata.get("operation"))
        if not operation and len(inputs) >= 2:
            raise ValueError("transform has multiple inputs but no operation")
        if operation == "join" and len(inputs) >= 2:
            join_spec = resolve_join_spec(metadata)
            return _join_tables(
                inputs[0],
                inputs[1],
                join_type=join_spec.join_type,
                left_key=join_spec.left_key,
                right_key=join_spec.right_key,
                left_keys=join_spec.left_keys,
                right_keys=join_spec.right_keys,
                allow_cross_join=join_spec.allow_cross_join,
                max_output_rows=self._preview_max_output_rows if self._preview_mode else None,
            )
        if operation == "streamJoin" and len(inputs) >= 2:
            return _stream_join_tables(
                inputs[0],
                inputs[1],
                metadata=metadata,
                max_output_rows=self._preview_max_output_rows if self._preview_mode else None,
            )
        if operation == "split":
            condition = str(metadata.get("condition") or metadata.get("expression") or "")
            return _filter_table(inputs[0], condition, parameters)
        if operation == "filter":
            return _filter_table(inputs[0], str(metadata.get("expression") or ""), parameters)
        if operation == "compute":
            table = inputs[0]
            # Structured compute (preferred): {"assignments":[{"column":"c","expression":"..."}]}
            assignments = (
                metadata.get("assignments")
                or metadata.get("computedColumns")
                or metadata.get("computed_columns")
            )
            if isinstance(assignments, list) and assignments:
                next_table = table
                for item in assignments:
                    if not isinstance(item, dict):
                        continue
                    target = str(item.get("column") or item.get("target") or item.get("name") or "").strip()
                    expr = str(item.get("expression") or item.get("expr") or item.get("formula") or "").strip()
                    if not target or not expr:
                        continue
                    next_table = _compute_assignment_table(next_table, target, expr, parameters)
                return next_table

            # Structured single-column compute: {"targetColumn":"c","formula":"..."}
            target = (
                metadata.get("targetColumn")
                or metadata.get("target_column")
                or metadata.get("target")
                or metadata.get("column")
            )
            formula = metadata.get("formula") or metadata.get("expr")
            if target and formula:
                target_text = str(target).strip()
                formula_text = str(formula).strip()
                if target_text and formula_text:
                    return _compute_assignment_table(table, target_text, formula_text, parameters)

            return _compute_table(table, str(metadata.get("expression") or ""), parameters)
        if operation == "explode":
            columns = metadata.get("columns") or []
            if columns:
                return _explode_table(inputs[0], str(columns[0]))
        if operation == "select":
            expressions = metadata.get("expressions") or metadata.get("selectExpr") or metadata.get("select_expr")
            if isinstance(expressions, list) and expressions:
                # Preview engine only supports plain column names (or "col as alias") in selectExpr.
                projected: list[str] = []
                renames: dict[str, str] = {}
                for expr in expressions:
                    text = str(expr or "").strip()
                    if not text:
                        continue
                    lower = text.lower()
                    if " as " in lower:
                        left, right = [part.strip() for part in text.rsplit(" as ", 1)]
                        if left and right:
                            projected.append(left)
                            renames[left] = right
                    else:
                        projected.append(text)
                if projected:
                    selected = _select_columns(inputs[0], projected)
                    if renames:
                        selected = _rename_columns(selected, renames)
                    return selected
            columns = metadata.get("columns") or []
            if columns:
                return _select_columns(inputs[0], columns)
        if operation == "drop":
            columns = metadata.get("columns") or []
            if columns:
                return _drop_columns(inputs[0], columns)
        if operation == "rename":
            rename_map = metadata.get("rename") or {}
            if rename_map:
                return _rename_columns(inputs[0], rename_map)
        if operation == "normalize":
            columns = metadata.get("columns") or []
            if columns:
                return _normalize_table(
                    inputs[0],
                    columns,
                    trim=bool(metadata.get("trim", True)),
                    empty_to_null=bool(metadata.get("emptyToNull", metadata.get("empty_to_null", True))),
                    whitespace_to_null=bool(metadata.get("whitespaceToNull", metadata.get("whitespace_to_null", True))),
                    lowercase=bool(metadata.get("lowercase", False)),
                    uppercase=bool(metadata.get("uppercase", False)),
                )
        if operation == "regexReplace":
            rules = _normalize_regex_rules(metadata)
            if rules:
                return _regex_replace_table(inputs[0], rules)
        if operation == "cast":
            casts = metadata.get("casts") or []
            if casts:
                return _cast_columns(
                    inputs[0],
                    casts,
                    cast_mode=self._cast_mode,
                    cast_stats=self._cast_stats,
                )
        if operation == "geospatial":
            return _geospatial_table(inputs[0], metadata, parameters)
        if operation == "patternMining":
            return _pattern_mining_table(inputs[0], metadata, parameters)
        if operation == "udf":
            return await self._apply_udf_transform(inputs[0], metadata)
        if operation == "dedupe":
            subset = metadata.get("columns") or []
            return _dedupe_table(inputs[0], subset)
        if operation == "sort":
            columns = metadata.get("columns") or []
            if columns:
                return _sort_table(inputs[0], columns)
        if operation == "union" and len(inputs) >= 2:
            union_mode = normalize_union_mode(metadata)
            return _union_tables(inputs[0], inputs[1], union_mode=union_mode)
        if operation in {"groupBy", "aggregate"}:
            return _group_by_table(inputs[0], metadata.get("groupBy") or [], metadata.get("aggregates") or [])
        if operation == "pivot":
            return _pivot_table(inputs[0], metadata.get("pivot") or {})
        if operation == "window":
            return _window_table(inputs[0], metadata.get("window") or {})
        return inputs[0]

    async def _apply_udf_transform(self, table: PipelineTable, metadata: Dict[str, Any]) -> PipelineTable:
        resolved = await resolve_udf_reference(
            metadata=metadata,
            pipeline_registry=self._pipeline_registry,
            require_reference=self._udf_require_reference,
            require_version_pinning=self._udf_require_version_pinning,
            code_cache=self._udf_code_cache,
        )
        fn = self._udf_callable_cache.get(resolved.cache_key)
        if fn is None:
            fn = compile_udf(resolved.code)
            self._udf_callable_cache[resolved.cache_key] = fn

        out_rows: List[Dict[str, Any]] = []
        for row in table.rows:
            out_rows.append(fn(dict(row)))

        columns: List[str] = list(table.columns)
        seen = set(columns)
        for row in out_rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    columns.append(key)
        return PipelineTable(columns=columns, rows=out_rows)

    def _table_to_sample(self, table: PipelineTable, *, limit: Optional[int]) -> Dict[str, Any]:
        inferred = _infer_column_types(table)
        columns = [{"name": name, "type": inferred.get(name, "xsd:string")} for name in table.columns]
        rows = table.limited_rows(limit or 200)
        return {
            "row_count": len(table.rows),
            "columns": columns,
            "rows": rows,
            "column_stats": compute_column_stats(rows=rows, columns=columns),
            "cast_stats": self._summarize_cast_stats(table.columns),
        }

    def _summarize_cast_stats(self, columns: List[str]) -> Dict[str, Dict[str, Any]]:
        summary: Dict[str, Dict[str, Any]] = {}
        for name in columns:
            stats = self._cast_stats.get(name)
            if not stats:
                continue
            attempted = int(stats.get("attempted") or 0)
            failed = int(stats.get("failed") or 0)
            failure_rate = (failed / attempted) if attempted else 0.0
            summary[name] = {
                "attempted": attempted,
                "failed": failed,
                "failure_rate": round(failure_rate, 4),
            }
        return summary

    def _select_table(self, result: PipelineRunResult, node_id: Optional[str]) -> PipelineTable:
        if node_id and node_id in result.tables:
            return result.tables[node_id]
        if result.output_nodes:
            return result.tables.get(result.output_nodes[-1], PipelineTable([], []))
        if result.tables:
            return list(result.tables.values())[-1]
        return PipelineTable([], [])


def _extract_schema_columns(schema: Any) -> List[str]:
    if not isinstance(schema, dict):
        return []
    if isinstance(schema.get("columns"), list):
        columns = []
        for col in schema["columns"]:
            if isinstance(col, dict) and col.get("name"):
                columns.append(str(col["name"]))
            elif isinstance(col, str):
                columns.append(col)
        return columns
    if isinstance(schema.get("fields"), list):
        return [str(col.get("name")) for col in schema["fields"] if isinstance(col, dict) and col.get("name")]
    if isinstance(schema.get("properties"), dict):
        return list(schema["properties"].keys())
    return []


def _extract_schema_types(schema: Any) -> Dict[str, str]:
    if not isinstance(schema, dict):
        return {}
    columns = schema.get("columns")
    if not isinstance(columns, list):
        columns = schema.get("fields")
    if isinstance(columns, list):
        output: Dict[str, str] = {}
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or "").strip()
                if not name:
                    continue
                output[name] = normalize_cast_target(col.get("type") or col.get("data_type") or "xsd:string")
            elif isinstance(col, str):
                output[col] = "xsd:string"
        return output
    properties = schema.get("properties")
    if isinstance(properties, dict):
        return {str(name): "xsd:string" for name in properties.keys() if name}
    return {}


def _extract_sample_rows(sample: Any) -> List[Dict[str, Any]]:
    if not isinstance(sample, dict):
        return []
    rows = sample.get("rows")
    if isinstance(rows, list):
        if rows and isinstance(rows[0], dict):
            return rows  # type: ignore[return-value]
        columns = _extract_schema_columns(sample)
        return [
            {columns[idx] if idx < len(columns) else f"col_{idx}": value for idx, value in enumerate(row)}
            for row in rows
            if isinstance(row, list)
        ]
    data_rows = sample.get("data")
    if isinstance(data_rows, list) and data_rows and isinstance(data_rows[0], dict):
        return data_rows  # type: ignore[return-value]
    return []


def _fallback_columns(node: Dict[str, Any]) -> List[str]:
    columns = node.get("columns")
    if isinstance(columns, list):
        return [str(col) for col in columns if col]
    subtitle = node.get("subtitle")
    if isinstance(subtitle, str) and subtitle:
        return [subtitle]
    return []


def _build_sample_rows(columns: List[str], count: int) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for idx in range(count):
        row = {}
        for col in columns:
            row[col] = f"{col}_{idx + 1}"
        output.append(row)
    return output


def _group_by_table(
    table: PipelineTable,
    group_by: List[str],
    aggregates: List[Dict[str, Any]],
) -> PipelineTable:
    if not aggregates:
        return table
    specs = []
    for agg in aggregates:
        column = str(agg.get("column") or "").strip()
        op = str(agg.get("op") or "").lower().strip()
        if not column or not op:
            continue
        alias = str(agg.get("alias") or f"{op}_{column}")
        specs.append({"column": column, "op": op, "alias": alias})
    if not specs:
        return table

    grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for row in table.rows:
        key = tuple(row.get(col) for col in group_by) if group_by else ("__all__",)
        state = grouped.setdefault(key, {spec["alias"]: {"op": spec["op"], "sum": 0, "count": 0, "min": None, "max": None} for spec in specs})
        for spec in specs:
            value = row.get(spec["column"])
            agg_state = state[spec["alias"]]
            if spec["op"] == "count":
                if value is not None:
                    agg_state["count"] += 1
            elif spec["op"] == "sum":
                if value is not None:
                    try:
                        agg_state["sum"] += float(value)
                    except (TypeError, ValueError):
                        # Preview engine is best-effort: ignore non-numeric values rather than crashing.
                        continue
            elif spec["op"] == "avg":
                if value is not None:
                    try:
                        agg_state["sum"] += float(value)
                        agg_state["count"] += 1
                    except (TypeError, ValueError):
                        continue
            elif spec["op"] == "min":
                if value is not None:
                    agg_state["min"] = value if agg_state["min"] is None else min(agg_state["min"], value)
            elif spec["op"] == "max":
                if value is not None:
                    agg_state["max"] = value if agg_state["max"] is None else max(agg_state["max"], value)

    rows: List[Dict[str, Any]] = []
    for key, state in grouped.items():
        row: Dict[str, Any] = {}
        if group_by:
            for idx, col in enumerate(group_by):
                row[col] = key[idx]
        for alias, agg_state in state.items():
            op = agg_state["op"]
            if op == "count":
                row[alias] = agg_state["count"]
            elif op == "sum":
                row[alias] = agg_state["sum"]
            elif op == "avg":
                row[alias] = agg_state["sum"] / agg_state["count"] if agg_state["count"] else None
            elif op == "min":
                row[alias] = agg_state["min"]
            elif op == "max":
                row[alias] = agg_state["max"]
        rows.append(row)

    output_columns = list(group_by) + [spec["alias"] for spec in specs]
    return PipelineTable(columns=output_columns, rows=rows)


def _pivot_table(table: PipelineTable, pivot_meta: Dict[str, Any]) -> PipelineTable:
    index_cols = pivot_meta.get("index") or []
    pivot_col = pivot_meta.get("columns")
    value_col = pivot_meta.get("values")
    agg = str(pivot_meta.get("agg") or "sum").lower()
    if not index_cols or not pivot_col or not value_col:
        return table
    result: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    pivot_values: set[Any] = set()
    for row in table.rows:
        key = tuple(row.get(col) for col in index_cols)
        pivot_key = row.get(pivot_col)
        value = row.get(value_col)
        if pivot_key is None:
            continue
        pivot_values.add(pivot_key)
        bucket = result.setdefault(key, {"__count__": {}, "__sum__": {}})
        counts = bucket["__count__"]
        sums = bucket["__sum__"]
        counts[pivot_key] = counts.get(pivot_key, 0) + 1
        if value is not None:
            sums[pivot_key] = sums.get(pivot_key, 0) + float(value)

    columns = list(index_cols) + [str(val) for val in sorted(pivot_values, key=lambda x: str(x))]
    rows: List[Dict[str, Any]] = []
    for key, bucket in result.items():
        row: Dict[str, Any] = {}
        for idx, col in enumerate(index_cols):
            row[col] = key[idx]
        counts = bucket["__count__"]
        sums = bucket["__sum__"]
        for pivot_key in pivot_values:
            if agg == "count":
                row[str(pivot_key)] = counts.get(pivot_key, 0)
            elif agg == "avg":
                count = counts.get(pivot_key, 0)
                row[str(pivot_key)] = sums.get(pivot_key, 0) / count if count else None
            else:
                row[str(pivot_key)] = sums.get(pivot_key, 0)
        rows.append(row)
    return PipelineTable(columns=columns, rows=rows)


def _nulls_last_sort_key(*, row: Dict[str, Any], column: str) -> Any:
    value = row.get(column)
    return (value is None, value)


def _window_table(table: PipelineTable, window_meta: Dict[str, Any]) -> PipelineTable:
    partition_by = window_meta.get("partitionBy") or []
    order_by = window_meta.get("orderBy") or []
    if not order_by:
        return table
    # orderBy supports:
    # - ["col1", "-col2"]  (prefix '-' for DESC)
    # - [{"column":"col1","direction":"asc|desc"}, ...]
    specs: list[tuple[str, str]] = []
    for item in order_by:
        if isinstance(item, str):
            col = item.strip()
            if not col:
                continue
            direction = "asc"
            if col.startswith("-"):
                direction = "desc"
                col = col[1:].strip()
            if col:
                specs.append((col, direction))
            continue
        if isinstance(item, dict):
            col = str(item.get("column") or item.get("name") or "").strip()
            if not col:
                continue
            direction = str(item.get("direction") or item.get("dir") or "asc").strip().lower()
            if direction not in {"asc", "desc"}:
                direction = "asc"
            specs.append((col, direction))
    if not specs:
        return table
    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for row in table.rows:
        key = tuple(row.get(col) for col in partition_by) if partition_by else ("__all__",)
        grouped.setdefault(key, []).append(row)
    rows: List[Dict[str, Any]] = []
    for key, bucket in grouped.items():
        bucket_sorted = list(bucket)
        # Multi-key sort with per-column direction (stable sort from last key to first).
        for col, direction in reversed(specs):
            reverse = direction == "desc"
            bucket_sorted.sort(
                key=lambda row, _col=col: _nulls_last_sort_key(row=row, column=_col),
                reverse=reverse,
            )
        for idx, row in enumerate(bucket_sorted, start=1):
            next_row = dict(row)
            next_row["row_number"] = idx
            rows.append(next_row)
    columns = list(table.columns)
    if "row_number" not in columns:
        columns.append("row_number")
    return PipelineTable(columns=columns, rows=rows)


def _select_columns(table: PipelineTable, columns: List[str]) -> PipelineTable:
    cols = [col for col in columns if col in table.columns]
    rows = [{col: row.get(col) for col in cols} for row in table.rows]
    return PipelineTable(columns=cols, rows=rows)


def _drop_columns(table: PipelineTable, columns: List[str]) -> PipelineTable:
    drop = set(columns)
    cols = [col for col in table.columns if col not in drop]
    rows = [{col: row.get(col) for col in cols} for row in table.rows]
    return PipelineTable(columns=cols, rows=rows)


def _rename_columns(table: PipelineTable, rename_map: Dict[str, Any]) -> PipelineTable:
    mapping = {str(k): str(v) for k, v in rename_map.items() if k}
    cols = [mapping.get(col, col) for col in table.columns]
    rows: List[Dict[str, Any]] = []
    for row in table.rows:
        new_row: Dict[str, Any] = {}
        for col in table.columns:
            new_row[mapping.get(col, col)] = row.get(col)
        rows.append(new_row)
    return PipelineTable(columns=cols, rows=rows)


def _record_cast_stat(
    cast_stats: Optional[Dict[str, Dict[str, int]]],
    *,
    column: str,
    attempted: bool,
    failed: bool,
) -> None:
    if not cast_stats or not attempted:
        return
    entry = cast_stats.setdefault(column, {"attempted": 0, "failed": 0})
    entry["attempted"] = int(entry.get("attempted") or 0) + 1
    if failed:
        entry["failed"] = int(entry.get("failed") or 0) + 1


def _cast_value_with_status(value: Any, target: str, *, cast_mode: str) -> tuple[Any, bool, bool]:
    normalized = normalize_cast_target(target)
    if value is None:
        return None, False, False
    if normalized == "xsd:string":
        # Preserve structured values (lists/dicts) so downstream transforms like explode can operate.
        if isinstance(value, (list, dict)):
            return value, False, False
        if isinstance(value, tuple):
            return list(value), False, False
        text = str(value).strip() if not isinstance(value, bool) else str(value)
        if cast_mode == "SAFE_NULL" and text == "":
            return None, False, False
        return text, False, False

    text = str(value).strip() if not isinstance(value, bool) else str(value)
    if text == "":
        if cast_mode == "SAFE_NULL":
            return None, False, False
        raise ValueError(f"invalid empty value for {normalized}")

    attempted = True
    if normalized == "xsd:boolean":
        if isinstance(value, bool):
            return value, attempted, False
        lowered = text.lower()
        if lowered in {"true", "t", "1", "yes", "y"}:
            return True, attempted, False
        if lowered in {"false", "f", "0", "no", "n"}:
            return False, attempted, False
        if cast_mode == "SAFE_NULL":
            return None, attempted, True
        raise ValueError(f"invalid boolean: {value}")

    if normalized in {"xsd:integer", "xsd:decimal"}:
        parsed = parse_decimal_text(text) if normalized == "xsd:decimal" else parse_int_text(text)
        if parsed is None:
            if cast_mode == "SAFE_NULL":
                return None, attempted, True
            raise ValueError(f"invalid numeric: {value}")
        return parsed, attempted, False

    if normalized in {"xsd:date", "xsd:dateTime"}:
        parsed = parse_datetime_text(text, allow_ambiguous=True)
        if parsed is None:
            if cast_mode == "SAFE_NULL":
                return None, attempted, True
            raise ValueError(f"invalid datetime: {value}")
        return (parsed.date(), attempted, False) if normalized == "xsd:date" else (parsed, attempted, False)
    return value, attempted, False


def _cast_value(value: Any, target: str, *, cast_mode: str) -> Any:
    casted, _, _ = _cast_value_with_status(value, target, cast_mode=cast_mode)
    return casted


def _apply_schema_casts(
    rows: List[Dict[str, Any]],
    *,
    schema_types: Dict[str, str],
    cast_mode: str,
    cast_stats: Optional[Dict[str, Dict[str, int]]] = None,
) -> List[Dict[str, Any]]:
    if not rows or not schema_types:
        return rows
    cast_map = {name: schema_types.get(name) for name in schema_types}
    output: List[Dict[str, Any]] = []
    for row in rows:
        new_row = dict(row)
        for column, target in cast_map.items():
            if column in new_row and target:
                casted, attempted, failed = _cast_value_with_status(new_row[column], target, cast_mode=cast_mode)
                _record_cast_stat(cast_stats, column=column, attempted=attempted, failed=failed)
                new_row[column] = casted
        output.append(new_row)
    return output


def _cast_columns(
    table: PipelineTable,
    casts: List[Dict[str, Any]],
    *,
    cast_mode: str,
    cast_stats: Optional[Dict[str, Dict[str, int]]] = None,
) -> PipelineTable:
    cast_map = {
        str(item.get("column")): str(item.get("type"))
        for item in casts
        if item.get("column") and item.get("type")
    }
    if not cast_map:
        return table
    rows: List[Dict[str, Any]] = []
    for row in table.rows:
        new_row = dict(row)
        for column, target in cast_map.items():
            if column in new_row:
                casted, attempted, failed = _cast_value_with_status(new_row[column], target, cast_mode=cast_mode)
                _record_cast_stat(cast_stats, column=column, attempted=attempted, failed=failed)
                new_row[column] = casted
        rows.append(new_row)
    return PipelineTable(columns=table.columns, rows=rows)


def _dedupe_table(table: PipelineTable, columns: List[str]) -> PipelineTable:
    seen = set()
    rows: List[Dict[str, Any]] = []
    keys = columns if columns else table.columns
    for row in table.rows:
        key = tuple(row.get(col) for col in keys)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return PipelineTable(columns=table.columns, rows=rows)


def _sort_table(table: PipelineTable, columns: List[Any]) -> PipelineTable:
    # columns supports:
    # - ["col1", "-col2"]  (prefix '-' for DESC)
    # - [{"column":"col1","direction":"asc|desc"}, ...]
    specs: list[tuple[str, str]] = []
    for item in columns or []:
        if isinstance(item, str):
            col = item.strip()
            if not col:
                continue
            direction = "asc"
            if col.startswith("-"):
                direction = "desc"
                col = col[1:].strip()
            if col and col in table.columns:
                specs.append((col, direction))
            continue
        if isinstance(item, dict):
            col = str(item.get("column") or item.get("name") or "").strip()
            if not col or col not in table.columns:
                continue
            direction = str(item.get("direction") or item.get("dir") or "asc").strip().lower()
            if direction not in {"asc", "desc"}:
                direction = "asc"
            specs.append((col, direction))
    if not specs:
        return table

    rows_sorted = list(table.rows)
    # Multi-key sort with per-column direction (stable sort from last key to first).
    for col, direction in reversed(specs):
        reverse = direction == "desc"
        rows_sorted.sort(
            key=lambda row, _col=col: _nulls_last_sort_key(row=row, column=_col),
            reverse=reverse,
        )

    return PipelineTable(columns=table.columns, rows=rows_sorted)


def _union_tables(left: PipelineTable, right: PipelineTable, *, union_mode: str = "strict") -> PipelineTable:
    mode = (union_mode or "strict").strip().lower()
    left_set = set(left.columns)
    right_set = set(right.columns)

    if mode == "strict":
        if left_set != right_set:
            missing_left = sorted(right_set - left_set)
            missing_right = sorted(left_set - right_set)
            raise ValueError(
                "union schema mismatch (strict): "
                f"missing_in_left={missing_left} missing_in_right={missing_right}"
            )
        columns = list(left.columns)
        rows = [{col: row.get(col) for col in columns} for row in left.rows] + [
            {col: row.get(col) for col in columns} for row in right.rows
        ]
        return PipelineTable(columns=columns, rows=rows)

    if mode == "common_only":
        common = [col for col in left.columns if col in right_set]
        if not common:
            raise ValueError("union has no common columns")
        rows = [{col: row.get(col) for col in common} for row in left.rows] + [
            {col: row.get(col) for col in common} for row in right.rows
        ]
        return PipelineTable(columns=common, rows=rows)

    if mode in {"pad_missing_nulls", "pad"}:
        columns = list(left.columns) + [col for col in right.columns if col not in left_set]
        rows = [{col: row.get(col) for col in columns} for row in left.rows] + [
            {col: row.get(col) for col in columns} for row in right.rows
        ]
        return PipelineTable(columns=columns, rows=rows)

    raise ValueError(f"Invalid unionMode: {union_mode}")


def _join_tables(
    left: PipelineTable,
    right: PipelineTable,
    join_type: Optional[str],
    left_key: Optional[str] = None,
    right_key: Optional[str] = None,
    join_key: Optional[str] = None,
    left_keys: Optional[List[str]] = None,
    right_keys: Optional[List[str]] = None,
    allow_cross_join: bool = False,
    max_output_rows: Optional[int] = None,
) -> PipelineTable:
    limit: Optional[int] = None
    if max_output_rows is not None:
        try:
            limit = int(max_output_rows)
        except (TypeError, ValueError):
            limit = None
        if limit is not None and limit <= 0:
            limit = None

    join_type = (join_type or "inner").lower()
    if join_key and not left_key:
        left_key = join_key
    if join_key and not right_key:
        right_key = join_key

    resolved_left = normalize_join_key_list(left_keys)
    resolved_right = normalize_join_key_list(right_keys)
    if left_key and not resolved_left:
        resolved_left = [left_key]
    if right_key and not resolved_right:
        resolved_right = [right_key]

    if not resolved_left or not resolved_right:
        if allow_cross_join:
            if join_type != "cross":
                raise ValueError("join allowCrossJoin requires joinType='cross'")
        else:
            raise ValueError("join requires leftKey/rightKey or leftKeys/rightKeys (or joinKey)")
    if resolved_left and resolved_right and len(resolved_left) != len(resolved_right):
        raise ValueError("join requires leftKeys/rightKeys of the same length")

    right_column_map = _build_join_output_layout(left.columns, right.columns)
    columns = left.columns + [mapped for _, mapped in right_column_map]

    rows: List[Dict[str, Any]] = []
    if resolved_left and resolved_right:
        if len(resolved_left) == 1:
            left_join = resolved_left[0]
            right_join = resolved_right[0]
            right_index: Dict[Any, List[Tuple[int, Dict[str, Any]]]] = {}
            for idx, row in enumerate(right.rows):
                right_index.setdefault(row.get(right_join), []).append((idx, row))
            matched_right: set[int] = set()
            for row in left.rows:
                key = row.get(left_join)
                matches = right_index.get(key) or []
                if matches:
                    for idx, match in matches:
                        rows.append(_merge_rows(row, match, right_column_map))
                        matched_right.add(idx)
                        if limit is not None and len(rows) >= limit:
                            return PipelineTable(columns=columns, rows=rows)
                elif join_type in {"left", "full"}:
                    rows.append(_merge_rows(row, None, right_column_map))
                    if limit is not None and len(rows) >= limit:
                        return PipelineTable(columns=columns, rows=rows)
            if join_type in {"right", "full"}:
                for idx, row in enumerate(right.rows):
                    if idx not in matched_right:
                        rows.append(_merge_rows(None, row, right_column_map))
                        if limit is not None and len(rows) >= limit:
                            return PipelineTable(columns=columns, rows=rows)
            return PipelineTable(columns=columns, rows=rows)

        def row_key(row: Dict[str, Any], keys: List[str]) -> Tuple[Any, ...]:
            return tuple(row.get(key) for key in keys)

        right_index: Dict[Tuple[Any, ...], List[Tuple[int, Dict[str, Any]]]] = {}
        for idx, row in enumerate(right.rows):
            right_index.setdefault(row_key(row, resolved_right), []).append((idx, row))
        matched_right: set[int] = set()
        for row in left.rows:
            key = row_key(row, resolved_left)
            matches = right_index.get(key) or []
            if matches:
                for idx, match in matches:
                    rows.append(_merge_rows(row, match, right_column_map))
                    matched_right.add(idx)
                    if limit is not None and len(rows) >= limit:
                        return PipelineTable(columns=columns, rows=rows)
            elif join_type in {"left", "full"}:
                rows.append(_merge_rows(row, None, right_column_map))
                if limit is not None and len(rows) >= limit:
                    return PipelineTable(columns=columns, rows=rows)
        if join_type in {"right", "full"}:
            for idx, row in enumerate(right.rows):
                if idx not in matched_right:
                    rows.append(_merge_rows(None, row, right_column_map))
                    if limit is not None and len(rows) >= limit:
                        return PipelineTable(columns=columns, rows=rows)
        return PipelineTable(columns=columns, rows=rows)

    for left_row in left.rows:
        for right_row in right.rows:
            rows.append(_merge_rows(left_row, right_row, right_column_map))
            if limit is not None and len(rows) >= limit:
                return PipelineTable(columns=columns, rows=rows)
    return PipelineTable(columns=columns, rows=rows)


def _build_join_output_layout(
    left_columns: List[str],
    right_columns: List[str],
) -> List[Tuple[str, str]]:
    # Deterministically rename right-side collisions to preserve both payloads.
    right_column_map: List[Tuple[str, str]] = []
    collision_renames: List[Tuple[str, str]] = []
    for col in right_columns:
        if col in left_columns:
            mapped = f"right_{col}"
            collision_renames.append((col, mapped))
        else:
            mapped = col
        right_column_map.append((col, mapped))

    if collision_renames:
        import logging

        logger = logging.getLogger(__name__)
        rename_list = ", ".join(f"'{orig}' -> '{new}'" for orig, new in collision_renames)
        logger.warning(
            f"JOIN COLUMN COLLISION: Columns renamed to avoid duplicates: {rename_list}. "
            "Downstream transforms should use the renamed column names."
        )
    return right_column_map


def _stream_join_tables(
    left: PipelineTable,
    right: PipelineTable,
    *,
    metadata: Dict[str, Any],
    max_output_rows: Optional[int],
) -> PipelineTable:
    join_spec = resolve_join_spec(metadata)
    stream_spec = resolve_stream_join_spec(metadata)
    strategy = stream_spec.strategy
    if strategy not in {"dynamic", "left_lookup", "static"}:
        raise ValueError(f"Invalid streamJoin strategy: {strategy}")
    effective_join_type = resolve_stream_join_effective_join_type(
        strategy=strategy,
        requested_join_type=join_spec.join_type,
    )

    if strategy == "static":
        return _join_tables(
            left,
            right,
            join_type=effective_join_type,
            left_key=join_spec.left_key,
            right_key=join_spec.right_key,
            left_keys=join_spec.left_keys,
            right_keys=join_spec.right_keys,
            allow_cross_join=join_spec.allow_cross_join,
            max_output_rows=max_output_rows,
        )

    left_keys = normalize_join_key_list(join_spec.left_keys)
    right_keys = normalize_join_key_list(join_spec.right_keys)
    if join_spec.left_key and not left_keys:
        left_keys = [join_spec.left_key]
    if join_spec.right_key and not right_keys:
        right_keys = [join_spec.right_key]
    if not left_keys or not right_keys:
        raise ValueError("streamJoin requires leftKeys/rightKeys")
    if len(left_keys) != len(right_keys):
        raise ValueError("streamJoin requires leftKeys/rightKeys of the same length")

    right_join_table = right
    if strategy == "left_lookup":
        right_event_col = str(stream_spec.right_event_time_column or "").strip()
        right_join_table = _right_latest_snapshot_table(
            right,
            right_keys=right_keys,
            right_event_time_column=right_event_col or None,
        )
        return _join_tables(
            left,
            right_join_table,
            join_type=effective_join_type,
            left_key=join_spec.left_key,
            right_key=join_spec.right_key,
            left_keys=left_keys,
            right_keys=right_keys,
            allow_cross_join=join_spec.allow_cross_join,
            max_output_rows=max_output_rows,
        )

    left_event_col = str(stream_spec.left_event_time_column or "").strip()
    right_event_col = str(stream_spec.right_event_time_column or "").strip()
    time_direction = str(stream_spec.time_direction or "backward").strip().lower() or "backward"
    if not left_event_col or not right_event_col:
        raise ValueError("streamJoin dynamic requires leftEventTimeColumn and rightEventTimeColumn")
    if stream_spec.allowed_lateness_seconds is None:
        raise ValueError("streamJoin dynamic requires allowedLatenessSeconds")
    if stream_spec.allowed_lateness_seconds < 0:
        raise ValueError("streamJoin allowedLatenessSeconds must be >= 0")
    if stream_spec.left_cache_expiration_seconds is None:
        raise ValueError("streamJoin dynamic requires leftCacheExpirationSeconds")
    if stream_spec.left_cache_expiration_seconds <= 0:
        raise ValueError("streamJoin leftCacheExpirationSeconds must be > 0")
    if stream_spec.right_cache_expiration_seconds is None:
        raise ValueError("streamJoin dynamic requires rightCacheExpirationSeconds")
    if stream_spec.right_cache_expiration_seconds <= 0:
        raise ValueError("streamJoin rightCacheExpirationSeconds must be > 0")
    if time_direction not in {"backward", "forward", "symmetric"}:
        raise ValueError(f"Invalid streamJoin timeDirection: {time_direction}")

    left_tagged = PipelineTable(
        columns=[*left.columns, "__stream_left_row_id__"],
        rows=[{**row, "__stream_left_row_id__": idx} for idx, row in enumerate(left.rows)],
    )
    right_tagged = PipelineTable(
        columns=[*right.columns, "__stream_right_row_id__"],
        rows=[{**row, "__stream_right_row_id__": idx} for idx, row in enumerate(right.rows)],
    )
    joined = _join_tables(
        left_tagged,
        right_tagged,
        join_type=effective_join_type,
        left_key=join_spec.left_key,
        right_key=join_spec.right_key,
        left_keys=left_keys,
        right_keys=right_keys,
        allow_cross_join=join_spec.allow_cross_join,
        max_output_rows=max_output_rows,
    )
    if left_event_col not in joined.columns:
        raise ValueError(f"streamJoin dynamic leftEventTimeColumn missing from join output: {left_event_col}")
    resolved_right_event_col = _resolve_stream_join_right_column(
        columns=joined.columns,
        left_col=left_event_col,
        right_col=right_event_col,
    )
    if resolved_right_event_col not in joined.columns:
        raise ValueError(f"streamJoin dynamic rightEventTimeColumn missing from join output: {right_event_col}")

    output_columns = [col for col in joined.columns if col not in {"__stream_left_row_id__", "__stream_right_row_id__"}]
    right_column_map = _build_join_output_layout(left.columns, right.columns)
    lateness_seconds = float(stream_spec.allowed_lateness_seconds)
    cache_horizon_seconds = min(
        float(stream_spec.left_cache_expiration_seconds or 0.0),
        float(stream_spec.right_cache_expiration_seconds or 0.0),
    )
    best_by_left: Dict[int, Tuple[Tuple[float, float, float, int], Dict[str, Any], int]] = {}
    for row in joined.rows:
        left_row_id = row.get("__stream_left_row_id__")
        right_row_id = row.get("__stream_right_row_id__")
        if left_row_id is None or right_row_id is None:
            continue
        try:
            left_idx = int(left_row_id)
            right_idx = int(right_row_id)
        except (TypeError, ValueError):
            continue

        left_epoch = _to_epoch_seconds(row.get(left_event_col))
        right_epoch = _to_epoch_seconds(row.get(resolved_right_event_col))

        if left_epoch is None or right_epoch is None:
            # Keep backward compatibility for sparse timestamps: treat as fallback match
            # if both rows are present, but always lower priority than valid event-time matches.
            score = (1.0, float("inf"), float("inf"), right_idx)
        else:
            if time_direction == "backward":
                delta = left_epoch - right_epoch
                is_within_lateness = right_epoch <= left_epoch and delta <= lateness_seconds
                is_within_cache = delta <= cache_horizon_seconds
                tie = -right_epoch
            elif time_direction == "forward":
                delta = right_epoch - left_epoch
                is_within_lateness = left_epoch <= right_epoch and delta <= lateness_seconds
                is_within_cache = delta <= cache_horizon_seconds
                tie = right_epoch
            else:
                delta = abs(left_epoch - right_epoch)
                is_within_lateness = delta <= lateness_seconds
                is_within_cache = delta <= cache_horizon_seconds
                tie = abs(left_epoch - right_epoch)
            if not (is_within_lateness and is_within_cache):
                continue
            score = (0.0, float(delta), float(tie), right_idx)

        cleaned = {col: row.get(col) for col in output_columns}
        current = best_by_left.get(left_idx)
        if current is None or score < current[0]:
            best_by_left[left_idx] = (score, cleaned, right_idx)

    rows: List[Dict[str, Any]] = []
    matched_right_ids = {item[2] for item in best_by_left.values()}

    for left_idx in sorted(best_by_left.keys()):
        rows.append({col: best_by_left[left_idx][1].get(col) for col in output_columns})
        if max_output_rows is not None and len(rows) >= int(max_output_rows):
            return PipelineTable(columns=output_columns, rows=rows)

    if effective_join_type in {"left", "full"}:
        for left_idx, left_row in enumerate(left.rows):
            if left_idx in best_by_left:
                continue
            rows.append({col: _merge_rows(left_row, None, right_column_map).get(col) for col in output_columns})
            if max_output_rows is not None and len(rows) >= int(max_output_rows):
                return PipelineTable(columns=output_columns, rows=rows)

    if effective_join_type in {"right", "full"}:
        for right_idx, right_row in enumerate(right.rows):
            if right_idx in matched_right_ids:
                continue
            rows.append({col: _merge_rows(None, right_row, right_column_map).get(col) for col in output_columns})
            if max_output_rows is not None and len(rows) >= int(max_output_rows):
                return PipelineTable(columns=output_columns, rows=rows)

    return PipelineTable(columns=output_columns, rows=rows)


def _right_latest_snapshot_table(
    table: PipelineTable,
    *,
    right_keys: List[str],
    right_event_time_column: Optional[str],
) -> PipelineTable:
    if not right_keys:
        return table
    latest: Dict[Tuple[Any, ...], Tuple[float, int, Dict[str, Any]]] = {}
    event_col = str(right_event_time_column or "").strip()
    for index, row in enumerate(table.rows):
        key = tuple(row.get(col) for col in right_keys)
        event_rank = _to_epoch_seconds(row.get(event_col)) if event_col else None
        rank_value = float(event_rank if event_rank is not None else -1e30)
        current = latest.get(key)
        candidate = (rank_value, index, row)
        if current is None or candidate > current:
            latest[key] = candidate
    selected_rows = [payload[2] for payload in sorted(latest.values(), key=lambda item: item[1])]
    return PipelineTable(columns=table.columns, rows=selected_rows)


def _resolve_stream_join_right_column(*, columns: List[str], left_col: str, right_col: str) -> str:
    left_norm = str(left_col or "").strip()
    right_norm = str(right_col or "").strip()
    if right_norm == left_norm:
        preferred = [f"right_{right_norm}", right_norm]
    else:
        preferred = [right_norm, f"right_{right_norm}"]
    for candidate in preferred:
        if candidate in columns:
            return candidate
    prefix = f"right_{right_norm}"
    for column in columns:
        if column.startswith(prefix):
            return column
    return right_norm


def _to_epoch_seconds(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return float(dt.timestamp())
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    parsed = _parse_timestamp_literal(str(value))
    if isinstance(parsed, datetime):
        dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return float(dt.timestamp())
    if isinstance(parsed, (int, float)):
        try:
            return float(parsed)
        except (TypeError, ValueError):
            return None
    return None


def _merge_rows(
    left: Optional[Dict[str, Any]],
    right: Optional[Dict[str, Any]],
    right_column_map: List[Tuple[str, str]],
) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    if left:
        output.update(left)
    if right:
        for col, mapped in right_column_map:
            output[mapped] = right.get(col)
    else:
        for _, mapped in right_column_map:
            output[mapped] = None
    return output


def _find_similar_columns(target: str, available: List[str], cutoff: float = 0.6) -> List[str]:
    """
    Enterprise Enhancement (2026-01):
    Find column names similar to the target for helpful error messages.

    Uses difflib sequence matching to suggest potential typo corrections.
    """
    if not target or not available:
        return []

    target_lower = target.lower()
    matches = []

    for col in available:
        # Check for exact case-insensitive match
        if col.lower() == target_lower:
            return [col]

        # Calculate similarity ratio
        ratio = difflib.SequenceMatcher(None, target_lower, col.lower()).ratio()
        if ratio >= cutoff:
            matches.append((col, ratio))

    # Sort by similarity and return column names
    matches.sort(key=lambda x: x[1], reverse=True)
    return [m[0] for m in matches]


def _filter_table(table: PipelineTable, expression: str, parameters: Dict[str, Any]) -> PipelineTable:
    """
    Filter table rows based on expression.

    Enterprise Enhancement (2026-01):
    Added column reference validation to detect typos that would silently
    return empty results instead of raising clear errors.
    """
    expression = apply_parameters((expression or "").strip(), parameters)
    if not expression:
        return table
    parsed = _parse_filter(expression, parameters)
    if not parsed:
        return table
    column, op, value = parsed

    # Enterprise Enhancement: Validate column reference exists
    if column not in table.columns:
        # Find similar column names for helpful error message
        similar_columns = _find_similar_columns(column, table.columns)
        similar_hint = ""
        if similar_columns:
            similar_hint = f" Did you mean: {', '.join(similar_columns[:3])}?"

        error_msg = (
            f"FILTER COLUMN NOT FOUND: Column '{column}' does not exist in table. "
            f"Available columns: {table.columns}.{similar_hint}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    filtered = []
    for row in table.rows:
        cell = row.get(column)
        if _compare(cell, op, value):
            filtered.append(row)

    # Enterprise Enhancement: Warn if filter removes all rows (potential logic error)
    if table.rows and not filtered:
        logger.warning(
            f"FILTER EMPTY RESULT: Expression '{expression}' filtered out all {len(table.rows)} rows. "
            f"Sample values for '{column}': {[row.get(column) for row in table.rows[:5]]}. "
            "This may indicate an incorrect filter condition."
        )

    return PipelineTable(columns=table.columns, rows=filtered)


def _parse_filter(expression: str, parameters: Dict[str, Any]) -> Optional[Tuple[str, str, Any]]:
    operators = [">=", "<=", "!=", "==", ">", "<", "="]
    for op in operators:
        if op in expression:
            left, right = expression.split(op, 1)
            right_literal = right.strip()
            if right_literal.startswith("$"):
                param_name = right_literal[1:]
                if param_name in parameters:
                    return left.strip(), op, parameters[param_name]
            if right_literal in parameters:
                return left.strip(), op, parameters[right_literal]
            return left.strip(), op, _parse_literal(right_literal)
    return None


def _compare(left: Any, op: str, right: Any) -> bool:
    if op in {"=", "=="}:
        return left == right
    if op == "!=":
        return left != right
    try:
        left_num = float(left) if left is not None else None
        right_num = float(right) if right is not None else None
    except (TypeError, ValueError):
        left_num = None
        right_num = None
    if left_num is None or right_num is None:
        return False
    if op == ">":
        return left_num > right_num
    if op == "<":
        return left_num < right_num
    if op == ">=":
        return left_num >= right_num
    if op == "<=":
        return left_num <= right_num
    return False


def _compute_assignment_table(
    table: PipelineTable,
    target: str,
    expression: str,
    parameters: Dict[str, Any],
) -> PipelineTable:
    target = (target or "").strip()
    if not target:
        return table
    expression = apply_parameters((expression or "").strip(), parameters)
    if not expression:
        return table
    rows: list[dict[str, Any]] = []
    for row in table.rows:
        computed = _safe_eval(expression, row, parameters)
        next_row = dict(row)
        next_row[target] = computed
        rows.append(next_row)
    columns = table.columns + ([target] if target not in table.columns else [])
    return PipelineTable(columns=columns, rows=rows)


def _compute_table(table: PipelineTable, expression: str, parameters: Dict[str, Any]) -> PipelineTable:
    expression = apply_parameters((expression or "").strip(), parameters)
    if not expression:
        return table
    target = ""
    expr = expression
    if "=" in expression:
        left, right = expression.split("=", 1)
        candidate = left.strip()
        # Disambiguate: if the LHS is an existing column, treat this as a comparison expression,
        # not an assignment. (Spark SQL uses '=' for comparisons; assignments are a DSL convenience.)
        if candidate and candidate not in table.columns:
            target = candidate
            expr = right.strip()
        else:
            target = "computed"
            expr = expression.strip()
    else:
        target = "computed"
        expr = expression.strip()
    rows = []
    for row in table.rows:
        computed = _safe_eval(expr, row, parameters)
        next_row = dict(row)
        next_row[target] = computed
        rows.append(next_row)
    columns = table.columns + ([target] if target not in table.columns else [])
    return PipelineTable(columns=columns, rows=rows)


def _explode_table(table: PipelineTable, column: str) -> PipelineTable:
    column = (column or "").strip()
    if not column:
        raise ValueError("explode requires a target column")
    if column not in table.columns:
        raise ValueError(f"explode missing column: {column}")

    rows: list[dict[str, Any]] = []
    for row in table.rows:
        value = row.get(column)
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                next_row = dict(row)
                next_row[column] = item
                rows.append(next_row)
            continue
        raise ValueError(f"explode expects array/list values for column '{column}'")

    return PipelineTable(columns=table.columns, rows=rows)


def _parse_assignment(expression: str) -> Tuple[str, str]:
    if "=" in expression:
        left, right = expression.split("=", 1)
        return left.strip(), right.strip()
    return "computed", expression


def _safe_eval(expression: str, row: Dict[str, Any], parameters: Dict[str, Any]) -> Any:
    expression = expression.strip()
    if not expression:
        return None
    # Make a best-effort attempt to interpret Spark-style '=' comparisons in preview mode.
    expression = re.sub(r"(?<![<>=!])=(?![=])", "==", expression)
    lower_expr = expression.lower()
    if lower_expr.startswith("to_timestamp(") and expression.endswith(")"):
        inner = expression[len("to_timestamp(") : -1].strip()
        return _parse_timestamp_literal(inner)
    if lower_expr.startswith("timestamp(") and expression.endswith(")"):
        inner = expression[len("timestamp(") : -1].strip()
        return _parse_timestamp_literal(inner)
    if expression.startswith("$"):
        return parameters.get(expression[1:])
    variables = {**parameters, **row}
    if expression in variables:
        return variables.get(expression)
    literal = _parse_literal(expression)
    if literal != expression:
        return literal
    try:
        tree = ast.parse(expression, mode="eval")
    except (SyntaxError, ValueError, TypeError):
        # Spark SQL expressions (e.g. CAST/TRY_CAST/date_trunc) are not Python.
        # In preview mode, treat unsupported expressions as NULL rather than leaking the raw expression
        # into downstream ops (e.g. groupBy(sum) would crash on float("cast(...)")).
        return None
    if not _is_safe_ast(tree):
        return None
    try:
        return _eval_ast(tree.body, variables)
    except (TypeError, ValueError, ZeroDivisionError, ArithmeticError, KeyError, AttributeError):
        return None


def _is_safe_ast(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Name, ast.Constant, ast.Load)):
            continue
        if isinstance(child, (ast.Compare, ast.BoolOp)):
            continue
        if isinstance(child, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.Pow, ast.USub, ast.UAdd)):
            continue
        if isinstance(child, (ast.And, ast.Or)):
            continue
        if isinstance(child, (ast.Eq, ast.NotEq, ast.Gt, ast.GtE, ast.Lt, ast.LtE)):
            continue
        return False
    return True


def _eval_ast(node: ast.AST, variables: Dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return variables.get(node.id)
    if isinstance(node, ast.UnaryOp):
        operand = _eval_ast(node.operand, variables)
        if isinstance(node.op, ast.USub):
            return -float(operand)
        if isinstance(node.op, ast.UAdd):
            return float(operand)
    if isinstance(node, ast.BinOp):
        left = _eval_ast(node.left, variables)
        right = _eval_ast(node.right, variables)

        # Enterprise Enhancement (2026-01): Explicit division by zero handling
        if isinstance(node.op, (ast.Div, ast.Mod)):
            if right == 0 or right is None:
                logger.warning(
                    f"DIVISION BY ZERO: Attempted {'division' if isinstance(node.op, ast.Div) else 'modulo'} "
                    f"with divisor=0 or NULL (left={left}, right={right}). Returning NULL. "
                    "Check your expression or add a guard condition (e.g., CASE WHEN divisor != 0)."
                )
                return None

        try:
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.Mod):
                return left % right
            if isinstance(node.op, ast.Pow):
                return left ** right
        except TypeError as e:
            logger.warning(
                f"TYPE ERROR IN EXPRESSION: Cannot perform operation on incompatible types "
                f"(left={left!r}, right={right!r}). Error: {e}. Returning NULL."
            )
            return None
        except Exception as e:
            logger.warning(
                f"EXPRESSION EVALUATION ERROR: Unexpected error during computation "
                f"(left={left!r}, right={right!r}). Error: {e}. Returning NULL."
            )
            return None
    if isinstance(node, ast.Compare):
        left = _eval_ast(node.left, variables)
        if len(node.ops) != 1 or len(node.comparators) != 1:
            return None
        right = _eval_ast(node.comparators[0], variables)
        op = node.ops[0]
        if isinstance(op, ast.Eq):
            return left == right
        if isinstance(op, ast.NotEq):
            return left != right
        try:
            if isinstance(op, ast.Gt):
                return left > right
            if isinstance(op, ast.GtE):
                return left >= right
            if isinstance(op, ast.Lt):
                return left < right
            if isinstance(op, ast.LtE):
                return left <= right
        except (TypeError, ValueError):
            return None
        return None

    if isinstance(node, ast.BoolOp):
        values = [_eval_ast(v, variables) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(bool(v) for v in values)
        if isinstance(node.op, ast.Or):
            return any(bool(v) for v in values)
        return None
    return None


def _parse_literal(raw: str) -> Any:
    if not raw:
        return raw
    lowered = raw.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if (raw.startswith("\"") and raw.endswith("\"")) or (raw.startswith("'") and raw.endswith("'")):
        return raw[1:-1]
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        return raw


def _parse_timestamp_literal(raw: str) -> Any:
    literal = _parse_literal(raw)
    if isinstance(literal, datetime):
        return literal
    if isinstance(literal, (int, float)):
        try:
            return datetime.fromtimestamp(literal, tz=timezone.utc)
        except (TypeError, ValueError, OSError, OverflowError):
            return literal
    if isinstance(literal, str):
        candidate = literal.strip()
        if candidate:
            try:
                return datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            except ValueError:
                return literal
    return literal


def _normalize_table(
    table: PipelineTable,
    columns: List[str],
    *,
    trim: bool,
    empty_to_null: bool,
    whitespace_to_null: bool,
    lowercase: bool,
    uppercase: bool,
) -> PipelineTable:
    if not columns:
        return table
    selected = [str(col).strip() for col in columns if str(col).strip()]
    selected = [col for col in selected if col in table.columns]
    if not selected:
        return table
    rows: List[Dict[str, Any]] = []
    for row in table.rows:
        next_row = dict(row)
        for col in selected:
            value = next_row.get(col)
            if isinstance(value, str):
                text = value
                if trim:
                    text = text.strip()
                if lowercase:
                    text = text.lower()
                if uppercase:
                    text = text.upper()
                if whitespace_to_null and text.strip() == "":
                    text = None
                if empty_to_null and text == "":
                    text = None
                next_row[col] = text
        rows.append(next_row)
    return PipelineTable(columns=table.columns, rows=rows)


def _geospatial_table(table: PipelineTable, metadata: Dict[str, Any], parameters: Dict[str, Any]) -> PipelineTable:
    geo = metadata.get("geospatial") if isinstance(metadata.get("geospatial"), dict) else metadata
    mode = str(geo.get("mode") or "").strip().lower()
    output_col = str(geo.get("outputColumn") or geo.get("output_column") or "").strip()
    if not output_col:
        output_col = "distance_km" if mode == "distance" else "geohash" if mode == "geohash" else "point"

    def _as_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    rows: List[Dict[str, Any]] = []
    for row in table.rows:
        next_row = dict(row)
        if mode in {"point", "geohash"}:
            lat_col = str(geo.get("latColumn") or geo.get("lat_column") or "").strip()
            lon_col = str(geo.get("lonColumn") or geo.get("lon_column") or "").strip()
            lat = _as_float(next_row.get(lat_col))
            lon = _as_float(next_row.get(lon_col))
            if lat is None or lon is None:
                next_row[output_col] = None
            elif mode == "point":
                next_row[output_col] = f"POINT({lon} {lat})"
            else:
                precision = int(geo.get("precision") or 7)
                if precision < 1 or precision > 12:
                    raise ValueError("geospatial geohash precision must be between 1 and 12")
                next_row[output_col] = _encode_geohash_text(lat, lon, precision)
        elif mode == "distance":
            lat1_col = str(geo.get("lat1Column") or geo.get("lat1_column") or "").strip()
            lon1_col = str(geo.get("lon1Column") or geo.get("lon1_column") or "").strip()
            lat2_col = str(geo.get("lat2Column") or geo.get("lat2_column") or "").strip()
            lon2_col = str(geo.get("lon2Column") or geo.get("lon2_column") or "").strip()
            lat1 = _as_float(next_row.get(lat1_col))
            lon1 = _as_float(next_row.get(lon1_col))
            lat2 = _as_float(next_row.get(lat2_col))
            lon2 = _as_float(next_row.get(lon2_col))
            if None in {lat1, lon1, lat2, lon2}:
                next_row[output_col] = None
            else:
                phi1 = math.radians(float(lat1))
                phi2 = math.radians(float(lat2))
                dphi = math.radians(float(lat2) - float(lat1))
                dlambda = math.radians(float(lon2) - float(lon1))
                a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                next_row[output_col] = 6371.0 * c
        rows.append(next_row)
    columns = list(table.columns)
    if output_col not in columns:
        columns.append(output_col)
    return PipelineTable(columns=columns, rows=rows)


def _pattern_mining_table(table: PipelineTable, metadata: Dict[str, Any], parameters: Dict[str, Any]) -> PipelineTable:
    pattern_meta = metadata.get("patternMining") if isinstance(metadata.get("patternMining"), dict) else metadata
    source_col = str(pattern_meta.get("sourceColumn") or pattern_meta.get("source_column") or "").strip()
    output_col = str(pattern_meta.get("outputColumn") or pattern_meta.get("output_column") or "").strip() or "pattern_match"
    pattern = apply_parameters(str(pattern_meta.get("pattern") or "").strip(), parameters)
    mode = str(pattern_meta.get("matchMode") or pattern_meta.get("match_mode") or "contains").strip().lower()
    if mode not in {"contains", "extract", "count"}:
        raise ValueError(f"Invalid patternMining matchMode: {mode}")
    if not source_col or not pattern:
        return table
    regex = re.compile(pattern)
    rows: List[Dict[str, Any]] = []
    for row in table.rows:
        next_row = dict(row)
        source_value = str(next_row.get(source_col) or "")
        match = regex.search(source_value)
        if mode == "extract":
            if not match:
                next_row[output_col] = None
            elif match.groups():
                next_row[output_col] = match.group(1)
            else:
                next_row[output_col] = match.group(0)
        elif mode == "count":
            next_row[output_col] = len(regex.findall(source_value)) if source_value else 0
        else:
            next_row[output_col] = bool(match)
        rows.append(next_row)
    columns = list(table.columns)
    if output_col not in columns:
        columns.append(output_col)
    return PipelineTable(columns=columns, rows=rows)


def _encode_geohash_text(lat: float, lon: float, precision: int) -> Optional[str]:
    if not (-90.0 <= float(lat) <= 90.0 and -180.0 <= float(lon) <= 180.0):
        return None
    base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    bits = [16, 8, 4, 2, 1]
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    geohash_chars: List[str] = []
    bit_index = 0
    ch = 0
    is_even = True
    while len(geohash_chars) < precision:
        if is_even:
            mid = (lon_range[0] + lon_range[1]) / 2.0
            if float(lon) >= mid:
                ch |= bits[bit_index]
                lon_range[0] = mid
            else:
                lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2.0
            if float(lat) >= mid:
                ch |= bits[bit_index]
                lat_range[0] = mid
            else:
                lat_range[1] = mid
        is_even = not is_even
        if bit_index < 4:
            bit_index += 1
            continue
        geohash_chars.append(base32[ch])
        bit_index = 0
        ch = 0
    return "".join(geohash_chars)


def _regex_flags(raw: Any) -> int:
    if raw is None:
        return 0
    text = str(raw or "")
    flags = 0
    if "i" in text:
        flags |= re.IGNORECASE
    if "m" in text:
        flags |= re.MULTILINE
    if "s" in text:
        flags |= re.DOTALL
    return flags


def _normalize_regex_rules(metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    rules: List[Dict[str, Any]] = []
    raw_rules = metadata.get("rules")
    if isinstance(raw_rules, list) and raw_rules:
        for rule in raw_rules:
            if not isinstance(rule, dict):
                continue
            column = str(rule.get("column") or "").strip()
            pattern = str(rule.get("pattern") or "").strip()
            if not column or not pattern:
                continue
            rules.append(
                {
                    "column": column,
                    "pattern": pattern,
                    "replacement": str(rule.get("replacement") or ""),
                    "flags": rule.get("flags"),
                }
            )
        return rules
    pattern = str(metadata.get("pattern") or "").strip()
    if not pattern:
        return rules
    columns = metadata.get("columns") or []
    for col in columns if isinstance(columns, list) else [columns]:
        col_name = str(col or "").strip()
        if not col_name:
            continue
        rules.append(
            {
                "column": col_name,
                "pattern": pattern,
                "replacement": str(metadata.get("replacement") or ""),
                "flags": metadata.get("flags"),
            }
        )
    return rules


def _regex_replace_table(table: PipelineTable, rules: List[Dict[str, Any]]) -> PipelineTable:
    if not rules:
        return table
    compiled: List[Tuple[str, re.Pattern[str], str]] = []
    for rule in rules:
        column = str(rule.get("column") or "").strip()
        pattern = str(rule.get("pattern") or "").strip()
        if not column or not pattern:
            continue
        flags = _regex_flags(rule.get("flags"))
        compiled.append((column, re.compile(pattern, flags=flags), str(rule.get("replacement") or "")))
    if not compiled:
        return table
    rows: List[Dict[str, Any]] = []
    for row in table.rows:
        next_row = dict(row)
        for column, regex, replacement in compiled:
            if column not in next_row:
                continue
            value = next_row.get(column)
            if value is None:
                continue
            text = str(value)
            next_row[column] = regex.sub(replacement, text)
        rows.append(next_row)
    return PipelineTable(columns=table.columns, rows=rows)


def _parse_csv_bytes(raw_bytes: bytes, *, max_rows: int = 200) -> List[Dict[str, Any]]:
    """
    Parse a CSV payload into row dicts.

    NOTE: Must handle quoted headers/fields (common in lakeFS-ingested CSVs),
    otherwise join keys become missing and joins degrade into cross-joins.
    """
    try:
        text = raw_bytes.decode("utf-8", errors="replace")
    except (UnicodeDecodeError, AttributeError):
        return []
    if not text.strip():
        return []

    # Detect delimiter from the first non-empty line.
    first_line = next((line for line in text.splitlines() if line.strip()), "")
    if not first_line:
        return []
    delimiter = ","
    if "\t" in first_line and first_line.count("\t") >= first_line.count(","):
        delimiter = "\t"
    elif ";" in first_line and first_line.count(";") >= first_line.count(","):
        delimiter = ";"

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    try:
        raw_header = next(reader)
    except StopIteration:
        return []

    header = []
    for idx, cell in enumerate(raw_header):
        name = (str(cell or "").strip() or f"column_{idx + 1}").lstrip("\ufeff")
        header.append(name)

    rows: List[Dict[str, Any]] = []
    resolved_limit = max(1, int(max_rows))
    for record in reader:
        if len(rows) >= resolved_limit:
            break
        if not record:
            continue
        if not any(str(cell or "").strip() for cell in record):
            continue
        row: Dict[str, Any] = {}
        for idx, key in enumerate(header):
            row[key] = str(record[idx]).strip() if idx < len(record) else ""
        rows.append(row)
    return rows


def _parse_excel_bytes(raw_bytes: bytes, *, max_rows: int = 200) -> List[Dict[str, Any]]:
    try:
        import pandas as pd
        from io import BytesIO

        frame = pd.read_excel(BytesIO(raw_bytes))
        resolved_limit = max(1, int(max_rows))
        return frame.fillna("").to_dict(orient="records")[:resolved_limit]
    except (ImportError, ValueError, TypeError, OSError):
        return []


def _parse_json_bytes(raw_bytes: bytes, *, max_rows: int = 200) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        text = raw_bytes.decode("utf-8", errors="replace")
        rows: List[Dict[str, Any]] = []
        resolved_limit = max(1, int(max_rows))
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
            if len(rows) >= resolved_limit:
                break
        return rows
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("data")
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            return rows[: max(1, int(max_rows))]
        if isinstance(rows, list) and rows and isinstance(rows[0], list):
            columns = _extract_schema_columns(payload)
            output = []
            resolved_limit = max(1, int(max_rows))
            for row in rows[:resolved_limit]:
                output.append({columns[idx] if idx < len(columns) else f"col_{idx}": value for idx, value in enumerate(row)})
            return output
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[: max(1, int(max_rows))]
    return []


def _infer_column_types(table: PipelineTable) -> Dict[str, str]:
    inferred: Dict[str, str] = {}
    for col in table.columns:
        values = [row.get(col) for row in table.rows if row.get(col) is not None]
        sample = values[:50]
        inferred[col] = infer_xsd_type_from_values(sample)
    return inferred


def _build_table_ops(table: PipelineTable) -> TableOps:
    columns = set(table.columns)
    inferred = _infer_column_types(table)
    total_count = len(table.rows)

    def total() -> int:
        return total_count

    def has_null(column: str) -> bool:
        if column not in columns:
            return total_count > 0
        return any(row.get(column) in (None, "") for row in table.rows)

    def has_empty(column: str) -> bool:
        if column not in columns:
            return total_count > 0
        return any(str(row.get(column) or "").strip() == "" for row in table.rows)

    def unique_count(cols: List[str]) -> int:
        if not cols:
            return 0
        if any(col not in columns for col in cols):
            return 1 if total_count > 0 else 0
        if len(cols) == 1:
            values = [row.get(cols[0]) for row in table.rows]
        else:
            values = [tuple(row.get(col) for col in cols) for row in table.rows]
        return len(set(values))

    def min_max(column: str) -> Tuple[Optional[float], Optional[float]]:
        if column not in columns:
            return (None, None)
        values = [normalize_number(row.get(column)) for row in table.rows]
        values = [value for value in values if value is not None]
        if not values:
            return (None, None)
        return (min(values), max(values))

    def regex_mismatch(column: str, pattern: str) -> bool:
        if column not in columns:
            if total_count == 0:
                return False
            import re
            return re.search(pattern, "") is None
        import re
        regex = re.compile(pattern)
        return any(not regex.search(str(row.get(column) or "")) for row in table.rows)

    def in_set_mismatch(column: str, allowed: List[Any]) -> bool:
        if column not in columns:
            return total_count > 0 and (None not in allowed)
        return any(row.get(column) not in allowed for row in table.rows)

    type_map = {name: inferred.get(name, "") for name in table.columns}
    return TableOps(
        columns=columns,
        type_map=type_map,
        total_count=total,
        has_null=has_null,
        has_empty=has_empty,
        unique_count=unique_count,
        min_max=min_max,
        regex_mismatch=regex_mismatch,
        in_set_mismatch=in_set_mismatch,
    )
