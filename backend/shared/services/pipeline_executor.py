"""
Pipeline Executor - Minimal transform engine for Pipeline Builder.

Executes a pipeline definition against dataset samples to produce preview/output data.
"""

from __future__ import annotations

import ast
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_registry import PipelineRegistry
from shared.services.pipeline_profiler import compute_column_stats
from shared.services.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes, topological_sort
from shared.services.pipeline_parameter_utils import apply_parameters, normalize_parameters
from shared.services.storage_service import StorageService
from shared.services.pipeline_udf_runtime import compile_row_udf
from shared.services.pipeline_definition_utils import (
    build_expectations_with_pk,
    resolve_delete_column,
    resolve_execution_semantics,
    resolve_pk_columns,
    resolve_pk_semantics,
    validate_pk_semantics,
)
from shared.services.pipeline_schema_utils import normalize_number
from shared.services.pipeline_dataset_utils import normalize_dataset_selection, resolve_dataset_version
from shared.services.pipeline_type_utils import infer_xsd_type_from_values, normalize_cast_target
from shared.services.pipeline_transform_spec import (
    normalize_operation,
    normalize_union_mode,
    resolve_join_spec,
)
from shared.services.pipeline_validation_utils import (
    TableOps,
    validate_expectations,
    validate_schema_checks,
    validate_schema_contract,
)
from shared.utils.s3_uri import parse_s3_uri


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
        root = base_path or os.getenv("PIPELINE_ARTIFACT_PATH") or "data/pipeline_artifacts"
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
        nodes = normalize_nodes(definition.get("nodes"))
        edges = normalize_edges(definition.get("edges"))
        order = topological_sort(nodes, edges, include_unordered=True)
        parameters = normalize_parameters(definition.get("parameters"))
        preview_meta = definition.get("__preview_meta__") or {}
        preview_branch = preview_meta.get("branch")

        tables: Dict[str, PipelineTable] = {}
        incoming_map = build_incoming(edges)

        schema_errors: Dict[str, List[str]] = {}
        for node_id in order:
            node = nodes[node_id]
            incoming_tables = [tables[src] for src in incoming_map.get(node_id, []) if src in tables]
            node_type = str(node.get("type") or "transform")
            metadata = node.get("metadata") or {}

            if node_type == "input":
                if input_overrides and node_id in input_overrides:
                    table = input_overrides[node_id]
                else:
                    table = await self._load_input(node, db_name, preview_branch)
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
        expectation_errors = pk_semantic_errors + validate_expectations(
            output_ops,
            build_expectations_with_pk(
                definition=definition,
                output_metadata=output_metadata,
                output_name=output_name,
                output_node_id=output_node_id,
                declared_outputs=declared_outputs,
                pk_semantics=pk_semantics,
                delete_column=delete_column,
                pk_columns=pk_columns,
                available_columns=available_columns,
            ),
        )
        if expectation_errors:
            raise PipelineExpectationError(f"Expectations failed: {expectation_errors}")
        return PipelineRunResult(tables=tables, output_nodes=output_nodes)

    async def _load_input(
        self,
        node: Dict[str, Any],
        db_name: str,
        branch: Optional[str] = None,
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
                rows = await self._load_rows_from_artifact(version.artifact_key) if version.artifact_key else []
                if not rows:
                    rows = _extract_sample_rows(version.sample_json)
                if not columns:
                    columns = _extract_schema_columns(version.sample_json)
        if not columns:
            columns = _fallback_columns(node)
        if not rows:
            rows = _build_sample_rows(columns, 8)
        return PipelineTable(columns=columns, rows=rows)

    async def _load_rows_from_artifact(self, artifact_key: Optional[str]) -> List[Dict[str, Any]]:
        if not artifact_key:
            return []
        parsed = parse_s3_uri(artifact_key)
        if not parsed or not self._storage_service:
            return []
        bucket, key = parsed
        try:
            raw_bytes = await self._storage_service.load_bytes(bucket, key)
        except Exception:
            return []
        extension = os.path.splitext(key)[1].lower()
        if extension == ".csv":
            return _parse_csv_bytes(raw_bytes)
        if extension in {".xlsx", ".xlsm"}:
            return _parse_excel_bytes(raw_bytes)
        if extension == ".json":
            return _parse_json_bytes(raw_bytes)
        prefix = key.rstrip("/")
        if not prefix:
            return []
        prefix = f"{prefix}/"
        try:
            objects = await self._storage_service.list_objects(bucket, prefix=prefix)
        except Exception:
            return []
        keys = [obj.get("Key") for obj in objects or [] if obj.get("Key")]
        for candidate in keys:
            ext = os.path.splitext(candidate)[1].lower()
            if ext not in {".json", ".csv", ".xlsx", ".xlsm"}:
                continue
            try:
                candidate_bytes = await self._storage_service.load_bytes(bucket, candidate)
            except Exception:
                continue
            if ext == ".csv":
                return _parse_csv_bytes(candidate_bytes)
            if ext in {".xlsx", ".xlsm"}:
                return _parse_excel_bytes(candidate_bytes)
            if ext == ".json":
                return _parse_json_bytes(candidate_bytes)
        return []

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
                allow_cross_join=join_spec.allow_cross_join,
            )
        if operation == "filter":
            return _filter_table(inputs[0], str(metadata.get("expression") or ""), parameters)
        if operation == "compute":
            return _compute_table(inputs[0], str(metadata.get("expression") or ""), parameters)
        if operation == "explode":
            columns = metadata.get("columns") or []
            if columns:
                return _explode_table(inputs[0], str(columns[0]))
        if operation == "select":
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
        if operation == "cast":
            casts = metadata.get("casts") or []
            if casts:
                return _cast_columns(inputs[0], casts)
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
        udf_code = (metadata.get("udfCode") or metadata.get("udf_code") or "").strip() or None
        udf_id = (metadata.get("udfId") or metadata.get("udf_id") or "").strip() or None
        udf_version = metadata.get("udfVersion") or metadata.get("udf_version")

        if not udf_code and udf_id:
            if not self._pipeline_registry:
                raise ValueError("udf requires pipeline_registry to resolve udfId")
            resolved_version: Optional[int] = None
            if udf_version is not None and str(udf_version).strip():
                try:
                    resolved_version = int(udf_version)
                except Exception as exc:
                    raise ValueError(f"Invalid udfVersion: {udf_version}") from exc
            if resolved_version is None:
                latest = await self._pipeline_registry.get_udf_latest_version(udf_id=udf_id)
                if not latest:
                    raise ValueError("udf not found")
                udf_code = latest.code
            else:
                version_row = await self._pipeline_registry.get_udf_version(udf_id=udf_id, version=resolved_version)
                if not version_row:
                    raise ValueError("udf version not found")
                udf_code = version_row.code

        if not udf_code:
            raise ValueError("udf missing code (udfCode or udfId)")

        fn = compile_row_udf(udf_code)
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
        }

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
                    agg_state["sum"] += float(value)
            elif spec["op"] == "avg":
                if value is not None:
                    agg_state["sum"] += float(value)
                    agg_state["count"] += 1
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


def _window_table(table: PipelineTable, window_meta: Dict[str, Any]) -> PipelineTable:
    partition_by = window_meta.get("partitionBy") or []
    order_by = window_meta.get("orderBy") or []
    if not order_by:
        return table
    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for row in table.rows:
        key = tuple(row.get(col) for col in partition_by) if partition_by else ("__all__",)
        grouped.setdefault(key, []).append(row)
    rows: List[Dict[str, Any]] = []
    for key, bucket in grouped.items():
        bucket_sorted = sorted(bucket, key=lambda r: tuple(r.get(col) for col in order_by))
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


def _cast_columns(table: PipelineTable, casts: List[Dict[str, Any]]) -> PipelineTable:
    def cast_value(value: Any, target: str) -> Any:
        if value is None:
            return None
        normalized = normalize_cast_target(target)
        if normalized == "xsd:integer":
            try:
                return int(value)
            except Exception:
                return value
        if normalized == "xsd:decimal":
            try:
                return float(value)
            except Exception:
                return value
        if normalized == "xsd:boolean":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in {"true", "1", "yes", "y"}
        return value

    cast_map = {str(item.get("column")): str(item.get("type")) for item in casts if item.get("column") and item.get("type")}
    if not cast_map:
        return table
    rows: List[Dict[str, Any]] = []
    for row in table.rows:
        new_row = dict(row)
        for column, target in cast_map.items():
            if column in new_row:
                new_row[column] = cast_value(new_row[column], target)
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


def _sort_table(table: PipelineTable, columns: List[str]) -> PipelineTable:
    cols = [col for col in columns if col in table.columns]
    if not cols:
        return table
    rows = sorted(table.rows, key=lambda row: tuple(row.get(col) for col in cols))
    return PipelineTable(columns=table.columns, rows=rows)


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
    allow_cross_join: bool = False,
) -> PipelineTable:
    join_type = (join_type or "inner").lower()
    candidate_key = join_key or left_key
    left_join = candidate_key
    right_join = right_key or candidate_key
    if not left_join or not right_join:
        if allow_cross_join:
            if join_type != "cross":
                raise ValueError("join allowCrossJoin requires joinType='cross'")
        else:
            raise ValueError("join requires leftKey/rightKey (or joinKey)")
    right_column_map: List[Tuple[str, str]] = []
    for col in right.columns:
        mapped = f"right_{col}" if col in left.columns else col
        right_column_map.append((col, mapped))
    columns = left.columns + [mapped for _, mapped in right_column_map]

    rows: List[Dict[str, Any]] = []
    if left_join and right_join:
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
            elif join_type in {"left", "full"}:
                rows.append(_merge_rows(row, None, right_column_map))
        if join_type in {"right", "full"}:
            for idx, row in enumerate(right.rows):
                if idx not in matched_right:
                    rows.append(_merge_rows(None, row, right_column_map))
        return PipelineTable(columns=columns, rows=rows)

    for left_row in left.rows:
        for right_row in right.rows:
            rows.append(_merge_rows(left_row, right_row, right_column_map))
    return PipelineTable(columns=columns, rows=rows)


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


def _filter_table(table: PipelineTable, expression: str, parameters: Dict[str, Any]) -> PipelineTable:
    expression = apply_parameters((expression or "").strip(), parameters)
    if not expression:
        return table
    parsed = _parse_filter(expression, parameters)
    if not parsed:
        return table
    column, op, value = parsed
    filtered = []
    for row in table.rows:
        cell = row.get(column)
        if _compare(cell, op, value):
            filtered.append(row)
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
    except Exception:
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


def _compute_table(table: PipelineTable, expression: str, parameters: Dict[str, Any]) -> PipelineTable:
    expression = apply_parameters((expression or "").strip(), parameters)
    if not expression:
        return table
    target, expr = _parse_assignment(expression)
    if not target:
        return table
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
    except Exception:
        return expression
    if not _is_safe_ast(tree):
        return expression
    return _eval_ast(tree.body, variables)


def _is_safe_ast(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Name, ast.Constant, ast.Load)):
            continue
        if isinstance(child, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.Pow, ast.USub, ast.UAdd)):
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
        except Exception:
            return None
    return None


def _parse_literal(raw: str) -> Any:
    if not raw:
        return raw
    if (raw.startswith("\"") and raw.endswith("\"")) or (raw.startswith("'") and raw.endswith("'")):
        return raw[1:-1]
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except Exception:
        return raw


def _parse_csv_bytes(raw_bytes: bytes) -> List[Dict[str, Any]]:
    try:
        text = raw_bytes.decode("utf-8", errors="replace")
    except Exception:
        return []
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return []
    delimiter = ","
    sample = lines[0]
    if "\t" in sample:
        delimiter = "\t"
    elif ";" in sample:
        delimiter = ";"
    header = [cell.strip() or f"column_{idx + 1}" for idx, cell in enumerate(sample.split(delimiter))]
    rows: List[Dict[str, Any]] = []
    for line in lines[1:201]:
        cells = line.split(delimiter)
        row: Dict[str, Any] = {}
        for idx, key in enumerate(header):
            row[key] = cells[idx].strip() if idx < len(cells) else ""
        rows.append(row)
    return rows


def _parse_excel_bytes(raw_bytes: bytes) -> List[Dict[str, Any]]:
    try:
        import pandas as pd
        from io import BytesIO

        frame = pd.read_excel(BytesIO(raw_bytes))
        return frame.fillna("").to_dict(orient="records")[:200]
    except Exception:
        return []


def _parse_json_bytes(raw_bytes: bytes) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception:
        text = raw_bytes.decode("utf-8", errors="replace")
        rows: List[Dict[str, Any]] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                rows.append(item)
            if len(rows) >= 200:
                break
        return rows
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("data")
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            return rows[:200]
        if isinstance(rows, list) and rows and isinstance(rows[0], list):
            columns = _extract_schema_columns(payload)
            output = []
            for row in rows[:200]:
                output.append({columns[idx] if idx < len(columns) else f"col_{idx}": value for idx, value in enumerate(row)})
            return output
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[:200]
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
