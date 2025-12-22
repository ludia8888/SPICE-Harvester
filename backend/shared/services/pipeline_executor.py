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
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shared.services.dataset_registry import DatasetRegistry


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
    def __init__(self, dataset_registry: DatasetRegistry, artifact_store: Optional[PipelineArtifactStore] = None) -> None:
        self._dataset_registry = dataset_registry
        self._artifact_store = artifact_store

    async def preview(
        self,
        *,
        definition: Dict[str, Any],
        db_name: str,
        node_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        result = await self.run(definition=definition, db_name=db_name)
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
    ) -> Tuple[PipelineTable, Dict[str, Any]]:
        result = await self.run(definition=definition, db_name=db_name)
        table = self._select_table(result, node_id)
        meta: Dict[str, Any] = {"row_count": len(table.rows)}
        if store_local and self._artifact_store:
            meta["artifact_key"] = self._artifact_store.save_table(
                table,
                dataset_name=(dataset_name or "pipeline_output"),
            )
        return table, meta

    async def run(self, *, definition: Dict[str, Any], db_name: str) -> PipelineRunResult:
        nodes = _normalize_nodes(definition.get("nodes"))
        edges = _normalize_edges(definition.get("edges"))
        order = _topological_sort(nodes, edges)
        parameters = _normalize_parameters(definition.get("parameters"))

        tables: Dict[str, PipelineTable] = {}
        incoming_map = _build_incoming(edges)

        for node_id in order:
            node = nodes[node_id]
            incoming_tables = [tables[src] for src in incoming_map.get(node_id, []) if src in tables]
            node_type = str(node.get("type") or "transform")
            metadata = node.get("metadata") or {}

            if node_type == "input":
                table = await self._load_input(node, db_name)
            elif node_type == "output":
                table = incoming_tables[0] if incoming_tables else PipelineTable([], [])
            else:
                table = self._apply_transform(metadata, incoming_tables, parameters)

            tables[node_id] = table

        output_nodes = [node_id for node_id, node in nodes.items() if node.get("type") == "output"]
        return PipelineRunResult(tables=tables, output_nodes=output_nodes)

    async def _load_input(self, node: Dict[str, Any], db_name: str) -> PipelineTable:
        metadata = node.get("metadata") or {}
        dataset_id = metadata.get("datasetId")
        dataset = None
        if dataset_id:
            dataset = await self._dataset_registry.get_dataset(dataset_id=str(dataset_id))
        if not dataset and metadata.get("datasetName"):
            dataset = await self._dataset_registry.get_dataset_by_name(
                db_name=db_name, name=str(metadata.get("datasetName"))
            )
        columns = _extract_schema_columns(dataset.schema_json if dataset else {})
        rows: List[Dict[str, Any]] = []
        if dataset:
            version = await self._dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
            if version:
                rows = _extract_sample_rows(version.sample_json)
                if not columns:
                    columns = _extract_schema_columns(version.sample_json)
        if not columns:
            columns = _fallback_columns(node)
        if not rows:
            rows = _build_sample_rows(columns, 8)
        return PipelineTable(columns=columns, rows=rows)

    def _apply_transform(
        self,
        metadata: Dict[str, Any],
        inputs: List[PipelineTable],
        parameters: Dict[str, Any],
    ) -> PipelineTable:
        if not inputs:
            return PipelineTable([], [])
        operation = str(metadata.get("operation") or "")
        if operation == "join" or len(inputs) >= 2:
            return _join_tables(
                inputs[0],
                inputs[1],
                join_type=metadata.get("joinType"),
                left_key=metadata.get("leftKey"),
                right_key=metadata.get("rightKey"),
                join_key=metadata.get("joinKey"),
            )
        if operation == "filter":
            return _filter_table(inputs[0], str(metadata.get("expression") or ""), parameters)
        if operation == "compute":
            return _compute_table(inputs[0], str(metadata.get("expression") or ""), parameters)
        return inputs[0]

    def _table_to_sample(self, table: PipelineTable, *, limit: Optional[int]) -> Dict[str, Any]:
        return {
            "columns": [{"name": name, "type": "String"} for name in table.columns],
            "rows": table.limited_rows(limit or 200),
        }

    def _select_table(self, result: PipelineRunResult, node_id: Optional[str]) -> PipelineTable:
        if node_id and node_id in result.tables:
            return result.tables[node_id]
        if result.output_nodes:
            return result.tables.get(result.output_nodes[-1], PipelineTable([], []))
        if result.tables:
            return list(result.tables.values())[-1]
        return PipelineTable([], [])


def _normalize_nodes(nodes_raw: Any) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    if not isinstance(nodes_raw, list):
        return output
    for node in nodes_raw:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "")
        if not node_id:
            continue
        output[node_id] = node
    return output


def _normalize_edges(edges_raw: Any) -> List[Dict[str, str]]:
    edges: List[Dict[str, str]] = []
    if not isinstance(edges_raw, list):
        return edges
    for edge in edges_raw:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("from") or "")
        dst = str(edge.get("to") or "")
        if not src or not dst:
            continue
        edges.append({"from": src, "to": dst})
    return edges


def _topological_sort(nodes: Dict[str, Dict[str, Any]], edges: List[Dict[str, str]]) -> List[str]:
    in_degree = {node_id: 0 for node_id in nodes.keys()}
    outgoing: Dict[str, List[str]] = {node_id: [] for node_id in nodes.keys()}
    for edge in edges:
        src = edge["from"]
        dst = edge["to"]
        if src not in nodes or dst not in nodes:
            continue
        outgoing[src].append(dst)
        in_degree[dst] = in_degree.get(dst, 0) + 1

    queue = [node_id for node_id, count in in_degree.items() if count == 0]
    order: List[str] = []
    while queue:
        node_id = queue.pop(0)
        order.append(node_id)
        for nxt in outgoing.get(node_id, []):
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                queue.append(nxt)
    # Fallback to any nodes not in order (cycle)
    for node_id in nodes.keys():
        if node_id not in order:
            order.append(node_id)
    return order


def _build_incoming(edges: List[Dict[str, str]]) -> Dict[str, List[str]]:
    incoming: Dict[str, List[str]] = {}
    for edge in edges:
        incoming.setdefault(edge["to"], []).append(edge["from"])
    return incoming


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


def _join_tables(
    left: PipelineTable,
    right: PipelineTable,
    join_type: Optional[str],
    left_key: Optional[str] = None,
    right_key: Optional[str] = None,
    join_key: Optional[str] = None,
) -> PipelineTable:
    join_type = (join_type or "inner").lower()
    candidate_key = join_key or left_key
    left_join = candidate_key
    right_join = right_key or candidate_key
    if not left_join or not right_join:
        common = [col for col in left.columns if col in right.columns]
        if common:
            left_join = common[0]
            right_join = common[0]
    right_columns = []
    for col in right.columns:
        if col in left.columns:
            right_columns.append(f"right_{col}")
        else:
            right_columns.append(col)
    columns = left.columns + right_columns

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
                    rows.append(_merge_rows(row, match, right_columns))
                    matched_right.add(idx)
            elif join_type in {"left", "full"}:
                rows.append(_merge_rows(row, None, right_columns))
        if join_type in {"right", "full"}:
            for idx, row in enumerate(right.rows):
                if idx not in matched_right:
                    rows.append(_merge_rows(None, row, right_columns))
        return PipelineTable(columns=columns, rows=rows)

    max_len = max(len(left.rows), len(right.rows))
    for idx in range(max_len):
        left_row = left.rows[idx] if idx < len(left.rows) else None
        right_row = right.rows[idx] if idx < len(right.rows) else None
        if left_row is None and right_row is None:
            continue
        if left_row is None and join_type in {"right", "full"}:
            rows.append(_merge_rows(None, right_row, right_columns))
            continue
        if right_row is None and join_type in {"left", "full"}:
            rows.append(_merge_rows(left_row, None, right_columns))
            continue
        if left_row and right_row:
            rows.append(_merge_rows(left_row, right_row, right_columns))
    return PipelineTable(columns=columns, rows=rows)


def _merge_rows(left: Optional[Dict[str, Any]], right: Optional[Dict[str, Any]], right_columns: List[str]) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    if left:
        output.update(left)
    if right:
        for col, mapped in zip(right.keys(), right_columns):
            output[mapped] = right.get(col)
    else:
        for mapped in right_columns:
            output[mapped] = None
    return output


def _filter_table(table: PipelineTable, expression: str, parameters: Dict[str, Any]) -> PipelineTable:
    expression = _apply_parameters((expression or "").strip(), parameters)
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
    expression = _apply_parameters((expression or "").strip(), parameters)
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


def _parse_assignment(expression: str) -> Tuple[str, str]:
    if "=" in expression:
        left, right = expression.split("=", 1)
        return left.strip(), right.strip()
    return "computed", expression


def _safe_eval(expression: str, row: Dict[str, Any], parameters: Dict[str, Any]) -> Any:
    expression = expression.strip()
    if not expression:
        return None
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


def _normalize_parameters(parameters_raw: Any) -> Dict[str, Any]:
    if not isinstance(parameters_raw, list):
        return {}
    output: Dict[str, Any] = {}
    for param in parameters_raw:
        if not isinstance(param, dict):
            continue
        name = str(param.get("name") or "").strip()
        if not name:
            continue
        value = param.get("value")
        output[name] = value
    return output


def _apply_parameters(expression: str, parameters: Dict[str, Any]) -> str:
    if not parameters:
        return expression
    output = expression
    for name, value in parameters.items():
        token = f"${name}"
        if token in output:
            replacement = value
            if isinstance(value, str) and not value.isnumeric() and not value.startswith("'"):
                replacement = f"'{value}'"
            output = output.replace(token, str(replacement))
        brace_token = f"{{{{{name}}}}}"
        if brace_token in output:
            output = output.replace(brace_token, str(value))
    return output


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
