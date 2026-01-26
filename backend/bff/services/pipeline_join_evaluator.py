"""
Pipeline join evaluator.

Computes join coverage, explosion ratio, and null-introduced ratios from sample runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Sequence

from shared.services.pipeline.pipeline_executor import PipelineExecutor, PipelineTable
from shared.services.storage.storage_service import StorageService
from shared.services.pipeline.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes
from shared.services.pipeline.pipeline_transform_spec import resolve_join_spec, normalize_operation


@dataclass(frozen=True)
class JoinEvaluation:
    node_id: str
    left_node_id: str
    right_node_id: str
    join_type: str
    left_key: Optional[str]
    right_key: Optional[str]
    left_keys: Optional[List[str]]
    right_keys: Optional[List[str]]
    left_row_count: int
    right_row_count: int
    output_row_count: int
    left_coverage: float
    right_coverage: float
    explosion_ratio: float
    left_missing_ratio: float
    right_missing_ratio: float
    left_null_introduced_ratio: float
    right_null_introduced_ratio: float


_JOIN_EVAL_SAMPLE_MIN_ROWS = 800


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / float(denominator), 4)


def _count_matches(
    left: PipelineTable,
    right: PipelineTable,
    left_keys: Optional[Sequence[str]],
    right_keys: Optional[Sequence[str]],
) -> Tuple[int, int]:
    if not left_keys or not right_keys or len(left_keys) != len(right_keys):
        return 0, 0
    if len(left_keys) == 1:
        left_key = left_keys[0]
        right_key = right_keys[0]
        right_index: Dict[Any, List[int]] = {}
        for idx, row in enumerate(right.rows):
            right_index.setdefault(row.get(right_key), []).append(idx)
        matched_right: set[int] = set()
        matched_left = 0
        for row in left.rows:
            key = row.get(left_key)
            matches = right_index.get(key) or []
            if matches:
                matched_left += 1
                matched_right.update(matches)
        return matched_left, len(matched_right)

    def row_key(row: Dict[str, Any], keys: Sequence[str]) -> Tuple[Any, ...]:
        return tuple(row.get(key) for key in keys)

    right_index: Dict[Tuple[Any, ...], List[int]] = {}
    for idx, row in enumerate(right.rows):
        right_index.setdefault(row_key(row, right_keys), []).append(idx)
    matched_right: set[int] = set()
    matched_left = 0
    for row in left.rows:
        key = row_key(row, left_keys)
        matches = right_index.get(key) or []
        if matches:
            matched_left += 1
            matched_right.update(matches)
    return matched_left, len(matched_right)


def _coerce_table(payload: Any) -> Optional[PipelineTable]:
    if not isinstance(payload, dict):
        return None
    columns_raw = payload.get("columns")
    rows_raw = payload.get("rows")
    if not isinstance(columns_raw, list) or not isinstance(rows_raw, list):
        return None
    columns: List[str] = []
    for col in columns_raw:
        if isinstance(col, dict):
            name = str(col.get("name") or "").strip()
        else:
            name = str(col or "").strip()
        if name:
            columns.append(name)
    rows: List[Dict[str, Any]] = [dict(row) for row in rows_raw if isinstance(row, dict)]
    return PipelineTable(columns=columns, rows=rows)


def _coerce_tables(payload: Any) -> Tuple[Dict[str, PipelineTable], List[str]]:
    tables: Dict[str, PipelineTable] = {}
    warnings: List[str] = []
    if not isinstance(payload, dict):
        return tables, warnings
    for node_id, table_payload in payload.items():
        if not isinstance(node_id, str) or not node_id.strip():
            continue
        table = _coerce_table(table_payload)
        if not table:
            warnings.append(f"run_tables missing or invalid for node {node_id}")
            continue
        tables[node_id] = table
    return tables, warnings


def _choose_join_inputs(
    inputs: List[str],
    tables: Dict[str, PipelineTable],
    left_key: Optional[str],
    right_key: Optional[str],
    left_keys: Optional[Sequence[str]],
    right_keys: Optional[Sequence[str]],
) -> Tuple[Optional[str], Optional[str], List[str]]:
    warnings: List[str] = []
    candidates = [item for item in inputs if isinstance(item, str) and item.strip()]
    if len(candidates) < 2:
        return None, None, warnings

    ordered = sorted(candidates)
    resolved_left = list(left_keys or [])
    resolved_right = list(right_keys or [])
    if left_key and not resolved_left:
        resolved_left = [left_key]
    if right_key and not resolved_right:
        resolved_right = [right_key]

    if not resolved_left and not resolved_right:
        return ordered[0], ordered[1], warnings

    scored: List[Tuple[int, str, str]] = []
    for left_id in ordered:
        for right_id in ordered:
            if left_id == right_id:
                continue
            left_table = tables.get(left_id)
            right_table = tables.get(right_id)
            score = 0
            if resolved_left and left_table and all(key in left_table.columns for key in resolved_left):
                score += 2
            if resolved_right and right_table and all(key in right_table.columns for key in resolved_right):
                score += 2
            if resolved_left and right_table and all(key in right_table.columns for key in resolved_left):
                score -= 1
            if resolved_right and left_table and all(key in left_table.columns for key in resolved_right):
                score -= 1
            scored.append((score, left_id, right_id))

    if scored:
        scored.sort(key=lambda item: (-item[0], item[1], item[2]))
        best_score, left_id, right_id = scored[0]
        if best_score <= 0:
            warnings.append("join input order ambiguous; falling back to stable ordering")
            return ordered[0], ordered[1], warnings
        return left_id, right_id, warnings

    return ordered[0], ordered[1], warnings


async def evaluate_pipeline_joins(
    *,
    definition_json: Dict[str, Any],
    db_name: str,
    dataset_registry: Any,
    node_filter: Optional[str] = None,
    run_tables: Optional[Dict[str, Any]] = None,
    storage_service: Optional[StorageService] = None,
) -> Tuple[List[JoinEvaluation], List[str]]:
    nodes = normalize_nodes(definition_json.get("nodes"))
    edges = normalize_edges(definition_json.get("edges"))
    incoming = build_incoming(edges)

    has_join = False
    for node in nodes.values():
        if not isinstance(node, dict):
            continue
        if node.get("type") != "transform":
            continue
        metadata = node.get("metadata") or {}
        if normalize_operation(metadata.get("operation")) == "join":
            has_join = True
            break

    warnings: List[str] = []
    if run_tables is not None:
        tables, table_warnings = _coerce_tables(run_tables)
        warnings.extend(table_warnings)
    else:
        executor = PipelineExecutor(dataset_registry, storage_service=storage_service)
        definition_for_run = dict(definition_json) if isinstance(definition_json, dict) else {}
        if has_join:
            preview_meta = dict(definition_for_run.get("__preview_meta__") or {})
            current_limit = None
            if "sample_limit" in preview_meta:
                try:
                    current_limit = int(preview_meta.get("sample_limit") or 0)
                except (TypeError, ValueError):
                    current_limit = None
            if current_limit is None or current_limit < _JOIN_EVAL_SAMPLE_MIN_ROWS:
                preview_meta["sample_limit"] = _JOIN_EVAL_SAMPLE_MIN_ROWS
            definition_for_run["__preview_meta__"] = preview_meta
        try:
            run_result = await executor.run(definition=definition_for_run, db_name=db_name)
        except Exception as exc:
            return [], [f"preview run failed: {exc}"]
        tables = run_result.tables
    evaluations: List[JoinEvaluation] = []

    for node_id, node in nodes.items():
        if node_filter and node_id != node_filter:
            continue
        if node.get("type") != "transform":
            continue
        metadata = node.get("metadata") or {}
        if normalize_operation(metadata.get("operation")) != "join":
            continue
        inputs = incoming.get(node_id, [])
        if len(inputs) < 2:
            warnings.append(f"join node {node_id} missing inputs")
            continue
        join_spec = resolve_join_spec(metadata)
        left_id, right_id, input_warnings = _choose_join_inputs(
            inputs,
            tables,
            join_spec.left_key,
            join_spec.right_key,
            join_spec.left_keys,
            join_spec.right_keys,
        )
        warnings.extend(input_warnings)
        if not left_id or not right_id:
            warnings.append(f"join node {node_id} missing inputs")
            continue
        left_table = tables.get(left_id)
        right_table = tables.get(right_id)
        output_table = tables.get(node_id)
        if not left_table or not right_table or not output_table:
            warnings.append(f"join node {node_id} missing tables")
            continue

        left_keys = list(join_spec.left_keys or [])
        right_keys = list(join_spec.right_keys or [])
        left_key = join_spec.left_key
        right_key = join_spec.right_key
        if left_key and not left_keys:
            left_keys = [left_key]
        if right_key and not right_keys:
            right_keys = [right_key]
        left_count = len(left_table.rows)
        right_count = len(right_table.rows)
        output_count = len(output_table.rows)

        matched_left, matched_right = _count_matches(left_table, right_table, left_keys, right_keys)
        left_coverage = _ratio(matched_left, left_count)
        right_coverage = _ratio(matched_right, right_count)
        explosion_ratio = _ratio(output_count, max(left_count, right_count, 1))
        left_missing_ratio = _ratio(left_count - matched_left, left_count)
        right_missing_ratio = _ratio(right_count - matched_right, right_count)
        left_null_ratio = left_missing_ratio if join_spec.join_type in {"left", "full"} else 0.0
        right_null_ratio = right_missing_ratio if join_spec.join_type in {"right", "full"} else 0.0

        evaluations.append(
            JoinEvaluation(
                node_id=node_id,
                left_node_id=left_id,
                right_node_id=right_id,
                join_type=join_spec.join_type,
                left_key=left_key,
                right_key=right_key,
                left_keys=left_keys or None,
                right_keys=right_keys or None,
                left_row_count=left_count,
                right_row_count=right_count,
                output_row_count=output_count,
                left_coverage=left_coverage,
                right_coverage=right_coverage,
                explosion_ratio=explosion_ratio,
                left_missing_ratio=left_missing_ratio,
                right_missing_ratio=right_missing_ratio,
                left_null_introduced_ratio=left_null_ratio,
                right_null_introduced_ratio=right_null_ratio,
            )
        )

    return evaluations, warnings
