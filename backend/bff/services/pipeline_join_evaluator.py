"""
Pipeline join evaluator.

Computes join coverage, explosion ratio, and null-introduced ratios from sample runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from shared.services.pipeline_executor import PipelineExecutor, PipelineTable
from shared.services.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes
from shared.services.pipeline_transform_spec import resolve_join_spec, normalize_operation


@dataclass(frozen=True)
class JoinEvaluation:
    node_id: str
    left_node_id: str
    right_node_id: str
    join_type: str
    left_key: Optional[str]
    right_key: Optional[str]
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


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / float(denominator), 4)


def _count_matches(
    left: PipelineTable,
    right: PipelineTable,
    left_key: Optional[str],
    right_key: Optional[str],
) -> Tuple[int, int]:
    if not left_key or not right_key:
        return 0, 0
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


async def evaluate_pipeline_joins(
    *,
    definition_json: Dict[str, Any],
    db_name: str,
    dataset_registry: Any,
    node_filter: Optional[str] = None,
) -> Tuple[List[JoinEvaluation], List[str]]:
    nodes = normalize_nodes(definition_json.get("nodes"))
    edges = normalize_edges(definition_json.get("edges"))
    incoming = build_incoming(edges)

    executor = PipelineExecutor(dataset_registry)
    try:
        run_result = await executor.run(definition=definition_json, db_name=db_name)
    except Exception as exc:
        return [], [f"preview run failed: {exc}"]

    tables = run_result.tables
    evaluations: List[JoinEvaluation] = []
    warnings: List[str] = []

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
        left_id, right_id = inputs[0], inputs[1]
        left_table = tables.get(left_id)
        right_table = tables.get(right_id)
        output_table = tables.get(node_id)
        if not left_table or not right_table or not output_table:
            warnings.append(f"join node {node_id} missing tables")
            continue

        join_spec = resolve_join_spec(metadata)
        left_key = join_spec.left_key
        right_key = join_spec.right_key
        left_count = len(left_table.rows)
        right_count = len(right_table.rows)
        output_count = len(output_table.rows)

        matched_left, matched_right = _count_matches(left_table, right_table, left_key, right_key)
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
