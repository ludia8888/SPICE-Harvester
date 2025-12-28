from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from shared.services.pipeline_executor import PipelineExecutor, PipelineRunResult, PipelineTable


@dataclass(frozen=True)
class PipelineUnitTestResult:
    name: str
    passed: bool
    diff: Dict[str, Any]
    error: Optional[str] = None


def _stable_row_key(row: Dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _normalize_columns(spec: Dict[str, Any]) -> List[str]:
    raw = spec.get("columns")
    if isinstance(raw, list):
        cols = [str(item) for item in raw if str(item).strip()]
    else:
        cols = []

    rows = spec.get("rows")
    if not cols and isinstance(rows, list):
        seen: set[str] = set()
        for item in rows:
            if not isinstance(item, dict):
                continue
            for key in item.keys():
                key_str = str(key)
                if key_str and key_str not in seen:
                    cols.append(key_str)
                    seen.add(key_str)
    return cols


def _normalize_rows(spec: Dict[str, Any], *, columns: List[str]) -> List[Dict[str, Any]]:
    raw = spec.get("rows")
    if not isinstance(raw, list):
        return []
    rows: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        if columns:
            rows.append({col: item.get(col) for col in columns})
        else:
            rows.append(dict(item))
    return rows


def _table_from_spec(spec: Dict[str, Any]) -> PipelineTable:
    columns = _normalize_columns(spec)
    rows = _normalize_rows(spec, columns=columns)
    return PipelineTable(columns=columns, rows=rows)


def _diff_tables(actual: PipelineTable, expected: PipelineTable, *, max_rows: int = 50) -> Dict[str, Any]:
    missing_columns = [col for col in expected.columns if col not in actual.columns]
    extra_columns = [col for col in actual.columns if col not in expected.columns]

    expected_rows = [_stable_row_key(row) for row in expected.rows]
    actual_rows = [_stable_row_key(row) for row in actual.rows]
    expected_counter = Counter(expected_rows)
    actual_counter = Counter(actual_rows)

    missing_rows: List[Dict[str, Any]] = []
    extra_rows: List[Dict[str, Any]] = []

    for row_key, count in (expected_counter - actual_counter).items():
        try:
            parsed = json.loads(row_key)
        except Exception:
            parsed = {"raw": row_key}
        for _ in range(min(count, max_rows - len(missing_rows))):
            missing_rows.append(parsed)
        if len(missing_rows) >= max_rows:
            break

    for row_key, count in (actual_counter - expected_counter).items():
        try:
            parsed = json.loads(row_key)
        except Exception:
            parsed = {"raw": row_key}
        for _ in range(min(count, max_rows - len(extra_rows))):
            extra_rows.append(parsed)
        if len(extra_rows) >= max_rows:
            break

    diff: Dict[str, Any] = {}
    if missing_columns:
        diff["missing_columns"] = missing_columns
    if extra_columns:
        diff["extra_columns"] = extra_columns
    if missing_rows:
        diff["missing_rows"] = missing_rows
    if extra_rows:
        diff["extra_rows"] = extra_rows
    return diff


def _select_table(result: PipelineRunResult, *, node_id: Optional[str]) -> PipelineTable:
    if node_id:
        table = result.tables.get(node_id)
        if table is None:
            raise ValueError(f"Unknown node_id in test: {node_id}")
        return table
    if result.output_nodes:
        return result.tables[result.output_nodes[0]]
    # Fallback: return the last computed table.
    if result.tables:
        last_key = list(result.tables.keys())[-1]
        return result.tables[last_key]
    return PipelineTable(columns=[], rows=[])


async def run_unit_tests(
    *,
    executor: PipelineExecutor,
    definition: Dict[str, Any],
    db_name: str,
    unit_tests: Optional[List[Dict[str, Any]]] = None,
) -> List[PipelineUnitTestResult]:
    raw_tests = unit_tests
    if raw_tests is None:
        candidate = definition.get("unitTests") or definition.get("unit_tests") or []
        raw_tests = candidate if isinstance(candidate, list) else []

    results: List[PipelineUnitTestResult] = []
    for raw in raw_tests:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip() or "unnamed"
        try:
            inputs_raw = raw.get("inputs") or {}
            if not isinstance(inputs_raw, dict):
                raise ValueError("inputs must be an object keyed by input node id")

            overrides: Dict[str, PipelineTable] = {}
            for node_id, spec in inputs_raw.items():
                if not isinstance(spec, dict):
                    raise ValueError(f"input {node_id} must be an object with columns/rows")
                overrides[str(node_id)] = _table_from_spec(spec)

            expected_raw = raw.get("expected") or {}
            if not isinstance(expected_raw, dict):
                raise ValueError("expected must be an object with columns/rows")
            expected_table = _table_from_spec(expected_raw)

            target_node_id = raw.get("target_node_id") or raw.get("targetNodeId")
            target_node_id = str(target_node_id).strip() if target_node_id else None

            run_result = await executor.run(definition=definition, db_name=db_name, input_overrides=overrides)
            actual_table = _select_table(run_result, node_id=target_node_id)

            diff = _diff_tables(actual_table, expected_table)
            results.append(PipelineUnitTestResult(name=name, passed=not diff, diff=diff))
        except Exception as exc:
            results.append(PipelineUnitTestResult(name=name, passed=False, diff={}, error=str(exc)))
    return results

