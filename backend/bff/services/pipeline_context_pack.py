"""
Pipeline Builder planning context pack.

Provides a small, PII-masked summary of available datasets (schema + sample stats)
to help an LLM propose cleansing/integration pipeline definitions.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import UUID

from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_profiler import compute_column_stats
from shared.services.pipeline_transform_spec import SUPPORTED_TRANSFORMS
from shared.utils.llm_safety import mask_pii


_NAME_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_STOP_TOKENS = {"id", "ids", "code", "key", "no", "num", "number", "value", "data", "info"}


def _normalize_uuid(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return str(UUID(str(value)))
    except Exception:
        return None


def _ensure_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    else:
        items = [value]
    output: list[str] = []
    for item in items:
        if item is None:
            continue
        raw = str(item).strip()
        if not raw:
            continue
        if "," in raw:
            output.extend([part.strip() for part in raw.split(",") if part.strip()])
        else:
            output.append(raw)
    seen: set[str] = set()
    deduped: list[str] = []
    for item in output:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _normalize_columns(schema_json: Any, sample_json: Any) -> list[dict[str, Any]]:
    for source in (schema_json, sample_json):
        if not isinstance(source, dict):
            continue
        cols = source.get("columns")
        if isinstance(cols, list) and cols:
            output: list[dict[str, Any]] = []
            for col in cols:
                if isinstance(col, dict):
                    name = str(col.get("name") or col.get("column") or "").strip()
                    if not name:
                        continue
                    output.append(
                        {
                            "name": name,
                            "type": str(col.get("type") or col.get("data_type") or "xsd:string"),
                        }
                    )
                else:
                    name = str(col or "").strip()
                    if name:
                        output.append({"name": name, "type": "xsd:string"})
            if output:
                return output
    return []


def _extract_rows(sample_json: Any, *, columns: list[dict[str, Any]], max_rows: int) -> list[dict[str, Any]]:
    if not isinstance(sample_json, dict):
        return []
    rows = sample_json.get("rows")
    if rows is None:
        rows = sample_json.get("data")
    if not isinstance(rows, list) or not rows:
        return []
    resolved_max = max(0, int(max_rows))
    rows = rows[:resolved_max] if resolved_max else []
    if not rows:
        return []

    if isinstance(rows[0], dict):
        return [dict(row) for row in rows if isinstance(row, dict)]

    col_names = [str(col.get("name") or "").strip() for col in (columns or [])]
    col_names = [name for name in col_names if name]
    if not col_names:
        return []
    output: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        output.append({col_names[idx]: (row[idx] if idx < len(row) else None) for idx in range(len(col_names))})
    return output


def _tokenize(name: str) -> tuple[list[str], list[str]]:
    raw = str(name or "")
    raw = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", raw)
    tokens = [m.group(0).lower() for m in _NAME_TOKEN_RE.finditer(raw)]
    core = [t for t in tokens if t not in _STOP_TOKENS]
    return tokens, core


def _jaccard(a: Sequence[str], b: Sequence[str]) -> float:
    if not a or not b:
        return 0.0
    set_a = set(a)
    set_b = set(b)
    denom = len(set_a | set_b)
    if denom <= 0:
        return 0.0
    return len(set_a & set_b) / float(denom)


def _name_similarity(left: str, right: str) -> float:
    lt, lc = _tokenize(left)
    rt, rc = _tokenize(right)
    if not lt or not rt:
        return 0.0
    if "".join(lt) == "".join(rt):
        return 1.0
    return max(_jaccard(lt, rt), _jaccard(lc, rc))


def _normalize_cell(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    raw = str(value).strip().lower()
    return raw or None


def _value_overlap(rows_a: list[dict[str, Any]], col_a: str, rows_b: list[dict[str, Any]], col_b: str) -> float:
    set_a = {_normalize_cell(row.get(col_a)) for row in rows_a if isinstance(row, dict)}
    set_b = {_normalize_cell(row.get(col_b)) for row in rows_b if isinstance(row, dict)}
    set_a.discard(None)
    set_b.discard(None)
    if not set_a or not set_b:
        return 0.0
    denom = min(len(set_a), len(set_b))
    if denom <= 0:
        return 0.0
    return len(set_a & set_b) / float(denom)


def _key_quality(column_stats: dict[str, Any], sample_row_count: int) -> float:
    if not isinstance(column_stats, dict) or sample_row_count <= 0:
        return 0.0
    null_count = int(column_stats.get("null_count") or 0)
    empty_count = int(column_stats.get("empty_count") or 0)
    distinct_count = int(column_stats.get("distinct_count") or 0)
    non_null = max(0, sample_row_count - null_count - empty_count)
    if non_null <= 0:
        return 0.0
    distinct_ratio = float(distinct_count) / float(non_null)
    missing_ratio = float(null_count + empty_count) / float(sample_row_count)
    return max(0.0, min(1.0, distinct_ratio * (1.0 - missing_ratio)))


def _suggest_join_keys(datasets: list[dict[str, Any]], *, max_candidates: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for i in range(len(datasets)):
        left = datasets[i]
        for j in range(i + 1, len(datasets)):
            right = datasets[j]
            left_cols = [str(col.get("name") or "").strip() for col in (left.get("columns") or []) if isinstance(col, dict)]
            right_cols = [str(col.get("name") or "").strip() for col in (right.get("columns") or []) if isinstance(col, dict)]
            left_cols = [c for c in left_cols if c]
            right_cols = [c for c in right_cols if c]
            if not left_cols or not right_cols:
                continue

            left_rows = left.get("_raw_rows") if isinstance(left.get("_raw_rows"), list) else []
            right_rows = right.get("_raw_rows") if isinstance(right.get("_raw_rows"), list) else []
            left_stats = left.get("_raw_column_stats") if isinstance(left.get("_raw_column_stats"), dict) else {}
            right_stats = right.get("_raw_column_stats") if isinstance(right.get("_raw_column_stats"), dict) else {}
            left_sample_n = int(left_stats.get("sample_row_count") or 0)
            right_sample_n = int(right_stats.get("sample_row_count") or 0)

            left_quality: dict[str, float] = {}
            for col in left_cols:
                col_stats = (left_stats.get("columns") or {}).get(col) if isinstance(left_stats.get("columns"), dict) else None
                left_quality[col] = _key_quality(col_stats or {}, left_sample_n)
            right_quality: dict[str, float] = {}
            for col in right_cols:
                col_stats = (right_stats.get("columns") or {}).get(col) if isinstance(right_stats.get("columns"), dict) else None
                right_quality[col] = _key_quality(col_stats or {}, right_sample_n)

            top_left = sorted(left_cols, key=lambda c: left_quality.get(c, 0.0), reverse=True)[:8]
            top_right = sorted(right_cols, key=lambda c: right_quality.get(c, 0.0), reverse=True)[:8]

            for col_a in top_left:
                for col_b in top_right:
                    name_sim = _name_similarity(col_a, col_b)
                    overlap = _value_overlap(left_rows, col_a, right_rows, col_b)
                    quality = min(left_quality.get(col_a, 0.0), right_quality.get(col_b, 0.0))
                    score = 0.45 * name_sim + 0.45 * overlap + 0.10 * quality
                    if score < 0.55 or (overlap <= 0.0 and name_sim < 0.9):
                        continue
                    reasons: list[str] = []
                    if name_sim >= 0.9:
                        reasons.append("column names match closely")
                    elif name_sim >= 0.7:
                        reasons.append("column names are similar")
                    if overlap >= 0.5:
                        reasons.append("many sample values overlap")
                    elif overlap > 0:
                        reasons.append("some sample values overlap")
                    if quality >= 0.7:
                        reasons.append("high distinctness / low missingness in sample")
                    candidates.append(
                        {
                            "left_dataset_id": left.get("dataset_id"),
                            "right_dataset_id": right.get("dataset_id"),
                            "left_column": col_a,
                            "right_column": col_b,
                            "score": round(float(score), 3),
                            "name_similarity": round(float(name_sim), 3),
                            "sample_value_overlap": round(float(overlap), 3),
                            "reasons": reasons,
                        }
                    )

    candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    return candidates[: max(0, int(max_candidates))]


def _suggest_cleansing(dataset: dict[str, Any]) -> list[dict[str, Any]]:
    stats = dataset.get("_raw_column_stats")
    if not isinstance(stats, dict):
        return []
    sample_n = int(stats.get("sample_row_count") or 0)
    columns = stats.get("columns") if isinstance(stats.get("columns"), dict) else {}
    suggestions: list[dict[str, Any]] = []
    for col_name, col_stats in (columns or {}).items():
        if not isinstance(col_stats, dict):
            continue
        null_count = int(col_stats.get("null_count") or 0)
        empty_count = int(col_stats.get("empty_count") or 0)
        whitespace_count = int(col_stats.get("whitespace_count") or 0)
        distinct_count = int(col_stats.get("distinct_count") or 0)
        missing = null_count + empty_count
        if whitespace_count > 0:
            suggestions.append({"column": col_name, "suggestion": "trim whitespace", "evidence": {"whitespace_count": whitespace_count}})
        if empty_count > 0:
            suggestions.append({"column": col_name, "suggestion": "normalize empty strings to null", "evidence": {"empty_count": empty_count}})
        if sample_n > 0 and missing / float(sample_n) >= 0.3:
            suggestions.append(
                {
                    "column": col_name,
                    "suggestion": "high missingness; consider filtering or default values",
                    "evidence": {"missing_ratio": round(missing / float(sample_n), 3)},
                }
            )
        if sample_n > 3 and distinct_count <= 1 and missing < sample_n:
            suggestions.append({"column": col_name, "suggestion": "constant column; consider dropping", "evidence": {"distinct_count": distinct_count}})
    return suggestions


async def build_pipeline_context_pack(
    *,
    db_name: str,
    branch: Optional[str],
    dataset_ids: Optional[Sequence[str]],
    dataset_registry: DatasetRegistry,
    max_datasets_overview: int = 20,
    max_selected_datasets: int = 6,
    max_sample_rows: int = 20,
    max_join_candidates: int = 10,
) -> Dict[str, Any]:
    db_name = str(db_name or "").strip()
    if not db_name:
        return {"datasets_overview": [], "selected_datasets": []}

    branch_value = str(branch or "").strip() or None
    raw = await dataset_registry.list_datasets(db_name=db_name, branch=branch_value)
    raw = list(raw or [])

    selected_ids = [_normalize_uuid(item) for item in _ensure_string_list(dataset_ids)]
    selected_ids = [item for item in selected_ids if item]
    selected_set = set(selected_ids)

    overview: list[dict[str, Any]] = []
    for item in raw[: max(0, int(max_datasets_overview))]:
        if not isinstance(item, dict):
            continue
        cols = _normalize_columns(item.get("schema_json"), item.get("sample_json"))
        overview.append(
            {
                "dataset_id": item.get("dataset_id"),
                "name": item.get("name"),
                "branch": item.get("branch"),
                "source_type": item.get("source_type"),
                "updated_at": (item.get("updated_at").isoformat() if item.get("updated_at") else None),
                "column_count": len(cols),
                "columns_preview": [col.get("name") for col in cols[:20] if isinstance(col, dict) and col.get("name")],
                "latest_commit_id": item.get("latest_commit_id"),
                "row_count": item.get("row_count"),
            }
        )

    selected: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        dataset_id = _normalize_uuid(item.get("dataset_id"))
        if not dataset_id or dataset_id not in selected_set:
            continue
        cols = _normalize_columns(item.get("schema_json"), item.get("sample_json"))
        raw_rows = _extract_rows(item.get("sample_json"), columns=cols, max_rows=max_sample_rows)
        masked_rows = mask_pii(raw_rows, max_string_chars=120)
        raw_stats = compute_column_stats(rows=raw_rows, columns=cols, max_top_values=5)
        masked_stats = mask_pii(raw_stats, max_string_chars=120)
        selected.append(
            {
                "dataset_id": dataset_id,
                "name": item.get("name"),
                "branch": item.get("branch"),
                "source_type": item.get("source_type"),
                "description": item.get("description"),
                "latest_commit_id": item.get("latest_commit_id"),
                "row_count": item.get("row_count"),
                "columns": cols,
                "sample_rows": masked_rows,
                "column_stats": masked_stats,
                "_raw_rows": raw_rows,
                "_raw_column_stats": raw_stats,
            }
        )
        if len(selected) >= max(0, int(max_selected_datasets)):
            break

    join_candidates = _suggest_join_keys(selected, max_candidates=max_join_candidates) if len(selected) >= 2 else []
    cleansing_suggestions: list[dict[str, Any]] = []
    for dataset in selected:
        for suggestion in _suggest_cleansing(dataset):
            cleansing_suggestions.append({"dataset_id": dataset.get("dataset_id"), **suggestion})

    for dataset in selected:
        dataset.pop("_raw_rows", None)
        dataset.pop("_raw_column_stats", None)

    return {
        "db_name": db_name,
        "branch": branch_value,
        "datasets_overview": overview,
        "selected_datasets": selected,
        "integration_suggestions": {
            "join_key_candidates": join_candidates,
            "cleansing_suggestions": cleansing_suggestions,
        },
        "pipeline_definition_hints": {
            "supported_operations": sorted(SUPPORTED_TRANSFORMS),
            "input_node_metadata": {
                "datasetId|dataset_id": "UUID of dataset",
                "datasetName|dataset_name": "dataset name (fallback)",
                "datasetBranch|dataset_branch": "lakeFS branch (default to plan scope branch/main)",
            },
            "transform_node_metadata": {
                "operation": "one of supported_operations",
            },
            "note": "Keep definitions sample-safe; avoid UDF. Use joins/unions + cleansing ops (rename/cast/dedupe/filter/compute).",
        },
    }
