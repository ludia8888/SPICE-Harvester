"""
Pipeline Builder planning context pack.

Provides a small, PII-masked summary of available datasets (schema + sample stats)
to help an LLM propose cleansing/integration pipeline definitions.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from itertools import combinations
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import UUID

from shared.services.dataset_registry import DatasetRegistry
from shared.services.dataset_profile_registry import DatasetProfileRegistry
from shared.services.pipeline_profiler import compute_column_stats
from shared.services.pipeline_schema_utils import normalize_schema_type
from shared.services.pipeline_transform_spec import SUPPORTED_TRANSFORMS
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.utils.llm_safety import mask_pii
from shared.utils.schema_hash import compute_schema_hash

logger = logging.getLogger(__name__)


_NAME_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_STOP_TOKENS = {"id", "ids", "code", "key", "no", "num", "number", "value", "data", "info"}
_PATTERN_KEEP = {"-", "_", "/", ":", "."}


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


def _pattern_signature(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    raw = str(value)
    if not raw:
        return None
    tokens: list[str] = []
    for ch in raw:
        if ch.isdigit():
            tokens.append("9")
        elif ch.isalpha():
            tokens.append("A")
        elif ch in _PATTERN_KEEP:
            tokens.append(ch)
        else:
            tokens.append("-")
    signature = "".join(tokens)
    signature = re.sub(r"-+", "-", signature).strip("-")
    return signature or None


def _column_format_profile(values: list[Any], *, max_patterns: int = 4) -> Dict[str, Any]:
    patterns: Counter[str] = Counter()
    lengths: list[int] = []
    digit_count = 0
    alpha_count = 0
    total_chars = 0
    for value in values:
        if value in (None, ""):
            continue
        text = str(value).strip()
        if not text:
            continue
        lengths.append(len(text))
        total_chars += len(text)
        digit_count += sum(1 for ch in text if ch.isdigit())
        alpha_count += sum(1 for ch in text if ch.isalpha())
        signature = _pattern_signature(text)
        if signature:
            patterns[signature] += 1
    if not lengths:
        return {}
    avg_length = sum(lengths) / float(len(lengths))
    pattern_samples = [
        {"pattern": pattern, "count": int(count)}
        for pattern, count in patterns.most_common(max(0, int(max_patterns)))
    ]
    digit_ratio = (digit_count / total_chars) if total_chars else 0.0
    alpha_ratio = (alpha_count / total_chars) if total_chars else 0.0
    return {
        "pattern_samples": pattern_samples,
        "avg_length": round(avg_length, 2),
        "min_length": int(min(lengths)),
        "max_length": int(max(lengths)),
        "digit_ratio": round(digit_ratio, 3),
        "alpha_ratio": round(alpha_ratio, 3),
    }


def _format_similarity(left_profile: Dict[str, Any], right_profile: Dict[str, Any]) -> float:
    left_patterns = {
        str(item.get("pattern"))
        for item in left_profile.get("pattern_samples", [])
        if isinstance(item, dict) and item.get("pattern")
    }
    right_patterns = {
        str(item.get("pattern"))
        for item in right_profile.get("pattern_samples", [])
        if isinstance(item, dict) and item.get("pattern")
    }
    if not left_patterns or not right_patterns:
        return 0.0
    denom = len(left_patterns | right_patterns)
    if denom <= 0:
        return 0.0
    return len(left_patterns & right_patterns) / float(denom)


def _value_overlap_stats(
    rows_a: list[dict[str, Any]],
    col_a: str,
    rows_b: list[dict[str, Any]],
    col_b: str,
) -> Dict[str, float]:
    set_a = {_normalize_cell(row.get(col_a)) for row in rows_a if isinstance(row, dict)}
    set_b = {_normalize_cell(row.get(col_b)) for row in rows_b if isinstance(row, dict)}
    set_a.discard(None)
    set_b.discard(None)
    if not set_a or not set_b:
        return {
            "overlap_ratio": 0.0,
            "left_containment": 0.0,
            "right_containment": 0.0,
            "left_value_count": float(len(set_a)),
            "right_value_count": float(len(set_b)),
        }
    intersection = set_a & set_b
    denom = min(len(set_a), len(set_b))
    overlap_ratio = (len(intersection) / float(denom)) if denom > 0 else 0.0
    left_containment = len(intersection) / float(len(set_a)) if set_a else 0.0
    right_containment = len(intersection) / float(len(set_b)) if set_b else 0.0
    return {
        "overlap_ratio": overlap_ratio,
        "left_containment": left_containment,
        "right_containment": right_containment,
        "left_value_count": float(len(set_a)),
        "right_value_count": float(len(set_b)),
    }


def _composite_key_stats(rows: list[dict[str, Any]], cols: tuple[str, ...]) -> tuple[float, float]:
    if not rows or not cols:
        return 0.0, 1.0
    total = len(rows)
    non_null_rows: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        values: list[Any] = []
        missing = False
        for col in cols:
            value = _normalize_cell(row.get(col))
            if value is None:
                missing = True
                break
            values.append(value)
        if not missing:
            non_null_rows.append(tuple(values))
    if not non_null_rows:
        return 0.0, 1.0
    distinct_ratio = len(set(non_null_rows)) / float(len(non_null_rows))
    missing_ratio = 1.0 - (len(non_null_rows) / float(total)) if total else 1.0
    return min(1.0, distinct_ratio), min(1.0, missing_ratio)


def _composite_overlap_stats(
    rows_a: list[dict[str, Any]],
    cols_a: tuple[str, ...],
    rows_b: list[dict[str, Any]],
    cols_b: tuple[str, ...],
) -> Dict[str, float]:
    def _collect(rows: list[dict[str, Any]], cols: tuple[str, ...]) -> set[tuple[str, ...]]:
        output: set[tuple[str, ...]] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            values: list[str] = []
            for col in cols:
                value = _normalize_cell(row.get(col))
                if value is None:
                    values = []
                    break
                values.append(value)
            if values:
                output.add(tuple(values))
        return output

    set_a = _collect(rows_a, cols_a)
    set_b = _collect(rows_b, cols_b)
    if not set_a or not set_b:
        return {
            "overlap_ratio": 0.0,
            "left_containment": 0.0,
            "right_containment": 0.0,
            "left_value_count": float(len(set_a)),
            "right_value_count": float(len(set_b)),
        }
    intersection = set_a & set_b
    denom = min(len(set_a), len(set_b))
    overlap_ratio = (len(intersection) / float(denom)) if denom > 0 else 0.0
    left_containment = len(intersection) / float(len(set_a)) if set_a else 0.0
    right_containment = len(intersection) / float(len(set_b)) if set_b else 0.0
    return {
        "overlap_ratio": overlap_ratio,
        "left_containment": left_containment,
        "right_containment": right_containment,
        "left_value_count": float(len(set_a)),
        "right_value_count": float(len(set_b)),
    }


def _distinct_ratio(column_stats: dict[str, Any], sample_row_count: int) -> float:
    if not isinstance(column_stats, dict) or sample_row_count <= 0:
        return 0.0
    null_count = int(column_stats.get("null_count") or 0)
    empty_count = int(column_stats.get("empty_count") or 0)
    distinct_count = int(column_stats.get("distinct_count") or 0)
    non_null = max(0, sample_row_count - null_count - empty_count)
    if non_null <= 0:
        return 0.0
    return min(1.0, distinct_count / float(non_null))


def _missing_ratio(column_stats: dict[str, Any], sample_row_count: int) -> float:
    if not isinstance(column_stats, dict) or sample_row_count <= 0:
        return 0.0
    null_count = int(column_stats.get("null_count") or 0)
    empty_count = int(column_stats.get("empty_count") or 0)
    return (null_count + empty_count) / float(sample_row_count)


def _estimate_cardinality(left_ratio: float, right_ratio: float, *, threshold: float = 0.95) -> str:
    left_unique = left_ratio >= threshold
    right_unique = right_ratio >= threshold
    if left_unique and right_unique:
        return "1:1"
    if left_unique and not right_unique:
        return "1:N"
    if right_unique and not left_unique:
        return "N:1"
    return "N:N"


def _cardinality_confidence(sample_count: int) -> float:
    if sample_count >= 2000:
        return 0.9
    if sample_count >= 1000:
        return 0.8
    if sample_count >= 500:
        return 0.65
    if sample_count >= 200:
        return 0.5
    if sample_count >= 50:
        return 0.35
    return 0.2


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


def _type_family(column_stats: dict[str, Any], profile: dict[str, Any]) -> str:
    raw_type = normalize_schema_type(column_stats.get("type") if isinstance(column_stats, dict) else None)
    if raw_type:
        if raw_type in {"xsd:integer", "xsd:decimal"}:
            return "number"
        if raw_type in {"xsd:boolean"}:
            return "boolean"
        if raw_type in {"xsd:date", "xsd:dateTime"}:
            return "date"
        if raw_type in {"xsd:string"}:
            return "string"

    fmt = profile.get("format") if isinstance(profile, dict) else {}
    digit_ratio = fmt.get("digit_ratio")
    alpha_ratio = fmt.get("alpha_ratio")
    if isinstance(digit_ratio, (int, float)) and isinstance(alpha_ratio, (int, float)):
        if digit_ratio >= 0.9 and alpha_ratio <= 0.1:
            return "number_like"
        if alpha_ratio >= 0.6:
            return "string_like"
    return "unknown"


def _type_compatibility(left_family: str, right_family: str) -> float:
    if left_family == right_family:
        return 0.4 if left_family == "unknown" else 1.0
    numeric = {"number", "number_like"}
    stringish = {"string", "string_like"}
    if left_family in numeric and right_family in numeric:
        return 0.8
    if left_family in stringish and right_family in stringish:
        return 0.8
    if "unknown" in (left_family, right_family):
        return 0.5
    return 0.1


def _select_candidate_columns(
    columns: list[str],
    quality_scores: dict[str, float],
    *,
    min_candidates: int = 4,
    max_candidates: int = 12,
) -> list[str]:
    if not columns:
        return []
    ordered = sorted(columns, key=lambda c: quality_scores.get(c, 0.0), reverse=True)
    if len(ordered) <= min_candidates:
        return ordered
    scores = [quality_scores.get(c, 0.0) for c in ordered]
    median = sorted(scores)[len(scores) // 2]
    threshold = max(0.2, min(0.9, median + 0.15))
    selected = [col for col in ordered if quality_scores.get(col, 0.0) >= threshold]
    if len(selected) < min_candidates:
        selected = ordered[:min_candidates]
    if len(selected) > max_candidates:
        selected = selected[:max_candidates]
    return selected


def _extend_candidates_with_name_matches(
    left_cols: list[str],
    right_cols: list[str],
    left_candidates: list[str],
    right_candidates: list[str],
    *,
    threshold: float = 0.9,
    max_pairs: int = 8,
    max_candidates: int = 14,
) -> tuple[list[str], list[str]]:
    pairs: list[tuple[float, str, str]] = []
    for col_a in left_cols:
        for col_b in right_cols:
            name_sim = _name_similarity(col_a, col_b)
            if name_sim >= threshold:
                pairs.append((name_sim, col_a, col_b))
    pairs.sort(key=lambda item: item[0], reverse=True)
    for _, col_a, col_b in pairs[: max(0, int(max_pairs))]:
        if col_a not in left_candidates:
            left_candidates.append(col_a)
        if col_b not in right_candidates:
            right_candidates.append(col_b)
    return left_candidates[: max(0, int(max_candidates))], right_candidates[: max(0, int(max_candidates))]


def _looks_like_id(name: str) -> bool:
    lowered = str(name or "").strip().lower()
    if not lowered:
        return False
    if lowered == "id":
        return True
    if lowered.endswith("_id"):
        return True
    if lowered.endswith("id") and len(lowered) <= 6:
        return True
    return False


def _score_pk_candidate(
    column_name: str,
    column_stats: dict[str, Any],
    sample_row_count: int,
) -> tuple[float, float, float, list[str]]:
    if sample_row_count <= 0:
        return 0.0, 0.0, 1.0, []
    null_count = int(column_stats.get("null_count") or 0)
    empty_count = int(column_stats.get("empty_count") or 0)
    distinct_count = int(column_stats.get("distinct_count") or 0)
    non_null = max(0, sample_row_count - null_count - empty_count)
    if non_null <= 0:
        return 0.0, 0.0, 1.0, []
    distinct_ratio = float(distinct_count) / float(non_null)
    missing_ratio = float(null_count + empty_count) / float(sample_row_count)
    score = 0.65 * distinct_ratio + 0.35 * (1.0 - missing_ratio)
    reasons: list[str] = []
    if _looks_like_id(column_name):
        score += 0.1
        reasons.append("name looks like an id")
    if distinct_ratio >= 0.95:
        reasons.append("high distinct ratio in sample")
    if missing_ratio <= 0.05:
        reasons.append("low missingness in sample")
    return min(1.0, round(score, 3)), round(distinct_ratio, 3), round(missing_ratio, 3), reasons


def _suggest_primary_keys(
    *,
    columns: list[dict[str, Any]],
    column_stats: dict[str, Any],
    sample_rows: list[dict[str, Any]],
    max_candidates: int,
) -> list[dict[str, Any]]:
    sample_row_count = int(column_stats.get("sample_row_count") or 0)
    stats_by_col = column_stats.get("columns") if isinstance(column_stats.get("columns"), dict) else {}
    candidates: list[dict[str, Any]] = []

    for col in columns:
        name = str(col.get("name") or "").strip()
        if not name:
            continue
        col_stats = stats_by_col.get(name) if isinstance(stats_by_col, dict) else {}
        score, distinct_ratio, missing_ratio, reasons = _score_pk_candidate(name, col_stats or {}, sample_row_count)
        if score <= 0.0:
            continue
        candidates.append(
            {
                "columns": [name],
                "score": score,
                "distinct_ratio": distinct_ratio,
                "missing_ratio": missing_ratio,
                "duplicate_ratio": round(1.0 - distinct_ratio, 3),
                "reasons": reasons,
            }
        )

    candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    top_candidates = candidates[: max(0, int(max_candidates))]

    if not sample_rows or len(top_candidates) < 2:
        return top_candidates

    combo_candidates: list[dict[str, Any]] = []
    combo_cols = [item["columns"][0] for item in top_candidates[:4]]
    for cols in combinations(combo_cols, 2):
        non_null_rows = [
            row for row in sample_rows
            if all(str(row.get(col) or "").strip() not in ("", "None") for col in cols)
        ]
        if not non_null_rows:
            continue
        unique_count = len({tuple(row.get(col) for col in cols) for row in non_null_rows})
        distinct_ratio = float(unique_count) / float(len(non_null_rows))
        missing_ratio = 1.0 - (len(non_null_rows) / float(len(sample_rows)))
        score = 0.65 * distinct_ratio + 0.35 * (1.0 - missing_ratio)
        if score < 0.7:
            continue
        combo_candidates.append(
            {
                "columns": list(cols),
                "score": round(score, 3),
                "distinct_ratio": round(distinct_ratio, 3),
                "missing_ratio": round(missing_ratio, 3),
                "reasons": ["composite key improves distinctness"],
            }
        )

    combined = top_candidates + combo_candidates
    combined.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    return combined[: max(0, int(max_candidates))]


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

            top_left = _select_candidate_columns(left_cols, left_quality)
            top_right = _select_candidate_columns(right_cols, right_quality)
            top_left, top_right = _extend_candidates_with_name_matches(
                left_cols,
                right_cols,
                top_left,
                top_right,
            )

            left_profiles = left.get("column_profiles") if isinstance(left.get("column_profiles"), dict) else {}
            right_profiles = right.get("column_profiles") if isinstance(right.get("column_profiles"), dict) else {}

            for col_a in top_left:
                left_profile = left_profiles.get(col_a) if isinstance(left_profiles, dict) else {}
                left_format = left_profile.get("format") if isinstance(left_profile, dict) else {}
                left_distinct = _distinct_ratio(
                    (left_stats.get("columns") or {}).get(col_a, {}) if isinstance(left_stats.get("columns"), dict) else {},
                    left_sample_n,
                )
                left_missing = _missing_ratio(
                    (left_stats.get("columns") or {}).get(col_a, {}) if isinstance(left_stats.get("columns"), dict) else {},
                    left_sample_n,
                )
                left_type = _type_family(
                    (left_stats.get("columns") or {}).get(col_a, {}) if isinstance(left_stats.get("columns"), dict) else {},
                    left_profile if isinstance(left_profile, dict) else {},
                )
                for col_b in top_right:
                    right_profile = right_profiles.get(col_b) if isinstance(right_profiles, dict) else {}
                    right_format = right_profile.get("format") if isinstance(right_profile, dict) else {}
                    name_sim = _name_similarity(col_a, col_b)
                    overlap_stats = _value_overlap_stats(left_rows, col_a, right_rows, col_b)
                    overlap = overlap_stats.get("overlap_ratio", 0.0)
                    left_containment = overlap_stats.get("left_containment", 0.0)
                    right_containment = overlap_stats.get("right_containment", 0.0)
                    quality = min(left_quality.get(col_a, 0.0), right_quality.get(col_b, 0.0))
                    right_distinct = _distinct_ratio(
                        (right_stats.get("columns") or {}).get(col_b, {}) if isinstance(right_stats.get("columns"), dict) else {},
                        right_sample_n,
                    )
                    right_missing = _missing_ratio(
                        (right_stats.get("columns") or {}).get(col_b, {}) if isinstance(right_stats.get("columns"), dict) else {},
                        right_sample_n,
                    )
                    right_type = _type_family(
                        (right_stats.get("columns") or {}).get(col_b, {}) if isinstance(right_stats.get("columns"), dict) else {},
                        right_profile if isinstance(right_profile, dict) else {},
                    )
                    format_sim = _format_similarity(left_format or {}, right_format or {})
                    type_sim = _type_compatibility(left_type, right_type)
                    score = 0.30 * name_sim + 0.30 * overlap + 0.20 * format_sim + 0.10 * type_sim + 0.10 * quality
                    if score < 0.5 or (overlap <= 0.0 and name_sim < 0.9 and format_sim < 0.7 and type_sim < 0.6):
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
                    if format_sim >= 0.6:
                        reasons.append("format patterns align")
                    if type_sim >= 0.8:
                        reasons.append("type families align")
                    elif type_sim <= 0.2:
                        reasons.append("type mismatch; cast may be needed")
                    if quality >= 0.7:
                        reasons.append("high distinctness / low missingness in sample")
                    cardinality = _estimate_cardinality(left_distinct, right_distinct)
                    if cardinality != "N:N":
                        reasons.append(f"cardinality hint {cardinality}")
                    sample_n = min(left_sample_n, right_sample_n)
                    cardinality_conf = _cardinality_confidence(sample_n)
                    if cardinality_conf < 0.6:
                        reasons.append("cardinality estimated from sample; may differ on full data")
                    candidates.append(
                        {
                            "left_dataset_id": left.get("dataset_id"),
                            "right_dataset_id": right.get("dataset_id"),
                            "left_dataset_name": left.get("name"),
                            "right_dataset_name": right.get("name"),
                            "left_column": col_a,
                            "right_column": col_b,
                            "left_columns": [col_a],
                            "right_columns": [col_b],
                            "score": round(float(score), 3),
                            "name_similarity": round(float(name_sim), 3),
                            "sample_value_overlap": round(float(overlap), 3),
                            "format_similarity": round(float(format_sim), 3),
                            "type_compatibility": round(float(type_sim), 3),
                            "left_type_family": left_type,
                            "right_type_family": right_type,
                            "left_distinct_ratio": round(float(left_distinct), 3),
                            "right_distinct_ratio": round(float(right_distinct), 3),
                            "left_missing_ratio": round(float(left_missing), 3),
                            "right_missing_ratio": round(float(right_missing), 3),
                            "left_containment_ratio": round(float(left_containment), 3),
                            "right_containment_ratio": round(float(right_containment), 3),
                            "cardinality_hint": cardinality,
                            "cardinality_confidence": round(float(cardinality_conf), 3),
                            "cardinality_note": "estimated from sample; may differ at full scale",
                            "sample_row_counts": {
                                "left": int(left_sample_n),
                                "right": int(right_sample_n),
                            },
                            "reasons": reasons,
                        }
                    )

            # composite (2-column) candidates
            left_pairs = list(combinations(top_left, 2))
            right_pairs = list(combinations(top_right, 2))
            for cols_a in left_pairs:
                left_distinct, left_missing = _composite_key_stats(left_rows, cols_a)
                left_quality_combo = min(left_quality.get(col, 0.0) for col in cols_a)
                left_type_a = _type_family(
                    (left_stats.get("columns") or {}).get(cols_a[0], {}) if isinstance(left_stats.get("columns"), dict) else {},
                    left_profiles.get(cols_a[0], {}) if isinstance(left_profiles, dict) else {},
                )
                left_type_b = _type_family(
                    (left_stats.get("columns") or {}).get(cols_a[1], {}) if isinstance(left_stats.get("columns"), dict) else {},
                    left_profiles.get(cols_a[1], {}) if isinstance(left_profiles, dict) else {},
                )
                for cols_b in right_pairs:
                    # align by best name similarity
                    sim_direct = (
                        _name_similarity(cols_a[0], cols_b[0]) + _name_similarity(cols_a[1], cols_b[1])
                    ) / 2.0
                    sim_swap = (
                        _name_similarity(cols_a[0], cols_b[1]) + _name_similarity(cols_a[1], cols_b[0])
                    ) / 2.0
                    if sim_swap > sim_direct:
                        aligned_b = (cols_b[1], cols_b[0])
                        name_sim = sim_swap
                    else:
                        aligned_b = cols_b
                        name_sim = sim_direct

                    right_distinct, right_missing = _composite_key_stats(right_rows, aligned_b)
                    right_quality_combo = min(right_quality.get(col, 0.0) for col in aligned_b)
                    right_type_a = _type_family(
                        (right_stats.get("columns") or {}).get(aligned_b[0], {}) if isinstance(right_stats.get("columns"), dict) else {},
                        right_profiles.get(aligned_b[0], {}) if isinstance(right_profiles, dict) else {},
                    )
                    right_type_b = _type_family(
                        (right_stats.get("columns") or {}).get(aligned_b[1], {}) if isinstance(right_stats.get("columns"), dict) else {},
                        right_profiles.get(aligned_b[1], {}) if isinstance(right_profiles, dict) else {},
                    )

                    overlap_stats = _composite_overlap_stats(left_rows, cols_a, right_rows, aligned_b)
                    overlap = overlap_stats.get("overlap_ratio", 0.0)
                    left_containment = overlap_stats.get("left_containment", 0.0)
                    right_containment = overlap_stats.get("right_containment", 0.0)
                    format_left = left_profiles if isinstance(left_profiles, dict) else {}
                    format_right = right_profiles if isinstance(right_profiles, dict) else {}
                    format_sim = (
                        _format_similarity(format_left.get(cols_a[0], {}).get("format", {}), format_right.get(aligned_b[0], {}).get("format", {}))
                        + _format_similarity(format_left.get(cols_a[1], {}).get("format", {}), format_right.get(aligned_b[1], {}).get("format", {}))
                    ) / 2.0
                    quality = min(left_quality_combo, right_quality_combo)
                    type_sim = (
                        _type_compatibility(left_type_a, right_type_a) + _type_compatibility(left_type_b, right_type_b)
                    ) / 2.0
                    score = 0.30 * name_sim + 0.30 * overlap + 0.20 * format_sim + 0.10 * type_sim + 0.10 * quality
                    if score < 0.5 or (overlap <= 0.0 and name_sim < 0.7 and format_sim < 0.6 and type_sim < 0.6):
                        continue
                    reasons: list[str] = ["composite key candidate"]
                    if name_sim >= 0.8:
                        reasons.append("column names align across pairs")
                    if overlap >= 0.4:
                        reasons.append("composite sample values overlap")
                    if format_sim >= 0.6:
                        reasons.append("format patterns align across pairs")
                    if type_sim >= 0.8:
                        reasons.append("type families align")
                    elif type_sim <= 0.2:
                        reasons.append("type mismatch; cast may be needed")
                    if quality >= 0.7:
                        reasons.append("high distinctness / low missingness in sample")
                    cardinality = _estimate_cardinality(left_distinct, right_distinct)
                    if cardinality != "N:N":
                        reasons.append(f"cardinality hint {cardinality}")
                    sample_n = min(left_sample_n, right_sample_n)
                    cardinality_conf = _cardinality_confidence(sample_n)
                    if cardinality_conf < 0.6:
                        reasons.append("cardinality estimated from sample; may differ on full data")
                    candidates.append(
                        {
                            "left_dataset_id": left.get("dataset_id"),
                            "right_dataset_id": right.get("dataset_id"),
                            "left_column": f"{cols_a[0]}+{cols_a[1]}",
                            "right_column": f"{aligned_b[0]}+{aligned_b[1]}",
                            "left_columns": list(cols_a),
                            "right_columns": list(aligned_b),
                            "score": round(float(score), 3),
                            "name_similarity": round(float(name_sim), 3),
                            "sample_value_overlap": round(float(overlap), 3),
                            "format_similarity": round(float(format_sim), 3),
                            "type_compatibility": round(float(type_sim), 3),
                            "left_type_families": [left_type_a, left_type_b],
                            "right_type_families": [right_type_a, right_type_b],
                            "left_distinct_ratio": round(float(left_distinct), 3),
                            "right_distinct_ratio": round(float(right_distinct), 3),
                            "left_missing_ratio": round(float(left_missing), 3),
                            "right_missing_ratio": round(float(right_missing), 3),
                            "left_containment_ratio": round(float(left_containment), 3),
                            "right_containment_ratio": round(float(right_containment), 3),
                            "cardinality_hint": cardinality,
                            "cardinality_confidence": round(float(cardinality_conf), 3),
                            "cardinality_note": "estimated from sample; may differ at full scale",
                            "sample_row_counts": {
                                "left": int(left_sample_n),
                                "right": int(right_sample_n),
                            },
                            "reasons": reasons,
                        }
                    )

    candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    _annotate_composite_hints(candidates)
    return candidates[: max(0, int(max_candidates))]


def _annotate_composite_hints(candidates: list[dict[str, Any]]) -> None:
    if not candidates:
        return
    single_scores: dict[tuple[str, str, str, str], float] = {}
    composite_groups_by_pair: dict[tuple[str, str, str, str], list[str]] = {}

    for candidate in candidates:
        left_id = str(candidate.get("left_dataset_id") or "").strip()
        right_id = str(candidate.get("right_dataset_id") or "").strip()
        left_cols = candidate.get("left_columns") or []
        right_cols = candidate.get("right_columns") or []
        if not left_id or not right_id:
            continue
        if len(left_cols) == 1 and len(right_cols) == 1:
            key = (left_id, right_id, str(left_cols[0]), str(right_cols[0]))
            single_scores[key] = float(candidate.get("score") or 0.0)

    for candidate in candidates:
        left_id = str(candidate.get("left_dataset_id") or "").strip()
        right_id = str(candidate.get("right_dataset_id") or "").strip()
        left_cols = candidate.get("left_columns") or []
        right_cols = candidate.get("right_columns") or []
        if not left_id or not right_id:
            continue
        if len(left_cols) > 1 or len(right_cols) > 1:
            group_id = f"cmp:{left_id}:{right_id}:{'+'.join(left_cols)}::{'+'.join(right_cols)}"
            candidate["composite_key"] = True
            candidate["composite_group_id"] = group_id
            for idx in range(min(len(left_cols), len(right_cols))):
                pair_key = (left_id, right_id, str(left_cols[idx]), str(right_cols[idx]))
                composite_groups_by_pair.setdefault(pair_key, []).append(group_id)

    for candidate in candidates:
        left_id = str(candidate.get("left_dataset_id") or "").strip()
        right_id = str(candidate.get("right_dataset_id") or "").strip()
        left_cols = candidate.get("left_columns") or []
        right_cols = candidate.get("right_columns") or []
        if not left_id or not right_id:
            continue
        if len(left_cols) == 1 and len(right_cols) == 1:
            candidate.setdefault("composite_key", False)
            pair_key = (left_id, right_id, str(left_cols[0]), str(right_cols[0]))
            group_ids = composite_groups_by_pair.get(pair_key)
            if group_ids:
                candidate["composite_group_ids"] = group_ids
            continue
        if len(left_cols) > 1 or len(right_cols) > 1:
            component_scores: list[dict[str, Any]] = []
            for idx in range(min(len(left_cols), len(right_cols))):
                left_col = str(left_cols[idx])
                right_col = str(right_cols[idx])
                key = (left_id, right_id, left_col, right_col)
                score = single_scores.get(key)
                if score is None:
                    reverse_key = (right_id, left_id, right_col, left_col)
                    score = single_scores.get(reverse_key)
                component_scores.append(
                    {
                        "left_column": left_col,
                        "right_column": right_col,
                        "score": round(float(score), 3) if score is not None else None,
                    }
                )
            candidate["component_scores"] = component_scores


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


def _build_column_profiles(
    *,
    columns: list[dict[str, Any]],
    column_stats: dict[str, Any],
    sample_rows: list[dict[str, Any]],
) -> Dict[str, Any]:
    profiles: Dict[str, Any] = {}
    sample_row_count = int(column_stats.get("sample_row_count") or 0)
    stats_by_col = column_stats.get("columns") if isinstance(column_stats.get("columns"), dict) else {}
    for col in columns:
        name = str(col.get("name") or "").strip()
        if not name:
            continue
        col_stats = stats_by_col.get(name) if isinstance(stats_by_col, dict) else {}
        values = [row.get(name) for row in sample_rows if isinstance(row, dict)]
        distinct_ratio = _distinct_ratio(col_stats or {}, sample_row_count)
        missing_ratio = _missing_ratio(col_stats or {}, sample_row_count)
        null_count = int((col_stats or {}).get("null_count") or 0)
        null_ratio = (null_count / float(sample_row_count)) if sample_row_count else 0.0
        profiles[name] = {
            "distinct_ratio": round(distinct_ratio, 3),
            "missing_ratio": round(missing_ratio, 3),
            "duplicate_ratio": round(max(0.0, 1.0 - distinct_ratio), 3),
            "null_ratio": round(null_ratio, 3),
            "format": _column_format_profile(values),
        }
    return profiles


async def build_pipeline_context_pack(
    *,
    db_name: str,
    branch: Optional[str],
    dataset_ids: Optional[Sequence[str]],
    dataset_registry: DatasetRegistry,
    profile_registry: Optional[DatasetProfileRegistry] = None,
    max_datasets_overview: int = 20,
    max_selected_datasets: int = 6,
    max_sample_rows: int = 20,
    max_join_candidates: int = 10,
    max_pk_candidates: int = 6,
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
    requested_count = len(selected_ids)
    max_selected = max(0, int(max_selected_datasets))
    if requested_count:
        max_selected = max(max_selected, requested_count)

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
    pk_scores: dict[tuple[str, str], float] = {}
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
        column_profiles = _build_column_profiles(columns=cols, column_stats=raw_stats, sample_rows=raw_rows)
        pk_candidates = _suggest_primary_keys(
            columns=cols,
            column_stats=raw_stats,
            sample_rows=raw_rows,
            max_candidates=max_pk_candidates,
        )
        for candidate in pk_candidates:
            cols_candidate = candidate.get("columns") or []
            if len(cols_candidate) == 1:
                col = str(cols_candidate[0])
                pk_scores[(dataset_id, col)] = float(candidate.get("score") or 0.0)
        schema_hash = compute_schema_hash(cols) if cols else None
        dataset_version_id = _normalize_uuid(item.get("dataset_version_id"))
        selected.append(
            {
                "dataset_id": dataset_id,
                "dataset_version_id": dataset_version_id,
                "name": item.get("name"),
                "branch": item.get("branch"),
                "source_type": item.get("source_type"),
                "description": item.get("description"),
                "latest_commit_id": item.get("latest_commit_id"),
                "row_count": item.get("row_count"),
                "columns": cols,
                "sample_rows": masked_rows,
                "column_stats": masked_stats,
                "column_profiles": column_profiles,
                "pk_candidates": pk_candidates,
                "schema_hash": schema_hash,
                "_raw_rows": raw_rows,
                "_raw_column_stats": raw_stats,
            }
        )
        if profile_registry and dataset_version_id:
            profile_payload = {
                "dataset_id": dataset_id,
                "dataset_version_id": dataset_version_id,
                "schema_hash": schema_hash,
                "column_stats": raw_stats,
                "column_profiles": column_profiles,
                "pk_candidates": pk_candidates,
                "dataset_signature": sha256_canonical_json_prefixed(
                    {
                        "name": item.get("name"),
                        "schema_hash": schema_hash,
                        "branch": item.get("branch"),
                    }
                ),
            }
            try:
                await profile_registry.upsert_profile(
                    dataset_id=dataset_id,
                    dataset_version_id=dataset_version_id,
                    db_name=db_name,
                    branch=str(item.get("branch") or "").strip() or None,
                    schema_hash=schema_hash,
                    profile=profile_payload,
                )
            except Exception as exc:
                logger.warning("Failed to persist dataset profile: %s", exc)
        if len(selected) >= max_selected:
            break

    join_candidates = _suggest_join_keys(selected, max_candidates=max_join_candidates) if len(selected) >= 2 else []
    fk_candidates: list[dict[str, Any]] = []
    for candidate in join_candidates:
        left_id = str(candidate.get("left_dataset_id") or "")
        right_id = str(candidate.get("right_dataset_id") or "")
        left_col = str(candidate.get("left_column") or "")
        right_col = str(candidate.get("right_column") or "")
        left_cols = candidate.get("left_columns") or []
        right_cols = candidate.get("right_columns") or []
        if len(left_cols) > 1 or len(right_cols) > 1:
            continue
        if not left_col and len(left_cols) == 1:
            left_col = str(left_cols[0])
        if not right_col and len(right_cols) == 1:
            right_col = str(right_cols[0])
        left_score = pk_scores.get((left_id, left_col), 0.0)
        right_score = pk_scores.get((right_id, right_col), 0.0)
        left_containment = float(candidate.get("left_containment_ratio") or 0.0)
        right_containment = float(candidate.get("right_containment_ratio") or 0.0)
        containment_best = max(left_containment, right_containment)
        if left_score <= 0 and right_score <= 0 and containment_best < 0.6:
            continue
        if left_score >= right_score and left_score > 0:
            parent_id, parent_col = left_id, left_col
            child_id, child_col = right_id, right_col
        elif right_score > 0:
            parent_id, parent_col = right_id, right_col
            child_id, child_col = left_id, left_col
        elif left_containment >= right_containment:
            parent_id, parent_col = right_id, right_col
            child_id, child_col = left_id, left_col
        else:
            parent_id, parent_col = left_id, left_col
            child_id, child_col = right_id, right_col
        fk_candidates.append(
            {
                "child_dataset_id": child_id,
                "child_column": child_col,
                "parent_dataset_id": parent_id,
                "parent_column": parent_col,
                "score": round(float(candidate.get("score") or 0.0) * max(left_score, right_score, containment_best), 3),
                "containment_ratio": round(containment_best, 3),
                "cardinality_hint": candidate.get("cardinality_hint"),
                "reasons": candidate.get("reasons") or [],
            }
        )
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
            "foreign_key_candidates": fk_candidates,
            "cleansing_suggestions": cleansing_suggestions,
        },
        "pipeline_definition_hints": {
            "supported_operations": sorted(SUPPORTED_TRANSFORMS),
            "input_node_metadata": {
                "dataset_id": "UUID of dataset (aka datasetId)",
                "dataset_name": "dataset name (aka datasetName, fallback if id unavailable)",
                "dataset_branch": "lakeFS branch (aka datasetBranch, default to plan scope branch/main)",
            },
            "transform_node_metadata": {
                "operation": "one of supported_operations",
            },
            "note": "Keep definitions sample-safe; avoid UDF. Use joins/unions + cleansing ops (rename/cast/dedupe/filter/compute).",
        },
    }
