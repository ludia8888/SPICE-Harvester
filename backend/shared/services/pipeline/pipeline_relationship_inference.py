from __future__ import annotations

"""
Deterministic relationship / join-chain inference.

Goal:
- Provide planner/join strategist with a small, structured "candidate space" for joining many datasets.
- Use only context pack signals (sample-safe) and avoid any side effects.

Enterprise Enhancement (2026-01):
Added warnings for common join key selection pitfalls:
- Name-only FK matching without value overlap verification
- High NULL ratio columns used as join keys
- Improved join type recommendations based on cardinality and containment
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# Enterprise Enhancement (2026-01): Thresholds for join key quality warnings
_HIGH_NULL_RATIO_THRESHOLD = 0.3  # >30% NULL is concerning for join keys
_LOW_CONTAINMENT_THRESHOLD = 0.5  # <50% containment suggests potential data loss
_NAME_ONLY_MATCH_PATTERNS = {"_id", "id", "_key", "key", "_code", "code", "_no", "no"}


def _as_str(value: Any) -> str:
    return str(value or "").strip()


def _normalize_key_list(value: Any) -> List[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    output: List[str] = []
    for item in items:
        raw = _as_str(item)
        if not raw:
            continue
        # Support "+"" (composite marker) as well as comma-separated values.
        parts = [part.strip() for part in raw.replace("+", ",").split(",") if part.strip()]
        output.extend(parts if parts else [raw])
    # Preserve order, de-dupe (case-insensitive).
    seen: set[str] = set()
    deduped: List[str] = []
    for col in output:
        lowered = col.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(col)
    return deduped


def _filter_selected_datasets(context_pack: Dict[str, Any], dataset_ids: Optional[List[str]]) -> List[Dict[str, Any]]:
    selected = context_pack.get("selected_datasets")
    if not isinstance(selected, list):
        return []
    wanted = {_as_str(item) for item in (dataset_ids or []) if _as_str(item)}
    out: List[Dict[str, Any]] = []
    for item in selected:
        if not isinstance(item, dict):
            continue
        ds_id = _as_str(item.get("dataset_id"))
        if not ds_id:
            continue
        if wanted and ds_id not in wanted:
            continue
        out.append(item)
    return out


def _build_pk_score_index(selected_datasets: List[Dict[str, Any]]) -> Dict[Tuple[str, str], float]:
    """
    Index single-column PK candidate scores.
    Key: (dataset_id, column_lower) -> score
    """
    scores: Dict[Tuple[str, str], float] = {}
    for ds in selected_datasets:
        ds_id = _as_str(ds.get("dataset_id"))
        if not ds_id:
            continue
        pk_candidates = ds.get("pk_candidates") if isinstance(ds.get("pk_candidates"), list) else []
        for cand in pk_candidates:
            if not isinstance(cand, dict):
                continue
            cols = cand.get("columns")
            if not isinstance(cols, list) or len(cols) != 1:
                continue
            col = _as_str(cols[0])
            if not col:
                continue
            try:
                score = float(cand.get("score") or 0.0)
            except Exception:
                score = 0.0
            scores[(ds_id, col.lower())] = max(scores.get((ds_id, col.lower()), 0.0), score)
    return scores


def _build_null_ratio_index(selected_datasets: List[Dict[str, Any]]) -> Dict[Tuple[str, str], float]:
    """
    Enterprise Enhancement (2026-01):
    Build index of NULL ratios for columns to warn about high-NULL join keys.
    Key: (dataset_id, column_lower) -> null_ratio
    """
    ratios: Dict[Tuple[str, str], float] = {}
    for ds in selected_datasets:
        ds_id = _as_str(ds.get("dataset_id"))
        if not ds_id:
            continue
        schema = ds.get("schema") if isinstance(ds.get("schema"), list) else []
        for col_info in schema:
            if not isinstance(col_info, dict):
                continue
            col_name = _as_str(col_info.get("name"))
            if not col_name:
                continue
            try:
                null_ratio = float(col_info.get("null_ratio") or col_info.get("nullRatio") or 0.0)
            except (TypeError, ValueError):
                null_ratio = 0.0
            ratios[(ds_id, col_name.lower())] = null_ratio
    return ratios


def _is_name_only_match(reasons: List[str]) -> bool:
    """
    Enterprise Enhancement (2026-01):
    Detect if FK candidate is based purely on column name matching.
    """
    if not reasons:
        return True  # No reasons means likely name-based

    name_related_keywords = {"name", "match", "similar", "suffix", "prefix", "pattern"}
    value_related_keywords = {"overlap", "containment", "value", "cardinality", "unique", "distinct"}

    has_name_reason = False
    has_value_reason = False

    for reason in reasons:
        reason_lower = reason.lower()
        if any(kw in reason_lower for kw in name_related_keywords):
            has_name_reason = True
        if any(kw in reason_lower for kw in value_related_keywords):
            has_value_reason = True

    # If only name-based reasons and no value-based reasons, it's name-only
    return has_name_reason and not has_value_reason


def _analyze_join_characteristics(
    *,
    cardinality_hint: Optional[str],
    child_containment: Optional[float],
    parent_containment: Optional[float],
    child_null_ratio: float,
    parent_null_ratio: float,
) -> Dict[str, Any]:
    """
    Raw Data-Centric (2026-01):
    Analyze join characteristics and return OBSERVATIONS, not recommendations.
    LLM should decide join type based on these observations + domain knowledge.

    Returns observations dict with:
    - metrics: raw numbers
    - observations: list of factual statements
    - considerations: things to think about when choosing join type
    """
    card = (cardinality_hint or "").upper()

    observations: List[str] = []
    considerations: List[str] = []

    # NULL ratio observations
    if parent_null_ratio > _HIGH_NULL_RATIO_THRESHOLD:
        observations.append(f"Parent key has {parent_null_ratio*100:.1f}% NULL values")
        considerations.append("NULLs in parent key will not match any child rows")

    if child_null_ratio > _HIGH_NULL_RATIO_THRESHOLD:
        observations.append(f"Child key has {child_null_ratio*100:.1f}% NULL values")
        considerations.append("NULL child keys will not match; rows may be lost in inner join")

    # Cardinality observations
    if card == "1:1":
        observations.append("Cardinality appears to be 1:1 (both sides unique)")
        considerations.append("1:1: inner join typically safe if containment is high")
    elif card == "N:1":
        observations.append("Cardinality appears to be N:1 (child has duplicates, parent unique)")
        considerations.append("N:1: typical FK relationship; check if all child keys exist in parent")
    elif card == "1:N":
        observations.append("Cardinality appears to be 1:N (child unique, parent has duplicates)")
        considerations.append("1:N: unusual for FK - verify direction is correct")
    elif card in {"M:N", "N:M"}:
        observations.append("Cardinality appears to be M:N (both sides have duplicates)")
        considerations.append("M:N: may need intermediate/junction table for proper relationship")

    # Containment observations
    if child_containment is not None:
        observations.append(f"Child→Parent containment: {child_containment*100:.1f}%")
        if child_containment >= 0.95:
            considerations.append("High containment: most child keys exist in parent")
        elif child_containment < _LOW_CONTAINMENT_THRESHOLD:
            considerations.append(f"Low containment: {(1-child_containment)*100:.1f}% of child keys missing from parent")
            considerations.append("Inner join would lose these non-matching rows")

    if parent_containment is not None:
        observations.append(f"Parent→Child containment: {parent_containment*100:.1f}%")

    # Join type considerations (not recommendations)
    join_considerations = {
        "inner": "Keeps only matching rows from both sides",
        "left": "Keeps all left-side rows; non-matching get NULL for right columns",
        "right": "Keeps all right-side rows; non-matching get NULL for left columns",
        "outer": "Keeps all rows from both sides; NULLs where no match",
    }

    return {
        "metrics": {
            "cardinality_hint": card or None,
            "child_containment": round(child_containment, 3) if child_containment else None,
            "parent_containment": round(parent_containment, 3) if parent_containment else None,
            "child_null_ratio": round(child_null_ratio, 3),
            "parent_null_ratio": round(parent_null_ratio, 3),
        },
        "observations": observations,
        "considerations": considerations,
        "join_type_reference": join_considerations,
        "agent_instruction": (
            "Based on these observations and your understanding of the business logic, "
            "decide which join type is appropriate. Consider: "
            "Do you need all rows from one side? Can you afford to lose non-matching rows?"
        ),
    }


def _recommend_join_type(
    *,
    cardinality_hint: Optional[str],
    child_containment: Optional[float],
    parent_containment: Optional[float],
    child_null_ratio: float,
    parent_null_ratio: float,
) -> Tuple[str, str]:
    """
    Legacy function for backwards compatibility.
    Returns (join_type, observation_note).

    Note: This is kept for compatibility but the observation-based approach
    (_analyze_join_characteristics) is preferred for LLM decision-making.
    """
    card = (cardinality_hint or "").upper()

    # Build observation-based response (not prescriptive)
    if parent_null_ratio > _HIGH_NULL_RATIO_THRESHOLD:
        return "left", f"observation: parent key has {parent_null_ratio*100:.1f}% NULLs"

    if child_null_ratio > _HIGH_NULL_RATIO_THRESHOLD:
        return "left", f"observation: child key has {child_null_ratio*100:.1f}% NULLs"

    if card == "1:1":
        if child_containment and child_containment >= 0.95:
            return "inner", "observation: 1:1 with high containment"
        return "left", "observation: 1:1 but containment not verified"

    if card == "N:1":
        if child_containment and child_containment >= 0.95:
            return "inner", "observation: N:1 with high containment"
        if child_containment and child_containment < _LOW_CONTAINMENT_THRESHOLD:
            return "left", f"observation: only {child_containment*100:.1f}% containment"
        return "left", "observation: N:1 relationship"

    if card == "1:N":
        return "left", "observation: 1:N detected - verify FK direction"

    if card in {"M:N", "N:M"}:
        return "left", "observation: M:N relationship"

    if child_containment is not None:
        if child_containment >= 0.95:
            return "inner", f"observation: {child_containment*100:.1f}% containment"
        if child_containment < _LOW_CONTAINMENT_THRESHOLD:
            return "left", f"observation: {child_containment*100:.1f}% containment"

    return "left", "observation: default (no strong signal)"


@dataclass(frozen=True)
class _Edge:
    child_dataset_id: str
    parent_dataset_id: str
    child_keys: Tuple[str, ...]
    parent_keys: Tuple[str, ...]
    score: float
    cardinality_hint: Optional[str]
    containment_ratio: Optional[float]
    reasons: Tuple[str, ...]
    source: str  # foreign_key_candidates|join_key_candidates
    # Enterprise Enhancement (2026-01): Additional quality signals
    child_null_ratio: float = 0.0
    parent_null_ratio: float = 0.0
    is_name_only_match: bool = False
    quality_warnings: Tuple[str, ...] = ()


class _UnionFind:
    def __init__(self, items: Iterable[str]) -> None:
        self._parent: Dict[str, str] = {}
        self._rank: Dict[str, int] = {}
        for item in items:
            self._parent[item] = item
            self._rank[item] = 0

    def find(self, x: str) -> str:
        parent = self._parent.get(x, x)
        if parent != x:
            parent = self.find(parent)
            self._parent[x] = parent
        return parent

    def union(self, a: str, b: str) -> bool:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return False
        rank_a = self._rank.get(ra, 0)
        rank_b = self._rank.get(rb, 0)
        if rank_a < rank_b:
            self._parent[ra] = rb
        elif rank_a > rank_b:
            self._parent[rb] = ra
        else:
            self._parent[rb] = ra
            self._rank[ra] = rank_a + 1
        return True

    def components(self) -> Dict[str, List[str]]:
        groups: Dict[str, List[str]] = {}
        for item in list(self._parent.keys()):
            root = self.find(item)
            groups.setdefault(root, []).append(item)
        return groups


def infer_keys_from_context_pack(
    context_pack: Dict[str, Any],
    *,
    dataset_ids: Optional[List[str]] = None,
    max_pk_candidates: int = 6,
    max_fk_candidates: int = 25,
) -> Dict[str, Any]:
    """
    Deterministic PK/FK inference wrapper (planner-friendly structure).

    Enterprise Enhancement (2026-01):
    Added quality warnings for FK candidates based on:
    - Name-only matching (no value overlap verification)
    - High NULL ratio in join columns
    - Low containment ratios

    This is intentionally "thin": it primarily re-exports context pack suggestions
    and enriches them with a few normalized fields.
    """
    selected = _filter_selected_datasets(context_pack, dataset_ids=dataset_ids)

    # Enterprise Enhancement: Build NULL ratio index for quality warnings
    null_ratios = _build_null_ratio_index(selected)

    pk: List[Dict[str, Any]] = []
    for ds in selected:
        pk_candidates = ds.get("pk_candidates") if isinstance(ds.get("pk_candidates"), list) else []
        trimmed = [item for item in pk_candidates[: max(0, int(max_pk_candidates))] if isinstance(item, dict)]
        best_pk = trimmed[0] if trimmed else None
        pk.append(
            {
                "dataset_id": ds.get("dataset_id"),
                "name": ds.get("name"),
                "row_count": ds.get("row_count"),
                "pk_candidates": trimmed,
                "best_pk": best_pk,
            }
        )

    suggestions = context_pack.get("integration_suggestions")
    if not isinstance(suggestions, dict):
        suggestions = {}
    fk_candidates = suggestions.get("foreign_key_candidates")
    if not isinstance(fk_candidates, list):
        fk_candidates = []
    fk_candidates = [item for item in fk_candidates if isinstance(item, dict)]
    fk_candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)

    wanted = {_as_str(item) for item in (dataset_ids or []) if _as_str(item)}
    fk_out: List[Dict[str, Any]] = []
    quality_warnings: List[str] = []

    for item in fk_candidates[: max(0, int(max_fk_candidates))]:
        child_id = _as_str(item.get("child_dataset_id"))
        parent_id = _as_str(item.get("parent_dataset_id"))
        if not child_id or not parent_id:
            continue
        if wanted and (child_id not in wanted or parent_id not in wanted):
            continue
        child_col = _as_str(item.get("child_column"))
        parent_col = _as_str(item.get("parent_column"))

        # Enterprise Enhancement: Check for quality issues
        item_warnings: List[str] = []

        # Check for name-only match
        reasons = item.get("reasons") if isinstance(item.get("reasons"), list) else []
        if _is_name_only_match([_as_str(r) for r in reasons]):
            item_warnings.append(
                f"NAME-ONLY MATCH: FK {child_col} -> {parent_col} based on column name similarity only. "
                "Verify actual value overlap before using."
            )

        # Check NULL ratios
        child_null = null_ratios.get((child_id, child_col.lower()), 0.0)
        parent_null = null_ratios.get((parent_id, parent_col.lower()), 0.0)

        if child_null > _HIGH_NULL_RATIO_THRESHOLD:
            item_warnings.append(
                f"HIGH NULL RATIO: Child column '{child_col}' has {child_null*100:.1f}% NULL values. "
                "NULLs will not match in joins."
            )
        if parent_null > _HIGH_NULL_RATIO_THRESHOLD:
            item_warnings.append(
                f"HIGH NULL RATIO: Parent column '{parent_col}' has {parent_null*100:.1f}% NULL values. "
                "Consider data quality issues."
            )

        # Check containment
        containment = item.get("containment_ratio")
        if containment is not None:
            try:
                cont_val = float(containment)
                if cont_val < _LOW_CONTAINMENT_THRESHOLD:
                    item_warnings.append(
                        f"LOW CONTAINMENT: Only {cont_val*100:.1f}% of child values exist in parent. "
                        f"Inner join would lose {(1-cont_val)*100:.1f}% of child rows."
                    )
            except (TypeError, ValueError):
                pass

        # Raw Data-Centric: Build join analysis (observations, not recommendations)
        cardinality = _as_str(item.get("cardinality_hint"))
        containment_val = float(item.get("containment_ratio") or 0.0) if item.get("containment_ratio") else None

        # Get detailed join analysis for LLM
        join_analysis = _analyze_join_characteristics(
            cardinality_hint=cardinality,
            child_containment=containment_val,
            parent_containment=None,
            child_null_ratio=child_null,
            parent_null_ratio=parent_null,
        )

        # Legacy: keep join_type for backwards compatibility
        join_type, join_reason = _recommend_join_type(
            cardinality_hint=cardinality,
            child_containment=containment_val,
            parent_containment=None,
            child_null_ratio=child_null,
            parent_null_ratio=parent_null,
        )

        fk_entry = {
            **item,
            "child_keys": _normalize_key_list(child_col) if child_col else [],
            "parent_keys": _normalize_key_list(parent_col) if parent_col else [],
            "direction": "child_to_parent",
            # Raw Data-Centric: Analysis for LLM to interpret
            "join_analysis": join_analysis,
            # Legacy fields for backwards compatibility
            "recommended_join_type": join_type,
            "join_type_reason": join_reason,
        }

        if item_warnings:
            fk_entry["quality_warnings"] = item_warnings
            quality_warnings.extend(item_warnings)

        fk_out.append(fk_entry)

    notes = ["inference is sample-based; validate keys before using for canonical mappings"]
    if quality_warnings:
        notes.append(f"QUALITY CONCERNS: {len(quality_warnings)} warning(s) found. Review before proceeding.")

    return {
        "primary_keys": pk,
        "foreign_keys": fk_out,
        "quality_warnings": quality_warnings,
        "notes": notes,
    }


def infer_join_plan_from_context_pack(
    context_pack: Dict[str, Any],
    *,
    dataset_ids: Optional[List[str]] = None,
    max_joins: int = 12,
    max_edges: int = 30,
) -> Dict[str, Any]:
    """
    Infer a "best-effort" join plan (spanning tree) from context pack candidates.

    Enterprise Enhancement (2026-01):
    Added quality warnings and improved join type recommendations based on:
    - NULL ratio analysis
    - Containment ratio thresholds
    - Cardinality hints

    Output join_plan items are shaped like PipelineJoinSelection / PipelinePlanAssociation-compatible dicts.
    """
    selected = _filter_selected_datasets(context_pack, dataset_ids=dataset_ids)
    ds_ids = [_as_str(ds.get("dataset_id")) for ds in selected if _as_str(ds.get("dataset_id"))]
    wanted = set(ds_ids)
    datasets = [
        {"dataset_id": ds.get("dataset_id"), "name": ds.get("name"), "row_count": ds.get("row_count")}
        for ds in selected
    ]

    suggestions = context_pack.get("integration_suggestions")
    if not isinstance(suggestions, dict):
        suggestions = {}

    pk_scores = _build_pk_score_index(selected)
    # Enterprise Enhancement: Build NULL ratio index
    null_ratios = _build_null_ratio_index(selected)

    edges: Dict[Tuple[str, str, Tuple[str, ...], Tuple[str, ...]], _Edge] = {}

    # 1) Prefer directed FK candidates (already oriented child->parent).
    fk_candidates = suggestions.get("foreign_key_candidates")
    if isinstance(fk_candidates, list):
        for cand in fk_candidates:
            if not isinstance(cand, dict):
                continue
            child_id = _as_str(cand.get("child_dataset_id"))
            parent_id = _as_str(cand.get("parent_dataset_id"))
            if not child_id or not parent_id:
                continue
            if wanted and (child_id not in wanted or parent_id not in wanted):
                continue
            child_col = _as_str(cand.get("child_column"))
            parent_col = _as_str(cand.get("parent_column"))
            child_keys = tuple(_normalize_key_list(child_col))
            parent_keys = tuple(_normalize_key_list(parent_col))
            if not child_keys or not parent_keys:
                continue
            try:
                score = float(cand.get("score") or 0.0)
            except Exception:
                score = 0.0
            key = (child_id, parent_id, child_keys, parent_keys)
            reasons = cand.get("reasons") if isinstance(cand.get("reasons"), list) else []

            # Enterprise Enhancement: Get NULL ratios for quality assessment
            child_null = max(
                (null_ratios.get((child_id, k.lower()), 0.0) for k in child_keys),
                default=0.0
            )
            parent_null = max(
                (null_ratios.get((parent_id, k.lower()), 0.0) for k in parent_keys),
                default=0.0
            )

            # Build quality warnings
            edge_warnings: List[str] = []
            reason_strs = [_as_str(r) for r in reasons if _as_str(r)]

            if _is_name_only_match(reason_strs):
                edge_warnings.append("name-only match (no value verification)")
            if child_null > _HIGH_NULL_RATIO_THRESHOLD:
                edge_warnings.append(f"child key has {child_null*100:.0f}% NULLs")
            if parent_null > _HIGH_NULL_RATIO_THRESHOLD:
                edge_warnings.append(f"parent key has {parent_null*100:.0f}% NULLs")

            edge = _Edge(
                child_dataset_id=child_id,
                parent_dataset_id=parent_id,
                child_keys=child_keys,
                parent_keys=parent_keys,
                score=score,
                cardinality_hint=_as_str(cand.get("cardinality_hint")) or None,
                containment_ratio=(float(cand.get("containment_ratio")) if cand.get("containment_ratio") is not None else None),
                reasons=tuple(reason_strs)[:6],
                source="foreign_key_candidates",
                child_null_ratio=child_null,
                parent_null_ratio=parent_null,
                is_name_only_match=_is_name_only_match(reason_strs),
                quality_warnings=tuple(edge_warnings),
            )
            existing = edges.get(key)
            if existing is None or edge.score > existing.score:
                edges[key] = edge

    # 2) Supplement with join-key candidates (undirected) when FK candidates are missing.
    join_candidates = suggestions.get("join_key_candidates")
    if isinstance(join_candidates, list):
        for cand in join_candidates:
            if not isinstance(cand, dict):
                continue
            left_id = _as_str(cand.get("left_dataset_id"))
            right_id = _as_str(cand.get("right_dataset_id"))
            if not left_id or not right_id:
                continue
            if wanted and (left_id not in wanted or right_id not in wanted):
                continue

            left_keys = _normalize_key_list(cand.get("left_columns") or cand.get("left_column"))
            right_keys = _normalize_key_list(cand.get("right_columns") or cand.get("right_column"))
            if not left_keys or not right_keys:
                continue

            try:
                score = float(cand.get("score") or 0.0)
            except Exception:
                score = 0.0

            card = _as_str(cand.get("cardinality_hint")).upper()
            left_cont = float(cand.get("left_containment_ratio") or 0.0)
            right_cont = float(cand.get("right_containment_ratio") or 0.0)

            # Orientation heuristic: prefer cardinality hint, then containment, then PK score.
            child_id, parent_id = left_id, right_id
            child_keys, parent_keys = left_keys, right_keys
            child_cont = left_cont

            if card == "N:1":
                child_id, parent_id = left_id, right_id
                child_keys, parent_keys = left_keys, right_keys
                child_cont = left_cont
            elif card == "1:N":
                child_id, parent_id = right_id, left_id
                child_keys, parent_keys = right_keys, left_keys
                child_cont = right_cont
            elif left_cont or right_cont:
                if right_cont > left_cont:
                    child_id, parent_id = right_id, left_id
                    child_keys, parent_keys = right_keys, left_keys
                    child_cont = right_cont
            else:
                left_pk = max((pk_scores.get((left_id, key.lower()), 0.0) for key in left_keys), default=0.0)
                right_pk = max((pk_scores.get((right_id, key.lower()), 0.0) for key in right_keys), default=0.0)
                if left_pk > right_pk:
                    child_id, parent_id = right_id, left_id
                    child_keys, parent_keys = right_keys, left_keys
                    child_cont = right_cont

            key = (child_id, parent_id, tuple(child_keys), tuple(parent_keys))
            reasons = cand.get("reasons") if isinstance(cand.get("reasons"), list) else []

            # Enterprise Enhancement: Get NULL ratios for quality assessment
            child_null = max(
                (null_ratios.get((child_id, k.lower()), 0.0) for k in child_keys),
                default=0.0
            )
            parent_null = max(
                (null_ratios.get((parent_id, k.lower()), 0.0) for k in parent_keys),
                default=0.0
            )

            # Build quality warnings
            edge_warnings: List[str] = []
            reason_strs = [_as_str(r) for r in reasons if _as_str(r)]

            if _is_name_only_match(reason_strs):
                edge_warnings.append("name-only match (no value verification)")
            if child_null > _HIGH_NULL_RATIO_THRESHOLD:
                edge_warnings.append(f"child key has {child_null*100:.0f}% NULLs")
            if parent_null > _HIGH_NULL_RATIO_THRESHOLD:
                edge_warnings.append(f"parent key has {parent_null*100:.0f}% NULLs")

            edge = _Edge(
                child_dataset_id=child_id,
                parent_dataset_id=parent_id,
                child_keys=tuple(child_keys),
                parent_keys=tuple(parent_keys),
                score=score,
                cardinality_hint=_as_str(cand.get("cardinality_hint")) or None,
                containment_ratio=child_cont if (left_cont or right_cont) else None,
                reasons=tuple(reason_strs)[:6],
                source="join_key_candidates",
                child_null_ratio=child_null,
                parent_null_ratio=parent_null,
                is_name_only_match=_is_name_only_match(reason_strs),
                quality_warnings=tuple(edge_warnings),
            )
            existing = edges.get(key)
            if existing is None or edge.score > existing.score:
                edges[key] = edge

    edge_list = sorted(edges.values(), key=lambda e: float(e.score), reverse=True)
    edge_list = edge_list[: max(0, int(max_edges))]

    if len(wanted) < 2:
        return {
            "datasets": datasets,
            "edges": [],
            "join_plan": [],
            "warnings": [],
            "notes": ["join plan inference skipped (need >=2 datasets)"],
        }

    uf = _UnionFind(wanted)
    join_plan: List[Dict[str, Any]] = []
    quality_warnings: List[str] = []

    for edge in edge_list:
        if len(join_plan) >= max(0, int(max_joins)):
            break
        if not uf.union(edge.child_dataset_id, edge.parent_dataset_id):
            continue

        reason = ", ".join([r for r in edge.reasons if r]) or f"from {edge.source}"

        # Enterprise Enhancement: Use intelligent join type recommendation
        join_type, join_reason = _recommend_join_type(
            cardinality_hint=edge.cardinality_hint,
            child_containment=edge.containment_ratio,
            parent_containment=None,
            child_null_ratio=edge.child_null_ratio,
            parent_null_ratio=edge.parent_null_ratio,
        )

        plan_entry = {
            "left_dataset_id": edge.child_dataset_id,
            "right_dataset_id": edge.parent_dataset_id,
            "left_column": edge.child_keys[0],
            "right_column": edge.parent_keys[0],
            "left_columns": list(edge.child_keys),
            "right_columns": list(edge.parent_keys),
            "join_type": join_type,
            "join_type_reason": join_reason,
            "cardinality": edge.cardinality_hint,
            "confidence": max(0.0, min(1.0, float(edge.score))),
            "reason": reason,
        }

        # Add quality warnings if present
        if edge.quality_warnings:
            plan_entry["quality_warnings"] = list(edge.quality_warnings)
            for w in edge.quality_warnings:
                quality_warnings.append(
                    f"Join {edge.child_dataset_id} -> {edge.parent_dataset_id}: {w}"
                )

        join_plan.append(plan_entry)

    components = uf.components()
    warnings: List[str] = []
    if len(components) > 1:
        unreachable = sorted([item for group in components.values() for item in group])
        warnings.append(f"join graph disconnected in sample; components={len(components)} datasets={unreachable}")

    # Enterprise Enhancement: Add quality warnings summary
    if quality_warnings:
        warnings.append(
            f"QUALITY CONCERNS: {len(quality_warnings)} issue(s) found in join plan. "
            "Review warnings before using this plan."
        )

    notes = ["join plan inference is sample-based; treat as a suggestion only"]
    if any(e.is_name_only_match for e in edge_list):
        notes.append(
            "WARNING: Some joins are based on column name matching only. "
            "Verify actual value overlap before production use."
        )

    return {
        "datasets": datasets,
        "edges": [
            {
                "child_dataset_id": e.child_dataset_id,
                "parent_dataset_id": e.parent_dataset_id,
                "child_keys": list(e.child_keys),
                "parent_keys": list(e.parent_keys),
                "score": round(float(e.score), 3),
                "cardinality_hint": e.cardinality_hint,
                "containment_ratio": (round(float(e.containment_ratio), 3) if e.containment_ratio is not None else None),
                "reasons": list(e.reasons),
                "source": e.source,
                # Enterprise Enhancement: Additional quality signals
                "child_null_ratio": round(float(e.child_null_ratio), 3) if e.child_null_ratio else None,
                "parent_null_ratio": round(float(e.parent_null_ratio), 3) if e.parent_null_ratio else None,
                "is_name_only_match": e.is_name_only_match,
                "quality_warnings": list(e.quality_warnings) if e.quality_warnings else [],
            }
            for e in edge_list
        ],
        "join_plan": join_plan,
        "warnings": warnings,
        "quality_warnings": quality_warnings,
        "notes": notes,
    }

