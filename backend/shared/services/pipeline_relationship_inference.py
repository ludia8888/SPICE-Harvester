from __future__ import annotations

"""
Deterministic relationship / join-chain inference.

Goal:
- Provide planner/join strategist with a small, structured "candidate space" for joining many datasets.
- Use only context pack signals (sample-safe) and avoid any side effects.
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


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

    This is intentionally "thin": it primarily re-exports context pack suggestions
    and enriches them with a few normalized fields.
    """
    selected = _filter_selected_datasets(context_pack, dataset_ids=dataset_ids)
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
    for item in fk_candidates[: max(0, int(max_fk_candidates))]:
        child_id = _as_str(item.get("child_dataset_id"))
        parent_id = _as_str(item.get("parent_dataset_id"))
        if not child_id or not parent_id:
            continue
        if wanted and (child_id not in wanted or parent_id not in wanted):
            continue
        child_col = _as_str(item.get("child_column"))
        parent_col = _as_str(item.get("parent_column"))
        fk_out.append(
            {
                **item,
                "child_keys": _normalize_key_list(child_col) if child_col else [],
                "parent_keys": _normalize_key_list(parent_col) if parent_col else [],
                "direction": "child_to_parent",
                "recommended_join_type": "left",
            }
        )

    return {
        "primary_keys": pk,
        "foreign_keys": fk_out,
        "notes": ["inference is sample-based; validate keys before using for canonical mappings"],
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
            edge = _Edge(
                child_dataset_id=child_id,
                parent_dataset_id=parent_id,
                child_keys=child_keys,
                parent_keys=parent_keys,
                score=score,
                cardinality_hint=_as_str(cand.get("cardinality_hint")) or None,
                containment_ratio=(float(cand.get("containment_ratio")) if cand.get("containment_ratio") is not None else None),
                reasons=tuple([_as_str(r) for r in reasons if _as_str(r)])[:6],
                source="foreign_key_candidates",
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
            edge = _Edge(
                child_dataset_id=child_id,
                parent_dataset_id=parent_id,
                child_keys=tuple(child_keys),
                parent_keys=tuple(parent_keys),
                score=score,
                cardinality_hint=_as_str(cand.get("cardinality_hint")) or None,
                containment_ratio=child_cont if (left_cont or right_cont) else None,
                reasons=tuple([_as_str(r) for r in reasons if _as_str(r)])[:6],
                source="join_key_candidates",
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
    for edge in edge_list:
        if len(join_plan) >= max(0, int(max_joins)):
            break
        if not uf.union(edge.child_dataset_id, edge.parent_dataset_id):
            continue
        reason = ", ".join([r for r in edge.reasons if r]) or f"from {edge.source}"
        join_plan.append(
            {
                "left_dataset_id": edge.child_dataset_id,
                "right_dataset_id": edge.parent_dataset_id,
                "left_column": edge.child_keys[0],
                "right_column": edge.parent_keys[0],
                "left_columns": list(edge.child_keys),
                "right_columns": list(edge.parent_keys),
                "join_type": "left",
                "cardinality": edge.cardinality_hint,
                "confidence": max(0.0, min(1.0, float(edge.score))),
                "reason": reason,
            }
        )

    components = uf.components()
    warnings: List[str] = []
    if len(components) > 1:
        unreachable = sorted([item for group in components.values() for item in group])
        warnings.append(f"join graph disconnected in sample; components={len(components)} datasets={unreachable}")

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
            }
            for e in edge_list
        ],
        "join_plan": join_plan,
        "warnings": warnings,
        "notes": ["join plan inference is sample-based; treat as a suggestion only"],
    }

