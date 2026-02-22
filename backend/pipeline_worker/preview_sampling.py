from __future__ import annotations

from typing import Any, Dict, Optional


def resolve_preview_flag(
    preview_meta: Dict[str, Any],
    *,
    snake_case_key: str,
    camel_case_key: str,
    default: bool,
) -> bool:
    raw = preview_meta.get(snake_case_key)
    if raw is None:
        raw = preview_meta.get(camel_case_key)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    if isinstance(raw, str):
        return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return bool(raw)


def resolve_preview_limit(
    *,
    preview_limit: Optional[Any],
    preview_meta: Optional[Dict[str, Any]] = None,
    default: int = 500,
) -> int:
    resolved_limit = preview_limit
    if resolved_limit is None and isinstance(preview_meta, dict):
        resolved_limit = preview_meta.get("sample_limit")
        if resolved_limit is None:
            resolved_limit = preview_meta.get("sampleLimit")
    try:
        numeric_limit = int(resolved_limit if resolved_limit is not None else default)
    except (TypeError, ValueError):
        numeric_limit = default
    return max(1, min(500, numeric_limit))


def resolve_sampling_strategy(
    metadata: Dict[str, Any],
    preview_meta: Dict[str, Any],
    *,
    preview_limit: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    raw = metadata.get("samplingStrategy") or metadata.get("sampling_strategy")
    if raw is None:
        raw = preview_meta.get("samplingStrategy") or preview_meta.get("sampling_strategy")
    if raw is None:
        if preview_limit and preview_limit > 0:
            return {"type": "limit", "limit": preview_limit}
        return None
    if isinstance(raw, str):
        strategy: Dict[str, Any] = {"type": raw}
        if str(raw).strip().lower() in {"limit", "head"} and preview_limit and preview_limit > 0:
            strategy["limit"] = preview_limit
        return strategy
    if isinstance(raw, dict):
        strategy = dict(raw)
        strategy_type = str(strategy.get("type") or strategy.get("mode") or "").strip().lower()
        if strategy_type in {"limit", "head"} and "limit" not in strategy and "rows" not in strategy:
            if preview_limit and preview_limit > 0:
                strategy["limit"] = preview_limit
        return strategy
    raise ValueError("sampling_strategy must be an object")


def attach_sampling_snapshot(
    input_snapshots: list[dict[str, Any]],
    *,
    node_id: str,
    sampling_strategy: Dict[str, Any],
) -> None:
    for snapshot in reversed(input_snapshots):
        if snapshot.get("node_id") == node_id:
            snapshot["sampling_strategy"] = sampling_strategy
            break


def _normalize_sampling_fraction(value: Any, *, field: str) -> float:
    try:
        fraction = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"sampling_strategy.{field} must be a number")
    if fraction <= 0 or fraction > 1:
        raise ValueError(f"sampling_strategy.{field} must be within (0, 1]")
    return fraction


def apply_sampling_strategy(
    df: Any,
    sampling_strategy: Dict[str, Any],
    *,
    node_id: str,
    seed: Optional[int],
) -> Any:
    strategy = sampling_strategy or {}
    strategy_type = str(strategy.get("type") or strategy.get("mode") or "").strip().lower()
    if not strategy_type:
        raise ValueError(f"sampling_strategy.type is required for input node {node_id}")
    if strategy_type in {"random", "sample", "bernoulli", "tablesample"}:
        fraction = strategy.get("fraction")
        if fraction is None:
            percent = strategy.get("percent")
            if percent is not None:
                fraction = float(percent) / 100.0
        if fraction is None:
            raise ValueError(f"sampling_strategy.fraction is required for input node {node_id}")
        resolved_fraction = _normalize_sampling_fraction(fraction, field="fraction")
        with_replacement = bool(strategy.get("with_replacement") or strategy.get("withReplacement") or False)
        return df.sample(withReplacement=with_replacement, fraction=resolved_fraction, seed=seed)
    if strategy_type in {"stratified", "sampleby"}:
        column = str(strategy.get("column") or "").strip()
        fractions_raw = strategy.get("fractions")
        if not column:
            raise ValueError(f"sampling_strategy.column is required for input node {node_id}")
        if not isinstance(fractions_raw, dict) or not fractions_raw:
            raise ValueError(f"sampling_strategy.fractions is required for input node {node_id}")
        fractions: Dict[Any, float] = {}
        for key, value in fractions_raw.items():
            fractions[key] = _normalize_sampling_fraction(value, field="fractions")
        return df.sampleBy(column, fractions, seed=seed)
    if strategy_type in {"limit", "head"}:
        limit = strategy.get("limit") or strategy.get("rows")
        try:
            limit_value = int(limit)
        except (TypeError, ValueError):
            raise ValueError(f"sampling_strategy.limit must be an integer for input node {node_id}")
        if limit_value <= 0:
            raise ValueError(f"sampling_strategy.limit must be positive for input node {node_id}")
        return df.limit(limit_value)
    raise ValueError(f"Unsupported sampling_strategy.type '{strategy_type}' for input node {node_id}")
