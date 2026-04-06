from __future__ import annotations

from typing import Any, Dict, Optional

from shared.utils.bool_utils import parse_boolish


def _preview_meta_value(
    preview_meta: Dict[str, Any],
    *,
    snake_case_key: str,
    camel_case_key: str,
) -> Any:
    raw = preview_meta.get(snake_case_key)
    if raw is None:
        raw = preview_meta.get(camel_case_key)
    return raw


def resolve_preview_flag(
    preview_meta: Dict[str, Any],
    *,
    snake_case_key: str,
    camel_case_key: str,
    default: bool,
) -> bool:
    raw = _preview_meta_value(
        preview_meta,
        snake_case_key=snake_case_key,
        camel_case_key=camel_case_key,
    )
    parsed = parse_boolish(raw, allow_numeric=True, allow_short_tokens=True)
    if parsed is not None:
        return parsed
    if raw is None:
        return default
    return bool(raw)


def _resolve_preview_limit_value(raw: Any, *, default: Optional[int]) -> Optional[int]:
    if raw is None:
        return default
    try:
        numeric_limit = int(raw)
    except (TypeError, ValueError):
        return default
    if numeric_limit <= 0:
        return default
    return max(1, min(500, numeric_limit))


def resolve_preview_meta_limit(
    preview_meta: Dict[str, Any],
    *,
    snake_case_key: str,
    camel_case_key: str,
    default: Optional[int],
) -> Optional[int]:
    raw = _preview_meta_value(
        preview_meta,
        snake_case_key=snake_case_key,
        camel_case_key=camel_case_key,
    )
    return _resolve_preview_limit_value(raw, default=default)


def resolve_preview_limit(
    *,
    preview_limit: Optional[Any],
    preview_meta: Optional[Dict[str, Any]] = None,
    default: int = 500,
) -> int:
    if preview_limit is not None:
        resolved_limit = preview_limit
    elif isinstance(preview_meta, dict):
        resolved_limit = _preview_meta_value(
            preview_meta,
            snake_case_key="sample_limit",
            camel_case_key="sampleLimit",
        )
    else:
        resolved_limit = None
    resolved = _resolve_preview_limit_value(resolved_limit, default=default)
    return default if resolved is None else resolved


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
