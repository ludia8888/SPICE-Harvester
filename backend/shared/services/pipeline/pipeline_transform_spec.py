from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

SUPPORTED_TRANSFORMS = frozenset(
    {
        "join",
        "filter",
        "compute",
        "groupBy",
        "aggregate",
        "pivot",
        "window",
        "explode",
        "select",
        "drop",
        "rename",
        "normalize",
        "regexReplace",
        "cast",
        "dedupe",
        "sort",
        "union",
    }
)


@dataclass(frozen=True)
class JoinSpec:
    join_type: str
    allow_cross_join: bool
    left_key: Optional[str]
    right_key: Optional[str]
    left_keys: Optional[List[str]]
    right_keys: Optional[List[str]]


def normalize_operation(value: Any) -> str:
    return str(value or "").strip()


def resolve_join_spec(metadata: Dict[str, Any]) -> JoinSpec:
    join_type = str(metadata.get("joinType") or metadata.get("join_type") or "inner").strip().lower()
    allow_cross_join = bool(metadata.get("allowCrossJoin") or metadata.get("allow_cross_join") or False)
    left_key = str(metadata.get("leftKey") or metadata.get("left_key") or "").strip() or None
    right_key = str(metadata.get("rightKey") or metadata.get("right_key") or "").strip() or None
    join_key = str(metadata.get("joinKey") or metadata.get("join_key") or "").strip() or None

    def _normalize_keys(value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, tuple):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str) and value.strip():
            return [value.strip()]
        return []

    left_keys = _normalize_keys(
        metadata.get("leftKeys")
        or metadata.get("left_keys")
        or metadata.get("leftOn")
        or metadata.get("left_on")
    )
    right_keys = _normalize_keys(
        metadata.get("rightKeys")
        or metadata.get("right_keys")
        or metadata.get("rightOn")
        or metadata.get("right_on")
    )
    if join_key and not left_key:
        left_key = join_key
    if join_key and not right_key:
        right_key = join_key
    if left_key and not left_keys:
        left_keys = [left_key]
    if right_key and not right_keys:
        right_keys = [right_key]
    if not left_key and len(left_keys) == 1:
        left_key = left_keys[0]
    if not right_key and len(right_keys) == 1:
        right_key = right_keys[0]
    return JoinSpec(
        join_type=join_type,
        allow_cross_join=allow_cross_join,
        left_key=left_key,
        right_key=right_key,
        left_keys=left_keys or None,
        right_keys=right_keys or None,
    )


def normalize_union_mode(metadata: Dict[str, Any]) -> str:
    return str(metadata.get("unionMode") or metadata.get("union_mode") or "strict").strip().lower()
