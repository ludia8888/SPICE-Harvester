from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

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


def normalize_operation(value: Any) -> str:
    return str(value or "").strip()


def resolve_join_spec(metadata: Dict[str, Any]) -> JoinSpec:
    join_type = str(metadata.get("joinType") or metadata.get("join_type") or "inner").strip().lower()
    allow_cross_join = bool(metadata.get("allowCrossJoin") or metadata.get("allow_cross_join") or False)
    left_key = str(metadata.get("leftKey") or metadata.get("left_key") or "").strip() or None
    right_key = str(metadata.get("rightKey") or metadata.get("right_key") or "").strip() or None
    join_key = str(metadata.get("joinKey") or metadata.get("join_key") or "").strip() or None
    if join_key and not left_key:
        left_key = join_key
    if join_key and not right_key:
        right_key = join_key
    return JoinSpec(join_type=join_type, allow_cross_join=allow_cross_join, left_key=left_key, right_key=right_key)


def normalize_union_mode(metadata: Dict[str, Any]) -> str:
    return str(metadata.get("unionMode") or metadata.get("union_mode") or "strict").strip().lower()
