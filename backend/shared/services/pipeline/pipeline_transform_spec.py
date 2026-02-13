from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from shared.services.pipeline.pipeline_join_keys import normalize_join_key_list

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
        "udf",
        "split",
        "geospatial",
        "patternMining",
        "streamJoin",
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


@dataclass(frozen=True)
class StreamJoinSpec:
    strategy: str
    left_event_time_column: Optional[str]
    right_event_time_column: Optional[str]
    allowed_lateness_seconds: Optional[float]


def normalize_operation(value: Any) -> str:
    return str(value or "").strip()


def resolve_join_spec(metadata: Dict[str, Any]) -> JoinSpec:
    join_type = str(metadata.get("joinType") or metadata.get("join_type") or "inner").strip().lower()
    allow_cross_join = bool(metadata.get("allowCrossJoin") or metadata.get("allow_cross_join") or False)
    left_key = str(metadata.get("leftKey") or metadata.get("left_key") or "").strip() or None
    right_key = str(metadata.get("rightKey") or metadata.get("right_key") or "").strip() or None
    join_key = str(metadata.get("joinKey") or metadata.get("join_key") or "").strip() or None

    left_keys = normalize_join_key_list(
        metadata.get("leftKeys")
        or metadata.get("left_keys")
        or metadata.get("leftOn")
        or metadata.get("left_on")
    )
    right_keys = normalize_join_key_list(
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


def resolve_stream_join_spec(metadata: Dict[str, Any]) -> StreamJoinSpec:
    stream_meta = metadata.get("streamJoin") if isinstance(metadata.get("streamJoin"), dict) else {}
    strategy = str(stream_meta.get("strategy") or metadata.get("strategy") or "dynamic").strip().lower() or "dynamic"

    def _resolve_text(keys: List[str]) -> Optional[str]:
        for source in (stream_meta, metadata):
            for key in keys:
                value = source.get(key)
                if value is None:
                    continue
                text = str(value).strip()
                if text:
                    return text
        return None

    left_event_time_column = _resolve_text(["leftEventTimeColumn", "left_event_time_column"])
    right_event_time_column = _resolve_text(["rightEventTimeColumn", "right_event_time_column"])

    allowed_lateness_raw: Optional[Any] = None
    for source in (stream_meta, metadata):
        for key in ("allowedLatenessSeconds", "allowed_lateness_seconds"):
            if key not in source:
                continue
            value = source.get(key)
            if value is None:
                continue
            if str(value).strip() == "":
                continue
            allowed_lateness_raw = value
            break
        if allowed_lateness_raw is not None:
            break

    allowed_lateness_seconds: Optional[float] = None
    if allowed_lateness_raw is not None:
        try:
            allowed_lateness_seconds = float(str(allowed_lateness_raw).strip())
        except (TypeError, ValueError) as exc:
            raise ValueError("streamJoin allowedLatenessSeconds must be a number") from exc

    return StreamJoinSpec(
        strategy=strategy,
        left_event_time_column=left_event_time_column,
        right_event_time_column=right_event_time_column,
        allowed_lateness_seconds=allowed_lateness_seconds,
    )


def normalize_union_mode(metadata: Dict[str, Any]) -> str:
    return str(metadata.get("unionMode") or metadata.get("union_mode") or "strict").strip().lower()
