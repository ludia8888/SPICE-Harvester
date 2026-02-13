from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

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
    time_direction: str
    left_cache_expiration_seconds: Optional[float]
    right_cache_expiration_seconds: Optional[float]


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
    time_direction = (
        _resolve_text(["timeDirection", "time_direction", "eventTimeDirection", "event_time_direction"])
        or "backward"
    ).lower()

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

    def _resolve_number(keys: List[str], *, error_message: str) -> Optional[float]:
        raw_value: Optional[Any] = None
        for source in (stream_meta, metadata):
            for key in keys:
                if key not in source:
                    continue
                value = source.get(key)
                if value is None or str(value).strip() == "":
                    continue
                raw_value = value
                break
            if raw_value is not None:
                break
        if raw_value is None:
            return None
        try:
            return float(str(raw_value).strip())
        except (TypeError, ValueError) as exc:
            raise ValueError(error_message) from exc

    left_cache_expiration_seconds = _resolve_number(
        ["leftCacheExpirationSeconds", "left_cache_expiration_seconds"],
        error_message="streamJoin leftCacheExpirationSeconds must be a number",
    )
    right_cache_expiration_seconds = _resolve_number(
        ["rightCacheExpirationSeconds", "right_cache_expiration_seconds"],
        error_message="streamJoin rightCacheExpirationSeconds must be a number",
    )

    return StreamJoinSpec(
        strategy=strategy,
        left_event_time_column=left_event_time_column,
        right_event_time_column=right_event_time_column,
        allowed_lateness_seconds=allowed_lateness_seconds,
        time_direction=time_direction,
        left_cache_expiration_seconds=left_cache_expiration_seconds,
        right_cache_expiration_seconds=right_cache_expiration_seconds,
    )


def resolve_stream_join_effective_join_type(*, strategy: str, requested_join_type: str) -> str:
    strategy_norm = str(strategy or "dynamic").strip().lower() or "dynamic"
    requested_norm = str(requested_join_type or "").strip().lower()
    if strategy_norm == "left_lookup":
        return "left"
    if requested_norm in {"full", "outer", "full_outer", "fullouter"}:
        return "full"
    # Foundry stream-stream join behavior is outer by default.
    return "full"


def resolve_input_read_mode(node: Mapping[str, Any] | None) -> str:
    metadata = node.get("metadata") if isinstance(node, Mapping) and isinstance(node.get("metadata"), Mapping) else node
    if not isinstance(metadata, Mapping):
        return ""
    read = metadata.get("read") if isinstance(metadata.get("read"), Mapping) else {}
    for key in ("mode", "readMode", "read_mode", "inputMode", "input_mode"):
        value = read.get(key)
        text = str(value or "").strip().lower()
        if text:
            return text
    return ""


def resolve_input_read_format(node: Mapping[str, Any] | None) -> str:
    metadata = node.get("metadata") if isinstance(node, Mapping) and isinstance(node.get("metadata"), Mapping) else node
    if not isinstance(metadata, Mapping):
        return ""
    read = metadata.get("read") if isinstance(metadata.get("read"), Mapping) else {}
    for key in ("format", "file_format", "fileFormat"):
        value = read.get(key)
        text = str(value or "").strip().lower()
        if text:
            return text
    return ""


def is_stream_like_input_node(node: Mapping[str, Any] | None) -> bool:
    mode = resolve_input_read_mode(node)
    fmt = resolve_input_read_format(node)
    if mode in {"stream", "streaming"}:
        return True
    if fmt in {"kafka", "kinesis", "pulsar"}:
        return True
    return False


def normalize_union_mode(metadata: Dict[str, Any]) -> str:
    return str(metadata.get("unionMode") or metadata.get("union_mode") or "strict").strip().lower()
