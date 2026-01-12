from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from shared.utils.canonical_json import canonical_json_dumps, sha256_canonical_json_prefixed


class ActionAuditPolicyError(ValueError):
    pass


@dataclass(frozen=True)
class NormalizedAuditPolicy:
    input_mode: str
    result_mode: str
    max_input_bytes: int
    max_result_bytes: int
    redact_keys: Set[str]
    redact_value: Any
    max_changes: int


def _json_size_bytes(value: Any) -> int:
    body = canonical_json_dumps(value).encode("utf-8")
    return len(body)


def _as_str_set(values: Any) -> Set[str]:
    if values is None:
        return set()
    if isinstance(values, str):
        return {values.strip().lower()} if values.strip() else set()
    if not isinstance(values, list):
        return set()
    out: Set[str] = set()
    for item in values:
        if isinstance(item, str) and item.strip():
            out.add(item.strip().lower())
    return out


def normalize_audit_policy(raw: Any) -> NormalizedAuditPolicy:
    policy = raw if isinstance(raw, dict) else {}

    input_mode = str(policy.get("input_mode") or "FULL").strip().upper()
    result_mode = str(policy.get("result_mode") or "FULL").strip().upper()
    if input_mode not in {"FULL", "NONE"}:
        input_mode = "FULL"
    if result_mode not in {"FULL", "NONE"}:
        result_mode = "FULL"

    def _as_int(key: str, default: int) -> int:
        value = policy.get(key)
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return default

    max_input_bytes = _as_int("max_input_bytes", 20_000)
    max_result_bytes = _as_int("max_result_bytes", 200_000)
    max_changes = _as_int("max_changes", 200) or 200

    redact_keys = _as_str_set(policy.get("redact_keys"))
    redact_value = policy.get("redact_value", "REDACTED")

    return NormalizedAuditPolicy(
        input_mode=input_mode,
        result_mode=result_mode,
        max_input_bytes=max_input_bytes,
        max_result_bytes=max_result_bytes,
        redact_keys=redact_keys,
        redact_value=redact_value,
        max_changes=max_changes,
    )


def _redact_recursive(value: Any, *, redact_keys: Set[str], redact_value: Any) -> Any:
    if not redact_keys:
        return value
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            key = str(k)
            if key.lower() in redact_keys:
                out[key] = redact_value
            else:
                out[key] = _redact_recursive(v, redact_keys=redact_keys, redact_value=redact_value)
        return out
    if isinstance(value, list):
        return [_redact_recursive(v, redact_keys=redact_keys, redact_value=redact_value) for v in value]
    return value


def _summarize_large_list(values: List[Any], *, max_items: int) -> Dict[str, Any]:
    sample_size = min(3, max_items, len(values))
    sample = values[:sample_size]
    return {
        "__truncated__": True,
        "count": len(values),
        "sha256": sha256_canonical_json_prefixed(values),
        "sample": sample,
        "sample_count": sample_size,
    }


def _summarize_known_arrays(value: Any, *, max_changes: int) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            if k in {"attempted_changes", "applied_changes", "conflicts", "targets"} and isinstance(v, list):
                if len(v) > max_changes:
                    out[k] = _summarize_large_list(v, max_items=max_changes)
                else:
                    out[k] = [_summarize_known_arrays(item, max_changes=max_changes) for item in v]
            else:
                out[k] = _summarize_known_arrays(v, max_changes=max_changes)
        return out
    if isinstance(value, list):
        return [_summarize_known_arrays(v, max_changes=max_changes) for v in value]
    return value


def audit_action_log_input(payload: Any, *, audit_policy: Any) -> Dict[str, Any]:
    policy = normalize_audit_policy(audit_policy)
    if policy.input_mode == "NONE":
        return {}
    redacted = _redact_recursive(payload, redact_keys=policy.redact_keys, redact_value=policy.redact_value)
    summarized = _summarize_known_arrays(redacted, max_changes=policy.max_changes)
    size = _json_size_bytes(summarized)
    if size > policy.max_input_bytes:
        return {"__truncated__": True, "sha256": sha256_canonical_json_prefixed(summarized), "bytes": size}
    if isinstance(summarized, dict):
        return summarized
    return {"value": summarized}


def audit_action_log_result(payload: Any, *, audit_policy: Any) -> Dict[str, Any]:
    policy = normalize_audit_policy(audit_policy)
    if policy.result_mode == "NONE":
        return {}
    redacted = _redact_recursive(payload, redact_keys=policy.redact_keys, redact_value=policy.redact_value)
    summarized = _summarize_known_arrays(redacted, max_changes=policy.max_changes)
    size = _json_size_bytes(summarized)
    if size > policy.max_result_bytes:
        return {"__truncated__": True, "sha256": sha256_canonical_json_prefixed(summarized), "bytes": size}
    if isinstance(summarized, dict):
        return summarized
    return {"value": summarized}

