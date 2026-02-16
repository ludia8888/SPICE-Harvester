from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Dict, Optional

from agent.models import AgentToolCall


@dataclass(frozen=True)
class AgentPolicyDecision:
    family: str
    recommended_action: str
    safe_to_auto_retry: bool
    reason: str
    details: Dict[str, Any]


def _stable_unit_interval(seed: str) -> float:
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big")
    return value / float(2**64)


def compute_backoff_s(*, seed: str, attempt: int, base_delay_s: float, max_delay_s: float) -> float:
    cap = min(max_delay_s, base_delay_s * (2 ** max(0, int(attempt))))
    if cap <= 0:
        return 0.0
    jitter = _stable_unit_interval(seed)
    # Equal jitter: [cap/2, cap)
    return max(0.0, (cap / 2.0) + (cap / 2.0) * jitter)


def compute_retry_delay_s(
    *,
    seed: str,
    attempt: int,
    base_delay_ms: int,
    max_delay_ms: int,
    jitter_strategy: str,
) -> float:
    base_delay_s = max(0.0, float(base_delay_ms) / 1000.0)
    max_delay_s = max(0.0, float(max_delay_ms) / 1000.0)
    strategy = str(jitter_strategy or "").strip().lower()
    if strategy in {"", "none"}:
        return max(0.0, min(max_delay_s, base_delay_s * (2 ** max(0, int(attempt)))))
    if strategy == "deterministic_equal_jitter":
        return compute_backoff_s(seed=seed, attempt=attempt, base_delay_s=base_delay_s, max_delay_s=max_delay_s)
    return compute_backoff_s(seed=seed, attempt=attempt, base_delay_s=base_delay_s, max_delay_s=max_delay_s)


def _enterprise_field(enterprise: Optional[Dict[str, Any]], key: str) -> Optional[str]:
    if not enterprise:
        return None
    value = enterprise.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _normalize_code(value: Optional[str]) -> str:
    return str(value or "").strip()


def _enterprise_int(enterprise: Optional[Dict[str, Any]], key: str) -> Optional[int]:
    if not enterprise:
        return None
    value = enterprise.get(key)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _enterprise_bool(enterprise: Optional[Dict[str, Any]], key: str) -> Optional[bool]:
    if not enterprise:
        return None
    value = enterprise.get(key)
    return value if isinstance(value, bool) else None


def _method_is_safe_to_retry(method: str) -> bool:
    return str(method or "").strip().upper() == "GET"


def decide_policy(
    *,
    tool_call: AgentToolCall,
    result: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
) -> AgentPolicyDecision:
    enterprise = result.get("enterprise") if isinstance(result.get("enterprise"), dict) else None
    enterprise_code = _enterprise_field(enterprise, "code")
    enterprise_class = _normalize_code(_enterprise_field(enterprise, "class")).lower() or None
    catalog_ref = _enterprise_field(enterprise, "catalog_ref")
    catalog_fingerprint = _enterprise_field(enterprise, "catalog_fingerprint")
    external_code = _enterprise_field(enterprise, "external_code") or _normalize_code(result.get("error_key") or "")
    api_code = _normalize_code(result.get("api_code") or "")
    retryable = _enterprise_bool(enterprise, "retryable")
    default_retry_policy = _normalize_code(_enterprise_field(enterprise, "default_retry_policy")).lower()
    human_required = _enterprise_bool(enterprise, "human_required")
    max_attempts = _enterprise_int(enterprise, "max_attempts")
    base_delay_ms = _enterprise_int(enterprise, "base_delay_ms")
    max_delay_ms = _enterprise_int(enterprise, "max_delay_ms")
    jitter_strategy = _normalize_code(_enterprise_field(enterprise, "jitter_strategy")).lower()
    retry_after_header_respect = _enterprise_bool(enterprise, "retry_after_header_respect")
    enterprise_action = _normalize_code(_enterprise_field(enterprise, "action")).lower() or None
    safe_next_actions_raw = enterprise.get("safe_next_actions") if isinstance(enterprise, dict) else None
    safe_next_actions = safe_next_actions_raw if isinstance(safe_next_actions_raw, list) else []

    action_log_signals = result.get("signals") if isinstance(result.get("signals"), dict) else {}
    criteria_reason = (
        _normalize_code(action_log_signals.get("action_log_reason"))
        if isinstance(action_log_signals, dict)
        else ""
    )

    enterprise_present = enterprise is not None

    if external_code == "overlay_degraded":
        return AgentPolicyDecision(
            family="overlay_degraded",
            recommended_action="safe_mode",
            safe_to_auto_retry=False,
            reason="Overlay is DEGRADED; block unsafe automation",
            details={"external_code": external_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if external_code == "idempotency_in_progress":
        return AgentPolicyDecision(
            family="idempotency_in_progress",
            recommended_action="retry",
            safe_to_auto_retry=True,
            reason="Idempotency key already in progress; safe to retry without duplicating side effects",
            details={"external_code": external_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if human_required is True:
        family = enterprise_class or "human_required"
        recommended_action = enterprise_action or "human_required"
        details: Dict[str, Any] = {
            "external_code": external_code,
            "enterprise_code": enterprise_code,
            "enterprise_class": enterprise_class,
            "api_code": api_code,
            "default_retry_policy": default_retry_policy or None,
            "max_attempts": max_attempts,
            "base_delay_ms": base_delay_ms,
            "max_delay_ms": max_delay_ms,
            "jitter_strategy": jitter_strategy or None,
            "retry_after_header_respect": retry_after_header_respect,
            "safe_next_actions": safe_next_actions,
            "enterprise_action": enterprise_action,
            "catalog_ref": catalog_ref,
            "catalog_fingerprint": catalog_fingerprint,
        }
        if external_code == "submission_criteria_failed" and criteria_reason:
            details["submission_criteria_reason"] = criteria_reason
            if criteria_reason in {"missing_role", "state_mismatch", "mixed"}:
                family = criteria_reason
            if criteria_reason == "state_mismatch":
                recommended_action = "check_state"
            elif criteria_reason == "missing_role":
                recommended_action = "request_access"
        return AgentPolicyDecision(
            family=family,
            recommended_action=recommended_action,
            safe_to_auto_retry=False,
            reason="Human intervention required by taxonomy",
            details=details,
        )

    if enterprise_present and retryable is True and default_retry_policy in {"backoff", "immediate"}:
        return AgentPolicyDecision(
            family="retry",
            recommended_action="retry",
            safe_to_auto_retry=_method_is_safe_to_retry(tool_call.method),
            reason=f"Retryable ({default_retry_policy})",
            details={
                "external_code": external_code,
                "enterprise_code": enterprise_code,
                "enterprise_class": enterprise_class,
                "api_code": api_code,
                "default_retry_policy": default_retry_policy,
                "max_attempts": max_attempts,
                "base_delay_ms": base_delay_ms,
                "max_delay_ms": max_delay_ms,
                "jitter_strategy": jitter_strategy or None,
                "retry_after_header_respect": retry_after_header_respect,
                "safe_next_actions": safe_next_actions,
                "catalog_ref": catalog_ref,
                "catalog_fingerprint": catalog_fingerprint,
            },
        )

    if enterprise_present and default_retry_policy == "after_refresh":
        return AgentPolicyDecision(
            family="after_refresh",
            recommended_action="after_refresh",
            safe_to_auto_retry=False,
            reason="Retry requires refresh step first",
            details={
                "external_code": external_code,
                "enterprise_code": enterprise_code,
                "enterprise_class": enterprise_class,
                "api_code": api_code,
                "default_retry_policy": default_retry_policy,
                "safe_next_actions": safe_next_actions,
                "catalog_ref": catalog_ref,
                "catalog_fingerprint": catalog_fingerprint,
            },
        )

    if enterprise_class in {"timeout"} or (enterprise_code and "-TMO-" in enterprise_code) or api_code in {
        "UPSTREAM_TIMEOUT",
        "DB_TIMEOUT",
    }:
        return AgentPolicyDecision(
            family="timeout",
            recommended_action="retry",
            safe_to_auto_retry=_method_is_safe_to_retry(tool_call.method),
            reason="Transient timeout",
            details={"external_code": external_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if enterprise_class in {"unavailable"} or (enterprise_code and "-UNA-" in enterprise_code) or api_code in {
        "UPSTREAM_UNAVAILABLE",
        "DB_UNAVAILABLE",
        "OMS_UNAVAILABLE",
    }:
        return AgentPolicyDecision(
            family="unavailable",
            recommended_action="retry",
            safe_to_auto_retry=_method_is_safe_to_retry(tool_call.method),
            reason="Upstream unavailable",
            details={"external_code": external_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if (
        enterprise_class in {"limit"}
        or (enterprise_code and "-RAT-LIM-" in enterprise_code)
        or api_code == "RATE_LIMITED"
        or external_code == "rate_limiter_unavailable"
    ):
        return AgentPolicyDecision(
            family="rate_limit",
            recommended_action="retry",
            safe_to_auto_retry=_method_is_safe_to_retry(tool_call.method),
            reason="Rate limited / limiter unavailable",
            details={"external_code": external_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if (
        enterprise_class in {"validation", "security"}
        or api_code in {"REQUEST_VALIDATION_FAILED", "JSON_DECODE_ERROR"}
        or external_code in {"action_input_invalid", "validation_failed", "VALUE_CONSTRAINT_FAILED"}
        or external_code.endswith("_invalid")
        or external_code in {"submission_criteria_error", "validation_rules_invalid", "PIPELINE_SCHEMA_CONTRACT_FAILED"}
    ):
        return AgentPolicyDecision(
            family="validation",
            recommended_action="no_retry",
            safe_to_auto_retry=False,
            reason="Input/schema/contract error (retry will not converge)",
            details={"external_code": external_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if (
        enterprise_class in {"auth", "permission"}
        or api_code in {"PERMISSION_DENIED", "AUTH_REQUIRED", "AUTH_INVALID", "AUTH_EXPIRED"}
        or external_code.startswith("writeback_acl_")
        or external_code in {"data_access_denied", "writeback_enforced", "submission_criteria_failed"}
    ):
        details: Dict[str, Any] = {
            "external_code": external_code,
            "enterprise_code": enterprise_code,
            "api_code": api_code,
        }
        if external_code == "submission_criteria_failed" and criteria_reason:
            details["submission_criteria_reason"] = criteria_reason
        return AgentPolicyDecision(
            family="permission",
            recommended_action="require_approval",
            safe_to_auto_retry=False,
            reason="Permission/criteria gate",
            details=details,
        )

    return AgentPolicyDecision(
        family="unknown",
        recommended_action="escalate",
        safe_to_auto_retry=False,
        reason="Unknown failure family; require human triage",
        details={"external_code": external_code, "enterprise_code": enterprise_code, "api_code": api_code},
    )
