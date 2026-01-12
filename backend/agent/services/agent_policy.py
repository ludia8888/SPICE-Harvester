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


def _enterprise_field(enterprise: Optional[Dict[str, Any]], key: str) -> Optional[str]:
    if not enterprise:
        return None
    value = enterprise.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _normalize_code(value: Optional[str]) -> str:
    return str(value or "").strip()


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
    enterprise_class = _enterprise_field(enterprise, "class")
    legacy_code = _enterprise_field(enterprise, "legacy_code") or _normalize_code(result.get("error_key") or "")
    api_code = _normalize_code(result.get("api_code") or "")
    retryable = enterprise.get("retryable") if isinstance(enterprise, dict) else None
    if not isinstance(retryable, bool):
        retryable = None
    default_retry_policy = enterprise.get("default_retry_policy") if isinstance(enterprise, dict) else None
    if not isinstance(default_retry_policy, str):
        default_retry_policy = None
    default_retry_policy = _normalize_code(default_retry_policy).lower()
    human_required = enterprise.get("human_required") if isinstance(enterprise, dict) else None
    if not isinstance(human_required, bool):
        human_required = None
    safe_next_actions_raw = enterprise.get("safe_next_actions") if isinstance(enterprise, dict) else None
    safe_next_actions = safe_next_actions_raw if isinstance(safe_next_actions_raw, list) else []

    action_log_signals = result.get("signals") if isinstance(result.get("signals"), dict) else {}
    criteria_reason = (
        _normalize_code(action_log_signals.get("action_log_reason"))
        if isinstance(action_log_signals, dict)
        else ""
    )

    if legacy_code == "overlay_degraded":
        return AgentPolicyDecision(
            family="overlay_degraded",
            recommended_action="safe_mode",
            safe_to_auto_retry=False,
            reason="Overlay is DEGRADED; block unsafe automation",
            details={"legacy_code": legacy_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if human_required is True:
        details: Dict[str, Any] = {
            "legacy_code": legacy_code,
            "enterprise_code": enterprise_code,
            "enterprise_class": enterprise_class,
            "api_code": api_code,
            "default_retry_policy": default_retry_policy or None,
            "safe_next_actions": safe_next_actions,
        }
        if legacy_code == "submission_criteria_failed" and criteria_reason:
            details["submission_criteria_reason"] = criteria_reason
        return AgentPolicyDecision(
            family=enterprise_class or "human_required",
            recommended_action="human_required",
            safe_to_auto_retry=False,
            reason="Human intervention required by taxonomy",
            details=details,
        )

    if retryable is True and default_retry_policy in {"backoff", "immediate"}:
        return AgentPolicyDecision(
            family="retry",
            recommended_action="retry",
            safe_to_auto_retry=_method_is_safe_to_retry(tool_call.method),
            reason=f"Retryable ({default_retry_policy})",
            details={
                "legacy_code": legacy_code,
                "enterprise_code": enterprise_code,
                "enterprise_class": enterprise_class,
                "api_code": api_code,
                "default_retry_policy": default_retry_policy,
                "safe_next_actions": safe_next_actions,
            },
        )

    if default_retry_policy == "after_refresh":
        return AgentPolicyDecision(
            family="after_refresh",
            recommended_action="after_refresh",
            safe_to_auto_retry=False,
            reason="Retry requires refresh step first",
            details={
                "legacy_code": legacy_code,
                "enterprise_code": enterprise_code,
                "enterprise_class": enterprise_class,
                "api_code": api_code,
                "default_retry_policy": default_retry_policy,
                "safe_next_actions": safe_next_actions,
            },
        )

    if enterprise_class in {"TIMEOUT"} or (enterprise_code and "-TMO-" in enterprise_code) or api_code in {
        "UPSTREAM_TIMEOUT",
        "DB_TIMEOUT",
    }:
        return AgentPolicyDecision(
            family="timeout",
            recommended_action="retry",
            safe_to_auto_retry=_method_is_safe_to_retry(tool_call.method),
            reason="Transient timeout",
            details={"legacy_code": legacy_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if enterprise_class in {"UNAVAILABLE"} or (enterprise_code and "-UNA-" in enterprise_code) or api_code in {
        "UPSTREAM_UNAVAILABLE",
        "DB_UNAVAILABLE",
        "OMS_UNAVAILABLE",
    }:
        return AgentPolicyDecision(
            family="unavailable",
            recommended_action="retry",
            safe_to_auto_retry=_method_is_safe_to_retry(tool_call.method),
            reason="Upstream unavailable",
            details={"legacy_code": legacy_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if (
        enterprise_class in {"LIMIT"}
        or (enterprise_code and "-RAT-LIM-" in enterprise_code)
        or api_code == "RATE_LIMITED"
        or legacy_code == "rate_limiter_unavailable"
    ):
        return AgentPolicyDecision(
            family="rate_limit",
            recommended_action="retry",
            safe_to_auto_retry=_method_is_safe_to_retry(tool_call.method),
            reason="Rate limited / limiter unavailable",
            details={"legacy_code": legacy_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if (
        enterprise_class in {"VALIDATION", "SECURITY"}
        or api_code in {"REQUEST_VALIDATION_FAILED", "JSON_DECODE_ERROR"}
        or legacy_code in {"action_input_invalid", "validation_failed", "VALUE_CONSTRAINT_FAILED"}
        or legacy_code.endswith("_invalid")
        or legacy_code in {"submission_criteria_error", "validation_rules_invalid", "PIPELINE_SCHEMA_CONTRACT_FAILED"}
    ):
        return AgentPolicyDecision(
            family="validation",
            recommended_action="no_retry",
            safe_to_auto_retry=False,
            reason="Input/schema/contract error (retry will not converge)",
            details={"legacy_code": legacy_code, "enterprise_code": enterprise_code, "api_code": api_code},
        )

    if (
        enterprise_class in {"AUTH", "PERMISSION"}
        or api_code in {"PERMISSION_DENIED", "AUTH_REQUIRED", "AUTH_INVALID", "AUTH_EXPIRED"}
        or legacy_code.startswith("writeback_acl_")
        or legacy_code in {"data_access_denied", "writeback_enforced", "submission_criteria_failed"}
    ):
        details: Dict[str, Any] = {
            "legacy_code": legacy_code,
            "enterprise_code": enterprise_code,
            "api_code": api_code,
        }
        if legacy_code == "submission_criteria_failed" and criteria_reason:
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
        details={"legacy_code": legacy_code, "enterprise_code": enterprise_code, "api_code": api_code},
    )
