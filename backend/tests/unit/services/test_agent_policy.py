from __future__ import annotations

import pytest

from agent.models import AgentToolCall
from agent.services.agent_policy import decide_policy


@pytest.mark.unit
def test_policy_overlay_degraded_safe_mode() -> None:
    tool_call = AgentToolCall(service="bff", method="GET", path="/api/v1/health")
    decision = decide_policy(
        tool_call=tool_call,
        result={"error_key": "overlay_degraded", "api_code": None, "enterprise": None, "signals": {}},
        context={},
    )
    assert decision.family == "overlay_degraded"
    assert decision.recommended_action == "safe_mode"
    assert decision.safe_to_auto_retry is False


@pytest.mark.unit
def test_policy_timeout_retry_for_reads() -> None:
    tool_call = AgentToolCall(service="bff", method="GET", path="/api/v1/health")
    decision = decide_policy(
        tool_call=tool_call,
        result={
            "enterprise": {
                "code": "SHV-BFF-UPS-TMO-0001",
                "class": "timeout",
                "legacy_code": "UPSTREAM_TIMEOUT",
                "retryable": True,
                "default_retry_policy": "backoff",
                "max_attempts": 3,
                "base_delay_ms": 500,
                "max_delay_ms": 10000,
                "jitter_strategy": "deterministic_equal_jitter",
                "retry_after_header_respect": False,
                "human_required": False,
                "safe_next_actions": ["retry_backoff"],
            },
            "api_code": "UPSTREAM_TIMEOUT",
            "error_key": "UPSTREAM_TIMEOUT",
            "signals": {},
        },
        context={},
    )
    assert decision.family == "retry"
    assert decision.recommended_action == "retry"
    assert decision.safe_to_auto_retry is True


@pytest.mark.unit
def test_policy_validation_no_retry() -> None:
    tool_call = AgentToolCall(service="bff", method="GET", path="/api/v1/health")
    decision = decide_policy(
        tool_call=tool_call,
        result={
            "enterprise": {
                "code": "SHV-OMS-INP-VAL-3001",
                "class": "validation",
                "legacy_code": "action_input_invalid",
                "retryable": False,
                "default_retry_policy": "none",
                "max_attempts": 1,
                "base_delay_ms": 0,
                "max_delay_ms": 0,
                "jitter_strategy": "none",
                "retry_after_header_respect": False,
                "human_required": True,
                "safe_next_actions": ["request_human"],
                "action": "fix_input",
            },
            "api_code": "REQUEST_VALIDATION_FAILED",
            "error_key": "action_input_invalid",
            "signals": {},
        },
        context={},
    )
    assert decision.family == "validation"
    assert decision.recommended_action == "fix_input"
    assert decision.safe_to_auto_retry is False


@pytest.mark.unit
def test_policy_submission_criteria_failed_includes_reason() -> None:
    tool_call = AgentToolCall(service="bff", method="GET", path="/api/v1/health")
    decision = decide_policy(
        tool_call=tool_call,
        result={
            "enterprise": {
                "code": "SHV-ACT-ACC-PER-3004",
                "class": "permission",
                "legacy_code": "submission_criteria_failed",
                "retryable": False,
                "default_retry_policy": "none",
                "max_attempts": 1,
                "base_delay_ms": 0,
                "max_delay_ms": 0,
                "jitter_strategy": "none",
                "retry_after_header_respect": False,
                "human_required": True,
                "safe_next_actions": ["request_human"],
                "action": "request_access",
            },
            "api_code": "PERMISSION_DENIED",
            "error_key": "submission_criteria_failed",
            "signals": {"action_log_reason": "missing_role"},
        },
        context={},
    )
    assert decision.family == "missing_role"
    assert decision.recommended_action == "request_access"
    assert decision.details.get("submission_criteria_reason") == "missing_role"


@pytest.mark.unit
def test_policy_submission_criteria_failed_state_mismatch_proposes_check_state() -> None:
    tool_call = AgentToolCall(service="bff", method="GET", path="/api/v1/health")
    decision = decide_policy(
        tool_call=tool_call,
        result={
            "enterprise": {
                "code": "SHV-ACT-ACC-PER-3004",
                "class": "permission",
                "legacy_code": "submission_criteria_failed",
                "retryable": False,
                "default_retry_policy": "none",
                "max_attempts": 1,
                "base_delay_ms": 0,
                "max_delay_ms": 0,
                "jitter_strategy": "none",
                "retry_after_header_respect": False,
                "human_required": True,
                "safe_next_actions": ["request_human"],
                "action": "request_access",
            },
            "api_code": "PERMISSION_DENIED",
            "error_key": "submission_criteria_failed",
            "signals": {"action_log_reason": "state_mismatch"},
        },
        context={},
    )
    assert decision.family == "state_mismatch"
    assert decision.recommended_action == "check_state"
    assert decision.safe_to_auto_retry is False
