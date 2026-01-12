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
                "class": "TIMEOUT",
                "legacy_code": "UPSTREAM_TIMEOUT",
            },
            "api_code": "UPSTREAM_TIMEOUT",
            "error_key": "UPSTREAM_TIMEOUT",
            "signals": {},
        },
        context={},
    )
    assert decision.family == "timeout"
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
                "class": "VALIDATION",
                "legacy_code": "action_input_invalid",
            },
            "api_code": "REQUEST_VALIDATION_FAILED",
            "error_key": "action_input_invalid",
            "signals": {},
        },
        context={},
    )
    assert decision.family == "validation"
    assert decision.recommended_action == "no_retry"
    assert decision.safe_to_auto_retry is False


@pytest.mark.unit
def test_policy_submission_criteria_failed_includes_reason() -> None:
    tool_call = AgentToolCall(service="bff", method="GET", path="/api/v1/health")
    decision = decide_policy(
        tool_call=tool_call,
        result={
            "enterprise": {
                "code": "SHV-ACT-ACC-PER-3004",
                "class": "PERMISSION",
                "legacy_code": "submission_criteria_failed",
            },
            "api_code": "PERMISSION_DENIED",
            "error_key": "submission_criteria_failed",
            "signals": {"action_log_reason": "missing_role"},
        },
        context={},
    )
    assert decision.family == "permission"
    assert decision.recommended_action == "require_approval"
    assert decision.details.get("submission_criteria_reason") == "missing_role"

