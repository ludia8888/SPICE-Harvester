from __future__ import annotations

import pytest

from agent.services.agent_runtime import _extract_action_simulation_rejection, _extract_action_simulation_signals


@pytest.mark.unit
def test_extract_action_simulation_signals_rejected_includes_reason() -> None:
    payload = {
        "status": "success",
        "data": {
            "preview_action_log_id": "preview-1",
            "results": [
                {
                    "scenario_id": "default",
                    "conflict_policy_override": None,
                    "status": "REJECTED",
                    "error": {
                        "error": "submission_criteria_failed",
                        "message": "submission_criteria evaluated to false",
                        "reason": "state_mismatch",
                        "enterprise": {"external_code": "submission_criteria_failed", "human_required": True},
                    },
                }
            ],
        },
    }

    signals = _extract_action_simulation_signals(payload)
    assert signals["action_simulation_effective_status"] == "REJECTED"
    assert signals["action_log_reason"] == "state_mismatch"
    assert signals["action_simulation_preview_action_log_id"] == "preview-1"


@pytest.mark.unit
def test_extract_action_simulation_rejection_returns_enterprise() -> None:
    payload = {
        "status": "success",
        "data": {
            "preview_action_log_id": "preview-2",
            "results": [
                {
                    "scenario_id": "default",
                    "conflict_policy_override": None,
                    "status": "REJECTED",
                    "error": {
                        "error": "data_access_denied",
                        "message": "Actor cannot access one or more target rows",
                        "enterprise": {"external_code": "data_access_denied", "human_required": True},
                    },
                }
            ],
        },
    }

    error_key, enterprise, message = _extract_action_simulation_rejection(payload)
    assert error_key == "data_access_denied"
    assert enterprise and enterprise.get("external_code") == "data_access_denied"
    assert "cannot access" in (message or "")


@pytest.mark.unit
def test_extract_action_simulation_rejection_returns_none_when_accepted() -> None:
    payload = {
        "status": "success",
        "data": {
            "preview_action_log_id": "preview-3",
            "results": [
                {"scenario_id": "default", "conflict_policy_override": None, "status": "ACCEPTED", "patchset_id": "p1"}
            ],
        },
    }

    error_key, enterprise, message = _extract_action_simulation_rejection(payload)
    assert error_key is None
    assert enterprise is None
    assert message is None
