from __future__ import annotations

import pytest

from shared.services.registries.agent_session_registry import (
    SESSION_STATUS_ACTIVE,
    SESSION_STATUS_COMPLETED,
    SESSION_STATUS_ERROR,
    SESSION_STATUS_RUNNING_TOOL,
    SESSION_STATUS_TERMINATED,
    SESSION_STATUS_WAITING_APPROVAL,
    validate_session_status_transition,
)


@pytest.mark.parametrize(
    ("current_status", "next_status"),
    [
        (SESSION_STATUS_ACTIVE, SESSION_STATUS_WAITING_APPROVAL),
        (SESSION_STATUS_ACTIVE, SESSION_STATUS_RUNNING_TOOL),
        (SESSION_STATUS_WAITING_APPROVAL, SESSION_STATUS_RUNNING_TOOL),
        (SESSION_STATUS_RUNNING_TOOL, SESSION_STATUS_COMPLETED),
        (SESSION_STATUS_RUNNING_TOOL, SESSION_STATUS_ERROR),
        (SESSION_STATUS_ERROR, SESSION_STATUS_ACTIVE),
        (SESSION_STATUS_ERROR, SESSION_STATUS_RUNNING_TOOL),
        (SESSION_STATUS_COMPLETED, SESSION_STATUS_ACTIVE),
        (SESSION_STATUS_COMPLETED, SESSION_STATUS_WAITING_APPROVAL),
        (SESSION_STATUS_COMPLETED, SESSION_STATUS_RUNNING_TOOL),
        (SESSION_STATUS_ACTIVE, SESSION_STATUS_TERMINATED),
    ],
)
def test_validate_session_status_transition_allows_valid_transitions(current_status: str, next_status: str) -> None:
    validate_session_status_transition(current_status=current_status, next_status=next_status)


@pytest.mark.parametrize(
    ("current_status", "next_status"),
    [
        (SESSION_STATUS_TERMINATED, SESSION_STATUS_ACTIVE),
        (SESSION_STATUS_TERMINATED, SESSION_STATUS_RUNNING_TOOL),
    ],
)
def test_validate_session_status_transition_rejects_invalid_transitions(current_status: str, next_status: str) -> None:
    with pytest.raises(ValueError):
        validate_session_status_transition(current_status=current_status, next_status=next_status)


def test_validate_session_status_transition_rejects_unknown_states() -> None:
    with pytest.raises(ValueError):
        validate_session_status_transition(current_status="WUT", next_status=SESSION_STATUS_ACTIVE)
    with pytest.raises(ValueError):
        validate_session_status_transition(current_status=SESSION_STATUS_ACTIVE, next_status="WUT")
