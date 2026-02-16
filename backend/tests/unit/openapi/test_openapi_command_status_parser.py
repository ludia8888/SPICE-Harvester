from __future__ import annotations

import pytest

from tests.test_openapi_contract_smoke import _extract_command_lifecycle_status


@pytest.mark.unit
def test_extract_command_lifecycle_status_prefers_nested_command_status() -> None:
    payload = {
        "status": "success",
        "data": {
            "command_id": "cmd_123",
            "status": "COMPLETED",
        },
    }
    assert _extract_command_lifecycle_status(payload) == "COMPLETED"


@pytest.mark.unit
def test_extract_command_lifecycle_status_handles_nested_state_alias() -> None:
    payload = {
        "status": "success",
        "data": {
            "state": "failed",
        },
    }
    assert _extract_command_lifecycle_status(payload) == "FAILED"


@pytest.mark.unit
def test_extract_command_lifecycle_status_uses_top_level_when_not_generic() -> None:
    payload = {
        "status": "cancelled",
    }
    assert _extract_command_lifecycle_status(payload) == "CANCELLED"


@pytest.mark.unit
def test_extract_command_lifecycle_status_ignores_generic_envelope_status_without_data_status() -> None:
    payload = {
        "status": "success",
        "data": {"message": "accepted"},
    }
    assert _extract_command_lifecycle_status(payload) == ""
