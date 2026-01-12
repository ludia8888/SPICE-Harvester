from __future__ import annotations

import pytest

from shared.utils.submission_criteria_diagnostics import infer_submission_criteria_failure_reason


@pytest.mark.unit
def test_submission_criteria_reason_missing_role() -> None:
    info = infer_submission_criteria_failure_reason("user.role in ['admin']")
    assert info["reason"] == "missing_role"
    assert "missing_role" in info["reasons"]


@pytest.mark.unit
def test_submission_criteria_reason_state_mismatch() -> None:
    info = infer_submission_criteria_failure_reason("target['status'] == 'APPROVED'")
    assert info["reason"] == "state_mismatch"
    assert "state_mismatch" in info["reasons"]


@pytest.mark.unit
def test_submission_criteria_reason_mixed() -> None:
    info = infer_submission_criteria_failure_reason("user.role == 'admin' and target.status == 'OPEN'")
    assert info["reason"] == "mixed"
    assert set(info["reasons"]) == {"missing_role", "state_mismatch"}

