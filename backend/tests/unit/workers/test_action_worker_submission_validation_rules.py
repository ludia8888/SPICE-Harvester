from __future__ import annotations

from typing import Any, Dict, List

import pytest

from action_worker.main import ActionWorker, _ActionRejected


class _FakeActionLogs:
    def __init__(self) -> None:
        self.failed: List[Dict[str, Any]] = []

    async def mark_failed(self, *, action_log_id: str, result: Dict[str, Any]) -> None:
        self.failed.append({"action_log_id": action_log_id, "result": result})


def _build_worker() -> tuple[ActionWorker, _FakeActionLogs]:
    worker = object.__new__(ActionWorker)
    fake_logs = _FakeActionLogs()
    worker.action_logs = fake_logs
    worker._audit_result = lambda *, audit_policy, payload: payload  # noqa: SLF001
    return worker, fake_logs


def _loaded_target(*, status: str = "OPEN") -> Dict[str, Any]:
    return {
        "class_id": "Ticket",
        "instance_id": "t-1",
        "lifecycle_id": "lc-0",
        "base_state": {"status": status},
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_submission_rules_requires_user_for_submission_criteria() -> None:
    worker, fake_logs = _build_worker()

    with pytest.raises(_ActionRejected) as exc_info:
        await worker._enforce_submission_and_validation_rules(
            action_log_id="log-1",
            audit_policy=None,
            spec={"submission_criteria": "user.role == 'Owner'"},
            submitted_by="",
            actor_role="Owner",
            input_payload={},
            loaded_targets=[_loaded_target()],
            db_name="demo",
            base_branch="main",
        )

    assert str(exc_info.value) == "submission_criteria_missing_user"
    assert fake_logs.failed
    result = fake_logs.failed[0]["result"]
    assert result["error"] == "submission_criteria_missing_user"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_submission_rules_rejects_false_submission_criteria() -> None:
    worker, fake_logs = _build_worker()

    with pytest.raises(_ActionRejected) as exc_info:
        await worker._enforce_submission_and_validation_rules(
            action_log_id="log-2",
            audit_policy=None,
            spec={"submission_criteria": "user.role == 'Admin'"},
            submitted_by="alice",
            actor_role="Owner",
            input_payload={},
            loaded_targets=[_loaded_target()],
            db_name="demo",
            base_branch="main",
        )

    assert str(exc_info.value) == "submission_criteria_failed"
    assert fake_logs.failed
    result = fake_logs.failed[0]["result"]
    assert result["error"] == "submission_criteria_failed"
    assert result["message"] == "submission_criteria evaluated to false"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_validation_rules_requires_list_type() -> None:
    worker, fake_logs = _build_worker()

    with pytest.raises(_ActionRejected) as exc_info:
        await worker._enforce_submission_and_validation_rules(
            action_log_id="log-3",
            audit_policy=None,
            spec={"validation_rules": "not-a-list"},
            submitted_by="alice",
            actor_role="Owner",
            input_payload={},
            loaded_targets=[_loaded_target()],
            db_name="demo",
            base_branch="main",
        )

    assert str(exc_info.value) == "validation_rules_invalid"
    assert fake_logs.failed
    result = fake_logs.failed[0]["result"]
    assert result["error"] == "validation_rules_invalid"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_each_target_validation_rule_includes_target_context() -> None:
    worker, fake_logs = _build_worker()

    with pytest.raises(_ActionRejected) as exc_info:
        await worker._enforce_submission_and_validation_rules(
            action_log_id="log-4",
            audit_policy=None,
            spec={
                "validation_rules": [
                    {
                        "type": "assert",
                        "scope": "each_target",
                        "expr": "target.status == 'OPEN'",
                        "message": "target must be OPEN",
                    }
                ]
            },
            submitted_by="alice",
            actor_role="Owner",
            input_payload={},
            loaded_targets=[_loaded_target(status="CLOSED")],
            db_name="demo",
            base_branch="main",
        )

    assert str(exc_info.value) == "validation_rule_failed"
    assert fake_logs.failed
    result = fake_logs.failed[0]["result"]
    assert result["error"] == "validation_rule_failed"
    assert result["target"]["class_id"] == "Ticket"
    assert result["target"]["instance_id"] == "t-1"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_submission_and_validation_rules_passes_for_valid_inputs() -> None:
    worker, fake_logs = _build_worker()

    await worker._enforce_submission_and_validation_rules(
        action_log_id="log-5",
        audit_policy=None,
        spec={
            "submission_criteria": "target.status == 'OPEN' and user.role == 'Owner'",
            "validation_rules": [
                {"type": "assert", "scope": "action", "expr": "input.amount > 0"},
                {"type": "assert", "scope": "each_target", "expr": "target.status == 'OPEN'"},
            ],
        },
        submitted_by="alice",
        actor_role="Owner",
        input_payload={"amount": 1},
        loaded_targets=[_loaded_target(status="OPEN")],
        db_name="demo",
        base_branch="main",
    )

    assert not fake_logs.failed
