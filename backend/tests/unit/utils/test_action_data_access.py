from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared.utils.action_data_access import evaluate_action_target_data_access


class _StubDatasetRegistry:
    def __init__(self, *, policy_by_class: dict[str, object] | None = None, raise_for: set[str] | None = None) -> None:
        self._policy_by_class = policy_by_class or {}
        self._raise_for = raise_for or set()

    async def get_access_policy(
        self,
        *,
        db_name: str,  # noqa: ARG002
        scope: str,  # noqa: ARG002
        subject_type: str,  # noqa: ARG002
        subject_id: str,
    ):
        if subject_id in self._raise_for:
            raise RuntimeError("registry unavailable")
        return self._policy_by_class.get(subject_id)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluate_action_target_data_access_denied() -> None:
    registry = _StubDatasetRegistry(
        policy_by_class={
            "Ticket": SimpleNamespace(
                policy={
                    "row_filters": [{"field": "status", "op": "eq", "value": "OPEN"}],
                    "filter_mode": "allow",
                }
            )
        }
    )
    report = await evaluate_action_target_data_access(
        dataset_registry=registry,
        db_name="demo",
        targets=[
            {
                "class_id": "Ticket",
                "instance_id": "t-1",
                "base_state": {"status": "CLOSED"},
            }
        ],
    )
    assert report.unverifiable == []
    assert report.denied == [{"class_id": "Ticket", "instance_id": "t-1"}]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluate_action_target_data_access_allows_when_policy_missing() -> None:
    registry = _StubDatasetRegistry(policy_by_class={})
    report = await evaluate_action_target_data_access(
        dataset_registry=registry,
        db_name="demo",
        targets=[
            {
                "class_id": "Ticket",
                "instance_id": "t-1",
                "base_state": {"status": "OPEN"},
            }
        ],
    )
    assert report.denied == []
    assert report.unverifiable == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluate_action_target_data_access_marks_unverifiable_on_registry_error() -> None:
    registry = _StubDatasetRegistry(raise_for={"Ticket"})
    report = await evaluate_action_target_data_access(
        dataset_registry=registry,
        db_name="demo",
        targets=[
            {
                "class_id": "Ticket",
                "instance_id": "t-1",
                "base_state": {"status": "OPEN"},
            }
        ],
    )
    assert report.denied == []
    assert report.unverifiable == [{"class_id": "Ticket", "instance_id": "t-1"}]

