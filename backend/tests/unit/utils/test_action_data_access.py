from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared.utils.action_data_access import evaluate_action_target_data_access


class _StubDatasetRegistry:
    def __init__(
        self,
        *,
        policy_by_class: dict[str, object] | None = None,
        policy_by_scope_class: dict[tuple[str, str], object] | None = None,
        raise_for: set[str] | None = None,
    ) -> None:
        self._policy_by_class = policy_by_class or {}
        self._policy_by_scope_class = policy_by_scope_class or {}
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
        scoped = self._policy_by_scope_class.get((scope, subject_id))
        if scoped is not None:
            return scoped
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluate_action_target_data_access_edit_denied_by_object_edit_policy() -> None:
    registry = _StubDatasetRegistry(
        policy_by_scope_class={
            ("object_edit", "Ticket"): SimpleNamespace(
                policy={"effect": "ALLOW", "principals": ["role:DataEngineer"]}
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
                "base_state": {"status": "OPEN"},
                "changes": {"set": {"status": "CLOSED"}, "unset": []},
            }
        ],
        enforce_data_access_policy=False,
        principal_tags={"user:alice", "role:DomainModeler"},
        enforce_object_edit_policy=True,
    )
    assert report.denied == []
    assert report.unverifiable == []
    assert report.edit_unverifiable == []
    assert report.edit_denied == [{"class_id": "Ticket", "instance_id": "t-1", "scope": "object_edit", "fields": "status"}]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluate_action_target_data_access_attachment_policy_missing_is_unverifiable() -> None:
    registry = _StubDatasetRegistry(policy_by_scope_class={})
    report = await evaluate_action_target_data_access(
        dataset_registry=registry,
        db_name="demo",
        targets=[
            {
                "class_id": "Ticket",
                "instance_id": "t-1",
                "base_state": {"receipt": "old.pdf"},
                "changes": {"set": {"receipt": "new.pdf"}, "unset": []},
                "field_types": {"receipt": "attachment"},
            }
        ],
        enforce_data_access_policy=False,
        principal_tags={"user:alice", "role:DataEngineer"},
        enforce_attachment_edit_policy=True,
        fail_on_missing_edit_policy=True,
    )
    assert report.edit_denied == []
    assert report.edit_unverifiable == [
        {"class_id": "Ticket", "instance_id": "t-1", "scope": "attachment_edit", "fields": "receipt"}
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluate_action_target_data_access_object_set_policy_enforced_for_link_changes() -> None:
    registry = _StubDatasetRegistry(
        policy_by_scope_class={
            ("object_set_edit", "Ticket"): SimpleNamespace(
                policy={"effect": "ALLOW", "principals": ["role:DataEngineer"]}
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
                "base_state": {"status": "OPEN"},
                "changes": {"set": {}, "unset": [], "link_add": [{"field": "watchers", "value": "User:u1"}]},
            }
        ],
        enforce_data_access_policy=False,
        principal_tags={"user:alice", "role:DomainModeler"},
        enforce_object_set_edit_policy=True,
    )
    assert report.edit_unverifiable == []
    assert report.edit_denied == [
        {"class_id": "Ticket", "instance_id": "t-1", "scope": "object_set_edit", "fields": "__links__"}
    ]
