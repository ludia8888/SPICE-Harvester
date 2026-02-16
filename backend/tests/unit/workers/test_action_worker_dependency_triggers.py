from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest

import action_worker.main as action_worker_module
from action_worker.main import ActionWorker
from shared.services.registries.action_log_registry import (
    ActionDependencyRecord,
    ActionLogRecord,
    ActionLogStatus,
)


def _log(
    *,
    action_log_id: str,
    db_name: str,
    status: str,
    action_type_id: str = "ApproveTicket",
) -> ActionLogRecord:
    now = datetime.now(timezone.utc)
    return ActionLogRecord(
        action_log_id=action_log_id,
        db_name=db_name,
        action_type_id=action_type_id,
        action_type_rid=f"action_type:{action_type_id}",
        resource_rid=None,
        ontology_commit_id="commit-1",
        input={"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
        status=status,
        result=None,
        correlation_id="corr-1",
        submitted_by="alice",
        submitted_at=now,
        finished_at=None,
        writeback_target={"repo": "ontology-writeback", "branch": "writeback-demo"},
        writeback_commit_id=None,
        action_applied_event_id=None,
        action_applied_seq=None,
        metadata={"__submit_context": {"base_branch": "main", "overlay_branch": "writeback-demo"}},
        updated_at=now,
    )


class _FakeActionLogs:
    def __init__(
        self,
        *,
        logs: Dict[str, ActionLogRecord],
        children_by_parent: Dict[str, List[str]],
        dependencies_by_child: Dict[str, List[ActionDependencyRecord]],
    ) -> None:
        self.logs = logs
        self.children_by_parent = children_by_parent
        self.dependencies_by_child = dependencies_by_child
        self.failed: List[str] = []

    async def get_log(self, *, action_log_id: str) -> ActionLogRecord | None:
        return self.logs.get(action_log_id)

    async def list_dependent_children(self, *, parent_action_log_id: str) -> List[str]:
        return list(self.children_by_parent.get(parent_action_log_id, []))

    async def list_dependency_status_for_child(self, *, child_action_log_id: str) -> List[ActionDependencyRecord]:
        return list(self.dependencies_by_child.get(child_action_log_id, []))

    async def mark_failed(self, *, action_log_id: str, result: Dict[str, Any] | None = None, finished_at: Any = None) -> None:  # noqa: ANN401
        _ = (result, finished_at)
        self.failed.append(action_log_id)


class _FakeEventStore:
    def __init__(self) -> None:
        self.events: List[Any] = []

    async def append_event(self, envelope: Any) -> str:
        self.events.append(envelope)
        return str(getattr(envelope, "event_id", ""))


def _worker(fake_logs: _FakeActionLogs) -> ActionWorker:
    worker = object.__new__(ActionWorker)
    worker.action_logs = fake_logs
    return worker


@pytest.mark.unit
@pytest.mark.asyncio
async def test_trigger_dependent_actions_emits_child_command(monkeypatch: pytest.MonkeyPatch) -> None:
    parent_id = "00000000-0000-0000-0000-000000000001"
    child_id = "00000000-0000-0000-0000-000000000002"
    fake_logs = _FakeActionLogs(
        logs={
            parent_id: _log(action_log_id=parent_id, db_name="demo", status=ActionLogStatus.SUCCEEDED.value),
            child_id: _log(action_log_id=child_id, db_name="demo", status=ActionLogStatus.PENDING.value),
        },
        children_by_parent={parent_id: [child_id]},
        dependencies_by_child={
            child_id: [
                ActionDependencyRecord(
                    child_action_log_id=child_id,
                    parent_action_log_id=parent_id,
                    trigger_on="SUCCEEDED",
                    parent_status=ActionLogStatus.SUCCEEDED.value,
                )
            ]
        },
    )
    fake_event_store = _FakeEventStore()
    monkeypatch.setattr(action_worker_module, "event_store", fake_event_store)

    worker = _worker(fake_logs)
    await worker._trigger_dependent_actions(db_name="demo", parent_action_log_id=parent_id)

    assert not fake_logs.failed
    assert len(fake_event_store.events) == 1
    command_data = fake_event_store.events[0].data
    assert command_data["action_log_id"] == child_id


@pytest.mark.unit
@pytest.mark.asyncio
async def test_trigger_dependent_actions_marks_failed_when_dependency_impossible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_id = "00000000-0000-0000-0000-000000000011"
    child_id = "00000000-0000-0000-0000-000000000012"
    fake_logs = _FakeActionLogs(
        logs={
            parent_id: _log(action_log_id=parent_id, db_name="demo", status=ActionLogStatus.FAILED.value),
            child_id: _log(action_log_id=child_id, db_name="demo", status=ActionLogStatus.PENDING.value),
        },
        children_by_parent={parent_id: [child_id]},
        dependencies_by_child={
            child_id: [
                ActionDependencyRecord(
                    child_action_log_id=child_id,
                    parent_action_log_id=parent_id,
                    trigger_on="SUCCEEDED",
                    parent_status=ActionLogStatus.FAILED.value,
                )
            ]
        },
    )
    fake_event_store = _FakeEventStore()
    monkeypatch.setattr(action_worker_module, "event_store", fake_event_store)

    worker = _worker(fake_logs)
    await worker._trigger_dependent_actions(db_name="demo", parent_action_log_id=parent_id)

    assert fake_logs.failed == [child_id]
    assert not fake_event_store.events


@pytest.mark.unit
@pytest.mark.asyncio
async def test_trigger_dependent_actions_waits_until_all_parents_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_id = "00000000-0000-0000-0000-000000000021"
    child_id = "00000000-0000-0000-0000-000000000022"
    fake_logs = _FakeActionLogs(
        logs={
            parent_id: _log(action_log_id=parent_id, db_name="demo", status=ActionLogStatus.SUCCEEDED.value),
            child_id: _log(action_log_id=child_id, db_name="demo", status=ActionLogStatus.PENDING.value),
        },
        children_by_parent={parent_id: [child_id]},
        dependencies_by_child={
            child_id: [
                ActionDependencyRecord(
                    child_action_log_id=child_id,
                    parent_action_log_id=parent_id,
                    trigger_on="SUCCEEDED",
                    parent_status=ActionLogStatus.SUCCEEDED.value,
                ),
                ActionDependencyRecord(
                    child_action_log_id=child_id,
                    parent_action_log_id="00000000-0000-0000-0000-000000000099",
                    trigger_on="COMPLETED",
                    parent_status=ActionLogStatus.PENDING.value,
                ),
            ]
        },
    )
    fake_event_store = _FakeEventStore()
    monkeypatch.setattr(action_worker_module, "event_store", fake_event_store)

    worker = _worker(fake_logs)
    await worker._trigger_dependent_actions(db_name="demo", parent_action_log_id=parent_id)

    assert not fake_logs.failed
    assert not fake_event_store.events
