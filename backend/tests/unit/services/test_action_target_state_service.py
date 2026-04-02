from __future__ import annotations

from types import SimpleNamespace

import pytest

from oms.services.action_target_state_service import (
    ActionTargetStateLoadError,
    load_action_target_states,
)
from shared.errors.infra_errors import StorageUnavailableError


class _FakeStorage:
    def __init__(self, *, state: object = None, list_exception: Exception | None = None) -> None:
        self._state = {} if state is None else state
        self._list_exception = list_exception
        self.list_calls = 0
        self.replay_calls = 0

    async def list_command_files(self, *, bucket: str, prefix: str) -> list[str]:
        _ = bucket, prefix
        self.list_calls += 1
        if self._list_exception is not None:
            raise self._list_exception
        return ["cmd.json"]

    async def replay_instance_state(self, *, bucket: str, command_files: list[str], strict: bool = False) -> object:
        _ = bucket, command_files, strict
        self.replay_calls += 1
        return self._state


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_action_target_states_reuses_contract_and_state_for_duplicate_targets() -> None:
    storage = _FakeStorage(state={"status": "OPEN"})
    contract_calls = 0
    seen_branches: list[str] = []

    async def _load_contract(*, db_name: str, class_id: str, branch: str, resources: object) -> object:
        nonlocal contract_calls
        _ = db_name, class_id, branch, resources
        contract_calls += 1
        seen_branches.append(branch)
        return SimpleNamespace(interfaces=["IApproval"], field_types={"status": "string"})

    loaded = await load_action_target_states(
        db_name="demo",
        base_branch="main",
        contract_branch="commit-1",
        compiled_targets=[
            SimpleNamespace(class_id="Ticket", instance_id="t1"),
            SimpleNamespace(class_id="Ticket", instance_id="t1"),
        ],
        resources=object(),
        storage=storage,
        required_interfaces={"IApproval"},
        load_runtime_contract_fn=_load_contract,
    )

    assert len(loaded) == 2
    assert loaded[0].base_state == {"status": "OPEN"}
    assert loaded[0].field_types == {"status": "string"}
    assert loaded[1].base_state == {"status": "OPEN"}
    assert contract_calls == 1
    assert seen_branches == ["commit-1"]
    assert storage.list_calls == 1
    assert storage.replay_calls == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_action_target_states_raises_when_contract_missing() -> None:
    async def _load_contract(*, db_name: str, class_id: str, branch: str, resources: object) -> object:
        _ = db_name, class_id, branch, resources
        return None

    with pytest.raises(ActionTargetStateLoadError) as exc_info:
        await load_action_target_states(
            db_name="demo",
            base_branch="main",
            compiled_targets=[SimpleNamespace(class_id="Ticket", instance_id="t1")],
            resources=object(),
            storage=_FakeStorage(state={"status": "OPEN"}),
            load_runtime_contract_fn=_load_contract,
        )

    assert exc_info.value.kind == "target_class_not_found"
    assert exc_info.value.details["class_id"] == "Ticket"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_action_target_states_raises_when_required_interfaces_missing() -> None:
    async def _load_contract(*, db_name: str, class_id: str, branch: str, resources: object) -> object:
        _ = db_name, class_id, branch, resources
        return SimpleNamespace(interfaces=["IComment"], field_types={})

    with pytest.raises(ActionTargetStateLoadError) as exc_info:
        await load_action_target_states(
            db_name="demo",
            base_branch="main",
            compiled_targets=[SimpleNamespace(class_id="Ticket", instance_id="t1")],
            resources=object(),
            storage=_FakeStorage(state={"status": "OPEN"}),
            required_interfaces={"IApproval"},
            load_runtime_contract_fn=_load_contract,
        )

    assert exc_info.value.kind == "required_interfaces_missing"
    assert exc_info.value.details["missing_interfaces"] == ["IApproval"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_action_target_states_raises_when_base_state_missing() -> None:
    async def _load_contract(*, db_name: str, class_id: str, branch: str, resources: object) -> object:
        _ = db_name, class_id, branch, resources
        return SimpleNamespace(interfaces=["IApproval"], field_types={})

    with pytest.raises(ActionTargetStateLoadError) as exc_info:
        await load_action_target_states(
            db_name="demo",
            base_branch="main",
            compiled_targets=[SimpleNamespace(class_id="Ticket", instance_id="t1")],
            resources=object(),
            storage=_FakeStorage(state={}),
            load_runtime_contract_fn=_load_contract,
        )

    assert exc_info.value.kind == "base_state_not_found"
    assert exc_info.value.details["instance_id"] == "t1"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_action_target_states_raises_when_storage_is_unavailable() -> None:
    async def _load_contract(*, db_name: str, class_id: str, branch: str, resources: object) -> object:
        _ = db_name, class_id, branch, resources
        return SimpleNamespace(interfaces=["IApproval"], field_types={})

    with pytest.raises(ActionTargetStateLoadError) as exc_info:
        await load_action_target_states(
            db_name="demo",
            base_branch="main",
            compiled_targets=[SimpleNamespace(class_id="Ticket", instance_id="t1")],
            resources=object(),
            storage=_FakeStorage(list_exception=RuntimeError("storage down")),
            load_runtime_contract_fn=_load_contract,
        )

    assert exc_info.value.kind == "storage_unavailable"
    assert exc_info.value.details["base_branch"] == "main"
    assert isinstance(exc_info.value.__cause__, StorageUnavailableError)
