from __future__ import annotations

import pytest

from oms.services import action_submit_service as action_submit_module


class _Deployments:
    async def get_latest_deployed_commit(self, **kwargs):  # noqa: ANN003, ANN201
        _ = kwargs
        return {"ontology_commit_id": "commit-1"}


class _Resources:
    async def get_resource(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        _ = args, kwargs
        return {
            "spec": {
                "writeback_target": {
                    "repo": "repo",
                    "branch": "main",
                }
            },
            "metadata": {},
        }


class _EventStore:
    async def append_event(self, _envelope) -> None:
        raise RuntimeError("append failed")


class _FakeActionLogRegistry:
    instances: list["_FakeActionLogRegistry"] = []

    def __init__(self) -> None:
        self.created: list[dict] = []
        self.failed: list[dict] = []
        self.closed = False
        self.__class__.instances.append(self)

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def create_log(self, **kwargs) -> None:  # noqa: ANN003
        self.created.append(kwargs)

    async def mark_failed(self, **kwargs) -> None:  # noqa: ANN003
        self.failed.append(kwargs)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_submit_action_marks_log_failed_when_command_append_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FakeActionLogRegistry.instances.clear()

    async def _allow_action(**kwargs):  # noqa: ANN003, ANN201
        _ = kwargs
        return "editor"

    monkeypatch.setattr(action_submit_module, "ActionLogRegistry", _FakeActionLogRegistry)
    monkeypatch.setattr(action_submit_module, "enforce_action_permission", _allow_action)
    monkeypatch.setattr(action_submit_module, "validate_action_input", lambda **kwargs: kwargs["payload"])
    monkeypatch.setattr(action_submit_module, "audit_action_log_input", lambda payload, audit_policy=None: payload)
    monkeypatch.setattr(action_submit_module, "resolve_action_permission_profile", lambda spec: {"spec": spec})
    monkeypatch.setattr(
        action_submit_module,
        "requires_action_data_access_enforcement",
        lambda **kwargs: False,
    )

    with pytest.raises(RuntimeError, match="append failed"):
        await action_submit_module.submit_action_request(
            db_name="demo",
            action_type_id="ApproveTicket",
            request_payload={
                "input": {},
                "metadata": {"user_id": "alice"},
            },
            base_branch_alias=None,
            event_store=_EventStore(),
            deployments_factory=_Deployments,
            resources_factory=_Resources,
            compile_action_change_shape_fn=lambda implementation, input_payload: [],
        )

    registry = _FakeActionLogRegistry.instances[-1]
    assert len(registry.created) == 1
    assert len(registry.failed) == 1
    assert registry.failed[0]["action_log_id"] == str(registry.created[0]["action_log_id"])
    assert registry.closed is True
