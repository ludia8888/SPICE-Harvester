from __future__ import annotations

import pytest

import oms.services.action_simulation_service as simulation_module
from oms.services.action_simulation_service import (
    ActionSimulationRejected,
    simulate_effects_for_patchset,
)


class _LakeFSStorage:
    pass


class _BaseStorage:
    pass


@pytest.mark.asyncio
async def test_simulate_effects_returns_503_when_authoritative_merge_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BrokenMergeService:
        def __init__(self, *, base_storage, lakefs_storage) -> None:  # noqa: ANN001
            _ = base_storage, lakefs_storage

        async def merge_instance(self, **kwargs):  # noqa: ANN003, ANN201
            _ = kwargs
            raise RuntimeError("corrupt replay")

    monkeypatch.setattr(simulation_module, "WritebackMergeService", _BrokenMergeService)

    with pytest.raises(ActionSimulationRejected) as exc_info:
        await simulate_effects_for_patchset(
            base_storage=_BaseStorage(),  # type: ignore[arg-type]
            lakefs_storage=_LakeFSStorage(),  # type: ignore[arg-type]
            db_name="demo",
            base_branch="main",
            overlay_branch="writeback/main",
            writeback_repo="repo",
            writeback_branch="wb",
            action_log_id="action-1",
            patchset_id="patchset-1",
            targets=[
                {
                    "resource_rid": "spice:Order",
                    "instance_id": "order-1",
                    "lifecycle_id": "lc-1",
                    "applied_changes": {"fields": {"status": {"set": "DONE"}}},
                    "base_token": {},
                }
            ],
        )

    assert exc_info.value.status_code == 503
