from __future__ import annotations

from datetime import datetime, timezone

import pytest

from bff.services.agent_plan_validation import validate_agent_plan
from shared.models.agent_plan import AgentPlan, AgentPlanStep
from shared.services.agent_tool_registry import AgentToolPolicyRecord


class _FakeToolRegistry:
    def __init__(self, policies: dict[str, AgentToolPolicyRecord]) -> None:
        self._policies = dict(policies)

    async def get_tool_policy(self, *, tool_id: str):
        return self._policies.get(tool_id)

    async def list_tool_policies(self, *, status=None, limit: int = 200):  # noqa: ANN001
        records = list(self._policies.values())
        if status:
            wanted = str(status).strip().upper()
            records = [p for p in records if str(p.status or "").strip().upper() == wanted]
        return records[: int(limit)]


def _policy(*, tool_id: str, method: str, path: str) -> AgentToolPolicyRecord:
    now = datetime.now(timezone.utc)
    return AgentToolPolicyRecord(
        tool_id=tool_id,
        method=method,
        path=path,
        risk_level="write",
        requires_approval=True,
        requires_idempotency_key=True,
        status="ACTIVE",
        roles=[],
        max_payload_bytes=None,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_action_submit_requires_prior_simulate():
    registry = _FakeToolRegistry(
        {
            "actions.submit": _policy(
                tool_id="actions.submit",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/submit",
            )
        }
    )

    sim_id = "11111111-1111-1111-1111-111111111111"
    plan = AgentPlan(
        goal="submit without simulate",
        requires_approval=True,
        steps=[
            AgentPlanStep(
                step_id="s1",
                tool_id="actions.submit",
                path_params={"db_name": "demo", "action_type_id": "ApproveTicket"},
                body={"input": {"ticket_id": "T-1"}, "metadata": {"simulation_id": sim_id}},
                consumes=[f"action_simulation.id.{sim_id}", f"action_simulation.effective.{sim_id}"],
                idempotency_key="k-submit",
            )
        ],
    )

    result = await validate_agent_plan(plan=plan, tool_registry=registry)  # type: ignore[arg-type]
    assert any("requires prior simulate" in e.lower() for e in result.errors)


@pytest.mark.asyncio
async def test_action_submit_ok_when_simulate_precedes_submit():
    registry = _FakeToolRegistry(
        {
            "actions.simulate": _policy(
                tool_id="actions.simulate",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/simulate",
            ),
            "actions.submit": _policy(
                tool_id="actions.submit",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/submit",
            ),
        }
    )

    sim_id = "22222222-2222-2222-2222-222222222222"
    plan = AgentPlan(
        goal="simulate then submit",
        requires_approval=True,
        steps=[
            AgentPlanStep(
                step_id="s1",
                tool_id="actions.simulate",
                path_params={"db_name": "demo", "action_type_id": "ApproveTicket"},
                body={"input": {"ticket_id": "T-1"}, "simulation_id": sim_id},
                produces=[f"action_simulation.id.{sim_id}", f"action_simulation.effective.{sim_id}"],
                idempotency_key="k-sim",
            ),
            AgentPlanStep(
                step_id="s2",
                tool_id="actions.submit",
                path_params={"db_name": "demo", "action_type_id": "ApproveTicket"},
                body={"input": {"ticket_id": "T-1"}, "metadata": {"simulation_id": sim_id}},
                consumes=[f"action_simulation.id.{sim_id}", f"action_simulation.effective.{sim_id}"],
                idempotency_key="k-submit",
            ),
        ],
    )

    result = await validate_agent_plan(plan=plan, tool_registry=registry)  # type: ignore[arg-type]
    assert not any("requires prior simulate" in e.lower() for e in result.errors)


@pytest.mark.asyncio
async def test_action_submit_simulate_mismatch_action_type_id_is_error():
    registry = _FakeToolRegistry(
        {
            "actions.simulate": _policy(
                tool_id="actions.simulate",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/simulate",
            ),
            "actions.submit": _policy(
                tool_id="actions.submit",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/submit",
            ),
        }
    )

    sim_id = "33333333-3333-3333-3333-333333333333"
    plan = AgentPlan(
        goal="simulate different action",
        requires_approval=True,
        steps=[
            AgentPlanStep(
                step_id="s1",
                tool_id="actions.simulate",
                path_params={"db_name": "demo", "action_type_id": "OtherAction"},
                body={"input": {"ticket_id": "T-1"}, "simulation_id": sim_id},
                produces=[f"action_simulation.id.{sim_id}", f"action_simulation.effective.{sim_id}"],
                idempotency_key="k-sim",
            ),
            AgentPlanStep(
                step_id="s2",
                tool_id="actions.submit",
                path_params={"db_name": "demo", "action_type_id": "ApproveTicket"},
                body={"input": {"ticket_id": "T-1"}, "metadata": {"simulation_id": sim_id}},
                consumes=[f"action_simulation.id.{sim_id}", f"action_simulation.effective.{sim_id}"],
                idempotency_key="k-submit",
            ),
        ],
    )

    result = await validate_agent_plan(plan=plan, tool_registry=registry)  # type: ignore[arg-type]
    assert any("requires prior simulate" in e.lower() for e in result.errors)
