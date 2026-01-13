from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from bff.services.agent_plan_validation import validate_agent_plan
from shared.models.agent_plan import AgentPlan
from shared.services.agent_tool_registry import AgentToolPolicyRecord


def _policy(
    *,
    tool_id: str,
    method: str,
    path: str,
    risk_level: str,
    requires_approval: bool,
    requires_idempotency_key: bool,
) -> AgentToolPolicyRecord:
    now = datetime.now(timezone.utc)
    return AgentToolPolicyRecord(
        tool_id=tool_id,
        method=method,
        path=path,
        risk_level=risk_level,
        requires_approval=requires_approval,
        requires_idempotency_key=requires_idempotency_key,
        status="ACTIVE",
        roles=[],
        max_payload_bytes=200000,
        created_at=now,
        updated_at=now,
    )


@dataclass
class _StubToolRegistry:
    policies: list[AgentToolPolicyRecord]

    async def list_tool_policies(self, *, status=None, limit: int = 200):  # noqa: ANN001
        items = list(self.policies)
        if status:
            items = [p for p in items if str(p.status).upper() == str(status).upper()]
        return items[: max(0, int(limit))]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_plan_template_rejects_unknown_step_reference() -> None:
    tool_registry = _StubToolRegistry(
        policies=[
            _policy(
                tool_id="pipelines.list",
                method="GET",
                path="/api/v1/pipelines",
                risk_level="read",
                requires_approval=False,
                requires_idempotency_key=False,
            ),
            _policy(
                tool_id="pipelines.get",
                method="GET",
                path="/api/v1/pipelines/{pipeline_id}",
                risk_level="read",
                requires_approval=False,
                requires_idempotency_key=False,
            ),
        ]
    )

    plan = AgentPlan.model_validate(
        {
            "plan_id": "00000000-0000-0000-0000-000000000010",
            "goal": "Get a pipeline",
            "requires_approval": False,
            "risk_level": "read",
            "data_scope": {"db_name": "demo", "branch": "main"},
            "steps": [
                {
                    "step_id": "list_pipelines",
                    "tool_id": "pipelines.list",
                    "method": "GET",
                    "path_params": {},
                    "query": {},
                    "produces": [],
                    "consumes": [],
                    "requires_approval": False,
                },
                {
                    "step_id": "get_pipeline",
                    "tool_id": "pipelines.get",
                    "method": "GET",
                    "path_params": {"pipeline_id": "${steps.unknown.pipeline_id}"},
                    "query": {},
                    "produces": [],
                    "consumes": [],
                    "requires_approval": False,
                },
            ],
        }
    )

    result = await validate_agent_plan(plan=plan, tool_registry=tool_registry)  # type: ignore[arg-type]

    assert any(d.code == "template_step_unknown" for d in result.compilation_report.diagnostics)
    assert any("references unknown template step" in msg for msg in result.errors)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_plan_template_artifact_reference_requires_consumes() -> None:
    tool_registry = _StubToolRegistry(
        policies=[
            _policy(
                tool_id="pipelines.list",
                method="GET",
                path="/api/v1/pipelines",
                risk_level="read",
                requires_approval=False,
                requires_idempotency_key=False,
            ),
        ]
    )

    plan = AgentPlan.model_validate(
        {
            "plan_id": "00000000-0000-0000-0000-000000000011",
            "goal": "Use an artifact template",
            "requires_approval": False,
            "risk_level": "read",
            "data_scope": {"db_name": "demo", "branch": "main"},
            "steps": [
                {
                    "step_id": "step_one",
                    "tool_id": "pipelines.list",
                    "method": "GET",
                    "path_params": {},
                    "query": {},
                    "produces": ["artifact_foo"],
                    "consumes": [],
                    "requires_approval": False,
                },
                {
                    "step_id": "step_two",
                    "tool_id": "pipelines.list",
                    "method": "GET",
                    "path_params": {},
                    "query": {"x": "${artifacts.artifact_foo}"},
                    "produces": [],
                    "consumes": [],
                    "requires_approval": False,
                },
            ],
        }
    )

    result = await validate_agent_plan(plan=plan, tool_registry=tool_registry)  # type: ignore[arg-type]

    assert any(d.code == "template_artifact_missing_consumes" for d in result.compilation_report.diagnostics)
    assert any(p.patch_id.startswith("add_consumes_for_artifact.") for p in result.compilation_report.patches)

