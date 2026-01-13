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
        max_payload_bytes=500000,
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
async def test_pipeline_create_requires_simulate_definition_first() -> None:
    tool_registry = _StubToolRegistry(
        policies=[
            _policy(
                tool_id="pipelines.simulate_definition",
                method="POST",
                path="/api/v1/pipelines/simulate-definition",
                risk_level="read",
                requires_approval=False,
                requires_idempotency_key=True,
            ),
            _policy(
                tool_id="pipelines.create",
                method="POST",
                path="/api/v1/pipelines",
                risk_level="write",
                requires_approval=True,
                requires_idempotency_key=True,
            ),
        ]
    )

    plan = AgentPlan.model_validate(
        {
            "plan_id": "00000000-0000-0000-0000-000000000001",
            "goal": "Create a pipeline",
            "requires_approval": True,
            "risk_level": "write",
            "data_scope": {"db_name": "demo", "branch": "main"},
            "steps": [
                {
                    "step_id": "create_pipeline",
                    "tool_id": "pipelines.create",
                    "method": "POST",
                    "path_params": {},
                    "query": {},
                    "body": {
                        "db_name": "demo",
                        "branch": "main",
                        "name": "demo-pipeline",
                        "pipeline_type": "batch",
                        "definition_json": {"nodes": [{"id": "n1", "type": "output"}], "edges": []},
                    },
                    "produces": [],
                    "consumes": [],
                    "requires_approval": True,
                    "idempotency_key": "k1",
                }
            ],
        }
    )

    result = await validate_agent_plan(plan=plan, tool_registry=tool_registry)  # type: ignore[arg-type]

    assert any("requires prior pipelines.simulate_definition" in msg for msg in result.errors)
    assert any(d.code == "pipeline_simulate_first_required" for d in result.compilation_report.diagnostics)
    assert any(p.patch_id.startswith("insert_pipeline_simulate_before.") for p in result.compilation_report.patches)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_build_requires_preview_first() -> None:
    tool_registry = _StubToolRegistry(
        policies=[
            _policy(
                tool_id="pipelines.preview",
                method="POST",
                path="/api/v1/pipelines/{pipeline_id}/preview",
                risk_level="read",
                requires_approval=False,
                requires_idempotency_key=True,
            ),
            _policy(
                tool_id="pipelines.build",
                method="POST",
                path="/api/v1/pipelines/{pipeline_id}/build",
                risk_level="write",
                requires_approval=True,
                requires_idempotency_key=True,
            ),
        ]
    )

    plan = AgentPlan.model_validate(
        {
            "plan_id": "00000000-0000-0000-0000-000000000002",
            "goal": "Build a pipeline",
            "requires_approval": True,
            "risk_level": "write",
            "data_scope": {"db_name": "demo", "branch": "main"},
            "steps": [
                {
                    "step_id": "build_pipeline",
                    "tool_id": "pipelines.build",
                    "method": "POST",
                    "path_params": {"pipeline_id": "11111111-1111-1111-1111-111111111111"},
                    "query": {},
                    "body": {"branch": "main"},
                    "produces": [],
                    "consumes": [],
                    "requires_approval": True,
                    "idempotency_key": "k2",
                }
            ],
        }
    )

    result = await validate_agent_plan(plan=plan, tool_registry=tool_registry)  # type: ignore[arg-type]

    assert any("requires prior pipelines.preview" in msg for msg in result.errors)
    assert any(d.code == "pipeline_preview_first_required" for d in result.compilation_report.diagnostics)
    assert any(p.patch_id.startswith("insert_pipeline_preview_before.") for p in result.compilation_report.patches)

