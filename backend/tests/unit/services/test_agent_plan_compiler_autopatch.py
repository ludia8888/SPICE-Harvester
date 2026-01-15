from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from bff.services.agent_plan_compiler import compile_agent_plan
from shared.models.agent_plan import AgentPlanDataScope
from shared.services.agent_tool_registry import AgentToolPolicyRecord
from shared.services.llm_gateway import LLMGateway


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
async def test_compile_agent_plan_auto_applies_server_patches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "mock")

    draft = {
        "plan": {
            "goal": "Preview pipeline",
            "requires_approval": False,
            "risk_level": "read",
            "data_scope": {"db_name": "demo", "branch": "main"},
            "steps": [
                {
                    "step_id": "preview_pipeline",
                    "tool_id": "pipelines.preview",
                    "path_params": {"pipeline_id": "11111111-1111-1111-1111-111111111111"},
                    "query": {},
                    "body": {"limit": 10},
                    "produces": [],
                    "consumes": [],
                    "requires_approval": False,
                }
            ],
        },
        "confidence": 0.9,
        "notes": [],
        "warnings": [],
    }
    monkeypatch.setenv("LLM_MOCK_JSON_AGENT_PLAN_COMPILE_V1", json.dumps(draft))

    tool_registry = _StubToolRegistry(
        policies=[
            _policy(
                tool_id="pipelines.preview",
                method="POST",
                path="/api/v1/pipelines/{pipeline_id}/preview",
                risk_level="read",
                requires_approval=False,
                requires_idempotency_key=True,
            )
        ]
    )

    result = await compile_agent_plan(
        goal="Preview pipeline",
        data_scope=AgentPlanDataScope(db_name="demo", branch="main"),
        answers=None,
        context_pack=None,
        actor="user-1",
        tool_registry=tool_registry,  # type: ignore[arg-type]
        llm_gateway=LLMGateway(),
        redis_service=None,
        audit_store=None,
    )

    assert result.status == "success"
    assert result.plan is not None
    assert result.plan.steps[0].idempotency_key
    assert any("server_auto_applied_patches" in msg for msg in (result.validation_warnings or []))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compile_agent_plan_auto_patches_multiple_steps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "mock")

    draft = {
        "plan": {
            "goal": "Preview pipeline and simulate definition",
            "requires_approval": False,
            "risk_level": "read",
            "data_scope": {"db_name": "demo", "branch": "main"},
            "steps": [
                {
                    "step_id": "preview_pipeline",
                    "tool_id": "pipelines.preview",
                    "path_params": {"pipeline_id": "11111111-1111-1111-1111-111111111111"},
                    "query": {},
                    "body": {"limit": 10},
                    "produces": [],
                    "consumes": [],
                    "requires_approval": False,
                },
                {
                    "step_id": "simulate_definition",
                    "tool_id": "pipelines.simulate_definition",
                    "path_params": {},
                    "query": {},
                    "body": {
                        "db_name": "demo",
                        "branch": "main",
                        "definition_json": {"nodes": [], "edges": []},
                        "limit": 5,
                    },
                    "produces": [],
                    "consumes": [],
                    "requires_approval": False,
                },
            ],
        },
        "confidence": 0.9,
        "notes": [],
        "warnings": [],
    }
    monkeypatch.setenv("LLM_MOCK_JSON_AGENT_PLAN_COMPILE_V1", json.dumps(draft))

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
                tool_id="pipelines.simulate_definition",
                method="POST",
                path="/api/v1/pipelines/simulate-definition",
                risk_level="read",
                requires_approval=False,
                requires_idempotency_key=True,
            ),
        ]
    )

    result = await compile_agent_plan(
        goal="Preview pipeline and simulate definition",
        data_scope=AgentPlanDataScope(db_name="demo", branch="main"),
        answers=None,
        context_pack=None,
        actor="user-1",
        tool_registry=tool_registry,  # type: ignore[arg-type]
        llm_gateway=LLMGateway(),
        redis_service=None,
        audit_store=None,
    )

    assert result.status == "success"
    assert result.plan is not None
    assert len(result.plan.steps) == 2
    assert result.plan.steps[0].idempotency_key
    assert result.plan.steps[1].idempotency_key
