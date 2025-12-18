import pytest
from dataclasses import dataclass
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from bff.main import app
from bff.dependencies import get_label_mapper, get_oms_client, get_terminus_service
from shared.dependencies.providers import (
    get_audit_log_store,
    get_lineage_store,
    get_llm_gateway,
    get_redis_service,
)
from shared.models.ai import AIAnswer, AIQueryPlan, AIQueryTool
from shared.services.llm_gateway import LLMCallMeta


@dataclass
class _FakeOMSClient:
    async def list_ontologies(self, db_name: str):
        return {
            "status": "success",
            "data": {
                "ontologies": [
                    {
                        "id": "Customer",
                        "label": "Customer",
                        "properties": [
                            {"name": "name", "display_label": "Name", "type": "xsd:string"},
                        ],
                        "relationships": [],
                    }
                ]
            },
        }


class _FakeGateway:
    def __init__(self, *, plan: AIQueryPlan, answer: AIAnswer | None = None):
        self._plan = plan
        self._answer = answer

    async def complete_json(self, *, task: str, response_model, **kwargs):
        if task == "QUERY_PLAN":
            return response_model.model_validate(self._plan.model_dump(mode="json")), LLMCallMeta(
                provider="mock",
                model="mock",
                cache_hit=False,
                latency_ms=1,
            )
        if task == "QUERY_ANSWER":
            assert self._answer is not None
            return response_model.model_validate(self._answer.model_dump(mode="json")), LLMCallMeta(
                provider="mock",
                model="mock",
                cache_hit=False,
                latency_ms=1,
            )
        raise AssertionError(f"Unexpected task: {task}")


@pytest.fixture
def client():
    return TestClient(app)


def _install_common_overrides(*, llm_gateway):
    fake_redis = AsyncMock()
    fake_redis.get_json.return_value = None
    fake_redis.set_json.return_value = None

    fake_audit = AsyncMock()
    fake_audit.log.return_value = None

    fake_lineage = AsyncMock()

    app.dependency_overrides[get_llm_gateway] = lambda: llm_gateway
    app.dependency_overrides[get_redis_service] = lambda: fake_redis
    app.dependency_overrides[get_audit_log_store] = lambda: fake_audit
    app.dependency_overrides[get_lineage_store] = lambda: fake_lineage
    app.dependency_overrides[get_oms_client] = lambda: _FakeOMSClient()
    # These deps are resolved even when the planner returns "unsupported".
    app.dependency_overrides[get_label_mapper] = lambda: AsyncMock()
    app.dependency_overrides[get_terminus_service] = lambda: AsyncMock()


def test_translate_query_plan_returns_plan(client):
    plan = AIQueryPlan(
        tool=AIQueryTool.unsupported,
        interpretation="현재 스키마로는 자동 질의 변환이 어렵습니다.",
        confidence=0.2,
        warnings=["schema is too small"],
    )

    _install_common_overrides(llm_gateway=_FakeGateway(plan=plan))
    try:
        res = client.post(
            "/api/v1/ai/translate/query-plan/testdb",
            json={"question": "고객 목록 보여줘", "mode": "auto", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 200
    body = res.json()
    assert body["plan"]["tool"] == "unsupported"
    assert "llm" in body


def test_translate_query_plan_enables_paths_when_question_asks_for_path(client):
    plan = AIQueryPlan(
        tool=AIQueryTool.graph_query,
        interpretation="관계 경로를 포함해 탐색합니다.",
        confidence=0.9,
        graph_query={
            "start_class": "Customer",
            "hops": [],
            "filters": None,
            "limit": 10,
            "offset": 0,
            "max_nodes": 100,
            "max_edges": 200,
            "include_paths": False,
            "path_depth_limit": 3,
            "include_documents": True,
            "include_provenance": True,
        },
        warnings=[],
    )

    _install_common_overrides(llm_gateway=_FakeGateway(plan=plan))
    try:
        res = client.post(
            "/api/v1/ai/translate/query-plan/testdb",
            json={"question": "A에서 C까지 왜 연결돼? 경로 보여줘", "mode": "auto", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 200
    body = res.json()
    assert body["plan"]["tool"] == "graph_query"
    assert body["plan"]["graph_query"]["include_paths"] is True


def test_ai_query_unsupported_returns_guidance_templates(client):
    plan = AIQueryPlan(
        tool=AIQueryTool.unsupported,
        interpretation="현재 질문은 자동 변환이 어렵습니다.",
        confidence=0.1,
        warnings=[],
    )

    _install_common_overrides(llm_gateway=_FakeGateway(plan=plan))
    try:
        res = client.post(
            "/api/v1/ai/query/testdb",
            json={"question": "그거 왜 그래?", "mode": "auto", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 200
    body = res.json()
    assert body["plan"]["tool"] == "unsupported"
    assert isinstance(body["answer"]["follow_ups"], list)
    assert len(body["answer"]["follow_ups"]) >= 3


def test_ai_query_label_query_executes_and_answers(client):
    plan = AIQueryPlan(
        tool=AIQueryTool.label_query,
        interpretation="Customer에서 Name이 Alice인 항목을 조회합니다.",
        confidence=0.9,
        query={
            "class_id": "Customer",
            "filters": [{"field": "Name", "operator": "eq", "value": "Alice"}],
            "select": ["Name"],
            "limit": 2,
            "offset": 0,
            "order_by": None,
            "order_direction": "asc",
        },
        warnings=[],
    )
    answer = AIAnswer(
        answer="Name이 Alice인 Customer는 1건입니다.",
        confidence=0.8,
        rationale=None,
        follow_ups=[],
    )

    llm_gateway = _FakeGateway(plan=plan, answer=answer)
    _install_common_overrides(llm_gateway=llm_gateway)

    fake_mapper = AsyncMock()
    fake_mapper.convert_query_to_internal.return_value = {
        "class_id": "Customer",
        "filters": [{"field": "name", "operator": "eq", "value": "Alice"}],
        "select": ["name"],
        "limit": 2,
        "offset": 0,
        "order_by": None,
        "order_direction": "asc",
    }
    fake_mapper.convert_to_display_batch.return_value = [{"Name": "Alice"}]

    fake_terminus = AsyncMock()
    fake_terminus.query_database.return_value = {"data": [{"name": "Alice"}], "count": 1}

    app.dependency_overrides[get_label_mapper] = lambda: fake_mapper
    app.dependency_overrides[get_terminus_service] = lambda: fake_terminus
    try:
        res = client.post(
            "/api/v1/ai/query/testdb",
            json={"question": "Name이 Alice인 고객 찾아줘", "mode": "auto", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 200
    body = res.json()
    assert body["plan"]["tool"] == "label_query"
    assert body["execution"]["total"] == 1
    assert body["execution"]["results"] == [{"Name": "Alice"}]
    assert body["answer"]["answer"] == "Name이 Alice인 Customer는 1건입니다."
