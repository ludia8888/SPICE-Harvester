from __future__ import annotations

import pytest

from shared.models.pipeline_plan import PipelinePlan
from shared.services.pipeline_claim_refuter import refute_pipeline_plan_claims


@pytest.mark.asyncio
async def test_refuter_pk_duplicate_is_hard_failure():
    plan = PipelinePlan.model_validate(
        {
            "goal": "pk check",
            "data_scope": {"db_name": "demo"},
            "definition_json": {
                "nodes": [
                    {
                        "id": "in1",
                        "type": "input",
                        "metadata": {
                            "datasetId": "ds-1",
                            "claims": [
                                {
                                    "id": "pk1",
                                    "kind": "PK",
                                    "severity": "HARD",
                                    "spec": {"key_cols": ["id"], "require_not_null": True},
                                }
                            ],
                        },
                    }
                ],
                "edges": [],
            },
            "outputs": [],
        }
    )
    report = await refute_pipeline_plan_claims(
        plan=plan,
        run_tables={
            "in1": {
                "columns": [{"name": "id"}, {"name": "value"}],
                "rows": [{"id": "a", "value": 1}, {"id": "a", "value": 2}],
            }
        },
    )
    assert report["status"] == "invalid"
    assert report["gate"] == "BLOCK"
    assert report["hard_failures"][0]["claim_id"] == "pk1"
    assert report["hard_failures"][0]["witness"]["description"] == "Duplicate PK key found"


@pytest.mark.asyncio
async def test_refuter_join_functional_right_detects_duplicate_right_matches_left():
    plan = PipelinePlan.model_validate(
        {
            "goal": "join functional",
            "data_scope": {"db_name": "demo"},
            "definition_json": {
                "nodes": [
                    {"id": "left", "type": "input", "metadata": {"datasetId": "ds-left"}},
                    {"id": "right", "type": "input", "metadata": {"datasetId": "ds-right"}},
                    {
                        "id": "j1",
                        "type": "transform",
                        "metadata": {
                            "operation": "join",
                            "joinType": "left",
                            "leftKeys": ["order_id"],
                            "rightKeys": ["order_id"],
                            "claims": [
                                {
                                    "id": "jfunc",
                                    "kind": "JOIN_FUNCTIONAL_RIGHT",
                                    "severity": "HARD",
                                }
                            ],
                        },
                    },
                ],
                "edges": [{"from": "left", "to": "j1"}, {"from": "right", "to": "j1"}],
            },
            "outputs": [],
        }
    )
    report = await refute_pipeline_plan_claims(
        plan=plan,
        run_tables={
            "left": {"columns": [{"name": "order_id"}], "rows": [{"order_id": 10}]},
            "right": {"columns": [{"name": "order_id"}], "rows": [{"order_id": 10}, {"order_id": 10}]},
        },
    )
    assert report["status"] == "invalid"
    assert report["gate"] == "BLOCK"
    assert report["hard_failures"][0]["claim_id"] == "jfunc"
    assert report["hard_failures"][0]["witness"]["description"] == "Non-functional join: left key matches multiple right rows"


@pytest.mark.asyncio
async def test_refuter_cast_success_finds_parse_failure():
    plan = PipelinePlan.model_validate(
        {
            "goal": "cast",
            "data_scope": {"db_name": "demo"},
            "definition_json": {
                "nodes": [
                    {"id": "src", "type": "input", "metadata": {"datasetId": "ds"}},
                    {
                        "id": "c1",
                        "type": "transform",
                        "metadata": {
                            "operation": "cast",
                            "casts": [{"column": "qty", "type": "xsd:integer"}],
                            "claims": [{"id": "cast_ok", "kind": "CAST_SUCCESS", "severity": "HARD"}],
                        },
                    },
                ],
                "edges": [{"from": "src", "to": "c1"}],
            },
            "outputs": [],
        }
    )
    report = await refute_pipeline_plan_claims(
        plan=plan,
        run_tables={
            "src": {"columns": [{"name": "qty"}], "rows": [{"qty": "abc"}]},
        },
    )
    assert report["status"] == "invalid"
    assert report["gate"] == "BLOCK"
    assert report["hard_failures"][0]["claim_id"] == "cast_ok"
    assert report["hard_failures"][0]["witness"]["description"] == "Cast parse failed"


@pytest.mark.asyncio
async def test_refuter_fk_hard_is_downgraded_to_soft():
    plan = PipelinePlan.model_validate(
        {
            "goal": "fk",
            "data_scope": {"db_name": "demo"},
            "definition_json": {
                "nodes": [
                    {
                        "id": "n1",
                        "type": "input",
                        "metadata": {
                            "datasetId": "ds",
                            "claims": [{"id": "fk1", "kind": "FK", "severity": "HARD"}],
                        },
                    }
                ],
                "edges": [],
            },
            "outputs": [],
        }
    )
    report = await refute_pipeline_plan_claims(plan=plan, run_tables={"n1": {"columns": [{"name": "id"}], "rows": []}})
    assert report["status"] == "success"
    assert report["gate"] == "PASS_NOT_REFUTED"
    assert report["hard_failures"] == []
    assert report["soft_warnings"]
    assert report["soft_warnings"][0]["claim_id"] == "fk1"

