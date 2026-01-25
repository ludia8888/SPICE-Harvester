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
async def test_refuter_cast_lossless_detects_leading_zero_loss():
    plan = PipelinePlan.model_validate(
        {
            "goal": "cast lossless",
            "data_scope": {"db_name": "demo"},
            "definition_json": {
                "nodes": [
                    {"id": "src", "type": "input", "metadata": {"datasetId": "ds"}},
                    {
                        "id": "c1",
                        "type": "transform",
                        "metadata": {
                            "operation": "cast",
                            "casts": [{"column": "code", "type": "xsd:integer"}],
                            "claims": [
                                {
                                    "id": "lossless_code",
                                    "kind": "CAST_LOSSLESS",
                                    "severity": "HARD",
                                    "spec": {"allowed_normalization": ["trim"]},
                                }
                            ],
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
            "src": {"columns": [{"name": "code"}], "rows": [{"code": "0012"}]},
            "c1": {"columns": [{"name": "code"}], "rows": [{"code": 12}]},
        },
    )
    assert report["status"] == "invalid"
    assert report["gate"] == "BLOCK"
    assert report["hard_failures"][0]["claim_id"] == "lossless_code"
    assert report["hard_failures"][0]["witness"]["description"] == "Round-trip mismatch (information loss)"


@pytest.mark.asyncio
async def test_refuter_cast_lossless_allows_strip_leading_zeros():
    plan = PipelinePlan.model_validate(
        {
            "goal": "cast lossless allow normalization",
            "data_scope": {"db_name": "demo"},
            "definition_json": {
                "nodes": [
                    {"id": "src", "type": "input", "metadata": {"datasetId": "ds"}},
                    {
                        "id": "c1",
                        "type": "transform",
                        "metadata": {
                            "operation": "cast",
                            "casts": [{"column": "code", "type": "xsd:integer"}],
                            "claims": [
                                {
                                    "id": "lossless_code",
                                    "kind": "CAST_LOSSLESS",
                                    "severity": "HARD",
                                    "spec": {"allowed_normalization": ["trim", "strip_leading_zeros"]},
                                }
                            ],
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
            "src": {"columns": [{"name": "code"}], "rows": [{"code": "0012"}]},
            "c1": {"columns": [{"name": "code"}], "rows": [{"code": 12}]},
        },
    )
    assert report["status"] == "success"
    assert report["gate"] == "PASS_NOT_REFUTED"


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


@pytest.mark.asyncio
async def test_refuter_filter_only_nulls_finds_removed_non_null_row():
    plan = PipelinePlan.model_validate(
        {
            "goal": "filter only nulls",
            "data_scope": {"db_name": "demo"},
            "definition_json": {
                "nodes": [
                    {"id": "src", "type": "input", "metadata": {"datasetId": "ds"}},
                    {
                        "id": "f1",
                        "type": "transform",
                        "metadata": {
                            "operation": "filter",
                            "expression": "a is not null",
                            "claims": [
                                {
                                    "id": "only_nulls",
                                    "kind": "FILTER_ONLY_NULLS",
                                    "severity": "HARD",
                                    "spec": {"column": "a"},
                                }
                            ],
                        },
                    },
                ],
                "edges": [{"from": "src", "to": "f1"}],
            },
            "outputs": [],
        }
    )
    # Input contains a non-null row that is missing from output -> witness.
    report = await refute_pipeline_plan_claims(
        plan=plan,
        run_tables={
            "src": {"columns": [{"name": "a"}], "rows": [{"a": 1}, {"a": None}]},
            "f1": {"columns": [{"name": "a"}], "rows": [{"a": None}]},
        },
    )
    assert report["status"] == "invalid"
    assert report["gate"] == "BLOCK"
    assert report["hard_failures"][0]["claim_id"] == "only_nulls"
    assert report["hard_failures"][0]["witness"]["description"] == "Filter removed non-null row"


@pytest.mark.asyncio
async def test_refuter_filter_min_retain_rate_finds_low_retention():
    plan = PipelinePlan.model_validate(
        {
            "goal": "filter retain rate",
            "data_scope": {"db_name": "demo"},
            "definition_json": {
                "nodes": [
                    {"id": "src", "type": "input", "metadata": {"datasetId": "ds"}},
                    {
                        "id": "f1",
                        "type": "transform",
                        "metadata": {
                            "operation": "filter",
                            "expression": "a > 0",
                            "claims": [
                                {
                                    "id": "min_retain",
                                    "kind": "FILTER_MIN_RETAIN_RATE",
                                    "severity": "HARD",
                                    "spec": {"min_rate": 0.5},
                                }
                            ],
                        },
                    },
                ],
                "edges": [{"from": "src", "to": "f1"}],
            },
            "outputs": [],
        }
    )
    report = await refute_pipeline_plan_claims(
        plan=plan,
        run_tables={
            "src": {"columns": [{"name": "a"}], "rows": [{"a": i} for i in range(10)]},
            "f1": {"columns": [{"name": "a"}], "rows": [{"a": 1}]},
        },
    )
    assert report["status"] == "invalid"
    assert report["gate"] == "BLOCK"
    assert report["hard_failures"][0]["claim_id"] == "min_retain"
    assert report["hard_failures"][0]["witness"]["description"] == "Retain rate too low"


@pytest.mark.asyncio
async def test_refuter_union_row_lossless_finds_missing_input_row():
    plan = PipelinePlan.model_validate(
        {
            "goal": "union lossless",
            "data_scope": {"db_name": "demo"},
            "definition_json": {
                "nodes": [
                    {"id": "a", "type": "input", "metadata": {"datasetId": "ds-a"}},
                    {"id": "b", "type": "input", "metadata": {"datasetId": "ds-b"}},
                    {
                        "id": "u1",
                        "type": "transform",
                        "metadata": {
                            "operation": "union",
                            "unionMode": "strict",
                            "claims": [{"id": "union_lossless", "kind": "UNION_ROW_LOSSLESS", "severity": "HARD"}],
                        },
                    },
                ],
                "edges": [{"from": "a", "to": "u1"}, {"from": "b", "to": "u1"}],
            },
            "outputs": [],
        }
    )
    report = await refute_pipeline_plan_claims(
        plan=plan,
        run_tables={
            "a": {"columns": [{"name": "id"}], "rows": [{"id": 1}]},
            "b": {"columns": [{"name": "id"}], "rows": [{"id": 2}]},
            # Output missing the row from B.
            "u1": {"columns": [{"name": "id"}], "rows": [{"id": 1}]},
        },
    )
    assert report["status"] == "invalid"
    assert report["gate"] == "BLOCK"
    assert report["hard_failures"][0]["claim_id"] == "union_lossless"
    assert report["hard_failures"][0]["witness"]["description"] == "Row from input missing in UNION output"
