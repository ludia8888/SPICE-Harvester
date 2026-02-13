from __future__ import annotations

import pytest

from mcp_servers.pipeline_tools.plan_tools import _plan_add_udf
from shared.services.pipeline.pipeline_plan_builder import PipelinePlanBuilderError, add_input, new_plan


@pytest.mark.unit
@pytest.mark.asyncio
async def test_plan_add_udf_accepts_reference_fields() -> None:
    plan = new_plan(goal="mcp udf", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan

    result = await _plan_add_udf(
        None,
        {
            "plan": plan,
            "input_node_id": "in_1",
            "udf_id": "udf-normalize-order",
            "udf_version": 5,
        },
    )

    node_id = result["node_id"]
    node = next(item for item in result["plan"]["definition_json"]["nodes"] if item["id"] == node_id)
    metadata = node["metadata"]
    assert metadata["operation"] == "udf"
    assert metadata["udfId"] == "udf-normalize-order"
    assert metadata["udfVersion"] == 5


@pytest.mark.unit
@pytest.mark.asyncio
async def test_plan_add_udf_accepts_camel_case_aliases() -> None:
    plan = new_plan(goal="mcp udf", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan

    result = await _plan_add_udf(
        None,
        {
            "plan": plan,
            "input_node_id": "in_1",
            "udfId": "udf-normalize-order",
            "udfVersion": 2,
        },
    )

    node_id = result["node_id"]
    node = next(item for item in result["plan"]["definition_json"]["nodes"] if item["id"] == node_id)
    metadata = node["metadata"]
    assert metadata["operation"] == "udf"
    assert metadata["udfId"] == "udf-normalize-order"
    assert metadata["udfVersion"] == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_plan_add_udf_requires_pinned_version() -> None:
    plan = new_plan(goal="mcp udf", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan

    with pytest.raises(PipelinePlanBuilderError):
        await _plan_add_udf(
            None,
            {
                "plan": plan,
                "input_node_id": "in_1",
                "udf_id": "udf-normalize-order",
            },
        )
