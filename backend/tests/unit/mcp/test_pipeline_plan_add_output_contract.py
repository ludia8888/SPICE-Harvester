from __future__ import annotations

import pytest

from mcp_servers.pipeline_tools.plan_tools import _plan_add_output
from shared.services.pipeline.pipeline_plan_builder import add_input, new_plan


@pytest.mark.unit
@pytest.mark.asyncio
async def test_plan_add_output_accepts_dataset_canonical_fields() -> None:
    plan = new_plan(goal="mcp output", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan

    result = await _plan_add_output(
        None,
        {
            "plan": plan,
            "input_node_id": "in_1",
            "output_name": "out_1",
            "output_kind": "dataset",
            "write_mode": "append_only_new_rows",
            "primary_key_columns": ["id"],
            "output_format": "avro",
            "partition_by": ["ds"],
        },
    )

    output_entry = result["plan"]["outputs"][0]
    metadata = output_entry["output_metadata"]
    assert metadata["write_mode"] == "append_only_new_rows"
    assert metadata["primary_key_columns"] == ["id"]
    assert metadata["output_format"] == "avro"
    assert metadata["partition_by"] == ["ds"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_plan_add_output_normalizes_dataset_camel_case_aliases() -> None:
    plan = new_plan(goal="mcp output", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan

    result = await _plan_add_output(
        None,
        {
            "plan": plan,
            "input_node_id": "in_1",
            "output_name": "out_1",
            "output_kind": "dataset",
            "writeMode": "snapshot_replace_and_remove",
            "primaryKeyColumns": ["id"],
            "postFilteringColumn": "is_deleted",
            "outputFormat": "orc",
            "partitionBy": ["region"],
        },
    )

    metadata = result["plan"]["outputs"][0]["output_metadata"]
    assert metadata["write_mode"] == "snapshot_replace_and_remove"
    assert metadata["primary_key_columns"] == ["id"]
    assert metadata["post_filtering_column"] == "is_deleted"
    assert metadata["output_format"] == "orc"
    assert metadata["partition_by"] == ["region"]
