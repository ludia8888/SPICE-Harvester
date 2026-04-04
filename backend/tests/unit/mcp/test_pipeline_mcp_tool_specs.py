from __future__ import annotations

import pytest

from mcp_servers.pipeline_mcp_tool_specs import build_pipeline_mcp_tool_specs


@pytest.mark.unit
def test_pipeline_mcp_tool_specs_include_core_tools() -> None:
    specs = build_pipeline_mcp_tool_specs()
    names = {spec["name"] for spec in specs}

    assert "plan_new" in names
    assert "plan_add_input" in names
    assert "list_schema_changes" in names


@pytest.mark.unit
def test_pipeline_execution_tool_specs_allow_optional_session_id() -> None:
    specs = {spec["name"]: spec for spec in build_pipeline_mcp_tool_specs()}

    for tool_name in (
        "pipeline_create_from_plan",
        "pipeline_update_from_plan",
        "pipeline_preview_wait",
        "pipeline_build_wait",
        "pipeline_deploy_promote_build",
    ):
        schema = specs[tool_name]["inputSchema"]
        assert "session_id" in schema["properties"]
