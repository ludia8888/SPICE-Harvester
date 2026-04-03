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
