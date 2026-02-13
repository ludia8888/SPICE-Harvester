from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest


def _extract_agent_allowed_tools() -> list[str]:
    module_path = Path(__file__).resolve().parents[3] / "bff" / "services" / "pipeline_agent_autonomous_loop.py"
    module = ast.parse(module_path.read_text(encoding="utf-8"))
    for node in module.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id != "_PIPELINE_AGENT_ALLOWED_TOOLS":
                continue
            value = node.value
            if not isinstance(value, (ast.Tuple, ast.List)):
                continue
            tools: list[str] = []
            for item in value.elts:
                if isinstance(item, ast.Constant) and isinstance(item.value, str):
                    tools.append(item.value)
            return tools
    raise AssertionError("_PIPELINE_AGENT_ALLOWED_TOOLS not found")


def _extract_pipeline_mcp_plan_tools() -> list[str]:
    server_path = Path(__file__).resolve().parents[3] / "mcp_servers" / "pipeline_mcp_server.py"
    text = server_path.read_text(encoding="utf-8")
    return sorted(set(re.findall(r'"name":\s*"(plan_[^"]+)"', text)))


@pytest.mark.unit
def test_agent_allowed_tools_include_advanced_plan_tools() -> None:
    allowed = set(_extract_agent_allowed_tools())
    required = {
        "plan_add_split",
        "plan_add_geospatial",
        "plan_add_pattern_mining",
        "plan_add_stream_join",
    }
    missing = sorted(required - allowed)
    assert not missing, f"missing advanced plan tools in agent allowlist: {missing}"


@pytest.mark.unit
def test_agent_allowed_tools_cover_pipeline_plan_tools() -> None:
    allowed = set(_extract_agent_allowed_tools())
    plan_tools = set(_extract_pipeline_mcp_plan_tools())
    missing = sorted(plan_tools - allowed)
    assert not missing, f"pipeline plan tools exposed by MCP but blocked by agent allowlist: {missing}"
