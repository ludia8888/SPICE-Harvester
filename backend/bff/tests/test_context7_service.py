from __future__ import annotations

import sys
import types

import pytest

from bff.services import context7_service


class _Manager:
    async def list_tools(self, name: str):  # noqa: ANN201
        assert name == "context7"
        return [{"name": "search"}, {"name": "analyze_ontology"}]


@pytest.mark.asyncio
async def test_check_context7_health_returns_unified_ready_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_module = types.ModuleType("shared.services.mcp_client")
    fake_module.get_mcp_manager = lambda: _Manager()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "shared.services.mcp_client", fake_module)

    payload = await context7_service.check_context7_health(
        client=object(),
        runtime_status={},
    )

    assert payload["status"] == "ready"
    assert payload["connected"] is True
    assert payload["dependency_status"]["context7"] == "ready"
    assert payload["impact_summary"]["hard_down_dependencies"] == []


@pytest.mark.asyncio
async def test_check_context7_health_returns_unified_degraded_surface_on_failure() -> None:
    async def _resolver():  # noqa: ANN202
        raise RuntimeError("context7 unavailable")

    payload = await context7_service.check_context7_health(
        client_resolver=_resolver,
        runtime_status={},
    )

    assert payload["status"] == "degraded"
    assert payload["connected"] is False
    assert payload["dependency_status"]["context7"] == "degraded"
    assert "context7.search" in payload["affected_features"]
    assert payload["impact_summary"]["classifications"]["unavailable"] == 1
