from __future__ import annotations

import sys
import types

import pytest


@pytest.mark.asyncio
async def test_ontology_search_classes_matches_id_only_property_names(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeServer:
        def __init__(self, name: str) -> None:
            self.name = name

        def list_tools(self):
            def _decorator(fn):
                return fn

            return _decorator

        def call_tool(self):
            def _decorator(fn):
                return fn

            return _decorator

    fake_mcp = types.ModuleType("mcp")
    fake_server_mod = types.ModuleType("mcp.server")
    fake_stdio_mod = types.ModuleType("mcp.server.stdio")
    fake_types_mod = types.ModuleType("mcp.types")
    fake_server_mod.InitializationOptions = object
    fake_server_mod.Server = _FakeServer
    fake_stdio_mod.stdio_server = object()
    fake_types_mod.ServerCapabilities = dict
    fake_types_mod.Tool = dict
    fake_types_mod.ToolsCapability = dict
    monkeypatch.setitem(sys.modules, "mcp", fake_mcp)
    monkeypatch.setitem(sys.modules, "mcp.server", fake_server_mod)
    monkeypatch.setitem(sys.modules, "mcp.server.stdio", fake_stdio_mod)
    monkeypatch.setitem(sys.modules, "mcp.types", fake_types_mod)

    from mcp_servers import ontology_mcp_server as target

    class _FakeOMSClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return None

        async def list_ontologies(self, db_name: str, branch: str = "main"):
            assert db_name == "demo"
            assert branch == "main"
            return {
                "data": [
                    {
                        "id": "Order",
                        "label": "Order",
                        "properties": [
                            {"id": "order_id", "label": "Order ID"},
                            {"name": "display_name", "label": "Display Name"},
                        ],
                    }
                ]
            }

    monkeypatch.setattr("bff.services.oms_client.OMSClient", _FakeOMSClient)

    server = target.OntologyMCPServer()
    result = await server._tool_ontology_search_classes({"db_name": "demo", "query": "order_id"})

    assert result["status"] == "success"
    assert result["result_count"] == 1
    assert result["results"] == [
        {
            "id": "Order",
            "label": "Order",
            "parent_class": None,
            "property_count": 2,
        }
    ]
