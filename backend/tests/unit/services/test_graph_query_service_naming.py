from __future__ import annotations

from typing import Any, Dict

import pytest

from bff.services.graph_query_service import (
    _resolve_graph_branches,
    execute_graph_query,
    execute_multi_hop_query,
    execute_simple_graph_query,
)
from shared.models.graph_query import GraphHop, GraphQueryRequest, SimpleGraphQueryRequest


class _GraphServiceSimpleStub:
    async def simple_graph_query(self, **kwargs):  # noqa: ANN003, ANN201
        _ = kwargs
        return {
            "documents": [
                {
                    "id": "Customer/c1",
                    "type": "Customer",
                    "data": {"name": "Alice"},
                }
            ],
            "nodes": [],
            "edges": [],
            "warnings": [],
            "count": 1,
        }


class _GraphServiceMultiHopStub:
    async def multi_hop_query(self, **kwargs):  # noqa: ANN003, ANN201
        _ = kwargs
        return {
            "nodes": [
                {
                    "id": "Customer/c1",
                    "type": "Customer",
                    "instance_id": "c1",
                    "data": {"name": "Alice"},
                    "data_status": "FULL",
                }
            ],
            "edges": [],
            "warnings": [],
            "count": 1,
            "documents": {},
        }


class _GraphServiceWithProvenanceStub:
    async def multi_hop_query(self, **kwargs):  # noqa: ANN003, ANN201
        _ = kwargs
        return {
            "nodes": [
                {
                    "id": "Customer/c1",
                    "type": "Customer",
                    "instance_id": "c1",
                    "data": {"name": "Alice"},
                    "data_status": "FULL",
                    "es_ref": {"index": "demo_instances", "id": "c1"},
                }
            ],
            "edges": [],
            "warnings": [],
            "count": 1,
            "documents": {},
            "paths": [],
        }


class _DatasetRegistryStub:
    async def get_access_policy(
        self,
        *,
        db_name: str,
        scope: str,
        subject_type: str,
        subject_id: str,
    ):  # noqa: ANN201
        _ = (db_name, scope, subject_type, subject_id)
        return None


class _LineageStoreStub:
    async def get_latest_edges_to(
        self,
        *,
        to_node_ids: list[str],
        edge_type: str,
        db_name: str,
        branch: str | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        _ = (db_name, branch)
        if edge_type == "event_wrote_graph_document":
            return {
                node_id: {"event_id": "evt-graph-1", "occurred_at": "2026-01-01T00:00:00+00:00"}
                for node_id in to_node_ids
                if node_id.startswith("artifact:graph:")
            }
        if edge_type == "event_materialized_es_document":
            return {
                node_id: {"event_id": "evt-es-1", "occurred_at": "2026-01-01T00:01:00+00:00"}
                for node_id in to_node_ids
                if node_id.startswith("artifact:es:")
            }
        return {}


@pytest.mark.asyncio
async def test_execute_simple_graph_query_uses_foundry_naming() -> None:
    result = await execute_simple_graph_query(
        db_name="demo",
        query=SimpleGraphQueryRequest(class_name="Customer", filters=None, limit=10),
        request=None,  # unused by implementation
        graph_service=_GraphServiceSimpleStub(),
        dataset_registry=_DatasetRegistryStub(),
        base_branch="master",
        overlay_branch=None,
        branch=None,
    )

    assert result["graph_branch"] == "master"
    assert result["read_model_base_branch"] == "master"
    assert "terminus_branch" not in result
    assert "es_base_branch" not in result
    assert "es_overlay_branch" not in result


@pytest.mark.asyncio
async def test_execute_multi_hop_query_uses_foundry_naming() -> None:
    payload = await execute_multi_hop_query(
        db_name="demo",
        query={
            "start_class": "Customer",
            "hops": [],
            "filters": {},
            "include_documents": True,
            "include_audit": False,
            "limit": 10,
        },
        request=None,  # unused by implementation
        graph_service=_GraphServiceMultiHopStub(),
        dataset_registry=_DatasetRegistryStub(),
        base_branch="master",
        overlay_branch=None,
        branch=None,
    )

    data = payload["data"]
    assert data["graph_branch"] == "master"
    assert data["read_model_base_branch"] == "master"
    assert "terminus_branch" not in data
    assert "es_base_branch" not in data
    assert "es_overlay_branch" not in data


@pytest.mark.asyncio
async def test_execute_graph_query_provenance_uses_graph_key() -> None:
    response = await execute_graph_query(
        db_name="demo",
        query=GraphQueryRequest(
            start_class="Customer",
            hops=[GraphHop(predicate="related_to", target_class="Customer", reverse=False)],
            filters={},
            limit=10,
            offset=0,
            include_paths=False,
            include_provenance=True,
            include_documents=True,
            include_audit=False,
        ),
        request=None,  # unused by implementation
        lineage_store=_LineageStoreStub(),
        graph_service=_GraphServiceWithProvenanceStub(),
        dataset_registry=_DatasetRegistryStub(),
        base_branch="master",
        overlay_branch=None,
        branch=None,
    )

    assert response.query.get("graph_branch") == "master"
    assert "terminus_branch" not in response.query
    assert response.nodes
    provenance = response.nodes[0].provenance or {}
    assert "graph" in provenance
    assert "terminus" not in provenance


def test_resolve_graph_branches_skips_virtualization_for_graph_only_queries() -> None:
    ctx = _resolve_graph_branches(
        db_name="demo",
        base_branch="main",
        overlay_branch=None,
        branch=None,
        include_documents=False,
        classes_in_query=["Person"],
    )

    assert ctx.graph_branch == "main"
    assert ctx.read_model_base_branch == "main"
    assert ctx.read_model_overlay_branch is None
    assert ctx.branch_virtualization_active is False
    assert ctx.overlay_required is False


def test_resolve_graph_branches_keeps_virtualization_for_document_queries() -> None:
    ctx = _resolve_graph_branches(
        db_name="demo",
        base_branch="main",
        overlay_branch=None,
        branch=None,
        include_documents=True,
        classes_in_query=["Person"],
    )

    assert ctx.graph_branch == "main"
    assert ctx.read_model_base_branch == "master"
    assert ctx.read_model_overlay_branch == "main"
    assert ctx.branch_virtualization_active is True
    assert ctx.overlay_required is True
