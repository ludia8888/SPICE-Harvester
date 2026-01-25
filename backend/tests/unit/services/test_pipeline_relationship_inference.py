from __future__ import annotations

from datetime import datetime, timezone

import pytest

from bff.services.pipeline_context_pack import build_pipeline_context_pack
from shared.services.pipeline_relationship_inference import (
    infer_join_plan_from_context_pack,
    infer_keys_from_context_pack,
)


class FakeDatasetRegistry:
    def __init__(self, datasets):
        self._datasets = list(datasets)

    async def list_datasets(self, *, db_name: str, branch: str | None = None):
        return list(self._datasets)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_relationship_inference_proposes_connected_join_plan_for_three_datasets() -> None:
    customers = {
        "dataset_id": "11111111-1111-1111-1111-111111111111",
        "db_name": "demo",
        "name": "customers",
        "description": None,
        "source_type": "csv_upload",
        "source_ref": "customers.csv",
        "branch": "main",
        "schema_json": {"columns": [{"name": "customer_id", "type": "xsd:string"}, {"name": "name", "type": "xsd:string"}]},
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "latest_commit_id": "c1",
        "artifact_key": None,
        "row_count": 3,
        "sample_json": {
            "columns": [{"name": "customer_id", "type": "xsd:string"}, {"name": "name", "type": "xsd:string"}],
            "rows": [
                {"customer_id": "1", "name": "Alice"},
                {"customer_id": "2", "name": "Bob"},
                {"customer_id": "3", "name": "Cara"},
            ],
        },
        "version_created_at": datetime.now(timezone.utc),
    }
    orders = {
        "dataset_id": "22222222-2222-2222-2222-222222222222",
        "db_name": "demo",
        "name": "orders",
        "description": None,
        "source_type": "csv_upload",
        "source_ref": "orders.csv",
        "branch": "main",
        "schema_json": {
            "columns": [
                {"name": "order_id", "type": "xsd:string"},
                {"name": "customer_id", "type": "xsd:string"},
                {"name": "amount", "type": "xsd:decimal"},
            ]
        },
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "latest_commit_id": "c2",
        "artifact_key": None,
        "row_count": 3,
        "sample_json": {
            "columns": [
                {"name": "order_id", "type": "xsd:string"},
                {"name": "customer_id", "type": "xsd:string"},
                {"name": "amount", "type": "xsd:decimal"},
            ],
            "rows": [
                {"order_id": "o1", "customer_id": "1", "amount": "10.5"},
                {"order_id": "o2", "customer_id": "1", "amount": "7.0"},
                {"order_id": "o3", "customer_id": "2", "amount": "3.0"},
            ],
        },
        "version_created_at": datetime.now(timezone.utc),
    }
    line_items = {
        "dataset_id": "33333333-3333-3333-3333-333333333333",
        "db_name": "demo",
        "name": "line_items",
        "description": None,
        "source_type": "csv_upload",
        "source_ref": "line_items.csv",
        "branch": "main",
        "schema_json": {
            "columns": [
                {"name": "line_item_id", "type": "xsd:string"},
                {"name": "order_id", "type": "xsd:string"},
                {"name": "sku", "type": "xsd:string"},
            ]
        },
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "latest_commit_id": "c3",
        "artifact_key": None,
        "row_count": 4,
        "sample_json": {
            "columns": [
                {"name": "line_item_id", "type": "xsd:string"},
                {"name": "order_id", "type": "xsd:string"},
                {"name": "sku", "type": "xsd:string"},
            ],
            "rows": [
                {"line_item_id": "l1", "order_id": "o1", "sku": "A"},
                {"line_item_id": "l2", "order_id": "o1", "sku": "B"},
                {"line_item_id": "l3", "order_id": "o2", "sku": "C"},
                {"line_item_id": "l4", "order_id": "o3", "sku": "D"},
            ],
        },
        "version_created_at": datetime.now(timezone.utc),
    }

    registry = FakeDatasetRegistry([customers, orders, line_items])
    pack = await build_pipeline_context_pack(
        db_name="demo",
        branch="main",
        dataset_ids=[customers["dataset_id"], orders["dataset_id"], line_items["dataset_id"]],
        dataset_registry=registry,  # type: ignore[arg-type]
        max_sample_rows=10,
        max_join_candidates=20,
        max_pk_candidates=6,
    )

    inferred = infer_join_plan_from_context_pack(pack, max_joins=10, max_edges=30)
    join_plan = inferred.get("join_plan") or []
    assert isinstance(join_plan, list)

    # Expect a spanning-tree sized plan for 3 datasets (2 edges), sample permitting.
    assert len(join_plan) >= 2

    # Join plan should connect all 3 dataset ids.
    connected: set[str] = set()
    for join in join_plan:
        connected.add(str(join.get("left_dataset_id") or "").strip())
        connected.add(str(join.get("right_dataset_id") or "").strip())
        assert str(join.get("join_type") or "").strip().lower() == "left"
    assert customers["dataset_id"] in connected
    assert orders["dataset_id"] in connected
    assert line_items["dataset_id"] in connected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_relationship_inference_enriches_key_inference_payload() -> None:
    dataset_a = {
        "dataset_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "db_name": "demo",
        "name": "parents",
        "description": None,
        "source_type": "csv_upload",
        "source_ref": "parents.csv",
        "branch": "main",
        "schema_json": {"columns": [{"name": "id", "type": "xsd:string"}]},
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "latest_commit_id": "c1",
        "artifact_key": None,
        "row_count": 2,
        "sample_json": {"columns": [{"name": "id", "type": "xsd:string"}], "rows": [{"id": "p1"}, {"id": "p2"}]},
        "version_created_at": datetime.now(timezone.utc),
    }
    dataset_b = {
        "dataset_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "db_name": "demo",
        "name": "children",
        "description": None,
        "source_type": "csv_upload",
        "source_ref": "children.csv",
        "branch": "main",
        "schema_json": {"columns": [{"name": "parent_id", "type": "xsd:string"}, {"name": "value", "type": "xsd:string"}]},
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "latest_commit_id": "c2",
        "artifact_key": None,
        "row_count": 2,
        "sample_json": {
            "columns": [{"name": "parent_id", "type": "xsd:string"}, {"name": "value", "type": "xsd:string"}],
            "rows": [{"parent_id": "p1", "value": "a"}, {"parent_id": "p2", "value": "b"}],
        },
        "version_created_at": datetime.now(timezone.utc),
    }
    registry = FakeDatasetRegistry([dataset_a, dataset_b])
    pack = await build_pipeline_context_pack(
        db_name="demo",
        branch="main",
        dataset_ids=[dataset_a["dataset_id"], dataset_b["dataset_id"]],
        dataset_registry=registry,  # type: ignore[arg-type]
        max_sample_rows=10,
        max_join_candidates=10,
        max_pk_candidates=6,
    )

    inf = infer_keys_from_context_pack(pack, max_pk_candidates=3, max_fk_candidates=10)
    assert "primary_keys" in inf
    assert "foreign_keys" in inf
    assert any(item.get("best_pk") for item in (inf.get("primary_keys") or []))

    fk = inf.get("foreign_keys") or []
    assert isinstance(fk, list)
    if fk:
        assert fk[0].get("direction") == "child_to_parent"
        assert fk[0].get("recommended_join_type") == "left"

