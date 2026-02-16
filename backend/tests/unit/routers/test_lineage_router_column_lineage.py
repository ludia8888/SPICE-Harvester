from __future__ import annotations

from types import SimpleNamespace

import pytest

from bff.routers.lineage import (
    _extract_column_lineage_ref_entry,
    _parse_objectify_mapping_spec_ref,
    get_lineage_column_lineage,
)


class _FakeLineageStore:
    def __init__(self, edges):
        self._edges = list(edges)
        self.calls = []

    async def list_edges(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)
        limit = int(kwargs.get("limit") or len(self._edges))
        return self._edges[:limit]


class _FakeObjectifyRegistry:
    def __init__(self, specs_by_id):
        self.specs_by_id = dict(specs_by_id)
        self.calls = []

    async def get_mapping_spec(self, *, mapping_spec_id):  # noqa: ANN003
        self.calls.append(mapping_spec_id)
        return self.specs_by_id.get(mapping_spec_id)


def test_parse_objectify_mapping_spec_ref() -> None:
    spec_id, version = _parse_objectify_mapping_spec_ref("objectify_mapping_spec:map-1:v3")
    assert spec_id == "map-1"
    assert version == 3

    spec_id_raw, version_raw = _parse_objectify_mapping_spec_ref("map-legacy")
    assert spec_id_raw == "map-legacy"
    assert version_raw is None


def test_extract_column_lineage_ref_entry_from_mapping_spec_fields() -> None:
    entry = _extract_column_lineage_ref_entry(
        {
            "mapping_spec_id": "map-2",
            "mapping_spec_version": "4",
            "target_class_id": "Account",
        }
    )

    assert entry is not None
    assert entry["ref"] == "objectify_mapping_spec:map-2:v4"
    assert entry["mapping_spec_id"] == "map-2"
    assert entry["mapping_spec_version"] == 4
    assert entry["target_class_id"] == "Account"


@pytest.mark.asyncio
async def test_get_lineage_column_lineage_resolves_mapping_pairs() -> None:
    lineage_store = _FakeLineageStore(
        edges=[
            {
                "edge_type": "dataset_version_objectified",
                "occurred_at": "2026-02-16T12:00:00+00:00",
                "metadata": {
                    "mapping_spec_id": "map-1",
                    "mapping_spec_version": 2,
                    "target_class_id": "Account",
                },
            },
            {
                "edge_type": "dataset_version_objectified",
                "occurred_at": "2026-02-16T12:10:00+00:00",
                "metadata": {
                    "column_lineage_ref": "objectify_mapping_spec:map-1:v2",
                    "target_class_id": "Account",
                },
            },
        ]
    )
    objectify_registry = _FakeObjectifyRegistry(
        specs_by_id={
            "map-1": SimpleNamespace(
                mapping_spec_id="map-1",
                version=2,
                target_class_id="Account",
                dataset_id="ds-1",
                dataset_branch="main",
                mappings=[
                    {"source_field": "customer_id", "target_field": "account_id"},
                    {"source_field": "customer_name", "target_field": "name"},
                ],
            )
        }
    )

    response = await get_lineage_column_lineage(
        db_name="demo",
        branch="main",
        run_id=None,
        source_field=None,
        target_field=None,
        target_class_id=None,
        since=None,
        until=None,
        edge_limit=5000,
        pair_limit=100,
        lineage_store=lineage_store,
        objectify_registry=objectify_registry,
    )

    assert response["status"] == "success"
    data = response["data"]
    assert data["resolved_ref_count"] == 1
    assert data["unresolved_ref_count"] == 0
    assert data["pair_count"] == 2
    assert data["summary"]["unique_source_field_count"] == 2
    assert data["summary"]["unique_target_field_count"] == 2
    assert data["pairs"][0]["mapping_spec_ids"] == ["map-1"]
    assert set(data["summary"]["unique_target_classes"]) == {"Account"}


@pytest.mark.asyncio
async def test_get_lineage_column_lineage_marks_version_mismatch_unresolved() -> None:
    lineage_store = _FakeLineageStore(
        edges=[
            {
                "edge_type": "dataset_version_objectified",
                "occurred_at": "2026-02-16T12:00:00+00:00",
                "metadata": {
                    "mapping_spec_id": "map-1",
                    "mapping_spec_version": 1,
                    "target_class_id": "Account",
                },
            }
        ]
    )
    objectify_registry = _FakeObjectifyRegistry(
        specs_by_id={
            "map-1": SimpleNamespace(
                mapping_spec_id="map-1",
                version=2,
                target_class_id="Account",
                dataset_id="ds-1",
                dataset_branch="main",
                mappings=[{"source_field": "a", "target_field": "b"}],
            )
        }
    )

    response = await get_lineage_column_lineage(
        db_name="demo",
        branch="main",
        run_id=None,
        source_field=None,
        target_field=None,
        target_class_id=None,
        since=None,
        until=None,
        edge_limit=5000,
        pair_limit=100,
        lineage_store=lineage_store,
        objectify_registry=objectify_registry,
    )

    assert response["status"] == "success"
    data = response["data"]
    assert data["resolved_ref_count"] == 0
    assert data["unresolved_ref_count"] == 1
    assert data["pair_count"] == 0
    assert data["pairs"] == []
    assert data["refs"][0]["status"] == "unresolved"
    assert "version_mismatch" in str(data["refs"][0]["reason"])


@pytest.mark.asyncio
async def test_get_lineage_column_lineage_uses_inline_pairs_when_ref_unresolved() -> None:
    lineage_store = _FakeLineageStore(
        edges=[
            {
                "edge_type": "dataset_version_objectified",
                "occurred_at": "2026-02-16T12:00:00+00:00",
                "metadata": {
                    "mapping_spec_id": "map-1",
                    "mapping_spec_version": 1,
                    "target_class_id": "Account",
                    "column_lineage_pairs": [
                        {"source_field": "customer_id", "target_field": "account_id"},
                        {"sourceField": "customer_name", "targetField": "name"},
                    ],
                },
            }
        ]
    )
    objectify_registry = _FakeObjectifyRegistry(
        specs_by_id={
            "map-1": SimpleNamespace(
                mapping_spec_id="map-1",
                version=2,
                target_class_id="Account",
                dataset_id="ds-1",
                dataset_branch="main",
                mappings=[],
            )
        }
    )

    response = await get_lineage_column_lineage(
        db_name="demo",
        branch="main",
        run_id=None,
        source_field=None,
        target_field=None,
        target_class_id=None,
        since=None,
        until=None,
        edge_limit=5000,
        pair_limit=100,
        lineage_store=lineage_store,
        objectify_registry=objectify_registry,
    )

    assert response["status"] == "success"
    data = response["data"]
    assert data["resolved_ref_count"] == 0
    assert data["unresolved_ref_count"] == 1
    assert data["pair_count"] == 2
    assert {row["source_field"] for row in data["pairs"]} == {"customer_id", "customer_name"}
