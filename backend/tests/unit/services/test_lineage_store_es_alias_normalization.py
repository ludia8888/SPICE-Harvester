from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from shared.services.registries.lineage_store import LineageStore


class _CaptureLineageStore(LineageStore):
    def __init__(self) -> None:
        super().__init__(dsn="postgresql://unused")
        self.upsert_calls: list[dict] = []
        self.insert_calls: list[dict] = []

    async def upsert_node(self, **kwargs):  # noqa: ANN003, ANN201
        self.upsert_calls.append(kwargs)

    async def insert_edge(self, **kwargs):  # noqa: ANN003, ANN201
        self.insert_calls.append(kwargs)
        return kwargs.get("edge_id") or uuid4()


def test_parse_node_id_rejects_legacy_es_artifact_kind() -> None:
    with pytest.raises(ValueError, match="legacy lineage node id alias"):
        LineageStore._parse_node_id("artifact:elasticsearch:demo:main:Customer/c1")


def test_canonicalize_edge_type_keeps_canonical_value() -> None:
    normalized = LineageStore._canonicalize_edge_type("event_materialized_es_document")
    assert normalized == "event_materialized_es_document"


def test_canonicalize_edge_type_rejects_legacy_alias() -> None:
    with pytest.raises(ValueError, match="legacy lineage edge type alias"):
        LineageStore._canonicalize_edge_type("event_wrote_es_document")


def test_canonicalize_edge_type_rejects_legacy_s3_alias() -> None:
    with pytest.raises(ValueError, match="legacy lineage edge type alias"):
        LineageStore._canonicalize_edge_type("event_wrote_s3_object")


def test_infer_branch_from_es_node_id() -> None:
    inferred = LineageStore._infer_branch_from_node_id("artifact:es:demo:main:Customer/c1")
    assert inferred == "main"


@pytest.mark.asyncio
async def test_record_link_rejects_legacy_es_ids_and_edge_type() -> None:
    store = _CaptureLineageStore()
    occurred_at = datetime(2026, 2, 15, 0, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="legacy lineage node id alias"):
        await store.record_link(
            from_node_id="event:cmd-1",
            to_node_id="artifact:elasticsearch:demo:main:Customer/c1",
            edge_type="event_materialized_es_document",
            occurred_at=occurred_at,
            edge_metadata={"db_name": "demo"},
        )

    with pytest.raises(ValueError, match="legacy lineage edge type alias"):
        await store.record_link(
            from_node_id="event:cmd-1",
            to_node_id="artifact:es:demo:main:Customer/c1",
            edge_type="event_wrote_es_document",
            occurred_at=occurred_at,
            edge_metadata={"db_name": "demo"},
        )

    with pytest.raises(ValueError, match="legacy lineage edge type alias"):
        await store.record_link(
            from_node_id="event:cmd-1",
            to_node_id="artifact:s3:spice-bucket:demo/main/file.json",
            edge_type="event_wrote_s3_object",
            occurred_at=occurred_at,
            edge_metadata={"db_name": "demo"},
        )


@pytest.mark.asyncio
async def test_record_link_accepts_canonical_es_ids_and_edge_type() -> None:
    store = _CaptureLineageStore()
    occurred_at = datetime(2026, 2, 15, 0, 0, tzinfo=timezone.utc)

    await store.record_link(
        from_node_id="event:cmd-1",
        to_node_id="artifact:es:demo:main:Customer/c1",
        edge_type="event_materialized_es_document",
        occurred_at=occurred_at,
        edge_metadata={"db_name": "demo"},
    )

    assert len(store.upsert_calls) == 2
    assert store.upsert_calls[1]["node_id"] == "artifact:es:demo:main:Customer/c1"
    assert len(store.insert_calls) == 1
    assert store.insert_calls[0]["edge_type"] == "event_materialized_es_document"
