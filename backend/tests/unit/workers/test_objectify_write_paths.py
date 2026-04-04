from __future__ import annotations

from typing import Any, Dict, List

import pytest

from objectify_worker.write_paths import DatasetPrimaryIndexWritePath
from shared.models.objectify_job import ObjectifyJob


def _build_job() -> ObjectifyJob:
    return ObjectifyJob(
        job_id="job-1",
        db_name="demo_db",
        dataset_id="dataset-1",
        dataset_version_id="version-1",
        dataset_branch="main",
        artifact_key="s3://bucket/path/file.csv",
        mapping_spec_id="mapping-1",
        mapping_spec_version=3,
        target_class_id="Order",
        options={},
    )


class _FakeElasticsearchService:
    def __init__(self) -> None:
        self.client = self
        self._indices: set[str] = set()
        self.created_indices: List[Dict[str, Any]] = []
        self.mapping_updates: List[Dict[str, Any]] = []
        self.bulk_calls: List[Dict[str, Any]] = []
        self.deleted_docs: List[tuple[str, str]] = []
        self.refreshed: List[str] = []
        self.search_responses: List[Dict[str, Any]] = []

    async def index_exists(self, index: str) -> bool:
        return index in self._indices

    async def create_index(self, index: str, mappings: Dict[str, Any], settings: Dict[str, Any]) -> bool:
        self._indices.add(index)
        self.created_indices.append({"index": index, "mappings": mappings, "settings": settings})
        return True

    async def update_mapping(self, index: str, properties: Dict[str, Any]) -> bool:
        self.mapping_updates.append({"index": index, "properties": properties})
        return True

    async def bulk_index(
        self,
        index: str,
        documents: List[Dict[str, Any]],
        chunk_size: int = 500,
        refresh: bool = False,
    ) -> Dict[str, int]:
        self.bulk_calls.append(
            {
                "index": index,
                "documents": documents,
                "chunk_size": chunk_size,
                "refresh": refresh,
            }
        )
        return {"success": len(documents), "failed": 0}

    async def delete_document(self, index: str, doc_id: str, refresh: bool = False) -> bool:
        self.deleted_docs.append((index, doc_id))
        return True

    async def refresh_index(self, index: str) -> bool:
        self.refreshed.append(index)
        return True

    async def search(self, index: str, body: Dict[str, Any]) -> Dict[str, Any]:
        _ = (index, body)
        if self.search_responses:
            return self.search_responses.pop(0)
        return {"hits": {"hits": []}}


class _FakeStorageService:
    def __init__(self) -> None:
        self.saved: List[Dict[str, Any]] = []

    async def save_json(self, bucket: str, key: str, payload: Dict[str, Any]) -> None:
        self.saved.append({"bucket": bucket, "key": key, "payload": payload})


@pytest.mark.asyncio
async def test_dataset_primary_write_path_indexes_instances_directly() -> None:
    job = _build_job()
    fake_es = _FakeElasticsearchService()
    writer = DatasetPrimaryIndexWritePath(
        elasticsearch_service=fake_es,  # type: ignore[arg-type]
        chunk_size=100,
        refresh=False,
        prune_stale_on_full=True,
    )

    result = await writer.write_instances(
        job=job,
        instances=[
            {"instance_id": "order-1", "order_id": "order-1", "status": "NEW"},
            {"instance_id": "order-2", "order_id": "order-2", "status": "PAID"},
        ],
        ontology_version={"ref": "branch:main", "commit": "abc123"},
        objectify_pk_fields=["order_id"],
        objectify_instance_id_field="order_id",
    )

    assert result.command_ids == []
    assert result.indexed_instance_ids == ["order-1", "order-2"]
    assert fake_es.created_indices
    assert fake_es.bulk_calls
    docs = fake_es.bulk_calls[0]["documents"]
    assert docs[0]["_id"] == "order-1"
    assert docs[0]["class_id"] == "Order"
    assert docs[0]["data"]["status"] == "NEW"


@pytest.mark.asyncio
async def test_build_document_populates_properties_from_flat_instance() -> None:
    """Properties nested array should be auto-built from flat instance fields."""
    job = _build_job()
    doc = DatasetPrimaryIndexWritePath._build_document(
        job=job,
        instance={
            "instance_id": "order-1",
            "order_id": "order-1",
            "status": "NEW",
            "amount": "150.0",
        },
        instance_id="order-1",
        branch="main",
        ontology_version={"ref": "branch:main", "commit": "abc"},
        now_iso="2026-01-01T00:00:00+00:00",
        event_sequence=1000,
        target_field_types={"order_id": "string", "status": "string", "amount": "decimal"},
    )

    props = doc["properties"]
    assert isinstance(props, list)
    assert len(props) >= 3  # order_id, status, amount

    props_by_name = {p["name"]: p for p in props}
    assert "order_id" in props_by_name
    assert props_by_name["order_id"]["value"] == "order-1"
    assert props_by_name["order_id"]["type"] == "string"
    assert props_by_name["status"]["value"] == "NEW"
    assert props_by_name["amount"]["type"] == "decimal"

    # Skipped keys should not appear
    assert "instance_id" not in props_by_name
    assert "class_id" not in props_by_name
    assert "db_name" not in props_by_name
    assert "branch" not in props_by_name


@pytest.mark.asyncio
async def test_build_document_preserves_existing_properties() -> None:
    """If properties list is already populated, it should not be overwritten."""
    job = _build_job()
    existing_props = [{"name": "custom", "value": "val1"}]
    doc = DatasetPrimaryIndexWritePath._build_document(
        job=job,
        instance={
            "instance_id": "order-1",
            "properties": existing_props,
            "status": "NEW",
        },
        instance_id="order-1",
        branch="main",
        ontology_version=None,
        now_iso="2026-01-01T00:00:00+00:00",
        event_sequence=1000,
    )

    assert doc["properties"] == existing_props


@pytest.mark.asyncio
async def test_build_document_skips_none_values() -> None:
    """Properties with None values should not be included."""
    job = _build_job()
    doc = DatasetPrimaryIndexWritePath._build_document(
        job=job,
        instance={"instance_id": "order-1", "status": "NEW", "notes": None},
        instance_id="order-1",
        branch="main",
        ontology_version=None,
        now_iso="2026-01-01T00:00:00+00:00",
        event_sequence=1000,
    )

    props_by_name = {p["name"]: p for p in doc["properties"]}
    assert "notes" not in props_by_name
    assert "status" in props_by_name


@pytest.mark.asyncio
async def test_write_instances_passes_target_field_types() -> None:
    """write_instances() should forward target_field_types to _build_document()."""
    job = _build_job()
    fake_es = _FakeElasticsearchService()
    writer = DatasetPrimaryIndexWritePath(
        elasticsearch_service=fake_es,  # type: ignore[arg-type]
        chunk_size=100,
        refresh=False,
    )

    await writer.write_instances(
        job=job,
        instances=[{"instance_id": "order-1", "price": "99.5"}],
        ontology_version=None,
        objectify_pk_fields=["order_id"],
        objectify_instance_id_field="order_id",
        target_field_types={"price": "decimal"},
    )

    docs = fake_es.bulk_calls[0]["documents"]
    props = docs[0]["properties"]
    price_prop = [p for p in props if p["name"] == "price"]
    assert len(price_prop) == 1
    assert price_prop[0]["type"] == "decimal"


@pytest.mark.asyncio
async def test_dataset_primary_finalize_prunes_stale_docs_on_full() -> None:
    job = _build_job()
    fake_es = _FakeElasticsearchService()
    fake_es.search_responses = [
        {
            "hits": {
                "hits": [
                    {"_source": {"instance_id": "order-1"}, "_id": "order-1", "sort": ["order-1"]},
                    {"_source": {"instance_id": "order-2"}, "_id": "order-2", "sort": ["order-2"]},
                ]
            }
        },
        {"hits": {"hits": []}},
    ]
    writer = DatasetPrimaryIndexWritePath(
        elasticsearch_service=fake_es,  # type: ignore[arg-type]
        chunk_size=100,
        refresh=True,
        prune_stale_on_full=True,
    )

    summary = await writer.finalize_job(
        job=job,
        execution_mode="full",
        indexed_instance_ids=["order-1"],
    )

    assert summary["stale_prune"]["executed"] is True
    assert summary["stale_prune"]["deleted"] == 1
    assert fake_es.deleted_docs
    assert fake_es.deleted_docs[0][1] == "order-2"


@pytest.mark.asyncio
async def test_dataset_primary_write_path_samples_command_ids_but_tracks_total_files() -> None:
    job = _build_job()
    fake_es = _FakeElasticsearchService()
    fake_storage = _FakeStorageService()
    writer = DatasetPrimaryIndexWritePath(
        elasticsearch_service=fake_es,  # type: ignore[arg-type]
        storage_service=fake_storage,  # type: ignore[arg-type]
        chunk_size=100,
        refresh=False,
    )

    instances = [
        {"instance_id": f"order-{idx}", "order_id": f"order-{idx}", "status": "NEW"}
        for idx in range(30)
    ]

    result = await writer.write_instances(
        job=job,
        instances=instances,
        ontology_version={"ref": "branch:main", "commit": "abc123"},
        objectify_pk_fields=["order_id"],
        objectify_instance_id_field="order_id",
    )

    assert result.instance_event_files_written == 30
    assert result.instance_event_file_failures == 0
    assert len(result.command_ids) == 25
    assert len(fake_storage.saved) == 30
