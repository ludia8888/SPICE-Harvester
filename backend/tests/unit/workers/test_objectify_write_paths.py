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
        self._documents: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.created_indices: List[Dict[str, Any]] = []
        self.mapping_updates: List[Dict[str, Any]] = []
        self.bulk_calls: List[Dict[str, Any]] = []
        self.update_calls: List[Dict[str, Any]] = []
        self.deleted_docs: List[tuple[str, str]] = []
        self.refreshed: List[str] = []
        self.search_responses: List[Dict[str, Any]] = []

    async def index_exists(self, index: str) -> bool:
        return index in self._indices

    async def create_index(self, index: str, mappings: Dict[str, Any], settings: Dict[str, Any]) -> bool:
        self._indices.add(index)
        self._documents.setdefault(index, {})
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
        store = self._documents.setdefault(index, {})
        for document in documents:
            store[str(document["_id"])] = dict(document)
        return {"success": len(documents), "failed": 0}

    async def update_document(
        self,
        index: str,
        doc_id: str,
        doc: Dict[str, Any] | None = None,
        script: Dict[str, Any] | None = None,
        upsert: Dict[str, Any] | None = None,
        refresh: bool = False,
    ) -> bool:
        _ = (script, refresh)
        store = self._documents.setdefault(index, {})
        if doc_id not in store:
            if upsert is None:
                return False
            store[doc_id] = dict(upsert)
        elif doc:
            store[doc_id].update(dict(doc))
        self.update_calls.append({"index": index, "doc_id": doc_id, "doc": dict(doc or {})})
        return True

    async def delete_document(self, index: str, doc_id: str, refresh: bool = False) -> bool:
        _ = refresh
        self.deleted_docs.append((index, doc_id))
        self._documents.setdefault(index, {}).pop(doc_id, None)
        return True

    async def refresh_index(self, index: str) -> bool:
        self.refreshed.append(index)
        return True

    async def search(self, index: str, body: Dict[str, Any]) -> Dict[str, Any]:
        if self.search_responses:
            return self.search_responses.pop(0)
        docs = list(self._documents.get(index, {}).values())
        hits = [{"_source": doc, "_id": doc.get("_id"), "sort": [doc.get("instance_id")]} for doc in docs]
        return {"hits": {"hits": hits}}

    async def mget(self, *, index: str, body: Dict[str, Any]) -> Dict[str, Any]:
        ids = [str(value) for value in list(body.get("ids") or [])]
        store = self._documents.get(index, {})
        docs = []
        for doc_id in ids:
            document = store.get(doc_id)
            if document is None:
                docs.append({"_id": doc_id, "found": False})
                continue
            docs.append({"_id": doc_id, "found": True, "_source": dict(document)})
        return {"docs": docs}


class _FakeStorageService:
    def __init__(self) -> None:
        self.saved: List[Dict[str, Any]] = []
        self.deleted: List[Dict[str, Any]] = []

    async def save_json(self, bucket: str, key: str, payload: Dict[str, Any]) -> None:
        self.saved.append({"bucket": bucket, "key": key, "payload": payload})

    async def delete_object(self, bucket: str, key: str) -> bool:
        self.deleted.append({"bucket": bucket, "key": key})
        return True


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
    assert docs[0]["objectify_visibility_state"] == "staged"


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

    await writer.write_instances(
        job=job,
        instances=[{"instance_id": "order-1", "order_id": "order-1", "status": "NEW"}],
        ontology_version=None,
        objectify_pk_fields=["order_id"],
        objectify_instance_id_field="order_id",
    )
    index_name = next(iter(fake_es._documents.keys()))
    fake_es._documents[index_name]["order-2"] = {
        "instance_id": "order-2",
        "objectify_visibility_state": "committed",
    }

    summary = await writer.finalize_job(
        job=job,
        execution_mode="full",
        indexed_instance_ids=["order-1"],
    )

    assert summary["stale_prune"]["executed"] is True
    assert summary["stale_prune"]["deleted"] == 1
    assert fake_es.deleted_docs
    assert fake_es.deleted_docs[0][1] == "order-2"
    assert summary["visibility_publish"]["published"] == 1
    assert fake_es.refreshed


@pytest.mark.asyncio
async def test_dataset_primary_write_path_publishes_instance_event_files_only_during_finalize() -> None:
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

    assert result.instance_event_files_written == 0
    assert result.instance_event_file_failures == 0
    assert result.command_ids == []
    assert fake_storage.saved == []

    summary = await writer.finalize_job(
        job=job,
        execution_mode="incremental",
        indexed_instance_ids=result.indexed_instance_ids,
    )

    instance_event_files = summary["instance_event_files"]
    assert instance_event_files["written"] == 30
    assert instance_event_files["failed"] == 0
    assert len(instance_event_files["command_ids"]) == 25
    assert len(fake_storage.saved) == 30


@pytest.mark.asyncio
async def test_dataset_primary_finalize_publishes_staged_docs_before_completion() -> None:
    job = _build_job()
    fake_es = _FakeElasticsearchService()
    writer = DatasetPrimaryIndexWritePath(
        elasticsearch_service=fake_es,  # type: ignore[arg-type]
        chunk_size=100,
        refresh=False,
        prune_stale_on_full=False,
    )

    await writer.write_instances(
        job=job,
        instances=[{"instance_id": "order-1", "order_id": "order-1", "status": "NEW"}],
        ontology_version=None,
        objectify_pk_fields=["order_id"],
        objectify_instance_id_field="order_id",
    )

    index_name = next(iter(fake_es._documents.keys()))
    assert fake_es._documents[index_name]["order-1"]["objectify_visibility_state"] == "staged"

    summary = await writer.finalize_job(
        job=job,
        execution_mode="incremental",
        indexed_instance_ids=["order-1"],
    )

    assert summary["visibility_publish"]["published"] == 1
    assert fake_es._documents[index_name]["order-1"]["objectify_visibility_state"] == "committed"
    assert fake_es.refreshed == [index_name]


@pytest.mark.asyncio
async def test_dataset_primary_finalize_requires_every_instance_to_publish_visibility() -> None:
    job = _build_job()
    fake_es = _FakeElasticsearchService()
    writer = DatasetPrimaryIndexWritePath(
        elasticsearch_service=fake_es,  # type: ignore[arg-type]
        chunk_size=100,
        refresh=False,
        prune_stale_on_full=False,
    )

    with pytest.raises(RuntimeError, match="objectify_visibility_publish_incomplete"):
        await writer.finalize_job(
            job=job,
            execution_mode="incremental",
            indexed_instance_ids=["missing-order"],
        )


@pytest.mark.asyncio
async def test_dataset_primary_finalize_applies_explicit_delta_deletes_after_commit() -> None:
    job = _build_job()
    fake_es = _FakeElasticsearchService()
    writer = DatasetPrimaryIndexWritePath(
        elasticsearch_service=fake_es,  # type: ignore[arg-type]
        chunk_size=100,
        refresh=False,
        prune_stale_on_full=False,
    )

    await writer.write_instances(
        job=job,
        instances=[{"instance_id": "order-1", "order_id": "order-1", "status": "NEW"}],
        ontology_version=None,
        objectify_pk_fields=["order_id"],
        objectify_instance_id_field="order_id",
    )

    index_name = next(iter(fake_es._documents.keys()))
    fake_es._documents[index_name]["order-2"] = {
        "instance_id": "order-2",
        "objectify_visibility_state": "committed",
    }

    summary = await writer.finalize_job(
        job=job,
        execution_mode="incremental",
        indexed_instance_ids=["order-1"],
        deleted_instance_ids=["order-2"],
    )

    assert summary["explicit_delete"]["executed"] is True
    assert summary["explicit_delete"]["deleted"] == 1
    assert "order-2" not in fake_es._documents[index_name]
