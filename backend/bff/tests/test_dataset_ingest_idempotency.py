from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import Any, Optional
from uuid import uuid4

import pytest
from fastapi import HTTPException
from starlette.datastructures import Headers, UploadFile

from bff.routers import pipeline as pipeline_router
from bff.routers.pipeline import upload_csv_dataset


class _FakeLakeFSStorage:
    async def save_bytes(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def save_json(self, *args: Any, **kwargs: Any) -> None:
        return None


class _FakeLakeFSClient:
    def __init__(self) -> None:
        self.commit_calls = 0

    async def commit(self, *args: Any, **kwargs: Any) -> str:
        self.commit_calls += 1
        return f"commit-{self.commit_calls}"

    async def create_branch(self, *args: Any, **kwargs: Any) -> None:
        return None


class _FakePipelineRegistry:
    def __init__(self) -> None:
        self.storage = _FakeLakeFSStorage()
        self.client = _FakeLakeFSClient()

    async def get_lakefs_storage(self, *args: Any, **kwargs: Any) -> _FakeLakeFSStorage:
        return self.storage

    async def get_lakefs_client(self, *args: Any, **kwargs: Any) -> _FakeLakeFSClient:
        return self.client


@dataclass
class _Request:
    headers: dict[str, str]


@dataclass
class _Dataset:
    dataset_id: str
    db_name: str
    name: str
    branch: str
    schema_json: dict[str, Any]


@dataclass
class _IngestRequest:
    ingest_request_id: str
    dataset_id: str
    db_name: str
    branch: str
    idempotency_key: str
    request_fingerprint: Optional[str]
    status: str
    lakefs_commit_id: Optional[str] = None
    artifact_key: Optional[str] = None
    schema_json: dict[str, Any] = field(default_factory=dict)
    schema_status: str = "PENDING"
    schema_approved_at: Optional[Any] = None
    schema_approved_by: Optional[str] = None
    sample_json: dict[str, Any] = field(default_factory=dict)
    row_count: Optional[int] = None
    source_metadata: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class _IngestTransaction:
    transaction_id: str
    ingest_request_id: str
    status: str = "PENDING"
    lakefs_commit_id: Optional[str] = None
    artifact_key: Optional[str] = None
    error: Optional[str] = None


@dataclass
class _DatasetVersion:
    version_id: str
    dataset_id: str
    lakefs_commit_id: str
    artifact_key: Optional[str]
    row_count: Optional[int]
    sample_json: dict[str, Any]
    schema_json: dict[str, Any]
    ingest_request_id: Optional[str] = None


class _FakeDatasetRegistry:
    def __init__(self) -> None:
        self.datasets_by_key: dict[tuple[str, str, str], _Dataset] = {}
        self.ingest_requests_by_key: dict[str, _IngestRequest] = {}
        self.ingest_requests_by_id: dict[str, _IngestRequest] = {}
        self.ingest_transactions_by_request: dict[str, _IngestTransaction] = {}
        self.versions_by_request: dict[str, _DatasetVersion] = {}

    async def get_dataset_by_name(self, *, db_name: str, name: str, branch: str) -> Optional[_Dataset]:
        return self.datasets_by_key.get((db_name, name, branch))

    async def create_dataset(
        self,
        *,
        db_name: str,
        name: str,
        description: Any,
        source_type: str,
        source_ref: str,
        schema_json: dict[str, Any],
        branch: str,
    ) -> _Dataset:
        dataset = _Dataset(
            dataset_id=f"ds-{uuid4().hex}",
            db_name=db_name,
            name=name,
            branch=branch,
            schema_json=schema_json,
        )
        self.datasets_by_key[(db_name, name, branch)] = dataset
        return dataset

    async def create_ingest_request(
        self,
        *,
        dataset_id: str,
        db_name: str,
        branch: str,
        idempotency_key: str,
        request_fingerprint: Optional[str],
        schema_json: Optional[dict[str, Any]] = None,
        sample_json: Optional[dict[str, Any]] = None,
        row_count: Optional[int] = None,
        source_metadata: Optional[dict[str, Any]] = None,
    ) -> tuple[_IngestRequest, bool]:
        existing = self.ingest_requests_by_key.get(idempotency_key)
        if existing:
            return existing, False
        ingest_request_id = str(uuid4())
        record = _IngestRequest(
            ingest_request_id=ingest_request_id,
            dataset_id=dataset_id,
            db_name=db_name,
            branch=branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            status="RECEIVED",
            schema_json=schema_json or {},
            schema_status="PENDING",
            schema_approved_at=None,
            schema_approved_by=None,
            sample_json=sample_json or {},
            row_count=row_count,
            source_metadata=source_metadata or {},
        )
        self.ingest_requests_by_key[idempotency_key] = record
        self.ingest_requests_by_id[ingest_request_id] = record
        return record, True

    async def get_ingest_transaction(self, *, ingest_request_id: str) -> Optional[_IngestTransaction]:
        return self.ingest_transactions_by_request.get(ingest_request_id)

    async def create_ingest_transaction(self, *, ingest_request_id: str) -> _IngestTransaction:
        transaction = _IngestTransaction(
            transaction_id=str(uuid4()),
            ingest_request_id=ingest_request_id,
        )
        self.ingest_transactions_by_request[ingest_request_id] = transaction
        return transaction

    async def mark_ingest_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
    ) -> _IngestRequest:
        record = self.ingest_requests_by_id[ingest_request_id]
        record.lakefs_commit_id = lakefs_commit_id
        record.artifact_key = artifact_key
        return record

    async def mark_ingest_transaction_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
    ) -> Optional[_IngestTransaction]:
        transaction = self.ingest_transactions_by_request.get(ingest_request_id)
        if transaction:
            transaction.status = "COMMITTED"
            transaction.lakefs_commit_id = lakefs_commit_id
            transaction.artifact_key = artifact_key
        return transaction

    async def get_version_by_ingest_request(self, *, ingest_request_id: str) -> Optional[_DatasetVersion]:
        return self.versions_by_request.get(ingest_request_id)

    async def publish_ingest_request(
        self,
        *,
        ingest_request_id: str,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
        row_count: Optional[int],
        sample_json: Optional[dict[str, Any]],
        schema_json: Optional[dict[str, Any]],
        apply_schema: bool = True,
        outbox_entries: Optional[list[dict[str, Any]]] = None,
    ) -> _DatasetVersion:
        existing = self.versions_by_request.get(ingest_request_id)
        if existing:
            record = self.ingest_requests_by_id.get(ingest_request_id)
            if record:
                record.status = "PUBLISHED"
            return existing
        version = _DatasetVersion(
            version_id=str(uuid4()),
            dataset_id=dataset_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
            row_count=row_count,
            sample_json=sample_json or {},
            schema_json=schema_json or {},
            ingest_request_id=ingest_request_id,
        )
        self.versions_by_request[ingest_request_id] = version
        record = self.ingest_requests_by_id.get(ingest_request_id)
        if record:
            record.status = "PUBLISHED"
            record.lakefs_commit_id = lakefs_commit_id
            record.artifact_key = artifact_key
        return version

    async def mark_ingest_failed(self, *, ingest_request_id: str, error: str) -> None:
        record = self.ingest_requests_by_id.get(ingest_request_id)
        if record:
            record.status = "FAILED"
            record.error = error

    async def mark_ingest_transaction_aborted(self, *, ingest_request_id: str, error: str) -> None:
        transaction = self.ingest_transactions_by_request.get(ingest_request_id)
        if transaction:
            transaction.status = "ABORTED"
            transaction.error = error


async def _noop_flush_outbox(*args: Any, **kwargs: Any) -> None:
    return None


def _build_upload(content: bytes) -> UploadFile:
    return UploadFile(
        file=io.BytesIO(content),
        filename="input.csv",
        headers=Headers({"content-type": "text/csv"}),
    )


@pytest.mark.asyncio
async def test_csv_upload_idempotency_key_reuses_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BFF_ADMIN_TOKEN", "testtoken")
    monkeypatch.setattr(pipeline_router, "flush_dataset_ingest_outbox", _noop_flush_outbox)

    dataset_registry = _FakeDatasetRegistry()
    pipeline_registry = _FakePipelineRegistry()

    key = f"idem-csv-{uuid4().hex}"
    request = _Request(headers={"X-Admin-Token": "testtoken", "Idempotency-Key": key, "X-DB-Name": "testdb"})
    file_content = b"id,name\n1,A\n2,B\n"

    res1 = await upload_csv_dataset(
        db_name="testdb",
        branch=None,
        file=_build_upload(file_content),
        dataset_name="csv_input",
        description=None,
        delimiter=None,
        has_header=True,
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=None,
        objectify_job_queue=None,
        lineage_store=None,
    )
    assert res1["status"] == "success"
    version1 = res1["data"]["version"]

    res2 = await upload_csv_dataset(
        db_name="testdb",
        branch=None,
        file=_build_upload(file_content),
        dataset_name="csv_input",
        description=None,
        delimiter=None,
        has_header=True,
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=None,
        objectify_job_queue=None,
        lineage_store=None,
    )
    assert res2["status"] == "success"
    version2 = res2["data"]["version"]
    assert version1["version_id"] == version2["version_id"]
    assert version1["lakefs_commit_id"] == version2["lakefs_commit_id"]
    assert pipeline_registry.client.commit_calls == 1

    with pytest.raises(HTTPException) as exc_info:
        await upload_csv_dataset(
            db_name="testdb",
            branch=None,
            file=_build_upload(b"id,name\n3,C\n"),
            dataset_name="csv_input",
            description=None,
            delimiter=None,
            has_header=True,
            request=request,
            pipeline_registry=pipeline_registry,
            dataset_registry=dataset_registry,
            objectify_registry=None,
            objectify_job_queue=None,
            lineage_store=None,
        )
    assert exc_info.value.status_code == 409
