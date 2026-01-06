import io
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from starlette.datastructures import UploadFile
from starlette.requests import Request

from bff.routers import pipeline as pipeline_router
from shared.services.dataset_registry import (
    DatasetIngestRequestRecord,
    DatasetIngestTransactionRecord,
    DatasetRecord,
    DatasetVersionRecord,
)


class _FakeLakeFSStorage:
    def __init__(self) -> None:
        self.saved = []

    async def save_fileobj(self, repo, key, fileobj, *, content_type=None, metadata=None, checksum=None):
        fileobj.seek(0)
        self.saved.append(
            {
                "repo": repo,
                "key": key,
                "content_type": content_type,
                "metadata": metadata,
                "checksum": checksum,
                "size": len(fileobj.read()),
            }
        )

    async def save_bytes(self, repo, key, content, *, content_type=None, metadata=None):
        self.saved.append(
            {
                "repo": repo,
                "key": key,
                "content_type": content_type,
                "metadata": metadata,
                "size": len(content),
            }
        )


class _FakeLakeFSClient:
    def __init__(self) -> None:
        self.commits = []
        self.branches = []

    async def commit(self, *, repository, branch, message, metadata):
        commit_id = f"commit-{len(self.commits) + 1}"
        self.commits.append(
            {
                "repository": repository,
                "branch": branch,
                "message": message,
                "metadata": metadata,
                "commit_id": commit_id,
            }
        )
        return commit_id

    async def create_branch(self, *, repository, name, source):
        self.branches.append({"repository": repository, "name": name, "source": source})


class _FakePipelineRegistry:
    def __init__(self) -> None:
        self.storage = _FakeLakeFSStorage()
        self.client = _FakeLakeFSClient()

    async def get_lakefs_storage(self, *, user_id=None):
        return self.storage

    async def get_lakefs_client(self, *, user_id=None):
        return self.client


class _FakeDatasetRegistry:
    def __init__(self, *, ingest_status: str = "CREATED") -> None:
        self._datasets: dict[tuple[str, str, str], DatasetRecord] = {}
        self._datasets_by_id: dict[str, DatasetRecord] = {}
        self._ingest_requests: dict[str, DatasetIngestRequestRecord] = {}
        self._ingest_transactions: dict[str, DatasetIngestTransactionRecord] = {}
        self._versions: dict[str, DatasetVersionRecord] = {}
        self._ingest_status = ingest_status

    async def get_dataset_by_name(self, *, db_name: str, name: str, branch: str):
        return self._datasets.get((db_name, name, branch))

    async def create_dataset(self, *, db_name, name, description, source_type, source_ref, schema_json, branch):
        now = datetime.now(timezone.utc)
        record = DatasetRecord(
            dataset_id=f"ds-{len(self._datasets) + 1}",
            db_name=db_name,
            name=name,
            description=description,
            source_type=source_type,
            source_ref=source_ref,
            branch=branch,
            schema_json=schema_json,
            created_at=now,
            updated_at=now,
        )
        self._datasets[(db_name, name, branch)] = record
        self._datasets_by_id[record.dataset_id] = record
        return record

    async def create_ingest_request(
        self,
        *,
        dataset_id,
        db_name,
        branch,
        idempotency_key,
        request_fingerprint,
        schema_json,
        sample_json,
        row_count,
        source_metadata,
    ):
        now = datetime.now(timezone.utc)
        record = DatasetIngestRequestRecord(
            ingest_request_id=f"ingest-{len(self._ingest_requests) + 1}",
            dataset_id=dataset_id,
            db_name=db_name,
            branch=branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            status=self._ingest_status,
            lakefs_commit_id=None,
            artifact_key=None,
            schema_json=schema_json,
            schema_status="PENDING",
            schema_approved_at=None,
            schema_approved_by=None,
            sample_json=sample_json,
            row_count=row_count,
            source_metadata=source_metadata,
            error=None,
            created_at=now,
            updated_at=now,
            published_at=None,
        )
        self._ingest_requests[record.ingest_request_id] = record
        return record, True

    async def get_ingest_request(self, *, ingest_request_id: str):
        return self._ingest_requests.get(ingest_request_id)

    async def approve_ingest_schema(self, *, ingest_request_id: str, schema_json=None, approved_by=None):
        record = self._ingest_requests.get(ingest_request_id)
        if not record:
            raise RuntimeError("Ingest request not found")
        payload = schema_json if schema_json is not None else record.schema_json
        if not isinstance(payload, dict) or not payload:
            raise ValueError("schema_json is required to approve")
        dataset = self._datasets_by_id.get(record.dataset_id)
        if not dataset:
            raise RuntimeError("Dataset not found for ingest request")
        now = datetime.now(timezone.utc)
        updated_dataset = replace(dataset, schema_json=payload, updated_at=now)
        self._datasets_by_id[dataset.dataset_id] = updated_dataset
        self._datasets[(dataset.db_name, dataset.name, dataset.branch)] = updated_dataset
        updated_request = replace(
            record,
            schema_json=payload,
            schema_status="APPROVED",
            schema_approved_at=now,
            schema_approved_by=approved_by,
            updated_at=now,
        )
        self._ingest_requests[ingest_request_id] = updated_request
        return updated_dataset, updated_request

    async def get_version_by_ingest_request(self, *, ingest_request_id: str):
        return self._versions.get(ingest_request_id)

    async def get_ingest_transaction(self, *, ingest_request_id: str):
        return self._ingest_transactions.get(ingest_request_id)

    async def create_ingest_transaction(self, *, ingest_request_id: str):
        now = datetime.now(timezone.utc)
        record = DatasetIngestTransactionRecord(
            transaction_id=f"txn-{len(self._ingest_transactions) + 1}",
            ingest_request_id=ingest_request_id,
            status="CREATED",
            lakefs_commit_id=None,
            artifact_key=None,
            error=None,
            created_at=now,
            updated_at=now,
            committed_at=None,
            aborted_at=None,
        )
        self._ingest_transactions[ingest_request_id] = record
        return record

    async def mark_ingest_committed(self, *, ingest_request_id, lakefs_commit_id, artifact_key):
        record = self._ingest_requests[ingest_request_id]
        updated = replace(record, lakefs_commit_id=lakefs_commit_id, artifact_key=artifact_key)
        self._ingest_requests[ingest_request_id] = updated
        return updated

    async def mark_ingest_transaction_committed(self, *, ingest_request_id, lakefs_commit_id, artifact_key):
        record = self._ingest_transactions[ingest_request_id]
        updated = replace(record, status="COMMITTED", lakefs_commit_id=lakefs_commit_id, artifact_key=artifact_key)
        self._ingest_transactions[ingest_request_id] = updated
        return updated

    async def update_ingest_request_payload(self, *, ingest_request_id, sample_json, row_count):
        record = self._ingest_requests[ingest_request_id]
        updated = replace(record, sample_json=sample_json, row_count=row_count)
        self._ingest_requests[ingest_request_id] = updated
        return updated

    async def publish_ingest_request(
        self,
        *,
        ingest_request_id,
        dataset_id,
        lakefs_commit_id,
        artifact_key,
        row_count,
        sample_json,
        schema_json,
        apply_schema=True,
        outbox_entries,
    ):
        now = datetime.now(timezone.utc)
        version = DatasetVersionRecord(
            version_id=f"ver-{len(self._versions) + 1}",
            dataset_id=dataset_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
            row_count=row_count,
            sample_json=sample_json,
            ingest_request_id=ingest_request_id,
            promoted_from_artifact_id=None,
            created_at=now,
        )
        self._versions[ingest_request_id] = version
        record = self._ingest_requests[ingest_request_id]
        self._ingest_requests[ingest_request_id] = replace(record, status="PUBLISHED")
        if apply_schema and isinstance(schema_json, dict):
            dataset = self._datasets_by_id.get(dataset_id)
            if dataset:
                updated_dataset = replace(dataset, schema_json=schema_json, updated_at=now)
                self._datasets_by_id[dataset_id] = updated_dataset
                self._datasets[(dataset.db_name, dataset.name, dataset.branch)] = updated_dataset
        return version

    async def mark_ingest_failed(self, *, ingest_request_id, error):
        record = self._ingest_requests.get(ingest_request_id)
        if record:
            self._ingest_requests[ingest_request_id] = replace(record, status="FAILED", error=error)

    async def mark_ingest_transaction_aborted(self, *, ingest_request_id, error):
        record = self._ingest_transactions.get(ingest_request_id)
        if record:
            self._ingest_transactions[ingest_request_id] = replace(record, status="ABORTED", error=error)


class _FakeFunnelClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def analyze_dataset(self, payload):
        return {"columns": [{"name": "id", "type": "int"}]}

    async def excel_to_structure_preview_stream(self, *args, **kwargs):
        preview = {
            "columns": ["id", "name"],
            "sample_data": [[1, "Ada"]],
            "total_rows": 1,
            "inferred_schema": [{"name": "id", "type": "int"}],
            "source_metadata": {"sheet": "Sheet1"},
        }
        return {"preview": preview}, "hash-123"


def _build_request(headers: dict[str, str]) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [(key.lower().encode(), value.encode()) for key, value in headers.items()],
    }
    return Request(scope)


def test_pipeline_helpers_normalize_inputs(monkeypatch):
    monkeypatch.setenv("PIPELINE_PROTECTED_BRANCHES", "main, release")
    monkeypatch.setenv("PIPELINE_REQUIRE_PROPOSALS", "true")

    assert pipeline_router._resolve_pipeline_protected_branches() == {"main", "release"}
    assert pipeline_router._pipeline_requires_proposal("main") is True
    assert pipeline_router._pipeline_requires_proposal("feature") is False

    assert pipeline_router._normalize_mapping_spec_ids(None) == []
    assert pipeline_router._normalize_mapping_spec_ids("a,b") == ["a", "b"]
    assert pipeline_router._normalize_mapping_spec_ids(["a", "a", "b"]) == ["a", "b"]

    metadata = pipeline_router._sanitize_s3_metadata({"hello": "world", "label": "안녕"})
    assert metadata["hello"] == "world"
    assert metadata["label"] != "안녕"

    assert pipeline_router._detect_csv_delimiter("a,b,c") == ","
    columns, preview_rows, total_rows = pipeline_router._parse_csv_content(
        "id,name\n1,Ada\n2,Ben\n",
        delimiter=",",
        has_header=True,
        preview_limit=1,
    )
    assert columns == ["id", "name"]
    assert preview_rows == [["1", "Ada"]]
    assert total_rows == 2


@pytest.mark.asyncio
async def test_upload_csv_dataset_creates_version(monkeypatch):
    async def _noop_flush(**_):
        return None

    import bff.services.funnel_client as funnel_client

    monkeypatch.setattr(pipeline_router, "flush_dataset_ingest_outbox", _noop_flush)
    monkeypatch.setattr(funnel_client, "FunnelClient", _FakeFunnelClient)

    pipeline_registry = _FakePipelineRegistry()
    dataset_registry = _FakeDatasetRegistry()

    csv_bytes = b"id,name\n1,Ada\n2,Ben\n"
    upload = UploadFile(filename="people.csv", file=io.BytesIO(csv_bytes))
    request = _build_request(
        {
            "Idempotency-Key": "idem-1",
            "X-DB-Name": "core-db",
            "X-User-ID": "user-1",
        }
    )

    response = await pipeline_router.upload_csv_dataset(
        db_name="core-db",
        branch="main",
        file=upload,
        dataset_name="people",
        description="People dataset",
        delimiter=",",
        has_header=True,
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=None,
        objectify_job_queue=None,
        lineage_store=None,
    )

    assert response["status"] == "success"
    assert response["data"]["dataset"]["name"] == "people"
    assert response["data"]["version"]["lakefs_commit_id"].startswith("commit-")


@pytest.mark.asyncio
async def test_upload_excel_dataset_commits_preview(monkeypatch):
    async def _noop_flush(**_):
        return None

    import bff.services.funnel_client as funnel_client

    monkeypatch.setattr(pipeline_router, "flush_dataset_ingest_outbox", _noop_flush)
    monkeypatch.setattr(funnel_client, "FunnelClient", _FakeFunnelClient)

    pipeline_registry = _FakePipelineRegistry()
    dataset_registry = _FakeDatasetRegistry()

    upload = UploadFile(filename="sheet.xlsx", file=io.BytesIO(b"excel-bytes"))
    request = _build_request(
        {
            "Idempotency-Key": "idem-2",
            "X-DB-Name": "core-db",
            "X-User-ID": "user-2",
        }
    )

    response = await pipeline_router.upload_excel_dataset(
        db_name="core-db",
        branch="main",
        file=upload,
        dataset_name="excel-data",
        description="Excel dataset",
        sheet_name="Sheet1",
        table_top=None,
        table_left=None,
        table_bottom=None,
        table_right=None,
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=None,
        objectify_job_queue=None,
        lineage_store=None,
    )

    assert response["status"] == "success"
    assert response["data"]["dataset"]["name"] == "excel-data"
    assert [col["name"] for col in response["data"]["preview"]["columns"]] == ["id", "name"]


@pytest.mark.asyncio
async def test_approve_dataset_schema_updates_dataset():
    dataset_registry = _FakeDatasetRegistry()
    dataset = await dataset_registry.create_dataset(
        db_name="core-db",
        name="incoming",
        description=None,
        source_type="csv_upload",
        source_ref="upload.csv",
        schema_json={},
        branch="main",
    )
    ingest_request, _ = await dataset_registry.create_ingest_request(
        dataset_id=dataset.dataset_id,
        db_name=dataset.db_name,
        branch=dataset.branch,
        idempotency_key="idem-approve",
        request_fingerprint="fingerprint",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}]},
        sample_json={"columns": ["id"], "rows": [[1]]},
        row_count=1,
        source_metadata={"source_type": "csv"},
    )
    request = _build_request({"X-DB-Name": "core-db", "X-User-ID": "user-approve"})

    response = await pipeline_router.approve_dataset_schema(
        ingest_request_id=ingest_request.ingest_request_id,
        payload={},
        request=request,
        dataset_registry=dataset_registry,
    )

    assert response["status"] == "success"
    assert response["data"]["dataset"]["schema_json"]["columns"][0]["name"] == "id"
    assert response["data"]["ingest_request"]["schema_status"] == "APPROVED"
    assert response["data"]["ingest_request"]["schema_approved_by"] == "user-approve"


@pytest.mark.asyncio
async def test_upload_media_dataset_stores_files(monkeypatch):
    async def _noop_flush(**_):
        return None

    monkeypatch.setattr(pipeline_router, "flush_dataset_ingest_outbox", _noop_flush)

    pipeline_registry = _FakePipelineRegistry()
    dataset_registry = _FakeDatasetRegistry()

    upload_one = UploadFile(filename="photo.png", file=io.BytesIO(b"pngdata"))
    upload_two = UploadFile(filename="doc.txt", file=io.BytesIO(b"textdata"))
    request = _build_request(
        {
            "Idempotency-Key": "idem-3",
            "X-DB-Name": "core-db",
            "X-User-ID": "user-3",
        }
    )

    response = await pipeline_router.upload_media_dataset(
        db_name="core-db",
        branch="main",
        files=[upload_one, upload_two],
        dataset_name="media",
        description="media data",
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=None,
        objectify_job_queue=None,
        lineage_store=None,
    )

    assert response["status"] == "success"
    assert response["data"]["version"]["artifact_key"].startswith("s3://")


@pytest.mark.asyncio
async def test_maybe_enqueue_objectify_job():
    mapping_spec = SimpleNamespace(
        mapping_spec_id="map-1",
        version=1,
        options={"ontology_branch": "main"},
        auto_sync=True,
        target_class_id="class-1",
    )

    class FakeObjectifyRegistry:
        async def get_active_mapping_spec(self, **kwargs):
            return mapping_spec

        def build_dedupe_key(self, **kwargs):
            return "dedupe-key"

        async def get_objectify_job_by_dedupe_key(self, **kwargs):
            return None

        async def enqueue_objectify_job(self, **kwargs):
            return None

    class FakeObjectifyJobQueue:
        def __init__(self):
            self.jobs = []

        async def publish(self, job, *, require_delivery: bool = True):
            self.jobs.append(job)

    dataset = SimpleNamespace(
        dataset_id="ds-1",
        db_name="core-db",
        name="orders",
        branch="main",
        schema_json={"columns": [{"name": "id"}]},
    )
    version = SimpleNamespace(
        version_id="ver-1",
        artifact_key="s3://raw/commit/object.csv",
        sample_json={"columns": ["id"]},
    )
    registry = FakeObjectifyRegistry()
    queue = FakeObjectifyJobQueue()

    job_id = await pipeline_router._maybe_enqueue_objectify_job(
        dataset=dataset,
        version=version,
        objectify_registry=registry,
        job_queue=queue,
        actor_user_id="user-1",
    )

    assert job_id is not None
    assert queue.jobs
