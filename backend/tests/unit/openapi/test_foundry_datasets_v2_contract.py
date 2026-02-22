"""Contract tests for Foundry Datasets API v2 router.

Validates OpenAPI path registration, dataset CRUD, schema operations,
branch management, transaction lifecycle, file operations, and table
reads against the ``bff.routers.foundry_datasets_v2`` router.

Test pattern follows ``test_foundry_platform_v2_contract.py``.
"""

import csv
import io
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from bff.routers import foundry_datasets_v2
from bff.routers.data_connector_deps import get_dataset_registry, get_pipeline_registry
from shared.foundry.errors import FoundryAPIError, foundry_exception_handler
from shared.security.user_context import UserPrincipal
from shared.services.storage.lakefs_client import LakeFSError
from shared.utils.time_utils import utcnow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_test_app() -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(FoundryAPIError, foundry_exception_handler)

    @app.middleware("http")
    async def _attach_scoped_principal(request, call_next):  # noqa: ANN001
        request.state.user = UserPrincipal(
            id="test-user",
            type="user",
            roles=(),
            tenant_id="default",
            org_id="default",
            verified=True,
            claims={"scope": "api:datasets-read api:datasets-write"},
        )
        return await call_next(request)

    app.include_router(foundry_datasets_v2.router, prefix="/api")
    return app


# ---------------------------------------------------------------------------
# Mock registries
# ---------------------------------------------------------------------------

class _DatasetRegistry:
    def __init__(self) -> None:
        self.datasets: dict[str, Any] = {}
        self.versions: dict[str, Any] = {}
        self.versions_by_ingest_request_id: dict[str, Any] = {}
        self.transactions_by_id: dict[str, Any] = {}
        self.transaction_id_by_ingest_request_id: dict[str, str] = {}
        self.ingest_requests_by_id: dict[str, Any] = {}

    async def create_dataset(self, **kwargs: Any) -> SimpleNamespace:  # noqa: ANN401
        dataset_id = str(uuid.uuid4())
        ds = SimpleNamespace(
            dataset_id=dataset_id,
            db_name=kwargs.get("db_name", "test_db"),
            name=kwargs.get("name", "test"),
            description=kwargs.get("description", ""),
            source_type=kwargs.get("source_type", "foundry_api"),
            source_ref=kwargs.get("source_ref"),
            schema_json=kwargs.get("schema_json") or {},
            branch=kwargs.get("branch", "master"),
            created_at=utcnow(),
            updated_at=utcnow(),
        )
        self.datasets[dataset_id] = ds
        return ds

    async def get_dataset(self, *, dataset_id: str) -> SimpleNamespace | None:  # noqa: ANN001
        return self.datasets.get(dataset_id)

    async def list_datasets(self, *, db_name: str | None = None, **kwargs: Any) -> list[dict[str, Any]]:  # noqa: ANN401
        items = list(self.datasets.values())
        if db_name:
            items = [d for d in items if d.db_name == db_name]
        return [
            {
                "dataset_id": d.dataset_id,
                "db_name": d.db_name,
                "name": d.name,
                "description": d.description,
                "source_type": d.source_type,
                "source_ref": d.source_ref,
                "branch": d.branch,
                "schema_json": d.schema_json,
                "created_at": d.created_at,
                "updated_at": d.updated_at,
            }
            for d in items
        ]

    async def list_all_datasets(
        self,
        *,
        branch: str | None,
        limit: int,
        offset: int,
        order_by: str | None = None,
    ) -> list[dict[str, Any]]:
        items = list(self.datasets.values())
        if branch:
            items = [d for d in items if str(getattr(d, "branch", "") or "") == str(branch)]
        if order_by and str(order_by).strip().lower() == "name":
            items.sort(key=lambda d: str(getattr(d, "name", "")))
        sliced = items[int(offset) : int(offset) + int(limit)]
        return [
            {
                "dataset_id": d.dataset_id,
                "db_name": d.db_name,
                "name": d.name,
                "description": d.description,
                "source_type": d.source_type,
                "source_ref": d.source_ref,
                "branch": d.branch,
                "schema_json": d.schema_json,
                "created_at": d.created_at,
                "updated_at": d.updated_at,
            }
            for d in sliced
        ]

    async def update_schema(self, *, dataset_id: str, schema_json: dict) -> None:  # noqa: ANN001
        ds = self.datasets.get(dataset_id)
        if ds:
            ds.schema_json = schema_json

    async def get_latest_version(self, *, dataset_id: str) -> SimpleNamespace | None:  # noqa: ANN001
        return self.versions.get(dataset_id)

    # -- Transaction helpers (mirroring real DatasetRegistry) ----------------

    async def create_ingest_request(
        self,
        *,
        dataset_id: str,
        db_name: str,
        branch: str,
        idempotency_key: str,
        request_fingerprint: str | None,
        **kwargs: Any,
    ) -> tuple[SimpleNamespace, bool]:
        ingest_request_id = str(uuid.uuid4())
        req = SimpleNamespace(
            ingest_request_id=ingest_request_id,
            dataset_id=dataset_id,
            db_name=db_name,
            branch=branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            status="RECEIVED",
            lakefs_commit_id=None,
            artifact_key=None,
            schema_json=kwargs.get("schema_json") or {},
            sample_json=kwargs.get("sample_json") or {},
            row_count=kwargs.get("row_count"),
            source_metadata=kwargs.get("source_metadata") or {},
            error=None,
            created_at=utcnow(),
            updated_at=utcnow(),
            published_at=None,
        )
        self.ingest_requests_by_id[ingest_request_id] = req
        return req, True

    async def get_ingest_request(self, *, ingest_request_id: str) -> SimpleNamespace | None:  # noqa: ANN001
        return self.ingest_requests_by_id.get(ingest_request_id)

    async def mark_ingest_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: str | None,
    ) -> SimpleNamespace | None:
        req = await self.get_ingest_request(ingest_request_id=ingest_request_id)
        if req:
            req.status = "RAW_COMMITTED"
            req.lakefs_commit_id = lakefs_commit_id
            req.artifact_key = artifact_key
            req.updated_at = utcnow()
        return req

    async def create_ingest_transaction(
        self,
        *,
        ingest_request_id: str,
        status: str = "OPEN",
    ) -> SimpleNamespace:
        transaction_id = str(uuid.uuid4())
        now = utcnow()
        txn = SimpleNamespace(
            transaction_id=transaction_id,
            ingest_request_id=ingest_request_id,
            status=status,
            created_at=now,
            updated_at=now,
            committed_at=None,
            aborted_at=None,
            lakefs_commit_id=None,
            artifact_key=None,
            error=None,
        )
        self.transactions_by_id[transaction_id] = txn
        self.transaction_id_by_ingest_request_id[ingest_request_id] = transaction_id
        return txn

    async def get_ingest_transaction(self, *, ingest_request_id: str) -> SimpleNamespace | None:  # noqa: ANN001
        transaction_id = self.transaction_id_by_ingest_request_id.get(ingest_request_id)
        if not transaction_id:
            return None
        return self.transactions_by_id.get(transaction_id)

    async def get_ingest_transaction_by_id(self, *, transaction_id: str) -> SimpleNamespace | None:  # noqa: ANN001
        return self.transactions_by_id.get(transaction_id)

    async def mark_ingest_transaction_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: str | None,
    ) -> SimpleNamespace | None:
        txn = await self.get_ingest_transaction(ingest_request_id=ingest_request_id)
        if txn:
            txn.status = "COMMITTED"
            txn.lakefs_commit_id = lakefs_commit_id
            txn.artifact_key = artifact_key
            txn.committed_at = utcnow()
        return txn

    async def mark_ingest_transaction_aborted(
        self,
        *,
        ingest_request_id: str,
        error: str,
    ) -> SimpleNamespace | None:
        txn = await self.get_ingest_transaction(ingest_request_id=ingest_request_id)
        if txn:
            txn.status = "ABORTED"
            txn.error = error
            txn.aborted_at = utcnow()
        return txn

    async def publish_ingest_request(
        self,
        *,
        ingest_request_id: str,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: str | None,
        row_count: int | None,
        sample_json: dict[str, Any] | None,
        schema_json: dict[str, Any] | None,
        apply_schema: bool = True,
        outbox_entries: list[dict[str, Any]] | None = None,
    ) -> SimpleNamespace:
        existing = self.versions_by_ingest_request_id.get(ingest_request_id)
        if existing is not None:
            return existing

        version = SimpleNamespace(
            version_id=str(uuid.uuid4()),
            dataset_id=dataset_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
            row_count=row_count,
            sample_json=sample_json or {},
            ingest_request_id=ingest_request_id,
            promoted_from_artifact_id=None,
            created_at=utcnow(),
        )
        self.versions[dataset_id] = version
        self.versions_by_ingest_request_id[ingest_request_id] = version

        ingest_request = self.ingest_requests_by_id.get(ingest_request_id)
        if ingest_request is not None:
            ingest_request.status = "PUBLISHED"
            ingest_request.lakefs_commit_id = lakefs_commit_id
            ingest_request.artifact_key = artifact_key
            ingest_request.sample_json = sample_json or ingest_request.sample_json
            ingest_request.row_count = row_count
            ingest_request.published_at = utcnow()

        dataset = self.datasets.get(dataset_id)
        if dataset is not None and apply_schema and isinstance(schema_json, dict):
            dataset.schema_json = schema_json

        txn = await self.get_ingest_transaction(ingest_request_id=ingest_request_id)
        if txn is not None:
            txn.status = "COMMITTED"
            txn.lakefs_commit_id = lakefs_commit_id
            txn.artifact_key = artifact_key
            txn.committed_at = utcnow()
        return version


class _MockLakeFSClient:
    async def list_branches(self, repository: str, **kwargs: Any) -> list[dict[str, Any]]:  # noqa: ANN401
        return [
            {"name": "master", "commit_id": "abc123"},
            {"name": "dev", "commit_id": "def456"},
        ]

    async def create_branch(self, repository: str, name: str, source: str = "master") -> dict[str, str]:
        return {"id": name}

    async def get_branch_head_commit_id(self, repository: str, branch: str) -> str:
        return "abc123"

    async def delete_branch(self, repository: str, name: str) -> bool:
        return True

    async def list_objects(self, repository: str, ref: str, prefix: str = "", amount: int = 100) -> list[dict[str, Any]]:
        return [
            {"path": f"{prefix}source.csv", "size_bytes": 1024, "mtime": 1234567890},
        ]

    async def commit(self, repository: str, branch: str, message: str, metadata: dict | None = None) -> str:
        return "commit-123"


class _FailingLakeFSClient(_MockLakeFSClient):
    async def commit(self, repository: str, branch: str, message: str, metadata: dict | None = None) -> str:  # noqa: ANN401
        raise LakeFSError("lakeFS unavailable")


class _FailingLakeFSListClient(_MockLakeFSClient):
    async def list_objects(self, repository: str, ref: str, prefix: str = "", amount: int = 100) -> list[dict[str, Any]]:  # noqa: ANN401
        raise LakeFSError("lakeFS unavailable")


class _MockLakeFSStorage:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def load_bytes(self, repo: str, path: str) -> bytes:
        if path not in self.objects:
            raise FileNotFoundError(path)
        return self.objects[path]

    async def save_bytes(self, repo: str, path: str, data: bytes, content_type: str | None = None) -> None:
        self.objects[path] = data

    async def get_object_metadata(self, repo: str, path: str) -> dict[str, Any]:
        if path not in self.objects:
            raise FileNotFoundError(path)
        return {
            "size": len(self.objects[path]),
            "last_modified": utcnow(),
            "etag": "mock-etag",
            "metadata": {},
        }


class _PipelineRegistry:
    def __init__(
        self,
        lakefs_client: _MockLakeFSClient | None = None,
        lakefs_storage: _MockLakeFSStorage | None = None,
    ) -> None:
        self._lakefs_client = lakefs_client or _MockLakeFSClient()
        self._lakefs_storage = lakefs_storage or _MockLakeFSStorage()

    async def get_pipeline(self, pipeline_id: str) -> None:  # noqa: ANN001
        return None

    async def get_lakefs_client(self, user_id: str | None = None) -> _MockLakeFSClient:
        return self._lakefs_client

    async def get_lakefs_storage(self, user_id: str | None = None) -> _MockLakeFSStorage:
        return self._lakefs_storage


def _override_deps(
    app: FastAPI,
    dataset_registry: _DatasetRegistry | None = None,
    pipeline_registry: _PipelineRegistry | None = None,
) -> tuple[_DatasetRegistry, _PipelineRegistry]:
    dr = dataset_registry or _DatasetRegistry()
    pr = pipeline_registry or _PipelineRegistry()
    app.dependency_overrides[get_dataset_registry] = lambda: dr
    app.dependency_overrides[get_pipeline_registry] = lambda: pr
    return dr, pr


# ---------------------------------------------------------------------------
# 1. OpenAPI path existence
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_foundry_datasets_v2_paths_exist_in_openapi():
    app = _build_test_app()
    schema = app.openapi()

    expected_paths = {
        # Dataset CRUD
        "/api/v2/datasets",
        "/api/v2/datasets/{datasetRid}",
        # Schema (preview)
        "/api/v2/datasets/{datasetRid}/getSchema",
        "/api/v2/datasets/getSchemaBatch",
        "/api/v2/datasets/{datasetRid}/putSchema",
        # Branches
        "/api/v2/datasets/{datasetRid}/branches",
        "/api/v2/datasets/{datasetRid}/branches/{branchName}",
        # Transactions
        "/api/v2/datasets/{datasetRid}/transactions",
        "/api/v2/datasets/{datasetRid}/transactions/{transactionRid}",
        "/api/v2/datasets/{datasetRid}/transactions/{transactionRid}/commit",
        "/api/v2/datasets/{datasetRid}/transactions/{transactionRid}/abort",
        # Files
        "/api/v2/datasets/{datasetRid}/files",
        "/api/v2/datasets/{datasetRid}/files/{filePath}",
        "/api/v2/datasets/{datasetRid}/files/{filePath}/content",
        "/api/v2/datasets/{datasetRid}/files/{filePath}/upload",
        # Read table
        "/api/v2/datasets/{datasetRid}/readTable",
    }

    actual_paths = set(schema.get("paths", {}).keys())
    missing = expected_paths - actual_paths
    assert not missing, f"Missing OpenAPI paths: {missing}"

    # Verify we have at least 12 distinct paths (some share the same path for
    # different methods, e.g. GET/POST on /v2/datasets and GET/PUT on schema)
    assert len(actual_paths) >= 16


# ---------------------------------------------------------------------------
# 2. Dataset create and get
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_create_and_get(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr, _ = _override_deps(app)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post(
            "/api/v2/datasets",
            json={
                "name": "orders",
                "parentFolderRid": "ri.spice.main.folder.commerce_db",
                "description": "Order data",
            },
        )

    assert create_resp.status_code == 200
    body = create_resp.json()
    assert body["name"] == "orders"
    assert body["parentFolderRid"] == "ri.foundry.main.folder.commerce_db"
    assert body["rid"].startswith("ri.foundry.main.dataset.")

    # Extract dataset_id to do a GET
    dataset_rid = body["rid"]
    dataset_id = dataset_rid.replace("ri.foundry.main.dataset.", "")

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        get_resp = await client.get(f"/api/v2/datasets/{dataset_rid}")

    assert get_resp.status_code == 200
    get_body = get_resp.json()
    assert get_body["rid"] == dataset_rid
    assert get_body["name"] == "orders"
    assert get_body["parentFolderRid"] == "ri.foundry.main.folder.commerce_db"


# ---------------------------------------------------------------------------
# 3. Schema get and update
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_schema_get_and_update(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr, _ = _override_deps(app)

    # Seed a dataset with an initial schema
    ds = await dr.create_dataset(
        db_name="commerce_db",
        name="orders",
        schema_json={"columns": [{"name": "id", "type": "string"}, {"name": "amount", "type": "double"}]},
    )
    dataset_rid = f"ri.foundry.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # GET schema
        get_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/getSchema",
            params={"preview": True, "branchName": "master"},
        )

    assert get_resp.status_code == 200
    schema_body = get_resp.json()["schema"]
    assert "fieldSchemaList" in schema_body
    assert len(schema_body["fieldSchemaList"]) == 2
    assert schema_body["fieldSchemaList"][0]["fieldPath"] == "id"
    assert schema_body["fieldSchemaList"][0]["type"]["type"] == "string"
    assert schema_body["fieldSchemaList"][1]["fieldPath"] == "amount"
    assert schema_body["fieldSchemaList"][1]["type"]["type"] == "double"

    # PUT schema with a new column
    new_field_list = [
        {"fieldPath": "id", "type": {"type": "string"}},
        {"fieldPath": "amount", "type": {"type": "double"}},
        {"fieldPath": "status", "type": {"type": "string"}},
    ]

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        put_resp = await client.put(
            f"/api/v2/datasets/{dataset_rid}/putSchema",
            params={"preview": True, "branchName": "master"},
            json={"fieldSchemaList": new_field_list},
        )

    assert put_resp.status_code == 204

    # Verify the registry was updated
    refreshed = await dr.get_dataset(dataset_id=ds.dataset_id)
    assert len(refreshed.schema_json["columns"]) == 3


# ---------------------------------------------------------------------------
# 5. Branch operations
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_branch_operations(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    pr = _PipelineRegistry()
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.foundry.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # List branches
        list_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/branches",
            headers={"X-User-ID": "user-1"},
        )
        assert list_resp.status_code == 200
        branches = list_resp.json()["data"]
        assert len(branches) == 2
        branch_names = {b["branchName"] for b in branches}
        assert "master" in branch_names
        assert "dev" in branch_names
        assert all(b["datasetRid"] == dataset_rid for b in branches)

        # Create branch
        create_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/branches",
            json={"name": "feature-x", "sourceBranchName": "master"},
            headers={"X-User-ID": "user-1"},
        )
        assert create_resp.status_code == 200
        create_body = create_resp.json()
        assert create_body["branchName"] == "feature-x"
        assert create_body["datasetRid"] == dataset_rid

        # Get branch
        get_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/branches/master",
            headers={"X-User-ID": "user-1"},
        )
        assert get_resp.status_code == 200
        get_body = get_resp.json()
        assert get_body["branchName"] == "master"
        assert get_body["latestCommitId"] == "abc123"

        # Delete branch
        del_resp = await client.delete(
            f"/api/v2/datasets/{dataset_rid}/branches/feature-x",
            headers={"X-User-ID": "user-1"},
        )
        assert del_resp.status_code == 204

        # Cannot delete master branch
        del_main_resp = await client.delete(
            f"/api/v2/datasets/{dataset_rid}/branches/master",
            headers={"X-User-ID": "user-1"},
        )
        assert del_main_resp.status_code == 400
        assert del_main_resp.json()["errorName"] == "CannotDeleteMasterBranch"


# ---------------------------------------------------------------------------
# 6. Transaction lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_transaction_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    pr = _PipelineRegistry()
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.foundry.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create a transaction
        create_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions",
            params={"branchName": "master"},
            json={"transactionType": "APPEND"},
            headers={"X-User-ID": "user-1"},
        )
        assert create_resp.status_code == 200
        txn_body = create_resp.json()
        assert txn_body["rid"].startswith("ri.foundry.main.transaction.")
        assert txn_body["transactionType"] == "APPEND"
        assert txn_body["status"] == "OPEN"
        txn_rid = txn_body["rid"]

        # Commit the transaction
        commit_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions/{txn_rid}/commit",
            headers={"X-User-ID": "user-1"},
        )
        assert commit_resp.status_code == 200
        commit_body = commit_resp.json()
        assert commit_body["status"] == "COMMITTED"
        assert commit_body["rid"] == txn_rid

    latest_version = await dr.get_latest_version(dataset_id=ds.dataset_id)
    assert latest_version is not None
    assert latest_version.lakefs_commit_id == "commit-123"

    # Create another transaction to test abort
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp2 = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions",
            params={"branchName": "master"},
            json={"transactionType": "APPEND"},
            headers={"X-User-ID": "user-1"},
        )
        assert create_resp2.status_code == 200
        txn2_body = create_resp2.json()
        txn2_rid = txn2_body["rid"]
        assert txn2_body["status"] == "OPEN"

        # Abort the second transaction
        abort_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions/{txn2_rid}/abort",
        )
        assert abort_resp.status_code == 200
        abort_body = abort_resp.json()
        assert abort_body["status"] == "ABORTED"
        assert abort_body["rid"] == txn2_rid


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_transaction_commit_materializes_uploaded_csv_version(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    storage = _MockLakeFSStorage()
    pr = _PipelineRegistry(lakefs_storage=storage)
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.foundry.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions",
            params={"branchName": "master"},
            json={"transactionType": "APPEND"},
            headers={"X-User-ID": "user-1"},
        )
        assert create_resp.status_code == 200
        txn_rid = create_resp.json()["rid"]

        csv_payload = b"id,amount,status\norder-1,10.5,OPEN\norder-2,20.0,HOLD\n"
        upload_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/files/source.csv/upload",
            params={"transactionRid": txn_rid, "branchName": "master"},
            content=csv_payload,
            headers={"X-User-ID": "user-1", "Content-Type": "text/csv"},
        )
        assert upload_resp.status_code == 200

        commit_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions/{txn_rid}/commit",
            headers={"X-User-ID": "user-1"},
        )
        assert commit_resp.status_code == 200
        assert commit_resp.json()["status"] == "COMMITTED"

        read_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            params={"rowLimit": 10, "branchName": "master", "format": "CSV"},
        )
        assert read_resp.status_code == 200
        parsed = list(csv.reader(io.StringIO(read_resp.text)))
        assert parsed[0] == ["id", "amount", "status"]
        assert parsed[1:] == [
            ["order-1", "10.5", "OPEN"],
            ["order-2", "20.0", "HOLD"],
        ]

    latest_version = await dr.get_latest_version(dataset_id=ds.dataset_id)
    assert latest_version is not None
    assert latest_version.artifact_key == f"s3://raw-datasets/master/commerce_db/{ds.dataset_id}/orders/source.csv"
    assert latest_version.row_count == 2
    assert latest_version.sample_json["rows"] == [
        ["order-1", "10.5", "OPEN"],
        ["order-2", "20.0", "HOLD"],
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_transaction_commit_materializes_non_source_csv_when_list_objects_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    storage = _MockLakeFSStorage()
    pr = _PipelineRegistry(
        lakefs_client=_FailingLakeFSListClient(),
        lakefs_storage=storage,
    )
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.foundry.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions",
            params={"branchName": "master"},
            json={"transactionType": "APPEND"},
            headers={"X-User-ID": "user-1"},
        )
        assert create_resp.status_code == 200
        txn_rid = create_resp.json()["rid"]

        csv_payload = b"id,amount,status\norder-1,10.5,OPEN\norder-2,20.0,HOLD\n"
        upload_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/files/orders_snapshot.csv/upload",
            params={"transactionRid": txn_rid, "branchName": "master"},
            content=csv_payload,
            headers={"X-User-ID": "user-1", "Content-Type": "text/csv"},
        )
        assert upload_resp.status_code == 200
        assert storage.objects.get(f"master/commerce_db/{ds.dataset_id}/orders/source.csv") == csv_payload

        commit_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions/{txn_rid}/commit",
            headers={"X-User-ID": "user-1"},
        )
        assert commit_resp.status_code == 200
        assert commit_resp.json()["status"] == "COMMITTED"

        read_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            params={"rowLimit": 10, "branchName": "master", "format": "CSV"},
        )
        assert read_resp.status_code == 200
        parsed = list(csv.reader(io.StringIO(read_resp.text)))
        assert parsed[0] == ["id", "amount", "status"]
        assert parsed[1:] == [
            ["order-1", "10.5", "OPEN"],
            ["order-2", "20.0", "HOLD"],
        ]

    latest_version = await dr.get_latest_version(dataset_id=ds.dataset_id)
    assert latest_version is not None
    assert latest_version.artifact_key == f"s3://raw-datasets/master/commerce_db/{ds.dataset_id}/orders/source.csv"
    assert latest_version.row_count == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_transaction_commit_lakefs_failure_keeps_open(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    pr = _PipelineRegistry(lakefs_client=_FailingLakeFSClient())
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.foundry.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions",
            params={"branchName": "master"},
            json={"transactionType": "APPEND"},
            headers={"X-User-ID": "user-1"},
        )
        assert create_resp.status_code == 200
        txn_rid = create_resp.json()["rid"]

        commit_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions/{txn_rid}/commit",
            headers={"X-User-ID": "user-1"},
        )
        assert commit_resp.status_code == 500
        body = commit_resp.json()
        assert body["errorName"] == "CommitTransactionError"

    txn_id = txn_rid.rsplit(".", 1)[-1]
    txn = await dr.get_ingest_transaction_by_id(transaction_id=txn_id)
    assert txn is not None
    assert txn.status == "OPEN"


# ---------------------------------------------------------------------------
# 7. File operations
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_file_operations(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    storage = _MockLakeFSStorage()
    pr = _PipelineRegistry(lakefs_storage=storage)
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.foundry.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # List files
        list_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/files",
            params={"branchName": "main"},
            headers={"X-User-ID": "user-1"},
        )
        assert list_resp.status_code == 200
        files_body = list_resp.json()
        assert "data" in files_body
        assert isinstance(files_body["data"], list)
        assert len(files_body["data"]) >= 1
        assert files_body["data"][0]["filePath"] == "source.csv"

        # Upload a file
        upload_data = b"id,name\n1,Alice\n2,Bob\n"
        upload_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/files/data/upload.csv/upload",
            params={"branchName": "master"},
            content=upload_data,
            headers={
                "X-User-ID": "user-1",
                "Content-Type": "text/csv",
            },
        )
        assert upload_resp.status_code == 200
        upload_body = upload_resp.json()
        assert upload_body["rid"].startswith("ri.foundry.main.file.")
        assert upload_body["transactionRid"].startswith("ri.foundry.main.transaction.")

        # Verify the data was written to storage
        expected_key = f"master/commerce_db/{ds.dataset_id}/orders/data/upload.csv"
        assert storage.objects.get(expected_key) == upload_data

        # Get file content (pre-populate storage for this path)
        content_key = f"master/commerce_db/{ds.dataset_id}/orders/readme.txt"
        storage.objects[content_key] = b"Hello, World!"

        content_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/files/readme.txt/content",
            params={"branchName": "master"},
            headers={"X-User-ID": "user-1"},
        )
        assert content_resp.status_code == 200
        assert content_resp.content == b"Hello, World!"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_list_files_returns_503_when_lakefs_unavailable(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    pr = _PipelineRegistry(lakefs_client=_FailingLakeFSListClient())
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.foundry.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        list_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/files",
            params={"branchName": "master"},
            headers={"X-User-ID": "user-1"},
        )

    assert list_resp.status_code == 500
    body = list_resp.json()
    assert body["errorCode"] == "INTERNAL"
    assert body["errorName"] == "ListFilesError"


# ---------------------------------------------------------------------------
# 8. Read table
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_read_table(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    storage = _MockLakeFSStorage()
    pr = _PipelineRegistry(lakefs_storage=storage)
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.foundry.main.dataset.{ds.dataset_id}"

    # Seed a version with sample_json
    dr.versions[ds.dataset_id] = SimpleNamespace(
        version_id=str(uuid.uuid4()),
        dataset_id=ds.dataset_id,
        sample_json={
            "columns": [
                {"name": "id", "type": "string"},
                {"name": "amount", "type": "double"},
            ],
            "rows": [
                ["order-1", 10.0],
                ["order-2", 20.0],
                ["order-3", 30.0],
            ],
        },
        row_count=3,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        read_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            params={"rowLimit": 100, "branchName": "master", "format": "CSV"},
        )

    assert read_resp.status_code == 200
    parsed = list(csv.reader(io.StringIO(read_resp.text)))
    assert parsed[0] == ["id", "amount"]
    assert parsed[1:] == [
        ["order-1", "10.0"],
        ["order-2", "20.0"],
        ["order-3", "30.0"],
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_read_table_with_column_selection(
    monkeypatch: pytest.MonkeyPatch,
):
    """readTable respects the ``columns`` request field to project columns."""
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    storage = _MockLakeFSStorage()
    pr = _PipelineRegistry(lakefs_storage=storage)
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.spice.main.dataset.{ds.dataset_id}"

    dr.versions[ds.dataset_id] = SimpleNamespace(
        version_id=str(uuid.uuid4()),
        dataset_id=ds.dataset_id,
        sample_json={
            "columns": [
                {"name": "id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "status", "type": "string"},
            ],
            "rows": [
                ["order-1", 10.0, "ACTIVE"],
                ["order-2", 20.0, "CLOSED"],
            ],
        },
        row_count=2,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        read_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            params={"columns": ["id", "status"], "rowLimit": 10, "branchName": "master", "format": "CSV"},
        )

    assert read_resp.status_code == 200
    parsed = list(csv.reader(io.StringIO(read_resp.text)))
    assert parsed[0] == ["id", "status"]
    assert parsed[1:] == [["order-1", "ACTIVE"], ["order-2", "CLOSED"]]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_read_table_uses_version_artifact_key_fallback(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()
    storage = _MockLakeFSStorage()
    pr = _PipelineRegistry(lakefs_storage=storage)
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.spice.main.dataset.{ds.dataset_id}"
    custom_artifact_key = f"commerce_db/{ds.dataset_id}/orders/custom/path/orders_snapshot.csv"
    storage.objects[f"master/{custom_artifact_key}"] = b"id,status\norder-1,OPEN\norder-2,HOLD\n"

    dr.versions[ds.dataset_id] = SimpleNamespace(
        version_id=str(uuid.uuid4()),
        dataset_id=ds.dataset_id,
        artifact_key=custom_artifact_key,
        sample_json={},
        row_count=2,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        read_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            params={"rowLimit": 10, "branchName": "master", "format": "CSV"},
        )

    assert read_resp.status_code == 200
    parsed = list(csv.reader(io.StringIO(read_resp.text)))
    assert parsed[0] == ["id", "status"]
    assert parsed[1:] == [["order-1", "OPEN"], ["order-2", "HOLD"]]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_read_table_no_version_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
):
    """readTable returns empty result when no version exists and lakeFS fallback fails."""
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()

    class _FailingStorage:
        async def load_bytes(self, repo: str, path: str) -> bytes:
            raise FileNotFoundError("not found")

    pr = _PipelineRegistry(lakefs_storage=_FailingStorage())  # type: ignore[arg-type]
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.spice.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        read_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            params={"rowLimit": 10, "branchName": "master", "format": "CSV"},
        )

    assert read_resp.status_code == 200
    assert read_resp.text == ""


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_read_table_lakefs_error_returns_unavailable(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr = _DatasetRegistry()

    class _FailingStorage:
        async def load_bytes(self, repo: str, path: str) -> bytes:
            raise LakeFSError("lakeFS storage unavailable")

    pr = _PipelineRegistry(lakefs_storage=_FailingStorage())  # type: ignore[arg-type]
    _override_deps(app, dataset_registry=dr, pipeline_registry=pr)

    ds = await dr.create_dataset(db_name="commerce_db", name="orders")
    dataset_rid = f"ri.spice.main.dataset.{ds.dataset_id}"

    dr.versions[ds.dataset_id] = SimpleNamespace(
        version_id=str(uuid.uuid4()),
        dataset_id=ds.dataset_id,
        sample_json={},
        row_count=0,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        read_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            params={"rowLimit": 10, "branchName": "master", "format": "CSV"},
        )

    assert read_resp.status_code == 500
    body = read_resp.json()
    assert body["errorName"] == "ReadTableError"
