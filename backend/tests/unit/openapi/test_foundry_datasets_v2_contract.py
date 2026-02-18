"""Contract tests for Foundry Datasets API v2 router.

Validates OpenAPI path registration, dataset CRUD, schema operations,
branch management, transaction lifecycle, file operations, and table
reads against the ``bff.routers.foundry_datasets_v2`` router.

Test pattern follows ``test_foundry_platform_v2_contract.py``.
"""

import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from bff.routers import foundry_datasets_v2
from bff.routers.data_connector_deps import get_dataset_registry, get_pipeline_registry
from shared.utils.time_utils import utcnow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(foundry_datasets_v2.router, prefix="/api")
    return app


# ---------------------------------------------------------------------------
# Mock registries
# ---------------------------------------------------------------------------

class _DatasetRegistry:
    def __init__(self) -> None:
        self.datasets: dict[str, Any] = {}
        self.versions: dict[str, Any] = {}
        self.transactions: dict[str, Any] = {}  # keyed by ingest_request_id

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
            branch=kwargs.get("branch", "main"),
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
        )
        return req, True

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
        # Key by both ingest_request_id and transaction_id because the router
        # returns the transaction_id in the RID but looks up via the
        # ingest_request_id parameter (passing the extracted transaction_id).
        self.transactions[ingest_request_id] = txn
        self.transactions[transaction_id] = txn
        return txn

    async def get_ingest_transaction(self, *, ingest_request_id: str) -> SimpleNamespace | None:  # noqa: ANN001
        return self.transactions.get(ingest_request_id)

    async def mark_ingest_transaction_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: str | None,
    ) -> SimpleNamespace | None:
        txn = self.transactions.get(ingest_request_id)
        if txn:
            txn.status = "COMMITTED"
            txn.lakefs_commit_id = lakefs_commit_id
            txn.committed_at = utcnow()
        return txn

    async def mark_ingest_transaction_aborted(
        self,
        *,
        ingest_request_id: str,
        error: str,
    ) -> SimpleNamespace | None:
        txn = self.transactions.get(ingest_request_id)
        if txn:
            txn.status = "ABORTED"
            txn.error = error
            txn.aborted_at = utcnow()
        return txn


class _MockLakeFSClient:
    async def list_branches(self, repository: str, **kwargs: Any) -> list[dict[str, Any]]:  # noqa: ANN401
        return [
            {"name": "main", "commit_id": "abc123"},
            {"name": "dev", "commit_id": "def456"},
        ]

    async def create_branch(self, repository: str, name: str, source: str = "main") -> dict[str, str]:
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


class _MockLakeFSStorage:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def load_bytes(self, repo: str, path: str) -> bytes:
        return self.objects.get(path, b"col1,col2\nval1,val2\n")

    async def save_bytes(self, repo: str, path: str, data: bytes, content_type: str | None = None) -> None:
        self.objects[path] = data


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
        "/api/v2/datasets/{datasetRid}/schema",
        # Branches
        "/api/v2/datasets/{datasetRid}/branches",
        "/api/v2/datasets/{datasetRid}/branches/{branchName}",
        # Transactions
        "/api/v2/datasets/{datasetRid}/transactions",
        "/api/v2/datasets/{datasetRid}/transactions/{transactionRid}/commit",
        "/api/v2/datasets/{datasetRid}/transactions/{transactionRid}/abort",
        # Files
        "/api/v2/datasets/{datasetRid}/files",
        "/api/v2/datasets/{datasetRid}/files/{filePath}/content",
        "/api/v2/datasets/{datasetRid}/files:upload",
        # Read table
        "/api/v2/datasets/{datasetRid}/readTable",
    }

    actual_paths = set(schema.get("paths", {}).keys())
    missing = expected_paths - actual_paths
    assert not missing, f"Missing OpenAPI paths: {missing}"

    # Verify dataset list has expected query params
    list_params = schema["paths"]["/api/v2/datasets"]["get"].get("parameters", [])
    param_names = [p["name"] for p in list_params]
    assert "pageSize" in param_names
    assert "pageToken" in param_names

    # Verify we have at least 12 distinct paths (some share the same path for
    # different methods, e.g. GET/POST on /v2/datasets and GET/PUT on schema)
    assert len(actual_paths) >= 12


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
                "branchName": "main",
                "description": "Order data",
            },
        )

    assert create_resp.status_code == 200
    body = create_resp.json()
    assert body["name"] == "orders"
    assert body["parentFolderRid"] == "ri.spice.main.folder.commerce_db"
    assert body["branchName"] == "main"
    assert body["createdTime"] is not None
    assert body["rid"].startswith("ri.spice.main.dataset.")

    # Extract dataset_id to do a GET
    dataset_rid = body["rid"]
    dataset_id = dataset_rid.replace("ri.spice.main.dataset.", "")

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        get_resp = await client.get(f"/api/v2/datasets/{dataset_rid}")

    assert get_resp.status_code == 200
    get_body = get_resp.json()
    assert get_body["rid"] == dataset_rid
    assert get_body["name"] == "orders"
    assert get_body["parentFolderRid"] == "ri.spice.main.folder.commerce_db"
    assert get_body["createdTime"] is not None


# ---------------------------------------------------------------------------
# 3. Dataset list with pagination
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_datasets_list_with_pagination(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        foundry_datasets_v2,
        "_resolve_lakefs_raw_repository",
        lambda: "raw-datasets",
    )

    app = _build_test_app()
    dr, _ = _override_deps(app)

    # Seed three datasets in the same db
    for i in range(3):
        await dr.create_dataset(
            db_name="commerce_db",
            name=f"dataset_{i}",
            description=f"Dataset {i}",
            source_type="foundry_api",
            branch="main",
        )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # First page: pageSize=2
        page1 = await client.get(
            "/api/v2/datasets",
            params={"parentFolderRid": "ri.spice.main.folder.commerce_db", "pageSize": 2},
        )

    assert page1.status_code == 200
    p1 = page1.json()
    assert len(p1["data"]) == 2
    assert p1["nextPageToken"] is not None

    # Second page using the token
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        page2 = await client.get(
            "/api/v2/datasets",
            params={
                "parentFolderRid": "ri.spice.main.folder.commerce_db",
                "pageSize": 2,
                "pageToken": p1["nextPageToken"],
            },
        )

    assert page2.status_code == 200
    p2 = page2.json()
    assert len(p2["data"]) == 1
    assert p2["nextPageToken"] is None

    # Verify all RIDs are distinct
    all_rids = [d["rid"] for d in p1["data"] + p2["data"]]
    assert len(set(all_rids)) == 3


# ---------------------------------------------------------------------------
# 4. Schema get and update
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
    dataset_rid = f"ri.spice.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # GET schema
        get_resp = await client.get(f"/api/v2/datasets/{dataset_rid}/schema")

    assert get_resp.status_code == 200
    schema_body = get_resp.json()
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
            f"/api/v2/datasets/{dataset_rid}/schema",
            json={"fieldSchemaList": new_field_list},
        )

    assert put_resp.status_code == 200
    updated = put_resp.json()
    assert len(updated["fieldSchemaList"]) == 3
    assert updated["fieldSchemaList"][2]["fieldPath"] == "status"

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
    dataset_rid = f"ri.spice.main.dataset.{ds.dataset_id}"

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
        assert "main" in branch_names
        assert "dev" in branch_names
        assert all(b["datasetRid"] == dataset_rid for b in branches)

        # Create branch
        create_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/branches",
            json={"branchName": "feature-x", "sourceBranchName": "main"},
            headers={"X-User-ID": "user-1"},
        )
        assert create_resp.status_code == 200
        create_body = create_resp.json()
        assert create_body["branchName"] == "feature-x"
        assert create_body["datasetRid"] == dataset_rid

        # Get branch
        get_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/branches/main",
            headers={"X-User-ID": "user-1"},
        )
        assert get_resp.status_code == 200
        get_body = get_resp.json()
        assert get_body["branchName"] == "main"
        assert get_body["latestCommitId"] == "abc123"

        # Delete branch
        del_resp = await client.delete(
            f"/api/v2/datasets/{dataset_rid}/branches/feature-x",
            headers={"X-User-ID": "user-1"},
        )
        assert del_resp.status_code == 204

        # Cannot delete main branch
        del_main_resp = await client.delete(
            f"/api/v2/datasets/{dataset_rid}/branches/main",
            headers={"X-User-ID": "user-1"},
        )
        assert del_main_resp.status_code == 400
        assert del_main_resp.json()["errorName"] == "CannotDeleteMainBranch"


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
    dataset_rid = f"ri.spice.main.dataset.{ds.dataset_id}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create a transaction
        create_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions",
            json={"transactionType": "APPEND"},
            headers={"X-User-ID": "user-1"},
        )
        assert create_resp.status_code == 200
        txn_body = create_resp.json()
        assert txn_body["rid"].startswith("ri.spice.main.transaction.")
        assert txn_body["datasetRid"] == dataset_rid
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
        assert commit_body["datasetRid"] == dataset_rid

    # Create another transaction to test abort
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp2 = await client.post(
            f"/api/v2/datasets/{dataset_rid}/transactions",
            json={},
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
        assert abort_body["datasetRid"] == dataset_rid


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
    dataset_rid = f"ri.spice.main.dataset.{ds.dataset_id}"

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
        assert files_body["data"][0]["path"] == "source.csv"

        # Upload a file
        upload_data = b"id,name\n1,Alice\n2,Bob\n"
        upload_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/files:upload",
            params={"filePath": "data/upload.csv", "branchName": "main"},
            content=upload_data,
            headers={
                "X-User-ID": "user-1",
                "Content-Type": "text/csv",
            },
        )
        assert upload_resp.status_code == 200
        upload_body = upload_resp.json()
        assert upload_body["path"] == "data/upload.csv"
        assert upload_body["sizeBytes"] == len(upload_data)

        # Verify the data was written to storage
        expected_key = f"main/commerce_db/{ds.dataset_id}/orders/data/upload.csv"
        assert storage.objects.get(expected_key) == upload_data

        # Get file content (pre-populate storage for this path)
        content_key = f"main/commerce_db/{ds.dataset_id}/orders/readme.txt"
        storage.objects[content_key] = b"Hello, World!"

        content_resp = await client.get(
            f"/api/v2/datasets/{dataset_rid}/files/readme.txt/content",
            params={"branchName": "main"},
            headers={"X-User-ID": "user-1"},
        )
        assert content_resp.status_code == 200
        assert content_resp.content == b"Hello, World!"


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
    dataset_rid = f"ri.spice.main.dataset.{ds.dataset_id}"

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
        read_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            json={"rowLimit": 100},
            headers={"X-User-ID": "user-1"},
        )

    assert read_resp.status_code == 200
    table = read_resp.json()
    assert len(table["columns"]) == 2
    assert table["columns"][0]["name"] == "id"
    assert table["columns"][1]["name"] == "amount"
    assert len(table["rows"]) == 3
    assert table["rows"][0] == ["order-1", 10.0]
    assert table["totalRowCount"] == 3


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
        read_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            json={"columns": ["id", "status"], "rowLimit": 10},
            headers={"X-User-ID": "user-1"},
        )

    assert read_resp.status_code == 200
    table = read_resp.json()
    assert len(table["columns"]) == 2
    col_names = [c["name"] for c in table["columns"]]
    assert col_names == ["id", "status"]
    assert table["rows"][0] == ["order-1", "ACTIVE"]
    assert table["rows"][1] == ["order-2", "CLOSED"]


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
        read_resp = await client.post(
            f"/api/v2/datasets/{dataset_rid}/readTable",
            json={},
            headers={"X-User-ID": "user-1"},
        )

    assert read_resp.status_code == 200
    table = read_resp.json()
    assert table["columns"] == []
    assert table["rows"] == []
    assert table["totalRowCount"] == 0
