import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from bff.dependencies import BFFDependencyProvider
from bff.routers import foundry_connectivity_v2, foundry_orchestration_v2
from bff.routers.data_connector_deps import (
    get_connector_registry,
    get_dataset_registry as get_connector_dataset_registry,
    get_google_sheets_service,
    get_objectify_job_queue,
    get_objectify_registry,
    get_pipeline_registry as get_connector_pipeline_registry,
)
from bff.routers.pipeline_deps import (
    get_dataset_registry as get_pipeline_dataset_registry,
    get_pipeline_job_queue,
    get_pipeline_registry,
)
from shared.dependencies.providers import get_audit_log_store, get_lineage_store


def _param_names(schema: dict, *, path: str, method: str) -> list[str]:
    path_item = schema.get("paths", {}).get(path, {}).get(method, {})
    return [param.get("name") for param in path_item.get("parameters", [])]


def _build_test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(foundry_orchestration_v2.router, prefix="/api")
    app.include_router(foundry_connectivity_v2.router, prefix="/api")
    return app


@pytest.mark.unit
def test_foundry_platform_v2_paths_exist_in_openapi():
    app = _build_test_app()
    schema = app.openapi()

    expected_paths = {
        "/api/v2/orchestration/builds/create",
        "/api/v2/orchestration/builds/{buildRid}",
        "/api/v2/orchestration/builds/getBatch",
        "/api/v2/orchestration/builds/{buildRid}/jobs",
        "/api/v2/orchestration/builds/{buildRid}/cancel",
        "/api/v2/connectivity/connections/{connectionRid}/tableImports",
        "/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}",
        "/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}/execute",
    }
    for path in expected_paths:
        assert path in schema.get("paths", {})

    build_params = _param_names(
        schema,
        path="/api/v2/orchestration/builds/create",
        method="post",
    )
    assert build_params == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_orchestration_create_build_returns_foundry_build_shape(
    monkeypatch: pytest.MonkeyPatch,
):
    pipeline_id = str(uuid.uuid4())

    class _PipelineRegistry:
        async def get_pipeline(self, pipeline_id: str):  # noqa: ANN001
            return SimpleNamespace(branch="main", pipeline_id=pipeline_id)

        async def get_run(self, *, pipeline_id: str, job_id: str):  # noqa: ANN001
            return {
                "pipeline_id": pipeline_id,
                "job_id": job_id,
                "status": "QUEUED",
                "started_at": None,
                "finished_at": None,
                "output_json": {},
            }

    async def _fake_build_pipeline(**kwargs: Any):  # noqa: ANN401
        _ = kwargs
        return {"data": {"job_id": "build-test-1"}}

    app = _build_test_app()
    app.dependency_overrides[get_pipeline_registry] = lambda: _PipelineRegistry()
    app.dependency_overrides[get_pipeline_job_queue] = lambda: object()
    app.dependency_overrides[get_pipeline_dataset_registry] = lambda: object()
    app.dependency_overrides[get_audit_log_store] = lambda: object()
    app.dependency_overrides[BFFDependencyProvider.get_oms_client] = lambda: object()
    monkeypatch.setattr(
        foundry_orchestration_v2.pipeline_execution_service,
        "build_pipeline",
        _fake_build_pipeline,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/orchestration/builds/create",
            json={
                "target": {"targetRids": [f"ri.spice.main.pipeline.{pipeline_id}"]},
                "branchName": "main",
            },
            headers={"X-User-ID": "user-123"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["rid"] == "ri.spice.main.build.build-test-1"
    assert payload["branchName"] == "main"
    assert payload["createdBy"] == "user-123"
    assert payload["jobRids"] == ["ri.spice.main.job.build-test-1"]
    assert payload["status"] == "RUNNING"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_orchestration_get_batch_jobs_and_cancel_flow():
    pipeline_id = str(uuid.uuid4())
    job_id = f"build-{pipeline_id}-{uuid.uuid4()}"
    captured: dict[str, Any] = {}

    class _PipelineRegistry:
        async def get_pipeline(self, pipeline_id: str):  # noqa: ANN001
            return SimpleNamespace(branch="main", pipeline_id=pipeline_id)

        async def get_run(self, *, pipeline_id: str, job_id: str):  # noqa: ANN001
            return {
                "pipeline_id": pipeline_id,
                "job_id": job_id,
                "status": "RUNNING",
                "started_at": None,
                "finished_at": None,
                "node_id": None,
                "row_count": None,
                "sample_json": {},
                "output_json": {
                    "outputs": [{"dataset_id": "dataset-1"}],
                    "requested_by": "alice",
                },
                "pipeline_spec_commit_id": None,
                "pipeline_spec_hash": None,
                "input_lakefs_commits": None,
                "output_lakefs_commit_id": None,
                "spark_conf": None,
                "code_version": None,
            }

        async def record_run(self, **kwargs: Any):  # noqa: ANN401
            captured.update(kwargs)

    app = _build_test_app()
    app.dependency_overrides[get_pipeline_registry] = lambda: _PipelineRegistry()

    build_rid = f"ri.spice.main.build.{job_id}"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        batch_resp = await client.post(
            "/api/v2/orchestration/builds/getBatch",
            json=[{"buildRid": build_rid}],
        )
        jobs_resp = await client.get(f"/api/v2/orchestration/builds/{build_rid}/jobs")
        cancel_resp = await client.post(f"/api/v2/orchestration/builds/{build_rid}/cancel")

    assert batch_resp.status_code == 200
    batch_payload = batch_resp.json()
    assert build_rid in batch_payload["data"]
    assert batch_payload["data"][build_rid]["status"] == "RUNNING"

    assert jobs_resp.status_code == 200
    jobs_payload = jobs_resp.json()
    assert len(jobs_payload["data"]) == 1
    assert jobs_payload["data"][0]["jobStatus"] == "RUNNING"
    assert jobs_payload["data"][0]["outputs"][0]["datasetRid"] == "ri.spice.main.dataset.dataset-1"

    assert cancel_resp.status_code == 204
    assert captured["status"] == "CANCELED"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connectivity_connection_scoped_table_import_create(
    monkeypatch: pytest.MonkeyPatch,
):
    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("google_sheets_connection", "conn-1"): SimpleNamespace(
                    source_id="conn-1",
                    source_type="google_sheets_connection",
                    enabled=True,
                    config_json={"label": "Google Sheets"},
                )
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict):  # noqa: ANN001
            self.sources[(source_type, source_id)] = SimpleNamespace(
                source_id=source_id,
                source_type=source_type,
                enabled=enabled,
                config_json=dict(config_json),
            )
            return self.sources[(source_type, source_id)]

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return SimpleNamespace(
                target_db_name="sales_db",
                target_branch="main",
                target_class_label="Order",
            )

        async def list_sources(self, *, source_type: str, enabled: bool, limit: int):  # noqa: ANN001
            _ = enabled, limit
            return [v for (kind, _), v in self.sources.items() if kind == source_type]

        async def set_source_enabled(self, *, source_type: str, source_id: str, enabled: bool):  # noqa: ANN001
            src = self.sources.get((source_type, source_id))
            if src is None:
                return False
            src.enabled = enabled
            return True

    class _DatasetRegistry:
        async def get_dataset(self, *, dataset_id: str):  # noqa: ANN001
            _ = dataset_id
            return None

        async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return None

    async def _fake_register_google_sheet(**kwargs: Any):  # noqa: ANN401
        registry = kwargs["connector_registry"]
        await registry.upsert_source(
            source_type="google_sheets",
            source_id="sheet-123",
            enabled=True,
            config_json={
                "sheet_url": "https://docs.google.com/spreadsheets/d/abc",
                "worksheet_name": "Sheet1",
                "connection_id": "conn-1",
            },
        )
        return {
            "data": {
                "sheet_id": "sheet-123",
                "registered_sheet": {
                    "sheet_url": "https://docs.google.com/spreadsheets/d/abc",
                    "worksheet_name": "Sheet1",
                },
                "dataset": {"dataset_id": "dataset-1"},
            }
        }

    app = _build_test_app()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: _DatasetRegistry()
    app.dependency_overrides[get_lineage_store] = lambda: object()
    monkeypatch.setattr(
        foundry_connectivity_v2.data_connector_registration_service,
        "register_google_sheet",
        _fake_register_google_sheet,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-1/tableImports",
            json={
                "displayName": "Orders import",
                "importMode": "SNAPSHOT",
                "allowSchemaChanges": True,
                "source": {
                    "sheetUrl": "https://docs.google.com/spreadsheets/d/abc",
                    "worksheetName": "Sheet1",
                },
                "destination": {
                    "ontology": "sales_db",
                    "objectType": "Order",
                    "branchName": "main",
                },
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["rid"] == "ri.spice.main.table-import.sheet-123"
    assert payload["connectionRid"] == "ri.spice.main.connection.conn-1"
    assert payload["datasetRid"] == "ri.spice.main.dataset.dataset-1"
    assert payload["importMode"] == "SNAPSHOT"
    assert payload["allowSchemaChanges"] is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connectivity_get_list_execute_and_delete_table_import(
    monkeypatch: pytest.MonkeyPatch,
):
    runs: dict[tuple[str, str], dict[str, Any]] = {}

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("google_sheets_connection", "conn-1"): SimpleNamespace(
                    source_id="conn-1",
                    source_type="google_sheets_connection",
                    enabled=True,
                    config_json={"label": "Google Sheets"},
                ),
                ("google_sheets", "sheet-123"): SimpleNamespace(
                    source_id="sheet-123",
                    source_type="google_sheets",
                    enabled=True,
                    config_json={
                        "connection_id": "conn-1",
                        "dataset_rid": "ri.spice.main.dataset.dataset-1",
                        "display_name": "Orders import",
                        "import_mode": "SNAPSHOT",
                        "allow_schema_changes": False,
                        "table_import_config": {
                            "type": "jdbcImportConfig",
                            "query": "SELECT * FROM EXTERNAL_SHEET('https://docs.google.com/spreadsheets/d/abc#Sheet1')",
                        },
                        "sheet_url": "https://docs.google.com/spreadsheets/d/abc",
                        "worksheet_name": "Sheet1",
                    },
                ),
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict):  # noqa: ANN001
            self.sources[(source_type, source_id)] = SimpleNamespace(
                source_id=source_id,
                source_type=source_type,
                enabled=enabled,
                config_json=dict(config_json),
            )
            return self.sources[(source_type, source_id)]

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return SimpleNamespace(
                target_db_name="sales_db",
                target_branch="main",
                target_class_label="Order",
            )

        async def list_sources(self, *, source_type: str, enabled: bool, limit: int):  # noqa: ANN001
            _ = enabled, limit
            return [v for (kind, _), v in self.sources.items() if kind == source_type]

        async def set_source_enabled(self, *, source_type: str, source_id: str, enabled: bool):  # noqa: ANN001
            src = self.sources.get((source_type, source_id))
            if src is None:
                return False
            src.enabled = enabled
            return True

    class _DatasetRegistry:
        async def get_dataset(self, *, dataset_id: str):  # noqa: ANN001
            _ = dataset_id
            return None

        async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return None

    async def _fake_start_pipelining_google_sheet(**kwargs: Any):  # noqa: ANN401
        _ = kwargs
        return {"status": "success", "data": {"dataset": {"dataset_id": "dataset-1"}}}

    class _PipelineRegistry:
        def __init__(self) -> None:
            self.pipelines: dict[str, Any] = {}

        async def get_pipeline(self, pipeline_id: str):  # noqa: ANN001
            return self.pipelines.get(pipeline_id)

        async def create_pipeline(self, **kwargs: Any):  # noqa: ANN401
            pipeline = SimpleNamespace(
                pipeline_id=kwargs["pipeline_id"],
                branch=kwargs.get("branch", "main"),
            )
            self.pipelines[kwargs["pipeline_id"]] = pipeline
            return pipeline

        async def get_run(self, *, pipeline_id: str, job_id: str):  # noqa: ANN001
            return runs.get((pipeline_id, job_id))

        async def record_run(self, **kwargs: Any):  # noqa: ANN401
            runs[(kwargs["pipeline_id"], kwargs["job_id"])] = {
                "pipeline_id": kwargs["pipeline_id"],
                "job_id": kwargs["job_id"],
                "status": kwargs["status"],
                "mode": kwargs["mode"],
                "node_id": kwargs.get("node_id"),
                "row_count": kwargs.get("row_count"),
                "sample_json": kwargs.get("sample_json") or {},
                "output_json": kwargs.get("output_json") or {},
                "pipeline_spec_commit_id": kwargs.get("pipeline_spec_commit_id"),
                "pipeline_spec_hash": kwargs.get("pipeline_spec_hash"),
                "input_lakefs_commits": kwargs.get("input_lakefs_commits"),
                "output_lakefs_commit_id": kwargs.get("output_lakefs_commit_id"),
                "spark_conf": kwargs.get("spark_conf"),
                "code_version": kwargs.get("code_version"),
                "started_at": kwargs.get("started_at"),
                "finished_at": kwargs.get("finished_at"),
            }

    app = _build_test_app()
    pipeline_registry = _PipelineRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: _DatasetRegistry()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()
    app.dependency_overrides[get_connector_pipeline_registry] = lambda: pipeline_registry
    app.dependency_overrides[get_pipeline_registry] = lambda: pipeline_registry
    app.dependency_overrides[get_objectify_registry] = lambda: object()
    app.dependency_overrides[get_objectify_job_queue] = lambda: object()
    app.dependency_overrides[get_lineage_store] = lambda: object()
    monkeypatch.setattr(
        foundry_connectivity_v2.data_connector_pipelining_service,
        "start_pipelining_google_sheet",
        _fake_start_pipelining_google_sheet,
    )

    base = "/api/v2/connectivity/connections/ri.spice.main.connection.conn-1/tableImports"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        get_resp = await client.get(f"{base}/ri.spice.main.table-import.sheet-123")
        list_resp = await client.get(base)
        execute_resp = await client.post(f"{base}/ri.spice.main.table-import.sheet-123/execute")
        build_rid = execute_resp.json() if execute_resp.status_code == 200 else ""
        orchestration_get_resp = await client.get(f"/api/v2/orchestration/builds/{build_rid}")
        delete_resp = await client.delete(f"{base}/ri.spice.main.table-import.sheet-123")

    assert get_resp.status_code == 200
    assert get_resp.json()["rid"] == "ri.spice.main.table-import.sheet-123"

    assert list_resp.status_code == 200
    assert len(list_resp.json()["data"]) >= 1

    assert execute_resp.status_code == 200
    assert execute_resp.json().startswith("ri.spice.main.build.build-")
    assert orchestration_get_resp.status_code == 200
    assert orchestration_get_resp.json()["status"] == "SUCCEEDED"

    assert delete_resp.status_code == 204
