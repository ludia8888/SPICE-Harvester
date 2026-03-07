import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI, Request
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
from shared.models.background_task import BackgroundTask, TaskResult, TaskStatus
from shared.dependencies.providers import (
    get_audit_log_store,
    get_background_task_manager,
    get_lineage_store,
)
from shared.foundry.errors import FoundryAPIError, foundry_exception_handler
from shared.security.user_context import UserPrincipal


def _param_names(schema: dict, *, path: str, method: str) -> list[str]:
    path_item = schema.get("paths", {}).get(path, {}).get(method, {})
    return [param.get("name") for param in path_item.get("parameters", [])]


def _build_test_app() -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(FoundryAPIError, foundry_exception_handler)

    @app.middleware("http")
    async def _inject_test_principal(request: Request, call_next):  # noqa: ANN001
        request.state.user = UserPrincipal(
            id="test-user",
            claims={
                "scope": " ".join(
                    [
                        "api:orchestration-read",
                        "api:orchestration-write",
                        "api:connectivity-read",
                        "api:connectivity-write",
                        "api:datasets-read",
                        "api:datasets-write",
                        "api:ontologies-read",
                        "api:ontologies-write",
                    ]
                )
            },
        )
        return await call_next(request)

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
        "/api/v2/connectivity/connections/{connectionRid}/getConfiguration",
        "/api/v2/connectivity/connections/getConfigurationBatch",
        "/api/v2/connectivity/connections/{connectionRid}/updateSecrets",
        "/api/v2/connectivity/connections/{connectionRid}/updateExportSettings",
        "/api/v2/connectivity/connections/{connectionRid}/exportRuns",
        "/api/v2/connectivity/connections/{connectionRid}/exportRuns/{exportRunRid}",
        "/api/v2/connectivity/connections/{connectionRid}/uploadCustomJdbcDrivers",
        "/api/v2/connectivity/connections/{connectionRid}/tableImports",
        "/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}",
        "/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}/execute",
        "/api/v2/connectivity/connections/{connectionRid}/fileImports",
        "/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}",
        "/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}/execute",
        "/api/v2/connectivity/connections/{connectionRid}/virtualTables",
        "/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}",
        "/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}/execute",
    }
    for path in expected_paths:
        assert path in schema.get("paths", {})

    build_params = _param_names(
        schema,
        path="/api/v2/orchestration/builds/create",
        method="post",
    )
    assert build_params == []

    create_connection_params = _param_names(
        schema,
        path="/api/v2/connectivity/connections",
        method="post",
    )
    assert "preview" in create_connection_params

    table_import_list_params = _param_names(
        schema,
        path="/api/v2/connectivity/connections/{connectionRid}/tableImports",
        method="get",
    )
    assert "preview" in table_import_list_params

    config_batch_params = _param_names(
        schema,
        path="/api/v2/connectivity/connections/getConfigurationBatch",
        method="post",
    )
    assert "preview" in config_batch_params


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
                "target": {"targetRids": [f"ri.foundry.main.pipeline.{pipeline_id}"]},
                "branchName": "main",
            },
            headers={"X-User-ID": "user-123"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["rid"] == "ri.foundry.main.build.build-test-1"
    assert payload["branchName"] == "main"
    assert payload["createdBy"] == "user-123"
    assert payload["jobRids"] == ["ri.foundry.main.job.build-test-1"]
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

    build_rid = f"ri.foundry.main.build.{job_id}"
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
    assert jobs_payload["data"][0]["outputs"][0]["datasetRid"] == "ri.foundry.main.dataset.dataset-1"

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
                enabled=True,
                status="confirmed",
                field_mappings=[],
            )

        async def upsert_mapping(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return SimpleNamespace(
                target_db_name="sales_db",
                target_branch="main",
                target_class_label="Order",
                enabled=True,
                status="confirmed",
                field_mappings=[],
            )

        async def list_sources(self, *, source_type: str, enabled: bool, limit: int):  # noqa: ANN001
            _ = enabled, limit
            return [v for (kind, _), v in self.sources.items() if kind == source_type]

        async def delete_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.pop((source_type, source_id), None) is not None

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
            params={"preview": "true"},
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
    assert payload["rid"] == "ri.foundry.main.table-import.sheet-123"
    assert payload["connectionRid"] == "ri.foundry.main.connection.conn-1"
    assert payload["datasetRid"] == "ri.foundry.main.dataset.dataset-1"
    assert payload["importMode"] == "SNAPSHOT"
    assert payload["allowSchemaChanges"] is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connectivity_table_import_create_accepts_streaming_mode_when_cdc_enabled(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-stream"): SimpleNamespace(
                    source_id="conn-stream",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "PG", "type": "PostgreSqlConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                )
            }
            self.mappings: dict[tuple[str, str], Any] = {}

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict):  # noqa: ANN001
            source = SimpleNamespace(
                source_id=source_id,
                source_type=source_type,
                enabled=enabled,
                config_json=dict(config_json),
                created_at=None,
                updated_at=None,
            )
            self.sources[(source_type, source_id)] = source
            return source

        async def upsert_mapping(self, **kwargs: Any):  # noqa: ANN401
            mapping = SimpleNamespace(
                target_db_name=kwargs.get("target_db_name"),
                target_branch=kwargs.get("target_branch"),
                target_class_label=kwargs.get("target_class_label"),
                enabled=kwargs.get("enabled"),
                status=kwargs.get("status"),
                field_mappings=[],
            )
            key = (str(kwargs.get("source_type") or ""), str(kwargs.get("source_id") or ""))
            self.mappings[key] = mapping
            return mapping

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.mappings.get((source_type, source_id))

        async def list_sources(self, *, source_type: str, enabled: bool, limit: int):  # noqa: ANN001
            _ = enabled, limit
            return [v for (kind, _), v in self.sources.items() if kind == source_type]

    class _DatasetRegistry:
        async def get_dataset(self, *, dataset_id: str):  # noqa: ANN001
            _ = dataset_id
            return None

        async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return None

    app = _build_test_app()
    registry = _ConnectorRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_connector_dataset_registry] = lambda: _DatasetRegistry()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()
    app.dependency_overrides[get_lineage_store] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-stream/tableImports",
            params={"preview": "true"},
            json={
                "displayName": "orders-stream",
                "importMode": "STREAMING",
                "destination": {"ontology": "sales_db", "branchName": "main", "objectType": "Order"},
                "config": {
                    "type": "postgreSqlImportConfig",
                    "query": "SELECT * FROM orders",
                    "watermarkColumn": "updated_at",
                },
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["importMode"] == "STREAMING"
    table_import_id = payload["rid"].split(".")[-1]
    source = registry.sources.get(("postgresql_table_import", table_import_id))
    assert source is not None
    assert source.config_json.get("import_mode") == "STREAMING"


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

        async def delete_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.pop((source_type, source_id), None) is not None

    class _DatasetRegistry:
        async def get_dataset(self, *, dataset_id: str):  # noqa: ANN001
            _ = dataset_id
            return None

        async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return None

    async def _fake_start_pipelining_table_import(**kwargs: Any):  # noqa: ANN401
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
    registry = _ConnectorRegistry()
    pipeline_registry = _PipelineRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_connector_dataset_registry] = lambda: _DatasetRegistry()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()
    app.dependency_overrides[get_connector_pipeline_registry] = lambda: pipeline_registry
    app.dependency_overrides[get_pipeline_registry] = lambda: pipeline_registry
    app.dependency_overrides[get_objectify_registry] = lambda: object()
    app.dependency_overrides[get_objectify_job_queue] = lambda: object()
    app.dependency_overrides[get_lineage_store] = lambda: object()
    monkeypatch.setattr(
        foundry_connectivity_v2.data_connector_pipelining_service,
        "start_pipelining_table_import",
        _fake_start_pipelining_table_import,
    )

    base = "/api/v2/connectivity/connections/ri.spice.main.connection.conn-1/tableImports"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        preview = {"preview": "true"}
        get_resp = await client.get(f"{base}/ri.spice.main.table-import.sheet-123", params=preview)
        list_resp = await client.get(base, params=preview)
        execute_resp = await client.post(f"{base}/ri.spice.main.table-import.sheet-123/execute", params=preview)
        build_rid = execute_resp.json() if execute_resp.status_code == 200 else ""
        orchestration_get_resp = await client.get(f"/api/v2/orchestration/builds/{build_rid}")
        delete_resp = await client.delete(f"{base}/ri.spice.main.table-import.sheet-123", params=preview)

    assert get_resp.status_code == 200
    assert get_resp.json()["rid"] == "ri.foundry.main.table-import.sheet-123"

    assert list_resp.status_code == 200
    assert len(list_resp.json()["data"]) >= 1

    assert execute_resp.status_code == 200
    assert execute_resp.json().startswith("ri.foundry.main.build.build-")
    assert orchestration_get_resp.status_code == 200
    assert orchestration_get_resp.json()["status"] == "SUCCEEDED"

    assert delete_resp.status_code == 204
    assert ("google_sheets", "sheet-123") not in registry.sources


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connectivity_export_runs_create_get_and_list():
    now = datetime.now(timezone.utc)

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("google_sheets_connection", "conn-1"): SimpleNamespace(
                    source_id="conn-1",
                    source_type="google_sheets_connection",
                    enabled=True,
                    config_json={
                        "display_name": "Sheets Conn",
                        "export_settings": {"exportsEnabled": True, "exportEnabledWithoutMarkingsValidation": False},
                    },
                    created_at=now,
                    updated_at=now,
                )
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def list_sources(self, *, source_type: str, enabled: bool, limit: int):  # noqa: ANN001
            _ = enabled, limit
            return [value for (kind, _), value in self.sources.items() if kind == source_type]

    class _TaskManager:
        def __init__(self) -> None:
            self.tasks: dict[str, BackgroundTask] = {}

        async def create_task(self, func, *args, **kwargs):  # noqa: ANN001, ANN003
            task_id = str(kwargs.pop("task_id"))
            task_name = str(kwargs.pop("task_name", "connectivity export"))
            task_type = str(kwargs.pop("task_type", "connectivity_export_run"))
            metadata = kwargs.pop("metadata", {}) or {}
            _ = kwargs.pop("priority", None)
            kwargs.setdefault("task_id", task_id)
            result = await func(*args, **kwargs)
            task = BackgroundTask(
                task_id=task_id,
                task_name=task_name,
                task_type=task_type,
                status=TaskStatus.COMPLETED,
                created_at=now,
                started_at=now,
                completed_at=now,
                metadata=dict(metadata),
                result=TaskResult(success=True, data=result, message="ok"),
            )
            self.tasks[task_id] = task
            return task_id

        async def get_task_status(self, task_id: str):
            return self.tasks.get(task_id)

        async def get_all_tasks(self, status=None, task_type=None, limit: int = 100):  # noqa: ANN001, ANN003
            _ = status, limit
            if not task_type:
                return list(self.tasks.values())
            return [task for task in self.tasks.values() if task.task_type == task_type]

    class _AuditStore:
        async def log(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return uuid.uuid4()

    app = _build_test_app()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    task_manager = _TaskManager()
    app.dependency_overrides[get_background_task_manager] = lambda: task_manager
    app.dependency_overrides[get_audit_log_store] = lambda: _AuditStore()

    base = "/api/v2/connectivity/connections/ri.spice.main.connection.conn-1/exportRuns"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post(
            base,
            params={"preview": "true"},
            json={
                "targetUrl": "https://example.com/webhook",
                "method": "POST",
                "dryRun": True,
                "payload": {"hello": "world"},
            },
        )

        assert create_resp.status_code == 202
        created_payload = create_resp.json()
        run_rid = str(created_payload["rid"])
        assert run_rid.startswith("ri.foundry.main.export-run.")
        assert created_payload["connectionRid"] == "ri.foundry.main.connection.conn-1"
        assert created_payload["status"] in {"SUCCEEDED", "QUEUED", "RUNNING"}
        assert created_payload.get("writebackStatus") in {"SKIPPED", "SUCCEEDED"}

        get_resp = await client.get(
            f"{base}/{run_rid}",
            params={"preview": "true"},
        )
        assert get_resp.status_code == 200
        get_payload = get_resp.json()
        assert get_payload["rid"] == run_rid
        assert get_payload["status"] == "SUCCEEDED"
        assert get_payload.get("auditLogId")

        list_resp = await client.get(base, params={"preview": "true", "pageSize": "50"})
        assert list_resp.status_code == 200
        list_payload = list_resp.json()
        assert isinstance(list_payload.get("data"), list)
        assert any(item.get("rid") == run_rid for item in list_payload["data"])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connectivity_export_run_requires_markings_when_enforced():
    now = datetime.now(timezone.utc)

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("google_sheets_connection", "conn-guard"): SimpleNamespace(
                    source_id="conn-guard",
                    source_type="google_sheets_connection",
                    enabled=True,
                    config_json={
                        "display_name": "Sheets Conn",
                        "export_settings": {
                            "exportsEnabled": True,
                            "exportEnabledWithoutMarkingsValidation": False,
                        },
                    },
                    created_at=now,
                    updated_at=now,
                )
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

    class _TaskManager:
        async def create_task(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            raise AssertionError("task manager must not be invoked when markings guard blocks request")

        async def get_task_status(self, _task_id: str):
            return None

    class _AuditStore:
        async def log(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return uuid.uuid4()

    app = _build_test_app()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_background_task_manager] = lambda: _TaskManager()
    app.dependency_overrides[get_audit_log_store] = lambda: _AuditStore()

    base = "/api/v2/connectivity/connections/ri.spice.main.connection.conn-guard/exportRuns"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        blocked_resp = await client.post(
            base,
            params={"preview": "true"},
            json={
                "targetUrl": "https://example.com/webhook",
                "method": "POST",
                "dryRun": False,
                "payload": {"hello": "world"},
            },
        )

    assert blocked_resp.status_code == 409
    blocked_payload = blocked_resp.json()
    assert blocked_payload["errorName"] == "ExportMarkingsRequired"


# ---------------------------------------------------------------------------
# Foundry Orchestration Schedules API v2 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_foundry_schedule_paths_exist_in_openapi():
    app = _build_test_app()
    schema = app.openapi()
    expected_schedule_paths = {
        "/api/v2/orchestration/schedules",
        "/api/v2/orchestration/schedules/{scheduleRid}",
        "/api/v2/orchestration/schedules/{scheduleRid}/pause",
        "/api/v2/orchestration/schedules/{scheduleRid}/unpause",
        "/api/v2/orchestration/schedules/{scheduleRid}/runs",
    }
    for path in expected_schedule_paths:
        assert path in schema.get("paths", {}), f"Missing path: {path}"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_schedule_create_get_pause_unpause_delete():
    pipeline_id = str(uuid.uuid4())
    schedule_updates: dict[str, Any] = {}

    class _PipelineRegistry:
        def __init__(self) -> None:
            self._schedule_cron: str | None = None
            self._schedule_interval: int | None = None
            self._status: str = "active"

        async def get_pipeline(self, pipeline_id: str):  # noqa: ANN001
            return SimpleNamespace(
                pipeline_id=pipeline_id,
                branch="main",
                status=self._status,
                schedule_cron=self._schedule_cron,
                schedule_interval_seconds=self._schedule_interval,
                created_at=None,
            )

        async def update_pipeline(self, *, pipeline_id: str, **kwargs: Any):  # noqa: ANN401
            schedule_updates.update(kwargs)
            if "schedule_cron" in kwargs:
                self._schedule_cron = kwargs["schedule_cron"] or None
            if "schedule_interval_seconds" in kwargs:
                val = kwargs["schedule_interval_seconds"]
                self._schedule_interval = val if val else None
            if "status" in kwargs:
                self._status = kwargs["status"]

        async def list_runs(self, *, pipeline_id: str, limit: int = 25):  # noqa: ANN001
            return [
                {
                    "job_id": f"build-{pipeline_id}-run1",
                    "status": "SUCCESS",
                    "started_at": None,
                    "finished_at": None,
                }
            ]

    app = _build_test_app()
    registry = _PipelineRegistry()
    app.dependency_overrides[get_pipeline_registry] = lambda: registry

    schedule_rid = f"ri.foundry.main.schedule.{pipeline_id}"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create schedule
        create_resp = await client.post(
            "/api/v2/orchestration/schedules",
            json={
                "targetRid": f"ri.foundry.main.pipeline.{pipeline_id}",
                "trigger": {"cronExpression": "0 0 * * *"},
            },
        )
        assert create_resp.status_code == 200
        body = create_resp.json()
        assert body["rid"] == schedule_rid
        assert body["trigger"]["cronExpression"] == "0 0 * * *"
        assert body["status"] == "ACTIVE"

        # Get schedule
        get_resp = await client.get(f"/api/v2/orchestration/schedules/{schedule_rid}")
        assert get_resp.status_code == 200
        assert get_resp.json()["rid"] == schedule_rid

        # Pause schedule
        pause_resp = await client.post(f"/api/v2/orchestration/schedules/{schedule_rid}/pause")
        assert pause_resp.status_code == 204
        assert registry._status == "paused"

        # Unpause schedule
        unpause_resp = await client.post(f"/api/v2/orchestration/schedules/{schedule_rid}/unpause")
        assert unpause_resp.status_code == 204
        assert registry._status == "active"

        # List runs
        runs_resp = await client.get(f"/api/v2/orchestration/schedules/{schedule_rid}/runs")
        assert runs_resp.status_code == 200
        runs_body = runs_resp.json()
        assert len(runs_body["data"]) == 1
        assert runs_body["data"][0]["status"] == "SUCCEEDED"
        assert runs_body["data"][0]["startedTime"] is None

        # Delete schedule
        delete_resp = await client.delete(f"/api/v2/orchestration/schedules/{schedule_rid}")
        assert delete_resp.status_code == 204


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_schedule_not_found_for_missing_pipeline():
    class _PipelineRegistry:
        async def get_pipeline(self, pipeline_id: str):  # noqa: ANN001
            return None

    app = _build_test_app()
    app.dependency_overrides[get_pipeline_registry] = lambda: _PipelineRegistry()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/api/v2/orchestration/schedules/ri.spice.main.schedule.{uuid.uuid4()}")
    assert resp.status_code == 404
    assert resp.json()["errorCode"] == "NOT_FOUND"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_schedule_runs_pagination_and_invalid_token():
    pipeline_id = str(uuid.uuid4())
    schedule_rid = f"ri.spice.main.schedule.{pipeline_id}"

    class _PipelineRegistry:
        async def get_pipeline(self, pipeline_id: str):  # noqa: ANN001
            return SimpleNamespace(
                pipeline_id=pipeline_id,
                branch="main",
                status="active",
                schedule_cron="0 0 * * *",
                schedule_interval_seconds=None,
                created_at=None,
            )

        async def list_runs(self, *, pipeline_id: str, limit: int = 25):  # noqa: ANN001
            data = [
                {"job_id": f"build-{pipeline_id}-run1", "status": "SUCCESS"},
                {"job_id": f"build-{pipeline_id}-run2", "status": "RUNNING"},
                {"job_id": f"build-{pipeline_id}-run3", "status": "FAILED"},
            ]
            return data[:limit]

    app = _build_test_app()
    app.dependency_overrides[get_pipeline_registry] = lambda: _PipelineRegistry()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        page1 = await client.get(
            f"/api/v2/orchestration/schedules/{schedule_rid}/runs",
            params={"pageSize": 1},
        )
        assert page1.status_code == 200
        body1 = page1.json()
        assert len(body1["data"]) == 1
        assert body1["nextPageToken"] == "1"

        page2 = await client.get(
            f"/api/v2/orchestration/schedules/{schedule_rid}/runs",
            params={"pageSize": 1, "pageToken": body1["nextPageToken"]},
        )
        assert page2.status_code == 200
        body2 = page2.json()
        assert len(body2["data"]) == 1
        assert body2["data"][0]["status"] == "RUNNING"
        assert body2["nextPageToken"] == "2"

        invalid = await client.get(
            f"/api/v2/orchestration/schedules/{schedule_rid}/runs",
            params={"pageToken": "not-a-number"},
        )
        assert invalid.status_code == 400
        assert invalid.json()["errorCode"] == "INVALID_ARGUMENT"


# ---------------------------------------------------------------------------
# Foundry Connectivity Connection CRUD tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_foundry_connection_crud_paths_exist_in_openapi():
    app = _build_test_app()
    schema = app.openapi()
    expected_connection_paths = {
        "/api/v2/connectivity/connections",
        "/api/v2/connectivity/connections/{connectionRid}",
        "/api/v2/connectivity/connections/{connectionRid}/test",
        "/api/v2/connectivity/connections/{connectionRid}/getConfiguration",
        "/api/v2/connectivity/connections/getConfigurationBatch",
        "/api/v2/connectivity/connections/{connectionRid}/updateSecrets",
        "/api/v2/connectivity/connections/{connectionRid}/updateExportSettings",
        "/api/v2/connectivity/connections/{connectionRid}/uploadCustomJdbcDrivers",
        "/api/v2/connectivity/connections/{connectionRid}/fileImports",
        "/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}",
        "/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}/execute",
        "/api/v2/connectivity/connections/{connectionRid}/virtualTables",
        "/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}",
        "/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}/execute",
    }
    for path in expected_connection_paths:
        assert path in schema.get("paths", {}), f"Missing path: {path}"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connection_create_get_list_delete():
    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {}

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict):  # noqa: ANN001
            from shared.utils.time_utils import utcnow
            self.sources[(source_type, source_id)] = SimpleNamespace(
                source_id=source_id,
                source_type=source_type,
                enabled=enabled,
                config_json=dict(config_json),
                created_at=utcnow(),
                updated_at=utcnow(),
            )
            return self.sources[(source_type, source_id)]

        async def list_sources(self, *, source_type: str, enabled: bool, limit: int):  # noqa: ANN001
            return [
                v for (kind, _), v in self.sources.items()
                if kind == source_type and (enabled is None or v.enabled == enabled)
            ]

        async def delete_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.pop((source_type, source_id), None) is not None

    app = _build_test_app()
    registry = _ConnectorRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create connection
        create_resp = await client.post(
            "/api/v2/connectivity/connections",
            params={"preview": "true"},
            json={
                "displayName": "My Google Sheets Connection",
                "connectionConfiguration": {
                    "type": "GoogleSheetsConnectionConfig",
                    "accountEmail": "test@example.com",
                },
                "parentFolderRid": "ri.compass.main.folder.connectivity",
                "markings": ["qa.internal", "pii.none"],
                "exportSettings": {
                    "exportsEnabled": True,
                    "exportEnabledWithoutMarkingsValidation": False,
                },
            },
        )
        assert create_resp.status_code == 201
        conn_body = create_resp.json()
        assert conn_body["displayName"] == "My Google Sheets Connection"
        assert conn_body["rid"].startswith("ri.foundry.main.connection.")
        assert conn_body["status"] == "CONNECTED"
        assert conn_body["parentFolderRid"] == "ri.compass.main.folder.connectivity"
        assert conn_body["markings"] == ["qa.internal", "pii.none"]
        assert conn_body["exportSettings"]["exportsEnabled"] is True
        assert conn_body["exportSettings"]["exportEnabledWithoutMarkingsValidation"] is False
        assert conn_body["connectionConfiguration"]["type"] == "GoogleSheetsConnectionConfig"
        assert conn_body["connectionConfiguration"]["accountEmail"] == "test@example.com"
        # Backward-compatible alias.
        assert conn_body["configuration"]["type"] == "GoogleSheetsConnectionConfig"
        assert conn_body["configuration"]["accountEmail"] == "test@example.com"

        connection_rid = conn_body["rid"]

        # Get connection
        get_resp = await client.get(f"/api/v2/connectivity/connections/{connection_rid}", params={"preview": "true"})
        assert get_resp.status_code == 200
        assert get_resp.json()["rid"] == connection_rid
        assert get_resp.json()["displayName"] == "My Google Sheets Connection"
        assert get_resp.json()["markings"] == ["qa.internal", "pii.none"]
        assert get_resp.json()["connectionConfiguration"]["accountEmail"] == "test@example.com"
        assert get_resp.json()["configuration"]["accountEmail"] == "test@example.com"

        # List connections
        list_resp = await client.get("/api/v2/connectivity/connections", params={"preview": "true"})
        assert list_resp.status_code == 200
        list_body = list_resp.json()
        assert len(list_body["data"]) == 1
        assert list_body["data"][0]["rid"] == connection_rid
        assert list_body["data"][0]["markings"] == ["qa.internal", "pii.none"]
        assert list_body["data"][0]["connectionConfiguration"]["accountEmail"] == "test@example.com"
        assert list_body["data"][0]["configuration"]["accountEmail"] == "test@example.com"

        # Delete connection
        delete_resp = await client.delete(f"/api/v2/connectivity/connections/{connection_rid}", params={"preview": "true"})
        assert delete_resp.status_code == 204

        # Verify deleted — listing should be empty now (disabled)
        list_resp2 = await client.get("/api/v2/connectivity/connections", params={"preview": "true"})
        assert list_resp2.status_code == 200
        assert len(list_resp2.json()["data"]) == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connection_get_not_found():
    class _ConnectorRegistry:
        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return None

    app = _build_test_app()
    registry = _ConnectorRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/v2/connectivity/connections/ri.spice.main.connection.nonexistent",
            params={"preview": "true"},
        )
    assert resp.status_code == 404
    assert resp.json()["errorCode"] == "NOT_FOUND"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connection_test_endpoint():
    class _ConnectorRegistry:
        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            if source_type == "google_sheets_connection" and source_id == "conn-test":
                return SimpleNamespace(
                    source_id="conn-test",
                    source_type="google_sheets_connection",
                    enabled=True,
                    config_json={"display_name": "Test Conn", "status": "CONNECTED"},
                    created_at=None,
                    updated_at=None,
                )
            return None

        async def upsert_source(self, **kwargs: Any):  # noqa: ANN401
            pass

        async def get_connection_secrets(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return {}

    app = _build_test_app()
    registry = _ConnectorRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-test/test",
            params={"preview": "true"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["connectionRid"] == "ri.foundry.main.connection.conn-test"
    # No credentials, so should get WARNING
    assert body["status"] == "WARNING"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connection_update_secrets_keeps_response_non_secret():
    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.source = SimpleNamespace(
                source_id="conn-secrets",
                source_type="postgresql_connection",
                enabled=True,
                config_json={
                    "display_name": "PG Conn",
                    "type": "PostgreSqlConnectionConfig",
                    "host": "localhost",
                    "database": "demo",
                    "username": "demo_user",
                },
                created_at=None,
                updated_at=None,
            )
            self.secrets: dict[str, Any] = {}

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            if source_type == "postgresql_connection" and source_id == "conn-secrets":
                return self.source
            return None

        async def get_connection_secrets(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return dict(self.secrets)

        async def upsert_connection_secrets(self, *, source_type: str, source_id: str, secrets_json: dict):  # noqa: ANN001
            _ = source_type, source_id
            self.secrets.update(dict(secrets_json))
            return dict(self.secrets)

    app = _build_test_app()
    registry = _ConnectorRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        update_resp = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-secrets/updateSecrets",
            params={"preview": "true"},
            json={"secrets": {"password": "secret-pass"}},
        )
        get_cfg_resp = await client.get(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-secrets/getConfiguration",
            params={"preview": "true"},
        )

    assert update_resp.status_code == 204
    assert get_cfg_resp.status_code == 200
    assert get_cfg_resp.json()["connectionConfiguration"]["type"] == "PostgreSqlConnectionConfig"
    cfg = get_cfg_resp.json()["configuration"]
    assert cfg["type"] == "PostgreSqlConnectionConfig"
    assert "password" not in cfg


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connection_update_export_settings_v2():
    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.source = SimpleNamespace(
                source_id="conn-export",
                source_type="postgresql_connection",
                enabled=True,
                config_json={
                    "display_name": "PG Conn",
                    "type": "PostgreSqlConnectionConfig",
                    "host": "localhost",
                    "database": "demo",
                },
                created_at=None,
                updated_at=None,
            )

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            if source_type == "postgresql_connection" and source_id == "conn-export":
                return self.source
            return None

        async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict):  # noqa: ANN001
            _ = source_type, source_id, enabled
            self.source.config_json = dict(config_json)
            return self.source

    app = _build_test_app()
    registry = _ConnectorRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        no_preview_resp = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-export/updateExportSettings",
            json={"exportSettings": {"exportsEnabled": True}},
        )
        update_resp = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-export/updateExportSettings",
            params={"preview": "true"},
            json={"exportSettings": {"exportsEnabled": False, "exportEnabledWithoutMarkingsValidation": True}},
        )

    assert no_preview_resp.status_code == 400
    assert no_preview_resp.json()["errorName"] == "ApiFeaturePreviewUsageOnly"
    assert update_resp.status_code == 204
    assert registry.source.config_json["export_settings"]["exportsEnabled"] is False
    assert registry.source.config_json["export_settings"]["exportEnabledWithoutMarkingsValidation"] is True

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        legacy_shape_resp = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-export/updateExportSettings",
            params={"preview": "true"},
            json={"exportSettings": {"sharing": {"scope": "DISABLED"}}},
        )

    assert legacy_shape_resp.status_code == 204
    assert registry.source.config_json["export_settings"]["exportsEnabled"] is True
    assert registry.source.config_json["export_settings"]["exportEnabledWithoutMarkingsValidation"] is False
    assert "sharing" not in registry.source.config_json["export_settings"]
    assert set(registry.source.config_json["export_settings"].keys()) == {
        "exportsEnabled",
        "exportEnabledWithoutMarkingsValidation",
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_connection_upload_custom_jdbc_drivers_v2():
    class _Storage:
        def __init__(self) -> None:
            self.saved: list[dict[str, Any]] = []

        async def save_bytes(
            self,
            bucket: str,
            key: str,
            data: bytes,
            content_type: str = "application/octet-stream",
            metadata: dict[str, str] | None = None,
        ) -> str:
            self.saved.append(
                {
                    "bucket": bucket,
                    "key": key,
                    "data": bytes(data),
                    "content_type": content_type,
                    "metadata": dict(metadata or {}),
                }
            )
            return "checksum"

    class _PipelineRegistry:
        def __init__(self, storage: _Storage) -> None:
            self._storage = storage

        async def get_lakefs_storage(self, *, user_id: str | None = None):  # noqa: ANN001
            _ = user_id
            return self._storage

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.source = SimpleNamespace(
                source_id="conn-jdbc",
                source_type="postgresql_connection",
                enabled=True,
                config_json={
                    "display_name": "PG Conn",
                    "type": "PostgreSqlConnectionConfig",
                    "host": "localhost",
                    "database": "demo",
                    "username": "demo_user",
                },
                created_at=None,
                updated_at=None,
            )

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            if source_type == "postgresql_connection" and source_id == "conn-jdbc":
                return self.source
            return None

        async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict):  # noqa: ANN001
            _ = source_type, source_id, enabled
            self.source.config_json = dict(config_json)
            return self.source

    app = _build_test_app()
    registry = _ConnectorRegistry()
    storage = _Storage()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_connector_pipeline_registry] = lambda: _PipelineRegistry(storage)
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    content = b"fake-jar-driver-binary"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        invalid_file_name_resp = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-jdbc/uploadCustomJdbcDrivers",
            params={"preview": "true", "fileName": "custom-driver.txt"},
            content=content,
            headers={"Content-Type": "application/octet-stream"},
        )
        upload_resp = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-jdbc/uploadCustomJdbcDrivers",
            params={"preview": "true", "fileName": "custom-driver.jar"},
            content=content,
            headers={"Content-Type": "application/octet-stream"},
        )

    assert invalid_file_name_resp.status_code == 400
    assert invalid_file_name_resp.json()["errorCode"] == "INVALID_ARGUMENT"
    assert upload_resp.status_code == 200
    body = upload_resp.json()
    assert body["rid"] == "ri.foundry.main.connection.conn-jdbc"
    conn_cfg = body["connectionConfiguration"]
    assert conn_cfg["type"] == "PostgreSqlConnectionConfig"
    assert "customJdbcDrivers" in conn_cfg
    assert conn_cfg["customJdbcDrivers"][0]["fileName"] == "custom-driver.jar"
    assert len(storage.saved) == 1
    assert storage.saved[0]["data"] == content
    assert storage.saved[0]["bucket"] == "pipeline-artifacts"
    assert "connectivity/jdbc-drivers/conn-jdbc/" in storage.saved[0]["key"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_file_import_requires_preview_and_supports_create(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-fi"): SimpleNamespace(
                    source_id="conn-fi",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "pg", "type": "PostgreSqlConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                )
            }
            self.mapping = None

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict):  # noqa: ANN001
            self.sources[(source_type, source_id)] = SimpleNamespace(
                source_id=source_id,
                source_type=source_type,
                enabled=enabled,
                config_json=dict(config_json),
                created_at=None,
                updated_at=None,
            )
            return self.sources[(source_type, source_id)]

        async def upsert_mapping(self, **kwargs: Any):  # noqa: ANN401
            self.mapping = SimpleNamespace(
                target_db_name=kwargs.get("target_db_name"),
                target_branch=kwargs.get("target_branch"),
                target_class_label=kwargs.get("target_class_label"),
                enabled=kwargs.get("enabled"),
                status=kwargs.get("status"),
                field_mappings=[],
            )
            return self.mapping

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return self.mapping

        async def list_sources(self, *, source_type: str, enabled: bool, limit: int):  # noqa: ANN001
            _ = enabled, limit
            return [v for (kind, _), v in self.sources.items() if kind == source_type]

    class _DatasetRegistry:
        async def get_dataset(self, *, dataset_id: str):  # noqa: ANN001
            _ = dataset_id
            return None

        async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return None

    app = _build_test_app()
    registry = _ConnectorRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_connector_dataset_registry] = lambda: _DatasetRegistry()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        no_preview = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-fi/fileImports",
            json={
                "displayName": "FI",
                "importMode": "SNAPSHOT",
                "destination": {"ontology": "demo", "branchName": "main", "objectType": "Order"},
                "config": {"type": "postgreSqlFileImportConfig", "query": "select 1 as id"},
            },
        )
        assert no_preview.status_code == 400
        assert no_preview.json()["errorName"] == "ApiFeaturePreviewUsageOnly"

        with_preview = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-fi/fileImports",
            params={"preview": "true"},
            json={
                "displayName": "FI",
                "importMode": "SNAPSHOT",
                "destination": {"ontology": "demo", "branchName": "main", "objectType": "Order"},
                "config": {"type": "postgreSqlFileImportConfig", "query": "select 1 as id"},
            },
        )
        assert with_preview.status_code == 200
        payload = with_preview.json()
        assert payload["name"] == "FI"
        assert payload["parentRid"] == "ri.foundry.main.connection.conn-fi"
        assert payload["connectionRid"] == "ri.foundry.main.connection.conn-fi"
        assert payload["rid"].startswith("ri.foundry.main.file-import.")
        file_import_id = payload["rid"].split(".")[-1]
        source = registry.sources.get(("postgresql_file_import", file_import_id))
        assert source is not None
        cfg = source.config_json or {}
        assert isinstance(cfg.get("file_import_config"), dict)
        assert "table_import_config" not in cfg


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_virtual_table_supports_foundry_name_and_parent_rid(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-vt"): SimpleNamespace(
                    source_id="conn-vt",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "pg", "type": "PostgreSqlConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                )
            }
            self.mapping = None

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict):  # noqa: ANN001
            self.sources[(source_type, source_id)] = SimpleNamespace(
                source_id=source_id,
                source_type=source_type,
                enabled=enabled,
                config_json=dict(config_json),
                created_at=None,
                updated_at=None,
            )
            return self.sources[(source_type, source_id)]

        async def upsert_mapping(self, **kwargs: Any):  # noqa: ANN401
            self.mapping = SimpleNamespace(
                target_db_name=kwargs.get("target_db_name"),
                target_branch=kwargs.get("target_branch"),
                target_class_label=kwargs.get("target_class_label"),
                enabled=kwargs.get("enabled"),
                status=kwargs.get("status"),
                field_mappings=[],
            )
            return self.mapping

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return self.mapping

        async def list_sources(self, *, source_type: str, enabled: bool, limit: int):  # noqa: ANN001
            _ = enabled, limit
            return [v for (kind, _), v in self.sources.items() if kind == source_type]

    class _DatasetRegistry:
        async def get_dataset(self, *, dataset_id: str):  # noqa: ANN001
            _ = dataset_id
            return None

        async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return None

    app = _build_test_app()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: _DatasetRegistry()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        no_preview = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-vt/virtualTables",
            json={
                "name": "VT-Orders",
                "parentRid": "ri.spice.main.connection.conn-vt",
                "destination": {"ontology": "demo", "branchName": "main", "objectType": "Order"},
                "config": {"type": "postgreSqlVirtualTableConfig", "query": "select * from orders"},
            },
        )
        assert no_preview.status_code == 400
        assert no_preview.json()["errorName"] == "ApiFeaturePreviewUsageOnly"

        with_preview = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-vt/virtualTables",
            params={"preview": "true"},
            json={
                "name": "VT-Orders",
                "parentRid": "ri.spice.main.connection.conn-vt",
                "destination": {"ontology": "demo", "branchName": "main", "objectType": "Order"},
                "config": {"type": "postgreSqlVirtualTableConfig", "query": "select * from orders"},
            },
        )
        assert with_preview.status_code == 200
        payload = with_preview.json()
        assert payload["name"] == "VT-Orders"
        assert payload["displayName"] == "VT-Orders"
        assert payload["parentRid"] == "ri.foundry.main.connection.conn-vt"
        assert payload["rid"].startswith("ri.foundry.main.virtual-table.")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_virtual_table_execute_records_build_run(
    monkeypatch: pytest.MonkeyPatch,
):
    runs: dict[tuple[str, str], dict[str, Any]] = {}

    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-vt-exec"): SimpleNamespace(
                    source_id="conn-vt-exec",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "pg", "type": "PostgreSqlConnectionConfig"},
                ),
                ("postgresql_virtual_table", "vt-1"): SimpleNamespace(
                    source_id="vt-1",
                    source_type="postgresql_virtual_table",
                    enabled=True,
                    config_json={
                        "connection_id": "conn-vt-exec",
                        "display_name": "Orders virtual table",
                        "virtual_table_config": {
                            "type": "postgreSqlVirtualTableConfig",
                            "query": "SELECT 1 AS id",
                        },
                    },
                ),
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return SimpleNamespace(
                target_db_name="sales_db",
                target_branch="main",
                target_class_label="Order",
            )

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
                "output_json": kwargs.get("output_json") or {},
                "started_at": kwargs.get("started_at"),
                "finished_at": kwargs.get("finished_at"),
            }

    async def _fake_start_pipelining_virtual_table(**kwargs: Any):  # noqa: ANN401
        _ = kwargs
        return {"status": "success", "data": {"dataset": {"dataset_id": "dataset-vt-1"}}}

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())
    monkeypatch.setattr(
        foundry_connectivity_v2.data_connector_pipelining_service,
        "start_pipelining_virtual_table",
        _fake_start_pipelining_virtual_table,
    )

    app = _build_test_app()
    pipeline_registry = _PipelineRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: object()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()
    app.dependency_overrides[get_connector_pipeline_registry] = lambda: pipeline_registry
    app.dependency_overrides[get_pipeline_registry] = lambda: pipeline_registry
    app.dependency_overrides[get_objectify_registry] = lambda: object()
    app.dependency_overrides[get_objectify_job_queue] = lambda: object()
    app.dependency_overrides[get_lineage_store] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        execute_resp = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-vt-exec/virtualTables/ri.spice.main.virtual-table.vt-1/execute",
            params={"preview": "true"},
        )
        assert execute_resp.status_code == 200
        build_rid = execute_resp.json()
        assert build_rid.startswith("ri.foundry.main.build.build-")

        orchestration_get_resp = await client.get(f"/api/v2/orchestration/builds/{build_rid}")
        assert orchestration_get_resp.status_code == 200
        assert orchestration_get_resp.json()["status"] == "SUCCEEDED"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_virtual_table_create_rejects_non_snapshot_mode(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-vt-invalid"): SimpleNamespace(
                    source_id="conn-vt-invalid",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "pg", "type": "PostgreSqlConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                )
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

    class _DatasetRegistry:
        async def get_dataset(self, *, dataset_id: str):  # noqa: ANN001
            _ = dataset_id
            return None

        async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return None

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())

    app = _build_test_app()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: _DatasetRegistry()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-vt-invalid/virtualTables",
            params={"preview": "true"},
            json={
                "name": "VT-invalid",
                "importMode": "INCREMENTAL",
                "destination": {"ontology": "demo", "branchName": "main", "objectType": "Order"},
                "config": {"type": "postgreSqlVirtualTableConfig", "query": "select * from orders"},
            },
        )

    assert response.status_code == 400
    payload = response.json()
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "VirtualTableInvalidConfig"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_file_import_create_rejects_missing_selector_for_google_sheets():
    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("google_sheets_connection", "conn-file-invalid"): SimpleNamespace(
                    source_id="conn-file-invalid",
                    source_type="google_sheets_connection",
                    enabled=True,
                    config_json={"display_name": "gs", "type": "GoogleSheetsConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                )
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

    class _DatasetRegistry:
        async def get_dataset(self, *, dataset_id: str):  # noqa: ANN001
            _ = dataset_id
            return None

        async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return None

    app = _build_test_app()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: _DatasetRegistry()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-file-invalid/fileImports",
            params={"preview": "true"},
            json={
                "displayName": "FI-invalid",
                "importMode": "SNAPSHOT",
                "destination": {"ontology": "demo", "branchName": "main", "objectType": "Order"},
                "config": {"type": "jdbcFileImportConfig"},
            },
        )

    assert response.status_code == 400
    payload = response.json()
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "FileImportInvalidConfig"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_file_import_execute_records_build_run(
    monkeypatch: pytest.MonkeyPatch,
):
    runs: dict[tuple[str, str], dict[str, Any]] = {}

    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-fi-exec"): SimpleNamespace(
                    source_id="conn-fi-exec",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "pg", "type": "PostgreSqlConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                ),
                ("postgresql_file_import", "fi-1"): SimpleNamespace(
                    source_id="fi-1",
                    source_type="postgresql_file_import",
                    enabled=True,
                    config_json={
                        "connection_id": "conn-fi-exec",
                        "display_name": "Orders file import",
                        "import_mode": "SNAPSHOT",
                        "file_import_config": {
                            "type": "postgreSqlFileImportConfig",
                            "query": "select 1 as id",
                        },
                    },
                    created_at=None,
                    updated_at=None,
                ),
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return SimpleNamespace(
                target_db_name="sales_db",
                target_branch="main",
                target_class_label="Order",
            )

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
                "output_json": kwargs.get("output_json") or {},
                "started_at": kwargs.get("started_at"),
                "finished_at": kwargs.get("finished_at"),
            }

    async def _fake_start_pipelining_file_import(**kwargs: Any):  # noqa: ANN401
        _ = kwargs
        return {"status": "success", "data": {"dataset": {"dataset_id": "dataset-fi-1"}}}

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())
    monkeypatch.setattr(
        foundry_connectivity_v2.data_connector_pipelining_service,
        "start_pipelining_file_import",
        _fake_start_pipelining_file_import,
    )

    app = _build_test_app()
    pipeline_registry = _PipelineRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: object()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()
    app.dependency_overrides[get_connector_pipeline_registry] = lambda: pipeline_registry
    app.dependency_overrides[get_pipeline_registry] = lambda: pipeline_registry
    app.dependency_overrides[get_objectify_registry] = lambda: object()
    app.dependency_overrides[get_objectify_job_queue] = lambda: object()
    app.dependency_overrides[get_lineage_store] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        execute_resp = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-fi-exec/fileImports/ri.spice.main.file-import.fi-1/execute",
            params={"preview": "true"},
        )
        assert execute_resp.status_code == 200
        build_rid = execute_resp.json()
        assert build_rid.startswith("ri.foundry.main.build.build-")

        orchestration_get_resp = await client.get(f"/api/v2/orchestration/builds/{build_rid}")
        assert orchestration_get_resp.status_code == 200
        assert orchestration_get_resp.json()["status"] == "SUCCEEDED"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_file_import_replace_preserves_shared_source_contract(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.mapping = SimpleNamespace(
                target_db_name="demo",
                target_branch="main",
                target_class_label=None,
                enabled=False,
                status="draft",
                field_mappings=[{"source": "id", "target": "id"}],
            )
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-fi-replace"): SimpleNamespace(
                    source_id="conn-fi-replace",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "pg", "type": "PostgreSqlConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                ),
                ("postgresql_file_import", "fi-replace"): SimpleNamespace(
                    source_id="fi-replace",
                    source_type="postgresql_file_import",
                    enabled=True,
                    config_json={
                        "connection_id": "conn-fi-replace",
                        "dataset_rid": "ri.spice.main.dataset.dataset-fi-replace",
                        "display_name": "Orders file import",
                        "name": "Orders file import",
                        "import_mode": "SNAPSHOT",
                        "allow_schema_changes": False,
                        "file_import_config": {
                            "type": "postgreSqlFileImportConfig",
                            "query": "select 1 as id",
                        },
                        "branch_name": "main",
                        "resource_kind": "file_import",
                    },
                    created_at=None,
                    updated_at=None,
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
                created_at=None,
                updated_at=None,
            )
            return self.sources[(source_type, source_id)]

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return self.mapping

        async def upsert_mapping(self, **kwargs: Any):  # noqa: ANN401
            self.mapping = SimpleNamespace(
                target_db_name=kwargs.get("target_db_name"),
                target_branch=kwargs.get("target_branch"),
                target_class_label=kwargs.get("target_class_label"),
                enabled=kwargs.get("enabled"),
                status=kwargs.get("status"),
                field_mappings=kwargs.get("field_mappings") or [],
            )
            return self.mapping

    class _DatasetRegistry:
        async def get_dataset(self, *, dataset_id: str):  # noqa: ANN001
            _ = dataset_id
            return None

        async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
            _ = kwargs
            return None

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())

    app = _build_test_app()
    registry = _ConnectorRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_connector_dataset_registry] = lambda: _DatasetRegistry()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.put(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-fi-replace/fileImports/ri.spice.main.file-import.fi-replace",
            params={"preview": "true"},
            json={
                "displayName": "Orders file import v2",
                "destination": {"ontology": "demo", "branchName": "feature-x", "objectType": "Order"},
                "config": {"type": "postgreSqlFileImportConfig", "query": "select 2 as id"},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["displayName"] == "Orders file import v2"
    assert payload["branchName"] == "feature-x"
    assert payload["allowSchemaChanges"] is False

    updated_source = registry.sources[("postgresql_file_import", "fi-replace")]
    assert updated_source.config_json["display_name"] == "Orders file import v2"
    assert updated_source.config_json["name"] == "Orders file import v2"
    assert updated_source.config_json["branch_name"] == "feature-x"
    assert updated_source.config_json["allow_schema_changes"] is False
    assert updated_source.config_json["resource_kind"] == "file_import"
    assert updated_source.config_json["file_import_config"]["query"] == "select 2 as id"

    assert registry.mapping.target_db_name == "demo"
    assert registry.mapping.target_branch == "feature-x"
    assert registry.mapping.target_class_label == "Order"
    assert registry.mapping.enabled is True
    assert registry.mapping.status == "confirmed"
    assert registry.mapping.field_mappings == [{"source": "id", "target": "id"}]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_file_import_execute_rejects_incomplete_mapping(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-fi-not-ready"): SimpleNamespace(
                    source_id="conn-fi-not-ready",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "pg", "type": "PostgreSqlConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                ),
                ("postgresql_file_import", "fi-not-ready"): SimpleNamespace(
                    source_id="fi-not-ready",
                    source_type="postgresql_file_import",
                    enabled=True,
                    config_json={
                        "connection_id": "conn-fi-not-ready",
                        "display_name": "Orders file import",
                        "import_mode": "SNAPSHOT",
                        "file_import_config": {
                            "type": "postgreSqlFileImportConfig",
                            "query": "select 1 as id",
                        },
                    },
                    created_at=None,
                    updated_at=None,
                ),
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return SimpleNamespace(
                target_db_name="",
                target_branch="main",
                target_class_label=None,
            )

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())

    app = _build_test_app()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: object()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()
    app.dependency_overrides[get_connector_pipeline_registry] = lambda: object()
    app.dependency_overrides[get_pipeline_registry] = lambda: object()
    app.dependency_overrides[get_objectify_registry] = lambda: object()
    app.dependency_overrides[get_objectify_job_queue] = lambda: object()
    app.dependency_overrides[get_lineage_store] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-fi-not-ready/fileImports/ri.spice.main.file-import.fi-not-ready/execute",
            params={"preview": "true"},
        )

    assert response.status_code == 409
    payload = response.json()
    assert payload["errorCode"] == "CONFLICT"
    assert payload["errorName"] == "FileImportNotReady"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_file_import_execute_rejects_invalid_runtime_config_before_pipeline(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-fi-exec-invalid"): SimpleNamespace(
                    source_id="conn-fi-exec-invalid",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "pg", "type": "PostgreSqlConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                ),
                ("postgresql_file_import", "fi-1"): SimpleNamespace(
                    source_id="fi-1",
                    source_type="postgresql_file_import",
                    enabled=True,
                    config_json={
                        "connection_id": "conn-fi-exec-invalid",
                        "display_name": "FI",
                        "import_mode": "SNAPSHOT",
                        "file_import_config": {"type": "postgreSqlFileImportConfig"},
                    },
                    created_at=None,
                    updated_at=None,
                ),
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return SimpleNamespace(
                target_db_name="sales_db",
                target_branch="main",
                target_class_label="Order",
            )

    async def _should_not_be_called(**kwargs: Any):  # noqa: ANN401
        raise AssertionError("start_pipelining_file_import should not run when pre-validation fails")

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())
    monkeypatch.setattr(
        foundry_connectivity_v2.data_connector_pipelining_service,
        "start_pipelining_file_import",
        _should_not_be_called,
    )

    app = _build_test_app()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: object()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()
    app.dependency_overrides[get_connector_pipeline_registry] = lambda: object()
    app.dependency_overrides[get_pipeline_registry] = lambda: object()
    app.dependency_overrides[get_objectify_registry] = lambda: object()
    app.dependency_overrides[get_objectify_job_queue] = lambda: object()
    app.dependency_overrides[get_lineage_store] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-fi-exec-invalid/fileImports/ri.spice.main.file-import.fi-1/execute",
            params={"preview": "true"},
        )

    assert response.status_code == 400
    payload = response.json()
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "FileImportInvalidConfig"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_virtual_table_execute_rejects_invalid_runtime_config_before_pipeline(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {
                ("postgresql_connection", "conn-vt-exec-invalid"): SimpleNamespace(
                    source_id="conn-vt-exec-invalid",
                    source_type="postgresql_connection",
                    enabled=True,
                    config_json={"display_name": "pg", "type": "PostgreSqlConnectionConfig"},
                    created_at=None,
                    updated_at=None,
                ),
                ("postgresql_virtual_table", "vt-1"): SimpleNamespace(
                    source_id="vt-1",
                    source_type="postgresql_virtual_table",
                    enabled=True,
                    config_json={
                        "connection_id": "conn-vt-exec-invalid",
                        "display_name": "VT",
                        "import_mode": "INCREMENTAL",
                        "virtual_table_config": {
                            "type": "postgreSqlVirtualTableConfig",
                            "query": "select 1 as id",
                            "watermarkColumn": "id",
                        },
                    },
                    created_at=None,
                    updated_at=None,
                ),
            }

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def get_mapping(self, *, source_type: str, source_id: str):  # noqa: ANN001
            _ = source_type, source_id
            return SimpleNamespace(
                target_db_name="sales_db",
                target_branch="main",
                target_class_label="Order",
            )

    async def _should_not_be_called(**kwargs: Any):  # noqa: ANN401
        raise AssertionError("start_pipelining_virtual_table should not run when pre-validation fails")

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())
    monkeypatch.setattr(
        foundry_connectivity_v2.data_connector_pipelining_service,
        "start_pipelining_virtual_table",
        _should_not_be_called,
    )

    app = _build_test_app()
    app.dependency_overrides[get_connector_registry] = lambda: _ConnectorRegistry()
    app.dependency_overrides[get_connector_dataset_registry] = lambda: object()
    app.dependency_overrides[get_google_sheets_service] = lambda: object()
    app.dependency_overrides[get_connector_pipeline_registry] = lambda: object()
    app.dependency_overrides[get_pipeline_registry] = lambda: object()
    app.dependency_overrides[get_objectify_registry] = lambda: object()
    app.dependency_overrides[get_objectify_job_queue] = lambda: object()
    app.dependency_overrides[get_lineage_store] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v2/connectivity/connections/ri.spice.main.connection.conn-vt-exec-invalid/virtualTables/ri.spice.main.virtual-table.vt-1/execute",
            params={"preview": "true"},
        )

    assert response.status_code == 400
    payload = response.json()
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "VirtualTableInvalidConfig"


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("connection_config", "expected_type", "expected_source_type", "secret_payload"),
    [
        (
            {
                "type": "MySqlConnectionConfig",
                "host": "mysql.internal",
                "port": 3306,
                "database": "demo",
                "username": "demo_user",
                "password": "top-secret",
            },
            "MySqlConnectionConfig",
            "mysql_connection",
            {"password": "new-secret"},
        ),
        (
            {
                "type": "SqlServerConnectionConfig",
                "host": "sql.internal",
                "port": 1433,
                "database": "demo",
                "username": "demo_user",
                "password": "top-secret",
            },
            "SqlServerConnectionConfig",
            "sqlserver_connection",
            {"connectionString": "DRIVER={ODBC Driver 18 for SQL Server};SERVER=sql.internal,1433;DATABASE=demo;UID=demo_user;PWD=rotated;"},
        ),
        (
            {
                "type": "OracleConnectionConfig",
                "host": "oracle.internal",
                "port": 1521,
                "serviceName": "ORCLPDB1",
                "username": "demo_user",
                "password": "top-secret",
            },
            "OracleConnectionConfig",
            "oracle_connection",
            {"password": "rotated"},
        ),
    ],
)
async def test_foundry_connection_create_jdbc_kinds(
    monkeypatch: pytest.MonkeyPatch,
    connection_config: dict[str, Any],
    expected_type: str,
    expected_source_type: str,
    secret_payload: dict[str, Any],
):
    class _Flags:
        enable_foundry_connectivity_jdbc = True
        foundry_connectivity_jdbc_db_allowlist = ""
        enable_foundry_connectivity_cdc = True
        foundry_connectivity_cdc_db_allowlist = ""

    class _Settings:
        feature_flags = _Flags()

    monkeypatch.setattr(foundry_connectivity_v2, "get_settings", lambda: _Settings())

    class _ConnectorRegistry:
        def __init__(self) -> None:
            self.sources: dict[tuple[str, str], Any] = {}
            self.secrets: dict[tuple[str, str], dict[str, Any]] = {}

        async def get_source(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return self.sources.get((source_type, source_id))

        async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict):  # noqa: ANN001
            from shared.utils.time_utils import utcnow

            self.sources[(source_type, source_id)] = SimpleNamespace(
                source_id=source_id,
                source_type=source_type,
                enabled=enabled,
                config_json=dict(config_json),
                created_at=utcnow(),
                updated_at=utcnow(),
            )
            return self.sources[(source_type, source_id)]

        async def list_sources(self, *, source_type: str, enabled: bool, limit: int):  # noqa: ANN001
            _ = enabled, limit
            return [v for (kind, _), v in self.sources.items() if kind == source_type]

        async def get_connection_secrets(self, *, source_type: str, source_id: str):  # noqa: ANN001
            return dict(self.secrets.get((source_type, source_id), {}))

        async def upsert_connection_secrets(self, *, source_type: str, source_id: str, secrets_json: dict):  # noqa: ANN001
            key = (source_type, source_id)
            existing = dict(self.secrets.get(key, {}))
            existing.update(dict(secrets_json))
            self.secrets[key] = existing
            return dict(existing)

    app = _build_test_app()
    registry = _ConnectorRegistry()
    app.dependency_overrides[get_connector_registry] = lambda: registry
    app.dependency_overrides[get_google_sheets_service] = lambda: object()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post(
            "/api/v2/connectivity/connections",
            params={"preview": "true"},
            json={
                "displayName": "JDBC Connection",
                "configuration": connection_config,
            },
        )
        assert create_resp.status_code == 201
        created = create_resp.json()
        rid = created["rid"]
        assert created["connectionConfiguration"]["type"] == expected_type
        assert created["configuration"]["type"] == expected_type
        assert "password" not in created["configuration"]
        assert "connectionString" not in created["configuration"]
        assert any(kind == expected_source_type for (kind, _connection_id) in registry.sources.keys())

        batch_resp = await client.post(
            "/api/v2/connectivity/connections/getConfigurationBatch",
            params={"preview": "true"},
            json=[{"connectionRid": rid}],
        )
        assert batch_resp.status_code == 200
        assert batch_resp.json()["data"][rid]["type"] == expected_type

        update_resp = await client.post(
            f"/api/v2/connectivity/connections/{rid}/updateSecrets",
            params={"preview": "true"},
            json={"secrets": secret_payload},
        )
        assert update_resp.status_code == 204
