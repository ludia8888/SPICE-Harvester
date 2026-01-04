from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pytest
from fastapi import HTTPException, status

import bff.routers.pipeline as pipeline_router
from bff.routers.pipeline import build_pipeline, deploy_pipeline, preview_pipeline

PIPELINE_ID = "00000000-0000-0000-0000-000000000004"


@dataclass
class _Request:
    headers: dict[str, str]


@dataclass
class _Pipeline:
    pipeline_id: str
    db_name: str
    name: str = "test-pipeline"
    pipeline_type: str = "batch"
    branch: str = "main"


@dataclass
class _Dataset:
    dataset_id: str
    db_name: str
    name: str
    branch: str = "main"
    schema_json: dict[str, Any] | None = None


@dataclass
class _DatasetVersion:
    version_id: str
    dataset_id: str
    lakefs_commit_id: str
    artifact_key: str


async def _noop_publish_lock(*args: Any, **kwargs: Any) -> None:
    return None


async def _noop_emit_event(*args: Any, **kwargs: Any) -> None:
    return None


class _PipelineRegistry:
    def __init__(
        self,
        *,
        pipeline: _Pipeline,
        build_run: dict[str, Any],
        lakefs_merge_calls: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        self._pipeline = pipeline
        self._build_run = build_run
        if lakefs_merge_calls is None:
            lakefs_merge_calls = []
        self._lakefs_merge_calls = lakefs_merge_calls
        self.recorded_runs: list[dict[str, Any]] = []
        self.recorded_builds: list[dict[str, Any]] = []
        self.recorded_previews: list[dict[str, Any]] = []
        self.recorded_manifests: list[dict[str, Any]] = []

    async def get_pipeline(self, *, pipeline_id: str) -> Optional[_Pipeline]:
        if pipeline_id == self._pipeline.pipeline_id:
            return self._pipeline
        return None

    async def get_pipeline_branch(self, *, db_name: str, branch: str) -> Optional[dict[str, Any]]:
        return {"db_name": str(db_name), "branch": str(branch), "archived": False}

    async def get_latest_version(self, *, pipeline_id: str, branch: Optional[str] = None) -> Any:
        class _Version:
            lakefs_commit_id = "c-def"
            definition_json: dict[str, Any] = {}

        return _Version()

    async def get_run(self, *, pipeline_id: str, job_id: str) -> Optional[dict[str, Any]]:
        if pipeline_id == self._pipeline.pipeline_id and job_id == self._build_run.get("job_id"):
            return self._build_run
        return None

    async def has_any_permissions(self, *, pipeline_id: str) -> bool:
        return True

    async def has_permission(self, *, pipeline_id: str, principal_type: str, principal_id: str, required_role: str) -> bool:
        return True

    async def record_run(
        self,
        *,
        pipeline_id: str,
        job_id: str,
        mode: str,
        status: str,
        output_json: dict[str, Any] | None = None,
        sample_json: dict[str, Any] | None = None,
        finished_at: Any = None,
        **kwargs: Any,
    ) -> None:
        self.recorded_runs.append(
            {
                "pipeline_id": pipeline_id,
                "job_id": job_id,
                "mode": mode,
                "status": status,
                "output_json": output_json,
                "sample_json": sample_json,
                "finished_at": finished_at,
            }
        )

    async def record_build(
        self,
        *,
        pipeline_id: str,
        status: str,
        output_json: dict[str, Any],
        deployed_commit_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self.recorded_builds.append(
            {
                "pipeline_id": pipeline_id,
                "status": status,
                "output_json": output_json,
                "deployed_commit_id": deployed_commit_id,
            }
        )

    async def record_preview(self, **kwargs: Any) -> None:
        self.recorded_previews.append(dict(kwargs))
        return None

    async def replace_dependencies(self, **kwargs: Any) -> None:
        return None

    async def update_pipeline(self, **kwargs: Any) -> None:
        return None

    async def get_lakefs_client(self, *, user_id: Optional[str] = None) -> Any:
        calls = self._lakefs_merge_calls

        class _LakeFS:
            async def merge(self, **kwargs: Any) -> str:
                calls.append(dict(kwargs))
                return "m-commit"

        return _LakeFS()

    async def get_artifact_by_job(self, *, pipeline_id: str, job_id: str, mode: str) -> Any:
        return None

    async def record_promotion_manifest(self, **kwargs: Any) -> str:
        self.recorded_manifests.append(dict(kwargs))
        return "manifest-1"


class _PipelineJobQueue:
    def __init__(self) -> None:
        self.published: list[Any] = []

    async def publish(self, job: Any, **kwargs: Any) -> None:
        self.published.append(job)
        return None


class _DatasetRegistry:
    def __init__(self) -> None:
        self.datasets: dict[tuple[str, str, str], _Dataset] = {}
        self.versions: list[dict[str, Any]] = []

    async def get_dataset_by_name(self, *, db_name: str, name: str, branch: str) -> Optional[_Dataset]:
        return self.datasets.get((db_name, name, branch))

    async def create_dataset(self, *, db_name: str, name: str, description: Any, source_type: str, source_ref: str, schema_json: dict[str, Any], branch: str) -> _Dataset:
        dataset = _Dataset(
            dataset_id=f"ds-{db_name}-{name}-{branch}",
            db_name=db_name,
            name=name,
            branch=branch,
            schema_json=schema_json,
        )
        self.datasets[(db_name, name, branch)] = dataset
        return dataset

    async def add_version(
        self,
        *,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: str,
        row_count: Any,
        sample_json: dict[str, Any],
        schema_json: dict[str, Any],
        promoted_from_artifact_id: Optional[str] = None,
    ) -> _DatasetVersion:
        self.versions.append(
            {
                "dataset_id": dataset_id,
                "lakefs_commit_id": lakefs_commit_id,
                "artifact_key": artifact_key,
                "row_count": row_count,
                "sample_json": sample_json,
                "schema_json": schema_json,
            }
        )
        return _DatasetVersion(
            version_id=f"v-{len(self.versions)}",
            dataset_id=dataset_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
        )


class _ObjectifyRegistry:
    async def get_active_mapping_spec(
        self,
        *,
        dataset_id: str,
        dataset_branch: str,
        target_class_id: Optional[str] = None,
        artifact_output_name: Optional[str] = None,
        schema_hash: Optional[str] = None,
    ) -> Any:
        return None


class _OMSClient:
    def __init__(self, head_commit_id: str = "c-main") -> None:
        self._head_commit_id = head_commit_id

    async def get_version_head(self, db_name: str, *, branch: str = "main") -> dict[str, Any]:
        return {"status": "success", "data": {"branch": branch, "head_commit_id": self._head_commit_id}}


class _AuditStore:
    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []

    async def log(self, **kwargs: Any) -> None:
        self.entries.append(dict(kwargs))


@pytest.fixture
def lakefs_merge_stub(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    return []


@pytest.mark.asyncio
async def test_build_enqueues_job_and_records_run() -> None:
    pipeline = _Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb", branch="main")
    registry = _PipelineRegistry(pipeline=pipeline, build_run={"job_id": "x"})
    queue = _PipelineJobQueue()

    response = await build_pipeline(
        pipeline_id=PIPELINE_ID,
        payload={
            "db_name": "testdb",
            "definition_json": {"nodes": [], "edges": []},
            "limit": 123,
        },
        pipeline_registry=registry,
        pipeline_job_queue=queue,
        oms_client=_OMSClient(head_commit_id="c-main"),
        audit_store=_AuditStore(),
    )

    assert response["status"] == "success"
    assert response["data"]["pipeline_id"] == PIPELINE_ID
    assert response["data"]["job_id"].startswith(f"build-{PIPELINE_ID}-")
    assert response["data"]["limit"] == 123

    assert len(queue.published) == 1
    job = queue.published[0]
    assert job.pipeline_id == PIPELINE_ID
    assert job.db_name == "testdb"
    assert job.mode == "build"
    assert job.preview_limit == 123
    meta = job.definition_json.get("__build_meta__") or {}
    assert meta.get("ontology", {}).get("commit") == "c-main"

    assert len(registry.recorded_runs) == 1
    run = registry.recorded_runs[0]
    assert run["pipeline_id"] == PIPELINE_ID
    assert run["mode"] == "build"
    assert run["status"] == "QUEUED"
    assert run["output_json"]["queued"] is True


@pytest.mark.asyncio
async def test_preview_enqueues_job_with_node_id_and_records_preview_and_run(monkeypatch: pytest.MonkeyPatch) -> None:
    class _EventStore:
        async def connect(self) -> None:
            return None

        async def append_event(self, event: Any) -> None:
            return None

    import bff.routers.pipeline as pipeline_router

    monkeypatch.setattr(pipeline_router, "event_store", _EventStore())

    pipeline = _Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb", branch="main")
    registry = _PipelineRegistry(pipeline=pipeline, build_run={"job_id": "x"})
    queue = _PipelineJobQueue()

    response = await preview_pipeline(
        pipeline_id=PIPELINE_ID,
        payload={
            "db_name": "testdb",
            "definition_json": {"nodes": [], "edges": []},
            "node_id": "node-x",
            "limit": 42,
        },
        pipeline_registry=registry,
        pipeline_job_queue=queue,
        audit_store=_AuditStore(),
    )

    assert response["status"] == "success"
    assert response["data"]["pipeline_id"] == PIPELINE_ID
    assert response["data"]["job_id"].startswith(f"preview-{PIPELINE_ID}-")
    assert response["data"]["limit"] == 42

    assert len(queue.published) == 1
    job = queue.published[0]
    assert job.pipeline_id == PIPELINE_ID
    assert job.db_name == "testdb"
    assert job.mode == "preview"
    assert job.preview_limit == 42
    assert job.node_id == "node-x"

    assert len(registry.recorded_previews) == 1
    assert registry.recorded_previews[0]["status"] == "QUEUED"
    assert registry.recorded_previews[0]["node_id"] == "node-x"

    assert len(registry.recorded_runs) == 1
    run = registry.recorded_runs[0]
    assert run["mode"] == "preview"
    assert run["status"] == "QUEUED"
    assert run["sample_json"]["queued"] is True


@pytest.mark.asyncio
async def test_promote_build_merges_build_branch_to_main_and_registers_version(
    lakefs_merge_stub: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_router, "_acquire_pipeline_publish_lock", _noop_publish_lock)
    monkeypatch.setattr(pipeline_router, "emit_pipeline_control_plane_event", _noop_emit_event)
    pipeline = _Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb", branch="main")
    build_job_id = "build-p-123"
    node_id = "node-1"

    build_branch = "build-p-123"
    staged_artifact_key = f"s3://pipeline-artifacts/{build_branch}/pipelines/testdb/p/pipeline_out"
    build_run = {
        "job_id": build_job_id,
        "mode": "build",
        "status": "SUCCESS",
        "output_json": {
            "outputs": [
                {
                    "node_id": node_id,
                    "dataset_name": "pipeline_out",
                    "artifact_key": staged_artifact_key,
                    "row_count": 2,
                    "columns": [{"name": "a", "type": "String"}],
                    "rows": [{"a": 1}, {"a": 2}],
                }
            ],
            "branch": "main",
            "ontology": {"branch": "main", "commit": "c-main"},
            "lakefs": {"repository": "pipeline-artifacts", "build_branch": build_branch},
        },
    }

    registry = _PipelineRegistry(pipeline=pipeline, build_run=build_run, lakefs_merge_calls=lakefs_merge_stub)
    dataset_registry = _DatasetRegistry()

    response = await deploy_pipeline(
        pipeline_id=PIPELINE_ID,
        payload={
            "promote_build": True,
            "build_job_id": build_job_id,
            "node_id": node_id,
            "definition_json": {},
        },
        request=_Request(headers={}),
        pipeline_registry=registry,
        dataset_registry=dataset_registry,
        objectify_registry=_ObjectifyRegistry(),
        oms_client=_OMSClient(head_commit_id="c-main"),
        lineage_store=None,
        audit_store=_AuditStore(),
    )

    assert response["status"] == "success"
    outputs = response["data"]["outputs"]
    assert outputs[0]["build_artifact_key"] == staged_artifact_key
    promoted_key = outputs[0]["artifact_key"]
    assert promoted_key == "s3://pipeline-artifacts/m-commit/pipelines/testdb/p/pipeline_out"

    assert lakefs_merge_stub, "Expected lakeFS merge to be called"
    call = lakefs_merge_stub[0]
    assert call["repository"] == "pipeline-artifacts"
    assert call["source_ref"] == build_branch
    assert call["destination_branch"] == "main"

    assert dataset_registry.versions, "Expected dataset_registry.add_version to be called"
    version = dataset_registry.versions[0]
    assert version["artifact_key"] == promoted_key
    assert version["lakefs_commit_id"] == "m-commit"


@pytest.mark.asyncio
async def test_promote_build_rejects_non_staged_artifact_key() -> None:
    pipeline = _Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb", branch="main")
    build_job_id = "build-p-456"
    node_id = "node-1"
    build_branch = "build-p-456"

    build_run = {
        "job_id": build_job_id,
        "mode": "build",
        "status": "SUCCESS",
        "output_json": {
            "outputs": [
                {
                    "node_id": node_id,
                    "dataset_name": "pipeline_out",
                    "artifact_key": "s3://pipeline-artifacts/wrong-ref/pipelines/testdb/p/pipeline_out",
                    "row_count": 1,
                    "columns": [{"name": "a", "type": "String"}],
                    "rows": [{"a": 1}],
                }
            ],
            "branch": "main",
            "ontology": {"branch": "main", "commit": "c-main"},
            "lakefs": {"repository": "pipeline-artifacts", "build_branch": build_branch},
        },
    }

    registry = _PipelineRegistry(pipeline=pipeline, build_run=build_run)

    with pytest.raises(HTTPException) as exc_info:
        await deploy_pipeline(
            pipeline_id=PIPELINE_ID,
            payload={
                "promote_build": True,
                "build_job_id": build_job_id,
                "node_id": node_id,
                "definition_json": {},
            },
            request=_Request(headers={}),
            pipeline_registry=registry,
            dataset_registry=_DatasetRegistry(),
            objectify_registry=_ObjectifyRegistry(),
            oms_client=_OMSClient(head_commit_id="c-main"),
            lineage_store=None,
            audit_store=_AuditStore(),
        )

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail["code"] == "INVALID_BUILD_REF"


@pytest.mark.asyncio
async def test_promote_build_surfaces_build_errors_when_build_failed() -> None:
    pipeline = _Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb", branch="main")
    build_job_id = "build-job-failed"

    build_run = {
        "job_id": build_job_id,
        "mode": "build",
        "status": "FAILED",
        "output_json": {"errors": ["schema contract missing column: missing_col"]},
    }

    registry = _PipelineRegistry(pipeline=pipeline, build_run=build_run)

    with pytest.raises(HTTPException) as exc_info:
        await deploy_pipeline(
            pipeline_id=PIPELINE_ID,
            payload={
                "promote_build": True,
                "build_job_id": build_job_id,
                "node_id": "node-1",
                "definition_json": {},
            },
            request=_Request(headers={}),
            pipeline_registry=registry,
            dataset_registry=_DatasetRegistry(),
            objectify_registry=_ObjectifyRegistry(),
            oms_client=_OMSClient(head_commit_id="c-main"),
            lineage_store=None,
            audit_store=_AuditStore(),
        )

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail["code"] == "BUILD_NOT_SUCCESS"
    assert exc_info.value.detail["build_status"] == "FAILED"
    assert exc_info.value.detail["errors"] == ["schema contract missing column: missing_col"]


@pytest.mark.asyncio
async def test_promote_build_blocks_deploy_when_expectations_failed() -> None:
    pipeline = _Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb", branch="main")
    build_job_id = "build-job-expectations-failed"

    build_run = {
        "job_id": build_job_id,
        "mode": "build",
        "status": "FAILED",
        "output_json": {"errors": ["unique failed: id"]},
    }

    registry = _PipelineRegistry(pipeline=pipeline, build_run=build_run)

    with pytest.raises(HTTPException) as exc_info:
        await deploy_pipeline(
            pipeline_id=PIPELINE_ID,
            payload={
                "promote_build": True,
                "build_job_id": build_job_id,
                "node_id": "node-1",
                "definition_json": {},
            },
            request=_Request(headers={}),
            pipeline_registry=registry,
            dataset_registry=_DatasetRegistry(),
            objectify_registry=_ObjectifyRegistry(),
            oms_client=_OMSClient(head_commit_id="c-main"),
            lineage_store=None,
            audit_store=_AuditStore(),
        )

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail["code"] == "BUILD_NOT_SUCCESS"
    assert exc_info.value.detail["errors"] == ["unique failed: id"]


@pytest.mark.asyncio
async def test_promote_build_requires_replay_for_breaking_schema_changes(
    lakefs_merge_stub: list[dict[str, Any]],
) -> None:
    pipeline = _Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb", branch="main")
    build_job_id = "build-p-breaking"
    node_id = "node-1"
    build_branch = "build-p-breaking"
    staged_artifact_key = f"s3://pipeline-artifacts/{build_branch}/pipelines/testdb/p/pipeline_out"
    build_run = {
        "job_id": build_job_id,
        "mode": "build",
        "status": "SUCCESS",
        "output_json": {
            "outputs": [
                {
                    "node_id": node_id,
                    "dataset_name": "pipeline_out",
                    "artifact_key": staged_artifact_key,
                    "row_count": 1,
                    "columns": [{"name": "new_col", "type": "xsd:string"}],
                    "rows": [{"new_col": "x"}],
                }
            ],
            "branch": "main",
            "ontology": {"branch": "main", "commit": "c-main"},
            "lakefs": {"repository": "pipeline-artifacts", "build_branch": build_branch},
        },
    }

    registry = _PipelineRegistry(pipeline=pipeline, build_run=build_run, lakefs_merge_calls=lakefs_merge_stub)
    dataset_registry = _DatasetRegistry()
    dataset_registry.datasets[("testdb", "pipeline_out", "main")] = _Dataset(
        dataset_id="ds-existing",
        db_name="testdb",
        name="pipeline_out",
        branch="main",
        schema_json={"columns": [{"name": "old_col", "type": "xsd:string"}]},
    )

    with pytest.raises(HTTPException) as exc_info:
        await deploy_pipeline(
            pipeline_id=PIPELINE_ID,
            payload={
                "promote_build": True,
                "build_job_id": build_job_id,
                "node_id": node_id,
                "definition_json": {},
            },
            request=_Request(headers={}),
            pipeline_registry=registry,
            dataset_registry=dataset_registry,
            objectify_registry=_ObjectifyRegistry(),
            lineage_store=None,
            oms_client=_OMSClient(head_commit_id="c-main"),
            audit_store=_AuditStore(),
        )

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail["code"] == "REPLAY_REQUIRED"
    assert exc_info.value.detail["breaking_changes"]


@pytest.mark.asyncio
async def test_promote_build_allows_breaking_schema_changes_with_replay_flag(
    lakefs_merge_stub: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_router, "_acquire_pipeline_publish_lock", _noop_publish_lock)
    monkeypatch.setattr(pipeline_router, "emit_pipeline_control_plane_event", _noop_emit_event)
    pipeline = _Pipeline(pipeline_id=PIPELINE_ID, db_name="testdb", branch="main")
    build_job_id = "build-p-breaking-allow"
    node_id = "node-1"
    build_branch = "build-p-breaking-allow"
    staged_artifact_key = f"s3://pipeline-artifacts/{build_branch}/pipelines/testdb/p/pipeline_out"
    build_run = {
        "job_id": build_job_id,
        "mode": "build",
        "status": "SUCCESS",
        "output_json": {
            "outputs": [
                {
                    "node_id": node_id,
                    "dataset_name": "pipeline_out",
                    "artifact_key": staged_artifact_key,
                    "row_count": 1,
                    "columns": [{"name": "new_col", "type": "xsd:string"}],
                    "rows": [{"new_col": "x"}],
                }
            ],
            "branch": "main",
            "ontology": {"branch": "main", "commit": "c-main"},
            "lakefs": {"repository": "pipeline-artifacts", "build_branch": build_branch},
        },
    }

    registry = _PipelineRegistry(pipeline=pipeline, build_run=build_run, lakefs_merge_calls=lakefs_merge_stub)
    dataset_registry = _DatasetRegistry()
    dataset_registry.datasets[("testdb", "pipeline_out", "main")] = _Dataset(
        dataset_id="ds-existing",
        db_name="testdb",
        name="pipeline_out",
        branch="main",
        schema_json={"columns": [{"name": "old_col", "type": "xsd:string"}]},
    )

    response = await deploy_pipeline(
        pipeline_id=PIPELINE_ID,
        payload={
            "promote_build": True,
            "build_job_id": build_job_id,
            "node_id": node_id,
            "definition_json": {},
            "replay": True,
        },
        request=_Request(headers={}),
        pipeline_registry=registry,
        dataset_registry=dataset_registry,
        objectify_registry=_ObjectifyRegistry(),
        lineage_store=None,
        oms_client=_OMSClient(head_commit_id="c-main"),
        audit_store=_AuditStore(),
    )

    assert response["status"] == "success"
    outputs = response["data"]["outputs"]
    assert outputs[0]["replay_on_deploy"] is True
    assert outputs[0]["breaking_changes"], "Expected breaking_changes to be surfaced for operator visibility"
