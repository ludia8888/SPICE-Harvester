from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from bff.schemas.objectify_requests import TriggerObjectifyRequest
from bff.services import objectify_run_service


class _FakeDatasetRegistry:
    def __init__(self, *, version: object | None = None) -> None:
        self._version = version

    async def get_dataset(self, *, dataset_id: str):
        if dataset_id != "ds-1":
            return None
        return SimpleNamespace(
            dataset_id="ds-1",
            db_name="demo_db",
            branch="main",
            name="orders",
            schema_json={"columns": [{"name": "order_id", "type": "string"}]},
        )

    async def get_version(self, *, version_id: str):
        if self._version and version_id == "ver-1":
            return self._version
        return None

    async def get_latest_version(self, *, dataset_id: str):  # pragma: no cover - strict gate should bypass
        _ = dataset_id
        raise AssertionError("get_latest_version must not be used when version pin gate is enabled")


class _FakeObjectifyRegistry:
    async def get_mapping_spec(self, *, mapping_spec_id: str):
        if mapping_spec_id != "map-1":
            return None
        return SimpleNamespace(
            mapping_spec_id="map-1",
            dataset_id="ds-1",
            artifact_output_name="orders",
            schema_hash=None,
            backing_datasource_version_id=None,
            version=1,
            options={},
            target_class_id="Order",
        )

    async def get_active_mapping_spec(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return None

    def build_dedupe_key(self, **kwargs):  # noqa: ANN003
        return f"dedupe::{kwargs.get('dataset_id')}::{kwargs.get('dataset_version_id')}"

    async def get_objectify_job_by_dedupe_key(self, *, dedupe_key: str):
        _ = dedupe_key
        return None


class _FakeJobQueue:
    def __init__(self) -> None:
        self.published = None

    async def publish(self, job, require_delivery: bool = False):  # noqa: ANN001
        self.published = {"job": job, "require_delivery": require_delivery}


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "headers": [
                (b"x-db-name", b"demo_db"),
                (b"x-project", b"demo_db"),
                (b"x-user-id", b"qa-user"),
            ],
        }
    )


@pytest.mark.asyncio
async def test_run_objectify_requires_dataset_version_id_without_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _allow_role(*args, **kwargs):  # noqa: ANN003
        _ = args, kwargs
        return None

    monkeypatch.setattr(objectify_run_service, "_require_db_role", _allow_role)
    monkeypatch.setattr(objectify_run_service, "enforce_db_scope", lambda headers, db_name: None)

    with pytest.raises(HTTPException) as exc_info:
        await objectify_run_service.run_objectify(
            dataset_id="ds-1",
            body=TriggerObjectifyRequest(mapping_spec_id="map-1", allow_partial=True),
            request=_request(),
            dataset_registry=_FakeDatasetRegistry(),
            objectify_registry=_FakeObjectifyRegistry(),
            job_queue=_FakeJobQueue(),
            pipeline_registry=SimpleNamespace(),
            oms_client=None,
        )

    error = exc_info.value
    assert getattr(error, "status_code", None) == 400
    assert "dataset_version_id is required" in str(getattr(error, "detail", ""))


@pytest.mark.asyncio
async def test_run_objectify_accepts_version_pinned_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _allow_role(*args, **kwargs):  # noqa: ANN003
        _ = args, kwargs
        return None

    monkeypatch.setattr(objectify_run_service, "_require_db_role", _allow_role)
    monkeypatch.setattr(objectify_run_service, "enforce_db_scope", lambda headers, db_name: None)

    dataset_version = SimpleNamespace(
        version_id="ver-1",
        dataset_id="ds-1",
        artifact_key="s3://raw-datasets/main/demo_db/ds-1/orders.csv",
        sample_json={"columns": [{"name": "order_id", "type": "string"}]},
    )
    queue = _FakeJobQueue()

    response = await objectify_run_service.run_objectify(
        dataset_id="ds-1",
        body=TriggerObjectifyRequest(
            mapping_spec_id="map-1",
            dataset_version_id="ver-1",
            allow_partial=True,
            max_rows=100,
        ),
        request=_request(),
        dataset_registry=_FakeDatasetRegistry(version=dataset_version),
        objectify_registry=_FakeObjectifyRegistry(),
        job_queue=queue,
        pipeline_registry=SimpleNamespace(),
        oms_client=None,
    )

    assert response["status"] == "success"
    payload = response.get("data") if isinstance(response.get("data"), dict) else {}
    assert payload.get("dataset_version_id") == "ver-1"
    assert payload.get("status") == "QUEUED"
    assert queue.published is not None
    assert queue.published["job"].dataset_version_id == "ver-1"
