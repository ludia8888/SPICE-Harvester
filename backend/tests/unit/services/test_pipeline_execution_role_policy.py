from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException, status

import bff.services.pipeline_execution_service as pipeline_execution_service


async def _noop_async(*args: Any, **kwargs: Any) -> None:
    _ = args, kwargs
    return None


def _noop_sync(*args: Any, **kwargs: Any) -> None:
    _ = args, kwargs
    return None


@pytest.mark.asyncio
async def test_preview_pipeline_uses_fail_closed_registry_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def _capture_role_guard(**kwargs: Any) -> None:
        captured.update(kwargs)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Database access registry unavailable"},
        )

    async def _prepare_preview_execution(**kwargs: Any) -> SimpleNamespace:
        _ = kwargs
        return SimpleNamespace(
            run_context=SimpleNamespace(db_name="demo", node_id="node-1"),
            request_payload=SimpleNamespace(limit=10),
            job_id="job-1",
        )

    monkeypatch.setattr(pipeline_execution_service, "_require_pipeline_idempotency_key", _noop_sync)
    monkeypatch.setattr(pipeline_execution_service, "_ensure_pipeline_permission", _noop_async)
    monkeypatch.setattr(pipeline_execution_service, "_prepare_preview_execution", _prepare_preview_execution)
    monkeypatch.setattr(pipeline_execution_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(
        pipeline_execution_service,
        "enforce_database_role_or_http_error",
        _capture_role_guard,
    )

    with pytest.raises(HTTPException) as exc_info:
        await pipeline_execution_service.preview_pipeline(
            pipeline_id="pipe-1",
            payload={},
            request=SimpleNamespace(headers={}),
            audit_store=object(),
            pipeline_registry=object(),
            pipeline_job_queue=object(),
            dataset_registry=object(),
            event_store=object(),
        )

    assert exc_info.value.status_code == 503
    assert captured["allow_if_registry_unavailable"] is False


@pytest.mark.asyncio
async def test_build_pipeline_uses_fail_closed_registry_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def _capture_role_guard(**kwargs: Any) -> None:
        captured.update(kwargs)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Database access registry unavailable"},
        )

    async def _prepare_build_execution(**kwargs: Any) -> SimpleNamespace:
        _ = kwargs
        return SimpleNamespace(
            run_context=SimpleNamespace(db_name="demo", node_id="node-1"),
            request_payload=SimpleNamespace(limit=10),
            job_id="job-1",
        )

    monkeypatch.setattr(pipeline_execution_service, "_require_pipeline_idempotency_key", _noop_sync)
    monkeypatch.setattr(pipeline_execution_service, "_ensure_pipeline_permission", _noop_async)
    monkeypatch.setattr(pipeline_execution_service, "_prepare_build_execution", _prepare_build_execution)
    monkeypatch.setattr(pipeline_execution_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(
        pipeline_execution_service,
        "enforce_database_role_or_http_error",
        _capture_role_guard,
    )

    with pytest.raises(HTTPException) as exc_info:
        await pipeline_execution_service.build_pipeline(
            pipeline_id="pipe-1",
            payload={},
            request=SimpleNamespace(headers={}),
            audit_store=object(),
            pipeline_registry=object(),
            pipeline_job_queue=object(),
            dataset_registry=object(),
            oms_client=object(),
            emit_pipeline_control_plane_event=_noop_async,
        )

    assert exc_info.value.status_code == 503
    assert captured["allow_if_registry_unavailable"] is False


@pytest.mark.asyncio
async def test_deploy_pipeline_uses_fail_closed_registry_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def _capture_role_guard(**kwargs: Any) -> None:
        captured.update(kwargs)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Database access registry unavailable"},
        )

    async def _prepare_deploy_execution(**kwargs: Any) -> SimpleNamespace:
        _ = kwargs
        return SimpleNamespace(db_name="demo")

    monkeypatch.setattr(pipeline_execution_service, "_require_pipeline_idempotency_key", _noop_sync)
    monkeypatch.setattr(pipeline_execution_service, "_ensure_pipeline_permission", _noop_async)
    monkeypatch.setattr(pipeline_execution_service, "_prepare_deploy_execution", _prepare_deploy_execution)
    monkeypatch.setattr(pipeline_execution_service, "enforce_db_scope_or_403", _noop_sync)
    monkeypatch.setattr(
        pipeline_execution_service,
        "enforce_database_role_or_http_error",
        _capture_role_guard,
    )

    with pytest.raises(HTTPException) as exc_info:
        await pipeline_execution_service.deploy_pipeline(
            pipeline_id="pipe-1",
            payload={},
            request=SimpleNamespace(headers={}),
            pipeline_registry=object(),
            dataset_registry=object(),
            objectify_registry=object(),
            oms_client=object(),
            lineage_store=object(),
            audit_store=object(),
            emit_pipeline_control_plane_event=_noop_async,
            _acquire_pipeline_publish_lock=_noop_async,
            _release_pipeline_publish_lock=_noop_async,
        )

    assert exc_info.value.status_code == 503
    assert captured["allow_if_registry_unavailable"] is False
