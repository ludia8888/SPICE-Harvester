from __future__ import annotations

import pytest
from starlette.requests import Request
from types import SimpleNamespace

from bff.routers import admin_recompute_projection as router_module
from shared.security.input_sanitizer import SecurityViolationError


class _LimiterStub:
    async def check_rate_limit(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return True, {"remaining": 99, "reset_in": 60}


def _make_request() -> Request:
    app = SimpleNamespace(state=SimpleNamespace(rate_limiter=_LimiterStub(), metrics_collector=None))
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/admin/reindex-instances",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "query_string": b"",
        "scheme": "http",
        "server": ("testserver", 80),
        "app": app,
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_reindex_instances_endpoint_invokes_service(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_reindex_all_instances(**kwargs):
        captured.update(kwargs)
        return {"status": "submitted", "submitted_jobs": 2}

    monkeypatch.setattr(router_module.admin_reindex_instances_service, "reindex_all_instances", _fake_reindex_all_instances)

    dataset_registry = object()
    objectify_registry = object()
    job_queue = object()
    elasticsearch_service = object()

    response = await router_module.reindex_instances_endpoint(
        http_request=_make_request(),
        db_name="qaopsdb",
        branch="main",
        delete_index_first=True,
        dataset_registry=dataset_registry,  # type: ignore[arg-type]
        objectify_registry=objectify_registry,  # type: ignore[arg-type]
        job_queue=job_queue,  # type: ignore[arg-type]
        elasticsearch_service=elasticsearch_service,  # type: ignore[arg-type]
    )

    assert response["status"] == "submitted"
    assert response["submitted_jobs"] == 2
    assert captured["db_name"] == "qaopsdb"
    assert captured["branch"] == "main"
    assert captured["delete_index_first"] is True
    assert captured["dataset_registry"] is dataset_registry
    assert captured["objectify_registry"] is objectify_registry
    assert captured["job_queue"] is job_queue
    assert captured["elasticsearch_service"] is elasticsearch_service


@pytest.mark.asyncio
async def test_reindex_instances_endpoint_validates_db_name() -> None:
    with pytest.raises(SecurityViolationError):
        await router_module.reindex_instances_endpoint(
            http_request=_make_request(),
            db_name="BAD DB NAME",
            branch="main",
            delete_index_first=False,
            dataset_registry=object(),  # type: ignore[arg-type]
            objectify_registry=object(),  # type: ignore[arg-type]
            job_queue=object(),  # type: ignore[arg-type]
            elasticsearch_service=object(),  # type: ignore[arg-type]
        )
