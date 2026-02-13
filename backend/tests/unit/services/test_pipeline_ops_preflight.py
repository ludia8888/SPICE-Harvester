from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from bff.routers import pipeline_ops_preflight


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_pipeline_preflight_fail_closed_raises_http_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _raise_preflight(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        pipeline_ops_preflight,
        "compute_pipeline_preflight",
        _raise_preflight,
    )
    monkeypatch.setattr(
        pipeline_ops_preflight,
        "get_settings",
        lambda: SimpleNamespace(pipeline=SimpleNamespace(preflight_fail_closed=True)),
    )

    with pytest.raises(HTTPException) as exc_info:
        await pipeline_ops_preflight._run_pipeline_preflight(
            definition_json={"nodes": [], "edges": []},
            db_name="demo",
            branch="main",
            dataset_registry=SimpleNamespace(),
        )

    assert exc_info.value.status_code == 500
    assert isinstance(exc_info.value.detail, dict)
    assert exc_info.value.detail.get("error_code") == "PIPELINE_PREFLIGHT_FAILED"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_pipeline_preflight_fail_open_returns_warning_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _raise_preflight(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        pipeline_ops_preflight,
        "compute_pipeline_preflight",
        _raise_preflight,
    )
    monkeypatch.setattr(
        pipeline_ops_preflight,
        "get_settings",
        lambda: SimpleNamespace(pipeline=SimpleNamespace(preflight_fail_closed=False)),
    )

    result = await pipeline_ops_preflight._run_pipeline_preflight(
        definition_json={"nodes": [], "edges": []},
        db_name="demo",
        branch="main",
        dataset_registry=SimpleNamespace(),
    )

    assert result.get("has_blocking_errors") is False
    issues = result.get("issues") or []
    assert issues and issues[0].get("kind") == "preflight_error"
