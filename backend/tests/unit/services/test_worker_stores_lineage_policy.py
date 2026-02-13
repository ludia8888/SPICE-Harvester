from __future__ import annotations

import pytest

from shared.services.core import worker_stores as worker_stores_module


class _DummyLineageStore:
    def __init__(self, should_fail: bool) -> None:
        self._should_fail = should_fail

    async def initialize(self) -> None:
        if self._should_fail:
            raise RuntimeError("lineage init failed")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_initialize_worker_stores_fails_when_lineage_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_LINEAGE", "true")
    monkeypatch.setenv("LINEAGE_FAIL_CLOSED", "true")
    monkeypatch.setenv("LINEAGE_FAIL_OPEN_OVERRIDE", "false")

    async def _fake_processed():
        return object()

    monkeypatch.setattr(worker_stores_module, "create_processed_event_registry", _fake_processed)
    monkeypatch.setattr(
        worker_stores_module,
        "create_lineage_store",
        lambda settings: _DummyLineageStore(should_fail=True),
    )

    with pytest.raises(RuntimeError, match="lineage init failed"):
        await worker_stores_module.initialize_worker_stores(
            enable_lineage=True,
            enable_audit_logs=False,
            logger=worker_stores_module.logging.getLogger("test"),
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_initialize_worker_stores_allows_fail_open_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_LINEAGE", "true")
    monkeypatch.setenv("LINEAGE_FAIL_CLOSED", "true")
    monkeypatch.setenv("LINEAGE_FAIL_OPEN_OVERRIDE", "true")

    async def _fake_processed():
        return object()

    monkeypatch.setattr(worker_stores_module, "create_processed_event_registry", _fake_processed)
    monkeypatch.setattr(
        worker_stores_module,
        "create_lineage_store",
        lambda settings: _DummyLineageStore(should_fail=True),
    )

    stores = await worker_stores_module.initialize_worker_stores(
        enable_lineage=True,
        enable_audit_logs=False,
        logger=worker_stores_module.logging.getLogger("test"),
    )

    assert stores.lineage_store is None
    assert stores.lineage_required is False
