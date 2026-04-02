from __future__ import annotations

from types import SimpleNamespace

import pytest

from ingest_reconciler_worker.main import IngestReconcilerWorker


class _FakeMetrics:
    def record_business_metric(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None


class _FailThenSucceedHttpClient:
    def __init__(self) -> None:
        self.calls = 0

    async def post(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("delivery failed")


@pytest.mark.asyncio
async def test_alert_cooldown_only_advances_after_successful_delivery() -> None:
    worker = IngestReconcilerWorker()
    worker.metrics = _FakeMetrics()
    worker.alert_webhook_url = "https://example.invalid/hook"
    worker.http = _FailThenSucceedHttpClient()
    worker.alert_cooldown_seconds = 60
    worker._last_alert_at = 0.0

    await worker._emit_alert({"kind": "dataset_ingest_reconcile"})
    assert worker._last_alert_at == 0.0

    await worker._emit_alert({"kind": "dataset_ingest_reconcile"})
    assert worker.http.calls == 2
    assert worker._last_alert_at > 0.0
