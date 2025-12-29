"""
Dataset ingest reconciler worker.

Runs ingest reconciliation in a dedicated service and exposes /metrics + /health.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI

from shared.observability.metrics import get_metrics_collector
from shared.services.dataset_registry import DatasetRegistry
from shared.services.service_factory import ServiceInfo, create_fastapi_service
from shared.utils.env_utils import parse_bool_env, parse_int_env
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

_ALERT_WEBHOOK_ENV_KEYS = ("INGEST_RECONCILER_ALERT_WEBHOOK_URL", "ALERT_WEBHOOK_URL")


def _resolve_webhook_url() -> Optional[str]:
    for key in _ALERT_WEBHOOK_ENV_KEYS:
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return None


class IngestReconcilerWorker:
    def __init__(self) -> None:
        self.enabled = bool(parse_bool_env("ENABLE_DATASET_INGEST_RECONCILER", True))
        self.poll_interval_seconds = parse_int_env(
            "DATASET_INGEST_RECONCILER_POLL_SECONDS", 60, min_value=5, max_value=3600
        )
        self.stale_after_seconds = parse_int_env(
            "DATASET_INGEST_RECONCILER_STALE_SECONDS", 3600, min_value=60, max_value=86_400
        )
        self.limit = parse_int_env("DATASET_INGEST_RECONCILER_LIMIT", 200, min_value=1, max_value=5000)
        self.alert_published_threshold = parse_int_env(
            "INGEST_RECONCILER_ALERT_PUBLISHED_THRESHOLD", 1, min_value=0, max_value=1_000_000
        )
        self.alert_aborted_threshold = parse_int_env(
            "INGEST_RECONCILER_ALERT_ABORTED_THRESHOLD", 1, min_value=0, max_value=1_000_000
        )
        self.alert_on_error = bool(parse_bool_env("INGEST_RECONCILER_ALERT_ON_ERROR", True))
        self.alert_cooldown_seconds = parse_int_env(
            "INGEST_RECONCILER_ALERT_COOLDOWN_SECONDS", 300, min_value=0, max_value=86_400
        )
        self.alert_webhook_url = _resolve_webhook_url()

        self.dataset_registry: Optional[DatasetRegistry] = None
        self.http: Optional[httpx.AsyncClient] = None
        self.metrics = get_metrics_collector("ingest-reconciler-worker")
        self.running = False
        self._last_alert_at = 0.0

    async def initialize(self) -> None:
        self.dataset_registry = DatasetRegistry()
        await self.dataset_registry.initialize()
        if self.alert_webhook_url:
            self.http = httpx.AsyncClient(timeout=10.0)

    async def close(self) -> None:
        if self.http:
            await self.http.aclose()
            self.http = None
        if self.dataset_registry:
            await self.dataset_registry.close()
            self.dataset_registry = None

    def _record_metrics(self, result: Dict[str, int]) -> None:
        self.metrics.record_business_metric("ingest_reconciler_runs", 1)
        if result.get("published"):
            self.metrics.record_business_metric("ingest_reconciler_published", result["published"])
        if result.get("aborted"):
            self.metrics.record_business_metric("ingest_reconciler_aborted", result["aborted"])
        if result.get("committed_tx"):
            self.metrics.record_business_metric("ingest_reconciler_committed_tx", result["committed_tx"])
        if result.get("skipped"):
            self.metrics.record_business_metric("ingest_reconciler_skipped", result["skipped"])

    def _record_error_metric(self) -> None:
        self.metrics.record_business_metric("ingest_reconciler_errors", 1)

    def _record_alert_metric(self) -> None:
        self.metrics.record_business_metric("ingest_reconciler_alerts", 1)

    def _record_alert_failure_metric(self) -> None:
        self.metrics.record_business_metric("ingest_reconciler_alert_failures", 1)

    def _should_alert(self, result: Dict[str, int]) -> bool:
        if self.alert_published_threshold > 0 and result.get("published", 0) >= self.alert_published_threshold:
            return True
        if self.alert_aborted_threshold > 0 and result.get("aborted", 0) >= self.alert_aborted_threshold:
            return True
        return False

    def _alert_allowed(self) -> bool:
        if self.alert_cooldown_seconds <= 0:
            return True
        now = time.monotonic()
        if now - self._last_alert_at >= self.alert_cooldown_seconds:
            self._last_alert_at = now
            return True
        return False

    async def _emit_alert(self, payload: Dict[str, Any]) -> None:
        logger.warning("Ingest reconciler alert: %s", payload)
        if not self.alert_webhook_url or not self.http:
            return
        if not self._alert_allowed():
            return
        try:
            await self.http.post(self.alert_webhook_url, json=payload)
            self._record_alert_metric()
        except Exception:
            self._record_alert_failure_metric()
            logger.exception("Failed to deliver ingest reconciler alert")

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self.enabled:
            logger.info("Ingest reconciler disabled; worker will not run.")
            return
        if not self.dataset_registry:
            raise RuntimeError("DatasetRegistry not initialized")

        self.running = True
        while not stop_event.is_set():
            started_at = time.monotonic()
            result: Optional[Dict[str, int]] = None
            try:
                result = await self.dataset_registry.reconcile_ingest_state(
                    stale_after_seconds=self.stale_after_seconds,
                    limit=self.limit,
                )
                if result is not None:
                    self._record_metrics(result)
                    if self._should_alert(result):
                        await self._emit_alert(
                            {
                                "kind": "dataset_ingest_reconcile",
                                "status": "warning",
                                "result": result,
                                "stale_after_seconds": self.stale_after_seconds,
                                "limit": self.limit,
                                "timestamp": utcnow().isoformat(),
                            }
                        )
            except Exception as exc:
                self._record_error_metric()
                if self.alert_on_error:
                    await self._emit_alert(
                        {
                            "kind": "dataset_ingest_reconcile",
                            "status": "error",
                            "error": str(exc),
                            "stale_after_seconds": self.stale_after_seconds,
                            "limit": self.limit,
                            "timestamp": utcnow().isoformat(),
                        }
                    )
                logger.warning("Ingest reconciler loop failed: %s", exc)

            elapsed = time.monotonic() - started_at
            self.metrics.record_business_metric("ingest_reconciler_duration_seconds", elapsed)
            if result and (result.get("published") or result.get("aborted") or result.get("committed_tx")):
                logger.info("Ingest reconciler ran: %s (duration=%.2fs)", result, elapsed)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.poll_interval_seconds)
            except asyncio.TimeoutError:
                continue


SERVICE_INFO = ServiceInfo(
    name="ingest-reconciler-worker",
    title="Dataset Ingest Reconciler",
    description="Reconciles ingest atomicity and emits operational metrics.",
    port=int(os.getenv("INGEST_RECONCILER_PORT", "8012")),
    host="0.0.0.0",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker = IngestReconcilerWorker()
    await worker.initialize()
    stop_event = asyncio.Event()
    task = asyncio.create_task(worker.run(stop_event))
    app.state.reconciler_worker = worker
    app.state.reconciler_stop = stop_event
    app.state.reconciler_task = task
    try:
        yield
    finally:
        stop_event.set()
        try:
            await task
        except Exception:
            logger.warning("Ingest reconciler shutdown failed", exc_info=True)
        await worker.close()


app = create_fastapi_service(
    service_info=SERVICE_INFO,
    custom_lifespan=lifespan,
    include_health_check=True,
    include_logging_middleware=True,
)


def main() -> None:
    import uvicorn

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    uvicorn.run(
        "ingest_reconciler_worker.main:app",
        host="0.0.0.0",
        port=SERVICE_INFO.port,
        log_level=os.getenv("LOG_LEVEL", "INFO").lower(),
    )


if __name__ == "__main__":
    main()
