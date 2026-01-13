"""
Pipeline Scheduler Service.

Polls pipeline registry for scheduled pipelines and enqueues jobs.
"""

from __future__ import annotations

import asyncio
import logging

from shared.config.settings import get_settings
from shared.observability.logging import install_trace_context_filter
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.pipeline_job_queue import PipelineJobQueue
from shared.services.pipeline_registry import PipelineRegistry
from shared.services.pipeline_scheduler import PipelineScheduler


async def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.observability.log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - trace_id=%(trace_id)s span_id=%(span_id)s req_id=%(request_id)s corr_id=%(correlation_id)s db=%(db_name)s - %(message)s",
    )
    install_trace_context_filter()
    tracing = get_tracing_service("pipeline-scheduler")
    metrics = get_metrics_collector("pipeline-scheduler")
    registry = PipelineRegistry()
    await registry.initialize()
    queue = PipelineJobQueue()
    poll_seconds = settings.pipeline.scheduler_poll_seconds
    scheduler = PipelineScheduler(registry, queue, poll_seconds=poll_seconds, tracing=tracing, metrics=metrics)
    await scheduler.run()


if __name__ == "__main__":
    asyncio.run(main())
