"""
Pipeline Scheduler Service.

Polls pipeline registry for scheduled pipelines and enqueues jobs.
"""

from __future__ import annotations

import asyncio
import logging

from shared.config.settings import get_settings
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.pipeline.pipeline_job_queue import PipelineJobQueue
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.pipeline.pipeline_scheduler import PipelineScheduler
from shared.utils.app_logger import configure_logging


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.observability.log_level)
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
