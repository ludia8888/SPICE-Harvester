"""
Pipeline Scheduler Service.

Polls pipeline registry for scheduled pipelines and enqueues jobs.
"""

from __future__ import annotations

import asyncio
import logging
import os

from shared.services.pipeline_job_queue import PipelineJobQueue
from shared.services.pipeline_registry import PipelineRegistry
from shared.services.pipeline_scheduler import PipelineScheduler


async def main() -> None:
    logging.basicConfig(level="INFO")
    registry = PipelineRegistry()
    await registry.initialize()
    queue = PipelineJobQueue()
    poll_seconds = int(os.getenv("PIPELINE_SCHEDULER_POLL_SECONDS", "30"))
    scheduler = PipelineScheduler(registry, queue, poll_seconds=poll_seconds)
    await scheduler.run()


if __name__ == "__main__":
    asyncio.run(main())
