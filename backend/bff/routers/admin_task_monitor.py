"""
Admin background-task monitoring helpers.

Admin endpoints typically start long-running background tasks and return a task_id
immediately. This module provides best-effort monitoring that logs completion
without blocking request handling.
"""


import asyncio
import logging

from shared.models.background_task import TaskStatus
from shared.services.core.background_task_manager import BackgroundTaskManager

logger = logging.getLogger(__name__)


async def monitor_admin_task(*, task_id: str, task_manager: BackgroundTaskManager) -> None:
    await asyncio.sleep(1)

    max_checks = 60
    check_interval = 5

    for _ in range(max_checks):
        task = await task_manager.get_task_status(task_id)
        if task and task.is_complete:
            if task.status == TaskStatus.COMPLETED:
                logger.info("Admin task %s completed successfully", task_id)
            else:
                logger.error(
                    "Admin task %s failed: %s",
                    task_id,
                    task.result.error if task.result else "Unknown error",
                )
            return
        await asyncio.sleep(check_interval)

    logger.warning("Admin task %s did not complete within monitoring window", task_id)

