"""
Pipeline Builder dependency providers.

This module supports router composition (Composite pattern) by centralizing
shared FastAPI dependencies (registries/clients) used across pipeline subrouters.
"""


from bff.routers.registry_deps import get_dataset_registry, get_objectify_registry, get_pipeline_registry

from shared.services.pipeline.pipeline_executor import PipelineExecutor
from shared.services.pipeline.pipeline_job_queue import PipelineJobQueue


async def get_pipeline_executor() -> PipelineExecutor:
    from bff.main import get_pipeline_executor as _get_pipeline_executor

    return await _get_pipeline_executor()


async def get_pipeline_job_queue() -> PipelineJobQueue:
    return PipelineJobQueue()
