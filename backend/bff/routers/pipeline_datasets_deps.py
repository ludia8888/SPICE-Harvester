"""
Pipeline datasets dependencies (BFF).

Centralizes FastAPI dependency providers for dataset subrouters.
"""


from fastapi import Depends

from bff.routers.pipeline_deps import get_objectify_registry
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.objectify_registry import ObjectifyRegistry


async def get_objectify_job_queue(
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ObjectifyJobQueue:
    return ObjectifyJobQueue(objectify_registry=objectify_registry)

