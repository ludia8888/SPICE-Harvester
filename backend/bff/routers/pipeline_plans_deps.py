
from bff.routers.registry_deps import get_agent_policy_registry
from shared.services.registries.dataset_profile_registry import DatasetProfileRegistry
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry


async def get_dataset_profile_registry() -> DatasetProfileRegistry:
    from bff.main import get_dataset_profile_registry as _get_dataset_profile_registry

    return await _get_dataset_profile_registry()


async def get_pipeline_plan_registry() -> PipelinePlanRegistry:
    from bff.main import get_pipeline_plan_registry as _get_pipeline_plan_registry

    return await _get_pipeline_plan_registry()
