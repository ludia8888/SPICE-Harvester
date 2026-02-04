"""
Shared BFF registry dependency providers.

This module is the Single Source of Truth for BFF registry dependencies that are
used across multiple routers. Centralizing them removes duplicated wrappers like:
`from bff.main import get_dataset_registry as _get_dataset_registry`.

Routers that previously defined local `get_*_registry()` helpers should import
these functions (or re-export them) to keep code DRY and consistent.
"""

from __future__ import annotations

from shared.services.registries.agent_policy_registry import AgentPolicyRegistry
from shared.services.registries.agent_session_registry import AgentSessionRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def get_objectify_registry() -> ObjectifyRegistry:
    from bff.main import get_objectify_registry as _get_objectify_registry

    return await _get_objectify_registry()


async def get_pipeline_registry() -> PipelineRegistry:
    from bff.main import get_pipeline_registry as _get_pipeline_registry

    return await _get_pipeline_registry()


async def get_agent_policy_registry() -> AgentPolicyRegistry:
    from bff.main import get_agent_policy_registry as _get_agent_policy_registry

    return await _get_agent_policy_registry()


async def get_agent_session_registry() -> AgentSessionRegistry:
    from bff.main import get_agent_session_registry as _get_agent_session_registry

    return await _get_agent_session_registry()
