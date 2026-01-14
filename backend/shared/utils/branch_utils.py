from __future__ import annotations

from typing import Iterable, Set

from shared.config.settings import get_settings

PROTECTED_BRANCH_WRITE_MESSAGE = (
    "Protected branch. Use proposal workflow for changes."
)


def protected_branch_write_message() -> str:
    return PROTECTED_BRANCH_WRITE_MESSAGE


def get_protected_branches(
    *,
    env_key: str = "ONTOLOGY_PROTECTED_BRANCHES",
    defaults: Iterable[str] = ("main", "master", "production", "prod"),
) -> Set[str]:
    settings = get_settings()
    if env_key == "ONTOLOGY_PROTECTED_BRANCHES":
        branches = set(settings.ontology.protected_branches_set)
        return branches or set(defaults)
    if env_key == "PIPELINE_PROTECTED_BRANCHES":
        branches = set(settings.pipeline.protected_branches_set)
        return branches or set(defaults)
    # Unknown key: fall back to provided defaults (avoid distributed env access).
    return set(defaults)
