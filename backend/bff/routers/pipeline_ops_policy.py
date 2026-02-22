"""Pipeline Builder governance helpers.

Small, stable helpers extracted from `bff.routers.pipeline_ops`.
"""


from shared.config.settings import get_settings


def _resolve_pipeline_protected_branches() -> set[str]:
    return get_settings().pipeline.protected_branches_set


def _pipeline_requires_proposal(branch: str) -> bool:
    settings = get_settings()
    if not settings.pipeline.require_proposals:
        return False
    resolved = (branch or "").strip() or "master"
    return resolved in settings.pipeline.protected_branches_set
