"""Pipeline Builder helper operations.

Composition root for pipeline helper modules.

Keeps a stable import surface for subrouters/tests while delegating
implementation details to smaller helpers (Composite + Facade).
"""

from __future__ import annotations

from bff.routers.pipeline_ops_augmentation import (
    _augment_definition_with_canonical_contract,
    _augment_definition_with_casts,
)
from bff.routers.pipeline_ops_definition import (
    _definition_diff,
    _normalize_location,
    _resolve_definition_commit_id,
    _stable_definition_hash,
)
from bff.routers.pipeline_ops_dependencies import (
    _format_dependencies_for_api,
    _normalize_dependencies_payload,
    _validate_dependency_targets,
)
from bff.routers.pipeline_ops_locks import _acquire_pipeline_publish_lock, _release_pipeline_publish_lock
from bff.routers.pipeline_ops_policy import _pipeline_requires_proposal, _resolve_pipeline_protected_branches
from bff.routers.pipeline_ops_preflight import _run_pipeline_preflight, _validate_pipeline_definition
from bff.routers.pipeline_ops_schema import _detect_breaking_schema_changes, _resolve_output_pk_columns

__all__ = [
    "_acquire_pipeline_publish_lock",
    "_augment_definition_with_canonical_contract",
    "_augment_definition_with_casts",
    "_definition_diff",
    "_detect_breaking_schema_changes",
    "_format_dependencies_for_api",
    "_normalize_dependencies_payload",
    "_normalize_location",
    "_pipeline_requires_proposal",
    "_release_pipeline_publish_lock",
    "_resolve_definition_commit_id",
    "_resolve_output_pk_columns",
    "_resolve_pipeline_protected_branches",
    "_run_pipeline_preflight",
    "_stable_definition_hash",
    "_validate_dependency_targets",
    "_validate_pipeline_definition",
]
