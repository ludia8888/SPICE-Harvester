"""Pipeline Builder preflight + definition validation.

Extracted from `bff.routers.pipeline_ops` to keep helpers cohesive.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from shared.services.pipeline.pipeline_definition_validator import (
    PipelineDefinitionValidationPolicy,
    validate_pipeline_definition,
)
from shared.services.pipeline.pipeline_preflight_utils import compute_pipeline_preflight
from shared.services.pipeline.pipeline_transform_spec import SUPPORTED_TRANSFORMS
from shared.services.registries.dataset_registry import DatasetRegistry

logger = logging.getLogger(__name__)


async def _run_pipeline_preflight(
    *,
    definition_json: Dict[str, Any],
    db_name: str,
    branch: Optional[str],
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    try:
        return await compute_pipeline_preflight(
            definition=definition_json,
            db_name=db_name,
            dataset_registry=dataset_registry,
            branch=branch,
        )
    except Exception as exc:
        logger.warning("Pipeline preflight failed: %s", exc)
        return {
            "issues": [
                {
                    "kind": "preflight_error",
                    "severity": "warning",
                    "message": f"preflight failed: {exc}",
                }
            ],
            "blocking_errors": [],
            "has_blocking_errors": False,
        }


def _validate_pipeline_definition(*, definition_json: Dict[str, Any], require_output: bool = True) -> list[str]:
    policy = PipelineDefinitionValidationPolicy(
        supported_ops=SUPPORTED_TRANSFORMS - {"udf"},
        require_output=require_output,
        normalize_metadata=True,
        udf_error_message_template=(
            "Unsupported operation '{operation}' on node {node_id} (Spark execution does not support udf yet)."
        ),
    )
    return validate_pipeline_definition(definition_json, policy=policy).errors
