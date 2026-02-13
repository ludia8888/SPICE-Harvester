"""Pipeline Builder preflight + definition validation.

Extracted from `bff.routers.pipeline_ops` to keep helpers cohesive.
"""


import logging
from typing import Any, Dict, Optional

from fastapi import status

from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.services.pipeline.pipeline_definition_validator import (
    PipelineDefinitionValidationPolicy,
    validate_pipeline_definition,
)
from shared.services.pipeline.pipeline_preflight_utils import compute_pipeline_preflight
from shared.services.pipeline.pipeline_transform_spec import SUPPORTED_TRANSFORMS
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

logger = logging.getLogger(__name__)


def _iter_udf_nodes(definition_json: Dict[str, Any]) -> list[dict[str, Any]]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list):
        return []
    output: list[dict[str, Any]] = []
    for node in nodes_raw:
        if not isinstance(node, dict):
            continue
        node_type = str(node.get("type") or "").strip().lower()
        if node_type != "transform":
            continue
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        if str(metadata.get("operation") or "").strip() != "udf":
            continue
        output.append(
            {
                "node_id": str(node.get("id") or "").strip(),
                "udf_id": str(metadata.get("udfId") or metadata.get("udf_id") or "").strip(),
                "udf_version_raw": metadata.get("udfVersion") or metadata.get("udf_version"),
            }
        )
    return output


def _udf_issue(*, kind: str, node_id: str, message: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "severity": "error",
        "node_id": node_id,
        "message": message,
    }


async def _validate_udf_preflight_references(
    *,
    definition_json: Dict[str, Any],
    db_name: str,
    pipeline_registry: Optional[PipelineRegistry],
) -> list[dict[str, Any]]:
    settings = get_settings()
    pipeline_settings = getattr(settings, "pipeline", settings)
    require_reference = bool(getattr(pipeline_settings, "udf_require_reference", True))
    require_version_pinning = bool(getattr(pipeline_settings, "udf_require_version_pinning", True))
    require_existence = bool(getattr(pipeline_settings, "udf_preflight_require_existence", True))

    udf_nodes = _iter_udf_nodes(definition_json)
    if not udf_nodes:
        return []

    issues: list[dict[str, Any]] = []
    udf_cache: dict[str, Any] = {}

    for entry in udf_nodes:
        node_id = str(entry.get("node_id") or "").strip() or "unknown"
        udf_id = str(entry.get("udf_id") or "").strip()
        udf_version_raw = entry.get("udf_version_raw")

        if require_reference and not udf_id:
            issues.append(
                _udf_issue(
                    kind="udf_reference_missing",
                    node_id=node_id,
                    message="UDF preflight failed: udfId is required",
                )
            )
            continue

        parsed_version: Optional[int] = None
        has_version = udf_version_raw is not None and str(udf_version_raw).strip() != ""
        if has_version:
            try:
                parsed_version = int(str(udf_version_raw).strip())
            except (TypeError, ValueError):
                issues.append(
                    _udf_issue(
                        kind="udf_version_invalid",
                        node_id=node_id,
                        message="UDF preflight failed: udfVersion must be an integer",
                    )
                )
                continue
            if parsed_version <= 0:
                issues.append(
                    _udf_issue(
                        kind="udf_version_invalid",
                        node_id=node_id,
                        message="UDF preflight failed: udfVersion must be >= 1",
                    )
                )
                continue

        if require_version_pinning and parsed_version is None:
            issues.append(
                _udf_issue(
                    kind="udf_version_missing",
                    node_id=node_id,
                    message="UDF preflight failed: udfVersion pinning is required",
                )
            )
            continue

        if not require_existence:
            continue
        if not udf_id:
            continue
        if pipeline_registry is None:
            issues.append(
                _udf_issue(
                    kind="udf_registry_unavailable",
                    node_id=node_id,
                    message="UDF preflight failed: pipeline registry is required for UDF existence checks",
                )
            )
            continue

        if udf_id not in udf_cache:
            udf_cache[udf_id] = await pipeline_registry.get_udf(udf_id=udf_id)
        udf_record = udf_cache.get(udf_id)
        if not udf_record:
            issues.append(
                _udf_issue(
                    kind="udf_reference_not_found",
                    node_id=node_id,
                    message=f"UDF preflight failed: udfId '{udf_id}' does not exist",
                )
            )
            continue

        udf_db_name = str(getattr(udf_record, "db_name", "") or "").strip()
        if udf_db_name and str(db_name or "").strip() and udf_db_name != str(db_name or "").strip():
            issues.append(
                _udf_issue(
                    kind="udf_reference_scope_mismatch",
                    node_id=node_id,
                    message=(
                        f"UDF preflight failed: udfId '{udf_id}' belongs to db '{udf_db_name}', "
                        f"not '{db_name}'"
                    ),
                )
            )
            continue

        if parsed_version is not None:
            udf_version = await pipeline_registry.get_udf_version(udf_id=udf_id, version=parsed_version)
            if not udf_version:
                issues.append(
                    _udf_issue(
                        kind="udf_version_not_found",
                        node_id=node_id,
                        message=f"UDF preflight failed: udfId '{udf_id}' version '{parsed_version}' does not exist",
                    )
                )
        else:
            latest = await pipeline_registry.get_udf_latest_version(udf_id=udf_id)
            if not latest:
                issues.append(
                    _udf_issue(
                        kind="udf_version_not_found",
                        node_id=node_id,
                        message=f"UDF preflight failed: udfId '{udf_id}' has no published versions",
                    )
                )
    return issues


async def _run_pipeline_preflight(
    *,
    definition_json: Dict[str, Any],
    db_name: str,
    branch: Optional[str],
    dataset_registry: DatasetRegistry,
    pipeline_registry: Optional[PipelineRegistry] = None,
) -> Dict[str, Any]:
    settings = get_settings()
    fail_closed = bool(settings.pipeline.preflight_fail_closed)
    try:
        preflight = await compute_pipeline_preflight(
            definition=definition_json,
            db_name=db_name,
            dataset_registry=dataset_registry,
            branch=branch,
        )
        udf_issues = await _validate_udf_preflight_references(
            definition_json=definition_json,
            db_name=db_name,
            pipeline_registry=pipeline_registry,
        )
        if udf_issues:
            issues = list(preflight.get("issues") or [])
            issues.extend(udf_issues)
            blocking_errors = list(preflight.get("blocking_errors") or [])
            blocking_errors.extend(issue for issue in udf_issues if str(issue.get("severity") or "").lower() == "error")
            preflight["issues"] = issues
            preflight["blocking_errors"] = blocking_errors
            preflight["has_blocking_errors"] = bool(blocking_errors)
        return preflight
    except Exception as exc:
        logger.warning("Pipeline preflight failed: %s", exc, exc_info=True)
        if fail_closed:
            raise classified_http_exception(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Pipeline preflight failed",
                code=ErrorCode.INTERNAL_ERROR,
                category=ErrorCategory.INTERNAL,
                external_code="PIPELINE_PREFLIGHT_FAILED",
                extra={"stage": "preflight", "reason": str(exc)},
            ) from exc
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
    settings = get_settings()
    pipeline_settings = getattr(settings, "pipeline", settings)
    policy = PipelineDefinitionValidationPolicy(
        supported_ops=SUPPORTED_TRANSFORMS,
        require_output=require_output,
        normalize_metadata=True,
        require_udf_reference=bool(getattr(pipeline_settings, "udf_require_reference", True)),
        require_udf_version_pinning=bool(getattr(pipeline_settings, "udf_require_version_pinning", True)),
    )
    return validate_pipeline_definition(definition_json, policy=policy).errors
