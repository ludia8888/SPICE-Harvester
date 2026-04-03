"""
Pipeline detail domain logic (BFF).

Extracted from `bff.routers.pipeline_detail` to keep routers thin.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.pipeline_ops import (
    _augment_definition_with_canonical_contract,
    _augment_definition_with_casts,
    _definition_diff,
    _format_dependencies_for_api,
    _normalize_dependencies_payload,
    _normalize_location,
    _resolve_pipeline_protected_branches,
    _validate_dependency_targets,
)
from bff.routers.pipeline_ops_preflight import _validate_pipeline_definition
from bff.routers.pipeline_shared import _ensure_pipeline_permission, _log_pipeline_audit
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.pipeline.pipeline_dataset_utils import (
    normalize_dataset_selection,
    resolve_dataset_version,
    resolve_fallback_branches,
)
from shared.services.pipeline.pipeline_scheduler import _is_valid_cron_expression
from shared.services.core.write_path_contract import (
    build_write_path_contract,
    followup_completed,
    followup_degraded,
    followup_skipped,
)
from shared.utils.event_utils import build_command_event
from shared.observability.tracing import trace_db_operation

logger = logging.getLogger(__name__)


@trace_db_operation("bff.pipeline_detail.get_pipeline")
async def get_pipeline(
    *,
    pipeline_id: str,
    pipeline_registry: Any,
    branch: Optional[str],
    preview_node_id: Optional[str],
    request: Request | Any = None,
) -> Dict[str, Any]:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.PIPELINE_NOT_FOUND)
        version = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=branch or pipeline.branch,
        )
        dependencies = await pipeline_registry.list_dependencies(pipeline_id=pipeline_id)
        dependencies_for_api = _format_dependencies_for_api(dependencies)
        definition_payload: Dict[str, Any] = version.definition_json if version else {}
        if not isinstance(definition_payload, dict):
            definition_payload = {}
        if dependencies_for_api:
            definition_payload = {**definition_payload, "dependencies": dependencies_for_api}
        pipeline_payload = {
            **pipeline.__dict__,
            "definition_json": definition_payload,
            "version_id": version.version_id if version else None,
            "version": version.lakefs_commit_id if version else None,
            "commit_id": version.lakefs_commit_id if version else None,
            "dependencies": dependencies_for_api,
        }
        if preview_node_id:
            preview_nodes = pipeline.last_preview_nodes or {}
            if preview_node_id in preview_nodes:
                pipeline_payload["last_preview_sample"] = preview_nodes.get(preview_node_id)
                pipeline_payload["last_preview_node_id"] = preview_node_id
        return ApiResponse.success(
            message="Pipeline fetched",
            data={"pipeline": pipeline_payload},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to fetch pipeline: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_db_operation("bff.pipeline_detail.get_pipeline_readiness")
async def get_pipeline_readiness(
    *,
    pipeline_id: str,
    branch: Optional[str],
    pipeline_registry: Any,
    dataset_registry: Any,
    request: Request | Any = None,
) -> Dict[str, Any]:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.PIPELINE_NOT_FOUND)

        resolved_branch = (branch or pipeline.branch or "main").strip() or "main"
        version = await pipeline_registry.get_latest_version(pipeline_id=pipeline_id, branch=resolved_branch)
        definition = version.definition_json if version else {}
        if not isinstance(definition, dict):
            definition = {}
        nodes = definition.get("nodes") or []
        if not isinstance(nodes, list):
            nodes = []

        fallback_candidates = resolve_fallback_branches()

        inputs: list[dict[str, Any]] = []
        overall_blocked = False
        overall_pending = False

        for node in nodes:
            if not isinstance(node, dict):
                continue
            if str(node.get("type") or "") != "input":
                continue
            node_id = str(node.get("id") or "").strip() or None
            metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
            selection = normalize_dataset_selection(metadata, default_branch=resolved_branch)
            if not selection.dataset_id and not selection.dataset_name:
                read_cfg = metadata.get("read") if isinstance(metadata.get("read"), dict) else {}
                fmt = str(
                    read_cfg.get("format") or read_cfg.get("file_format") or read_cfg.get("fileFormat") or ""
                ).strip().lower()
                if fmt:
                    options = read_cfg.get("options") if isinstance(read_cfg.get("options"), dict) else {}

                    def _opt(key: str) -> str:
                        return str(options.get(key) or "").strip()

                    if fmt == "jdbc":
                        if not _opt("url") or (not _opt("dbtable") and not _opt("query")):
                            overall_blocked = True
                            inputs.append(
                                {
                                    "node_id": node_id,
                                    "status": "INVALID_CONFIG",
                                    "detail": "external JDBC input requires read.options.url and read.options.dbtable (or read.options.query)",
                                    "source_type": "external",
                                    "format": fmt,
                                }
                            )
                            continue
                    if fmt == "kafka":
                        bootstrap = _opt("kafka.bootstrap.servers")
                        topic_spec = _opt("subscribe") or _opt("subscribePattern") or _opt("assign")
                        if not bootstrap or not topic_spec:
                            overall_blocked = True
                            inputs.append(
                                {
                                    "node_id": node_id,
                                    "status": "INVALID_CONFIG",
                                    "detail": "external Kafka input requires read.options.kafka.bootstrap.servers and one of subscribe/subscribePattern/assign",
                                    "source_type": "external",
                                    "format": fmt,
                                }
                            )
                            continue

                    inputs.append(
                        {
                            "node_id": node_id,
                            "status": "READY",
                            "source_type": "external",
                            "format": fmt,
                        }
                    )
                    continue

                overall_blocked = True
                inputs.append(
                    {
                        "node_id": node_id,
                        "status": "INVALID_CONFIG",
                        "detail": "datasetId/datasetName or metadata.read.format is required",
                    }
                )
                continue
            resolution = await resolve_dataset_version(
                dataset_registry,
                db_name=pipeline.db_name,
                selection=selection,
            )
            dataset = resolution.dataset
            version_rec = resolution.version
            resolved_dataset_branch = resolution.resolved_branch or (dataset.branch if dataset else None)

            if not dataset:
                overall_blocked = True
                inputs.append(
                    {
                        "node_id": node_id,
                        "dataset_id": selection.dataset_id,
                        "dataset_name": selection.dataset_name,
                        "requested_branch": selection.requested_branch,
                        "status": "MISSING_DATASET",
                        "detail": "dataset not found in requested/fallback branches",
                    }
                )
                continue

            if not version_rec:
                overall_pending = True
                inputs.append(
                    {
                        "node_id": node_id,
                        "dataset_id": dataset.dataset_id,
                        "dataset_name": dataset.name,
                        "requested_branch": selection.requested_branch,
                        "resolved_branch": resolved_dataset_branch or dataset.branch,
                        "status": "NO_VERSIONS",
                        "detail": "dataset has no versions in requested/fallback branches",
                    }
                )
                continue

            inputs.append(
                {
                    "node_id": node_id,
                    "dataset_id": dataset.dataset_id,
                    "dataset_name": dataset.name,
                    "requested_branch": selection.requested_branch,
                    "resolved_branch": resolved_dataset_branch or dataset.branch,
                    "used_fallback": resolution.used_fallback,
                    "status": "READY",
                    "latest_commit_id": version_rec.lakefs_commit_id,
                    "latest_version": version_rec.lakefs_commit_id,
                    "artifact_key": version_rec.artifact_key,
                }
            )

        overall_status = "READY_FOR_PREVIEW"
        if overall_blocked:
            overall_status = "BLOCKED"
        elif overall_pending:
            overall_status = "PENDING"

        return ApiResponse.success(
            message="Pipeline readiness fetched",
            data={
                "pipeline_id": pipeline_id,
                "branch": resolved_branch,
                "version_id": version.version_id if version else None,
                "commit_id": version.lakefs_commit_id if version else None,
                "version": version.lakefs_commit_id if version else None,
                "status": overall_status,
                "inputs": inputs,
                "fallback_branches": fallback_candidates,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to fetch pipeline readiness: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_db_operation("bff.pipeline_detail.update_pipeline")
async def update_pipeline(
    *,
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: Any,
    pipeline_registry: Any,
    dataset_registry: Any,
    request: Request | Any = None,
    event_store: Any,
) -> Dict[str, Any]:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        sanitized = sanitize_input(payload)
        definition_json = sanitized.get("definition_json") if isinstance(sanitized, dict) else None
        if definition_json is not None and not isinstance(definition_json, dict):
            definition_json = {}

        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.PIPELINE_NOT_FOUND)
        previous_version = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=pipeline.branch,
        )
        previous_definition = previous_version.definition_json if previous_version else {}
        branch_state = await pipeline_registry.get_pipeline_branch(db_name=pipeline.db_name, branch=pipeline.branch)
        if branch_state and branch_state.get("archived"):
            raise classified_http_exception(status.HTTP_409_CONFLICT, "Archived branch cannot be updated; restore the branch before making changes", code=ErrorCode.CONFLICT)
        if pipeline.branch in _resolve_pipeline_protected_branches() and definition_json is not None:
            settings = get_settings()
            state = getattr(request, "state", None)
            dev_bypass = bool(getattr(state, "dev_master_auth", False)) if request is not None else False
            if settings.is_development and settings.auth.dev_master_auth_enabled and dev_bypass:
                pass
            else:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Protected branch cannot be updated directly; create a branch and submit a proposal", code=ErrorCode.CONFLICT)
        db_name = str(pipeline.db_name)

        dependencies_raw: Any = None
        if "dependencies" in sanitized:
            dependencies_raw = sanitized.get("dependencies")
        elif isinstance(definition_json, dict) and "dependencies" in definition_json:
            dependencies_raw = definition_json.get("dependencies")
        dependencies: Optional[list[dict[str, str]]] = None
        if dependencies_raw is not None:
            dependencies = _normalize_dependencies_payload(dependencies_raw)
            await _validate_dependency_targets(
                pipeline_registry,
                db_name=db_name,
                pipeline_id=pipeline_id,
                dependencies=dependencies,
            )
            if isinstance(definition_json, dict):
                definition_json = {**definition_json}
                definition_json.pop("dependencies", None)
        schedule_interval_seconds = sanitized.get("schedule_interval_seconds")
        schedule_cron = sanitized.get("schedule_cron")
        schedule = sanitized.get("schedule") if isinstance(sanitized.get("schedule"), dict) else None
        proposal_submitted_at = sanitized.get("proposal_submitted_at")
        proposal_reviewed_at = sanitized.get("proposal_reviewed_at")
        proposal_review_comment = str(sanitized.get("proposal_review_comment") or "").strip() or None
        branch = str(sanitized.get("branch") or "").strip() or None

        if schedule_interval_seconds is not None:
            try:
                schedule_interval_seconds = int(schedule_interval_seconds)
            except Exception as exc:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule_interval_seconds must be integer", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
        if schedule_interval_seconds is not None and schedule_interval_seconds < 0:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule_interval_seconds must be non-negative", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        if schedule_cron is not None:
            schedule_cron = str(schedule_cron).strip() or None
        if schedule_cron and not _is_valid_cron_expression(schedule_cron):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule_cron must be a valid cron expression", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        if schedule:
            schedule_cron = str(schedule.get("cron") or "").strip() or None
            schedule_interval_seconds = schedule.get("interval_seconds")
            if schedule_interval_seconds is not None:
                try:
                    schedule_interval_seconds = int(schedule_interval_seconds)
                except Exception as exc:
                    raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule.interval_seconds must be integer", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
            if schedule_cron and not _is_valid_cron_expression(schedule_cron):
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule.cron must be a valid cron expression", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        if proposal_submitted_at is not None:
            proposal_submitted_at = str(proposal_submitted_at).strip() or None
        if proposal_reviewed_at is not None:
            proposal_reviewed_at = str(proposal_reviewed_at).strip() or None

        sampling_strategy = sanitized.get("sampling_strategy") or sanitized.get("samplingStrategy")
        if sampling_strategy is not None and not isinstance(sampling_strategy, dict):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "sampling_strategy must be an object", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        expectations = sanitized.get("expectations") if isinstance(sanitized.get("expectations"), list) else None
        schema_contract = sanitized.get("schema_contract") if isinstance(sanitized.get("schema_contract"), list) else None
        if expectations is not None:
            definition_json = {**definition_json, "expectations": expectations}
        if schema_contract is not None:
            definition_json = {**definition_json, "schemaContract": schema_contract}

        if isinstance(definition_json, dict) and definition_json:
            definition_json = await _augment_definition_with_casts(
                definition_json=definition_json,
                db_name=db_name,
                branch=branch or pipeline.branch,
                dataset_registry=dataset_registry,
            )
            definition_json = _augment_definition_with_canonical_contract(
                definition_json=definition_json,
                branch=branch or pipeline.branch,
            )
            validation_errors = _validate_pipeline_definition(
                definition_json=definition_json,
                require_output=False,
            )
            if validation_errors:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Pipeline definition invalid",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                    extra={"errors": validation_errors},
                )

        update = await pipeline_registry.update_pipeline(
            pipeline_id=pipeline_id,
            name=str(sanitized.get("name") or "").strip() or None,
            description=str(sanitized.get("description") or "").strip() or None,
            location=(_normalize_location(str(sanitized.get("location") or "")) if "location" in sanitized else None),
            status=str(sanitized.get("status") or "").strip() or None,
            schedule_interval_seconds=schedule_interval_seconds,
            schedule_cron=schedule_cron,
            branch=branch,
            proposal_status=str(sanitized.get("proposal_status") or "").strip() or None,
            proposal_title=str(sanitized.get("proposal_title") or "").strip() or None,
            proposal_description=str(sanitized.get("proposal_description") or "").strip() or None,
            proposal_submitted_at=proposal_submitted_at,
            proposal_reviewed_at=proposal_reviewed_at,
            proposal_review_comment=proposal_review_comment,
        )
        version = None
        if definition_json is not None:
            version = await pipeline_registry.add_version(
                pipeline_id=pipeline_id,
                branch=branch or update.branch,
                definition_json=definition_json,
            )
        write_path_followups: list[dict[str, Any]] = [
            followup_completed(
                "pipeline_version_commit",
                details={"commit_id": version.lakefs_commit_id if version else None},
            )
            if version is not None
            else followup_skipped(
                "pipeline_version_commit",
                details={"reason": "definition_unchanged"},
            )
        ]
        if dependencies is not None:
            await pipeline_registry.replace_dependencies(pipeline_id=pipeline_id, dependencies=dependencies)
            write_path_followups.append(
                followup_completed(
                    "pipeline_dependencies",
                    details={"count": len(dependencies)},
                )
            )
        else:
            write_path_followups.append(
                followup_skipped(
                    "pipeline_dependencies",
                    details={"reason": "not_provided"},
                )
            )
        dependencies_payload = await pipeline_registry.list_dependencies(pipeline_id=pipeline_id)
        dependencies_for_api = _format_dependencies_for_api(dependencies_payload)
        if definition_json is not None and dependencies_for_api:
            definition_json = {**definition_json, "dependencies": dependencies_for_api}

        if definition_json is not None:
            diff_payload = _definition_diff(previous_definition, definition_json)
        else:
            diff_payload = {}

        event = build_command_event(
            event_type="PIPELINE_UPDATED",
            aggregate_type="Pipeline",
            aggregate_id=pipeline_id,
            data={
                "pipeline_id": pipeline_id,
                "definition_json": definition_json,
                "version_id": version.version_id if version else None,
                "commit_id": version.lakefs_commit_id if version else None,
                "version": version.lakefs_commit_id if version else None,
                "branch": branch or update.branch,
            },
            command_type="UPDATE_PIPELINE",
        )
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
        event_followup: dict[str, Any]
        try:
            await event_store.connect()
            await event_store.append_event(event)
            event_followup = followup_completed(
                "pipeline_updated_event",
                details={"topic": AppConfig.PIPELINE_EVENTS_TOPIC},
            )
        except Exception as exc:
            logger.warning("Failed to append pipeline update event: %s", exc)
            event_followup = followup_degraded(
                "pipeline_updated_event",
                error=str(exc),
                details={"topic": AppConfig.PIPELINE_EVENTS_TOPIC},
            )
        write_path_followups.append(event_followup)

        contract = build_write_path_contract(
            authoritative_write="pipeline_update",
            followups=[
                *write_path_followups,
                followup_completed("pipeline_audit", details={"action": "PIPELINE_UPDATED"}),
            ],
        )

        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_UPDATED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={
                "branch": branch or update.branch,
                "commit_id": version.lakefs_commit_id if version else None,
                "definition_diff": diff_payload,
                "write_path_contract": contract,
            },
        )

        return ApiResponse.success(
            message="Pipeline updated",
            data={
                "pipeline": {
                    **update.__dict__,
                    "definition_json": definition_json if definition_json is not None else None,
                    "version_id": version.version_id if version else None,
                    "commit_id": version.lakefs_commit_id if version else None,
                    "version": version.lakefs_commit_id if version else None,
                    "dependencies": dependencies_for_api,
                }
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_UPDATED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(exc),
        )
        logger.error("Failed to update pipeline: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
