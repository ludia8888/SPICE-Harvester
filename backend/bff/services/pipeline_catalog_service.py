"""Pipeline catalog domain logic (BFF).

Extracted from `bff.routers.pipeline_catalog` to keep routers thin.
"""

import logging
from typing import Any, Optional
from uuid import UUID

from fastapi import HTTPException, status

from bff.routers.pipeline_ops import (
    _augment_definition_with_canonical_contract,
    _augment_definition_with_casts,
    _format_dependencies_for_api,
    _normalize_dependencies_payload,
    _normalize_location,
    _validate_dependency_targets,
)
from bff.routers.pipeline_shared import (
    _filter_pipeline_records_for_read_access,
    _log_pipeline_audit,
    _require_pipeline_idempotency_key,
    _resolve_principal,
)
from shared.config.app_config import AppConfig
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.pipeline.pipeline_scheduler import _is_valid_cron_expression
from shared.services.registries.pipeline_registry import PipelineAlreadyExistsError
from shared.utils.event_utils import build_command_event

logger = logging.getLogger(__name__)


async def list_pipelines(
    *,
    db_name: str,
    branch: Optional[str],
    pipeline_registry: Any,
    request: Any,
) -> dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        pipelines = await pipeline_registry.list_pipelines(db_name=db_name, branch=branch)
        filtered = await _filter_pipeline_records_for_read_access(
            pipeline_registry,
            records=pipelines,
            request=request,
        )
        return ApiResponse.success(
            message="Pipelines fetched",
            data={"pipelines": filtered, "count": len(filtered)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list pipelines: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def create_pipeline(
    *,
    payload: dict[str, Any],
    audit_store: Any,
    pipeline_registry: Any,
    dataset_registry: Any,
    event_store: Any,
    request: Any,
) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    try:
        _require_pipeline_idempotency_key(request, operation="pipeline create")
        sanitized = sanitize_input(payload)
        pipeline_id: Optional[str] = None
        raw_pipeline_id = sanitized.get("pipeline_id") or sanitized.get("pipelineId")
        if raw_pipeline_id:
            try:
                pipeline_id = str(UUID(str(raw_pipeline_id)))
            except Exception as exc:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="pipeline_id must be a UUID") from exc
        db_name = validate_db_name(str(sanitized.get("db_name") or ""))
        name = str(sanitized.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name is required")
        pipeline_type = str(sanitized.get("pipeline_type") or "batch").strip() or "batch"
        location = _normalize_location(str(sanitized.get("location") or ""))
        description = str(sanitized.get("description") or "").strip() or None
        definition_json = sanitized.get("definition_json") if isinstance(sanitized, dict) else {}
        if not isinstance(definition_json, dict):
            definition_json = {}

        dependencies_raw: Any = None
        if "dependencies" in sanitized:
            dependencies_raw = sanitized.get("dependencies")
        elif "dependencies" in definition_json:
            dependencies_raw = definition_json.get("dependencies")
        dependencies: Optional[list[dict[str, str]]] = None
        if dependencies_raw is not None:
            dependencies = _normalize_dependencies_payload(dependencies_raw)
            await _validate_dependency_targets(
                pipeline_registry,
                db_name=db_name,
                pipeline_id=None,
                dependencies=dependencies,
            )
            definition_json = {**definition_json}
            definition_json.pop("dependencies", None)

        branch = str(sanitized.get("branch") or "main").strip() or "main"
        proposal_status = str(sanitized.get("proposal_status") or "").strip() or None
        proposal_title = str(sanitized.get("proposal_title") or "").strip() or None
        proposal_description = str(sanitized.get("proposal_description") or "").strip() or None
        schedule_interval_seconds = sanitized.get("schedule_interval_seconds")
        schedule_cron = sanitized.get("schedule_cron")
        schedule = sanitized.get("schedule") if isinstance(sanitized.get("schedule"), dict) else None
        proposal_submitted_at = sanitized.get("proposal_submitted_at")
        proposal_reviewed_at = sanitized.get("proposal_reviewed_at")
        proposal_review_comment = sanitized.get("proposal_review_comment")
        if schedule:
            schedule_interval_seconds = schedule.get("interval_seconds", schedule_interval_seconds)
            schedule_cron = schedule.get("cron", schedule_cron)
        if schedule_interval_seconds is not None:
            try:
                schedule_interval_seconds = int(schedule_interval_seconds)
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="schedule_interval_seconds must be integer",
                ) from exc
            if schedule_interval_seconds <= 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="schedule_interval_seconds must be > 0",
                )
        schedule_cron = str(schedule_cron).strip() if schedule_cron else None
        if schedule_interval_seconds and schedule_cron:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Provide either schedule_interval_seconds or schedule_cron (not both)",
            )
        if schedule_cron and not _is_valid_cron_expression(schedule_cron):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="schedule_cron must be a supported 5-field cron expression",
            )
        proposal_submitted_at = None if not proposal_submitted_at else proposal_submitted_at
        proposal_reviewed_at = None if not proposal_reviewed_at else proposal_reviewed_at
        proposal_review_comment = str(proposal_review_comment).strip() if proposal_review_comment else None

        if isinstance(definition_json, dict) and definition_json:
            definition_json = await _augment_definition_with_casts(
                definition_json=definition_json,
                db_name=db_name,
                branch=branch,
                dataset_registry=dataset_registry,
            )
            definition_json = _augment_definition_with_canonical_contract(
                definition_json=definition_json,
                branch=branch,
            )

        record = await pipeline_registry.create_pipeline(
            db_name=db_name,
            name=name,
            description=description,
            pipeline_type=pipeline_type,
            location=location,
            branch=branch,
            proposal_status=proposal_status,
            proposal_title=proposal_title,
            proposal_description=proposal_description,
            proposal_submitted_at=proposal_submitted_at,
            proposal_reviewed_at=proposal_reviewed_at,
            proposal_review_comment=proposal_review_comment,
            schedule_interval_seconds=schedule_interval_seconds,
            schedule_cron=schedule_cron,
            pipeline_id=pipeline_id,
        )
        principal_type, principal_id = _resolve_principal(request)
        await pipeline_registry.grant_permission(
            pipeline_id=record.pipeline_id,
            principal_type=principal_type,
            principal_id=principal_id,
            role="admin",
        )
        version = await pipeline_registry.add_version(
            pipeline_id=record.pipeline_id,
            branch=branch,
            definition_json=definition_json,
        )
        if dependencies is not None:
            await pipeline_registry.replace_dependencies(pipeline_id=record.pipeline_id, dependencies=dependencies)
        dependencies_payload = await pipeline_registry.list_dependencies(pipeline_id=record.pipeline_id)
        dependencies_for_api = _format_dependencies_for_api(dependencies_payload)
        if dependencies_for_api:
            definition_json = {**definition_json, "dependencies": dependencies_for_api}

        event = build_command_event(
            event_type="PIPELINE_CREATED",
            aggregate_type="Pipeline",
            aggregate_id=record.pipeline_id,
            data={
                "db_name": db_name,
                "pipeline_id": record.pipeline_id,
                "name": name,
                "pipeline_type": pipeline_type,
                "location": location,
                "definition_json": definition_json,
                "version_id": version.version_id,
                "commit_id": version.lakefs_commit_id,
                "version": version.lakefs_commit_id,
                "branch": branch,
            },
            command_type="CREATE_PIPELINE",
        )
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as exc:
            logger.warning("Failed to append pipeline create event: %s", exc)

        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_CREATED",
            status="success",
            pipeline_id=record.pipeline_id,
            metadata={
                "db_name": db_name,
                "name": name,
                "pipeline_type": pipeline_type,
                "branch": branch,
                "commit_id": version.lakefs_commit_id if version else None,
            },
        )

        return ApiResponse.success(
            message="Pipeline created",
            data={
                "pipeline": {
                    **record.__dict__,
                    "definition_json": definition_json,
                    "version_id": version.version_id,
                    "commit_id": version.lakefs_commit_id,
                    "version": version.lakefs_commit_id,
                    "dependencies": dependencies_for_api,
                },
            },
        ).to_dict()
    except HTTPException:
        raise
    except PipelineAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "PIPELINE_ALREADY_EXISTS", "message": str(exc)},
        ) from exc
    except Exception as exc:
        db_name = ""
        if isinstance(sanitized, dict):
            db_name = str(sanitized.get("db_name") or "")
        if not db_name and isinstance(payload, dict):
            db_name = str(payload.get("db_name") or "")
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_CREATED",
            status="failure",
            db_name=db_name,
            error=str(exc),
        )
        logger.error("Failed to create pipeline: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

