"""Pipeline catalog domain logic (BFF).

Extracted from `bff.routers.pipeline_catalog` to keep routers thin.
"""

import logging
from typing import Any, Optional
from uuid import NAMESPACE_URL, UUID, uuid5

from fastapi import HTTPException, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.pipeline_ops import (
    _augment_definition_with_canonical_contract,
    _augment_definition_with_casts,
    _format_dependencies_for_api,
    _normalize_dependencies_payload,
    _normalize_location,
    _validate_dependency_targets,
)
from bff.routers.pipeline_ops_preflight import _validate_pipeline_definition
from bff.routers.pipeline_shared import (
    _filter_pipeline_records_for_read_access,
    _log_pipeline_audit,
    _require_pipeline_idempotency_key,
    _resolve_principal,
)
from bff.services.database_role_guard import enforce_database_role_or_http_error
from bff.services.input_validation_service import enforce_db_scope_or_403
from shared.config.app_config import AppConfig
from shared.models.requests import ApiResponse
from shared.security.database_access import DATA_ENGINEER_ROLES
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.pipeline.pipeline_scheduler import _is_valid_cron_expression
from shared.services.core.write_path_contract import (
    build_write_path_contract,
    followup_completed,
    followup_degraded,
    followup_skipped,
)
from shared.services.registries.pipeline_registry import PipelineAlreadyExistsError
from shared.utils.event_utils import build_command_event
from shared.observability.tracing import trace_db_operation

logger = logging.getLogger(__name__)


async def _grant_pipeline_admin_best_effort(
    *,
    pipeline_registry: Any,
    request: Any,
    pipeline_id: str,
) -> dict[str, Any]:
    principal_type, principal_id = _resolve_principal(request)
    details = {
        "pipeline_id": str(pipeline_id),
        "principal_type": str(principal_type),
        "principal_id": str(principal_id),
    }
    try:
        await pipeline_registry.grant_permission(
            pipeline_id=pipeline_id,
            principal_type=principal_type,
            principal_id=principal_id,
            role="admin",
        )
        return followup_completed("pipeline_admin_grant", details=details)
    except Exception as exc:
        logger.warning(
            "Pipeline create completed but admin grant failed (pipeline_id=%s): %s",
            pipeline_id,
            exc,
        )
        return followup_degraded(
            "pipeline_admin_grant",
            error=str(exc),
            details=details,
        )


def _build_pipeline_create_response(
    *,
    record: Any,
    version: Any,
    definition_json: dict[str, Any],
    dependencies_payload: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    dependencies_for_api = _format_dependencies_for_api(dependencies_payload)
    response_definition_json = dict(version.definition_json or {}) if version is not None else dict(definition_json or {})
    if dependencies_for_api:
        response_definition_json["dependencies"] = dependencies_for_api
    return ApiResponse.success(
        message="Pipeline created",
        data={
            "pipeline": {
                **record.__dict__,
                "definition_json": response_definition_json,
                "version_id": version.version_id if version is not None else None,
                "commit_id": version.lakefs_commit_id if version is not None else None,
                "version": version.lakefs_commit_id if version is not None else None,
                "dependencies": dependencies_for_api,
            },
        },
    ).to_dict()


def _idempotent_pipeline_id(*, idempotency_key: str, db_name: str, branch: str) -> str:
    seed = f"bff:create_pipeline:{db_name}:{branch}:{idempotency_key}"
    return str(uuid5(NAMESPACE_URL, seed))


def _normalize_pipeline_create_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _normalize_dependencies_for_compare(raw: Optional[list[dict[str, Any]]]) -> list[tuple[str, str]]:
    normalized: set[tuple[str, str]] = set()
    for dep in raw or []:
        if not isinstance(dep, dict):
            continue
        pipeline_id = str(dep.get("pipeline_id") or dep.get("pipelineId") or "").strip()
        if not pipeline_id:
            continue
        status_value = str(dep.get("status") or "DEPLOYED").strip().upper() or "DEPLOYED"
        normalized.add((pipeline_id, status_value))
    return sorted(normalized)


def _pipeline_create_conflict() -> HTTPException:
    return classified_http_exception(
        status.HTTP_409_CONFLICT,
        "Idempotency-Key already used for a different pipeline create request",
        code=ErrorCode.CONFLICT,
    )


def _existing_pipeline_matches_request(
    existing: Any,
    *,
    db_name: str,
    name: str,
    description: Optional[str],
    pipeline_type: str,
    location: str,
    branch: str,
    proposal_status: Optional[str],
    proposal_title: Optional[str],
    proposal_description: Optional[str],
    proposal_submitted_at: Any,
    proposal_reviewed_at: Any,
    proposal_review_comment: Optional[str],
    schedule_interval_seconds: Optional[int],
    schedule_cron: Optional[str],
) -> bool:
    comparisons = (
        (getattr(existing, "db_name", None), db_name),
        (getattr(existing, "name", None), name),
        (getattr(existing, "description", None), description),
        (getattr(existing, "pipeline_type", None), pipeline_type),
        (getattr(existing, "location", None), location),
        (getattr(existing, "branch", None), branch),
        (getattr(existing, "proposal_status", None), proposal_status),
        (getattr(existing, "proposal_title", None), proposal_title),
        (getattr(existing, "proposal_description", None), proposal_description),
        (getattr(existing, "proposal_submitted_at", None), proposal_submitted_at),
        (getattr(existing, "proposal_reviewed_at", None), proposal_reviewed_at),
        (getattr(existing, "proposal_review_comment", None), proposal_review_comment),
        (getattr(existing, "schedule_interval_seconds", None), schedule_interval_seconds),
        (getattr(existing, "schedule_cron", None), schedule_cron),
    )
    return all(
        _normalize_pipeline_create_value(current) == _normalize_pipeline_create_value(expected)
        for current, expected in comparisons
    )


async def _maybe_resume_idempotent_pipeline_create(
    *,
    pipeline_registry: Any,
    request: Any,
    pipeline_id: str,
    db_name: str,
    name: str,
    description: Optional[str],
    pipeline_type: str,
    location: str,
    branch: str,
    proposal_status: Optional[str],
    proposal_title: Optional[str],
    proposal_description: Optional[str],
    proposal_submitted_at: Any,
    proposal_reviewed_at: Any,
    proposal_review_comment: Optional[str],
    schedule_interval_seconds: Optional[int],
    schedule_cron: Optional[str],
    definition_json: dict[str, Any],
    dependencies: Optional[list[dict[str, str]]],
) -> Optional[dict[str, Any]]:
    existing = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if not existing:
        return None

    if not _existing_pipeline_matches_request(
        existing,
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
    ):
        raise _pipeline_create_conflict()

    version = await pipeline_registry.get_latest_version(pipeline_id=existing.pipeline_id, branch=branch)
    if version is None:
        version = await pipeline_registry.add_version(
            pipeline_id=existing.pipeline_id,
            branch=branch,
            definition_json=definition_json,
        )
    elif dict(version.definition_json or {}) != dict(definition_json or {}):
        raise _pipeline_create_conflict()

    existing_dependencies_payload = await pipeline_registry.list_dependencies(pipeline_id=existing.pipeline_id)
    existing_dependencies = _normalize_dependencies_for_compare(existing_dependencies_payload)
    requested_dependencies = _normalize_dependencies_for_compare(dependencies)
    if dependencies is None:
        if existing_dependencies:
            raise _pipeline_create_conflict()
    else:
        if not existing_dependencies and requested_dependencies:
            await pipeline_registry.replace_dependencies(pipeline_id=existing.pipeline_id, dependencies=dependencies)
            existing_dependencies_payload = await pipeline_registry.list_dependencies(pipeline_id=existing.pipeline_id)
            existing_dependencies = _normalize_dependencies_for_compare(existing_dependencies_payload)
        elif existing_dependencies != requested_dependencies:
            raise _pipeline_create_conflict()

    await _grant_pipeline_admin_best_effort(
        pipeline_registry=pipeline_registry,
        request=request,
        pipeline_id=existing.pipeline_id,
    )

    logger.info(
        "Recovered idempotent pipeline create request (pipeline_id=%s branch=%s)",
        existing.pipeline_id,
        branch,
        )
    return _build_pipeline_create_response(
        record=existing,
        version=version,
        definition_json=definition_json,
        dependencies_payload=existing_dependencies_payload,
    )


@trace_db_operation("bff.pipeline_catalog.list_pipelines")
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
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_db_operation("bff.pipeline_catalog.create_pipeline")
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
    effective_pipeline_id: Optional[str] = None
    created_record: Any = None
    try:
        idempotency_key = _require_pipeline_idempotency_key(request, operation="pipeline create")
        sanitized = sanitize_input(payload)
        pipeline_id: Optional[str] = None
        raw_pipeline_id = sanitized.get("pipeline_id") or sanitized.get("pipelineId")
        if raw_pipeline_id:
            try:
                pipeline_id = str(UUID(str(raw_pipeline_id)))
            except Exception as exc:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "pipeline_id must be a UUID", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
        db_name = validate_db_name(str(sanitized.get("db_name") or ""))
        if request is not None:
            enforce_db_scope_or_403(request, db_name=db_name)
            await enforce_database_role_or_http_error(
                headers=request.headers,
                db_name=db_name,
                required_roles=DATA_ENGINEER_ROLES,
            )
        name = str(sanitized.get("name") or "").strip()
        if not name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
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
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule_interval_seconds must be integer", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
            if schedule_interval_seconds <= 0:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule_interval_seconds must be > 0", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        schedule_cron = str(schedule_cron).strip() if schedule_cron else None
        if schedule_interval_seconds and schedule_cron:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Provide either schedule_interval_seconds or schedule_cron (not both)", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        if schedule_cron and not _is_valid_cron_expression(schedule_cron):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule_cron must be a supported 5-field cron expression", code=ErrorCode.PIPELINE_VALIDATION_FAILED)
        proposal_submitted_at = None if not proposal_submitted_at else proposal_submitted_at
        proposal_reviewed_at = None if not proposal_reviewed_at else proposal_reviewed_at
        proposal_review_comment = str(proposal_review_comment).strip() if proposal_review_comment else None
        effective_pipeline_id = pipeline_id or _idempotent_pipeline_id(
            idempotency_key=idempotency_key,
            db_name=db_name,
            branch=branch,
        )

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

        recovered = await _maybe_resume_idempotent_pipeline_create(
            pipeline_registry=pipeline_registry,
            request=request,
            pipeline_id=effective_pipeline_id,
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
            definition_json=definition_json,
            dependencies=dependencies,
        )
        if recovered is not None:
            return recovered

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
            pipeline_id=effective_pipeline_id,
        )
        created_record = record
        version = await pipeline_registry.add_version(
            pipeline_id=record.pipeline_id,
            branch=branch,
            definition_json=definition_json,
        )
        write_path_followups: list[dict[str, Any]] = []
        if dependencies is not None:
            await pipeline_registry.replace_dependencies(pipeline_id=record.pipeline_id, dependencies=dependencies)
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
        dependencies_payload = await pipeline_registry.list_dependencies(pipeline_id=record.pipeline_id)
        write_path_followups.append(
            await _grant_pipeline_admin_best_effort(
                pipeline_registry=pipeline_registry,
                request=request,
                pipeline_id=record.pipeline_id,
            )
        )

        event_followup: dict[str, Any]
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
                "definition_json": dict(version.definition_json or {}),
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
            event_followup = followup_completed(
                "pipeline_created_event",
                details={"topic": AppConfig.PIPELINE_EVENTS_TOPIC},
            )
        except Exception as exc:
            logger.warning("Failed to append pipeline create event: %s", exc)
            event_followup = followup_degraded(
                "pipeline_created_event",
                error=str(exc),
                details={"topic": AppConfig.PIPELINE_EVENTS_TOPIC},
            )
        write_path_followups.append(event_followup)

        contract = build_write_path_contract(
            authoritative_write="pipeline_create",
            followups=[
                *write_path_followups,
                followup_completed(
                    "pipeline_audit",
                    details={"action": "PIPELINE_CREATED"},
                ),
            ],
        )

        try:
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
                    "write_path_contract": contract,
                },
            )
        except Exception as exc:
            degraded_contract = build_write_path_contract(
                authoritative_write="pipeline_create",
                followups=[
                    *write_path_followups,
                    followup_degraded(
                        "pipeline_audit",
                        error=str(exc),
                        details={"action": "PIPELINE_CREATED"},
                    ),
                ],
            )
            logger.warning(
                "Failed to record pipeline create audit log: %s write_path_contract=%s",
                exc,
                degraded_contract,
            )

        return _build_pipeline_create_response(
            record=record,
            version=version,
            definition_json=definition_json,
            dependencies_payload=dependencies_payload,
        )
    except HTTPException:
        raise
    except PipelineAlreadyExistsError as exc:
        if effective_pipeline_id:
            recovered = await _maybe_resume_idempotent_pipeline_create(
                pipeline_registry=pipeline_registry,
                request=request,
                pipeline_id=effective_pipeline_id,
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
                definition_json=definition_json,
                dependencies=dependencies,
            )
            if recovered is not None:
                return recovered
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            str(exc),
            code=ErrorCode.RESOURCE_ALREADY_EXISTS,
        ) from exc
    except Exception as exc:
        if effective_pipeline_id and created_record is not None:
            recovered = await _maybe_resume_idempotent_pipeline_create(
                pipeline_registry=pipeline_registry,
                request=request,
                pipeline_id=effective_pipeline_id,
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
                definition_json=definition_json,
                dependencies=dependencies,
            )
            if recovered is not None:
                return recovered
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
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
