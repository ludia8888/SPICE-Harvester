import logging
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse

from bff.dependencies import get_oms_client
from bff.routers.pipeline_deps import (
    get_dataset_registry,
    get_pipeline_job_queue,
    get_pipeline_registry,
)
from bff.services import pipeline_execution_service
from bff.services.oms_client import OMSClient
from shared.dependencies.providers import AuditLogStoreDep
from shared.foundry.auth import require_scopes
from shared.foundry.errors import foundry_error
from shared.foundry.rids import build_rid, parse_rid
from shared.observability.tracing import trace_endpoint
from shared.services.pipeline.pipeline_job_queue import PipelineJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/orchestration", tags=["Foundry Orchestration v2"])


def _foundry_error(
    status_code: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return foundry_error(
        int(status_code),
        error_code=str(error_code),
        error_name=str(error_name),
        parameters=dict(parameters or {}),
    )


def _pipeline_id_from_target_rid(target_rid: str) -> str | None:
    text = str(target_rid or "").strip()
    if not text:
        return None
    try:
        kind, rid_id = parse_rid(text)
    except ValueError:
        return None
    if kind != "pipeline":
        return None
    return rid_id


def _job_id_from_build_rid(build_rid: str) -> str | None:
    text = str(build_rid or "").strip()
    if not text:
        return None
    try:
        kind, rid_id = parse_rid(text)
    except ValueError:
        return None
    if kind != "build":
        return None
    return rid_id


def _pipeline_id_from_build_job_id(job_id: str) -> str | None:
    text = str(job_id or "").strip()
    if not text.startswith("build-"):
        return None
    # Internal build job ID format: build-{pipeline_uuid}-{job_uuid}
    candidate = text[6:42]
    try:
        UUID(candidate)
        return candidate
    except ValueError:
        return None


def _build_rid(job_id: str) -> str:
    return build_rid("build", job_id)


def _job_rid(job_id: str) -> str:
    return build_rid("job", job_id)


def _to_int(value: Any, *, field_name: str, minimum: int | None = None) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if minimum is not None and resolved < minimum:
        raise ValueError(f"{field_name} must be >= {minimum}")
    return resolved


def _iso_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _normalize_build_status(run_status: Any) -> str:
    text = str(run_status or "").strip().upper()
    if text in {"SUCCESS", "SUCCEEDED", "COMPLETED", "DONE"}:
        return "SUCCEEDED"
    if text in {"FAILED", "ERROR"}:
        return "FAILED"
    if text in {"CANCELED", "CANCELLED", "ABORTED"}:
        return "CANCELED"
    return "RUNNING"


def _normalize_job_status(run_status: Any) -> str:
    text = str(run_status or "").strip().upper()
    if text in {"QUEUED", "WAITING", "PENDING"}:
        return "WAITING"
    if text in {"RUNNING", "IN_PROGRESS"}:
        return "RUNNING"
    if text in {"SUCCESS", "SUCCEEDED", "COMPLETED", "DONE"}:
        return "SUCCEEDED"
    if text in {"FAILED", "ERROR"}:
        return "FAILED"
    if text in {"CANCELED", "CANCELLED", "ABORTED"}:
        return "CANCELED"
    return "DID_NOT_RUN"


def _extract_created_by(*, run: dict[str, Any] | None, fallback: str) -> str:
    if isinstance(run, dict):
        output = run.get("output_json") if isinstance(run.get("output_json"), dict) else {}
        marker = output.get("requested_by")
        if isinstance(marker, str) and marker.strip():
            return marker.strip()
    return fallback


def _extract_build_branch(
    *,
    run: dict[str, Any] | None,
    pipeline_branch: str | None,
    requested_branch: str | None,
) -> str:
    if isinstance(run, dict):
        output = run.get("output_json") if isinstance(run.get("output_json"), dict) else {}
        branch = str(output.get("branch") or "").strip()
        if branch:
            return branch
    if requested_branch:
        text = str(requested_branch).strip()
        if text:
            return text
    if pipeline_branch:
        text = str(pipeline_branch).strip()
        if text:
            return text
    return "master"


def _extract_job_outputs(run: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(run, dict):
        return []
    output = run.get("output_json") if isinstance(run.get("output_json"), dict) else {}
    raw_outputs = output.get("outputs")
    if not isinstance(raw_outputs, list):
        return []

    materialized: list[dict[str, Any]] = []
    for item in raw_outputs:
        if not isinstance(item, dict):
            continue
        dataset_rid = str(item.get("datasetRid") or item.get("dataset_rid") or "").strip()
        if not dataset_rid:
            dataset_id = str(item.get("dataset_id") or "").strip()
            if dataset_id:
                dataset_rid = build_rid("dataset", dataset_id)
        if not dataset_rid:
            continue
        try:
            kind, rid_id = parse_rid(dataset_rid)
            if kind == "dataset":
                dataset_rid = build_rid("dataset", rid_id)
        except ValueError as exc:
            logger.debug("Failed to normalize dataset RID in build outputs (datasetRid=%s): %s", dataset_rid, exc)
        row: dict[str, Any] = {
            "type": "datasetJobOutput",
            "datasetRid": dataset_rid,
        }
        transaction_rid = str(item.get("outputTransactionRid") or item.get("output_transaction_rid") or "").strip()
        if transaction_rid:
            try:
                kind, rid_id = parse_rid(transaction_rid)
                if kind == "transaction":
                    transaction_rid = build_rid("transaction", rid_id)
            except ValueError as exc:
                logger.debug(
                    "Failed to normalize output transaction RID in build outputs (outputTransactionRid=%s): %s",
                    transaction_rid,
                    exc,
                )
            row["outputTransactionRid"] = transaction_rid
        materialized.append(row)
    return materialized


@dataclass(frozen=True)
class _ResolvedCreateBuildInput:
    pipeline_id: str
    target_rid: str
    build_payload: dict[str, Any]
    branch_name: str
    fallback_branches: list[str]
    retry_count: int
    retry_backoff_duration: str
    abort_on_failure: bool
    created_by: str


def _resolve_create_build_input(payload: dict[str, Any], *, request: Request) -> _ResolvedCreateBuildInput:
    target = payload.get("target") if isinstance(payload.get("target"), dict) else {}
    target_rids = target.get("targetRids")
    if not isinstance(target_rids, list) or not target_rids:
        single_target_rid = target.get("targetRid") or payload.get("targetRid")
        if single_target_rid:
            target_rids = [single_target_rid]
        else:
            target_rids = []

    if len(target_rids) != 1:
        raise ValueError("target.targetRids must contain exactly one pipeline RID")

    target_rid = str(target_rids[0] or "").strip()
    pipeline_id = _pipeline_id_from_target_rid(target_rid)
    if not pipeline_id:
        raise ValueError("targetRid must reference a pipeline RID")

    build_payload: dict[str, Any] = {}
    branch_name = str(
        payload.get("branchName")
        or payload.get("branch")
        or target.get("branchName")
        or ""
    ).strip() or "master"
    build_payload["branch"] = branch_name

    parameters = payload.get("parameters") if isinstance(payload.get("parameters"), dict) else {}
    node_id = str(
        payload.get("nodeId")
        or parameters.get("nodeId")
        or parameters.get("node_id")
        or ""
    ).strip()
    if node_id:
        build_payload["node_id"] = node_id

    limit_value = (
        payload.get("limit")
        if payload.get("limit") is not None
        else parameters.get("limit")
    )
    if limit_value is not None:
        build_payload["limit"] = _to_int(limit_value, field_name="limit", minimum=1)

    fallback_raw = payload.get("fallbackBranches")
    fallback_branches: list[str]
    if fallback_raw is None:
        fallback_branches = []
    elif isinstance(fallback_raw, list):
        fallback_branches = [str(item).strip() for item in fallback_raw if str(item).strip()]
    else:
        raise ValueError("fallbackBranches must be a list of branch names")

    retry_count_raw = payload.get("retryCount")
    retry_count = 0 if retry_count_raw is None else _to_int(retry_count_raw, field_name="retryCount", minimum=0)

    retry_backoff_duration = str(payload.get("retryBackoffDuration") or "").strip() or "PT0S"
    abort_on_failure = bool(payload.get("abortOnFailure") or False)

    created_by = str(request.headers.get("X-User-ID") or "").strip() or "system"

    return _ResolvedCreateBuildInput(
        pipeline_id=pipeline_id,
        target_rid=target_rid,
        build_payload=build_payload,
        branch_name=branch_name,
        fallback_branches=fallback_branches,
        retry_count=retry_count,
        retry_backoff_duration=retry_backoff_duration,
        abort_on_failure=abort_on_failure,
        created_by=created_by,
    )


def _request_with_internal_idempotency(request: Request) -> Any:
    headers = {key: value for key, value in request.headers.items()}
    header_keys = {key.lower() for key in headers}
    if "idempotency-key" not in header_keys:
        headers["Idempotency-Key"] = f"fdry-build-{uuid4().hex}"
    return SimpleNamespace(headers=headers)


def _build_response(
    *,
    job_id: str,
    run: dict[str, Any] | None,
    branch_name: str,
    created_by: str,
    fallback_branches: list[str],
    retry_count: int,
    retry_backoff_duration: str,
    abort_on_failure: bool,
    pipeline: Any = None,
) -> dict[str, Any]:
    # Determine scheduleRid from pipeline schedule fields
    schedule_rid_value: str | None = None
    if pipeline is not None:
        cron = getattr(pipeline, "schedule_cron", None) or (pipeline.get("schedule_cron") if isinstance(pipeline, dict) else None)
        interval = getattr(pipeline, "schedule_interval_seconds", None) or (pipeline.get("schedule_interval_seconds") if isinstance(pipeline, dict) else None)
        if cron or interval:
            pid = getattr(pipeline, "pipeline_id", None) or (pipeline.get("pipeline_id") if isinstance(pipeline, dict) else None)
            if pid:
                schedule_rid_value = build_rid("schedule", str(pid))

    return {
        "rid": _build_rid(job_id),
        "branchName": branch_name,
        "createdTime": _iso_timestamp(run.get("started_at") if isinstance(run, dict) else None),
        "createdBy": created_by,
        "fallbackBranches": fallback_branches,
        "jobRids": [_job_rid(job_id)],
        "retryCount": retry_count,
        "retryBackoffDuration": retry_backoff_duration,
        "abortOnFailure": abort_on_failure,
        "status": _normalize_build_status(run.get("status") if isinstance(run, dict) else "RUNNING"),
        "scheduleRid": schedule_rid_value,
    }


async def _load_build_run(
    *,
    build_rid: str,
    pipeline_registry: PipelineRegistry,
) -> tuple[str, str, dict[str, Any] | None]:
    job_id = _job_id_from_build_rid(build_rid)
    if not job_id:
        raise ValueError("buildRid is invalid")

    pipeline_id = _pipeline_id_from_build_job_id(job_id)
    if not pipeline_id:
        raise ValueError("Unsupported buildRid format")

    run = await pipeline_registry.get_run(pipeline_id=pipeline_id, job_id=job_id)
    return pipeline_id, job_id, run


@router.post("/builds/create")
@trace_endpoint("bff.foundry_v2_orchestration.create_build")
async def create_build_v2(
    payload: Dict[str, Any],
    request: Request,
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    pipeline_job_queue: PipelineJobQueue = Depends(get_pipeline_job_queue),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    oms_client: OMSClient = Depends(get_oms_client),
    _: None = require_scopes(["api:orchestration-write"]),
):
    try:
        resolved = _resolve_create_build_input(payload, request=request)
    except ValueError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    pipeline = await pipeline_registry.get_pipeline(pipeline_id=resolved.pipeline_id)
    if pipeline is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="PipelineNotFound",
            parameters={"pipelineId": resolved.pipeline_id, "targetRid": resolved.target_rid},
        )

    try:
        service_request = _request_with_internal_idempotency(request)
        response = await pipeline_execution_service.build_pipeline(
            pipeline_id=resolved.pipeline_id,
            payload=resolved.build_payload,
            request=service_request,
            audit_store=audit_store,
            pipeline_registry=pipeline_registry,
            pipeline_job_queue=pipeline_job_queue,
            dataset_registry=dataset_registry,
            oms_client=oms_client,
            emit_pipeline_control_plane_event=lambda **_: None,
        )
        data = response.get("data") if isinstance(response, dict) else {}
        job_id = str(data.get("job_id") or "").strip()
        if not job_id:
            raise ValueError("build job_id is missing")

        run = await pipeline_registry.get_run(pipeline_id=resolved.pipeline_id, job_id=job_id)
        branch_name = _extract_build_branch(
            run=run,
            pipeline_branch=getattr(pipeline, "branch", None),
            requested_branch=resolved.branch_name,
        )
        return _build_response(
            job_id=job_id,
            run=run,
            branch_name=branch_name,
            created_by=resolved.created_by,
            fallback_branches=resolved.fallback_branches,
            retry_count=resolved.retry_count,
            retry_backoff_duration=resolved.retry_backoff_duration,
            abort_on_failure=resolved.abort_on_failure,
            pipeline=pipeline,
        )
    except HTTPException as exc:
        detail = exc.detail
        message = detail.get("message") if isinstance(detail, dict) else str(detail or "Build request failed")
        error_code = "PERMISSION_DENIED" if exc.status_code == status.HTTP_403_FORBIDDEN else "UPSTREAM_ERROR"
        error_name = "PermissionDenied" if exc.status_code == status.HTTP_403_FORBIDDEN else "BuildRequestFailed"
        return _foundry_error(
            exc.status_code,
            error_code=error_code,
            error_name=error_name,
            parameters={"message": message},
        )
    except Exception as exc:
        logger.error("Failed to create Foundry orchestration build: %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to create build"},
        )


@router.get("/builds/{buildRid}")
@trace_endpoint("bff.foundry_v2_orchestration.get_build")
async def get_build_v2(
    buildRid: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-read"]),
):
    try:
        pipeline_id, job_id, run = await _load_build_run(
            build_rid=buildRid,
            pipeline_registry=pipeline_registry,
        )
    except ValueError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    if not isinstance(run, dict):
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="BuildNotFound",
            parameters={"buildRid": buildRid},
        )

    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    branch_name = _extract_build_branch(
        run=run,
        pipeline_branch=getattr(pipeline, "branch", None) if pipeline else None,
        requested_branch=None,
    )
    return _build_response(
        job_id=job_id,
        run=run,
        branch_name=branch_name,
        created_by=_extract_created_by(run=run, fallback="system"),
        fallback_branches=[],
        retry_count=0,
        retry_backoff_duration="PT0S",
        abort_on_failure=False,
        pipeline=pipeline,
    )


@router.post("/builds/getBatch")
@trace_endpoint("bff.foundry_v2_orchestration.get_build_batch")
async def get_builds_batch_v2(
    payload: list[dict[str, Any]],
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-read"]),
):
    if not isinstance(payload, list) or not payload:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "Request body must be a non-empty list"},
        )

    result: dict[str, Any] = {}
    for item in payload[:100]:
        if not isinstance(item, dict):
            continue
        build_rid = str(item.get("buildRid") or "").strip()
        if not build_rid:
            continue
        try:
            pipeline_id, job_id, run = await _load_build_run(
                build_rid=build_rid,
                pipeline_registry=pipeline_registry,
            )
        except ValueError:
            continue
        if not isinstance(run, dict):
            continue
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        branch_name = _extract_build_branch(
            run=run,
            pipeline_branch=getattr(pipeline, "branch", None) if pipeline else None,
            requested_branch=None,
        )
        result[build_rid] = _build_response(
            job_id=job_id,
            run=run,
            branch_name=branch_name,
            created_by=_extract_created_by(run=run, fallback="system"),
            fallback_branches=[],
            retry_count=0,
            retry_backoff_duration="PT0S",
            abort_on_failure=False,
            pipeline=pipeline,
        )
    return {"data": result}


@router.get("/builds/{buildRid}/jobs")
@trace_endpoint("bff.foundry_v2_orchestration.list_build_jobs")
async def list_build_jobs_v2(
    buildRid: str,
    pageSize: int | None = Query(default=None),
    pageToken: str | None = Query(default=None),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-read"]),
):
    try:
        _pipeline_id, job_id, run = await _load_build_run(
            build_rid=buildRid,
            pipeline_registry=pipeline_registry,
        )
    except ValueError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    if not isinstance(run, dict):
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="BuildNotFound",
            parameters={"buildRid": buildRid},
        )

    if pageSize is not None and pageSize <= 0:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "pageSize must be > 0"},
        )

    if pageToken:
        return {"data": [], "nextPageToken": None}

    started_time = _iso_timestamp(run.get("started_at"))
    job_payload: dict[str, Any] = {
        "rid": _job_rid(job_id),
        "buildRid": buildRid,
        "startedTime": started_time,
        "latestAttemptStartTime": started_time,
        "finishedTime": _iso_timestamp(run.get("finished_at")) if run.get("finished_at") else None,
        "jobStatus": _normalize_job_status(run.get("status")),
        "outputs": _extract_job_outputs(run),
    }
    return {"data": [job_payload], "nextPageToken": None}


@router.post("/builds/{buildRid}/cancel")
@trace_endpoint("bff.foundry_v2_orchestration.cancel_build")
async def cancel_build_v2(
    buildRid: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-write"]),
):
    try:
        pipeline_id, job_id, run = await _load_build_run(
            build_rid=buildRid,
            pipeline_registry=pipeline_registry,
        )
    except ValueError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    if not isinstance(run, dict):
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="BuildNotFound",
            parameters={"buildRid": buildRid},
        )

    normalized = _normalize_build_status(run.get("status"))
    if normalized in {"SUCCEEDED", "FAILED", "CANCELED"}:
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    output_json = run.get("output_json") if isinstance(run.get("output_json"), dict) else {}
    await pipeline_registry.record_run(
        pipeline_id=pipeline_id,
        job_id=job_id,
        mode="build",
        status="CANCELED",
        node_id=run.get("node_id"),
        row_count=run.get("row_count"),
        sample_json=run.get("sample_json") if isinstance(run.get("sample_json"), dict) else {},
        output_json={**output_json, "cancelRequestedAt": utcnow().isoformat()},
        pipeline_spec_commit_id=run.get("pipeline_spec_commit_id"),
        pipeline_spec_hash=run.get("pipeline_spec_hash"),
        input_lakefs_commits=run.get("input_lakefs_commits"),
        output_lakefs_commit_id=run.get("output_lakefs_commit_id"),
        spark_conf=run.get("spark_conf"),
        code_version=run.get("code_version"),
        started_at=run.get("started_at"),
        finished_at=utcnow(),
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# Schedules — Foundry Orchestration Schedules API v2
# ---------------------------------------------------------------------------


def _schedule_rid(pipeline_id: str) -> str:
    return build_rid("schedule", pipeline_id)


def _pipeline_id_from_schedule_rid(schedule_rid: str) -> str | None:
    text = str(schedule_rid or "").strip()
    if not text:
        return None
    try:
        kind, rid_id = parse_rid(text)
    except ValueError:
        return None
    if kind != "schedule":
        return None
    return rid_id


def _schedule_response(pipeline: Any) -> dict[str, Any]:
    pipeline_id = getattr(pipeline, "pipeline_id", None) or (pipeline.get("pipeline_id") if isinstance(pipeline, dict) else None)
    schedule_cron = getattr(pipeline, "schedule_cron", None) or (pipeline.get("schedule_cron") if isinstance(pipeline, dict) else None)
    schedule_interval = getattr(pipeline, "schedule_interval_seconds", None) or (pipeline.get("schedule_interval_seconds") if isinstance(pipeline, dict) else None)
    branch = getattr(pipeline, "branch", None) or (pipeline.get("branch") if isinstance(pipeline, dict) else None)
    pipeline_status = str(getattr(pipeline, "status", None) or (pipeline.get("status") if isinstance(pipeline, dict) else "") or "").strip().lower()

    trigger: dict[str, Any] = {"type": "time"}
    if schedule_cron:
        trigger["cronExpression"] = str(schedule_cron)
    if schedule_interval:
        trigger["intervalSeconds"] = int(schedule_interval)

    schedule_status = "ACTIVE"
    if pipeline_status in {"paused", "disabled"}:
        schedule_status = "PAUSED"

    return {
        "rid": _schedule_rid(pipeline_id),
        "targetRid": build_rid("pipeline", str(pipeline_id)),
        "trigger": trigger,
        "action": {
            "type": "build",
            "branchName": str(branch or "master"),
        },
        "status": schedule_status,
        "createdTime": _iso_timestamp(getattr(pipeline, "created_at", None) or (pipeline.get("created_at") if isinstance(pipeline, dict) else None)),
        "createdBy": "system",
    }


def _has_schedule(pipeline: Any) -> bool:
    cron = getattr(pipeline, "schedule_cron", None) or (pipeline.get("schedule_cron") if isinstance(pipeline, dict) else None)
    interval = getattr(pipeline, "schedule_interval_seconds", None) or (pipeline.get("schedule_interval_seconds") if isinstance(pipeline, dict) else None)
    return bool(cron) or bool(interval)


@router.post("/schedules")
@trace_endpoint("bff.foundry_v2_orchestration.create_schedule")
async def create_schedule_v2(
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-write"]),
) -> JSONResponse:
    """POST /v2/orchestration/schedules — Create a schedule."""
    target_rid = str(payload.get("targetRid") or "").strip()
    pipeline_id = _pipeline_id_from_target_rid(target_rid)
    if not pipeline_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidArgument", parameters={"message": "targetRid must reference a pipeline RID"})

    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if pipeline is None:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="PipelineNotFound", parameters={"pipelineId": pipeline_id})

    trigger = payload.get("trigger") if isinstance(payload.get("trigger"), dict) else {}
    cron_expression = str(trigger.get("cronExpression") or "").strip() or None
    interval_seconds = trigger.get("intervalSeconds")
    if interval_seconds is not None:
        try:
            interval_seconds = int(interval_seconds)
        except (TypeError, ValueError):
            return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidArgument", parameters={"message": "intervalSeconds must be an integer"})

    if not cron_expression and not interval_seconds:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidArgument", parameters={"message": "trigger.cronExpression or trigger.intervalSeconds required"})

    try:
        await pipeline_registry.update_pipeline(
            pipeline_id=pipeline_id,
            schedule_cron=cron_expression,
            schedule_interval_seconds=interval_seconds,
        )
    except Exception as exc:
        logger.error("Failed to create schedule: %s", exc)
        return _foundry_error(500, error_code="INTERNAL", error_name="ScheduleCreationFailed", parameters={"message": str(exc)})

    updated_pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    return JSONResponse(content=_schedule_response(updated_pipeline or pipeline))


@router.get("/schedules/{scheduleRid}")
@trace_endpoint("bff.foundry_v2_orchestration.get_schedule")
async def get_schedule_v2(
    scheduleRid: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-read"]),
) -> JSONResponse:
    """GET /v2/orchestration/schedules/{scheduleRid} — Get a schedule."""
    pipeline_id = _pipeline_id_from_schedule_rid(scheduleRid)
    if not pipeline_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidScheduleRid", parameters={"scheduleRid": scheduleRid})

    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if pipeline is None:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="ScheduleNotFound", parameters={"scheduleRid": scheduleRid})

    if not _has_schedule(pipeline):
        return _foundry_error(404, error_code="NOT_FOUND", error_name="ScheduleNotFound", parameters={"scheduleRid": scheduleRid})

    return JSONResponse(content=_schedule_response(pipeline))


@router.delete("/schedules/{scheduleRid}", status_code=status.HTTP_204_NO_CONTENT)
@trace_endpoint("bff.foundry_v2_orchestration.delete_schedule")
async def delete_schedule_v2(
    scheduleRid: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-write"]),
) -> Response:
    """DELETE /v2/orchestration/schedules/{scheduleRid} — Remove schedule."""
    pipeline_id = _pipeline_id_from_schedule_rid(scheduleRid)
    if not pipeline_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidScheduleRid", parameters={"scheduleRid": scheduleRid})

    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if pipeline is None:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="ScheduleNotFound", parameters={"scheduleRid": scheduleRid})

    try:
        await pipeline_registry.update_pipeline(
            pipeline_id=pipeline_id,
            schedule_cron="",
            schedule_interval_seconds=0,
        )
    except Exception as exc:
        logger.error("Failed to delete schedule: %s", exc)
        return _foundry_error(500, error_code="INTERNAL", error_name="ScheduleDeletionFailed", parameters={"message": str(exc)})

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/schedules/{scheduleRid}/pause", status_code=status.HTTP_204_NO_CONTENT)
@trace_endpoint("bff.foundry_v2_orchestration.pause_schedule")
async def pause_schedule_v2(
    scheduleRid: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-write"]),
) -> Response:
    """POST /v2/orchestration/schedules/{scheduleRid}/pause — Pause schedule."""
    pipeline_id = _pipeline_id_from_schedule_rid(scheduleRid)
    if not pipeline_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidScheduleRid", parameters={"scheduleRid": scheduleRid})

    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if pipeline is None:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="ScheduleNotFound", parameters={"scheduleRid": scheduleRid})

    try:
        await pipeline_registry.update_pipeline(pipeline_id=pipeline_id, status="paused")
    except Exception as exc:
        logger.error("Failed to pause schedule: %s", exc)
        return _foundry_error(500, error_code="INTERNAL", error_name="SchedulePauseFailed", parameters={"message": str(exc)})

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/schedules/{scheduleRid}/unpause", status_code=status.HTTP_204_NO_CONTENT)
@trace_endpoint("bff.foundry_v2_orchestration.unpause_schedule")
async def unpause_schedule_v2(
    scheduleRid: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-write"]),
) -> Response:
    """POST /v2/orchestration/schedules/{scheduleRid}/unpause — Unpause schedule."""
    pipeline_id = _pipeline_id_from_schedule_rid(scheduleRid)
    if not pipeline_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidScheduleRid", parameters={"scheduleRid": scheduleRid})

    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if pipeline is None:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="ScheduleNotFound", parameters={"scheduleRid": scheduleRid})

    try:
        await pipeline_registry.update_pipeline(pipeline_id=pipeline_id, status="active")
    except Exception as exc:
        logger.error("Failed to unpause schedule: %s", exc)
        return _foundry_error(500, error_code="INTERNAL", error_name="ScheduleUnpauseFailed", parameters={"message": str(exc)})

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/schedules/{scheduleRid}/runs")
@trace_endpoint("bff.foundry_v2_orchestration.list_schedule_runs")
async def list_schedule_runs_v2(
    scheduleRid: str,
    pageSize: int = Query(default=25, ge=1, le=100),
    pageToken: str = Query(default=""),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:orchestration-read"]),
) -> JSONResponse:
    """GET /v2/orchestration/schedules/{scheduleRid}/runs — List schedule runs."""
    pipeline_id = _pipeline_id_from_schedule_rid(scheduleRid)
    if not pipeline_id:
        return _foundry_error(400, error_code="INVALID_ARGUMENT", error_name="InvalidScheduleRid", parameters={"scheduleRid": scheduleRid})

    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if pipeline is None:
        return _foundry_error(404, error_code="NOT_FOUND", error_name="ScheduleNotFound", parameters={"scheduleRid": scheduleRid})

    offset = 0
    if pageToken:
        try:
            offset = int(pageToken)
        except (TypeError, ValueError):
            return _foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )
        if offset < 0:
            return _foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )

    # Load enough runs to materialize the requested page when using an offset token.
    fetch_limit = min(max(pageSize + offset + 1, pageSize + 1), 5000)
    try:
        runs = await pipeline_registry.list_runs(pipeline_id=pipeline_id, limit=fetch_limit)
    except Exception:
        runs = []

    page = runs[offset: offset + pageSize] if isinstance(runs, list) else []
    next_offset = offset + len(page)
    next_token = str(next_offset) if next_offset < len(runs) else None

    data = []
    for run_item in page:
        if isinstance(run_item, dict):
            job_id = str(run_item.get("job_id") or "").strip()
            data.append({
                "buildRid": _build_rid(job_id) if job_id else None,
                "status": _normalize_build_status(run_item.get("status")),
                "startedTime": _iso_timestamp(run_item.get("started_at")),
                "finishedTime": _iso_timestamp(run_item.get("finished_at")) if run_item.get("finished_at") else None,
            })

    return JSONResponse(content={"data": data, "nextPageToken": next_token})
