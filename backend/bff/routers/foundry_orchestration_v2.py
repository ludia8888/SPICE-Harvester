import logging
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query, Request, Response, status
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
from shared.observability.tracing import trace_endpoint
from shared.services.pipeline.pipeline_job_queue import PipelineJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/orchestration", tags=["Foundry Orchestration v2"])

_PIPELINE_RID_PREFIXES = (
    "ri.spice.main.pipeline.",
    "ri.foundry.main.pipeline.",
    "ri.pipeline.",
)
_BUILD_RID_PREFIXES = (
    "ri.spice.main.build.",
    "ri.foundry.main.build.",
)


def _foundry_error(
    status_code: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "errorCode": error_code,
            "errorName": error_name,
            "errorInstanceId": str(uuid4()),
            "parameters": parameters or {},
        },
    )


def _pipeline_id_from_target_rid(target_rid: str) -> str | None:
    text = str(target_rid or "").strip()
    if not text:
        return None
    for prefix in _PIPELINE_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    try:
        UUID(text)
        return text
    except ValueError:
        return None


def _job_id_from_build_rid(build_rid: str) -> str | None:
    text = str(build_rid or "").strip()
    for prefix in _BUILD_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    return None


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
    return f"ri.spice.main.build.{job_id}"


def _job_rid(job_id: str) -> str:
    return f"ri.spice.main.job.{job_id}"


def _to_int(value: Any, *, field_name: str, minimum: int | None = None) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if minimum is not None and resolved < minimum:
        raise ValueError(f"{field_name} must be >= {minimum}")
    return resolved


def _iso_timestamp(value: Any) -> str:
    if value is None:
        return utcnow().isoformat()
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
    return "main"


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
                dataset_rid = f"ri.spice.main.dataset.{dataset_id}"
        if not dataset_rid:
            continue
        row: dict[str, Any] = {
            "type": "datasetJobOutput",
            "datasetRid": dataset_rid,
        }
        transaction_rid = str(item.get("outputTransactionRid") or item.get("output_transaction_rid") or "").strip()
        if transaction_rid:
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
    ).strip() or "main"
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
) -> dict[str, Any]:
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
        "scheduleRid": None,
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

    pipeline = await pipeline_registry.get_pipeline(resolved.pipeline_id)
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

    pipeline = await pipeline_registry.get_pipeline(pipeline_id)
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
    )


@router.post("/builds/getBatch")
@trace_endpoint("bff.foundry_v2_orchestration.get_build_batch")
async def get_builds_batch_v2(
    payload: list[dict[str, Any]],
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
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
        pipeline = await pipeline_registry.get_pipeline(pipeline_id)
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
        )
    return {"data": result}


@router.get("/builds/{buildRid}/jobs")
@trace_endpoint("bff.foundry_v2_orchestration.list_build_jobs")
async def list_build_jobs_v2(
    buildRid: str,
    pageSize: int | None = Query(default=None),
    pageToken: str | None = Query(default=None),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
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
