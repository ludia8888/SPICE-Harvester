from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from shared.errors.external_codes import ExternalErrorCode
from shared.models.pipeline_plan import PipelinePlan
from shared.observability.tracing import trace_external_call
from shared.services.pipeline.pipeline_preview_inspector import inspect_preview
from shared.utils.llm_safety import mask_pii

from mcp_servers.pipeline_mcp_helpers import (
    extract_spark_error_details,
    trim_build_output,
    trim_preview_payload,
)
from mcp_servers.pipeline_mcp_http import bff_json
from mcp_servers.pipeline_mcp_errors import tool_error
from shared.errors.error_types import ErrorCategory, ErrorCode

logger = logging.getLogger(__name__)

ToolHandler = Callable[[Any, Dict[str, Any]], Awaitable[Any]]


def _run_status(run: Dict[str, Any]) -> str:
    return str(run.get("status") or "").strip().upper() or "QUEUED"


async def _pipeline_wait_for_mode(
    server: Any,
    *,
    pipeline_id: str,
    mode: str,
    enqueue_path: str,
    output_builder: Callable[[Dict[str, Any], str, bool], Dict[str, Any]],
    enqueue_job_id_extractor: Callable[[Dict[str, Any]], str],
    timeout_message: str,
    arguments: Dict[str, Any],
) -> Any:
    limit = int(arguments.get("limit") or 200)
    limit = max(1, min(limit, 500))
    node_id = str(arguments.get("node_id") or "").strip() or None
    branch = str(arguments.get("branch") or "").strip() or None
    job_id_arg = str(arguments.get("job_id") or "").strip() or None
    force = bool(arguments.get("force") or False)
    wait = bool(arguments.get("wait", True))
    timeout_seconds = float(arguments.get("timeout_seconds") or 180.0)
    poll_s = float(arguments.get("poll_interval_seconds") or 2.0)
    db_name = str(arguments.get("db_name") or "").strip()
    principal_id = str(arguments.get("principal_id") or "").strip() or None
    principal_type = str(arguments.get("principal_type") or "").strip() or None

    async def _fetch_runs() -> Dict[str, Any]:
        return await bff_json(
            "GET",
            f"/pipelines/{pipeline_id}/runs",
            db_name=db_name,
            principal_id=principal_id,
            principal_type=principal_type,
            params={"limit": 50},
            timeout_seconds=15.0,
        )

    job_id = job_id_arg
    reused_existing = False
    if not job_id and not force:
        # If there's already a queued/running job for this mode+node, reuse it.
        runs = await _fetch_runs()
        if not runs.get("error"):
            runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
            run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
            for item in run_list:
                if not isinstance(item, dict):
                    continue
                if str(item.get("mode") or "").strip().lower() != mode:
                    continue
                status_value = _run_status(item)
                if status_value not in {"QUEUED", "RUNNING"}:
                    continue
                item_node_id = str(item.get("node_id") or "").strip() or None
                if item_node_id != node_id:
                    continue
                candidate = str(item.get("job_id") or "").strip()
                if candidate:
                    job_id = candidate
                    reused_existing = True
                    break

    if not job_id:
        enqueue_body: Dict[str, Any] = {"limit": limit}
        if node_id:
            enqueue_body["node_id"] = node_id
        if branch:
            enqueue_body["branch"] = branch
        resp = await bff_json(
            "POST",
            enqueue_path,
            db_name=db_name,
            principal_id=principal_id,
            principal_type=principal_type,
            json_body=enqueue_body,
            timeout_seconds=30.0,
        )
        if resp.get("error"):
            return resp
        job_id = enqueue_job_id_extractor(resp)
        if not job_id:
            return {"error": f"{mode} enqueue did not return job_id", "response": resp}

    if not wait:
        # If the caller provided a job_id, return a one-shot status snapshot.
        if job_id_arg:
            runs = await _fetch_runs()
            if runs.get("error"):
                return {"status": "queued", "job_id": job_id, "reused_existing_job": reused_existing}
            runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
            run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
            selected_run: Optional[Dict[str, Any]] = None
            for item in run_list:
                if not isinstance(item, dict):
                    continue
                if str(item.get("job_id") or "").strip() == job_id:
                    selected_run = item
                    break
            if selected_run:
                status_value = _run_status(selected_run)
                if status_value in {"SUCCESS", "FAILED"}:
                    return output_builder(selected_run, job_id, reused_existing)
                return {"status": status_value.lower(), "job_id": job_id, "reused_existing_job": reused_existing}
        return {"status": "queued", "job_id": job_id, "reused_existing_job": reused_existing, "limit": limit}

    deadline = asyncio.get_running_loop().time() + max(1.0, timeout_seconds)
    last_status: Optional[str] = None
    while asyncio.get_running_loop().time() < deadline:
        runs = await _fetch_runs()
        if runs.get("error"):
            return runs
        runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
        run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
        selected_run: Optional[Dict[str, Any]] = None
        for item in run_list:
            if not isinstance(item, dict):
                continue
            if str(item.get("job_id") or "").strip() == job_id:
                selected_run = item
                break
        if not selected_run:
            await asyncio.sleep(max(0.2, poll_s))
            continue
        status_value = _run_status(selected_run)
        last_status = status_value
        if status_value in {"SUCCESS", "FAILED"}:
            return output_builder(selected_run, job_id, reused_existing)
        await asyncio.sleep(max(0.2, poll_s))

    return {
        "status": "timeout",
        "job_id": job_id,
        "reused_existing_job": reused_existing,
        "last_status": last_status,
        "message": timeout_message,
    }


def _preview_job_id(resp: Dict[str, Any]) -> str:
    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
    job_id = str(data.get("job_id") or "").strip()
    if not job_id and isinstance(data.get("sample"), dict):
        job_id = str((data.get("sample") or {}).get("job_id") or "").strip()
    return job_id


def _build_job_id(resp: Dict[str, Any]) -> str:
    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
    return str(data.get("job_id") or "").strip()


def _preview_output(selected_run: Dict[str, Any], job_id: str, reused_existing: bool) -> Dict[str, Any]:
    status_value = _run_status(selected_run)
    sample_json = selected_run.get("sample_json") if isinstance(selected_run.get("sample_json"), dict) else {}
    masked = mask_pii(sample_json)
    result = {
        "status": status_value.lower(),
        "job_id": job_id,
        "reused_existing_job": reused_existing,
        "preview": trim_preview_payload(masked, max_rows=8),
    }
    if status_value == "FAILED":
        result.update(extract_spark_error_details(selected_run))
    return result


def _build_output(selected_run: Dict[str, Any], job_id: str, reused_existing: bool) -> Dict[str, Any]:
    status_value = _run_status(selected_run)
    output_json = selected_run.get("output_json") if isinstance(selected_run.get("output_json"), dict) else {}
    trimmed = trim_build_output(output_json, max_rows=6)
    masked = mask_pii(trimmed)
    result = {
        "status": status_value.lower(),
        "job_id": job_id,
        "reused_existing_job": reused_existing,
        "artifact_id": output_json.get("artifact_id") if isinstance(output_json, dict) else None,
        "output": masked,
    }
    if status_value == "FAILED":
        result.update(extract_spark_error_details(selected_run))
    return result


@trace_external_call("mcp.preview_inspect")
async def _preview_inspect(_server: Any, arguments: Dict[str, Any]) -> Any:
    preview = arguments.get("preview") or {}
    if not isinstance(preview, dict):
        return tool_error(
            "preview must be an object",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.preview_inspect",
        )
    inspection = inspect_preview(preview)
    return {"status": "success", "inspector": mask_pii(inspection)}


@trace_external_call("mcp.pipeline_create_from_plan")
async def _pipeline_create_from_plan(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan_obj = arguments.get("plan") or {}
    try:
        plan = PipelinePlan.model_validate(plan_obj)
    except Exception as exc:
        logger.warning("pipeline_create_from_plan validation failed: %s", exc, exc_info=True)
        return tool_error(
            f"Invalid plan: {exc}",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.create_from_plan.validate",
        )
    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        return tool_error(
            "plan.data_scope.db_name is required",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.create_from_plan.validate",
        )

    pipeline_name = str(arguments.get("name") or "").strip()
    if not pipeline_name:
        return tool_error(
            "name is required",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.create_from_plan.validate",
        )
    location = str(arguments.get("location") or "").strip()
    if not location:
        return tool_error(
            "location is required",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.create_from_plan.validate",
        )

    payload: Dict[str, Any] = {
        "db_name": db_name,
        "name": pipeline_name,
        "location": location,
        "branch": str(arguments.get("branch") or plan.data_scope.branch or "main").strip() or "main",
        "pipeline_type": str(arguments.get("pipeline_type") or "batch").strip() or "batch",
        "definition_json": dict(plan.definition_json or {}),
    }
    description = str(arguments.get("description") or "").strip() or None
    if description:
        payload["description"] = description
    pipeline_id = str(arguments.get("pipeline_id") or "").strip() or None
    if pipeline_id:
        payload["pipeline_id"] = pipeline_id

    principal_id = str(arguments.get("principal_id") or "").strip() or None
    principal_type = str(arguments.get("principal_type") or "").strip() or None
    resp = await bff_json(
        "POST",
        "/pipelines",
        db_name=db_name,
        principal_id=principal_id,
        principal_type=principal_type,
        json_body=payload,
        timeout_seconds=30.0,
    )
    if resp.get("error"):
        # Upsert behavior: if the pipeline already exists, update its definition.
        if int(resp.get("status_code") or 0) == 409 and isinstance(resp.get("response"), dict):
            resp_body = resp.get("response") or {}
            detail = resp_body.get("detail") if isinstance(resp_body.get("detail"), dict) else {}
            detail_code = str(detail.get("code") or "").strip().upper()
            enterprise = resp_body.get("enterprise") if isinstance(resp_body.get("enterprise"), dict) else {}
            legacy_code = str(enterprise.get("legacy_code") or "").strip().upper()
            if detail_code == "PIPELINE_ALREADY_EXISTS" or legacy_code == "PIPELINE_ALREADY_EXISTS":
                branch = str(payload.get("branch") or "main").strip() or "main"
                list_resp = await bff_json(
                    "GET",
                    "/pipelines",
                    db_name=db_name,
                    principal_id=principal_id,
                    principal_type=principal_type,
                    params={"db_name": db_name, "limit": 200},
                    timeout_seconds=30.0,
                )
                if not list_resp.get("error"):
                    data = list_resp.get("data") if isinstance(list_resp.get("data"), dict) else {}
                    items = data.get("pipelines") if isinstance(data.get("pipelines"), list) else []
                    match = None
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        if str(item.get("name") or "").strip() != pipeline_name:
                            continue
                        if str(item.get("branch") or "main").strip() != branch:
                            continue
                        match = item
                        break
                    pipeline_id_existing = (
                        str((match or {}).get("pipeline_id") or (match or {}).get("pipelineId") or "").strip()
                        if isinstance(match, dict)
                        else ""
                    )
                    if pipeline_id_existing:
                        update_resp = await bff_json(
                            "PUT",
                            f"/pipelines/{pipeline_id_existing}",
                            db_name=db_name,
                            principal_id=principal_id,
                            principal_type=principal_type,
                            json_body={"definition_json": dict(plan.definition_json or {})},
                            timeout_seconds=30.0,
                        )
                        if update_resp.get("error"):
                            return update_resp
                        update_data = update_resp.get("data") if isinstance(update_resp.get("data"), dict) else {}
                        pipeline = update_data.get("pipeline") if isinstance(update_data.get("pipeline"), dict) else {}
                        return {
                            "status": "success",
                            "reused": True,
                            "pipeline": {
                                "pipeline_id": pipeline.get("pipeline_id")
                                or pipeline.get("pipelineId")
                                or pipeline_id_existing,
                                "db_name": pipeline.get("db_name") or db_name,
                                "name": pipeline.get("name") or pipeline_name,
                                "branch": pipeline.get("branch") or branch,
                                "location": pipeline.get("location") or location,
                                "version": pipeline.get("version") or pipeline.get("commit_id") or pipeline.get("commitId"),
                            },
                        }
        return resp
    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
    pipeline = data.get("pipeline") if isinstance(data.get("pipeline"), dict) else {}
    return {
        "status": "success",
        "pipeline": {
            "pipeline_id": pipeline.get("pipeline_id") or pipeline.get("pipelineId"),
            "db_name": pipeline.get("db_name"),
            "name": pipeline.get("name"),
            "branch": pipeline.get("branch"),
            "location": pipeline.get("location"),
            "version": pipeline.get("version") or pipeline.get("commit_id") or pipeline.get("commitId"),
        },
    }


@trace_external_call("mcp.pipeline_update_from_plan")
async def _pipeline_update_from_plan(_server: Any, arguments: Dict[str, Any]) -> Any:
    pipeline_id = str(arguments.get("pipeline_id") or "").strip()
    if not pipeline_id:
        return tool_error(
            "pipeline_id is required",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.update_from_plan.validate",
        )
    plan_obj = arguments.get("plan") or {}
    try:
        plan = PipelinePlan.model_validate(plan_obj)
    except Exception as exc:
        logger.warning("pipeline_update_from_plan validation failed: %s", exc, exc_info=True)
        return tool_error(
            f"Invalid plan: {exc}",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.update_from_plan.validate",
        )
    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        return tool_error(
            "plan.data_scope.db_name is required",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.update_from_plan.validate",
        )
    payload: Dict[str, Any] = {"definition_json": dict(plan.definition_json or {})}
    branch = str(arguments.get("branch") or "").strip() or None
    if branch:
        payload["branch"] = branch
    principal_id = str(arguments.get("principal_id") or "").strip() or None
    principal_type = str(arguments.get("principal_type") or "").strip() or None
    resp = await bff_json(
        "PUT",
        f"/pipelines/{pipeline_id}",
        db_name=db_name,
        principal_id=principal_id,
        principal_type=principal_type,
        json_body=payload,
        timeout_seconds=30.0,
    )
    if resp.get("error"):
        return resp
    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
    pipeline = data.get("pipeline") if isinstance(data.get("pipeline"), dict) else {}
    return {
        "status": "success",
        "pipeline": {
            "pipeline_id": pipeline.get("pipeline_id") or pipeline.get("pipelineId"),
            "db_name": pipeline.get("db_name"),
            "name": pipeline.get("name"),
            "branch": pipeline.get("branch"),
            "location": pipeline.get("location"),
            "version": pipeline.get("version") or pipeline.get("commit_id") or pipeline.get("commitId"),
        },
    }


@trace_external_call("mcp.pipeline_preview_wait")
async def _pipeline_preview_wait(server: Any, arguments: Dict[str, Any]) -> Any:
    pipeline_id = str(arguments.get("pipeline_id") or "").strip()
    if not pipeline_id:
        return tool_error(
            "pipeline_id is required",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.preview_wait.validate",
        )
    return await _pipeline_wait_for_mode(
        server,
        pipeline_id=pipeline_id,
        mode="preview",
        enqueue_path=f"/pipelines/{pipeline_id}/preview",
        output_builder=_preview_output,
        enqueue_job_id_extractor=_preview_job_id,
        timeout_message="preview still running",
        arguments=arguments,
    )


@trace_external_call("mcp.pipeline_build_wait")
async def _pipeline_build_wait(server: Any, arguments: Dict[str, Any]) -> Any:
    pipeline_id = str(arguments.get("pipeline_id") or "").strip()
    if not pipeline_id:
        return tool_error(
            "pipeline_id is required",
            status_code=400,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            operation="pipeline.build_wait.validate",
        )
    return await _pipeline_wait_for_mode(
        server,
        pipeline_id=pipeline_id,
        mode="build",
        enqueue_path=f"/pipelines/{pipeline_id}/build",
        output_builder=_build_output,
        enqueue_job_id_extractor=_build_job_id,
        timeout_message="build still running",
        arguments=arguments,
    )


@trace_external_call("mcp.pipeline_deploy_promote_build")
async def _pipeline_deploy_promote_build(server: Any, arguments: Dict[str, Any]) -> Any:
    pipeline_id = str(arguments.get("pipeline_id") or "").strip()
    if not pipeline_id:
        return {"status": "invalid", "errors": ["pipeline_id is required"]}
    build_job_id = str(arguments.get("build_job_id") or "").strip()
    if not build_job_id:
        return {"status": "invalid", "errors": ["build_job_id is required"]}
    node_id = str(arguments.get("node_id") or "").strip()
    if not node_id:
        return {"status": "invalid", "errors": ["node_id is required"]}
    db_name = str(arguments.get("db_name") or "").strip()
    dataset_name = str(arguments.get("dataset_name") or "").strip()
    if not db_name or not dataset_name:
        return {"status": "invalid", "errors": ["db_name and dataset_name are required"]}
    principal_id = str(arguments.get("principal_id") or "").strip() or None
    principal_type = str(arguments.get("principal_type") or "").strip() or None
    branch = str(arguments.get("branch") or "").strip() or None
    definition_json = arguments.get("definition_json") if isinstance(arguments.get("definition_json"), dict) else None
    pipeline_spec_commit_id = str(arguments.get("pipeline_spec_commit_id") or "").strip() or None

    # Deploy hash mismatches happen when the pipeline definition changes between build and deploy.
    # Prefer the exact build snapshot (pipeline_spec_commit_id -> pipeline_versions.definition_json).
    if not definition_json:
        if not pipeline_spec_commit_id:
            runs = await bff_json(
                "GET",
                f"/pipelines/{pipeline_id}/runs",
                db_name=db_name,
                principal_id=principal_id,
                principal_type=principal_type,
                params={"limit": 50},
                timeout_seconds=15.0,
            )
            if not runs.get("error"):
                runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
                run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
                selected_run: Optional[Dict[str, Any]] = None
                for item in run_list:
                    if not isinstance(item, dict):
                        continue
                    if str(item.get("job_id") or "").strip() == build_job_id:
                        selected_run = item
                        break
                if selected_run:
                    output_json = selected_run.get("output_json") if isinstance(selected_run.get("output_json"), dict) else {}
                    pipeline_spec_commit_id = (
                        str(selected_run.get("pipeline_spec_commit_id") or output_json.get("pipeline_spec_commit_id") or "").strip()
                        or None
                    )
        if pipeline_spec_commit_id:
            try:
                pipeline_registry = await server._ensure_pipeline_registry()
                version = await pipeline_registry.get_version(
                    pipeline_id=pipeline_id,
                    lakefs_commit_id=pipeline_spec_commit_id,
                    branch=branch,
                )
                if version:
                    definition_json = dict(version.definition_json or {})
            except Exception as exc:
                logger.warning(
                    "pipeline_deploy_promote_build: failed to fetch pipeline version snapshot pipeline_id=%s commit=%s: %s",
                    pipeline_id,
                    pipeline_spec_commit_id,
                    exc,
                )

    payload: Dict[str, Any] = {
        "promote_build": True,
        "build_job_id": build_job_id,
        "node_id": node_id,
        "output": {"db_name": db_name, "dataset_name": dataset_name},
        "replay_on_deploy": bool(arguments.get("replay_on_deploy") or False),
    }
    if definition_json:
        payload["definition_json"] = definition_json
    artifact_id = str(arguments.get("artifact_id") or "").strip() or None
    if artifact_id:
        payload["artifact_id"] = artifact_id
    if branch:
        payload["branch"] = branch

    resp = await bff_json(
        "POST",
        f"/pipelines/{pipeline_id}/deploy",
        db_name=db_name,
        principal_id=principal_id,
        principal_type=principal_type,
        json_body=payload,
        timeout_seconds=60.0,
    )
    if resp.get("error"):
        # Common and expected race: deploy called before build completes.
        if resp.get("status_code") == 409 and isinstance(resp.get("response"), dict):
            detail = resp.get("response", {}).get("detail")
            if isinstance(detail, dict) and str(detail.get("code") or "").strip() == "BUILD_NOT_SUCCESS":
                return {
                    "status": "not_ready",
                    "pipeline_id": pipeline_id,
                    "build_job_id": build_job_id,
                    "build_status": detail.get("build_status") or detail.get("buildStatus"),
                    "errors": detail.get("errors"),
                    "message": detail.get("message") or "Build is not successful yet",
                }
            if isinstance(detail, dict) and str(detail.get("code") or "").strip() == ExternalErrorCode.REPLAY_REQUIRED.value:
                return {
                    "status": "replay_required",
                    "pipeline_id": pipeline_id,
                    "build_job_id": build_job_id,
                    "node_id": node_id,
                    "db_name": db_name,
                    "dataset_name": dataset_name,
                    "code": ExternalErrorCode.REPLAY_REQUIRED.value,
                    "message": detail.get("message") or resp.get("error") or "Replay is required to deploy",
                    "detail": detail,
                    "hint": "Retry with replay_on_deploy=true OR deploy to a new dataset_name.",
                }
            if isinstance(detail, str) and "definition" in detail.lower() and "match" in detail.lower():
                return {
                    "status": "conflict",
                    "pipeline_id": pipeline_id,
                    "code": ExternalErrorCode.DEFINITION_MISMATCH.value,
                    "message": detail,
                    "hint": "Pass definition_json (exact build snapshot) or pipeline_spec_commit_id from the build output.",
                }
        return resp
    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
    outputs = data.get("outputs") or []

    dataset_ids = []
    dataset_version_ids = []
    for output in outputs:
        if isinstance(output, dict):
            ds_id = str(output.get("dataset_id") or "").strip()
            dv_id = str(output.get("dataset_version_id") or "").strip()
            if ds_id:
                dataset_ids.append(ds_id)
            if dv_id:
                dataset_version_ids.append(dv_id)

    return {
        "status": "success",
        "pipeline_id": data.get("pipeline_id") or pipeline_id,
        "job_id": data.get("job_id"),
        "deployed_commit_id": data.get("deployed_commit_id"),
        "artifact_id": data.get("artifact_id"),
        "outputs": outputs,
        "dataset_ids": dataset_ids,
        "dataset_version_ids": dataset_version_ids,
        "definition_included": bool(definition_json),
        "pipeline_spec_commit_id": pipeline_spec_commit_id,
    }


PIPELINE_TOOL_HANDLERS: Dict[str, ToolHandler] = {
    "preview_inspect": _preview_inspect,
    "pipeline_create_from_plan": _pipeline_create_from_plan,
    "pipeline_update_from_plan": _pipeline_update_from_plan,
    "pipeline_preview_wait": _pipeline_preview_wait,
    "pipeline_build_wait": _pipeline_build_wait,
    "pipeline_deploy_promote_build": _pipeline_deploy_promote_build,
}


def build_pipeline_tool_handlers() -> Dict[str, ToolHandler]:
    return dict(PIPELINE_TOOL_HANDLERS)
