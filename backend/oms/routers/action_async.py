"""
OMS async Action router - Action-only writeback submission path.

This endpoint accepts intent-only payloads (action_type_id + input) and writes an ActionCommand
to the Event Store for the action worker to execute.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, Field, field_validator

from oms.dependencies import EventStoreDep, ensure_database_exists
from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2
from oms.services.ontology_resources import OntologyResourceService
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.foundry.compute_routing import choose_writeback_backend
from shared.foundry.spark_on_demand_dispatcher import get_spark_on_demand_dispatcher
from shared.models.commands import ActionCommand
from shared.models.event_envelope import EventEnvelope
from shared.observability.request_context import get_correlation_id
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input, validate_db_name
from shared.services.core.write_path_contract import (
    emit_write_path_contract,
    followup_completed,
    followup_degraded,
    followup_skipped,
)
from shared.services.registries.action_log_registry import ActionLogRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
from shared.services.storage.storage_service import create_storage_service
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.utils.action_template_engine import compile_action_change_shape
from shared.utils.action_simulation_utils import reject_simulation_delete_flag
from shared.utils.resource_rid import format_resource_rid, parse_metadata_rev
from shared.utils.access_policy import apply_access_policy
from shared.utils.action_data_access import evaluate_action_target_data_access
from shared.utils.principal_policy import build_principal_tags
from shared.utils.action_permission_profile import (
    ActionPermissionProfileError,
    requires_action_data_access_enforcement,
    resolve_action_permission_profile,
)
from shared.observability.tracing import trace_endpoint

from oms.services.action_simulation_service import (
    ActionSimulationRejected,
    build_patchset_for_scenario,
    enforce_action_permission,
    preflight_action_writeback,
    simulate_effects_for_patchset,
)
from oms.services.action_submit_service import submit_action_request

logger = logging.getLogger(__name__)

foundry_router = APIRouter(prefix="/v2/ontologies/{ontology}/actions", tags=["Foundry Actions v2"])


def _emit_batch_write_path_contract(
    *,
    followups: List[Dict[str, Any]],
    level: str = "info",
) -> Dict[str, Any]:
    return emit_write_path_contract(
        logger,
        authoritative_write="action_batch_submission",
        followups=followups,
        level=level,
        message_prefix="Action batch write path contract",
    )


class ActionSubmitRequest(BaseModel):
    input: Dict[str, Any] = Field(default_factory=dict, description="Intent-only action input payload")
    correlation_id: Optional[str] = Field(default=None, description="Correlation id for trace/audit")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional metadata")
    base_branch: str = Field("main", description="Base branch for authoritative reads (default: main)")
    overlay_branch: Optional[str] = Field(
        default=None,
        description="Optional overlay branch override (default derived from writeback_target)",
    )


class ActionSubmitResponse(BaseModel):
    action_log_id: str
    status: str
    db_name: str
    action_type_id: str
    ontology_commit_id: str
    base_branch: str
    overlay_branch: str
    writeback_target: Dict[str, Any]


class ActionSubmitBatchDependencyRequest(BaseModel):
    on: str = Field(..., description="Dependency reference (request_id from same batch)")
    trigger_on: str = Field(default="SUCCEEDED", description="SUCCEEDED|FAILED|COMPLETED")

    @field_validator("trigger_on")
    @classmethod
    def _normalize_trigger_on(cls, value: str) -> str:
        normalized = str(value or "").strip().upper() or "SUCCEEDED"
        if normalized not in {"SUCCEEDED", "FAILED", "COMPLETED"}:
            raise ValueError("trigger_on must be one of SUCCEEDED|FAILED|COMPLETED")
        return normalized


class ActionSubmitBatchItemRequest(BaseModel):
    request_id: Optional[str] = Field(default=None, description="Client item id for dependency references")
    input: Dict[str, Any] = Field(default_factory=dict, description="Intent-only action input payload")
    correlation_id: Optional[str] = Field(default=None, description="Correlation id for trace/audit")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional metadata")
    base_branch: Optional[str] = Field(default=None, description="Optional per-item base branch override")
    overlay_branch: Optional[str] = Field(default=None, description="Optional per-item overlay branch override")
    depends_on: List[str] = Field(default_factory=list, description="Legacy alias for dependencies[].on")
    trigger_on: str = Field(default="SUCCEEDED", description="Legacy alias for dependencies[].trigger_on")
    dependencies: List[ActionSubmitBatchDependencyRequest] = Field(
        default_factory=list,
        description="Dependency list. Item runs only when all dependencies are satisfied.",
    )


class ActionSubmitBatchRequest(BaseModel):
    items: List[ActionSubmitBatchItemRequest] = Field(default_factory=list, min_length=1, max_length=500)
    base_branch: str = Field(default="main", description="Default base branch for items")
    overlay_branch: Optional[str] = Field(default=None, description="Default overlay branch for items")


class ActionSubmitBatchItemResponse(BaseModel):
    index: int
    request_id: str
    action_log_id: str
    status: str
    depends_on: List[str] = Field(default_factory=list)


class ActionSubmitBatchResponse(BaseModel):
    batch_id: str
    db_name: str
    action_type_id: str
    items: List[ActionSubmitBatchItemResponse]


class ActionSimulateScenarioRequest(BaseModel):
    scenario_id: Optional[str] = Field(default=None, description="Optional client-provided scenario identifier")
    conflict_policy: Optional[str] = Field(
        default=None,
        description="Optional conflict_policy override for this scenario (WRITEBACK_WINS|BASE_WINS|FAIL|MANUAL_REVIEW)",
    )


class ActionSimulateStatePatch(BaseModel):
    """Patch-like state override for decision simulation (what-if)."""

    set: Dict[str, Any] = Field(default_factory=dict)
    unset: List[str] = Field(default_factory=list)
    link_add: List[Any] = Field(default_factory=list)
    link_remove: List[Any] = Field(default_factory=list)
    delete: bool = Field(default=False, description="delete is not supported for simulation assumptions")
    _reject_delete = field_validator("delete")(reject_simulation_delete_flag)


class ActionSimulateObservedBaseOverrides(BaseModel):
    """Override observed_base snapshot fields/links to simulate stale reads."""

    fields: Dict[str, Any] = Field(default_factory=dict)
    links: Dict[str, Any] = Field(default_factory=dict)


class ActionSimulateTargetAssumption(BaseModel):
    class_id: str
    instance_id: str
    base_overrides: Optional[ActionSimulateStatePatch] = None
    observed_base_overrides: Optional[ActionSimulateObservedBaseOverrides] = None


class ActionSimulateAssumptions(BaseModel):
    targets: List[ActionSimulateTargetAssumption] = Field(
        default_factory=list,
        description="Per-target state injections (Level 2 what-if base state assumptions).",
    )


class ActionSimulateRequest(BaseModel):
    input: Dict[str, Any] = Field(default_factory=dict, description="Intent-only action input payload")
    correlation_id: Optional[str] = Field(default=None, description="Correlation id for trace/audit")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional metadata")
    base_branch: str = Field("main", description="Base branch for authoritative reads (default: main)")
    overlay_branch: Optional[str] = Field(
        default=None,
        description="Optional overlay branch override (default derived from writeback_target)",
    )
    scenarios: Optional[list[ActionSimulateScenarioRequest]] = Field(
        default=None,
        description="Optional scenario list (policy comparisons). If omitted, server simulates the effective policy only.",
    )
    use_branch_head: bool = Field(
        default=False,
        description="Use branch HEAD instead of deployed commit for action type resolution (dev/test only)",
    )
    include_effects: bool = Field(default=True, description="If true, compute downstream lakeFS/ES overlay effects")
    assumptions: Optional[ActionSimulateAssumptions] = Field(
        default=None,
        description="Optional decision simulation assumptions (Level 2 state injection).",
    )


class ApplyActionRequestOptionsV2(BaseModel):
    mode: Optional[str] = None


class ApplyActionRequestV2(BaseModel):
    options: Optional[ApplyActionRequestOptionsV2] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = Field(default=None, alias="correlationId")


class ApplyActionBatchRequestItemV2(BaseModel):
    parameters: Dict[str, Any] = Field(default_factory=dict)


class ApplyActionBatchRequestOptionsV2(BaseModel):
    return_edits: Optional[str] = Field(default=None, alias="returnEdits")


class ApplyActionBatchRequestV2(BaseModel):
    options: Optional[ApplyActionBatchRequestOptionsV2] = None
    requests: List[ApplyActionBatchRequestItemV2] = Field(default_factory=list, min_length=1, max_length=20)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = Field(default=None, alias="correlationId")


async def _ensure_ontology_database_exists(ontology: str) -> str:
    return await ensure_database_exists(db_name=validate_db_name(ontology))


def _resolve_v2_apply_mode(*, explicit_mode: Optional[str]) -> str:
    mode = str(explicit_mode or "").strip().upper()
    if not mode:
        return "VALIDATE_AND_EXECUTE"
    if mode not in {"VALIDATE_ONLY", "VALIDATE_AND_EXECUTE"}:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "options.mode must be VALIDATE_ONLY or VALIDATE_AND_EXECUTE",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    return mode


def _default_action_parameter_results(parameters: Dict[str, Any] | None) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    if not isinstance(parameters, dict):
        return results
    for raw_name in parameters.keys():
        name = str(raw_name or "").strip()
        if not name:
            continue
        results[name] = {
            "required": True,
            "evaluatedConstraints": [],
            "result": "VALID",
        }
    return results


_OBJECT_TYPE_KEY_CANDIDATES = ("class_id", "classId", "objectTypeApiName", "objectType")
_INSTANCE_KEY_CANDIDATES = ("instance_id", "instanceId", "primaryKey", "id")


def _normalize_locator_value(value: Any) -> str:
    return str(value or "").strip()


def _extract_first_non_empty(payload: Dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        candidate = _normalize_locator_value(payload.get(key))
        if candidate:
            return candidate
    return ""


def _collect_writeback_target_markers(payload: Any, markers: Set[tuple[str, str]]) -> None:
    if isinstance(payload, dict):
        object_type = _extract_first_non_empty(payload, _OBJECT_TYPE_KEY_CANDIDATES)
        instance_id = _extract_first_non_empty(payload, _INSTANCE_KEY_CANDIDATES)
        if object_type and instance_id:
            markers.add((object_type, instance_id))
        for value in payload.values():
            _collect_writeback_target_markers(value, markers)
        return
    if isinstance(payload, list):
        for value in payload:
            _collect_writeback_target_markers(value, markers)


def _estimate_writeback_object_count(parameters: Dict[str, Any] | None) -> int:
    if not isinstance(parameters, dict) or not parameters:
        return 0
    markers: Set[tuple[str, str]] = set()
    _collect_writeback_target_markers(parameters, markers)
    if markers:
        return len(markers)
    return 1


def _resolve_writeback_execution_backend(*, spark_routed: bool) -> str:
    if spark_routed:
        return "spark_on_demand"
    return "index_pruning"


def _build_writeback_compute_routing_metadata(
    *,
    action_type_id: str,
    branch: str,
    parameters_list: List[Dict[str, Any]],
) -> Dict[str, Any]:
    settings = get_settings()
    estimated_count = sum(_estimate_writeback_object_count(parameters) for parameters in parameters_list)
    decision = choose_writeback_backend(
        estimated_count=estimated_count,
        threshold=int(settings.ontology.writeback_spark_threshold),
    )
    execution_backend = _resolve_writeback_execution_backend(
        spark_routed=decision.spark_routed,
    )
    decision_metadata = decision.as_metadata(
        execution_backend=execution_backend,
    )
    routing_metadata = {
        **decision_metadata,
        "action_type_id": action_type_id,
        "branch": branch,
        "request_count": len(parameters_list),
    }
    logger.info(
        "Writeback compute route (action=%s branch=%s decision=%s)",
        action_type_id,
        branch,
        routing_metadata,
    )
    return routing_metadata


def _stamp_compute_routing_metadata_on_submit_items(
    *,
    request: ActionSubmitBatchRequest,
    routing_metadata: Dict[str, Any],
) -> None:
    stamped = dict(routing_metadata or {})
    for item in request.items:
        item_metadata = dict(item.metadata or {})
        item_metadata["__compute_routing"] = dict(stamped)
        item.metadata = item_metadata


async def _submit_action_batch_with_compute_backend(
    *,
    db_name: str,
    action_type_id: str,
    request: ActionSubmitBatchRequest,
    event_store: Any,
    routing_metadata: Dict[str, Any],
) -> ActionSubmitBatchResponse:
    execution_backend = str(routing_metadata.get("execution_backend") or "index_pruning").strip().lower()
    if execution_backend != "spark_on_demand":
        return await submit_action_batch_async(
            db_name=db_name,
            action_type_id=action_type_id,
            request=request,
            event_store=event_store,
        )

    dispatcher = get_spark_on_demand_dispatcher()
    dispatch_result = await dispatcher.dispatch(
        route="writeback",
        payload={
            "db_name": db_name,
            "action_type_id": action_type_id,
            "branch": request.base_branch,
            "request_count": len(request.items),
        },
        execute=lambda context: _submit_action_batch_via_on_demand_job(
            db_name=db_name,
            action_type_id=action_type_id,
            request=request,
            event_store=event_store,
            spark_job_id=context.job_id,
        ),
        timeout_seconds=60.0,
    )
    routing_metadata["spark_job_id"] = dispatch_result.job_id
    routing_metadata["queue_wait_ms"] = dispatch_result.queue_wait_ms
    routing_metadata["execution_ms"] = dispatch_result.execution_ms
    _stamp_compute_routing_metadata_on_submit_items(
        request=request,
        routing_metadata=routing_metadata,
    )
    return dispatch_result.result


async def _submit_action_batch_via_on_demand_job(
    *,
    db_name: str,
    action_type_id: str,
    request: ActionSubmitBatchRequest,
    event_store: Any,
    spark_job_id: str,
) -> ActionSubmitBatchResponse:
    for item in request.items:
        item_metadata = dict(item.metadata or {})
        routing = dict(item_metadata.get("__compute_routing") or {})
        routing["spark_job_id"] = spark_job_id
        item_metadata["__compute_routing"] = routing
        item.metadata = item_metadata
    return await submit_action_batch_async(
        db_name=db_name,
        action_type_id=action_type_id,
        request=request,
        event_store=event_store,
    )


def _foundry_valid_action_validation_payload_for_parameters(
    *,
    parameters: Dict[str, Any] | None,
    action_log_id: Optional[str] = None,
    writeback_status: Optional[str] = None,
    side_effect_delivery: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    parameter_results = _default_action_parameter_results(parameters)
    payload: Dict[str, Any] = {
        "validation": {
            "result": "VALID",
            "submissionCriteria": [],
            "parameters": parameter_results,
        },
        "parameters": parameter_results,
    }
    if action_log_id:
        payload["action_log_id"] = action_log_id
        payload["auditLogId"] = action_log_id
    if writeback_status:
        payload["writebackStatus"] = writeback_status
    if side_effect_delivery is not None:
        payload["sideEffectDelivery"] = side_effect_delivery
    return payload


def _resolve_writeback_target(
    *,
    db_name: str,
    raw_target: Dict[str, Any],
) -> Dict[str, Any]:
    repo = str(raw_target.get("repo") or AppConfig.ONTOLOGY_WRITEBACK_REPO).strip()
    branch_tmpl = str(raw_target.get("branch") or AppConfig.get_ontology_writeback_branch(db_name)).strip()
    if not repo:
        raise ValueError("writeback_target.repo is required")
    if not branch_tmpl:
        raise ValueError("writeback_target.branch is required")
    branch = branch_tmpl.replace("{db_name}", db_name).replace("{db}", db_name)
    branch = AppConfig.sanitize_lakefs_branch_id(branch)
    return {"repo": repo, "branch": branch}


def _normalize_batch_dependency_entries(item: ActionSubmitBatchItemRequest) -> List[ActionSubmitBatchDependencyRequest]:
    deps: List[ActionSubmitBatchDependencyRequest] = []
    if item.dependencies:
        deps.extend(item.dependencies)
    elif item.depends_on:
        for ref in item.depends_on:
            deps.append(ActionSubmitBatchDependencyRequest(on=ref, trigger_on=item.trigger_on))
    return deps


def _validate_batch_dependency_graph(
    *,
    request_ids: List[str],
    dependencies_by_request_id: Dict[str, List[ActionSubmitBatchDependencyRequest]],
) -> None:
    request_id_set = set(request_ids)
    for req_id, deps in dependencies_by_request_id.items():
        for dep in deps:
            dep_id = str(dep.on or "").strip()
            if not dep_id:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    f"Batch dependency is missing reference for request_id={req_id}",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )
            if dep_id not in request_id_set:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    f"Batch dependency target not found: {dep_id}",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )
            if dep_id == req_id:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    f"Batch dependency cannot reference itself: {req_id}",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )

    visiting: Set[str] = set()
    visited: Set[str] = set()

    def _dfs(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Batch dependency cycle detected",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        visiting.add(node)
        for dep in dependencies_by_request_id.get(node, []):
            _dfs(str(dep.on))
        visiting.remove(node)
        visited.add(node)

    for req_id in request_ids:
        _dfs(req_id)


def _resolve_batch_cleanup_action_log_ids(
    *,
    normalized_items: List[tuple[str, ActionSubmitBatchItemRequest]],
    submit_results: Dict[str, ActionSubmitResponse],
    emitted_request_ids: Set[str],
) -> List[str]:
    cleanup_ids: List[str] = []
    for request_id, _item in normalized_items:
        if request_id in emitted_request_ids:
            continue
        submit_resp = submit_results.get(request_id)
        if submit_resp is None:
            continue
        cleanup_ids.append(submit_resp.action_log_id)
    return cleanup_ids


async def submit_action_async(
    db_name: str,
    action_type_id: str,
    *,
    request: ActionSubmitRequest,
    base_branch: Optional[str] = None,
    event_store: Any,
) -> ActionSubmitResponse:
    """
    Internal helper that submits one Action for async execution.

    The action worker is responsible for:
    - permission checks
    - patchset computation
    - lakeFS writeback commit
    - ActionApplied emission
    """
    payload = await submit_action_request(
        db_name=db_name,
        action_type_id=action_type_id,
        request_payload=request.model_dump(),
        base_branch_alias=base_branch,
        event_store=event_store,
        deployments_factory=OntologyDeploymentRegistryV2,
        resources_factory=OntologyResourceService,
        settings_getter=get_settings,
        storage_factory=create_storage_service,
        dataset_registry_factory=DatasetRegistry,
        compile_action_change_shape_fn=compile_action_change_shape,
        evaluate_action_target_data_access_fn=evaluate_action_target_data_access,
    )
    return ActionSubmitResponse(**payload)


@trace_endpoint("oms.action.submit_batch")
async def submit_action_batch_async(
    db_name: str = Depends(ensure_database_exists),
    action_type_id: str = Path(..., description="Action type identifier"),
    *,
    request: ActionSubmitBatchRequest,
    event_store=EventStoreDep,
) -> ActionSubmitBatchResponse:
    action_type_id = str(action_type_id or "").strip()
    if not action_type_id:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "action_type_id is required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    if not request.items:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "items must not be empty",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    batch_id = uuid4().hex
    request_ids: List[str] = []
    dependencies_by_request_id: Dict[str, List[ActionSubmitBatchDependencyRequest]] = {}
    normalized_items: List[tuple[str, ActionSubmitBatchItemRequest]] = []

    for idx, item in enumerate(request.items):
        request_id = str(item.request_id or "").strip() or f"item-{idx}"
        if request_id in dependencies_by_request_id:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"Duplicate request_id in batch: {request_id}",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        request_ids.append(request_id)
        dependencies_by_request_id[request_id] = _normalize_batch_dependency_entries(item)
        normalized_items.append((request_id, item))

    _validate_batch_dependency_graph(
        request_ids=request_ids,
        dependencies_by_request_id=dependencies_by_request_id,
    )

    class _NoopEventStore:
        async def append_event(self, _envelope: Any) -> None:  # noqa: ANN401
            return None

    noop_event_store = _NoopEventStore()
    submit_results: Dict[str, ActionSubmitResponse] = {}
    dependency_registry: Optional[ActionLogRegistry] = None
    emitted_request_ids: Set[str] = set()
    dependency_edges_registered = 0

    try:
        for request_id, item in normalized_items:
            item_base_branch = str(item.base_branch or request.base_branch or "main").strip() or "main"
            item_overlay_branch = str(item.overlay_branch or request.overlay_branch or "").strip() or None
            item_metadata = dict(item.metadata or {})
            item_metadata["__batch"] = {"batch_id": batch_id, "request_id": request_id}
            item_metadata["__batch_payload"] = sanitize_input(item.input)
            deps = dependencies_by_request_id.get(request_id) or []
            if deps:
                item_metadata["__dependency"] = {
                    "depends_on": [str(dep.on) for dep in deps],
                    "trigger_on": [str(dep.trigger_on) for dep in deps],
                }

            submit_req = ActionSubmitRequest(
                input=item.input,
                correlation_id=item.correlation_id,
                metadata=item_metadata,
                base_branch=item_base_branch,
                overlay_branch=item_overlay_branch,
            )
            submit_resp = await submit_action_async(
                db_name=db_name,
                action_type_id=action_type_id,
                request=submit_req,
                base_branch=None,
                event_store=noop_event_store,
            )
            submit_results[request_id] = submit_resp

        write_path_followups: List[Dict[str, Any]] = [
            followup_completed(
                "batch_action_logs",
                details={"count": len(submit_results)},
            )
        ]

        dependency_registry = ActionLogRegistry()
        await dependency_registry.connect()
        for child_request_id, deps in dependencies_by_request_id.items():
            if not deps:
                continue
            child_log_id = submit_results[child_request_id].action_log_id
            for dep in deps:
                parent_request_id = str(dep.on)
                parent_log_id = submit_results[parent_request_id].action_log_id
                await dependency_registry.add_dependency(
                    child_action_log_id=child_log_id,
                    parent_action_log_id=parent_log_id,
                    trigger_on=str(dep.trigger_on),
                )
                dependency_edges_registered += 1

        if dependency_edges_registered:
            write_path_followups.append(
                followup_completed(
                    "batch_dependencies",
                    details={"count": dependency_edges_registered},
                )
            )
        else:
            write_path_followups.append(
                followup_skipped(
                    "batch_dependencies",
                    details={"reason": "none_declared"},
                )
            )

        # Emit root commands only after dependencies are fully registered.
        for request_id, item in normalized_items:
            if dependencies_by_request_id.get(request_id):
                continue
            submit_resp = submit_results[request_id]
            correlation_id = (item.correlation_id or get_correlation_id() or "").strip() or None
            payload = sanitize_input(item.input)
            metadata_payload = {
                **(item.metadata or {}),
                "__batch": {"batch_id": batch_id, "request_id": request_id},
                "correlation_id": correlation_id,
                "submitted_at": datetime.now(timezone.utc).isoformat(),
                "ontology": {"ref": f"branch:{submit_resp.base_branch}", "commit": submit_resp.ontology_commit_id},
            }
            command = ActionCommand(
                command_id=UUID(submit_resp.action_log_id),
                db_name=db_name,
                action_log_id=UUID(submit_resp.action_log_id),
                action_type_id=action_type_id,
                ontology_commit_id=submit_resp.ontology_commit_id,
                base_branch=submit_resp.base_branch,
                overlay_branch=submit_resp.overlay_branch,
                correlation_id=correlation_id,
                payload=payload,
                metadata=metadata_payload,
            )
            envelope = EventEnvelope.from_command(
                command,
                actor=str((item.metadata or {}).get("user_id") or ""),
                kafka_topic=AppConfig.ACTION_COMMANDS_TOPIC,
                metadata={"service": "oms", "mode": "action_writeback_batch"},
            )
            await event_store.append_event(envelope)
            emitted_request_ids.add(request_id)

        write_path_followups.append(
            followup_completed(
                "batch_root_command_events",
                details={"count": len(emitted_request_ids)},
            )
        )
        write_path_followups.append(
            followup_skipped(
                "batch_cleanup",
                details={"reason": "not_needed"},
            )
        )
        _emit_batch_write_path_contract(followups=write_path_followups)

        response_items: List[ActionSubmitBatchItemResponse] = []
        for idx, (request_id, _item) in enumerate(normalized_items):
            submit_resp = submit_results[request_id]
            deps = [str(dep.on) for dep in dependencies_by_request_id.get(request_id, [])]
            response_items.append(
                ActionSubmitBatchItemResponse(
                    index=idx,
                    request_id=request_id,
                    action_log_id=submit_resp.action_log_id,
                    status="WAITING_DEPENDENCY" if deps else submit_resp.status,
                    depends_on=deps,
                )
            )

        return ActionSubmitBatchResponse(
            batch_id=batch_id,
            db_name=db_name,
            action_type_id=action_type_id,
            items=response_items,
        )
    except Exception as exc:
        cleanup_count = 0
        if submit_results:
            registry_for_cleanup = dependency_registry
            cleanup_registry_created = False
            if registry_for_cleanup is None:
                try:
                    registry_for_cleanup = ActionLogRegistry()
                    await registry_for_cleanup.connect()
                    cleanup_registry_created = True
                except Exception:
                    logger.warning(
                        "Failed to connect ActionLogRegistry for batch cleanup",
                        exc_info=True,
                    )
                    registry_for_cleanup = None
            if registry_for_cleanup is not None:
                cleanup_payload = {
                    "error": "batch_submission_failed",
                    "message": str(exc),
                    "batch_id": batch_id,
                }
                cleanup_action_log_ids = _resolve_batch_cleanup_action_log_ids(
                    normalized_items=normalized_items,
                    submit_results=submit_results,
                    emitted_request_ids=emitted_request_ids,
                )
                cleanup_count = len(cleanup_action_log_ids)
                for action_log_id in cleanup_action_log_ids:
                    try:
                        await registry_for_cleanup.mark_failed(
                            action_log_id=action_log_id,
                            result=cleanup_payload,
                        )
                    except Exception:
                        logger.warning(
                            "Failed to mark batch action log failed during cleanup",
                            exc_info=True,
                        )
                if cleanup_registry_created:
                    try:
                        await registry_for_cleanup.close()
                    except Exception:
                        logger.warning(
                            "Failed to close ActionLogRegistry after batch cleanup",
                            exc_info=True,
                        )
        failure_followups: List[Dict[str, Any]] = [
            followup_completed(
                "batch_action_logs",
                details={"count": len(submit_results)},
            )
        ]
        if dependency_edges_registered:
            failure_followups.append(
                followup_completed(
                    "batch_dependencies",
                    details={"count": dependency_edges_registered},
                )
            )
        else:
            failure_followups.append(
                followup_skipped(
                    "batch_dependencies",
                    details={"reason": "none_registered"},
                )
            )
        failure_followups.append(
            followup_degraded(
                "batch_root_command_events",
                error=str(exc),
                details={"emitted_count": len(emitted_request_ids)},
            )
        )
        failure_followups.append(
            followup_completed(
                "batch_cleanup",
                details={"count": cleanup_count},
            )
            if cleanup_count
            else followup_skipped(
                "batch_cleanup",
                details={"reason": "no_pending_action_logs"},
            )
        )
        _emit_batch_write_path_contract(followups=failure_followups, level="warning")
        raise
    finally:
        if dependency_registry is not None:
            await dependency_registry.close()


@trace_endpoint("oms.action.simulate")
async def simulate_action_async(
    db_name: str,
    action_type_id: str,
    *,
    request: ActionSimulateRequest,
) -> Dict[str, Any]:
    """
    Simulate an Action writeback (dry-run).

    This endpoint:
    - validates inputs + submission criteria + validation rules + governance gates,
    - computes conflict_policy resolution outcomes,
    - and returns predicted downstream artifacts (patchset / queue / overlay docs),
    without writing to lakeFS / ES / EventStore.
    """

    dataset_registry: Optional[DatasetRegistry] = None
    action_type_rid: Optional[str] = None
    ontology_commit_id: Optional[str] = None
    submitted_by: Optional[str] = None
    submitted_by_type: str = "user"
    preview_action_log_id: Optional[str] = None
    try:
        action_type_id = str(action_type_id or "").strip()
        if not action_type_id:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "action_type_id is required",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        resolved_base_branch = str(request.base_branch or "").strip() or "main"

        if request.use_branch_head:
            ontology_commit_id = f"branch:{resolved_base_branch}"
            logger.info("Simulate using branch ref: %s (dev mode)", ontology_commit_id)
        else:
            deployments = OntologyDeploymentRegistryV2()
            latest = await deployments.get_latest_deployed_commit(db_name=db_name, target_branch=resolved_base_branch)
            if not latest or not latest.get("ontology_commit_id"):
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "No deployed ontology commit found; deploy ontology before simulating actions.",
                    code=ErrorCode.CONFLICT,
                )
            ontology_commit_id = str(latest["ontology_commit_id"])

        resources = OntologyResourceService()
        action_resource = await resources.get_resource(
            db_name,
            branch=ontology_commit_id,
            resource_type="action_type",
            resource_id=action_type_id,
        )
        if not action_resource:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"action_type_not_found: {action_type_id}",
                code=ErrorCode.ACTION_TYPE_NOT_FOUND,
            )
        spec = action_resource.get("spec") if isinstance(action_resource, dict) else None
        if not isinstance(spec, dict):
            spec = {}
        try:
            permission_profile = resolve_action_permission_profile(spec)
        except ActionPermissionProfileError as exc:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                f"action_type_permission_profile_invalid for {action_type_id}: {exc}",
                code=ErrorCode.ACTION_TEMPLATE_ERROR,
            ) from exc
        enforce_data_access = requires_action_data_access_enforcement(
            profile=permission_profile,
            global_enforcement=AppConfig.WRITEBACK_ENFORCE_ACTION_DATA_ACCESS,
        )
        action_meta = action_resource.get("metadata") if isinstance(action_resource, dict) else None
        action_type_rid = format_resource_rid(
            resource_type="action_type",
            resource_id=action_type_id,
            rev=parse_metadata_rev(action_meta),
        )

        submitted_by = str((request.metadata or {}).get("user_id") or "").strip()
        submitted_by_type = str((request.metadata or {}).get("user_type") or "user").strip().lower() or "user"
        actor_role = await enforce_action_permission(
            db_name=db_name,
            submitted_by=submitted_by,
            submitted_by_type=submitted_by_type,
            action_spec=spec,
        )

        principal_tags = None
        if submitted_by and submitted_by != "system":
            principal_tags = build_principal_tags(
                principal_type=submitted_by_type,
                principal_id=submitted_by,
                role=actor_role,
            )
        enforce_edit_access = bool(principal_tags)

        settings = get_settings()
        base_storage = create_storage_service(settings)
        if not base_storage:
            raise classified_http_exception(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "StorageService unavailable",
                code=ErrorCode.STORAGE_UNAVAILABLE,
            )
        lakefs_storage = create_lakefs_storage_service(settings)
        if not lakefs_storage:
            raise classified_http_exception(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "LakeFSStorageService unavailable",
                code=ErrorCode.STORAGE_UNAVAILABLE,
            )

        if AppConfig.WRITEBACK_ENFORCE_GOVERNANCE or enforce_data_access or enforce_edit_access:
            dataset_registry = DatasetRegistry()
            await dataset_registry.connect()

        try:
            sanitized_input = sanitize_input(request.input)
        except SecurityViolationError as exc:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                str(exc),
                code=ErrorCode.INPUT_SANITIZATION_FAILED,
            ) from exc

        preflight = await preflight_action_writeback(
            resources=resources,
            base_storage=base_storage,
            dataset_registry=dataset_registry,
            db_name=db_name,
            action_type_id=action_type_id,
            ontology_commit_id=ontology_commit_id,
            action_spec=spec,
            action_type_rid=action_type_rid,
            input_payload=sanitized_input,
            assumptions=request.assumptions.model_dump(exclude_none=True) if request.assumptions is not None else None,
            submitted_by=submitted_by,
            submitted_by_type=submitted_by_type,
            actor_role=actor_role,
            permission_profile=permission_profile,
            base_branch=resolved_base_branch,
            overlay_branch=request.overlay_branch,
        )

        preview_action_log_id = uuid4().hex
        scenarios = request.scenarios or [ActionSimulateScenarioRequest(scenario_id="default", conflict_policy=None)]

        access_policies: Dict[str, Dict[str, Any]] = {}
        if dataset_registry:
            for tgt in preflight.loaded_targets:
                if tgt.class_id in access_policies:
                    continue
                try:
                    rec = await dataset_registry.get_access_policy(
                        db_name=db_name,
                        scope="data_access",
                        subject_type="object_type",
                        subject_id=tgt.class_id,
                    )
                except Exception:
                    logging.getLogger(__name__).warning("Exception fallback at oms/routers/action_async.py:637", exc_info=True)
                    rec = None
                policy = rec.policy if rec and isinstance(rec.policy, dict) else None
                if isinstance(policy, dict) and policy:
                    access_policies[tgt.class_id] = policy

        base_overrides_by_target: Dict[tuple[str, str], Dict[str, Any]] = {}
        if request.assumptions is not None:
            for tgt in request.assumptions.targets or []:
                if tgt.base_overrides is None:
                    continue
                class_id = str(tgt.class_id or "").strip()
                instance_id = str(tgt.instance_id or "").strip()
                if not class_id or not instance_id:
                    continue
                base_overrides_by_target[(class_id, instance_id)] = tgt.base_overrides.model_dump(exclude_none=True)

        results: List[Dict[str, Any]] = []
        for scenario in scenarios:
            scenario_id = str(scenario.scenario_id or "default").strip() or "default"
            conflict_override = str(scenario.conflict_policy or "").strip() or None

            try:
                patchset, targets, conflicts, policies_used = build_patchset_for_scenario(
                    preflight=preflight,
                    action_log_id=preview_action_log_id,
                    conflict_policy_override=conflict_override,
                )
                patchset_id = sha256_canonical_json_prefixed(patchset)

                effects = None
                if request.include_effects:
                    effects = await simulate_effects_for_patchset(
                        base_storage=base_storage,
                        lakefs_storage=lakefs_storage,
                        db_name=db_name,
                        base_branch=preflight.base_branch,
                        overlay_branch=preflight.overlay_branch,
                        writeback_repo=str(preflight.writeback_target.get("repo") or ""),
                        writeback_branch=str(preflight.writeback_target.get("branch") or ""),
                        action_log_id=preview_action_log_id,
                        patchset_id=patchset_id,
                        targets=targets,
                        base_overrides_by_target=base_overrides_by_target or None,
                    )

                # Apply access policy masking to any returned base-derived artifacts (observed_base + overlay docs).
                if access_policies:
                    for tgt in patchset.get("targets") if isinstance(patchset.get("targets"), list) else []:
                        resource_rid = str(tgt.get("resource_rid") or "").strip()
                        class_id = resource_rid.split("@", 1)[0].split(":", 1)[-1] if resource_rid else ""
                        policy = access_policies.get(class_id)
                        if not policy:
                            continue
                        mask_columns = policy.get("mask_columns") or []
                        if isinstance(mask_columns, str):
                            mask_columns = [c.strip() for c in mask_columns.split(",") if c.strip()]
                        if not isinstance(mask_columns, list) or not mask_columns:
                            continue
                        mask_value = policy.get("mask_value")
                        observed = tgt.get("observed_base")
                        if isinstance(observed, dict):
                            fields = observed.get("fields") if isinstance(observed.get("fields"), dict) else {}
                            links = observed.get("links") if isinstance(observed.get("links"), dict) else {}
                            for col in mask_columns:
                                key = str(col or "").strip()
                                if not key:
                                    continue
                                if key in fields:
                                    fields[key] = mask_value
                                if key in links:
                                    links[key] = mask_value
                            observed["fields"] = fields
                            observed["links"] = links
                        conflict = tgt.get("conflict") if isinstance(tgt.get("conflict"), dict) else {}
                        link_conflicts = conflict.get("links") if isinstance(conflict.get("links"), list) else []
                        for item in link_conflicts:
                            if not isinstance(item, dict):
                                continue
                            field = str(item.get("field") or "").strip()
                            if field and field in mask_columns and "value" in item:
                                item["value"] = mask_value

                    for conflict in conflicts:
                        if not isinstance(conflict, dict):
                            continue
                        class_id = str(conflict.get("class_id") or "").strip()
                        policy = access_policies.get(class_id)
                        if not policy:
                            continue
                        mask_columns = policy.get("mask_columns") or []
                        if isinstance(mask_columns, str):
                            mask_columns = [c.strip() for c in mask_columns.split(",") if c.strip()]
                        if not isinstance(mask_columns, list) or not mask_columns:
                            continue
                        mask_value = policy.get("mask_value")
                        for item in conflict.get("links") if isinstance(conflict.get("links"), list) else []:
                            if not isinstance(item, dict):
                                continue
                            field = str(item.get("field") or "").strip()
                            if field and field in mask_columns and "value" in item:
                                item["value"] = mask_value

                    if effects and isinstance(effects, dict):
                        docs = effects.get("es_overlay") if isinstance(effects.get("es_overlay"), dict) else {}
                        per_target = docs.get("documents") if isinstance(docs.get("documents"), list) else []
                        for entry in per_target:
                            if not isinstance(entry, dict):
                                continue
                            class_id = str(entry.get("class_id") or "").strip()
                            policy = access_policies.get(class_id)
                            if not policy:
                                continue
                            overlay_doc = entry.get("overlay_document")
                            data = overlay_doc.get("data") if isinstance(overlay_doc, dict) else None
                            if not isinstance(data, dict):
                                continue
                            filtered, info = apply_access_policy([data], policy=policy)
                            if filtered:
                                overlay_doc["data"] = filtered[0]
                                entry["access_policy"] = {"allowed": True, **(info or {})}
                            else:
                                overlay_doc["data"] = {}
                                entry["access_policy"] = {"allowed": False, **(info or {})}

                results.append(
                    {
                        "scenario_id": scenario_id,
                        "conflict_policy_override": conflict_override,
                        "status": "ACCEPTED",
                        "patchset_id": patchset_id,
                        "conflicts": conflicts,
                        "conflict_policies_used": policies_used,
                        "patchset": patchset,
                        "effects": effects,
                    }
                )
            except ActionSimulationRejected as exc:
                error_payload = exc.payload
                if access_policies and isinstance(error_payload, dict):
                    attempted_changes = error_payload.get("attempted_changes")
                    if isinstance(attempted_changes, list):
                        for tgt in attempted_changes:
                            if not isinstance(tgt, dict):
                                continue
                            resource_rid = str(tgt.get("resource_rid") or "").strip()
                            class_id = resource_rid.split("@", 1)[0].split(":", 1)[-1] if resource_rid else ""
                            policy = access_policies.get(class_id)
                            if not policy:
                                continue
                            mask_columns = policy.get("mask_columns") or []
                            if isinstance(mask_columns, str):
                                mask_columns = [c.strip() for c in mask_columns.split(",") if c.strip()]
                            if not isinstance(mask_columns, list) or not mask_columns:
                                continue
                            mask_value = policy.get("mask_value")
                            observed = tgt.get("observed_base")
                            if isinstance(observed, dict):
                                fields = observed.get("fields") if isinstance(observed.get("fields"), dict) else {}
                                links = observed.get("links") if isinstance(observed.get("links"), dict) else {}
                                for col in mask_columns:
                                    key = str(col or "").strip()
                                    if not key:
                                        continue
                                    if key in fields:
                                        fields[key] = mask_value
                                    if key in links:
                                        links[key] = mask_value
                                observed["fields"] = fields
                                observed["links"] = links
                            conflict = tgt.get("conflict") if isinstance(tgt.get("conflict"), dict) else {}
                            link_conflicts = conflict.get("links") if isinstance(conflict.get("links"), list) else []
                            for item in link_conflicts:
                                if not isinstance(item, dict):
                                    continue
                                field = str(item.get("field") or "").strip()
                                if field and field in mask_columns and "value" in item:
                                    item["value"] = mask_value
                    for conflict in error_payload.get("conflicts") if isinstance(error_payload.get("conflicts"), list) else []:
                        if not isinstance(conflict, dict):
                            continue
                        class_id = str(conflict.get("class_id") or "").strip()
                        policy = access_policies.get(class_id)
                        if not policy:
                            continue
                        mask_columns = policy.get("mask_columns") or []
                        if isinstance(mask_columns, str):
                            mask_columns = [c.strip() for c in mask_columns.split(",") if c.strip()]
                        if not isinstance(mask_columns, list) or not mask_columns:
                            continue
                        mask_value = policy.get("mask_value")
                        for item in conflict.get("links") if isinstance(conflict.get("links"), list) else []:
                            if not isinstance(item, dict):
                                continue
                            field = str(item.get("field") or "").strip()
                            if field and field in mask_columns and "value" in item:
                                item["value"] = mask_value

                results.append(
                    {
                        "scenario_id": scenario_id,
                        "conflict_policy_override": conflict_override,
                        "status": "REJECTED",
                        "error": error_payload,
                    }
                )

        result_payload = {
            "status": "success",
            "data": {
                "db_name": db_name,
                "action_type_id": action_type_id,
                "action_type_rid": action_type_rid,
                "ontology_commit_id": ontology_commit_id,
                "base_branch": preflight.base_branch,
                "overlay_branch": preflight.overlay_branch,
                "writeback_target": preflight.writeback_target,
                "preview_action_log_id": preview_action_log_id,
                "assumptions": request.assumptions.model_dump(exclude_none=True) if request.assumptions is not None else None,
                "assumptions_applied": [
                    {"class_id": t.class_id, "instance_id": t.instance_id, **(t.assumptions or {})}
                    for t in preflight.loaded_targets
                    if t.assumptions is not None
                ]
                or None,
                "results": results,
            },
        }

        return result_payload

    except ActionSimulationRejected as exc:
        raise classified_http_exception(
            exc.status_code,
            str(exc.payload),
            code=ErrorCode.ACTION_CONFLICT_POLICY_FAILED,
        ) from exc
    except HTTPException:
        raise
    finally:
        if dataset_registry:
            await dataset_registry.close()


@foundry_router.post(
    "/{action}/apply",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("oms.action.v2.apply")
async def apply_action_v2_oms(
    ontology: str,
    action: str,
    *,
    body: ApplyActionRequestV2,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: Optional[str] = Query(default=None, alias="sdkPackageRid"),
    sdk_version: Optional[str] = Query(default=None, alias="sdkVersion"),
    transaction_id: Optional[str] = Query(default=None, alias="transactionId"),
    event_store=EventStoreDep,
) -> Dict[str, Any]:
    _ = sdk_package_rid, sdk_version, transaction_id
    db_name = await _ensure_ontology_database_exists(ontology)
    action_type_id = str(action or "").strip()
    if not action_type_id:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "action is required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    metadata = dict(body.metadata or {})
    metadata.setdefault("user_id", "system")
    metadata.setdefault("user_type", "user")
    parameters_payload = dict(body.parameters or {})
    mode = _resolve_v2_apply_mode(
        explicit_mode=body.options.mode if body.options else None,
    )
    resolved_branch = str(branch or "").strip() or "main"
    routing_metadata = _build_writeback_compute_routing_metadata(
        action_type_id=action_type_id,
        branch=resolved_branch,
        parameters_list=[parameters_payload],
    )
    metadata["__compute_routing"] = dict(routing_metadata)

    if mode == "VALIDATE_ONLY":
        await simulate_action_async(
            db_name=db_name,
            action_type_id=action_type_id,
            request=ActionSimulateRequest(
                input=parameters_payload,
                correlation_id=body.correlation_id,
                metadata=metadata,
                base_branch=resolved_branch,
                include_effects=False,
            ),
        )
        return _foundry_valid_action_validation_payload_for_parameters(
            parameters=parameters_payload,
            writeback_status="not_submitted",
        )

    submit_request = ActionSubmitBatchRequest(
        items=[
            ActionSubmitBatchItemRequest(
                input=parameters_payload,
                correlation_id=body.correlation_id,
                metadata=metadata,
            )
        ],
        base_branch=resolved_branch,
        overlay_branch=resolved_branch,
    )
    _stamp_compute_routing_metadata_on_submit_items(
        request=submit_request,
        routing_metadata=routing_metadata,
    )
    submit_response = await _submit_action_batch_with_compute_backend(
        db_name=db_name,
        action_type_id=action_type_id,
        request=submit_request,
        event_store=event_store,
        routing_metadata=routing_metadata,
    )
    first_item = submit_response.items[0] if submit_response.items else None
    action_log_id = str(first_item.action_log_id).strip() if first_item and first_item.action_log_id else None
    return _foundry_valid_action_validation_payload_for_parameters(
        parameters=parameters_payload,
        action_log_id=action_log_id,
        writeback_status="submitted" if action_log_id else "missing",
        side_effect_delivery={"status": "not_configured"},
    )


@foundry_router.post(
    "/{action}/applyBatch",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("oms.action.v2.apply_batch")
async def apply_action_batch_v2_oms(
    ontology: str,
    action: str,
    *,
    body: ApplyActionBatchRequestV2,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: Optional[str] = Query(default=None, alias="sdkPackageRid"),
    sdk_version: Optional[str] = Query(default=None, alias="sdkVersion"),
    event_store=EventStoreDep,
) -> Dict[str, Any]:
    _ = sdk_package_rid, sdk_version
    db_name = await _ensure_ontology_database_exists(ontology)
    action_type_id = str(action or "").strip()
    if not action_type_id:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "action is required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    if not body.requests:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "requests must not be empty",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    base_metadata = dict(body.metadata or {})
    base_metadata.setdefault("user_id", "system")
    base_metadata.setdefault("user_type", "user")
    resolved_branch = str(branch or "").strip() or "main"
    request_parameter_payloads: List[Dict[str, Any]] = [dict(item.parameters or {}) for item in body.requests]
    routing_metadata = _build_writeback_compute_routing_metadata(
        action_type_id=action_type_id,
        branch=resolved_branch,
        parameters_list=request_parameter_payloads,
    )
    base_metadata["__compute_routing"] = dict(routing_metadata)

    items: List[ActionSubmitBatchItemRequest] = []
    for item, parameters_payload in zip(body.requests, request_parameter_payloads):
        items.append(
            ActionSubmitBatchItemRequest(
                input=parameters_payload,
                correlation_id=body.correlation_id,
                metadata=dict(base_metadata),
            )
        )

    submit_request = ActionSubmitBatchRequest(
        items=items,
        base_branch=resolved_branch,
    )
    _stamp_compute_routing_metadata_on_submit_items(
        request=submit_request,
        routing_metadata=routing_metadata,
    )
    await _submit_action_batch_with_compute_backend(
        db_name=db_name,
        action_type_id=action_type_id,
        request=submit_request,
        event_store=event_store,
        routing_metadata=routing_metadata,
    )
    return {}


# FastAPI + postponed annotations safety:
# Keep request parameter annotations concrete so they are always treated as body models.
submit_action_async.__annotations__["request"] = ActionSubmitRequest
submit_action_batch_async.__annotations__["request"] = ActionSubmitBatchRequest
simulate_action_async.__annotations__["request"] = ActionSimulateRequest
apply_action_v2_oms.__annotations__["body"] = ApplyActionRequestV2
apply_action_batch_v2_oms.__annotations__["body"] = ApplyActionBatchRequestV2
