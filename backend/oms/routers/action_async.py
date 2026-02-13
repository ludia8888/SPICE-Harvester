"""
OMS async Action router - Action-only writeback submission path.

This endpoint accepts intent-only payloads (action_type_id + input) and writes an ActionCommand
to the Event Store for the action worker to execute.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, Field, field_validator

from oms.dependencies import EventStoreDep, TerminusServiceDep, ensure_database_exists
from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2
from oms.services.ontology_resources import OntologyResourceService
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.models.commands import ActionCommand
from shared.models.event_envelope import EventEnvelope
from shared.observability.context_propagation import enrich_metadata_with_current_trace
from shared.observability.request_context import get_correlation_id
from shared.security.database_access import DOMAIN_MODEL_ROLES, get_database_access_role
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input
from shared.services.registries.action_log_registry import ActionLogRegistry
from shared.services.registries.action_simulation_registry import ActionSimulationRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
from shared.services.storage.storage_service import create_storage_service
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.utils.action_audit_policy import audit_action_log_input
from shared.utils.action_input_schema import (
    ActionInputSchemaError,
    ActionInputValidationError,
    validate_action_input,
)
from shared.utils.action_template_engine import (
    ActionImplementationError,
    compile_template_v1_change_shape,
)
from shared.utils.action_simulation_utils import reject_simulation_delete_flag
from shared.utils.principal_policy import build_principal_tags, policy_allows
from shared.utils.resource_rid import format_resource_rid, parse_metadata_rev
from shared.utils.writeback_conflicts import (
    compute_base_token,
    compute_observed_base,
)
from shared.utils.access_policy import apply_access_policy
from shared.utils.writeback_lifecycle import derive_lifecycle_id
from shared.observability.tracing import trace_endpoint

from oms.services.action_simulation_service import (
    ActionSimulationRejected,
    build_patchset_for_scenario,
    enforce_action_permission,
    preflight_action_writeback,
    simulate_effects_for_patchset,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/actions/{db_name}/async", tags=["Async Actions"])


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
    simulation_id: Optional[str] = Field(default=None, description="Optional simulation id for versioned reruns")
    title: Optional[str] = Field(default=None, max_length=200)
    description: Optional[str] = Field(default=None, max_length=2000)
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


@router.post(
    "/{action_type_id}/submit",
    response_model=ActionSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@trace_endpoint("oms.action.submit")
async def submit_action_async(
    db_name: str = Depends(ensure_database_exists),
    action_type_id: str = Path(..., description="Action type identifier"),
    request: ActionSubmitRequest = ...,
    base_branch: Optional[str] = Query(default=None, description="Deprecated alias; use base_branch in body"),
    terminus=TerminusServiceDep,
    event_store=EventStoreDep,
) -> ActionSubmitResponse:
    """
    Submit an Action for async execution.

    The action worker is responsible for:
    - permission checks
    - patchset computation
    - lakeFS writeback commit
    - ActionApplied emission
    """
    try:
        action_type_id = str(action_type_id or "").strip()
        if not action_type_id:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "action_type_id is required",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # Prefer body base_branch; allow query alias for back-compat with existing patterns.
        resolved_base_branch = str(request.base_branch or "").strip() or "main"
        if base_branch:
            resolved_base_branch = str(base_branch).strip() or resolved_base_branch

        # Resolve deployed ontology commit (only deployed commits are executable).
        deployments = OntologyDeploymentRegistryV2()
        latest = await deployments.get_latest_deployed_commit(db_name=db_name, target_branch=resolved_base_branch)
        if not latest or not latest.get("ontology_commit_id"):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "No deployed ontology commit found; deploy ontology before executing actions.",
                code=ErrorCode.CONFLICT,
            )
        ontology_commit_id = str(latest["ontology_commit_id"])

        # Load action_type spec at the deployed commit.
        resources = OntologyResourceService(terminus)
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
        action_meta = action_resource.get("metadata") if isinstance(action_resource, dict) else None
        action_type_rid = format_resource_rid(
            resource_type="action_type",
            resource_id=action_type_id,
            rev=parse_metadata_rev(action_meta),
        )

        raw_writeback_target = spec.get("writeback_target")
        if not isinstance(raw_writeback_target, dict) or not raw_writeback_target:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                f"action_type_missing_writeback_target: {action_type_id}",
                code=ErrorCode.CONFLICT,
            )
        writeback_target = _resolve_writeback_target(db_name=db_name, raw_target=raw_writeback_target)

        overlay_branch_resolved = str(request.overlay_branch or "").strip() or str(writeback_target["branch"])

        # Sanitize input payload.
        sanitized_input = sanitize_input(request.input)

        # Validate against ActionType.input_schema (P0 type system).
        try:
            validated_input = validate_action_input(input_schema=spec.get("input_schema"), payload=sanitized_input)
        except ActionInputValidationError as exc:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"action_input_invalid for {action_type_id}: {exc}",
                code=ErrorCode.ACTION_INPUT_INVALID,
            ) from exc
        except ActionInputSchemaError as exc:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                f"action_type_input_schema_invalid for {action_type_id}: {exc}",
                code=ErrorCode.ACTION_TEMPLATE_ERROR,
            ) from exc

        audited_log_input = audit_action_log_input(validated_input, audit_policy=spec.get("audit_policy"))

        # Best-effort: capture observed_base at submission time to enable field-level conflict checks
        # when the worker executes later (ACTION_WRITEBACK_DESIGN.md).
        snapshot_targets: list[dict[str, Any]] = []
        implementation = spec.get("implementation")
        try:
            compiled_shape = compile_template_v1_change_shape(implementation, input_payload=validated_input)
        except ActionImplementationError as exc:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                f"action_type_implementation_invalid for {action_type_id}: {exc}",
                code=ErrorCode.ACTION_TEMPLATE_ERROR,
            ) from exc

        if compiled_shape:
            settings = get_settings()
            storage = create_storage_service(settings)
            if storage:
                max_targets = max(0, int(settings.writeback.writeback_submission_snapshot_max_targets))
                for item in compiled_shape[: max(0, max_targets)]:
                    target_class_id = str(item.class_id or "").strip()
                    target_instance_id = str(item.instance_id or "").strip()
                    if not target_class_id or not target_instance_id:
                        continue
                    changes = dict(item.changes or {})

                    prefix = f"{db_name}/{resolved_base_branch}/{target_class_id}/{target_instance_id}/"
                    command_files = await storage.list_command_files(bucket=AppConfig.INSTANCE_BUCKET, prefix=prefix)
                    base_state = await storage.replay_instance_state(
                        bucket=AppConfig.INSTANCE_BUCKET,
                        command_files=command_files,
                    )
                    if not isinstance(base_state, dict):
                        base_state = {}

                    lifecycle_id = derive_lifecycle_id(base_state)

                    snapshot_targets.append(
                        {
                            "class_id": target_class_id,
                            "instance_id": target_instance_id,
                            "lifecycle_id": lifecycle_id,
                            "base_token": compute_base_token(
                                db_name=db_name,
                                class_id=target_class_id,
                                instance_id=target_instance_id,
                                lifecycle_id=lifecycle_id,
                                base_doc=base_state,
                            ),
                            "observed_base": compute_observed_base(base=base_state, changes=changes),
                        }
                    )

        # Create ActionLog (audit SSoT).
        action_log_id = uuid4()
        submitted_by = str((request.metadata or {}).get("user_id") or "").strip()
        submitted_by_type = str((request.metadata or {}).get("user_type") or "user").strip().lower() or "user"
        if not submitted_by:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "request.metadata.user_id is required for action submissions",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if submitted_by != "system":
            role = await get_database_access_role(
                db_name=db_name,
                principal_type=submitted_by_type,
                principal_id=submitted_by,
            )
            if role not in DOMAIN_MODEL_ROLES:
                raise classified_http_exception(
                    status.HTTP_403_FORBIDDEN,
                    "Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                )
            tags = build_principal_tags(principal_type=submitted_by_type, principal_id=submitted_by, role=role)
            if not policy_allows(policy=spec.get("permission_policy"), principal_tags=tags):
                raise classified_http_exception(
                    status.HTTP_403_FORBIDDEN,
                    "Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                )
        log_metadata = dict(request.metadata or {})
        # Durable trace correlation: persist W3C context on the ActionLog (ontology object)
        # so outbox/reconciler workers can attach original traces even when running later.
        enrich_metadata_with_current_trace(log_metadata)
        if snapshot_targets:
            log_metadata["__writeback_submission"] = {
                "base_branch": resolved_base_branch,
                "overlay_branch": overlay_branch_resolved,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "targets": snapshot_targets,
            }
        registry = ActionLogRegistry()
        await registry.connect()
        effective_correlation_id = (request.correlation_id or get_correlation_id() or "").strip() or None
        await registry.create_log(
            action_log_id=action_log_id,
            db_name=db_name,
            action_type_id=action_type_id,
            action_type_rid=action_type_rid,
            resource_rid=None,
            ontology_commit_id=ontology_commit_id,
            input_payload=audited_log_input,
            correlation_id=effective_correlation_id,
            submitted_by=submitted_by,
            writeback_target=writeback_target,
            metadata=log_metadata,
        )

        # Append ActionCommand to Event Store (SSoT) for the worker.
        command = ActionCommand(
            command_id=action_log_id,
            db_name=db_name,
            action_log_id=action_log_id,
            action_type_id=action_type_id,
            ontology_commit_id=ontology_commit_id,
            base_branch=resolved_base_branch,
            overlay_branch=overlay_branch_resolved,
            correlation_id=effective_correlation_id,
            payload=validated_input,
            metadata={
                **(request.metadata or {}),
                "correlation_id": effective_correlation_id,
                "submitted_at": datetime.now(timezone.utc).isoformat(),
                "ontology": {"ref": f"branch:{resolved_base_branch}", "commit": ontology_commit_id},
            },
        )

        envelope = EventEnvelope.from_command(
            command,
            actor=submitted_by,
            kafka_topic=AppConfig.ACTION_COMMANDS_TOPIC,
            metadata={"service": "oms", "mode": "action_writeback"},
        )
        await event_store.append_event(envelope)

        return ActionSubmitResponse(
            action_log_id=str(action_log_id),
            status="PENDING",
            db_name=db_name,
            action_type_id=action_type_id,
            ontology_commit_id=ontology_commit_id,
            base_branch=resolved_base_branch,
            overlay_branch=overlay_branch_resolved,
            writeback_target=writeback_target,
        )

    except SecurityViolationError as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            str(e),
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        ) from e


@router.post(
    "/{action_type_id}/simulate",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("oms.action.simulate")
async def simulate_action_async(
    db_name: str = Depends(ensure_database_exists),
    action_type_id: str = Path(..., description="Action type identifier"),
    request: ActionSimulateRequest = ...,
    terminus=TerminusServiceDep,
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
    simulation_registry: Optional[ActionSimulationRegistry] = None
    simulation_id: Optional[str] = None
    version: Optional[int] = None
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
            # Dev/test mode: use branch HEAD commit for action type resolution
            # (allows simulating action types that have not been deployed yet).
            from shared.utils.commit_utils import coerce_commit_id
            branches = await terminus.version_control_service.list_branches(db_name)
            head_commit = None
            for item in branches or []:
                if isinstance(item, dict) and item.get("name") == resolved_base_branch:
                    head_commit = coerce_commit_id(item.get("head"))
                    break
            if not head_commit:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    f"Branch HEAD not found for '{resolved_base_branch}'",
                    code=ErrorCode.CONFLICT,
                )
            ontology_commit_id = str(head_commit)
            logger.info("Simulate using branch HEAD: %s (dev mode)", ontology_commit_id)
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

        resources = OntologyResourceService(terminus)
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

        simulation_registry = ActionSimulationRegistry()
        await simulation_registry.connect()
        simulation_id = str(request.simulation_id or "").strip()
        if simulation_id:
            try:
                simulation_id = str(UUID(simulation_id))
            except Exception as exc:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "simulation_id must be a UUID",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                ) from exc
            existing = await simulation_registry.get_simulation(simulation_id=simulation_id)
            if not existing:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    "ActionSimulation not found",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                )
            if existing.db_name != db_name or existing.action_type_id != action_type_id:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "simulation_id does not match db_name/action_type_id",
                    code=ErrorCode.CONFLICT,
                )
        else:
            simulation_id = str(uuid4())
            await simulation_registry.create_simulation(
                simulation_id=simulation_id,
                db_name=db_name,
                action_type_id=action_type_id,
                title=request.title,
                description=request.description,
                created_by=submitted_by,
                created_by_type=submitted_by_type,
            )
        version = await simulation_registry.next_version(simulation_id=simulation_id)

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

        if AppConfig.WRITEBACK_ENFORCE_GOVERNANCE:
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
            terminus=terminus,
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
                "simulation_id": simulation_id,
                "version": version,
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

        if simulation_registry:
            await simulation_registry.create_version(
                simulation_id=simulation_id,
                version=version,
                status="COMPUTED",
                base_branch=preflight.base_branch,
                overlay_branch=preflight.overlay_branch,
                ontology_commit_id=ontology_commit_id,
                action_type_rid=action_type_rid,
                preview_action_log_id=preview_action_log_id,
                input_payload=sanitized_input,
                assumptions=request.assumptions.model_dump(exclude_none=True) if request.assumptions is not None else None,
                scenarios=[s.model_dump(exclude_none=True) for s in scenarios],
                result=result_payload.get("data") if isinstance(result_payload.get("data"), dict) else None,
                error=None,
                created_by=submitted_by,
                created_by_type=submitted_by_type,
            )

        return result_payload

    except ActionSimulationRejected as exc:
        if simulation_registry and simulation_id and version is not None:
            try:
                await simulation_registry.create_version(
                    simulation_id=simulation_id,
                    version=version,
                    status="REJECTED",
                    base_branch=str(request.base_branch or "").strip() or "main",
                    overlay_branch=str(request.overlay_branch or "").strip() or "main",
                    ontology_commit_id=ontology_commit_id,
                    action_type_rid=action_type_rid,
                    preview_action_log_id=preview_action_log_id,
                    input_payload=sanitize_input(request.input),
                    assumptions=request.assumptions.model_dump(exclude_none=True) if request.assumptions is not None else None,
                    scenarios=[s.model_dump(exclude_none=True) for s in (request.scenarios or [])],
                    result=None,
                    error=exc.payload,
                    created_by=submitted_by or "system",
                    created_by_type=submitted_by_type or "user",
                )
            except Exception:
                pass
        raise classified_http_exception(
            exc.status_code,
            str(exc.payload),
            code=ErrorCode.ACTION_CONFLICT_POLICY_FAILED,
        ) from exc
    except HTTPException as exc:
        if simulation_registry and simulation_id and version is not None:
            try:
                await simulation_registry.create_version(
                    simulation_id=simulation_id,
                    version=version,
                    status="FAILED",
                    base_branch=str(request.base_branch or "").strip() or "main",
                    overlay_branch=str(request.overlay_branch or "").strip() or "main",
                    ontology_commit_id=ontology_commit_id,
                    action_type_rid=action_type_rid,
                    preview_action_log_id=preview_action_log_id,
                    input_payload=sanitize_input(request.input),
                    assumptions=request.assumptions.model_dump(exclude_none=True) if request.assumptions is not None else None,
                    scenarios=[s.model_dump(exclude_none=True) for s in (request.scenarios or [])],
                    result=None,
                    error={"error": ErrorCode.HTTP_ERROR.value, "status_code": int(exc.status_code), "detail": exc.detail},
                    created_by=submitted_by or "system",
                    created_by_type=submitted_by_type or "user",
                )
            except Exception:
                pass
        raise
    finally:
        if dataset_registry:
            await dataset_registry.close()
        if simulation_registry:
            await simulation_registry.close()
