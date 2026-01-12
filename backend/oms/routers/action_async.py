"""
OMS async Action router - Action-only writeback submission path.

This endpoint accepts intent-only payloads (action_type_id + input) and writes an ActionCommand
to the Event Store for the action worker to execute.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, Field

from oms.dependencies import EventStoreDep, TerminusServiceDep, ensure_database_exists
from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2
from oms.services.ontology_resources import OntologyResourceService
from shared.config.app_config import AppConfig
from shared.config.settings import ApplicationSettings
from shared.models.commands import ActionCommand
from shared.models.event_envelope import EventEnvelope
from shared.observability.context_propagation import enrich_metadata_with_current_trace
from shared.security.database_access import DOMAIN_MODEL_ROLES, get_database_access_role
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input
from shared.services.action_log_registry import ActionLogRegistry
from shared.services.storage_service import create_storage_service
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
from shared.utils.principal_policy import build_principal_tags, policy_allows
from shared.utils.resource_rid import format_resource_rid, parse_metadata_rev
from shared.utils.writeback_conflicts import (
    compute_base_token,
    compute_observed_base,
)
from shared.utils.writeback_lifecycle import derive_lifecycle_id

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
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="action_type_id is required")

        # Prefer body base_branch; allow query alias for back-compat with existing patterns.
        resolved_base_branch = str(request.base_branch or "").strip() or "main"
        if base_branch:
            resolved_base_branch = str(base_branch).strip() or resolved_base_branch

        # Resolve deployed ontology commit (only deployed commits are executable).
        deployments = OntologyDeploymentRegistryV2()
        latest = await deployments.get_latest_deployed_commit(db_name=db_name, target_branch=resolved_base_branch)
        if not latest or not latest.get("ontology_commit_id"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "no_deployed_ontology",
                    "message": "No deployed ontology commit found; deploy ontology before executing actions.",
                    "db_name": db_name,
                    "target_branch": resolved_base_branch,
                },
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
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "action_type_not_found", "action_type_id": action_type_id},
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
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "action_type_missing_writeback_target",
                    "action_type_id": action_type_id,
                },
            )
        writeback_target = _resolve_writeback_target(db_name=db_name, raw_target=raw_writeback_target)

        overlay_branch_resolved = str(request.overlay_branch or "").strip() or str(writeback_target["branch"])

        # Sanitize input payload.
        sanitized_input = sanitize_input(request.input)

        # Validate against ActionType.input_schema (P0 type system).
        try:
            validated_input = validate_action_input(input_schema=spec.get("input_schema"), payload=sanitized_input)
        except ActionInputValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "action_input_invalid",
                    "action_type_id": action_type_id,
                    "message": str(exc),
                },
            ) from exc
        except ActionInputSchemaError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "action_type_input_schema_invalid",
                    "action_type_id": action_type_id,
                    "message": str(exc),
                },
            ) from exc

        audited_log_input = audit_action_log_input(validated_input, audit_policy=spec.get("audit_policy"))

        # Best-effort: capture observed_base at submission time to enable field-level conflict checks
        # when the worker executes later (ACTION_WRITEBACK_DESIGN.md).
        snapshot_targets: list[dict[str, Any]] = []
        implementation = spec.get("implementation")
        try:
            compiled_shape = compile_template_v1_change_shape(implementation, input_payload=validated_input)
        except ActionImplementationError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "action_type_implementation_invalid",
                    "action_type_id": action_type_id,
                    "message": str(exc),
                },
            ) from exc

        if compiled_shape:
            settings = ApplicationSettings()
            storage = create_storage_service(settings)
            if storage:
                max_targets = int(os.getenv("WRITEBACK_SUBMISSION_SNAPSHOT_MAX_TARGETS", "200"))
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
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "submitted_by_required",
                    "message": "request.metadata.user_id is required for action submissions",
                },
            )
        if submitted_by != "system":
            role = await get_database_access_role(
                db_name=db_name,
                principal_type=submitted_by_type,
                principal_id=submitted_by,
            )
            if role not in DOMAIN_MODEL_ROLES:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied")
            tags = build_principal_tags(principal_type=submitted_by_type, principal_id=submitted_by, role=role)
            if not policy_allows(policy=spec.get("permission_policy"), principal_tags=tags):
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied")
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
        await registry.create_log(
            action_log_id=action_log_id,
            db_name=db_name,
            action_type_id=action_type_id,
            action_type_rid=action_type_rid,
            resource_rid=None,
            ontology_commit_id=ontology_commit_id,
            input_payload=audited_log_input,
            correlation_id=request.correlation_id,
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
            payload=validated_input,
            metadata={
                **(request.metadata or {}),
                "correlation_id": request.correlation_id,
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
