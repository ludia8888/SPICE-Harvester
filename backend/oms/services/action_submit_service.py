"""
Application service for async action submission.

Router-facing contract:
- input: primitive request payload dict (already validated by FastAPI model)
- output: dict compatible with ActionSubmitResponse
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import status

from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2
from oms.services.ontology_resources import OntologyResourceService
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.models.commands import ActionCommand
from shared.models.event_envelope import EventEnvelope
from shared.observability.context_propagation import enrich_metadata_with_current_trace
from shared.observability.request_context import get_correlation_id
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input
from shared.services.registries.action_log_registry import ActionLogRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.storage_service import create_storage_service
from shared.utils.action_audit_policy import audit_action_log_input
from shared.utils.action_data_access import evaluate_action_target_data_access
from shared.utils.action_input_schema import (
    ActionInputSchemaError,
    ActionInputValidationError,
    validate_action_input,
)
from shared.utils.action_permission_profile import (
    ActionPermissionProfileError,
    requires_action_data_access_enforcement,
    resolve_action_permission_profile,
)
from shared.utils.action_runtime_contracts import (
    extract_required_action_interfaces,
    load_action_target_runtime_contract,
)
from shared.utils.action_template_engine import (
    ActionImplementationError,
    compile_action_change_shape,
)
from shared.utils.principal_policy import build_principal_tags
from shared.utils.resource_rid import format_resource_rid, parse_metadata_rev
from shared.utils.writeback_conflicts import compute_base_token, compute_observed_base
from shared.utils.writeback_lifecycle import derive_lifecycle_id

from oms.services.action_simulation_service import (
    ActionSimulationRejected,
    enforce_action_permission,
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


async def submit_action_request(
    *,
    db_name: str,
    action_type_id: str,
    request_payload: Dict[str, Any],
    base_branch_alias: Optional[str],
    event_store: Any,
    deployments_factory: Any = OntologyDeploymentRegistryV2,
    resources_factory: Any = OntologyResourceService,
    settings_getter: Any = get_settings,
    storage_factory: Any = create_storage_service,
    dataset_registry_factory: Any = DatasetRegistry,
    compile_action_change_shape_fn: Any = compile_action_change_shape,
    evaluate_action_target_data_access_fn: Any = evaluate_action_target_data_access,
) -> Dict[str, Any]:
    """
    Submit an async action command and return ActionSubmitResponse-compatible dict.
    """
    dataset_registry: Optional[DatasetRegistry] = None
    try:
        action_type_id = str(action_type_id or "").strip()
        if not action_type_id:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "action_type_id is required",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        metadata = request_payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        request_input = request_payload.get("input")
        if not isinstance(request_input, dict):
            request_input = {}
        request_base_branch = str(request_payload.get("base_branch") or "").strip()
        request_overlay_branch = str(request_payload.get("overlay_branch") or "").strip()
        request_correlation_id = str(request_payload.get("correlation_id") or "").strip()

        # Prefer body base_branch; allow query alias for backward compatibility.
        resolved_base_branch = request_base_branch or "main"
        if base_branch_alias:
            resolved_base_branch = str(base_branch_alias).strip() or resolved_base_branch

        deployments = deployments_factory()
        latest = await deployments.get_latest_deployed_commit(db_name=db_name, target_branch=resolved_base_branch)
        if not latest or not latest.get("ontology_commit_id"):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "No deployed ontology commit found; deploy ontology before executing actions.",
                code=ErrorCode.CONFLICT,
            )
        ontology_commit_id = str(latest["ontology_commit_id"])

        resources = resources_factory()
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

        raw_writeback_target = spec.get("writeback_target")
        if not isinstance(raw_writeback_target, dict) or not raw_writeback_target:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                f"action_type_missing_writeback_target: {action_type_id}",
                code=ErrorCode.CONFLICT,
            )
        writeback_target = _resolve_writeback_target(db_name=db_name, raw_target=raw_writeback_target)
        overlay_branch_resolved = request_overlay_branch or str(writeback_target["branch"])

        sanitized_input = sanitize_input(request_input)
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

        submitted_by = str(metadata.get("user_id") or "").strip()
        submitted_by_type = str(metadata.get("user_type") or "user").strip().lower() or "user"
        if not submitted_by:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "request.metadata.user_id is required for action submissions",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        actor_role = await enforce_action_permission(
            db_name=db_name,
            submitted_by=submitted_by,
            submitted_by_type=submitted_by_type,
            action_spec=spec,
        )
        principal_tags = build_principal_tags(
            principal_type=submitted_by_type,
            principal_id=submitted_by,
            role=actor_role,
        )
        enforce_edit_access = submitted_by != "system" and bool(principal_tags)

        snapshot_targets: list[dict[str, Any]] = []
        access_targets: list[dict[str, Any]] = []
        implementation = spec.get("implementation")
        try:
            compiled_shape = compile_action_change_shape_fn(implementation, input_payload=validated_input)
        except ActionImplementationError as exc:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                f"action_type_implementation_invalid for {action_type_id}: {exc}",
                code=ErrorCode.ACTION_TEMPLATE_ERROR,
            ) from exc

        if compiled_shape:
            settings = settings_getter()
            storage = storage_factory(settings)
            if (enforce_data_access or enforce_edit_access) and not storage:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "StorageService unavailable for action target access checks",
                    code=ErrorCode.STORAGE_UNAVAILABLE,
                )
            required_interfaces = set(extract_required_action_interfaces(spec))
            class_interface_refs: dict[str, list[str]] = {}
            class_field_types: dict[str, dict[str, str]] = {}

            async def _ensure_class_contract(class_id: str) -> None:
                if class_id in class_interface_refs:
                    return
                contract = await load_action_target_runtime_contract(
                    db_name=db_name,
                    class_id=class_id,
                    branch=ontology_commit_id,
                    resources=resources,
                )
                if contract is None:
                    raise classified_http_exception(
                        status.HTTP_404_NOT_FOUND,
                        f"target class not found at ontology commit: {class_id}",
                        code=ErrorCode.RESOURCE_NOT_FOUND,
                    )
                class_interface_refs[class_id] = contract.interfaces
                class_field_types[class_id] = contract.field_types

            if storage:
                max_targets = max(0, int(settings.writeback.writeback_submission_snapshot_max_targets))
                access_scan_limit = len(compiled_shape) if (enforce_data_access or enforce_edit_access) else max_targets
                for idx, item in enumerate(compiled_shape[: max(0, access_scan_limit)]):
                    target_class_id = str(item.class_id or "").strip()
                    target_instance_id = str(item.instance_id or "").strip()
                    if not target_class_id or not target_instance_id:
                        continue
                    changes = dict(item.changes or {})
                    await _ensure_class_contract(target_class_id)
                    if required_interfaces:
                        implemented = set(class_interface_refs.get(target_class_id, []))
                        missing = sorted(required_interfaces - implemented)
                        if missing:
                            raise classified_http_exception(
                                status.HTTP_403_FORBIDDEN,
                                (
                                    "Action target class does not satisfy required interfaces "
                                    f"(class_id={target_class_id}, missing={missing})"
                                ),
                                code=ErrorCode.PERMISSION_DENIED,
                            )

                    prefix = f"{db_name}/{resolved_base_branch}/{target_class_id}/{target_instance_id}/"
                    command_files = await storage.list_command_files(bucket=AppConfig.INSTANCE_BUCKET, prefix=prefix)
                    base_state = await storage.replay_instance_state(
                        bucket=AppConfig.INSTANCE_BUCKET,
                        command_files=command_files,
                    )
                    if not isinstance(base_state, dict):
                        base_state = {}
                    if enforce_data_access or enforce_edit_access:
                        access_targets.append(
                            {
                                "class_id": target_class_id,
                                "instance_id": target_instance_id,
                                "base_state": base_state,
                                "changes": changes,
                                "field_types": class_field_types.get(target_class_id, {}),
                            }
                        )

                    if idx < max_targets:
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

        if (enforce_data_access or enforce_edit_access) and access_targets:
            dataset_registry = dataset_registry_factory()
            await dataset_registry.connect()
            access_report = await evaluate_action_target_data_access_fn(
                dataset_registry=dataset_registry,
                db_name=db_name,
                targets=access_targets,
                enforce_data_access_policy=enforce_data_access,
                principal_tags=principal_tags if enforce_edit_access else None,
                enforce_object_edit_policy=enforce_edit_access,
                enforce_attachment_edit_policy=enforce_edit_access,
                enforce_object_set_edit_policy=enforce_edit_access,
            )
            if enforce_data_access and access_report.unverifiable:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "Unable to verify action target data access policy",
                    code=ErrorCode.STORAGE_UNAVAILABLE,
                )
            if enforce_data_access and access_report.denied:
                raise classified_http_exception(
                    status.HTTP_403_FORBIDDEN,
                    "Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                )
            if enforce_edit_access and access_report.edit_unverifiable:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "Unable to verify action target edit permissions",
                    code=ErrorCode.STORAGE_UNAVAILABLE,
                )
            if enforce_edit_access and access_report.edit_denied:
                raise classified_http_exception(
                    status.HTTP_403_FORBIDDEN,
                    "Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                )

        action_log_id = uuid4()
        log_metadata = dict(metadata)
        log_metadata["__submit_context"] = {
            "base_branch": resolved_base_branch,
            "overlay_branch": overlay_branch_resolved,
        }
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
        effective_correlation_id = (request_correlation_id or get_correlation_id() or "").strip() or None
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
                **metadata,
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

        return {
            "action_log_id": str(action_log_id),
            "status": "PENDING",
            "db_name": db_name,
            "action_type_id": action_type_id,
            "ontology_commit_id": ontology_commit_id,
            "base_branch": resolved_base_branch,
            "overlay_branch": overlay_branch_resolved,
            "writeback_target": writeback_target,
        }

    except ActionSimulationRejected as exc:
        raise classified_http_exception(
            exc.status_code,
            str(exc.payload),
            code=ErrorCode.ACTION_CONFLICT_POLICY_FAILED,
        ) from exc
    except SecurityViolationError as exc:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            str(exc),
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        ) from exc
    finally:
        if dataset_registry:
            await dataset_registry.close()
