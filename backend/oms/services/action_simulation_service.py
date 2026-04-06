from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from oms.services.action_target_state_service import ActionTargetStateLoadError, load_action_target_states
from oms.services.ontology_resources import OntologyResourceService
from shared.config.app_config import AppConfig
from shared.errors.enterprise_catalog import is_external_code, resolve_enterprise_error
from shared.errors.error_types import ErrorCode
from shared.security.database_access import DATA_ENGINEER_ROLES, DOMAIN_MODEL_ROLES, inspect_database_access
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.lakefs_storage_service import LakeFSStorageService
from shared.services.storage.storage_service import StorageService
from shared.services.core.object_type_meta_resolver import build_object_type_meta_resolver
from shared.services.core.writeback_merge_service import WritebackMergeService
from shared.utils.action_data_access import evaluate_action_target_data_access
from shared.utils.bool_utils import coerce_optional_bool as _coerce_optional_bool
from shared.utils.action_permission_profile import (
    ActionPermissionProfile,
    ActionPermissionProfileError,
    PERMISSION_MODEL_ONTOLOGY_ROLES,
    requires_action_data_access_enforcement,
    resolve_action_permission_profile,
)
from shared.utils.action_input_schema import (
    ActionInputSchemaError,
    ActionInputValidationError,
    validate_action_input,
)
from shared.utils.action_template_engine import (
    ActionImplementationError,
    compile_action_change_shape,
    compile_action_implementation,
)
from shared.utils.principal_policy import build_principal_tags, policy_allows
from shared.utils.resource_rid import format_resource_rid, strip_rid_revision
from shared.utils.safe_bool_expression import BoolExpressionError, safe_eval_bool_expression
from shared.utils.submission_criteria_diagnostics import infer_submission_criteria_failure_reason
from shared.utils.writeback_conflicts import (
    compute_base_token,
    compute_observed_base,
    detect_overlap_fields,
    detect_overlap_links,
    normalize_conflict_policy,
    parse_conflict_policy,
    resolve_applied_changes,
)
from shared.utils.writeback_governance import extract_backing_dataset_id, policies_aligned
from shared.utils.writeback_lifecycle import DEFAULT_LIFECYCLE_ID, derive_lifecycle_id, overlay_doc_id
from shared.utils.writeback_paths import queue_entry_prefix, queue_entry_key, ref_key, writeback_patchset_key
from shared.utils.action_writeback import is_noop_changes, safe_str
from shared.observability.tracing import trace_external_call
from shared.utils.action_runtime_contracts import (
    extract_required_action_interfaces,
    load_action_target_runtime_contract,
)
from shared.utils.writeback_patch_apply import apply_changes_to_payload
from shared.utils.time_utils import utcnow
import logging

logger = logging.getLogger(__name__)


class ActionSimulationRejected(Exception):
    def __init__(self, payload: Dict[str, Any], *, status_code: int = 400) -> None:
        super().__init__(str(payload.get("message") or payload.get("error") or "action_simulation_rejected"))
        self.payload = payload
        self.status_code = int(status_code)


def _enterprise_payload_for_error(*, error_key: str) -> Optional[Dict[str, Any]]:
    key = str(error_key or "").strip()
    if not key:
        return None
    try:
        if is_external_code(key):
            return resolve_enterprise_error(
                service_name="oms",
                code=None,
                category=None,
                status_code=400,
                external_code=key,
            ).to_dict()
        return resolve_enterprise_error(
            service_name="oms",
            code=ErrorCode(key),
            category=None,
            status_code=400,
            external_code=None,
        ).to_dict()
    except Exception:
        logging.getLogger(__name__).warning("Exception fallback at oms/services/action_simulation_service.py:80", exc_info=True)
        return None


def _attach_enterprise(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload or {})
    if "enterprise" in out:
        return out
    enterprise = _enterprise_payload_for_error(error_key=str(out.get("error") or "").strip())
    if enterprise is not None:
        out["enterprise"] = enterprise
    return out


_ASSUMPTION_FORBIDDEN_FIELDS = {
    "instance_id",
    "class_id",
    "db_name",
    "branch",
    "lifecycle_id",
    "rid",
    "overlay_tombstone",
    "base_token",
    "patchset_commit_id",
    "action_log_id",
}


def _assumption_is_forbidden_field(field: str) -> bool:
    key = str(field or "").strip()
    if not key:
        return True
    if key.startswith("_"):
        return True
    return key in _ASSUMPTION_FORBIDDEN_FIELDS


def _extract_link_fields(ops: Any) -> List[str]:
    fields: set[str] = set()
    if not isinstance(ops, list):
        return []
    for item in ops:
        field = None
        if isinstance(item, dict):
            field = item.get("field") or item.get("predicate") or item.get("name")
            if field is None and len(item) == 1:
                field = next(iter(item.keys()))
        elif isinstance(item, str):
            raw = item.strip()
            if ":" in raw:
                field, _value = raw.split(":", 1)
        field_str = str(field or "").strip()
        if field_str:
            fields.add(field_str)
    return sorted(fields)


def _extract_patch_field_lists(patch: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    if not isinstance(patch, dict):
        return [], []
    set_ops = patch.get("set") if isinstance(patch.get("set"), dict) else {}
    unset_ops = patch.get("unset") if isinstance(patch.get("unset"), list) else []

    fields: set[str] = set()
    links: set[str] = set()

    for key in (set_ops or {}).keys():
        key_str = str(key or "").strip()
        if key_str:
            fields.add(key_str)
    for key in unset_ops or []:
        key_str = str(key or "").strip()
        if key_str:
            fields.add(key_str)
    for key in _extract_link_fields(patch.get("link_add")) + _extract_link_fields(patch.get("link_remove")):
        if key:
            links.add(key)
    return sorted(fields), sorted(links)


def _apply_assumption_patch(
    *,
    scope: str,
    base_state: Dict[str, Any],
    patch: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if not isinstance(base_state, dict):
        raise ValueError("base_state must be a dict")
    if not isinstance(patch, dict):
        return dict(base_state), {"fields": [], "links": []}
    if bool(patch.get("delete")):
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "simulation_assumption_invalid",
                    "message": "delete is not allowed in simulation assumptions",
                    "scope": scope,
                }
            ),
            status_code=400,
        )

    fields, links = _extract_patch_field_lists(patch)
    forbidden = [f for f in fields + links if _assumption_is_forbidden_field(f)]
    if forbidden:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "simulation_assumption_forbidden_field",
                    "message": "One or more fields are not allowed in simulation assumptions",
                    "scope": scope,
                    "fields": sorted(set(forbidden)),
                }
            ),
            status_code=400,
        )

    assumed = dict(base_state)
    tombstone = apply_changes_to_payload(assumed, patch)
    if tombstone:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "simulation_assumption_invalid",
                    "message": "delete is not allowed in simulation assumptions",
                    "scope": scope,
                }
            ),
            status_code=400,
        )
    return assumed, {"fields": fields, "links": links}


def _apply_observed_base_overrides(
    *,
    observed_base: Dict[str, Any],
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(observed_base, dict) or not isinstance(overrides, dict):
        return {"fields": [], "links": []}

    overridden_fields: List[str] = []
    overridden_links: List[str] = []

    fields = observed_base.get("fields") if isinstance(observed_base.get("fields"), dict) else {}
    links = observed_base.get("links") if isinstance(observed_base.get("links"), dict) else {}

    override_fields = overrides.get("fields") if isinstance(overrides.get("fields"), dict) else {}
    override_links = overrides.get("links") if isinstance(overrides.get("links"), dict) else {}

    for key, value in override_fields.items():
        field = str(key or "").strip()
        if not field:
            continue
        if field not in fields:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "simulation_assumption_invalid",
                        "message": "observed_base_overrides.fields may only include touched fields",
                        "field": field,
                    }
                ),
                status_code=400,
            )
        if _assumption_is_forbidden_field(field):
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "simulation_assumption_forbidden_field",
                        "message": "observed_base_overrides contains forbidden field",
                        "field": field,
                        "scope": "observed_base_overrides.fields",
                    }
                ),
                status_code=400,
            )
        fields[field] = value
        overridden_fields.append(field)

    for key, value in override_links.items():
        field = str(key or "").strip()
        if not field:
            continue
        if field not in links:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "simulation_assumption_invalid",
                        "message": "observed_base_overrides.links may only include touched link fields",
                        "field": field,
                    }
                ),
                status_code=400,
            )
        if _assumption_is_forbidden_field(field):
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "simulation_assumption_forbidden_field",
                        "message": "observed_base_overrides contains forbidden link field",
                        "field": field,
                        "scope": "observed_base_overrides.links",
                    }
                ),
                status_code=400,
            )
        links[field] = value
        overridden_links.append(field)

    observed_base["fields"] = fields
    observed_base["links"] = links
    return {"fields": sorted(set(overridden_fields)), "links": sorted(set(overridden_links))}


@dataclass(frozen=True)
class ActionSimulationScenario:
    scenario_id: str
    conflict_policy: Optional[str] = None


@dataclass(frozen=True)
class TargetPreflight:
    resource_rid: str
    class_id: str
    instance_id: str
    lifecycle_id: str
    base_state: Dict[str, Any]
    access_base_state: Dict[str, Any]
    changes: Dict[str, Any]
    observed_base: Dict[str, Any]
    base_token: Dict[str, Any]
    conflict_fields: List[str]
    conflict_links: List[Dict[str, str]]
    object_conflict_policy: Optional[str]
    field_types: Dict[str, str]
    assumptions: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class ActionPreflight:
    db_name: str
    action_type_id: str
    action_type_rid: str
    ontology_commit_id: str
    base_branch: str
    overlay_branch: str
    writeback_target: Dict[str, Any]
    submitted_by: str
    submitted_by_type: str
    actor_role: Optional[str]
    input_payload: Dict[str, Any]
    action_conflict_policy: Optional[str]
    loaded_targets: List[TargetPreflight]


def _coerce_overlay_branch(*, db_name: str, writeback_target: Dict[str, Any], overlay_branch: Optional[str]) -> str:
    if overlay_branch and str(overlay_branch).strip():
        return str(overlay_branch).strip()
    raw = str(writeback_target.get("branch") or "").strip()
    if raw:
        return raw
    return AppConfig.get_ontology_writeback_branch(db_name)


def _first_defined(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None
def _resolve_project_policy_contract(action_spec: Dict[str, Any]) -> Dict[str, Any]:
    spec = action_spec if isinstance(action_spec, dict) else {}
    permission_policy = spec.get("permission_policy") if isinstance(spec.get("permission_policy"), dict) else {}

    inherit_project_policy = _coerce_optional_bool(
        _first_defined(
            permission_policy.get("inherit_project_policy"),
            permission_policy.get("inheritProjectPolicy"),
            spec.get("inherit_project_policy"),
            spec.get("inheritProjectPolicy"),
        ),
        default=False,
    )
    require_project_policy = _coerce_optional_bool(
        _first_defined(
            permission_policy.get("require_project_policy"),
            permission_policy.get("requireProjectPolicy"),
            spec.get("require_project_policy"),
            spec.get("requireProjectPolicy"),
        ),
        default=True,
    )
    scope = str(
        _first_defined(
            permission_policy.get("project_policy_scope"),
            permission_policy.get("projectPolicyScope"),
            spec.get("project_policy_scope"),
            spec.get("projectPolicyScope"),
            "action_access",
        )
        or "action_access"
    ).strip() or "action_access"
    subject_type = str(
        _first_defined(
            permission_policy.get("project_policy_subject_type"),
            permission_policy.get("projectPolicySubjectType"),
            spec.get("project_policy_subject_type"),
            spec.get("projectPolicySubjectType"),
            "project",
        )
        or "project"
    ).strip() or "project"
    raw_subject_id = _first_defined(
        permission_policy.get("project_policy_subject_id"),
        permission_policy.get("projectPolicySubjectId"),
        spec.get("project_policy_subject_id"),
        spec.get("projectPolicySubjectId"),
    )
    subject_id = str(raw_subject_id or "").strip() or None
    return {
        "inherit_project_policy": inherit_project_policy,
        "require_project_policy": require_project_policy,
        "scope": scope,
        "subject_type": subject_type,
        "subject_id": subject_id,
    }


@trace_external_call("oms.action_simulation.enforce_action_permission")
async def enforce_action_permission(
    *,
    db_name: str,
    submitted_by: str,
    submitted_by_type: str,
    action_spec: Dict[str, Any],
) -> Optional[str]:
    try:
        permission_profile = resolve_action_permission_profile(action_spec)
    except ActionPermissionProfileError as exc:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "action_permission_profile_invalid",
                    "message": str(exc),
                    "field": exc.field,
                }
            ),
            status_code=409,
        ) from exc

    actor = str(submitted_by or "").strip()
    if not actor:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "submitted_by_required",
                    "message": "request.metadata.user_id is required",
                }
            ),
            status_code=400,
        )
    if actor == "system":
        return None

    actor_type = str(submitted_by_type or "user").strip().lower() or "user"
    inspection = await inspect_database_access(
        db_name=db_name,
        principal_type=actor_type,
        principal_id=actor,
    )
    if inspection.is_unavailable:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "database_access_registry_unavailable",
                    "message": "Unable to verify actor database access role",
                }
            ),
            status_code=503,
        )
    role = inspection.role
    if permission_profile.permission_model == PERMISSION_MODEL_ONTOLOGY_ROLES:
        if role not in DOMAIN_MODEL_ROLES:
            raise ActionSimulationRejected(
                _attach_enterprise({"error": ErrorCode.PERMISSION_DENIED.value, "message": "Permission denied"}),
                status_code=403,
            )
    else:
        if role not in DATA_ENGINEER_ROLES:
            raise ActionSimulationRejected(
                _attach_enterprise({"error": ErrorCode.PERMISSION_DENIED.value, "message": "Permission denied"}),
                status_code=403,
            )

    if permission_profile.permission_model == PERMISSION_MODEL_ONTOLOGY_ROLES:
        tags = build_principal_tags(principal_type=actor_type, principal_id=actor, role=role)
        if not policy_allows(policy=action_spec.get("permission_policy"), principal_tags=tags):
            raise ActionSimulationRejected(
                _attach_enterprise({"error": ErrorCode.PERMISSION_DENIED.value, "message": "Permission denied"}),
                status_code=403,
            )
        project_policy_contract = _resolve_project_policy_contract(action_spec)
        if project_policy_contract["inherit_project_policy"]:
            dataset_registry = DatasetRegistry()
            try:
                await dataset_registry.connect()
                access_policy = await dataset_registry.get_access_policy(
                    db_name=db_name,
                    scope=str(project_policy_contract["scope"]),
                    subject_type=str(project_policy_contract["subject_type"]),
                    subject_id=str(project_policy_contract["subject_id"] or db_name),
                )
            except Exception as exc:
                raise ActionSimulationRejected(
                    _attach_enterprise(
                        {
                            "error": "project_policy_unverifiable",
                            "message": "Unable to verify inherited project policy",
                            "scope": str(project_policy_contract["scope"]),
                            "subject_type": str(project_policy_contract["subject_type"]),
                            "subject_id": str(project_policy_contract["subject_id"] or db_name),
                            "detail": str(exc),
                        }
                    ),
                    status_code=503,
                ) from exc
            finally:
                try:
                    close = getattr(dataset_registry, "close", None)
                    if callable(close):
                        await close()
                except Exception:
                    logger.warning(
                        "Failed to close DatasetRegistry after inherited project policy lookup",
                        exc_info=True,
                    )
            inherited_policy = access_policy.policy if access_policy is not None else None
            if (not isinstance(inherited_policy, dict) or not inherited_policy) and bool(
                project_policy_contract["require_project_policy"]
            ):
                raise ActionSimulationRejected(
                    _attach_enterprise(
                        {
                            "error": ErrorCode.PERMISSION_DENIED.value,
                            "message": "Permission denied",
                            "scope": str(project_policy_contract["scope"]),
                            "subject_type": str(project_policy_contract["subject_type"]),
                            "subject_id": str(project_policy_contract["subject_id"] or db_name),
                        }
                    ),
                    status_code=403,
                )
            if isinstance(inherited_policy, dict) and inherited_policy and not policy_allows(
                policy=inherited_policy,
                principal_tags=tags,
            ):
                raise ActionSimulationRejected(
                    _attach_enterprise(
                        {
                            "error": ErrorCode.PERMISSION_DENIED.value,
                            "message": "Permission denied",
                            "scope": str(project_policy_contract["scope"]),
                            "subject_type": str(project_policy_contract["subject_type"]),
                            "subject_id": str(project_policy_contract["subject_id"] or db_name),
                        }
                    ),
                    status_code=403,
                )

    if permission_profile.edits_beyond_actions and role not in DATA_ENGINEER_ROLES:
        raise ActionSimulationRejected(
            _attach_enterprise({"error": ErrorCode.PERMISSION_DENIED.value, "message": "Permission denied"}),
            status_code=403,
        )
    return role


async def _check_writeback_dataset_acl_alignment(
    *,
    db_name: str,
    submitted_by: str,
    submitted_by_type: str,
    actor_role: Optional[str],
    ontology_commit_id: str,
    resources: OntologyResourceService,
    dataset_registry: DatasetRegistry,
    class_ids: set[str],
) -> None:
    if not AppConfig.WRITEBACK_ENFORCE_GOVERNANCE:
        return
    if not class_ids:
        return

    scope = AppConfig.WRITEBACK_DATASET_ACL_SCOPE
    writeback_dataset_id = AppConfig.ONTOLOGY_WRITEBACK_DATASET_ID
    if not writeback_dataset_id:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "writeback_acl_unverifiable",
                    "message": "ONTOLOGY_WRITEBACK_DATASET_ID is required when WRITEBACK_ENFORCE_GOVERNANCE=true",
                    "scope": scope,
                }
            )
        )

    try:
        writeback_dataset = await dataset_registry.get_dataset(dataset_id=writeback_dataset_id)
    except Exception as exc:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "writeback_acl_unverifiable",
                    "message": "Failed to load writeback dataset",
                    "scope": scope,
                    "writeback_dataset_id": writeback_dataset_id,
                    "detail": str(exc),
                }
            )
        ) from exc
    if not writeback_dataset:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "writeback_acl_unverifiable",
                    "message": "Writeback dataset not found",
                    "scope": scope,
                    "writeback_dataset_id": writeback_dataset_id,
                }
            )
        )
    if writeback_dataset.db_name != db_name:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "writeback_acl_unverifiable",
                    "message": "Writeback dataset db_name mismatch",
                    "scope": scope,
                    "writeback_dataset_id": writeback_dataset_id,
                    "writeback_db_name": writeback_dataset.db_name,
                    "expected_db_name": db_name,
                }
            )
        )

    try:
        writeback_acl = await dataset_registry.get_access_policy(
            db_name=db_name,
            scope=scope,
            subject_type="dataset",
            subject_id=writeback_dataset_id,
        )
    except Exception as exc:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "writeback_acl_unverifiable",
                    "message": "Failed to load writeback dataset ACL policy",
                    "scope": scope,
                    "writeback_dataset_id": writeback_dataset_id,
                    "detail": str(exc),
                }
            )
        ) from exc

    if not writeback_acl or not isinstance(writeback_acl.policy, dict) or not writeback_acl.policy:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "writeback_acl_unverifiable",
                    "message": "Writeback dataset ACL policy missing/empty",
                    "scope": scope,
                    "writeback_dataset_id": writeback_dataset_id,
                }
            )
        )

    actor_id = str(submitted_by or "").strip()
    if actor_id and actor_id != "system":
        tags = build_principal_tags(
            principal_type=submitted_by_type,
            principal_id=actor_id,
            role=actor_role,
        )
        if not policy_allows(policy=writeback_acl.policy, principal_tags=tags):
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_acl_denied",
                        "message": "Actor is not allowed by writeback dataset ACL",
                        "scope": scope,
                        "writeback_dataset_id": writeback_dataset_id,
                        "submitted_by": actor_id,
                        "role": actor_role,
                    }
                )
            )

    for class_id in sorted({cid for cid in class_ids if cid}):
        try:
            object_resource = await resources.get_resource(
                db_name,
                branch=ontology_commit_id,
                resource_type="object_type",
                resource_id=class_id,
            )
        except Exception as exc:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_acl_unverifiable",
                        "message": "Failed to load object_type resource for writeback governance checks",
                        "scope": scope,
                        "class_id": class_id,
                        "detail": str(exc),
                    }
                )
            ) from exc

        obj_spec = object_resource.get("spec") if isinstance(object_resource, dict) else None
        if not isinstance(obj_spec, dict):
            obj_spec = {}
        backing_dataset_id = extract_backing_dataset_id(obj_spec)
        if not backing_dataset_id:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_acl_unverifiable",
                        "message": "object_type.spec.backing_source.dataset_id is required for writeback governance checks",
                        "scope": scope,
                        "class_id": class_id,
                    }
                )
            )

        try:
            backing_dataset = await dataset_registry.get_dataset(dataset_id=backing_dataset_id)
        except Exception as exc:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_acl_unverifiable",
                        "message": "Failed to load backing dataset for writeback governance checks",
                        "scope": scope,
                        "class_id": class_id,
                        "backing_dataset_id": backing_dataset_id,
                        "detail": str(exc),
                    }
                )
            ) from exc

        if not backing_dataset:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_acl_unverifiable",
                        "message": "Backing dataset not found",
                        "scope": scope,
                        "class_id": class_id,
                        "backing_dataset_id": backing_dataset_id,
                    }
                )
            )
        if backing_dataset.db_name != db_name:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_acl_unverifiable",
                        "message": "Backing dataset db_name mismatch",
                        "scope": scope,
                        "class_id": class_id,
                        "backing_dataset_id": backing_dataset_id,
                        "backing_db_name": backing_dataset.db_name,
                        "expected_db_name": db_name,
                    }
                )
            )

        try:
            backing_acl = await dataset_registry.get_access_policy(
                db_name=db_name,
                scope=scope,
                subject_type="dataset",
                subject_id=backing_dataset_id,
            )
        except Exception as exc:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_acl_unverifiable",
                        "message": "Failed to load backing dataset ACL policy",
                        "scope": scope,
                        "class_id": class_id,
                        "backing_dataset_id": backing_dataset_id,
                        "detail": str(exc),
                    }
                )
            ) from exc

        if not backing_acl or not isinstance(backing_acl.policy, dict) or not backing_acl.policy:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_acl_unverifiable",
                        "message": "Backing dataset ACL policy missing/empty",
                        "scope": scope,
                        "class_id": class_id,
                        "backing_dataset_id": backing_dataset_id,
                    }
                )
            )

        if not policies_aligned(backing_acl.policy, writeback_acl.policy):
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_acl_misaligned",
                        "message": "Writeback dataset ACL must match backing dataset ACL",
                        "scope": scope,
                        "class_id": class_id,
                        "writeback_dataset_id": writeback_dataset_id,
                        "backing_dataset_id": backing_dataset_id,
                    }
                )
            )


@trace_external_call("oms.action_simulation.preflight_action_writeback")
async def preflight_action_writeback(
    *,
    resources: Optional[OntologyResourceService] = None,
    base_storage: StorageService,
    dataset_registry: Optional[DatasetRegistry],
    db_name: str,
    action_type_id: str,
    ontology_commit_id: str,
    action_spec: Dict[str, Any],
    action_type_rid: str,
    input_payload: Dict[str, Any],
    assumptions: Optional[Dict[str, Any]] = None,
    submitted_by: str,
    submitted_by_type: str,
    actor_role: Optional[str],
    permission_profile: Optional[ActionPermissionProfile],
    base_branch: str,
    overlay_branch: Optional[str],
) -> ActionPreflight:
    resource_service = resources or OntologyResourceService()

    # Input schema validation.
    try:
        validated_input = validate_action_input(input_schema=action_spec.get("input_schema"), payload=input_payload)
    except ActionInputValidationError as exc:
        raise ActionSimulationRejected(
            _attach_enterprise({"error": "action_input_invalid", "message": str(exc)}),
            status_code=400,
        ) from exc
    except ActionInputSchemaError as exc:
        raise ActionSimulationRejected(
            _attach_enterprise({"error": "action_type_input_schema_invalid", "message": str(exc)}),
            status_code=500,
        ) from exc

    implementation = action_spec.get("implementation")
    try:
        compiled_shape = compile_action_change_shape(implementation, input_payload=validated_input)
    except ActionImplementationError as exc:
        raise ActionSimulationRejected(
            _attach_enterprise({"error": "action_implementation_invalid", "message": str(exc)}),
            status_code=500,
        ) from exc

    if not compiled_shape:
        raise ActionSimulationRejected(
            _attach_enterprise({"error": "action_no_targets", "message": "implementation resolved to zero targets"}),
            status_code=400,
        )

    assumptions_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if assumptions is not None:
        if not isinstance(assumptions, dict):
            raise ActionSimulationRejected(
                _attach_enterprise({"error": "simulation_assumption_invalid", "message": "assumptions must be an object"}),
                status_code=400,
            )
        raw_targets = assumptions.get("targets")
        if raw_targets is not None and not isinstance(raw_targets, list):
            raise ActionSimulationRejected(
                _attach_enterprise({"error": "simulation_assumption_invalid", "message": "assumptions.targets must be a list"}),
                status_code=400,
            )
        for idx, item in enumerate(raw_targets or []):
            if not isinstance(item, dict):
                raise ActionSimulationRejected(
                    _attach_enterprise({"error": "simulation_assumption_invalid", "message": "assumptions.targets entries must be objects", "index": idx}),
                    status_code=400,
                )
            class_id = safe_str(item.get("class_id"))
            instance_id = safe_str(item.get("instance_id"))
            if not class_id or not instance_id:
                raise ActionSimulationRejected(
                    _attach_enterprise({"error": "simulation_assumption_invalid", "message": "assumptions.targets requires class_id and instance_id", "index": idx}),
                    status_code=400,
                )
            key = (class_id, instance_id)
            if key in assumptions_by_key:
                raise ActionSimulationRejected(
                    _attach_enterprise({"error": "simulation_assumption_invalid", "message": "duplicate assumptions target", "class_id": class_id, "instance_id": instance_id}),
                    status_code=400,
                )
            assumptions_by_key[key] = item

    target_keys = {(safe_str(t.class_id), safe_str(t.instance_id)) for t in compiled_shape}
    unknown_targets = [{"class_id": c, "instance_id": i} for (c, i) in assumptions_by_key.keys() if (c, i) not in target_keys]
    unknown_targets.sort(key=lambda item: (str(item.get("class_id") or ""), str(item.get("instance_id") or "")))
    if unknown_targets:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "simulation_assumption_target_not_found",
                    "message": "assumptions provided for a target that is not part of this action",
                    "targets": unknown_targets,
                }
            ),
            status_code=400,
        )

    writeback_target = action_spec.get("writeback_target")
    if not isinstance(writeback_target, dict):
        writeback_target = {}
    repo = str(writeback_target.get("repo") or AppConfig.ONTOLOGY_WRITEBACK_REPO).strip()
    branch = str(writeback_target.get("branch") or AppConfig.get_ontology_writeback_branch(db_name)).strip()
    if not repo or not branch:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "action_type_missing_writeback_target",
                    "message": "writeback_target.repo and .branch are required",
                }
            ),
            status_code=500,
        )

    overlay_branch_resolved = _coerce_overlay_branch(db_name=db_name, writeback_target={"repo": repo, "branch": branch}, overlay_branch=overlay_branch)

    if AppConfig.WRITEBACK_ENFORCE_GOVERNANCE:
        if not dataset_registry:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "writeback_governance_unavailable",
                        "message": "DatasetRegistry not available (WRITEBACK_ENFORCE_GOVERNANCE=true)",
                    }
                ),
                status_code=503,
            )
        await _check_writeback_dataset_acl_alignment(
            db_name=db_name,
            submitted_by=submitted_by,
            submitted_by_type=submitted_by_type,
            actor_role=actor_role,
            ontology_commit_id=ontology_commit_id,
            resources=resource_service,
            dataset_registry=dataset_registry,
            class_ids={t.class_id for t in compiled_shape if t.class_id},
        )

    action_conflict_policy = parse_conflict_policy(action_spec.get("conflict_policy"))
    get_object_type_meta = build_object_type_meta_resolver(
        resources=resource_service,
        db_name=db_name,
        branch=ontology_commit_id,
    )
    required_interfaces = extract_required_action_interfaces(action_spec)
    required_interface_set = set(required_interfaces)
    class_interface_refs: Dict[str, List[str]] = {}
    class_field_types: Dict[str, Dict[str, str]] = {}

    target_docs: Dict[Tuple[str, str], Dict[str, Any]] = {}
    access_docs: Dict[Tuple[str, str], Dict[str, Any]] = {}
    assumption_reports: Dict[Tuple[str, str], Dict[str, Any]] = {}
    try:
        loaded_target_states = await load_action_target_states(
            db_name=db_name,
            base_branch=base_branch,
            contract_branch=ontology_commit_id,
            compiled_targets=compiled_shape,
            resources=resource_service,
            storage=base_storage,
            required_interfaces=required_interface_set,
            allow_missing_base_state=False,
            load_runtime_contract_fn=load_action_target_runtime_contract,
        )
    except ActionTargetStateLoadError as exc:
        if exc.kind == "invalid_target":
            raise ActionSimulationRejected(
                _attach_enterprise({"error": "action_implementation_invalid", "message": "each compiled target requires class_id and instance_id"}),
                status_code=500,
            ) from exc
        if exc.kind == "target_class_not_found":
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "action_target_class_not_found",
                        "message": "Target class not found at ontology commit",
                        "class_id": exc.details.get("class_id"),
                        "ontology_commit_id": ontology_commit_id,
                    }
                ),
                status_code=404,
            ) from exc
        if exc.kind == "base_state_not_found":
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "base_instance_not_found",
                        "message": "Base instance state not found",
                        "class_id": exc.details.get("class_id"),
                        "instance_id": exc.details.get("instance_id"),
                        "base_branch": base_branch,
                    }
                ),
                status_code=404,
            ) from exc
        if exc.kind == "required_interfaces_missing":
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "action_interface_not_implemented",
                        "message": "Action target class does not satisfy required interfaces",
                        "class_id": exc.details.get("class_id"),
                        "required_interfaces": exc.details.get("required_interfaces"),
                        "implemented_interfaces": exc.details.get("implemented_interfaces"),
                        "missing_interfaces": exc.details.get("missing_interfaces"),
                    }
                ),
                status_code=403,
            ) from exc
        if exc.kind == "storage_unavailable":
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "base_instance_state_unavailable",
                        "message": "Unable to load base instance state",
                        "class_id": exc.details.get("class_id"),
                        "instance_id": exc.details.get("instance_id"),
                        "base_branch": base_branch,
                    }
                ),
                status_code=503,
            ) from exc
        raise

    for loaded_target in loaded_target_states:
        class_id = loaded_target.class_id
        instance_id = loaded_target.instance_id
        class_interface_refs[class_id] = list(loaded_target.interfaces)
        class_field_types[class_id] = dict(loaded_target.field_types)
        base_state = dict(loaded_target.base_state)
        access_docs[(class_id, instance_id)] = base_state

        assumed_state = base_state
        assumption = assumptions_by_key.get((class_id, instance_id))
        base_overrides_report: Dict[str, Any] = {"fields": [], "links": []}
        if assumption and isinstance(assumption.get("base_overrides"), dict):
            assumed_state, base_overrides_report = _apply_assumption_patch(
                scope="base_overrides",
                base_state=base_state,
                patch=assumption.get("base_overrides") or {},
            )
        target_docs[(class_id, instance_id)] = assumed_state
        if assumption:
            assumption_reports[(class_id, instance_id)] = {
                "base_overrides_applied": base_overrides_report,
                "observed_base_overrides_applied": {"fields": [], "links": []},
            }

    user_ctx: Dict[str, Any] = {
        "id": submitted_by,
        "role": actor_role,
        "is_system": submitted_by == "system",
    }
    try:
        compiled_targets = compile_action_implementation(
            implementation,
            input_payload=validated_input,
            user=user_ctx,
            target_docs=target_docs,
            now=utcnow(),
        )
    except ActionImplementationError as exc:
        raise ActionSimulationRejected(
            _attach_enterprise({"error": "action_implementation_compile_error", "message": str(exc)}),
            status_code=500,
        ) from exc

    changes_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {(t.class_id, t.instance_id): dict(t.changes or {}) for t in compiled_targets}

    loaded: List[TargetPreflight] = []
    for item in compiled_shape:
        class_id = safe_str(item.class_id)
        instance_id = safe_str(item.instance_id)
        base_state = target_docs.get((class_id, instance_id))
        if not isinstance(base_state, dict) or not base_state:
            raise ActionSimulationRejected(
                _attach_enterprise({"error": "base_instance_state_unavailable", "message": "base_state missing for compiled target"}),
                status_code=500,
            )
        access_state = access_docs.get((class_id, instance_id))
        if not isinstance(access_state, dict) or not access_state:
            access_state = base_state
        changes = changes_by_key.get((class_id, instance_id))
        if not isinstance(changes, dict):
            raise ActionSimulationRejected(
                _attach_enterprise({"error": "action_implementation_compile_error", "message": "compiled changes missing for target"}),
                status_code=500,
            )

        lifecycle_id = derive_lifecycle_id(base_state) or DEFAULT_LIFECYCLE_ID
        obj_meta = await get_object_type_meta(class_id)
        object_type_rid = format_resource_rid(
            resource_type="object_type",
            resource_id=class_id,
            rev=obj_meta.get("rev") if isinstance(obj_meta, dict) else None,
        )
        observed_base = compute_observed_base(base=base_state, changes=changes)
        assumption = assumptions_by_key.get((class_id, instance_id))
        observed_override_report: Dict[str, Any] = {"fields": [], "links": []}
        if assumption and isinstance(assumption.get("observed_base_overrides"), dict):
            observed_override_report = _apply_observed_base_overrides(
                observed_base=observed_base,
                overrides=assumption.get("observed_base_overrides") or {},
            )
        base_token = compute_base_token(
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            lifecycle_id=lifecycle_id,
            base_doc=base_state,
            object_type_version_id=object_type_rid,
        )
        conflict_fields = detect_overlap_fields(observed_base=observed_base, current_base=base_state)
        conflict_links = detect_overlap_links(observed_base=observed_base, current_base=base_state, changes=changes)

        assumptions_payload = None
        report = assumption_reports.get((class_id, instance_id))
        if report is not None:
            report["observed_base_overrides_applied"] = observed_override_report
            assumptions_payload = report

        loaded.append(
            TargetPreflight(
                resource_rid=object_type_rid,
                class_id=class_id,
                instance_id=instance_id,
                lifecycle_id=lifecycle_id,
                base_state=base_state,
                access_base_state=access_state,
                changes=changes,
                observed_base=observed_base,
                base_token=base_token,
                conflict_fields=conflict_fields,
                conflict_links=conflict_links,
                object_conflict_policy=obj_meta.get("conflict_policy") if isinstance(obj_meta, dict) else None,
                field_types=class_field_types.get(class_id, {}),
                assumptions=assumptions_payload,
            )
        )

    try:
        resolved_permission_profile = permission_profile or resolve_action_permission_profile(action_spec)
    except ActionPermissionProfileError as exc:
        raise ActionSimulationRejected(
            _attach_enterprise(
                {
                    "error": "action_permission_profile_invalid",
                    "message": str(exc),
                    "field": exc.field,
                }
            ),
            status_code=409,
        ) from exc

    enforce_data_access = requires_action_data_access_enforcement(
        profile=resolved_permission_profile,
        global_enforcement=AppConfig.WRITEBACK_ENFORCE_ACTION_DATA_ACCESS,
    )
    principal_tags = None
    if submitted_by and submitted_by != "system":
        principal_tags = build_principal_tags(
            principal_type=submitted_by_type,
            principal_id=submitted_by,
            role=actor_role,
        )
    enforce_edit_access = bool(principal_tags)

    if enforce_data_access or enforce_edit_access:
        if not dataset_registry:
            error_key = "data_access_unverifiable" if enforce_data_access else "edit_access_unverifiable"
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": error_key,
                        "message": (
                            "DatasetRegistry not available "
                            "(required for action target access/edit policy verification)"
                        ),
                    }
                ),
                status_code=503,
            )
        access_report = await evaluate_action_target_data_access(
            dataset_registry=dataset_registry,
            db_name=db_name,
            targets=[
                {
                    "class_id": tgt.class_id,
                    "instance_id": tgt.instance_id,
                    "base_state": tgt.access_base_state,
                    "changes": tgt.changes,
                    "field_types": tgt.field_types,
                }
                for tgt in loaded
            ],
            enforce_data_access_policy=enforce_data_access,
            principal_tags=principal_tags,
            enforce_object_edit_policy=enforce_edit_access,
            enforce_attachment_edit_policy=enforce_edit_access,
            enforce_object_set_edit_policy=enforce_edit_access,
            fail_on_missing_edit_policy=False,
        )
        if enforce_data_access and access_report.unverifiable:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "data_access_unverifiable",
                        "message": "Unable to verify one or more target rows under data_access policy",
                        "unverifiable": access_report.unverifiable,
                    }
                ),
                status_code=503,
            )
        if enforce_data_access and access_report.denied:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "data_access_denied",
                        "message": "Actor cannot access one or more target rows under data_access policy",
                        "denied": access_report.denied,
                    }
                ),
                status_code=403,
            )
        if enforce_edit_access and access_report.edit_unverifiable:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "edit_access_unverifiable",
                        "message": "Unable to verify one or more target edit permissions",
                        "unverifiable": access_report.edit_unverifiable,
                    }
                ),
                status_code=503,
            )
        if enforce_edit_access and access_report.edit_denied:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "edit_access_denied",
                        "message": "Actor cannot edit one or more target object types/fields",
                        "denied": access_report.edit_denied,
                    }
                ),
                status_code=403,
            )

    submission_criteria = str(action_spec.get("submission_criteria") or "").strip()
    if submission_criteria:
        target_docs_list = [t.base_state for t in loaded]
        target_meta = [{"class_id": t.class_id, "instance_id": t.instance_id, "lifecycle_id": t.lifecycle_id} for t in loaded]
        criteria_vars: Dict[str, Any] = {
            "user": {"id": submitted_by, "role": actor_role, "is_system": submitted_by == "system"},
            "input": validated_input,
            "targets": target_docs_list,
            "target": target_docs_list[0] if len(target_docs_list) == 1 else None,
            "db_name": db_name,
            "base_branch": base_branch,
            "targets_meta": target_meta,
        }
        try:
            criteria_ok = safe_eval_bool_expression(submission_criteria, variables=criteria_vars)
        except BoolExpressionError as exc:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "submission_criteria_error",
                        "message": str(exc),
                        "submission_criteria": submission_criteria,
                        "targets": target_meta,
                    }
                ),
                status_code=500,
            ) from exc
        if not criteria_ok:
            failure_info = infer_submission_criteria_failure_reason(submission_criteria)
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "submission_criteria_failed",
                        "message": "submission_criteria evaluated to false",
                        "reason": failure_info.get("reason"),
                        "reasons": failure_info.get("reasons"),
                        "criteria_identifiers": failure_info.get("identifiers"),
                        "actor_role": actor_role,
                        "submission_criteria": submission_criteria,
                        "targets": target_meta,
                    }
                ),
                status_code=403,
            )

    validation_rules = action_spec.get("validation_rules")
    if validation_rules is not None and not isinstance(validation_rules, list):
        raise ActionSimulationRejected(
            _attach_enterprise({"error": "validation_rules_invalid", "message": "validation_rules must be a list"}),
            status_code=500,
        )
    if isinstance(validation_rules, list) and validation_rules:
        target_docs_list = [t.base_state for t in loaded]
        target_meta = [{"class_id": t.class_id, "instance_id": t.instance_id, "lifecycle_id": t.lifecycle_id} for t in loaded]
        base_vars: Dict[str, Any] = {
            "user": {"id": submitted_by, "role": actor_role, "is_system": submitted_by == "system"},
            "input": validated_input,
            "targets": target_docs_list,
            "target": target_docs_list[0] if len(target_docs_list) == 1 else None,
            "db_name": db_name,
            "base_branch": base_branch,
            "targets_meta": target_meta,
        }
        for rule_idx, rule in enumerate(validation_rules):
            if not isinstance(rule, dict):
                raise ActionSimulationRejected(
                    _attach_enterprise(
                        {"error": "validation_rule_invalid", "message": "validation_rules entries must be objects", "rule_index": rule_idx}
                    ),
                    status_code=500,
                )
            rule_type = str(rule.get("type") or "").strip().lower()
            if rule_type != "assert":
                raise ActionSimulationRejected(
                    _attach_enterprise(
                        {
                            "error": "validation_rule_invalid",
                            "message": "only validation_rules.type=assert is supported in P0",
                            "rule_index": rule_idx,
                        }
                    ),
                    status_code=500,
                )
            scope = str(rule.get("scope") or "each_target").strip().lower()
            expr = str(rule.get("expr") or rule.get("expression") or "").strip()
            msg = str(rule.get("message") or "").strip() or None
            if not expr:
                raise ActionSimulationRejected(
                    _attach_enterprise({"error": "validation_rule_invalid", "message": "validation_rules.assert requires expr", "rule_index": rule_idx}),
                    status_code=500,
                )

            if scope == "action":
                try:
                    ok = safe_eval_bool_expression(expr, variables=base_vars)
                except BoolExpressionError as exc:
                    raise ActionSimulationRejected(
                        _attach_enterprise({"error": "validation_rule_error", "message": str(exc), "rule_index": rule_idx, "scope": scope, "expr": expr}),
                        status_code=500,
                    ) from exc
                if not ok:
                    raise ActionSimulationRejected(
                        _attach_enterprise({"error": "validation_rule_failed", "message": msg or "validation rule evaluated to false", "rule_index": rule_idx, "scope": scope, "expr": expr}),
                        status_code=400,
                    )
                continue

            if scope == "each_target":
                for target_idx, target_doc in enumerate(target_docs_list):
                    each_vars = dict(base_vars)
                    each_vars["target"] = target_doc
                    try:
                        ok = safe_eval_bool_expression(expr, variables=each_vars)
                    except BoolExpressionError as exc:
                        raise ActionSimulationRejected(
                            _attach_enterprise({"error": "validation_rule_error", "message": str(exc), "rule_index": rule_idx, "scope": scope, "expr": expr, "target": target_meta[target_idx] if target_idx < len(target_meta) else None}),
                            status_code=500,
                        ) from exc
                    if not ok:
                        raise ActionSimulationRejected(
                            _attach_enterprise({"error": "validation_rule_failed", "message": msg or "validation rule evaluated to false", "rule_index": rule_idx, "scope": scope, "expr": expr, "target": target_meta[target_idx] if target_idx < len(target_meta) else None}),
                            status_code=400,
                        )
                continue

            raise ActionSimulationRejected(
                _attach_enterprise({"error": "validation_rule_invalid", "message": "validation_rules.assert scope must be action or each_target", "rule_index": rule_idx, "scope": scope}),
                status_code=500,
            )

    return ActionPreflight(
        db_name=db_name,
        action_type_id=action_type_id,
        action_type_rid=action_type_rid,
        ontology_commit_id=ontology_commit_id,
        base_branch=base_branch,
        overlay_branch=overlay_branch_resolved,
        writeback_target={"repo": repo, "branch": branch},
        submitted_by=submitted_by,
        submitted_by_type=submitted_by_type,
        actor_role=actor_role,
        input_payload=validated_input,
        action_conflict_policy=action_conflict_policy,
        loaded_targets=loaded,
    )


def build_patchset_for_scenario(
    *,
    preflight: ActionPreflight,
    action_log_id: str,
    conflict_policy_override: Optional[str],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    targets: List[Dict[str, Any]] = []
    conflicts: List[Dict[str, Any]] = []
    policies_used: set[str] = set()

    for tgt in preflight.loaded_targets:
        policy = None
        if conflict_policy_override:
            policy = normalize_conflict_policy(conflict_policy_override)
        else:
            policy = preflight.action_conflict_policy or tgt.object_conflict_policy or "FAIL"
        policy = normalize_conflict_policy(policy)
        policies_used.add(policy)

        applied_changes, resolution = resolve_applied_changes(
            conflict_policy=policy,
            changes=tgt.changes,
            conflict_fields=tgt.conflict_fields,
            conflict_links=tgt.conflict_links,
        )
        has_conflict = bool(tgt.conflict_fields) or bool(tgt.conflict_links)
        if has_conflict:
            conflicts.append(
                {
                    "class_id": tgt.class_id,
                    "instance_id": tgt.instance_id,
                    "lifecycle_id": tgt.lifecycle_id,
                    "fields": tgt.conflict_fields,
                    "links": tgt.conflict_links,
                    "policy": policy,
                    "resolution": resolution,
                }
            )

        targets.append(
            {
                "resource_rid": tgt.resource_rid,
                "instance_id": tgt.instance_id,
                "lifecycle_id": tgt.lifecycle_id,
                "base_token": tgt.base_token,
                "observed_base": tgt.observed_base,
                "changes": tgt.changes,
                "applied_changes": applied_changes,
                "conflict": {
                    "status": "OVERLAP" if has_conflict else "NONE",
                    "fields": tgt.conflict_fields,
                    "links": tgt.conflict_links,
                    "policy": policy,
                    "resolution": resolution,
                },
            }
        )

    should_reject = any(str(c.get("resolution") or "").strip().upper() == "REJECTED" for c in conflicts)
    if should_reject:
        error_payload = _attach_enterprise(
            {
                "error": "conflict_detected",
                "conflict_policy": conflict_policy_override or preflight.action_conflict_policy,
                "conflict_policies_used": sorted(policies_used),
                "conflicts": conflicts,
                "attempted_changes": targets,
            }
        )
        raise ActionSimulationRejected(error_payload, status_code=409)

    patchset = {
        "action_log_id": action_log_id,
        "action_type_rid": preflight.action_type_rid,
        "ontology_commit_id": preflight.ontology_commit_id,
        "targets": targets,
        "metadata": {
            "submitted_by": preflight.submitted_by,
            "submitted_at": utcnow().isoformat(),
            "correlation_id": None,
            "conflict_policy": conflict_policy_override or preflight.action_conflict_policy,
            "conflict_policies_used": sorted(policies_used),
            "simulation": True,
        },
    }
    return patchset, targets, conflicts, sorted(policies_used)


@trace_external_call("oms.action_simulation.simulate_effects_for_patchset")
async def simulate_effects_for_patchset(
    *,
    base_storage: StorageService,
    lakefs_storage: LakeFSStorageService,
    db_name: str,
    base_branch: str,
    overlay_branch: str,
    writeback_repo: str,
    writeback_branch: str,
    action_log_id: str,
    patchset_id: str,
    targets: List[Dict[str, Any]],
    base_overrides_by_target: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    merger = WritebackMergeService(base_storage=base_storage, lakefs_storage=lakefs_storage)

    per_target: List[Dict[str, Any]] = []
    queue_objects: List[Dict[str, Any]] = []

    for t in targets:
        resource_rid = safe_str(t.get("resource_rid"))
        class_id = strip_rid_revision(resource_rid) or ""
        instance_id = safe_str(t.get("instance_id"))
        lifecycle_id = safe_str(t.get("lifecycle_id") or DEFAULT_LIFECYCLE_ID) or DEFAULT_LIFECYCLE_ID
        applied_changes = t.get("applied_changes") if isinstance(t.get("applied_changes"), dict) else {}
        base_token = t.get("base_token") if isinstance(t.get("base_token"), dict) else {}

        if not class_id or not instance_id:
            continue

        try:
            merged = await merger.merge_instance(
                db_name=db_name,
                base_branch=base_branch,
                overlay_branch=overlay_branch,
                class_id=class_id,
                instance_id=instance_id,
                writeback_repo=writeback_repo,
                writeback_branch=writeback_branch,
            )
            baseline = merged.document
        except FileNotFoundError:
            baseline = {
                "instance_id": instance_id,
                "class_id": class_id,
                "db_name": db_name,
                "branch": overlay_branch,
                "overlay_tombstone": False,
                "lifecycle_id": lifecycle_id,
                "data": {},
            }
        except Exception as exc:
            raise ActionSimulationRejected(
                _attach_enterprise(
                    {
                        "error": "base_instance_state_unavailable",
                        "message": "Unable to load authoritative base instance state",
                        "class_id": class_id,
                        "instance_id": instance_id,
                        "base_branch": base_branch,
                    }
                ),
                status_code=503,
            ) from exc

        effective = dict(baseline or {})
        data_payload = effective.get("data") if isinstance(effective.get("data"), dict) else {}

        assumption_patch = None
        if base_overrides_by_target and isinstance(base_overrides_by_target.get((class_id, instance_id)), dict):
            assumption_patch = base_overrides_by_target.get((class_id, instance_id)) or {}
        base_overrides_report: Dict[str, Any] = {"fields": [], "links": []}
        if isinstance(assumption_patch, dict) and assumption_patch:
            data_payload, base_overrides_report = _apply_assumption_patch(
                scope="base_overrides",
                base_state=data_payload,
                patch=assumption_patch,
            )

        tombstone = apply_changes_to_payload(data_payload, applied_changes)
        if tombstone:
            effective.update(
                {
                    "branch": overlay_branch,
                    "overlay_tombstone": True,
                    "lifecycle_id": lifecycle_id,
                    "data": {
                        "instance_id": instance_id,
                        "class_id": class_id,
                        "db_name": db_name,
                        "lifecycle_id": lifecycle_id,
                        "_metadata": {
                            "deleted": True,
                            "deleted_via": "writeback",
                            "deleted_patchset_id": patchset_id,
                        },
                    },
                }
            )
        else:
            effective["data"] = data_payload
            effective.update(
                {
                    "instance_id": instance_id,
                    "class_id": class_id,
                    "db_name": db_name,
                    "branch": overlay_branch,
                    "overlay_tombstone": False,
                    "lifecycle_id": lifecycle_id,
                    "base_token": base_token,
                    "patchset_commit_id": patchset_id,
                    "action_log_id": action_log_id,
                }
            )

        per_target.append(
            {
                "class_id": class_id,
                "instance_id": instance_id,
                "lifecycle_id": lifecycle_id,
                "overlay_doc_id": overlay_doc_id(instance_id=instance_id, lifecycle_id=lifecycle_id),
                "overlay_document": effective,
                "overlay_would_write": not is_noop_changes(applied_changes),
                "queue_would_write": not is_noop_changes(applied_changes),
                "assumptions_applied": {"base_overrides_applied": base_overrides_report} if base_overrides_report != {"fields": [], "links": []} else None,
            }
        )

        if not is_noop_changes(applied_changes):
            queue_objects.append(
                {
                    "object_type": class_id,
                    "instance_id": instance_id,
                    "lifecycle_id": lifecycle_id,
                    "queue_entry_prefix": ref_key(writeback_branch, queue_entry_prefix(object_type=class_id, instance_id=instance_id, lifecycle_id=lifecycle_id)),
                    "queue_entry_key_template": ref_key(
                        writeback_branch,
                        queue_entry_key(
                            object_type=class_id,
                            instance_id=instance_id,
                            lifecycle_id=lifecycle_id,
                            action_applied_seq="<seq>",
                            action_log_id=action_log_id,
                        ),
                    ),
                }
            )

    patchset_key = ref_key(patchset_id, writeback_patchset_key(action_log_id))

    return {
        "lakefs": {
            "writeback_repo": writeback_repo,
            "writeback_branch": writeback_branch,
            "patchset_key": patchset_key,
            "queue_objects": queue_objects,
        },
        "es_overlay": {
            "overlay_branch": overlay_branch,
            "documents": per_target,
        },
    }
