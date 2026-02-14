from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

PERMISSION_MODEL_ONTOLOGY_ROLES = "ontology_roles"
PERMISSION_MODEL_DATASOURCE_DERIVED = "datasource_derived"

_PERMISSION_MODEL_ALIASES = {
    "ontology_roles": PERMISSION_MODEL_ONTOLOGY_ROLES,
    "ontology": PERMISSION_MODEL_ONTOLOGY_ROLES,
    "roles": PERMISSION_MODEL_ONTOLOGY_ROLES,
    "datasource_derived": PERMISSION_MODEL_DATASOURCE_DERIVED,
    "datasource": PERMISSION_MODEL_DATASOURCE_DERIVED,
    "data_access": PERMISSION_MODEL_DATASOURCE_DERIVED,
    "dataset_access": PERMISSION_MODEL_DATASOURCE_DERIVED,
}


class ActionPermissionProfileError(ValueError):
    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field


@dataclass(frozen=True)
class ActionPermissionProfile:
    permission_model: str = PERMISSION_MODEL_ONTOLOGY_ROLES
    edits_beyond_actions: bool = False

    @property
    def requires_permission_policy(self) -> bool:
        return self.permission_model == PERMISSION_MODEL_ONTOLOGY_ROLES

    @property
    def requires_data_access_enforcement(self) -> bool:
        return self.permission_model == PERMISSION_MODEL_DATASOURCE_DERIVED


def _coerce_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ActionPermissionProfileError(f"{field} must be a boolean", field=field)


def resolve_action_permission_profile(action_spec: Mapping[str, Any] | None) -> ActionPermissionProfile:
    spec = action_spec if isinstance(action_spec, Mapping) else {}
    raw_model = spec.get("permission_model")
    if raw_model is None:
        raw_model = spec.get("permissionModel")
    if raw_model is None or str(raw_model).strip() == "":
        permission_model = PERMISSION_MODEL_ONTOLOGY_ROLES
    else:
        normalized = _PERMISSION_MODEL_ALIASES.get(str(raw_model).strip().lower())
        if not normalized:
            allowed = ", ".join(sorted({PERMISSION_MODEL_ONTOLOGY_ROLES, PERMISSION_MODEL_DATASOURCE_DERIVED}))
            raise ActionPermissionProfileError(
                f"permission_model must be one of: {allowed}",
                field="permission_model",
            )
        permission_model = normalized

    raw_edits_beyond_actions = spec.get("edits_beyond_actions")
    if raw_edits_beyond_actions is None:
        raw_edits_beyond_actions = spec.get("editsBeyondActions")

    edits_beyond_actions = (
        _coerce_bool(raw_edits_beyond_actions, field="edits_beyond_actions")
        if raw_edits_beyond_actions is not None
        else False
    )
    return ActionPermissionProfile(
        permission_model=permission_model,
        edits_beyond_actions=edits_beyond_actions,
    )


def requires_action_data_access_enforcement(
    *,
    profile: ActionPermissionProfile,
    global_enforcement: bool,
) -> bool:
    return bool(global_enforcement) or profile.requires_data_access_enforcement
