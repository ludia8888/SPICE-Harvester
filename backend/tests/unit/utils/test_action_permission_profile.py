from __future__ import annotations

import pytest

from shared.utils.action_permission_profile import (
    ActionPermissionProfileError,
    PERMISSION_MODEL_DATASOURCE_DERIVED,
    PERMISSION_MODEL_ONTOLOGY_ROLES,
    requires_action_data_access_enforcement,
    resolve_action_permission_profile,
)


@pytest.mark.unit
def test_resolve_action_permission_profile_defaults() -> None:
    profile = resolve_action_permission_profile({})
    assert profile.permission_model == PERMISSION_MODEL_ONTOLOGY_ROLES
    assert profile.edits_beyond_actions is False
    assert profile.requires_permission_policy is True
    assert profile.requires_data_access_enforcement is False


@pytest.mark.unit
def test_resolve_action_permission_profile_supports_aliases() -> None:
    profile = resolve_action_permission_profile(
        {
            "permissionModel": "data_access",
            "editsBeyondActions": True,
        }
    )
    assert profile.permission_model == PERMISSION_MODEL_DATASOURCE_DERIVED
    assert profile.edits_beyond_actions is True
    assert profile.requires_permission_policy is False
    assert profile.requires_data_access_enforcement is True


@pytest.mark.unit
def test_resolve_action_permission_profile_rejects_invalid_model() -> None:
    with pytest.raises(ActionPermissionProfileError) as exc:
        resolve_action_permission_profile({"permission_model": "invalid"})
    assert exc.value.field == "permission_model"


@pytest.mark.unit
def test_resolve_action_permission_profile_rejects_non_boolean_edits_flag() -> None:
    with pytest.raises(ActionPermissionProfileError) as exc:
        resolve_action_permission_profile({"edits_beyond_actions": "true"})
    assert exc.value.field == "edits_beyond_actions"


@pytest.mark.unit
def test_requires_action_data_access_enforcement_for_profile_or_global_flag() -> None:
    ontology_profile = resolve_action_permission_profile({"permission_model": "ontology_roles"})
    datasource_profile = resolve_action_permission_profile({"permission_model": "datasource_derived"})

    assert requires_action_data_access_enforcement(profile=ontology_profile, global_enforcement=False) is False
    assert requires_action_data_access_enforcement(profile=datasource_profile, global_enforcement=False) is True
    assert requires_action_data_access_enforcement(profile=ontology_profile, global_enforcement=True) is True
