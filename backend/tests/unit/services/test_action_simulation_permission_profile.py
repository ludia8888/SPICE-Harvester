from __future__ import annotations

from types import SimpleNamespace

import pytest

import oms.services.action_simulation_service as simulation_service
from oms.services.action_simulation_service import ActionSimulationRejected, enforce_action_permission
from shared.security.database_access import DatabaseAccessInspection, DatabaseAccessState


def _inspection(
    *,
    role: str | None = None,
    state: DatabaseAccessState = DatabaseAccessState.CONFIGURED,
) -> DatabaseAccessInspection:
    return DatabaseAccessInspection(state=state, role=role)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_allows_ontology_roles_model(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="DomainModeler")

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)

    role = await enforce_action_permission(
        db_name="demo",
        submitted_by="alice",
        submitted_by_type="user",
        action_spec={
            "permission_model": "ontology_roles",
            "permission_policy": {"effect": "ALLOW", "principals": ["role:DomainModeler"]},
        },
    )
    assert role == "DomainModeler"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_rejects_policy_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="DomainModeler")

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)

    with pytest.raises(ActionSimulationRejected) as exc:
        await enforce_action_permission(
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={
                "permission_model": "ontology_roles",
                "permission_policy": {"effect": "ALLOW", "principals": ["role:Owner"]},
            },
        )
    assert exc.value.status_code == 403
    assert exc.value.payload.get("error") == "PERMISSION_DENIED"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_allows_datasource_derived_without_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="DataEngineer")

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)

    role = await enforce_action_permission(
        db_name="demo",
        submitted_by="alice",
        submitted_by_type="user",
        action_spec={"permission_model": "datasource_derived"},
    )
    assert role == "DataEngineer"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_rejects_datasource_derived_for_non_engineer_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="DomainModeler")

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)

    with pytest.raises(ActionSimulationRejected) as exc:
        await enforce_action_permission(
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={"permission_model": "datasource_derived"},
        )
    assert exc.value.status_code == 403
    assert exc.value.payload.get("error") == "PERMISSION_DENIED"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_rejects_edits_beyond_actions_without_engineer_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="DomainModeler")

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)

    with pytest.raises(ActionSimulationRejected) as exc:
        await enforce_action_permission(
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={
                "permission_model": "ontology_roles",
                "permission_policy": {"effect": "ALLOW", "principals": ["role:DomainModeler"]},
                "edits_beyond_actions": True,
            },
        )
    assert exc.value.status_code == 403
    assert exc.value.payload.get("error") == "PERMISSION_DENIED"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_rejects_invalid_permission_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="Owner")

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)

    with pytest.raises(ActionSimulationRejected) as exc:
        await enforce_action_permission(
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={"permission_model": "invalid-model"},
        )
    assert exc.value.status_code == 409
    assert exc.value.payload.get("error") == "action_permission_profile_invalid"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_allows_when_project_policy_inheritance_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="Owner")

    class _Registry:
        async def connect(self) -> None:
            return None

        async def get_access_policy(self, *, db_name: str, scope: str, subject_type: str, subject_id: str):  # noqa: ANN201
            _ = db_name, scope, subject_type, subject_id
            return SimpleNamespace(policy={"effect": "ALLOW", "principals": ["role:Owner"]})

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)
    monkeypatch.setattr(simulation_service, "DatasetRegistry", lambda: _Registry())

    role = await enforce_action_permission(
        db_name="demo",
        submitted_by="alice",
        submitted_by_type="user",
        action_spec={
            "permission_model": "ontology_roles",
            "permission_policy": {
                "effect": "ALLOW",
                "principals": ["role:Owner"],
                "inherit_project_policy": True,
                "project_policy_scope": "action_access",
                "project_policy_subject_type": "project",
            },
        },
    )
    assert role == "Owner"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_rejects_when_required_project_policy_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="Owner")

    class _Registry:
        async def connect(self) -> None:
            return None

        async def get_access_policy(self, *, db_name: str, scope: str, subject_type: str, subject_id: str):  # noqa: ANN201
            _ = db_name, scope, subject_type, subject_id
            return None

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)
    monkeypatch.setattr(simulation_service, "DatasetRegistry", lambda: _Registry())

    with pytest.raises(ActionSimulationRejected) as exc:
        await enforce_action_permission(
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={
                "permission_model": "ontology_roles",
                "permission_policy": {
                    "effect": "ALLOW",
                    "principals": ["role:Owner"],
                    "inherit_project_policy": True,
                    "require_project_policy": True,
                },
            },
        )
    assert exc.value.status_code == 403
    assert exc.value.payload.get("error") == "PERMISSION_DENIED"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_rejects_when_inherited_project_policy_denies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="Owner")

    class _Registry:
        async def connect(self) -> None:
            return None

        async def get_access_policy(self, *, db_name: str, scope: str, subject_type: str, subject_id: str):  # noqa: ANN201
            _ = db_name, scope, subject_type, subject_id
            return SimpleNamespace(policy={"effect": "ALLOW", "principals": ["role:Security"]})

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)
    monkeypatch.setattr(simulation_service, "DatasetRegistry", lambda: _Registry())

    with pytest.raises(ActionSimulationRejected) as exc:
        await enforce_action_permission(
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={
                "permission_model": "ontology_roles",
                "permission_policy": {
                    "effect": "ALLOW",
                    "principals": ["role:Owner"],
                    "inherit_project_policy": True,
                },
            },
        )
    assert exc.value.status_code == 403
    assert exc.value.payload.get("error") == "PERMISSION_DENIED"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_rejects_when_registry_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(state=DatabaseAccessState.UNAVAILABLE)

    monkeypatch.setattr(simulation_service, "inspect_database_access", _fake_inspection)

    with pytest.raises(ActionSimulationRejected) as exc:
        await enforce_action_permission(
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={"permission_model": "datasource_derived"},
        )

    assert exc.value.status_code == 503
    assert exc.value.payload.get("error") == "database_access_registry_unavailable"
