from __future__ import annotations

import pytest

import oms.services.action_simulation_service as simulation_service
from oms.services.action_simulation_service import ActionSimulationRejected, enforce_action_permission


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_action_permission_allows_ontology_roles_model(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:
        return "DomainModeler"

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)

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
    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:
        return "DomainModeler"

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)

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
    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:
        return "DataEngineer"

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)

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
    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:
        return "DomainModeler"

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)

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
    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:
        return "DomainModeler"

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)

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
    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:
        return "Owner"

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)

    with pytest.raises(ActionSimulationRejected) as exc:
        await enforce_action_permission(
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={"permission_model": "invalid-model"},
        )
    assert exc.value.status_code == 409
    assert exc.value.payload.get("error") == "action_permission_profile_invalid"
