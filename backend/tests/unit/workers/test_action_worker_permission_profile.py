from __future__ import annotations

import pytest

import action_worker.main as action_worker_module
from action_worker.main import ActionWorker
from shared.security.database_access import DatabaseAccessInspection, DatabaseAccessState
from shared.utils.action_permission_profile import ActionPermissionProfileError


def _worker_without_init() -> ActionWorker:
    return object.__new__(ActionWorker)


def _inspection(
    *,
    role: str | None = None,
    state: DatabaseAccessState = DatabaseAccessState.CONFIGURED,
) -> DatabaseAccessInspection:
    return DatabaseAccessInspection(state=state, role=role)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_action_worker_enforce_permission_allows_datasource_derived_without_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="DataEngineer")

    monkeypatch.setattr(action_worker_module, "inspect_database_access", _fake_inspection)

    role, profile = await ActionWorker._enforce_permission(
        _worker_without_init(),
        db_name="demo",
        submitted_by="alice",
        submitted_by_type="user",
        action_spec={
            "permission_model": "datasource_derived",
            "permission_policy": {"effect": "DENY", "principals": ["role:DataEngineer"]},
        },
    )
    assert role == "DataEngineer"
    assert profile.permission_model == "datasource_derived"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_action_worker_enforce_permission_rejects_edits_beyond_actions_without_engineer_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="DomainModeler")

    monkeypatch.setattr(action_worker_module, "inspect_database_access", _fake_inspection)

    with pytest.raises(PermissionError, match="Permission denied"):
        await ActionWorker._enforce_permission(
            _worker_without_init(),
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={
                "permission_model": "ontology_roles",
                "permission_policy": {"effect": "ALLOW", "principals": ["role:DomainModeler"]},
                "edits_beyond_actions": True,
            },
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_action_worker_enforce_permission_rejects_invalid_permission_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(role="Owner")

    monkeypatch.setattr(action_worker_module, "inspect_database_access", _fake_inspection)

    with pytest.raises(ActionPermissionProfileError):
        await ActionWorker._enforce_permission(
            _worker_without_init(),
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={"permission_model": "invalid-model"},
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_action_worker_enforce_permission_rejects_when_registry_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_inspection(*, db_name: str, principal_type: str, principal_id: str) -> DatabaseAccessInspection:
        return _inspection(state=DatabaseAccessState.UNAVAILABLE)

    monkeypatch.setattr(action_worker_module, "inspect_database_access", _fake_inspection)

    with pytest.raises(RuntimeError, match="Database access registry unavailable"):
        await ActionWorker._enforce_permission(
            _worker_without_init(),
            db_name="demo",
            submitted_by="alice",
            submitted_by_type="user",
            action_spec={"permission_model": "datasource_derived"},
        )
