from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

import oms.routers.action_async as action_async
import oms.services.action_simulation_service as simulation_service
from oms.dependencies import OMSDependencyProvider


def _build_action_spec() -> Dict[str, Any]:
    return {
        "permission_model": "datasource_derived",
        "input_schema": {
            "fields": [
                {
                    "name": "ticket",
                    "type": "object_ref",
                    "required": True,
                    "object_type": "Ticket",
                }
            ]
        },
        "writeback_target": {
            "repo": "ontology-writeback",
            "branch": "writeback-{db_name}",
        },
        "implementation": {
            "type": "template_v1",
            "targets": [
                {
                    "target": {"from": "input.ticket"},
                    "changes": {"set": {"status": "APPROVED"}},
                }
            ],
        },
    }


def _install_deployment_and_resource_mocks(
    monkeypatch: pytest.MonkeyPatch,
    *,
    action_spec: Dict[str, Any],
) -> None:
    class _FakeDeployments:
        async def get_latest_deployed_commit(self, *, db_name: str, target_branch: str):  # noqa: ANN001
            return {"ontology_commit_id": "commit-1"}

    class _FakeResourceService:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def get_resource(
            self,
            db_name: str,  # noqa: ARG002
            *,
            branch: str,  # noqa: ARG002
            resource_type: str,  # noqa: ARG002
            resource_id: str,  # noqa: ARG002
        ) -> Dict[str, Any]:
            return {"spec": action_spec, "metadata": {"rev": 1}}

    monkeypatch.setattr(action_async, "OntologyDeploymentRegistryV2", _FakeDeployments)
    monkeypatch.setattr(action_async, "OntologyResourceService", _FakeResourceService)


class _FakeEventStore:
    async def append_event(self, _event: Any) -> None:
        return None


@pytest.fixture
def action_async_app() -> FastAPI:
    app = FastAPI()
    app.include_router(action_async.foundry_router)

    async def _fake_db_name(db_name: str) -> str:
        return db_name

    async def _fake_event_store() -> _FakeEventStore:
        return _FakeEventStore()

    app.dependency_overrides[action_async.ensure_database_exists] = _fake_db_name
    app.dependency_overrides[OMSDependencyProvider.get_event_store] = _fake_event_store
    return app


@pytest.mark.unit
@pytest.mark.asyncio
async def test_submit_batch_returns_403_for_datasource_derived_without_data_engineer_role(
    action_async_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_ensure_ontology_database_exists(ontology: str) -> str:
        return ontology

    monkeypatch.setattr(action_async, "_ensure_ontology_database_exists", _fake_ensure_ontology_database_exists)
    _install_deployment_and_resource_mocks(monkeypatch, action_spec=_build_action_spec())
    monkeypatch.setattr(action_async, "compile_action_change_shape", lambda _impl, input_payload: [])

    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:  # noqa: ARG001
        return "DomainModeler"

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)

    transport = ASGITransport(app=action_async_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v2/ontologies/demo/actions/ApproveTicket/apply?branch=main",
            json={
                "options": {"mode": "VALIDATE_AND_EXECUTE"},
                "parameters": {"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
                "metadata": {"user_id": "alice", "user_type": "user"},
            },
        )

    assert response.status_code == 403, response.text


@pytest.mark.unit
@pytest.mark.asyncio
async def test_simulate_returns_503_when_datasource_derived_data_access_is_unverifiable(
    action_async_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_deployment_and_resource_mocks(monkeypatch, action_spec=_build_action_spec())

    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:  # noqa: ARG001
        return "DataEngineer"

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)

    class _FakeDatasetRegistry:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

    async def _fake_preflight(**kwargs: Any):  # noqa: ANN401, ARG001
        raise action_async.ActionSimulationRejected(
            {
                "error": "data_access_unverifiable",
                "message": "Unable to verify one or more target rows under data_access policy",
            },
            status_code=503,
        )

    monkeypatch.setattr(action_async, "DatasetRegistry", _FakeDatasetRegistry)
    monkeypatch.setattr(action_async, "create_storage_service", lambda _settings: object())
    monkeypatch.setattr(action_async, "create_lakefs_storage_service", lambda _settings: object())
    monkeypatch.setattr(action_async, "preflight_action_writeback", _fake_preflight)

    with pytest.raises(HTTPException) as exc_info:
        await action_async.simulate_action_async(
            db_name="demo",
            action_type_id="ApproveTicket",
            request=action_async.ActionSimulateRequest(
                input={"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
                metadata={"user_id": "alice", "user_type": "user"},
                base_branch="main",
                include_effects=False,
            ),
        )

    assert exc_info.value.status_code == 503
    assert "data_access_unverifiable" in str(exc_info.value.detail)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_submit_batch_returns_403_when_target_class_misses_required_interface(
    action_async_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_ensure_ontology_database_exists(ontology: str) -> str:
        return ontology

    monkeypatch.setattr(action_async, "_ensure_ontology_database_exists", _fake_ensure_ontology_database_exists)
    action_spec = _build_action_spec()
    action_spec["target_interfaces"] = ["IApproval"]
    _install_deployment_and_resource_mocks(monkeypatch, action_spec=action_spec)

    monkeypatch.setattr(
        action_async,
        "compile_action_change_shape",
        lambda _impl, input_payload: [
            SimpleNamespace(
                class_id="Ticket",
                instance_id="t1",
                changes={"set": {"status": "APPROVED"}, "unset": [], "link_add": [], "link_remove": [], "delete": False},
            )
        ],
    )

    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:  # noqa: ARG001
        return "DataEngineer"

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)

    transport = ASGITransport(app=action_async_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v2/ontologies/demo/actions/ApproveTicket/apply?branch=main",
            json={
                "options": {"mode": "VALIDATE_AND_EXECUTE"},
                "parameters": {"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
                "metadata": {"user_id": "alice", "user_type": "user"},
            },
        )

    assert response.status_code == 403, response.text
    assert "required interfaces" in response.text


@pytest.mark.unit
@pytest.mark.asyncio
async def test_submit_batch_returns_503_when_target_edit_access_is_unverifiable(
    action_async_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_ensure_ontology_database_exists(ontology: str) -> str:
        return ontology

    monkeypatch.setattr(action_async, "_ensure_ontology_database_exists", _fake_ensure_ontology_database_exists)
    _install_deployment_and_resource_mocks(monkeypatch, action_spec=_build_action_spec())

    monkeypatch.setattr(
        action_async,
        "compile_action_change_shape",
        lambda _impl, input_payload: [
            SimpleNamespace(
                class_id="Ticket",
                instance_id="t1",
                changes={"set": {"status": "APPROVED"}, "unset": [], "link_add": [], "link_remove": [], "delete": False},
            )
        ],
    )

    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:  # noqa: ARG001
        return "DataEngineer"

    class _FakeDatasetRegistry:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)
    monkeypatch.setattr(action_async, "DatasetRegistry", _FakeDatasetRegistry)
    async def _fake_access_report(**kwargs: Any) -> Any:  # noqa: ANN401, ARG001
        return SimpleNamespace(
            denied=[],
            unverifiable=[],
            edit_denied=[],
            edit_unverifiable=[{"class_id": "Ticket", "instance_id": "t1", "scope": "object_edit"}],
        )

    monkeypatch.setattr(action_async, "evaluate_action_target_data_access", _fake_access_report)

    transport = ASGITransport(app=action_async_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v2/ontologies/demo/actions/ApproveTicket/apply?branch=main",
            json={
                "options": {"mode": "VALIDATE_AND_EXECUTE"},
                "parameters": {"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
                "metadata": {"user_id": "alice", "user_type": "user"},
            },
        )

    assert response.status_code == 503, response.text


@pytest.mark.unit
@pytest.mark.asyncio
async def test_simulate_use_branch_head_no_longer_requires_terminus(
    action_async_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_deployment_and_resource_mocks(monkeypatch, action_spec=_build_action_spec())

    async def _fake_role(*, db_name: str, principal_type: str, principal_id: str) -> str:  # noqa: ARG001
        return "DataEngineer"

    class _FakeDatasetRegistry:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

    async def _fake_preflight(**kwargs: Any):  # noqa: ANN401
        raise action_async.ActionSimulationRejected(
            {
                "error": "probe",
                "ontology_commit_id": kwargs.get("ontology_commit_id"),
            },
            status_code=503,
        )

    monkeypatch.setattr(simulation_service, "get_database_access_role", _fake_role)
    monkeypatch.setattr(action_async, "DatasetRegistry", _FakeDatasetRegistry)
    monkeypatch.setattr(action_async, "create_storage_service", lambda _settings: object())
    monkeypatch.setattr(action_async, "create_lakefs_storage_service", lambda _settings: object())
    monkeypatch.setattr(action_async, "preflight_action_writeback", _fake_preflight)

    with pytest.raises(HTTPException) as exc_info:
        await action_async.simulate_action_async(
            db_name="demo",
            action_type_id="ApproveTicket",
            request=action_async.ActionSimulateRequest(
                input={"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
                metadata={"user_id": "alice", "user_type": "user"},
                base_branch="main",
                use_branch_head=True,
                include_effects=False,
            ),
        )

    assert exc_info.value.status_code == 503
    assert "branch:main" in str(exc_info.value.detail)
