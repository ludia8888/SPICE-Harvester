from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from bff.routers.agent_functions import (
    AgentFunctionExecuteRequest,
    AgentFunctionUpsertRequest,
    execute_agent_function,
    list_agent_functions,
    upsert_agent_function,
)
from shared.security.user_context import UserPrincipal
from shared.services.agent_function_registry import AgentFunctionRecord


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.state = SimpleNamespace(user=principal)


def _principal(
    *,
    user_id: str = "user-1",
    tenant_id: str = "tenant-1",
    roles: tuple[str, ...] = (),
    verified: bool = True,
) -> UserPrincipal:
    return UserPrincipal(id=user_id, tenant_id=tenant_id, roles=roles, verified=verified)


class _FakeRegistry:
    def __init__(self) -> None:
        self.records: dict[tuple[str, str], AgentFunctionRecord] = {}

    async def upsert_function(self, **kwargs) -> AgentFunctionRecord:  # noqa: ANN003
        now = datetime.now(timezone.utc)
        record = AgentFunctionRecord(
            function_id=str(kwargs["function_id"]),
            version=str(kwargs.get("version") or "v1"),
            status=str(kwargs.get("status") or "ACTIVE"),
            handler=str(kwargs["handler"]),
            tags=list(kwargs.get("tags") or []),
            roles=list(kwargs.get("roles") or []),
            input_schema=dict(kwargs.get("input_schema") or {}),
            output_schema=dict(kwargs.get("output_schema") or {}),
            metadata=dict(kwargs.get("metadata") or {}),
            created_at=now,
            updated_at=now,
        )
        self.records[(record.function_id, record.version)] = record
        return record

    async def get_function(self, *, function_id: str, version: str, status: str | None = None):  # noqa: ANN001
        record = self.records.get((function_id, version))
        if not record:
            return None
        if status and str(record.status).upper() != str(status).upper():
            return None
        return record

    async def list_functions(  # noqa: ANN001
        self,
        *,
        function_id: str | None = None,
        status: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ):
        items = list(self.records.values())
        if function_id:
            items = [i for i in items if i.function_id == function_id]
        if status:
            items = [i for i in items if str(i.status).upper() == str(status).upper()]
        return items[offset : offset + limit]


@pytest.mark.asyncio
async def test_agent_functions_require_verified_user() -> None:
    registry = _FakeRegistry()
    req = _Request(principal=_principal(verified=False))
    with pytest.raises(HTTPException) as exc:
        await list_agent_functions(request=req, registry=registry)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_agent_functions_admin_upsert_and_execute() -> None:
    registry = _FakeRegistry()
    non_admin = _Request(principal=_principal(user_id="user-1", roles=(), verified=True))
    admin = _Request(principal=_principal(user_id="admin-1", roles=("admin",), verified=True))

    with pytest.raises(HTTPException) as exc:
        await upsert_agent_function(
            body=AgentFunctionUpsertRequest(function_id="echo", handler="echo", roles=["analyst"]),
            request=non_admin,
            registry=registry,
        )
    assert exc.value.status_code == 403

    created = await upsert_agent_function(
        body=AgentFunctionUpsertRequest(function_id="echo", handler="echo", roles=["analyst"]),
        request=admin,
        registry=registry,
    )
    assert created.status == "created"

    listed = await list_agent_functions(request=non_admin, registry=registry, status_filter="ACTIVE")
    assert listed.status == "success"
    assert listed.data and listed.data["count"] == 1

    with pytest.raises(HTTPException) as exc:
        await execute_agent_function(
            function_id="echo",
            body=AgentFunctionExecuteRequest(input={"hello": "world"}),
            request=non_admin,
            registry=registry,
        )
    assert exc.value.status_code == 403

    exec_req = _Request(principal=_principal(user_id="user-2", roles=("analyst",), verified=True))
    executed = await execute_agent_function(
        function_id="echo",
        body=AgentFunctionExecuteRequest(input={"hello": "world"}),
        request=exec_req,
        registry=registry,
    )
    assert executed.status == "success"
    assert executed.data and executed.data["result"] == {"hello": "world"}


@pytest.mark.asyncio
async def test_agent_functions_json_patch_handler() -> None:
    registry = _FakeRegistry()
    admin = _Request(principal=_principal(user_id="admin-1", roles=("admin",), verified=True))
    exec_req = _Request(principal=_principal(user_id="user-1", roles=(), verified=True))

    created = await upsert_agent_function(
        body=AgentFunctionUpsertRequest(function_id="patch", handler="json_patch"),
        request=admin,
        registry=registry,
    )
    assert created.status == "created"

    executed = await execute_agent_function(
        function_id="patch",
        body=AgentFunctionExecuteRequest(
            input={
                "base": {"a": 1},
                "ops": [{"op": "add", "path": "/b", "value": 2}],
            }
        ),
        request=exec_req,
        registry=registry,
    )
    assert executed.status == "success"
    assert executed.data and executed.data["result"] == {"a": 1, "b": 2}

