"""
Agent registry smoke test (BFF -> Agent -> Postgres).

This validates:
- allowlist upsert via BFF admin
- plan validation + approval recording
- agent run execution via BFF proxy
- registry persistence in spice_agent schema
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from urllib.parse import urlparse

import aiohttp
import asyncpg
import pytest

from tests.utils.auth import bff_auth_headers


BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")


async def _wait_for_run_status(
    session: aiohttp.ClientSession,
    *,
    run_id: str,
    timeout_seconds: int = 30,
    poll_interval_seconds: float = 0.5,
) -> str:
    deadline = time.monotonic() + timeout_seconds
    last_status = ""
    while time.monotonic() < deadline:
        async with session.get(f"{BFF_URL}/api/v1/agent/runs/{run_id}") as resp:
            if resp.status != 200:
                await asyncio.sleep(poll_interval_seconds)
                continue
            payload = await resp.json()
            last_status = ((payload.get("data") or {}).get("status") or "").lower()
            if last_status in {"completed", "failed"}:
                return last_status
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for agent run completion (last_status={last_status})")


async def _connect_postgres() -> asyncpg.Connection:
    dsn = os.getenv("POSTGRES_URL") or "postgresql://spiceadmin:spicepass123@localhost:55433/spicedb"
    parsed = urlparse(dsn)
    return await asyncpg.connect(
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        user=parsed.username or "spiceadmin",
        password=parsed.password or "spicepass123",
        database=(parsed.path or "/spicedb").lstrip("/") or "spicedb",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_agent_registry_smoke() -> None:
    headers = bff_auth_headers()
    tool_id = f"system.health.smoke.{uuid.uuid4().hex[:8]}"
    plan_id = str(uuid.uuid4())

    async with aiohttp.ClientSession(headers=headers) as session:
        resp = await session.post(
            f"{BFF_URL}/api/v1/admin/agent-tools",
            json={
                "tool_id": tool_id,
                "method": "GET",
                "path": "/api/v1/health",
                "risk_level": "read",
                "requires_approval": False,
                "requires_idempotency_key": False,
                "status": "ACTIVE",
            },
        )
        assert resp.status in {200, 201}
        payload = await resp.json()
        assert ((payload.get("data") or {}).get("policy") or {}).get("tool_id") == tool_id

        resp = await session.post(
            f"{BFF_URL}/api/v1/agent-plans/validate",
            json={
                "plan_id": plan_id,
                "goal": "agent registry smoke",
                "risk_level": "read",
                "requires_approval": False,
                "steps": [
                    {
                        "step_id": "step_0",
                        "tool_id": tool_id,
                        "method": "GET",
                    }
                ],
            },
        )
        assert resp.status == 200

        resp = await session.post(
            f"{BFF_URL}/api/v1/agent-plans/{plan_id}/approvals",
            json={"decision": "APPROVE", "comment": "smoke"},
        )
        assert resp.status in {200, 201}
        payload = await resp.json()
        assert ((payload.get("data") or {}).get("approval") or {}).get("plan_id") == plan_id

        resp = await session.post(
            f"{BFF_URL}/api/v1/agent/runs",
            json={
                "goal": "agent registry smoke",
                "steps": [
                    {
                        "tool_id": tool_id,
                        "service": "bff",
                        "method": "GET",
                        "path": "/api/v1/health",
                    }
                ],
                "context": {"plan_id": plan_id, "risk_level": "read"},
            },
        )
        assert resp.status in {200, 202}
        payload = await resp.json()
        run_id = ((payload.get("data") or {}).get("run_id") or "").strip()
        assert run_id

        status = await _wait_for_run_status(session, run_id=run_id)
        assert status == "completed"

    conn = await _connect_postgres()
    try:
        run_row = await conn.fetchrow(
            "SELECT run_id, plan_id, status FROM spice_agent.agent_runs WHERE run_id = $1",
            run_id,
        )
        assert run_row is not None
        assert str(run_row["plan_id"]) == plan_id
        assert str(run_row["status"]).upper() == "COMPLETED"

        step_row = await conn.fetchrow(
            "SELECT run_id, step_id, tool_id, status FROM spice_agent.agent_steps WHERE run_id = $1",
            run_id,
        )
        assert step_row is not None
        assert str(step_row["tool_id"]) == tool_id
        assert str(step_row["status"]).upper() == "SUCCESS"

        approval_row = await conn.fetchrow(
            "SELECT plan_id, decision FROM spice_agent.agent_approvals WHERE plan_id = $1",
            plan_id,
        )
        assert approval_row is not None
        assert str(approval_row["decision"]).upper() == "APPROVE"
    finally:
        await conn.close()
