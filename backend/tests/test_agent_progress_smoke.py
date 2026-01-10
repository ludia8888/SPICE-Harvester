"""
Agent progress smoke test (BFF -> Agent -> OMS bulk-create).

Validates:
- agent run execution via BFF proxy
- progress updates surfaced on GET /api/v1/agent/runs/{run_id}
- command completion via /api/v1/commands/{command_id}/status
"""

from __future__ import annotations

import asyncio
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pytest

from tests.utils.auth import bff_auth_headers


BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
INSTANCE_COUNT = int(os.getenv("AGENT_PROGRESS_INSTANCE_COUNT", "25"))
PK_FIELDS_RAW = os.getenv("AGENT_PROGRESS_PK_FIELDS", "customer_id,external_id")


def _normalize_pk_fields(raw: str) -> List[Tuple[str, str]]:
    output: List[Tuple[str, str]] = []
    for entry in (raw or "").split(","):
        field = entry.strip().lower()
        if not field:
            continue
        if not field.endswith("_id"):
            raise AssertionError(f"PK field must end with _id: {field}")
        class_id = field[: -len("_id")]
        if not class_id:
            raise AssertionError(f"PK field has empty class_id base: {field}")
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_:-]*$", class_id):
            raise AssertionError(f"Invalid class_id derived from PK field: {class_id}")
        output.append((field, class_id))
    if not output:
        raise AssertionError("No PK fields configured for agent progress smoke")
    return output


async def _wait_for_command_completed(
    session: aiohttp.ClientSession,
    *,
    command_id: str,
    timeout_seconds: int = 180,
    poll_interval_seconds: float = 2.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[Dict[str, Any]] = None
    while time.monotonic() < deadline:
        async with session.get(f"{BFF_URL}/api/v1/commands/{command_id}/status") as resp:
            if resp.status != 200:
                last = {"status": resp.status, "body": await resp.text()}
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()
        status_value = str(last.get("status") or "").upper()
        if status_value in {"COMPLETED", "FAILED", "CANCELLED"}:
            if status_value != "COMPLETED":
                raise AssertionError(f"Command {command_id} ended in {status_value}: {last}")
            return last
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for command completion (command_id={command_id}, last={last})")


def _extract_command_id(payload: Dict[str, Any]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("command_id", "commandId"):
            value = data.get(key)
            if value:
                return str(value)
    for key in ("command_id", "commandId"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


async def _create_database(session: aiohttp.ClientSession, *, db_name: str) -> None:
    async with session.post(
        f"{BFF_URL}/api/v1/databases",
        json={"name": db_name, "description": "agent progress smoke"},
    ) as resp:
        payload = await resp.json()
        if resp.status == 202:
            command_id = _extract_command_id(payload)
            if command_id:
                await _wait_for_command_completed(session, command_id=command_id)
            return
        if resp.status in {200, 201, 409}:
            return
        raise AssertionError(f"Database create failed (status={resp.status}, body={payload})")


async def _create_ontology(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    class_id: str,
    pk_field: str,
) -> None:
    payload = {
        "id": class_id,
        "label": class_id,
        "description": "agent progress smoke class",
        "properties": [
            {
                "name": pk_field,
                "type": "xsd:string",
                "label": pk_field,
                "required": True,
                "primaryKey": True,
                "titleKey": True,
            },
            {
                "name": "name",
                "type": "xsd:string",
                "label": "name",
                "required": False,
            },
        ],
        "relationships": [],
    }
    async with session.post(
        f"{BFF_URL}/api/v1/databases/{db_name}/ontology",
        params={"branch": "main"},
        json=payload,
    ) as resp:
        body = await resp.json()
        if resp.status == 202:
            command_id = _extract_command_id(body)
            if command_id:
                await _wait_for_command_completed(session, command_id=command_id)
            return
        if resp.status in {200, 201, 409}:
            return
        raise AssertionError(f"Ontology create failed (status={resp.status}, body={body})")


async def _start_agent_bulk_create(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    class_id: str,
    pk_field: str,
    count: int,
) -> str:
    instances = [
        {pk_field: f"item_{idx:04d}", "name": f"Item {idx}"}
        for idx in range(1, count + 1)
    ]
    payload = {
        "goal": "agent progress smoke",
        "steps": [
            {
                "service": "bff",
                "method": "POST",
                "path": f"/api/v1/databases/{db_name}/instances/{class_id}/bulk-create",
                "body": {"instances": instances, "metadata": {"source": "agent_progress_smoke"}},
            }
        ],
        "context": {"risk_level": "write"},
    }
    async with session.post(f"{BFF_URL}/api/v1/agent/runs", json=payload) as resp:
        body = await resp.json()
        if resp.status not in {200, 202}:
            raise AssertionError(f"Agent run start failed (status={resp.status}, body={body})")
        run_id = ((body.get("data") or {}).get("run_id") or "").strip()
        if not run_id:
            raise AssertionError(f"Missing run_id in response: {body}")
        return run_id


async def _wait_for_run_completion(
    session: aiohttp.ClientSession,
    *,
    run_id: str,
    timeout_seconds: int = 180,
    poll_interval_seconds: float = 1.0,
) -> Tuple[bool, Optional[str]]:
    deadline = time.monotonic() + timeout_seconds
    progress_seen = False
    command_id: Optional[str] = None
    last_payload: Optional[Dict[str, Any]] = None

    while time.monotonic() < deadline:
        async with session.get(f"{BFF_URL}/api/v1/agent/runs/{run_id}") as resp:
            if resp.status != 200:
                await asyncio.sleep(poll_interval_seconds)
                continue
            last_payload = await resp.json()
        data = last_payload.get("data") or {}
        status = str(data.get("status") or "").lower()
        progress = data.get("progress") or {}
        if progress.get("progress"):
            progress_seen = True
            if progress.get("command_id"):
                command_id = str(progress.get("command_id"))
        if status in {"completed", "failed"}:
            if status != "completed":
                raise AssertionError(f"Agent run failed: {last_payload}")
            return progress_seen, command_id
        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(f"Timed out waiting for run completion (run_id={run_id}, last={last_payload})")


@pytest.mark.integration
@pytest.mark.requires_infra
@pytest.mark.asyncio
async def test_agent_progress_smoke() -> None:
    pk_fields = _normalize_pk_fields(PK_FIELDS_RAW)
    db_name = f"agent_progress_{uuid.uuid4().hex[:8]}"
    headers = bff_auth_headers()

    async with aiohttp.ClientSession(headers=headers) as session:
        await _create_database(session, db_name=db_name)

        for pk_field, class_id in pk_fields:
            await _create_ontology(
                session,
                db_name=db_name,
                class_id=class_id,
                pk_field=pk_field,
            )
            run_id = await _start_agent_bulk_create(
                session,
                db_name=db_name,
                class_id=class_id,
                pk_field=pk_field,
                count=INSTANCE_COUNT,
            )
            progress_seen, command_id = await _wait_for_run_completion(session, run_id=run_id)
            assert progress_seen, f"No progress updates observed for run {run_id}"
            if command_id:
                await _wait_for_command_completed(session, command_id=command_id)
