from __future__ import annotations

import asyncio
import time
from typing import Optional

import httpx


def normalize_base_url(base_url: str) -> str:
    base = str(base_url or "").rstrip("/")
    if base.endswith("/api/v1"):
        return base
    return f"{base}/api/v1"


def extract_command_id(payload: object) -> Optional[str]:
    """
    Extract a command id from common BFF response shapes.

    Supports:
    - {"command_id": "..."} / {"commandId": "..."}
    - {"data": {"command_id": "..."}} / {"data": {"commandId": "..."}}
    - {"data": {"write": {"commands": [{"command_id": "..."}]}}}
    """

    if not isinstance(payload, dict):
        return None

    direct = payload.get("command_id") or payload.get("commandId")
    if isinstance(direct, str) and direct:
        return direct

    data = payload.get("data")
    if not isinstance(data, dict):
        return None

    direct = data.get("command_id") or data.get("commandId")
    if isinstance(direct, str) and direct:
        return direct

    write = data.get("write")
    if isinstance(write, dict):
        commands = write.get("commands")
        if isinstance(commands, list) and commands:
            first = commands[0]
            if isinstance(first, dict):
                direct = first.get("command_id") or first.get("commandId")
                if isinstance(direct, str) and direct:
                    return direct

    return None


async def wait_for_command(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    command_id: str,
    timeout_seconds: int = 180,
    poll_interval_seconds: float = 0.5,
) -> dict:
    deadline = time.monotonic() + int(timeout_seconds)
    last: dict = {}
    while time.monotonic() < deadline:
        resp = await client.get(f"{base_url}/commands/{command_id}/status")
        if resp.status_code == 404:
            await asyncio.sleep(poll_interval_seconds)
            continue
        resp.raise_for_status()
        last = resp.json()
        status = last.get("status") or last.get("state")
        if status in {"COMPLETED", "FAILED", "CANCELLED"}:
            return last
        await asyncio.sleep(poll_interval_seconds)
    raise TimeoutError(last)


async def list_databases(client: httpx.AsyncClient, *, base_url: str) -> list[str]:
    resp = await client.get(f"{base_url}/databases")
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict) and isinstance(data.get("databases"), list):
        items = data.get("databases") or []
    elif isinstance(payload, dict) and isinstance(payload.get("databases"), list):
        items = payload.get("databases") or []
    else:
        items = []

    names: list[str] = []
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("name"), str):
            names.append(item["name"])
        elif isinstance(item, str):
            names.append(item)
    return names


async def delete_database(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    db_name: str,
    expected_seq: Optional[int] = None,
    allow_missing: bool = False,
    timeout_seconds: int = 180,
) -> dict:
    params = None
    if expected_seq is not None:
        params = {"expected_seq": int(expected_seq)}

    resp = await client.delete(f"{base_url}/databases/{db_name}", params=params)
    if resp.status_code == 404 and allow_missing:
        return {"status": "missing"}
    if resp.status_code == 409:
        raise RuntimeError(f"OCC conflict deleting {db_name} (expected_seq={expected_seq})")
    resp.raise_for_status()

    payload = resp.json() if resp.content else {}
    cmd = extract_command_id(payload)
    if cmd:
        return await wait_for_command(client, base_url=base_url, command_id=cmd, timeout_seconds=timeout_seconds)
    return payload if isinstance(payload, dict) else {"raw": payload}
