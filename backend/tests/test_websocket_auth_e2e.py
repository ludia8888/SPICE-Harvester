"""
WebSocket auth E2E tests (no mocks).
"""

from __future__ import annotations

import json
import os
import uuid

import aiohttp
import pytest

from tests.utils.auth import bff_auth_headers


BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")


def _ws_url(path: str) -> str:
    if BFF_URL.startswith("https://"):
        base = "wss://" + BFF_URL[len("https://"):]
    elif BFF_URL.startswith("http://"):
        base = "ws://" + BFF_URL[len("http://"):]
    else:
        base = f"ws://{BFF_URL}"
    return f"{base}{path}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_requires_token():
    command_id = str(uuid.uuid4())
    url = _ws_url(f"/api/v1/ws/commands/{command_id}")

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        try:
            ws = await session.ws_connect(url)
        except aiohttp.WSServerHandshakeError:
            return

        msg = await ws.receive(timeout=5)
        if msg.type == aiohttp.WSMsgType.TEXT:
            payload = json.loads(msg.data)
            assert payload.get("type") != "connection_established"
        else:
            assert msg.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}
        await ws.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_allows_token():
    command_id = str(uuid.uuid4())
    token = bff_auth_headers()["X-Admin-Token"]
    url = _ws_url(f"/api/v1/ws/commands/{command_id}?token={token}")

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        ws = await session.ws_connect(url, headers={"X-Admin-Token": token})
        msg = await ws.receive(timeout=5)
        assert msg.type == aiohttp.WSMsgType.TEXT
        payload = json.loads(msg.data)
        assert payload.get("type") == "connection_established"
        await ws.close()
