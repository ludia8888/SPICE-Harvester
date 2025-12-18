"""
Critical fixes E2E validation (no mocks).

These tests exercise live services via HTTP and real infra
to verify fixes for config monitoring, i18n, rate limiting,
OpenAPI exposure, and Redis-down fallback behavior.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import aiohttp
import pytest


BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")


def _docker(*args: str) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise AssertionError(f"docker {' '.join(args)} failed: {detail}")
    return (result.stdout or "").strip()


def _resolve_redis_container() -> str:
    names = set(_docker("ps", "--format", "{{.Names}}").splitlines())
    explicit = (os.getenv("REDIS_CONTAINER") or "").strip()
    if explicit:
        if explicit not in names:
            raise AssertionError(f"REDIS_CONTAINER={explicit} is not running")
        return explicit
    for candidate in ("spice_redis", "spice-foundry-redis"):
        if candidate in names:
            return candidate
    raise AssertionError("Redis container not found. Set REDIS_CONTAINER to the running Redis container name.")


REDIS_CONTAINER = _resolve_redis_container()


async def _wait_for_ok(session: aiohttp.ClientSession, url: str, timeout_seconds: int = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[int] = None
    while time.monotonic() < deadline:
        try:
            async with session.get(url) as resp:
                last = resp.status
                if resp.status == 200:
                    return
        except Exception:
            last = None
        await asyncio.sleep(1)
    raise AssertionError(f"Timed out waiting for {url} (last_status={last})")


async def _wait_for_command_status_ok(
    session: aiohttp.ClientSession,
    command_id: str,
    timeout_seconds: int = 60,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_status: Optional[int] = None
    last_body: Optional[str] = None
    while time.monotonic() < deadline:
        async with session.get(f"{BFF_URL}/api/v1/commands/{command_id}/status") as resp:
            last_status = resp.status
            last_body = await resp.text()
            if resp.status == 200:
                return await resp.json()
        await asyncio.sleep(1)
    raise AssertionError(
        f"Timed out waiting for command status 200 (status={last_status}, body={last_body})"
    )


@asynccontextmanager
async def _redis_down():
    _docker("stop", REDIS_CONTAINER)
    try:
        await asyncio.sleep(1.0)
        yield
    finally:
        _docker("start", REDIS_CONTAINER)
        await asyncio.sleep(2.0)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_config_monitor_current_returns_payload():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        await _wait_for_ok(session, f"{BFF_URL}/api/v1/health")
        async with session.get(f"{BFF_URL}/api/v1/config/config/current") as resp:
            assert resp.status == 200
            payload = await resp.json()
            assert "configuration" in payload
            assert "config_hash" in payload
            assert "environment" in payload


@pytest.mark.integration
@pytest.mark.asyncio
async def test_openapi_excludes_wip_projections():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        await _wait_for_ok(session, f"{BFF_URL}/api/v1/health")
        async with session.get(f"{BFF_URL}/openapi.json") as resp:
            assert resp.status == 200
            spec = await resp.json()

        paths = spec.get("paths") or {}
        assert all(not path.startswith("/api/v1/projections") for path in paths), paths.keys()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_i18n_translates_health_description():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        await _wait_for_ok(session, f"{BFF_URL}/api/v1/health")
        async with session.get(
            f"{BFF_URL}/api/v1/health",
            headers={"Accept-Language": "en"},
        ) as resp:
            assert resp.status == 200
            payload = await resp.json()
            description = (payload.get("data") or {}).get("description")
            assert description == "Backend for Frontend service."


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rate_limit_headers_present_on_success():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        await _wait_for_ok(session, f"{BFF_URL}/api/v1/health")
        async with session.get(f"{BFF_URL}/api/v1/data-connectors/google-sheets/registered") as resp:
            assert resp.status == 200
            assert resp.headers.get("X-RateLimit-Limit")
            assert resp.headers.get("X-RateLimit-Remaining") is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_down_rate_limit_and_command_status_fallback():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
        await _wait_for_ok(session, f"{BFF_URL}/api/v1/health")

        db_name = f"redis_down_{uuid.uuid4().hex[:8]}"
        async with session.post(
            f"{BFF_URL}/api/v1/databases",
            json={"name": db_name, "description": "redis fallback test"},
        ) as resp:
            assert resp.status in {200, 202}
            payload = await resp.json()

        command_id = (
            (payload.get("data") or {}).get("command_id")
            or (payload.get("data") or {}).get("commandId")
            or payload.get("command_id")
        )
        assert command_id, f"Missing command_id in response: {payload}"

        async with _redis_down():
            async with session.get(f"{BFF_URL}/api/v1/data-connectors/google-sheets/registered") as resp:
                assert resp.status == 503
                assert resp.headers.get("X-RateLimit-Disabled") == "true"

            status_payload = await _wait_for_command_status_ok(session, command_id)
            assert str(status_payload.get("status") or "").upper() in {
                "PENDING",
                "PROCESSING",
                "COMPLETED",
                "FAILED",
                "CANCELLED",
                "RETRYING",
            }
