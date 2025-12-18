"""
Auth hardening E2E tests (no mocks).

Validates:
- Auth disable requires explicit allow flags.
- OMS write endpoints reject missing auth.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Dict

import aiohttp
import pytest

from tests.utils.auth import oms_auth_headers


OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")


def _strip_auth_env(env: Dict[str, str]) -> Dict[str, str]:
    for key in (
        "BFF_ADMIN_TOKEN",
        "BFF_WRITE_TOKEN",
        "OMS_ADMIN_TOKEN",
        "OMS_CLIENT_TOKEN",
        "ADMIN_TOKEN",
        "ADMIN_API_KEY",
        "SMOKE_ADMIN_TOKEN",
        "ALLOW_INSECURE_BFF_AUTH_DISABLE",
        "ALLOW_INSECURE_OMS_AUTH_DISABLE",
        "ALLOW_INSECURE_AUTH_DISABLE",
    ):
        env.pop(key, None)
    return env


def _run_auth_check(module: str, func: str, env: Dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", f"from {module} import {func}; {func}()"],
        env=env,
        capture_output=True,
        text=True,
    )


def test_auth_disabled_requires_explicit_allow():
    env = _strip_auth_env(os.environ.copy())
    env["BFF_REQUIRE_AUTH"] = "false"
    result = _run_auth_check("bff.middleware.auth", "ensure_bff_auth_configured", env)
    assert result.returncode != 0

    env = _strip_auth_env(os.environ.copy())
    env["OMS_REQUIRE_AUTH"] = "false"
    result = _run_auth_check("oms.middleware.auth", "ensure_oms_auth_configured", env)
    assert result.returncode != 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_oms_write_requires_auth():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        async with session.post(
            f"{OMS_URL}/api/v1/database/create",
            json={},
        ) as resp:
            assert resp.status == 401

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        headers=oms_auth_headers(),
    ) as session:
        async with session.post(
            f"{OMS_URL}/api/v1/database/create",
            json={},
        ) as resp:
            assert resp.status in {400, 422}
