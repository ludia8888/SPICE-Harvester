"""
Command status TTL E2E tests (no mocks).
"""

from __future__ import annotations

import asyncio
import os
from contextlib import contextmanager
from typing import Tuple
from uuid import uuid4
from urllib.parse import urlparse

import pytest

from shared.config.app_config import AppConfig
from shared.services.command_status_service import CommandStatusService, CommandStatus
from shared.services.redis_service import RedisService


@contextmanager
def _set_env(**updates):
    original = {key: os.environ.get(key) for key in updates}
    for key, value in updates.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _redis_params() -> Tuple[str, int, str, int]:
    redis_url = (os.getenv("REDIS_URL") or "").strip()
    if redis_url:
        parsed = urlparse(redis_url)
        host = parsed.hostname or "localhost"
        port = parsed.port or 6379
        password = parsed.password or ""
        db = int((parsed.path or "/0").lstrip("/") or 0)
        return host, port, password, db

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.getenv("REDIS_PASSWORD", "")
    db = int(os.getenv("REDIS_DB", "0"))
    return host, port, password, db


@pytest.mark.integration
@pytest.mark.asyncio
async def test_command_status_ttl_configurable():
    host, port, password, db = _redis_params()
    redis_service = RedisService(host=host, port=port, password=password or None, db=db)
    await redis_service.connect()
    try:
        with _set_env(COMMAND_STATUS_TTL_SECONDS="1"):
            service = CommandStatusService(redis_service)
            command_id = str(uuid4())
            await service.create_command_status(
                command_id=command_id,
                command_type="TTL_TEST",
                aggregate_id="agg",
                payload={},
                user_id=None,
            )
            key = AppConfig.get_command_status_key(command_id)
            ttl = await redis_service.client.ttl(key)
            assert ttl > 0
            await asyncio.sleep(2)
            assert await redis_service.get_command_status(command_id) is None

        with _set_env(COMMAND_STATUS_TTL_SECONDS="0"):
            service = CommandStatusService(redis_service)
            command_id = str(uuid4())
            await service.create_command_status(
                command_id=command_id,
                command_type="TTL_TEST",
                aggregate_id="agg",
                payload={},
                user_id=None,
            )
            key = AppConfig.get_command_status_key(command_id)
            ttl = await redis_service.client.ttl(key)
            assert ttl == -1
            await redis_service.client.delete(key)
    finally:
        await redis_service.disconnect()
