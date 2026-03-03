from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import pytest

from shared.security.startup_guard import ensure_startup_security


@contextmanager
def _set_env(**updates: str | None) -> Iterator[None]:
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


@pytest.mark.unit
def test_startup_guard_blocks_weak_admin_token_in_production() -> None:
    with _set_env(
        ENVIRONMENT="production",
        ADMIN_TOKEN="change_me",
        BFF_ADMIN_TOKEN=None,
        BFF_WRITE_TOKEN=None,
        BFF_AGENT_TOKEN=None,
        OMS_ADMIN_TOKEN=None,
        OMS_WRITE_TOKEN=None,
        USER_JWT_HS256_SECRET="strong-user-jwt-secret-value",
        AGENT_BFF_TOKEN=None,
    ):
        with pytest.raises(RuntimeError):
            ensure_startup_security("bff")


@pytest.mark.unit
def test_startup_guard_allows_weak_tokens_in_development() -> None:
    with _set_env(
        ENVIRONMENT="development",
        ADMIN_TOKEN="change_me",
    ):
        ensure_startup_security("bff")
