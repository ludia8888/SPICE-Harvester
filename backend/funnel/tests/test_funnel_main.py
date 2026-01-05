from __future__ import annotations

import pytest

from funnel import main as funnel_main


class _FakeRateLimiter:
    def __init__(self) -> None:
        self.initialized = False
        self.closed = False

    async def initialize(self) -> None:
        self.initialized = True

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_funnel_root_and_health() -> None:
    root = await funnel_main.root()
    assert root["service"] == "funnel"
    assert root["status"] == "running"

    health = await funnel_main.health_check()
    assert health["status"] == "success"
    assert health["data"]["status"] == "healthy"


@pytest.mark.asyncio
async def test_funnel_lifespan_initializes_rate_limiter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(funnel_main, "RateLimiter", _FakeRateLimiter)

    async with funnel_main.lifespan(funnel_main.app):
        limiter = funnel_main.app.state.rate_limiter
        assert isinstance(limiter, _FakeRateLimiter)
        assert limiter.initialized is True

    assert limiter.closed is True
