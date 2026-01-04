import pytest

from shared.services.health_check import HealthStatus, RedisHealthCheck


class RedisServiceWithInfo:
    async def ping(self):
        return None

    async def info(self):
        return {"used_memory_human": "12M", "connected_clients": 3}


class RedisServiceInfoError:
    async def ping(self):
        return None

    async def info(self):
        raise RuntimeError("boom")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_health_check_includes_info_details():
    checker = RedisHealthCheck(RedisServiceWithInfo())

    result = await checker.health_check()

    assert result.status == HealthStatus.HEALTHY
    assert result.details["memory_usage"] == "12M"
    assert result.details["connected_clients"] == 3
    assert "response_time_category" in result.details


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_health_check_ignores_info_errors():
    checker = RedisHealthCheck(RedisServiceInfoError())

    result = await checker.health_check()

    assert result.status == HealthStatus.HEALTHY
    assert "memory_usage" not in result.details
    assert "connected_clients" not in result.details
