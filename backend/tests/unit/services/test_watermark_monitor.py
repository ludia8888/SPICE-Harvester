from __future__ import annotations

import os

import pytest
import redis.asyncio as aioredis

from shared.config.service_config import ServiceConfig
from shared.services.watermark_monitor import GlobalWatermark, PartitionWatermark, WatermarkMonitor


@pytest.mark.asyncio
async def test_watermark_monitor_metrics_and_alerts() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url)

    monitor = WatermarkMonitor(
        kafka_config={"bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers()},
        redis_client=redis_client,
        consumer_groups=["group"],
        topics=["topic"],
        alert_threshold_ms=1,
    )

    try:
        monitor.partition_watermarks = {
            "topic": {
                0: PartitionWatermark(
                    topic="topic",
                    partition=0,
                    low_watermark=0,
                    high_watermark=100,
                    committed_offset=50,
                    lag=50,
                ),
                1: PartitionWatermark(
                    topic="topic",
                    partition=1,
                    low_watermark=0,
                    high_watermark=200,
                    committed_offset=100,
                    lag=100,
                ),
            }
        }

        monitor.global_watermark = monitor.calculate_global_watermark()
        assert monitor.global_watermark.total_lag == 150
        assert monitor.global_watermark.partition_count == 2

        await monitor.store_metrics()
        await monitor.check_alerts()
        await monitor.export_prometheus_metrics()

        stored = await redis_client.hgetall("watermark:global:current")
        assert stored
        alert = await redis_client.lrange("alerts:watermark:high_lag", 0, 0)
        assert alert
        prom = await redis_client.get("metrics:prometheus:watermark")
        assert prom
    finally:
        await redis_client.delete("watermark:global:current")
        await redis_client.delete("alerts:watermark:high_lag")
        await redis_client.delete("metrics:prometheus:watermark")
        await redis_client.aclose()


def test_partition_and_global_watermark_helpers() -> None:
    part = PartitionWatermark(
        topic="topic",
        partition=0,
        low_watermark=0,
        high_watermark=10,
        committed_offset=5,
        lag=5,
    )
    assert part.progress_percentage == 50.0

    global_wm = GlobalWatermark(
        total_lag=0,
        max_lag=0,
        min_lag=0,
        avg_lag=0,
        total_messages=0,
        processed_messages=0,
        progress_percentage=100.0,
        partition_count=0,
    )
    assert global_wm.is_healthy is True
    assert global_wm.estimated_catch_up_time_ms == 0
