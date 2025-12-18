#!/usr/bin/env python3
"""
🔥 THINK ULTRA! S3/MinIO Event Store Monitoring Dashboard

Real-time monitoring of S3 Event Store metrics:
- Event write/read rates
- Storage usage and growth
- Performance metrics
- Error tracking
- Publisher checkpoint freshness
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from collections import defaultdict
import aioboto3
from prometheus_client import Counter, Histogram, Gauge, generate_latest
import logging
import os

from oms.services.event_store import event_store
from shared.config.service_config import ServiceConfig

logger = logging.getLogger(__name__)


# Prometheus Metrics
event_writes_total = Counter(
    'event_store_writes_total', 
    'Total number of events written to S3',
    ['aggregate_type', 'event_type']
)

event_reads_total = Counter(
    'event_store_reads_total',
    'Total number of events read from S3',
    ['aggregate_type', 'operation']
)

event_write_duration = Histogram(
    'event_store_write_duration_seconds',
    'Time taken to write event to S3',
    ['aggregate_type']
)

event_read_duration = Histogram(
    'event_store_read_duration_seconds',
    'Time taken to read event from S3',
    ['operation']
)

storage_bytes = Gauge(
    'event_store_storage_bytes',
    'Total storage used by Event Store in bytes',
    ['bucket']
)

events_per_aggregate = Gauge(
    'event_store_events_per_aggregate',
    'Number of events per aggregate type',
    ['aggregate_type']
)

error_rate = Counter(
    'event_store_errors_total',
    'Total number of Event Store errors',
    ['operation', 'error_type']
)

publisher_checkpoint_age_seconds = Gauge(
    "event_publisher_checkpoint_age_seconds",
    "Seconds since the EventPublisher checkpoint was last updated",
    ["bucket", "checkpoint_key"],
)

publisher_checkpoint_lag_seconds = Gauge(
    "event_publisher_checkpoint_lag_seconds",
    "Seconds between now and the EventPublisher checkpoint last_timestamp_ms (best-effort lag)",
    ["bucket", "checkpoint_key"],
)

publisher_checkpoint_last_timestamp_ms = Gauge(
    "event_publisher_checkpoint_last_timestamp_ms",
    "EventPublisher checkpoint last_timestamp_ms (epoch ms)",
    ["bucket", "checkpoint_key"],
)


class S3EventStoreDashboard:
    """S3/MinIO Event Store Monitoring Dashboard"""
    
    def __init__(self):
        self.session = aioboto3.Session()
        self.endpoint_url = ServiceConfig.get_minio_endpoint()
        self.bucket_name = os.getenv("EVENT_STORE_BUCKET", "spice-event-store")
        self.checkpoint_key = os.getenv("EVENT_PUBLISHER_CHECKPOINT_KEY", "checkpoints/event_publisher.json")
        self.metrics_cache = defaultdict(dict)
        self.last_update = datetime.now(timezone.utc)
        
    async def connect(self):
        """Initialize connection to S3/MinIO"""
        await event_store.connect()
        logger.info(f"Connected to S3 Event Store at {self.endpoint_url}")
    
    async def collect_storage_metrics(self) -> Dict[str, Any]:
        """Collect storage usage metrics"""
        async with self.session.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=ServiceConfig.get_minio_access_key(),
            aws_secret_access_key=ServiceConfig.get_minio_secret_key()
        ) as s3_client:
            
            # Get bucket statistics
            try:
                # List all objects to calculate storage
                total_size = 0
                object_count = 0
                aggregate_counts = defaultdict(int)
                
                paginator = s3_client.get_paginator('list_objects_v2')
                async for page in paginator.paginate(Bucket=self.bucket_name):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            total_size += obj['Size']
                            object_count += 1
                            
                            # Parse aggregate type from key
                            # Format: events/YYYY/MM/DD/AggregateType/AggregateId/EventId.json
                            parts = obj['Key'].split('/')
                            if len(parts) >= 5:
                                aggregate_type = parts[4]
                                aggregate_counts[aggregate_type] += 1
                
                # Update Prometheus metrics
                storage_bytes.labels(bucket=self.bucket_name).set(total_size)
                
                for agg_type, count in aggregate_counts.items():
                    events_per_aggregate.labels(aggregate_type=agg_type).set(count)
                
                return {
                    "total_size_bytes": total_size,
                    "total_size_mb": round(total_size / (1024 * 1024), 2),
                    "object_count": object_count,
                    "aggregate_distribution": dict(aggregate_counts),
                    "average_event_size": round(total_size / object_count, 2) if object_count > 0 else 0
                }
                
            except Exception as e:
                error_rate.labels(operation='storage_metrics', error_type=type(e).__name__).inc()
                logger.error(f"Error collecting storage metrics: {e}")
                return {}
    
    async def collect_performance_metrics(self) -> Dict[str, Any]:
        """Collect performance metrics from recent operations"""
        
        # These would normally come from actual operations
        # For dashboard, we're showing structure
        return {
            "write_operations": {
                "last_hour": self.metrics_cache.get("writes_last_hour", 0),
                "average_duration_ms": self.metrics_cache.get("avg_write_duration", 0),
                "success_rate": self.metrics_cache.get("write_success_rate", 100.0)
            },
            "read_operations": {
                "last_hour": self.metrics_cache.get("reads_last_hour", 0),
                "average_duration_ms": self.metrics_cache.get("avg_read_duration", 0),
                "cache_hit_rate": self.metrics_cache.get("cache_hit_rate", 0)
            },
            "replay_operations": {
                "total_replays": self.metrics_cache.get("total_replays", 0),
                "average_events_per_replay": self.metrics_cache.get("avg_events_per_replay", 0)
            }
        }
    
    async def collect_publisher_checkpoint_metrics(self) -> Dict[str, Any]:
        """Collect EventPublisher checkpoint metrics from S3/MinIO."""

        async with self.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=ServiceConfig.get_minio_access_key(),
            aws_secret_access_key=ServiceConfig.get_minio_secret_key(),
        ) as s3_client:
            try:
                obj = await s3_client.get_object(Bucket=self.bucket_name, Key=self.checkpoint_key)
                raw = await obj["Body"].read()
                checkpoint = json.loads(raw.decode("utf-8"))
            except Exception as e:
                error_rate.labels(operation="publisher_checkpoint", error_type=type(e).__name__).inc()
                publisher_checkpoint_age_seconds.labels(
                    bucket=self.bucket_name, checkpoint_key=self.checkpoint_key
                ).set(-1)
                publisher_checkpoint_lag_seconds.labels(
                    bucket=self.bucket_name, checkpoint_key=self.checkpoint_key
                ).set(-1)
                publisher_checkpoint_last_timestamp_ms.labels(
                    bucket=self.bucket_name, checkpoint_key=self.checkpoint_key
                ).set(0)
                return {"exists": False, "error": str(e)}

        now = datetime.now(timezone.utc)
        updated_at_iso = checkpoint.get("updated_at")
        last_ts_ms = checkpoint.get("last_timestamp_ms")

        age_s: Optional[float] = None
        if isinstance(updated_at_iso, str) and updated_at_iso:
            try:
                dt = datetime.fromisoformat(updated_at_iso.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                age_s = max(0.0, (now - dt.astimezone(timezone.utc)).total_seconds())
            except Exception:
                age_s = None

        lag_s: Optional[float] = None
        if isinstance(last_ts_ms, (int, float)) and float(last_ts_ms) > 0:
            lag_s = max(0.0, now.timestamp() - (float(last_ts_ms) / 1000.0))

        publisher_checkpoint_age_seconds.labels(bucket=self.bucket_name, checkpoint_key=self.checkpoint_key).set(
            age_s if age_s is not None else -1
        )
        publisher_checkpoint_lag_seconds.labels(bucket=self.bucket_name, checkpoint_key=self.checkpoint_key).set(
            lag_s if lag_s is not None else -1
        )
        publisher_checkpoint_last_timestamp_ms.labels(
            bucket=self.bucket_name, checkpoint_key=self.checkpoint_key
        ).set(float(last_ts_ms) if isinstance(last_ts_ms, (int, float)) else 0)

        return {
            "exists": True,
            "checkpoint_key": self.checkpoint_key,
            "updated_at": updated_at_iso,
            "age_seconds": age_s,
            "last_timestamp_ms": last_ts_ms,
            "lag_seconds": lag_s,
        }
    
    async def collect_health_metrics(self) -> Dict[str, Any]:
        """Collect health and availability metrics"""
        
        health_status = {
            "s3_connection": False,
            "bucket_accessible": False,
            "write_permission": False,
            "read_permission": False
        }
        
        async with self.session.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=ServiceConfig.get_minio_access_key(),
            aws_secret_access_key=ServiceConfig.get_minio_secret_key()
        ) as s3_client:
            
            try:
                # Test connection
                await s3_client.list_buckets()
                health_status["s3_connection"] = True
                
                # Test bucket access
                await s3_client.head_bucket(Bucket=self.bucket_name)
                health_status["bucket_accessible"] = True
                
                # Test write permission
                test_key = f"health_check/{datetime.now(timezone.utc).isoformat()}.json"
                await s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=test_key,
                    Body=json.dumps({"health": "check"})
                )
                health_status["write_permission"] = True
                
                # Test read permission and cleanup
                await s3_client.get_object(Bucket=self.bucket_name, Key=test_key)
                health_status["read_permission"] = True
                
                # Cleanup
                await s3_client.delete_object(Bucket=self.bucket_name, Key=test_key)
                
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                error_rate.labels(operation='health_check', error_type=type(e).__name__).inc()
        
        return health_status
    
    async def generate_dashboard(self) -> Dict[str, Any]:
        """Generate complete dashboard data"""
        
        dashboard = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_store": {
                "endpoint": self.endpoint_url,
                "bucket": self.bucket_name
            },
            "storage": await self.collect_storage_metrics(),
            "performance": await self.collect_performance_metrics(),
            "publisher": await self.collect_publisher_checkpoint_metrics(),
            "health": await self.collect_health_metrics()
        }
        
        # Calculate summary statistics
        storage = dashboard["storage"]
        if storage:
            dashboard["summary"] = {
                "total_events": storage.get("object_count", 0),
                "storage_used_mb": storage.get("total_size_mb", 0),
                "unique_aggregates": len(storage.get("aggregate_distribution", {})),
                "health_score": sum(dashboard["health"].values()) / len(dashboard["health"]) * 100
            }
        
        self.last_update = datetime.now(timezone.utc)
        return dashboard
    
    async def start_monitoring(self, interval_seconds: int = 60):
        """Start continuous monitoring"""
        
        await self.connect()
        
        while True:
            try:
                dashboard_data = await self.generate_dashboard()
                
                # Log key metrics
                logger.info(f"S3 Event Store Dashboard Update:")
                logger.info(f"  Total Events: {dashboard_data['summary']['total_events']}")
                logger.info(f"  Storage Used: {dashboard_data['summary']['storage_used_mb']} MB")
                if (dashboard_data.get("publisher") or {}).get("exists"):
                    logger.info(f"  Publisher Checkpoint Age: {dashboard_data['publisher'].get('age_seconds')}s")
                logger.info(f"  Health Score: {dashboard_data['summary']['health_score']}%")
                
                # Could send to external monitoring system here
                # e.g., send_to_grafana(dashboard_data)
                
                await asyncio.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Dashboard update failed: {e}")
                error_rate.labels(operation='dashboard_update', error_type=type(e).__name__).inc()
                await asyncio.sleep(interval_seconds)
    
    def get_prometheus_metrics(self) -> bytes:
        """Export metrics in Prometheus format"""
        return generate_latest()
    
    async def get_json_dashboard(self) -> str:
        """Get dashboard data as JSON"""
        dashboard = await self.generate_dashboard()
        return json.dumps(dashboard, indent=2, default=str)


# CLI for testing
async def main():
    """Run dashboard in CLI mode"""
    
    print("🔥 THINK ULTRA! S3 Event Store Monitoring Dashboard")
    print("=" * 60)
    
    dashboard = S3EventStoreDashboard()
    
    # Generate one-time dashboard
    data = await dashboard.generate_dashboard()
    
    print("\n📊 STORAGE METRICS:")
    storage = data.get("storage", {})
    print(f"  Total Events: {storage.get('object_count', 0)}")
    print(f"  Storage Used: {storage.get('total_size_mb', 0)} MB")
    print(f"  Average Event Size: {storage.get('average_event_size', 0)} bytes")
    
    print("\n🧭 PUBLISHER CHECKPOINT:")
    publisher = data.get("publisher", {})
    print(f"  Exists: {publisher.get('exists', False)}")
    print(f"  Key: {publisher.get('checkpoint_key', '')}")
    print(f"  Age Seconds: {publisher.get('age_seconds', None)}")
    print(f"  Lag Seconds: {publisher.get('lag_seconds', None)}")
    
    print("\n❤️ HEALTH STATUS:")
    health = data.get("health", {})
    for check, status in health.items():
        status_emoji = "✅" if status else "❌"
        print(f"  {status_emoji} {check}: {status}")
    
    print("\n📈 SUMMARY:")
    summary = data.get("summary", {})
    print(f"  Health Score: {summary.get('health_score', 0)}%")
    print(f"  Unique Aggregates: {summary.get('unique_aggregates', 0)}")
    
    print("\n✅ Dashboard generated successfully!")
    
    # Optionally start continuous monitoring
    # await dashboard.start_monitoring()


if __name__ == "__main__":
    asyncio.run(main())
