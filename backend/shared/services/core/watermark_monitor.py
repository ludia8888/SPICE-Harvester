"""
Global Watermark Monitoring Service
THINK ULTRA³ - Real-time lag tracking and performance monitoring

This service tracks the global watermark across all partitions,
providing visibility into system lag and processing performance.
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import logging
import json
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


@dataclass
class PartitionWatermark:
    """Watermark information for a single partition"""
    topic: str
    partition: int
    low_watermark: int  # Earliest available offset
    high_watermark: int  # Latest offset (end of partition)
    committed_offset: int  # Last committed offset by consumer group
    lag: int  # high_watermark - committed_offset
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress as percentage"""
        if self.high_watermark <= self.low_watermark:
            return 100.0
        total = self.high_watermark - self.low_watermark
        processed = self.committed_offset - self.low_watermark
        return (processed / total) * 100 if total > 0 else 100.0


@dataclass
class GlobalWatermark:
    """Aggregated watermark across all partitions"""
    total_lag: int
    max_lag: int
    min_lag: int
    avg_lag: float
    total_messages: int
    processed_messages: int
    progress_percentage: float
    partition_count: int
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    
    @property
    def is_healthy(self) -> bool:
        """Check if lag is within acceptable limits"""
        # Healthy if average lag < 1000 messages and max lag < 5000
        return self.avg_lag < 1000 and self.max_lag < 5000
    
    @property
    def estimated_catch_up_time_ms(self) -> int:
        """Estimate time to catch up based on processing rate"""
        # Assume 1000 messages/second processing rate
        # This should be calculated from actual metrics
        if self.total_lag == 0:
            return 0
        processing_rate = 1000  # messages per second
        return int((self.total_lag / processing_rate) * 1000)


class WatermarkMonitor:
    """
    Monitor Kafka consumer lag and watermarks across all partitions
    
    Features:
    - Per-partition watermark tracking
    - Global watermark aggregation
    - Lag alerting and monitoring
    - Prometheus metrics export
    - Historical lag tracking
    """
    
    def __init__(
        self,
        kafka_config: Dict[str, Any],
        redis_client: aioredis.Redis,
        consumer_groups: List[str],
        topics: List[str],
        alert_threshold_ms: int = 5000
    ):
        """
        Initialize watermark monitor
        
        Args:
            kafka_config: Kafka configuration
            redis_client: Redis client for storing metrics
            consumer_groups: List of consumer groups to monitor
            topics: List of topics to monitor
            alert_threshold_ms: Alert if lag exceeds this threshold
        """
        self.kafka_config = kafka_config
        self.redis_client = redis_client
        self.consumer_groups = consumer_groups
        self.topics = topics
        self.alert_threshold_ms = alert_threshold_ms
        
        # Admin client for metadata
        self.admin_client = AdminClient({'bootstrap.servers': kafka_config['bootstrap.servers']})
        
        # Consumers for each group (for reading committed offsets)
        self.group_consumers: Dict[str, Consumer] = {}
        
        # Cached watermarks
        self.partition_watermarks: Dict[str, Dict[int, PartitionWatermark]] = {}
        self.global_watermark: Optional[GlobalWatermark] = None
        
        # Monitoring state
        self.monitoring = False
        self.monitor_task: Optional[asyncio.Task] = None
        
    async def start_monitoring(self, interval_seconds: int = 10):
        """
        Start monitoring watermarks
        
        Args:
            interval_seconds: Monitoring interval
        """
        if self.monitoring:
            logger.warning("Monitoring already started")
            return
        
        self.monitoring = True
        self.monitor_task = asyncio.create_task(
            self._monitor_loop(interval_seconds)
        )
        logger.info(f"Started watermark monitoring with {interval_seconds}s interval")
    
    async def stop_monitoring(self):
        """Stop monitoring watermarks"""
        self.monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        
        # Close consumers
        for consumer in self.group_consumers.values():
            consumer.close()
        self.group_consumers.clear()
        
        logger.info("Stopped watermark monitoring")
    
    async def _monitor_loop(self, interval_seconds: int):
        """Main monitoring loop"""
        while self.monitoring:
            try:
                # Update watermarks for all consumer groups
                for group in self.consumer_groups:
                    await self.update_watermarks(group)
                
                # Calculate global watermark
                self.global_watermark = self.calculate_global_watermark()
                
                # Store metrics in Redis
                await self.store_metrics()
                
                # Check for alerts
                await self.check_alerts()
                
                # Export Prometheus metrics
                await self.export_prometheus_metrics()
                
                # Wait for next iteration
                await asyncio.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(interval_seconds)
    
    async def update_watermarks(self, consumer_group: str):
        """
        Update watermarks for a consumer group
        
        Args:
            consumer_group: Consumer group to monitor
        """
        # Get or create consumer for this group
        if consumer_group not in self.group_consumers:
            config = self.kafka_config.copy()
            config['group.id'] = consumer_group
            config['enable.auto.commit'] = False
            self.group_consumers[consumer_group] = Consumer(config)
        
        consumer = self.group_consumers[consumer_group]
        
        for topic in self.topics:
            # Get topic metadata
            metadata = consumer.list_topics(topic, timeout=10)
            if topic not in metadata.topics:
                logger.warning(f"Topic {topic} not found")
                continue
            
            topic_metadata = metadata.topics[topic]
            
            # Initialize topic watermarks if needed
            if topic not in self.partition_watermarks:
                self.partition_watermarks[topic] = {}
            
            # Update each partition
            for partition_id in topic_metadata.partitions:
                tp = TopicPartition(topic, partition_id)
                
                # Get watermarks (low and high offsets)
                low, high = consumer.get_watermark_offsets(tp, timeout=5)
                
                # Get committed offset for this consumer group
                committed = consumer.committed([tp], timeout=5)[0]
                committed_offset = committed.offset if committed and committed.offset >= 0 else low
                
                # Calculate lag
                lag = high - committed_offset if high > committed_offset else 0
                
                # Create watermark entry
                self.partition_watermarks[topic][partition_id] = PartitionWatermark(
                    topic=topic,
                    partition=partition_id,
                    low_watermark=low,
                    high_watermark=high,
                    committed_offset=committed_offset,
                    lag=lag
                )
    
    def calculate_global_watermark(self) -> GlobalWatermark:
        """
        Calculate global watermark across all partitions
        
        Returns:
            Global watermark statistics
        """
        all_watermarks = []
        for topic_watermarks in self.partition_watermarks.values():
            all_watermarks.extend(topic_watermarks.values())
        
        if not all_watermarks:
            return GlobalWatermark(
                total_lag=0,
                max_lag=0,
                min_lag=0,
                avg_lag=0,
                total_messages=0,
                processed_messages=0,
                progress_percentage=100.0,
                partition_count=0
            )
        
        lags = [w.lag for w in all_watermarks]
        total_messages = sum(w.high_watermark - w.low_watermark for w in all_watermarks)
        processed_messages = sum(w.committed_offset - w.low_watermark for w in all_watermarks)
        
        return GlobalWatermark(
            total_lag=sum(lags),
            max_lag=max(lags),
            min_lag=min(lags),
            avg_lag=sum(lags) / len(lags) if lags else 0,
            total_messages=total_messages,
            processed_messages=processed_messages,
            progress_percentage=(processed_messages / total_messages * 100) if total_messages > 0 else 100.0,
            partition_count=len(all_watermarks)
        )
    
    async def store_metrics(self):
        """Store metrics in Redis for historical tracking"""
        if not self.global_watermark:
            return
        
        try:
            # Store current global watermark
            await self.redis_client.hset(
                "watermark:global:current",
                mapping={
                    "total_lag": self.global_watermark.total_lag,
                    "max_lag": self.global_watermark.max_lag,
                    "avg_lag": self.global_watermark.avg_lag,
                    "progress": self.global_watermark.progress_percentage,
                    "timestamp": self.global_watermark.timestamp_ms
                }
            )
            
            # Store historical data (keep last 1 hour with 10s granularity)
            history_key = f"watermark:history:{int(time.time() // 10) * 10}"
            await self.redis_client.setex(
                history_key,
                3600,  # 1 hour TTL
                str(self.global_watermark.total_lag)
            )
            
            # Store per-partition watermarks
            for topic, partitions in self.partition_watermarks.items():
                for partition_id, watermark in partitions.items():
                    key = f"watermark:partition:{topic}:{partition_id}"
                    await self.redis_client.hset(
                        key,
                        mapping={
                            "lag": watermark.lag,
                            "progress": watermark.progress_percentage,
                            "committed": watermark.committed_offset,
                            "high": watermark.high_watermark
                        }
                    )
                    await self.redis_client.expire(key, 300)  # 5 minute TTL
                    
        except Exception as e:
            logger.error(f"Failed to store metrics: {e}")
    
    async def check_alerts(self):
        """Check for lag alerts and trigger notifications"""
        if not self.global_watermark:
            return
        
        # Check if lag exceeds threshold
        if self.global_watermark.estimated_catch_up_time_ms > self.alert_threshold_ms:
            alert_data = {
                "type": "HIGH_LAG",
                "total_lag": self.global_watermark.total_lag,
                "max_lag": self.global_watermark.max_lag,
                "estimated_catch_up_ms": self.global_watermark.estimated_catch_up_time_ms,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Store alert in Redis
            await self.redis_client.lpush(
                "alerts:watermark:high_lag",
                json.dumps(alert_data)
            )
            await self.redis_client.ltrim("alerts:watermark:high_lag", 0, 99)  # Keep last 100
            
            logger.warning(f"HIGH LAG ALERT: {alert_data}")
            
            # TODO: Send to alerting system (Slack, PagerDuty, etc.)
    
    async def export_prometheus_metrics(self):
        """Export metrics in Prometheus format"""
        if not self.global_watermark:
            return
        
        try:
            # These would typically be exported to a Prometheus pushgateway
            # or exposed via an HTTP endpoint
            metrics = [
                f'kafka_consumer_lag_total{{{{"service":"spice-harvester"}}}} {self.global_watermark.total_lag}',
                f'kafka_consumer_lag_max{{{{"service":"spice-harvester"}}}} {self.global_watermark.max_lag}',
                f'kafka_consumer_lag_avg{{{{"service":"spice-harvester"}}}} {self.global_watermark.avg_lag}',
                f'kafka_consumer_progress_percentage{{{{"service":"spice-harvester"}}}} {self.global_watermark.progress_percentage}',
                f'kafka_consumer_estimated_catchup_ms{{{{"service":"spice-harvester"}}}} {self.global_watermark.estimated_catch_up_time_ms}',
            ]
            
            # Store in Redis for Prometheus scraping
            await self.redis_client.set(
                "metrics:prometheus:watermark",
                "\n".join(metrics),
                ex=60  # 1 minute TTL
            )
            
        except Exception as e:
            logger.error(f"Failed to export Prometheus metrics: {e}")
    
    async def get_current_lag(self) -> Dict[str, Any]:
        """
        Get current lag information
        
        Returns:
            Current lag statistics
        """
        if not self.global_watermark:
            return {
                "status": "no_data",
                "message": "No watermark data available"
            }
        
        return {
            "status": "healthy" if self.global_watermark.is_healthy else "unhealthy",
            "total_lag": self.global_watermark.total_lag,
            "max_lag": self.global_watermark.max_lag,
            "avg_lag": self.global_watermark.avg_lag,
            "progress_percentage": self.global_watermark.progress_percentage,
            "estimated_catch_up_ms": self.global_watermark.estimated_catch_up_time_ms,
            "partition_count": self.global_watermark.partition_count,
            "timestamp": self.global_watermark.timestamp_ms
        }
    
    async def get_partition_details(self, topic: str) -> List[Dict[str, Any]]:
        """
        Get detailed lag information for a specific topic
        
        Args:
            topic: Topic name
            
        Returns:
            List of partition details
        """
        if topic not in self.partition_watermarks:
            return []
        
        return [
            {
                "partition": p.partition,
                "lag": p.lag,
                "progress": p.progress_percentage,
                "committed_offset": p.committed_offset,
                "high_watermark": p.high_watermark,
                "low_watermark": p.low_watermark
            }
            for p in self.partition_watermarks[topic].values()
        ]


# Convenience function to create and start monitor
async def create_watermark_monitor(
    kafka_config: Dict[str, Any],
    redis_url: str = "redis://localhost:6379",
    consumer_groups: Optional[List[str]] = None,
    topics: Optional[List[str]] = None
) -> WatermarkMonitor:
    """
    Create and start a watermark monitor
    
    Args:
        kafka_config: Kafka configuration
        redis_url: Redis connection URL
        consumer_groups: Consumer groups to monitor
        topics: Topics to monitor
        
    Returns:
        Running WatermarkMonitor instance
    """
    from shared.config.app_config import AppConfig
    
    # Default consumer groups and topics
    if consumer_groups is None:
        consumer_groups = [
            'instance-worker-group',
            'projection-worker-group',
            'ontology-worker-group'
        ]
    
    if topics is None:
        topics = [
            AppConfig.INSTANCE_EVENTS_TOPIC,
            AppConfig.ONTOLOGY_EVENTS_TOPIC,
            AppConfig.INSTANCE_COMMANDS_TOPIC,
            AppConfig.ONTOLOGY_COMMANDS_TOPIC
        ]
    
    # Create Redis client
    redis_client = aioredis.from_url(redis_url)
    
    # Create monitor
    monitor = WatermarkMonitor(
        kafka_config=kafka_config,
        redis_client=redis_client,
        consumer_groups=consumer_groups,
        topics=topics,
        alert_threshold_ms=5000
    )
    
    # Start monitoring
    await monitor.start_monitoring(interval_seconds=10)
    
    return monitor
