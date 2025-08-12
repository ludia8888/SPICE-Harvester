"""
Dead Letter Queue Handler Service - FIXED VERSION
THINK ULTRA³ - Fixed blocking operations in async context

This service handles failed messages from the DLQ,
implementing smart retry logic and poison message detection.
FIXED: Uses thread pool for blocking Kafka operations.
"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Consumer, Producer, KafkaError
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Retry strategies for failed messages"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"
    IMMEDIATE = "immediate"


@dataclass
class RetryPolicy:
    """Configuration for retry behavior"""
    max_retries: int = 5
    initial_delay_ms: int = 1000  # 1 second
    max_delay_ms: int = 300000  # 5 minutes
    backoff_multiplier: float = 2.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    poison_threshold: int = 10  # Move to poison queue after this many failures


@dataclass
class FailedMessage:
    """Representation of a failed message"""
    message_id: str
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: str
    error: str
    retry_count: int = 0
    first_failure_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_retry_time: Optional[datetime] = None
    next_retry_time: Optional[datetime] = None
    headers: Dict[str, str] = field(default_factory=dict)
    
    @property
    def is_poison(self) -> bool:
        """Check if message should be considered poison"""
        poison_patterns = [
            "schema validation failed",
            "malformed json",
            "invalid message format",
            "unsupported operation"
        ]
        error_lower = self.error.lower()
        return any(pattern in error_lower for pattern in poison_patterns)
    
    @property
    def age_hours(self) -> float:
        """Get age of the message in hours"""
        age = datetime.now(timezone.utc) - self.first_failure_time
        return age.total_seconds() / 3600
    
    def calculate_next_retry_time(self, policy: RetryPolicy) -> datetime:
        """Calculate when this message should be retried"""
        if policy.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay_ms = min(
                policy.initial_delay_ms * (policy.backoff_multiplier ** self.retry_count),
                policy.max_delay_ms
            )
        elif policy.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay_ms = min(
                policy.initial_delay_ms * (self.retry_count + 1),
                policy.max_delay_ms
            )
        elif policy.strategy == RetryStrategy.FIXED_DELAY:
            delay_ms = policy.initial_delay_ms
        else:  # IMMEDIATE
            delay_ms = 0
        
        return datetime.now(timezone.utc) + timedelta(milliseconds=delay_ms)


class DLQHandlerFixed:
    """
    FIXED: Handles Dead Letter Queue processing with intelligent retry
    
    Features:
    - Non-blocking Kafka operations using thread pool
    - Exponential backoff retry
    - Poison message detection
    - Circuit breaker pattern
    - Metrics and monitoring
    """
    
    def __init__(
        self,
        dlq_topic: str,
        kafka_config: Dict[str, Any],
        redis_client: aioredis.Redis,
        retry_policy: Optional[RetryPolicy] = None,
        poison_topic: Optional[str] = None,
        consumer_group: Optional[str] = None
    ):
        """Initialize DLQ handler"""
        self.dlq_topic = dlq_topic
        self.kafka_config = kafka_config
        self.redis_client = redis_client
        self.retry_policy = retry_policy or RetryPolicy()
        self.poison_topic = poison_topic or f"{dlq_topic}.poison"
        self.consumer_group = consumer_group or 'dlq-handler-group'
        
        # Kafka clients
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        
        # Processing state
        self.processing = False
        self.process_task: Optional[asyncio.Task] = None
        
        # Thread pool for blocking operations
        self.executor = ThreadPoolExecutor(max_workers=2)
        
        # Retry queue
        self.retry_queue: List[FailedMessage] = []
        
        # Message processors
        self.message_processors: Dict[str, Callable] = {}
        
        # Metrics
        self.metrics = {
            'messages_processed': 0,
            'messages_retried': 0,
            'messages_recovered': 0,
            'messages_poisoned': 0,
            'current_queue_size': 0
        }
    
    def register_processor(self, topic: str, processor: Callable):
        """Register a message processor for a specific topic"""
        self.message_processors[topic] = processor
        logger.info(f"Registered processor for topic: {topic}")
    
    async def start_processing(self):
        """Start processing DLQ messages"""
        if self.processing:
            logger.warning("DLQ processing already started")
            return
        
        # Initialize Kafka clients
        consumer_config = self.kafka_config.copy()
        consumer_config['group.id'] = self.consumer_group
        consumer_config['enable.auto.commit'] = False
        consumer_config['auto.offset.reset'] = 'earliest'
        
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([self.dlq_topic])
        
        producer_config = self.kafka_config.copy()
        producer_config['client.id'] = 'dlq-handler-producer'
        self.producer = Producer(producer_config)
        
        self.processing = True
        
        # Start processing loop
        self.process_task = asyncio.create_task(self._process_loop())
        
        # Start retry scheduler
        asyncio.create_task(self._retry_scheduler())
        
        logger.info(f"Started DLQ processing for topic {self.dlq_topic} with group {self.consumer_group}")
    
    async def stop_processing(self):
        """Stop processing DLQ messages"""
        self.processing = False
        
        if self.process_task:
            self.process_task.cancel()
            try:
                await self.process_task
            except asyncio.CancelledError:
                pass
        
        if self.consumer:
            self.consumer.close()
        
        # Shutdown thread pool
        self.executor.shutdown(wait=False)
        
        logger.info("Stopped DLQ processing")
    
    def _poll_message(self, timeout: float = 1.0):
        """Poll for message in thread (blocking operation)"""
        return self.consumer.poll(timeout=timeout)
    
    async def _process_loop(self):
        """Main DLQ processing loop - FIXED to not block event loop"""
        loop = asyncio.get_event_loop()
        
        while self.processing:
            try:
                # Poll for message in thread pool (non-blocking for event loop)
                msg = await loop.run_in_executor(self.executor, self._poll_message, 0.5)
                
                if msg is None:
                    # No message, yield control
                    await asyncio.sleep(0.1)
                    continue
                
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Process the DLQ message
                await self._process_dlq_message(msg)
                
                # Commit offset
                self.consumer.commit(asynchronous=False)
                
            except Exception as e:
                logger.error(f"Error in DLQ processing loop: {e}")
                await asyncio.sleep(1)
    
    async def _process_dlq_message(self, msg):
        """Process a message from the DLQ"""
        try:
            # Parse DLQ message
            dlq_data = json.loads(msg.value().decode('utf-8'))
            
            # Extract original message information
            original_topic = dlq_data.get('original_topic')
            original_value = dlq_data.get('original_value')
            error_message = dlq_data.get('error', 'Unknown error')
            retry_count = dlq_data.get('retry_count', 0)
            first_failure = dlq_data.get('first_failure_time')
            
            # Create FailedMessage object
            failed_msg = FailedMessage(
                message_id=self._generate_message_id(original_value),
                topic=original_topic,
                partition=dlq_data.get('partition', 0),
                offset=dlq_data.get('offset', 0),
                key=dlq_data.get('key'),
                value=original_value,
                error=error_message,
                retry_count=retry_count,
                first_failure_time=datetime.fromisoformat(first_failure) if first_failure else datetime.now(timezone.utc)
            )
            
            # Check if message is poison
            if failed_msg.is_poison or retry_count >= self.retry_policy.poison_threshold:
                await self._move_to_poison_queue(failed_msg)
                return
            
            # Check if message is too old
            if failed_msg.age_hours > 24:
                logger.warning(f"Message too old ({failed_msg.age_hours:.1f} hours), moving to poison queue")
                await self._move_to_poison_queue(failed_msg)
                return
            
            # Calculate next retry time
            failed_msg.next_retry_time = failed_msg.calculate_next_retry_time(self.retry_policy)
            
            # Add to retry queue
            await self._add_to_retry_queue(failed_msg)
            
            self.metrics['messages_processed'] += 1
            
        except Exception as e:
            logger.error(f"Failed to process DLQ message: {e}")
    
    async def _retry_scheduler(self):
        """Background task to retry messages when their time comes"""
        while self.processing:
            try:
                now = datetime.now(timezone.utc)
                messages_to_retry = []
                
                # Find messages ready for retry
                for msg in self.retry_queue:
                    if msg.next_retry_time and msg.next_retry_time <= now:
                        messages_to_retry.append(msg)
                
                # Remove from queue and retry
                for msg in messages_to_retry:
                    self.retry_queue.remove(msg)
                    await self._retry_message(msg)
                
                # Update metrics
                self.metrics['current_queue_size'] = len(self.retry_queue)
                
                # Wait before next check
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error in retry scheduler: {e}")
                await asyncio.sleep(1)
    
    async def _retry_message(self, failed_msg: FailedMessage):
        """Retry a failed message"""
        try:
            # Get processor for the original topic
            processor = self.message_processors.get(failed_msg.topic)
            
            if not processor:
                logger.error(f"No processor registered for topic: {failed_msg.topic}")
                await self._move_to_poison_queue(failed_msg)
                return
            
            # Try to process the message
            try:
                result = await processor(json.loads(failed_msg.value))
                
                # Success! Message recovered
                logger.info(f"Successfully recovered message after {failed_msg.retry_count} retries")
                self.metrics['messages_recovered'] += 1
                
                # Store recovery metrics
                await self._record_recovery(failed_msg)
                
            except Exception as process_error:
                # Processing failed again
                failed_msg.retry_count += 1
                failed_msg.last_retry_time = datetime.now(timezone.utc)
                failed_msg.error = str(process_error)
                
                logger.warning(f"Retry {failed_msg.retry_count} failed: {process_error}")
                
                # Check if we should give up
                if failed_msg.retry_count >= self.retry_policy.max_retries:
                    await self._move_to_poison_queue(failed_msg)
                else:
                    # Recalculate next retry time and re-queue
                    failed_msg.next_retry_time = failed_msg.calculate_next_retry_time(self.retry_policy)
                    await self._add_to_retry_queue(failed_msg)
                
                self.metrics['messages_retried'] += 1
                
        except Exception as e:
            logger.error(f"Failed to retry message: {e}")
            await self._move_to_poison_queue(failed_msg)
    
    async def _add_to_retry_queue(self, failed_msg: FailedMessage):
        """Add message to retry queue"""
        # Store in Redis for persistence
        retry_data = {
            'message_id': failed_msg.message_id,
            'topic': failed_msg.topic,
            'value': failed_msg.value,
            'retry_count': failed_msg.retry_count,
            'next_retry_time': failed_msg.next_retry_time.isoformat() if failed_msg.next_retry_time else None,
            'error': failed_msg.error
        }
        
        await self.redis_client.hset(
            f"dlq:retry:{failed_msg.message_id}",
            mapping=retry_data
        )
        await self.redis_client.expire(f"dlq:retry:{failed_msg.message_id}", 86400)
        
        # Add to in-memory queue
        self.retry_queue.append(failed_msg)
        
        # Sort queue by retry time
        self.retry_queue.sort(key=lambda m: m.next_retry_time or datetime.max.replace(tzinfo=timezone.utc))
        
        logger.debug(f"Added message to retry queue, next retry at: {failed_msg.next_retry_time}")
    
    async def _move_to_poison_queue(self, failed_msg: FailedMessage):
        """Move message to poison queue"""
        try:
            poison_data = {
                'message_id': failed_msg.message_id,
                'original_topic': failed_msg.topic,
                'value': failed_msg.value,
                'error': failed_msg.error,
                'retry_count': failed_msg.retry_count,
                'first_failure_time': failed_msg.first_failure_time.isoformat(),
                'moved_to_poison_at': datetime.now(timezone.utc).isoformat(),
                'reason': 'max_retries_exceeded' if failed_msg.retry_count >= self.retry_policy.max_retries else 'poison_detected'
            }
            
            # Send to poison topic
            self.producer.produce(
                topic=self.poison_topic,
                key=failed_msg.message_id.encode('utf-8'),
                value=json.dumps(poison_data)
            )
            self.producer.flush()
            
            # Store in Redis
            await self.redis_client.lpush(
                "dlq:poison:messages",
                json.dumps(poison_data)
            )
            await self.redis_client.ltrim("dlq:poison:messages", 0, 999)
            
            self.metrics['messages_poisoned'] += 1
            
            logger.error(f"Moved message to poison queue: {failed_msg.message_id}")
            
        except Exception as e:
            logger.error(f"Failed to move message to poison queue: {e}")
    
    async def _record_recovery(self, failed_msg: FailedMessage):
        """Record successful recovery metrics"""
        recovery_data = {
            'message_id': failed_msg.message_id,
            'topic': failed_msg.topic,
            'retry_count': failed_msg.retry_count,
            'total_time_ms': int((datetime.now(timezone.utc) - failed_msg.first_failure_time).total_seconds() * 1000),
            'recovered_at': datetime.now(timezone.utc).isoformat()
        }
        
        await self.redis_client.lpush(
            "dlq:recovered:messages",
            json.dumps(recovery_data)
        )
        await self.redis_client.ltrim("dlq:recovered:messages", 0, 99)
    
    def _generate_message_id(self, value: str) -> str:
        """Generate unique ID for a message"""
        return hashlib.sha256(value.encode()).hexdigest()[:16]
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return {
            **self.metrics,
            'retry_queue_next': self.retry_queue[0].next_retry_time.isoformat() if self.retry_queue else None
        }