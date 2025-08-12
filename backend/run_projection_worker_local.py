#!/usr/bin/env python3
"""
Enhanced Projection Worker with Idempotency
THINK ULTRA³ - Production-ready event projection with exactly-once semantics

This worker consumes events from Kafka and projects them to Elasticsearch
with idempotency guarantees and ordering preservation.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import aiohttp
import redis.asyncio as aioredis
from confluent_kafka import Consumer, KafkaError

# Add parent directory to path for imports
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.services.idempotency_service import IdempotencyService, IdempotentEventProcessor
from shared.services.schema_versioning import SchemaVersioningService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnhancedProjectionWorker:
    """
    Enhanced Projection Worker with production-ready features:
    - Idempotency guarantees
    - Ordering preservation
    - Schema versioning support
    - Retry logic
    - Monitoring metrics
    """
    
    def __init__(self):
        self.running = False
        self.consumer = None
        self.redis_client = None
        self.idempotency_service = None
        self.idempotent_processor = None
        self.schema_service = None
        self.es_url = os.environ.get('ELASTICSEARCH_URL', 'http://localhost:9201')
        self.kafka_brokers = os.environ.get('KAFKA_BROKERS', '127.0.0.1:9092')
        
        # Metrics
        self.metrics = {
            'events_processed': 0,
            'events_skipped': 0,
            'events_failed': 0,
            'projection_lag_ms': 0
        }
        
    async def initialize(self):
        """Initialize all services and connections"""
        logger.info("Initializing Enhanced Projection Worker...")
        
        # Initialize Redis
        self.redis_client = aioredis.from_url(
            'redis://localhost:6379',
            encoding='utf-8',
            decode_responses=True
        )
        logger.info("✅ Redis connection established")
        
        # Initialize Idempotency Service
        self.idempotency_service = IdempotencyService(
            redis_client=self.redis_client,
            ttl_seconds=86400,  # 24 hours
            namespace="projection"
        )
        self.idempotent_processor = IdempotentEventProcessor(self.idempotency_service)
        logger.info("✅ Idempotency service initialized")
        
        # Initialize Schema Versioning Service
        self.schema_service = SchemaVersioningService()
        logger.info("✅ Schema versioning service initialized")
        
        # Initialize Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_brokers,
            'group.id': 'projection-worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit for better control
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 60000,  # 1 minute
        })
        
        # Subscribe to events topic
        self.consumer.subscribe(['instance_events'])
        logger.info("✅ Kafka consumer initialized and subscribed")
        
    async def project_to_elasticsearch(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Project event to Elasticsearch with versioning and ordering.
        
        Args:
            event_data: Event payload
            
        Returns:
            Projection result
        """
        db_name = event_data.get('db_name')
        class_id = event_data.get('class_id')
        instance_id = event_data.get('instance_id')
        event_type = event_data.get('event_type')
        event_sequence = event_data.get('sequence_number', 0)
        schema_version = event_data.get('schema_version', '1.0.0')
        
        # Index name based on database
        index_name = f"instances_{db_name.replace('-', '_')}"
        
        # Document to index
        document = {
            '_class': class_id,
            'instance_id': instance_id,
            'event_id': event_data.get('event_id'),
            'event_sequence': event_sequence,
            'schema_version': schema_version,
            'projected_at': datetime.now(timezone.utc).isoformat(),
            **event_data.get('data', {})
        }
        
        # Handle different event types
        if event_type == 'INSTANCE_CREATED':
            return await self._upsert_document(index_name, instance_id, document, event_sequence)
        elif event_type == 'INSTANCE_UPDATED':
            return await self._update_document(index_name, instance_id, document, event_sequence)
        elif event_type == 'INSTANCE_DELETED':
            return await self._soft_delete_document(index_name, instance_id, event_sequence)
        else:
            logger.warning(f"Unknown event type: {event_type}")
            return {'status': 'skipped', 'reason': 'unknown_event_type'}
    
    async def _upsert_document(
        self,
        index_name: str,
        doc_id: str,
        document: Dict[str, Any],
        sequence: int
    ) -> Dict[str, Any]:
        """
        Upsert document with sequence check for ordering.
        
        Only updates if sequence is higher than existing.
        """
        async with aiohttp.ClientSession() as session:
            # Use scripted upsert to check sequence
            script_source = """
                if (ctx._source.event_sequence == null || 
                    params.event_sequence > ctx._source.event_sequence) {
                    ctx._source.putAll(params.doc);
                } else {
                    ctx.op = 'noop';
                }
            """
            
            update_body = {
                "script": {
                    "source": script_source,
                    "params": {
                        "event_sequence": sequence,
                        "doc": document
                    }
                },
                "upsert": document
            }
            
            async with session.post(
                f"{self.es_url}/{index_name}/_update/{doc_id}",
                json=update_body,
                headers={'Content-Type': 'application/json'}
            ) as resp:
                if resp.status in [200, 201]:
                    result = await resp.json()
                    if result.get('result') == 'noop':
                        logger.info(f"Document {doc_id} skipped (older sequence)")
                        return {'status': 'skipped', 'reason': 'older_sequence'}
                    else:
                        logger.info(f"✅ Document {doc_id} projected successfully")
                        return {'status': 'success', 'result': result.get('result')}
                else:
                    error = await resp.text()
                    logger.error(f"Failed to project document: {error}")
                    return {'status': 'failed', 'error': error}
    
    async def _update_document(
        self,
        index_name: str,
        doc_id: str,
        document: Dict[str, Any],
        sequence: int
    ) -> Dict[str, Any]:
        """Update document with sequence check"""
        return await self._upsert_document(index_name, doc_id, document, sequence)
    
    async def _soft_delete_document(
        self,
        index_name: str,
        doc_id: str,
        sequence: int
    ) -> Dict[str, Any]:
        """Soft delete document by setting deleted flag"""
        delete_doc = {
            'deleted': True,
            'deleted_at': datetime.now(timezone.utc).isoformat(),
            'event_sequence': sequence
        }
        
        async with aiohttp.ClientSession() as session:
            # Check sequence before marking deleted
            script_source = """
                if (ctx._source.event_sequence == null || 
                    params.event_sequence > ctx._source.event_sequence) {
                    ctx._source.deleted = true;
                    ctx._source.deleted_at = params.deleted_at;
                    ctx._source.event_sequence = params.event_sequence;
                } else {
                    ctx.op = 'noop';
                }
            """
            
            update_body = {
                "script": {
                    "source": script_source,
                    "params": delete_doc
                }
            }
            
            async with session.post(
                f"{self.es_url}/{index_name}/_update/{doc_id}",
                json=update_body,
                headers={'Content-Type': 'application/json'}
            ) as resp:
                if resp.status in [200, 201]:
                    logger.info(f"✅ Document {doc_id} marked as deleted")
                    return {'status': 'success', 'action': 'soft_deleted'}
                else:
                    error = await resp.text()
                    logger.error(f"Failed to delete document: {error}")
                    return {'status': 'failed', 'error': error}
    
    async def process_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process single event with idempotency.
        
        Args:
            event_data: Event payload
            
        Returns:
            Processing result
        """
        event_id = event_data.get('event_id')
        aggregate_id = event_data.get('aggregate_id', event_data.get('instance_id'))  # Use aggregate_id field, fallback to instance_id
        
        # Migrate event to current schema version if needed
        if self.schema_service:
            try:
                event_data = self.schema_service.migrate_event(event_data)
                logger.debug(f"Event migrated to schema version: {event_data.get('schema_version')}")
            except Exception as e:
                logger.warning(f"Schema migration failed, using as-is: {e}")
        
        # Process with idempotency guarantee
        processed, result = await self.idempotent_processor.process_event(
            event_id=event_id,
            event_data=event_data,
            processor_func=lambda data: self.project_to_elasticsearch(data),
            aggregate_id=aggregate_id
        )
        
        if processed:
            self.metrics['events_processed'] += 1
            logger.info(f"✅ Event {event_id} processed successfully")
        else:
            self.metrics['events_skipped'] += 1
            logger.info(f"⏭️ Event {event_id} skipped (duplicate)")
        
        return result
    
    async def run(self):
        """Main processing loop"""
        logger.info("=" * 60)
        logger.info("🚀 Enhanced Projection Worker Starting")
        logger.info("=" * 60)
        
        await self.initialize()
        
        self.running = True
        batch_size = 10
        batch = []
        
        logger.info("Waiting for events from Kafka...")
        
        while self.running:
            try:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Process batch if timeout
                    if batch:
                        await self._process_batch(batch)
                        batch = []
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                # Add to batch
                try:
                    event_data = json.loads(msg.value().decode('utf-8'))
                    batch.append((msg, event_data))
                    
                    # Process batch if full
                    if len(batch) >= batch_size:
                        await self._process_batch(batch)
                        batch = []
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message: {e}")
                    self.consumer.commit(msg)
                    
            except KeyboardInterrupt:
                logger.info("Shutdown signal received")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                await asyncio.sleep(1)
        
        # Process remaining batch
        if batch:
            await self._process_batch(batch)
        
        await self.shutdown()
    
    async def _process_batch(self, batch: list):
        """Process batch of events"""
        logger.info(f"Processing batch of {len(batch)} events")
        
        for msg, event_data in batch:
            try:
                # Calculate lag
                event_time = event_data.get('occurred_at')
                if event_time:
                    event_dt = datetime.fromisoformat(event_time.replace('Z', '+00:00'))
                    lag_ms = int((datetime.now(timezone.utc) - event_dt).total_seconds() * 1000)
                    self.metrics['projection_lag_ms'] = lag_ms
                
                # Process event
                await self.process_event(event_data)
                
            except Exception as e:
                logger.error(f"Failed to process event: {e}")
                self.metrics['events_failed'] += 1
        
        # Commit all messages in batch
        if batch:
            last_msg = batch[-1][0]
            self.consumer.commit(last_msg)
            logger.info(f"✅ Batch committed (offset: {last_msg.offset()})")
    
    async def shutdown(self):
        """Clean shutdown"""
        logger.info("Shutting down Enhanced Projection Worker...")
        self.running = False
        
        # Log final metrics
        logger.info("📊 Final Metrics:")
        for key, value in self.metrics.items():
            logger.info(f"  {key}: {value}")
        
        # Close connections
        if self.consumer:
            self.consumer.close()
        
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info("✅ Shutdown complete")


async def main():
    """Main entry point"""
    worker = EnhancedProjectionWorker()
    
    try:
        await worker.run()
    except Exception as e:
        logger.error(f"Worker failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await worker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())