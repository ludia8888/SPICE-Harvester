#!/usr/bin/env python3
"""
Test Projection Worker
이벤트→ES 프로젝션 동작 확인

Tests:
1. Kafka에 이벤트 발행
2. Projection Worker가 ES에 프로젝션하는지 확인
3. 이벤트 순서 보장 확인
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from uuid import uuid4
import aiohttp
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_projection_worker():
    """Test Projection Worker functionality"""
    
    logger.info("🔄 TESTING PROJECTION WORKER")
    logger.info("=" * 60)
    
    # Kafka producer setup
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    db_name = "projection_test_db"
    
    # 1. Send test events to Kafka
    logger.info("\n1️⃣ Sending test events to Kafka...")
    
    events = []
    aggregate_id = f"TEST_AGG_{uuid4().hex[:8]}"
    
    for i in range(5):
        instance_id = f"TEST_{uuid4().hex[:8]}"
        event = {
            "event_id": f"evt_{uuid4()}",
            "event_type": "INSTANCE_CREATED" if i == 0 else "INSTANCE_UPDATED",
            "sequence_number": i,
            "aggregate_id": aggregate_id,
            "aggregate_type": "TestAggregate",
            "db_name": db_name,
            "class_id": "TestEntity",
            "instance_id": instance_id,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "data": {
                "instance_id": instance_id,  # Instance ID must be in data too
                "class_id": "TestEntity",
                "field1": f"value_{i}",
                "field2": i * 100,
                "version": i + 1
            },
            "metadata": {
                "user_id": "test_user",
                "correlation_id": f"corr_{uuid4()}"
            }
        }
        
        # Send to instance_events topic (underscore, not hyphen!)
        producer.send('instance_events', value=event, key=aggregate_id)
        events.append(event)
        logger.info(f"  ✅ Sent event {i}: {event['event_type']} (seq: {i})")
        
    producer.flush()
    
    # 2. Wait for projection
    logger.info("\n2️⃣ Waiting for projection to ES...")
    await asyncio.sleep(5)
    
    # 3. Check Elasticsearch for projections
    logger.info("\n3️⃣ Checking Elasticsearch for projections...")
    
    async with aiohttp.ClientSession() as session:
        # Check instances index (uses underscore, not hyphen)
        index_name = f"{db_name.lower()}_instances"
        
        async with session.post(
            f"http://localhost:9200/{index_name}/_search",
            json={
                "query": {
                    "term": {"aggregate_id": aggregate_id}
                },
                "sort": [{"version": "asc"}]
            },
            auth=aiohttp.BasicAuth("elastic", "spice123!")
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                hits = result.get("hits", {}).get("hits", [])
                logger.info(f"  ✅ Found {len(hits)} projections in ES")
                
                for hit in hits:
                    source = hit["_source"]
                    logger.info(f"    • Version {source.get('version')}: {source.get('field1')}")
                    
                # Check if latest version is correct
                if hits:
                    latest = hits[-1]["_source"]
                    expected_version = len(events)
                    if latest.get("version") == expected_version:
                        logger.info(f"  ✅ Latest version is correct: {expected_version}")
                    else:
                        logger.warning(f"  ⚠️  Version mismatch: expected {expected_version}, got {latest.get('version')}")
            else:
                logger.warning(f"  ⚠️  ES query failed: {resp.status}")
                
        # Check events index (audit log)
        events_index = f"{db_name.lower()}_events"
        
        async with session.post(
            f"http://localhost:9200/{events_index}/_search",
            json={
                "query": {
                    "term": {"aggregate_id": aggregate_id}
                },
                "sort": [{"sequence_number": "asc"}],
                "size": 100
            },
            auth=aiohttp.BasicAuth("elastic", "spice123!")
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                hits = result.get("hits", {}).get("hits", [])
                logger.info(f"\n  📋 Found {len(hits)} events in audit log")
                
                # Check sequence ordering
                sequences = [hit["_source"].get("sequence_number") for hit in hits]
                if sequences == sorted(sequences):
                    logger.info(f"  ✅ Event sequence is correct: {sequences}")
                else:
                    logger.warning(f"  ⚠️  Event sequence out of order: {sequences}")
                    
            elif resp.status == 404:
                logger.info(f"  ℹ️  Events index doesn't exist (may not be configured)")
            else:
                logger.warning(f"  ⚠️  Events query failed: {resp.status}")
    
    # 4. Test eventual consistency
    logger.info("\n4️⃣ Testing eventual consistency...")
    
    # Send more events rapidly
    for i in range(5, 10):
        instance_id = f"TEST_{uuid4().hex[:8]}"
        event = {
            "event_id": f"evt_{uuid4()}",
            "event_type": "INSTANCE_UPDATED",
            "sequence_number": i,
            "aggregate_id": aggregate_id,
            "aggregate_type": "TestAggregate",
            "db_name": db_name,
            "class_id": "TestEntity",
            "instance_id": instance_id,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "data": {
                "instance_id": instance_id,  # Instance ID must be in data too
                "class_id": "TestEntity",
                "field1": f"rapid_value_{i}",
                "field2": i * 100,
                "version": i + 1
            }
        }
        producer.send('instance_events', value=event, key=aggregate_id)
        
    producer.flush()
    logger.info(f"  ✅ Sent 5 more rapid events")
    
    # Wait and check
    await asyncio.sleep(3)
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"http://localhost:9200/{index_name}/_search",
            json={
                "query": {
                    "term": {"aggregate_id": aggregate_id}
                },
                "sort": [{"version": "desc"}],
                "size": 1
            },
            auth=aiohttp.BasicAuth("elastic", "spice123!")
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                hits = result.get("hits", {}).get("hits", [])
                if hits:
                    latest_version = hits[0]["_source"].get("version")
                    if latest_version == 10:
                        logger.info(f"  ✅ Eventual consistency achieved: version {latest_version}")
                    else:
                        logger.warning(f"  ⚠️  Not all events projected yet: version {latest_version}/10")
            else:
                logger.warning(f"  ⚠️  Final check failed: {resp.status}")
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ PROJECTION WORKER TEST COMPLETE")
    logger.info("\n📊 Summary:")
    logger.info("  • Event publishing: ✅")
    logger.info("  • ES projection: Check results above")
    logger.info("  • Event ordering: Check sequence above")
    logger.info("  • Eventual consistency: Check final version above")
    
    producer.close()


if __name__ == "__main__":
    asyncio.run(test_projection_worker())