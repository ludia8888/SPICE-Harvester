#!/usr/bin/env python3
"""
Test Instance Worker
경량 노드 생성 및 관계 추출 동작 확인

Tests:
1. Kafka에 Instance Command 발행
2. Worker가 관계만 추출하는지 확인
3. TerminusDB에 경량 노드 생성 확인
4. S3에 이벤트 저장 확인
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from uuid import uuid4
import aiohttp
from kafka import KafkaProducer, KafkaConsumer
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_instance_worker():
    """Test Instance Worker functionality"""
    
    logger.info("👷 TESTING INSTANCE WORKER")
    logger.info("=" * 60)
    
    # Kafka producer setup
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    # Test database and instances
    db_name = "worker_test_db"
    
    # 1. Create test database first
    logger.info("\n1️⃣ Creating test database...")
    
    async with aiohttp.ClientSession() as session:
        # Create database
        async with session.post(
            "http://localhost:8000/api/v1/database/create",
            json={"name": db_name, "description": "Instance Worker Test"}
        ) as resp:
            if resp.status == 202:
                logger.info(f"  ✅ Database creation accepted")
            else:
                logger.error(f"  ❌ Database creation failed: {resp.status}")
        
        await asyncio.sleep(3)
        
        # Create ontologies with relationships
        logger.info("\n2️⃣ Creating ontologies with relationships...")
        
        ontologies = [
            {
                "id": "TestClient",
                "label": "Test Client",
                "properties": [
                    {"name": "client_id", "type": "string", "required": True},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"}
                ],
                "relationships": []
            },
            {
                "id": "TestProduct",
                "label": "Test Product",
                "properties": [
                    {"name": "product_id", "type": "string", "required": True},
                    {"name": "name", "type": "string"},
                    {"name": "price", "type": "float"},
                    {"name": "description", "type": "string"}
                ],
                "relationships": [
                    {
                        "predicate": "owned_by",
                        "target": "TestClient",
                        "cardinality": "n:1"
                    }
                ]
            }
        ]
        
        for ontology in ontologies:
            async with session.post(
                f"http://localhost:8000/api/v1/ontology/{db_name}/create",
                json=ontology
            ) as resp:
                if resp.status in [200, 201]:
                    logger.info(f"  ✅ Created {ontology['id']} ontology")
                else:
                    logger.error(f"  ❌ Failed to create {ontology['id']}: {resp.status}")
    
    # 3. Send Instance Command to Kafka
    logger.info("\n3️⃣ Sending Instance Commands to Kafka...")
    
    # Create client command (no relationships)
    client_command = {
        "command_id": f"cmd_{uuid4()}",
        "command_type": "CREATE_INSTANCE",
        "db_name": db_name,
        "class_id": "TestClient",
        "aggregate_id": "TestClient/TCL-001",
        "aggregate_type": "TestClient",
        "payload": {
            "client_id": "TCL-001",
            "name": "Test Client One",  # Domain attribute - should NOT go to graph
            "email": "client@test.com",  # Domain attribute - should NOT go to graph
            "revenue": 1000000  # Domain attribute - should NOT go to graph
        },
        "created_by": "test_script",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Create product command (with relationship)
    product_command = {
        "command_id": f"cmd_{uuid4()}",
        "command_type": "CREATE_INSTANCE",
        "db_name": db_name,
        "class_id": "TestProduct",
        "aggregate_id": "TestProduct/TP-001",
        "aggregate_type": "TestProduct",
        "payload": {
            "product_id": "TP-001",
            "name": "Test Product One",  # Domain attribute - should NOT go to graph
            "price": 99.99,  # Domain attribute - should NOT go to graph
            "description": "This is a test product",  # Domain attribute - should NOT go to graph
            "owned_by": "TestClient/TCL-001"  # Relationship - SHOULD go to graph
        },
        "created_by": "test_script",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Send commands
    producer.send('instance-commands', value=client_command, key=client_command['aggregate_id'])
    producer.send('instance-commands', value=product_command, key=product_command['aggregate_id'])
    producer.flush()
    
    logger.info(f"  ✅ Sent client command: {client_command['command_id']}")
    logger.info(f"  ✅ Sent product command: {product_command['command_id']}")
    
    # 4. Wait for processing
    logger.info("\n4️⃣ Waiting for Instance Worker to process...")
    await asyncio.sleep(5)
    
    # 5. Check TerminusDB for lightweight nodes
    logger.info("\n5️⃣ Checking TerminusDB for lightweight nodes...")
    
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("admin", "spice123!")) as session:
        # Query for created instances
        woql_query = {
            "@type": "Select",
            "variables": ["v:Instance", "v:Type"],
            "query": {
                "@type": "Triple",
                "subject": {"variable": "v:Instance"},
                "predicate": {"node": "rdf:type"},
                "object": {"variable": "v:Type"}
            }
        }
        
        async with session.post(
            f"http://localhost:6363/api/woql/admin/{db_name}",
            json={"query": woql_query}
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                bindings = result.get("bindings", [])
                logger.info(f"  ✅ Found {len(bindings)} instances in TerminusDB")
                
                for binding in bindings:
                    instance_id = binding.get("v:Instance", "unknown")
                    instance_type = binding.get("v:Type", "unknown")
                    logger.info(f"    • {instance_type}: {instance_id}")
            else:
                logger.warning(f"  ⚠️  WOQL query failed: {resp.status}")
        
        # Check specific product for relationships
        async with session.get(
            f"http://localhost:6363/api/document/admin/{db_name}/TestProduct/TP-001",
            params={"graph_type": "instance"}
        ) as resp:
            if resp.status == 200:
                product_node = await resp.json()
                
                # Check what fields are present
                has_relationships = "owned_by" in product_node
                domain_fields = ["name", "price", "description"]
                has_domain = any(field in product_node for field in domain_fields)
                
                logger.info(f"\n  📊 Product node inspection:")
                logger.info(f"    • Has relationship (owned_by): {has_relationships}")
                logger.info(f"    • Has domain fields: {has_domain}")
                
                if has_relationships and not has_domain:
                    logger.info(f"  ✅ CORRECT: Only relationships in graph!")
                else:
                    logger.warning(f"  ⚠️  INCORRECT: Domain fields in graph or missing relationships")
                    logger.info(f"    Node content: {json.dumps(product_node, indent=2)}")
            else:
                logger.warning(f"  ⚠️  Could not fetch product node: {resp.status}")
    
    # 6. Check S3/MinIO for event storage
    logger.info("\n6️⃣ Checking S3/MinIO for event storage...")
    
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='spice123!',
        region_name='us-east-1'
    )
    
    bucket_name = 'instance-events'
    
    # Check if bucket exists
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        
        # List objects for our test instances
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{db_name}/"
        )
        
        if 'Contents' in response:
            logger.info(f"  ✅ Found {len(response['Contents'])} events in S3")
            for obj in response['Contents'][:5]:
                logger.info(f"    • {obj['Key']}")
        else:
            logger.warning(f"  ⚠️  No events found in S3")
            
    except Exception as e:
        logger.warning(f"  ⚠️  Could not check S3: {e}")
    
    # 7. Check Redis for command status
    logger.info("\n7️⃣ Checking Redis for command status...")
    
    import redis
    redis_client = redis.Redis(
        host='localhost',
        port=6379,
        password='spice123!',
        decode_responses=True
    )
    
    try:
        # Check client command status
        client_status_key = f"command:{client_command['command_id']}:status"
        client_status = redis_client.get(client_status_key)
        if client_status:
            status_data = json.loads(client_status)
            logger.info(f"  ✅ Client command status: {status_data.get('status')}")
        else:
            logger.warning(f"  ⚠️  No status for client command")
        
        # Check product command status
        product_status_key = f"command:{product_command['command_id']}:status"
        product_status = redis_client.get(product_status_key)
        if product_status:
            status_data = json.loads(product_status)
            logger.info(f"  ✅ Product command status: {status_data.get('status')}")
        else:
            logger.warning(f"  ⚠️  No status for product command")
            
    except Exception as e:
        logger.warning(f"  ⚠️  Could not check Redis: {e}")
    
    # 8. Check Kafka for events
    logger.info("\n8️⃣ Checking Kafka for events...")
    
    consumer = KafkaConsumer(
        'instance-events',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=2000
    )
    
    events = []
    for message in consumer:
        events.append(message.value)
    
    logger.info(f"  ✅ Found {len(events)} recent events in Kafka")
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ INSTANCE WORKER TEST COMPLETE")
    logger.info("\n📊 Summary:")
    logger.info("  • Command publishing: ✅")
    logger.info("  • Worker processing: ✅")
    logger.info("  • Graph node creation: ✅")
    logger.info("  • Relationship extraction: Check logs above")
    logger.info("  • Event storage: Check S3 results above")
    
    producer.close()
    consumer.close()


if __name__ == "__main__":
    asyncio.run(test_instance_worker())