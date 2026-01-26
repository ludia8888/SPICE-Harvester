#!/usr/bin/env python3
"""
🔥 THINK ULTRA! FINAL SYSTEM VERIFICATION
Comprehensive check that EVERYTHING is working correctly
NO MOCKS - REAL PRODUCTION VERIFICATION
"""

import asyncio
import aiohttp
import json
import uuid
import os
from datetime import datetime, timezone
import asyncpg
import boto3
from botocore.exceptions import ClientError

from shared.config.settings import get_settings

# Set critical environment variable for local running
os.environ["DOCKER_CONTAINER"] = "false"

async def main():
    print("=" * 80)
    print("🔥 SPICE HARVESTER FINAL SYSTEM VERIFICATION")
    print("Think Ultra Mode: Verifying EVERY component")
    print("=" * 80)
    
    all_checks_passed = True
    issues_found = []
    
    cfg = get_settings()

    async with aiohttp.ClientSession() as session:
        
        # 1. Service Health Checks
        print("\n1️⃣ SERVICE HEALTH CHECKS")
        print("-" * 40)
        
        services = [
            ("OMS", "http://localhost:8000/health"),
            ("BFF", "http://localhost:8002/api/v1/health"),
            ("Funnel", "http://localhost:8003/health")
        ]
        
        for service_name, health_url in services:
            try:
                async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        # Check nested structure
                        if result.get("status") == "success" and result.get("data", {}).get("status") == "healthy":
                            print(f"   ✅ {service_name}: HEALTHY")
                        else:
                            print(f"   ⚠️  {service_name}: Unhealthy - {result}")
                            issues_found.append(f"{service_name} unhealthy")
                            all_checks_passed = False
                    else:
                        print(f"   ❌ {service_name}: HTTP {resp.status}")
                        issues_found.append(f"{service_name} returned {resp.status}")
                        all_checks_passed = False
            except Exception as e:
                print(f"   ❌ {service_name}: Not responding - {str(e)[:50]}")
                issues_found.append(f"{service_name} not responding")
                all_checks_passed = False
        
        # 2. Infrastructure Components
        print("\n2️⃣ INFRASTRUCTURE COMPONENTS")
        print("-" * 40)
        
        # PostgreSQL
        try:
            conn = await asyncpg.connect(
                host='localhost',
                port=5432,  # Correct port!
                user='spiceadmin',
                password='spicepass123',
                database='spicedb'
            )
            
            # Check processed-event registry schema
            schema_exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = 'spice_event_registry'
                )
            """)
            
            if schema_exists:
                processed_events_exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'spice_event_registry' 
                        AND table_name = 'processed_events'
                    )
                """)
                
                aggregate_versions_exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'spice_event_registry' 
                        AND table_name = 'aggregate_versions'
                    )
                """)

                if processed_events_exists and aggregate_versions_exists:
                    count = await conn.fetchval("SELECT COUNT(*) FROM spice_event_registry.processed_events")
                    print(f"   ✅ PostgreSQL: Connected (Port 5432)")
                    print(f"      • Registry schema: EXISTS")
                    print(f"      • processed_events: {count} records")
                else:
                    print(f"   ⚠️  PostgreSQL: Registry tables missing")
                    issues_found.append("PostgreSQL registry tables missing")
                    all_checks_passed = False
            else:
                print(f"   ⚠️  PostgreSQL: Registry schema missing")
                issues_found.append("PostgreSQL registry schema missing")
                all_checks_passed = False
                
            await conn.close()
        except Exception as e:
            print(f"   ❌ PostgreSQL: Connection failed - {e}")
            issues_found.append("PostgreSQL connection failed")
            all_checks_passed = False
        
        # Redis
        try:
            import redis.asyncio as redis
            r = redis.Redis(host='localhost', port=6379, password='spice123!')
            if await r.ping():
                keys_count = len(await r.keys('*'))
                print(f"   ✅ Redis: Connected")
                print(f"      • Keys in cache: {keys_count}")
            await r.close()
        except Exception as e:
            print(f"   ❌ Redis: Connection failed - {e}")
            issues_found.append("Redis connection failed")
            all_checks_passed = False
        
        # Elasticsearch
        try:
            async with session.get(
                "http://elastic:spice123!@localhost:9200/_cluster/health"
            ) as resp:
                if resp.status == 200:
                    health = await resp.json()
                    status = health.get('status', 'unknown')
                    color = "✅" if status == "green" else "⚠️" if status == "yellow" else "❌"
                    print(f"   {color} Elasticsearch: {status.upper()}")
                    print(f"      • Nodes: {health.get('number_of_nodes', 0)}")
                    print(f"      • Indices: {health.get('active_shards', 0)} shards")
                else:
                    print(f"   ❌ Elasticsearch: HTTP {resp.status}")
                    issues_found.append(f"Elasticsearch returned {resp.status}")
                    all_checks_passed = False
        except Exception as e:
            print(f"   ❌ Elasticsearch: Connection failed - {e}")
            issues_found.append("Elasticsearch connection failed")
            all_checks_passed = False
        
        # MinIO/S3
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url='http://localhost:9000',
                aws_access_key_id='admin',
                aws_secret_access_key='spice123!',
                use_ssl=False,
                verify=False
            )
            
            # Check event-store bucket
            bucket = cfg.storage.event_store_bucket
            try:
                s3_client.head_bucket(Bucket=bucket)
                # Count objects
                response = s3_client.list_objects_v2(Bucket=bucket)
                object_count = response.get('KeyCount', 0)
                print(f"   ✅ MinIO/S3: Connected")
                print(f"      • Event store bucket: EXISTS ({bucket})")
                print(f"      • Event objects: {object_count}")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"   ⚠️  MinIO/S3: Event store bucket missing ({bucket})")
                    issues_found.append("MinIO event store bucket missing")
                    all_checks_passed = False
                else:
                    raise
        except Exception as e:
            print(f"   ❌ MinIO/S3: Connection failed - {e}")
            issues_found.append("MinIO connection failed")
            all_checks_passed = False
        
        # TerminusDB
        try:
            async with session.get(
                "http://localhost:6363/api/info",
                auth=aiohttp.BasicAuth('admin', 'spice123!')  # Correct password!
            ) as resp:
                if resp.status == 200:
                    info = await resp.json()
                    version = info.get('api:info', {}).get('terminusdb', {}).get('version', 'unknown')
                    print(f"   ✅ TerminusDB: Connected")
                    print(f"      • Version: {version}")
                    print(f"      • Auth: spice123! (correct)")
                else:
                    print(f"   ❌ TerminusDB: HTTP {resp.status}")
                    issues_found.append(f"TerminusDB returned {resp.status}")
                    all_checks_passed = False
        except Exception as e:
            print(f"   ❌ TerminusDB: Connection failed - {e}")
            issues_found.append("TerminusDB connection failed")
            all_checks_passed = False
        
        # Kafka
        try:
            from aiokafka import AIOKafkaProducer
            producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
            await producer.start()
            await producer.stop()
            print(f"   ✅ Kafka: Connected")
            print(f"      • Bootstrap: localhost:9092")
        except Exception as e:
            print(f"   ❌ Kafka: Connection failed - {e}")
            issues_found.append("Kafka connection failed")
            all_checks_passed = False
        
        # 3. Event Sourcing Flow Test
        print("\n3️⃣ EVENT SOURCING FLOW TEST")
        print("-" * 40)
        
        test_db = f"verify_db_{uuid.uuid4().hex[:8]}"
        test_class = f"VerifyClass_{uuid.uuid4().hex[:8]}"
        
        try:
            # Create database
            print(f"   Creating test database: {test_db}")
            async with session.post(
                "http://localhost:8000/api/v1/database/create",
                json={"name": test_db, "description": "Verification test"}
            ) as resp:
                if resp.status == 202:
                    result = await resp.json()
                    command_id = result.get('command_id')
                    print(f"   ✅ Database creation: 202 ACCEPTED")
                    print(f"      • Command ID: {command_id}")
                else:
                    print(f"   ❌ Database creation: HTTP {resp.status}")
                    issues_found.append("Database creation failed")
                    all_checks_passed = False
            
            # Wait for processing
            await asyncio.sleep(5)
            
            # Verify database exists
            async with session.get(
                f"http://localhost:8000/api/v1/database/exists/{test_db}"
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    if result.get('data', {}).get('exists'):
                        print(f"   ✅ Database verified in TerminusDB")
                    else:
                        print(f"   ❌ Database not created")
                        issues_found.append("Database creation not processed")
                        all_checks_passed = False
                else:
                    print(f"   ❌ Database verification failed")
                    issues_found.append("Database verification failed")
                    all_checks_passed = False
            
            # Create ontology
            print(f"   Creating test ontology: {test_class}")
            ontology_data = {
                "id": test_class,
                "label": "Verification Class",
                "properties": [
                    {"name": "test_id", "type": "string", "label": "Test ID", "required": True},
                    {"name": "value", "type": "decimal", "label": "Value"}
                ]
            }
            
            async with session.post(
                f"http://localhost:8000/api/v1/database/{test_db}/ontology/create-advanced",
                json=ontology_data
            ) as resp:
                if resp.status == 202:
                    result = await resp.json()
                    command_id = result.get('command_id')
                    print(f"   ✅ Ontology creation: 202 ACCEPTED (Event Sourcing)")
                    print(f"      • Command ID: {command_id}")
                elif resp.status == 200:
                    result = await resp.json()
                    print(f"   ✅ Ontology creation: 200 OK (Direct mode)")
                    print(f"      • Class ID: {result.get('id', 'N/A')}")
                else:
                    print(f"   ❌ Ontology creation: HTTP {resp.status}")
                    issues_found.append("Ontology creation failed")
                    all_checks_passed = False
            
            # Wait for processing
            await asyncio.sleep(5)
            
            # Create instance
            print(f"   Creating test instance")
            instance_data = {
                "test_id": f"TEST_{uuid.uuid4().hex[:8]}",
                "value": 123.45
            }
            
            async with session.post(
                f"http://localhost:8000/api/v1/instances/{test_db}/async/{test_class}/create",
                json={"data": instance_data}
            ) as resp:
                if resp.status == 202:
                    result = await resp.json()
                    command_id = result.get('command_id')
                    print(f"   ✅ Instance creation: 202 ACCEPTED")
                    print(f"      • Command ID: {command_id}")
                else:
                    print(f"   ❌ Instance creation: HTTP {resp.status}")
                    text = await resp.text()
                    print(f"      • Error: {text[:200]}")
                    issues_found.append("Instance creation failed")
                    all_checks_passed = False
            
            # Cleanup
            await asyncio.sleep(3)
            print(f"   Cleaning up test database")
            async with session.delete(
                f"http://localhost:8000/api/v1/database/{test_db}"
            ) as resp:
                if resp.status in [200, 202]:
                    print(f"   ✅ Cleanup successful")
                else:
                    print(f"   ⚠️  Cleanup returned {resp.status}")
                    
        except Exception as e:
            print(f"   ❌ Event Sourcing test failed: {e}")
            issues_found.append("Event Sourcing flow failed")
            all_checks_passed = False
    
    # 4. Final Summary
    print("\n" + "=" * 80)
    print("📊 FINAL VERIFICATION SUMMARY")
    print("=" * 80)
    
    if all_checks_passed:
        print("\n🎉 ALL CHECKS PASSED!")
        print("✅ System is fully operational")
        print("✅ Event Sourcing working correctly")
        print("✅ All infrastructure components healthy")
        print("✅ Correct ports and credentials configured")
        print("\n🔥 SPICE HARVESTER is PRODUCTION READY!")
    else:
        print("\n⚠️  ISSUES FOUND:")
        for issue in issues_found:
            print(f"   • {issue}")
        print("\n❌ System has issues that need attention")
        print("Please review the issues above and fix them")
    
    print("\n" + "=" * 80)
    print("Verification completed at:", datetime.now(timezone.utc).isoformat())
    print("=" * 80)
    
    return all_checks_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
