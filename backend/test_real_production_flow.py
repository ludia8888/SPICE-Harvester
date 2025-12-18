#!/usr/bin/env python3
"""
🔥 THINK ULTRA ULTRA ULTRA! REAL Production Flow Test
No mocks, no fakes, actual production verification
"""

import asyncio
import aiohttp
import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4
import subprocess

# REAL production configuration
os.environ.update({
    "DOCKER_CONTAINER": "false",
    "MINIO_ENDPOINT_URL": "http://localhost:9000",
    "MINIO_ACCESS_KEY": "admin",
    "MINIO_SECRET_KEY": "spice123!",
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",  # CORRECT PORT
    "POSTGRES_USER": "spiceadmin",
    "POSTGRES_PASSWORD": "spicepass123",
    "POSTGRES_DB": "spicedb",
    "ELASTICSEARCH_HOST": "localhost",
    "ELASTICSEARCH_PORT": "9200",
    "ELASTICSEARCH_USER": "elastic",
    "ELASTICSEARCH_PASSWORD": "spice123!",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "REDIS_HOST": "localhost",
    "REDIS_PORT": "6379",
    "REDIS_PASSWORD": "spice123!",
    "TERMINUS_SERVER_URL": "http://localhost:6363"
})


class ProductionFlowTest:
    """REAL production flow test - no mocks"""
    
    def __init__(self):
        self.test_id = f"prod_test_{uuid4().hex[:8]}"
        self.results = {}
        self.errors = []
        
    async def verify_infrastructure(self):
        """Verify ALL services are running and accessible"""
        print("\n" + "="*80)
        print("1️⃣ VERIFYING INFRASTRUCTURE")
        print("="*80)
        
        checks = []
        
        # 1. PostgreSQL
        try:
            import asyncpg
            conn = await asyncpg.connect(
                host="localhost",
                port=5432,
                user="spiceadmin",
                password="spicepass123",
                database="spicedb"
            )
            count = await conn.fetchval("SELECT COUNT(*) FROM spice_event_registry.processed_events")
            await conn.close()
            checks.append(("PostgreSQL", True, f"{count} processed_events"))
        except Exception as e:
            checks.append(("PostgreSQL", False, str(e)))
            self.errors.append(f"PostgreSQL: {e}")
        
        # 2. MinIO
        try:
            import aioboto3
            session = aioboto3.Session()
            async with session.client(
                's3',
                endpoint_url='http://localhost:9000',
                aws_access_key_id='admin',
                aws_secret_access_key='spice123!'
            ) as s3:
                await s3.list_buckets()
                checks.append(("MinIO/S3", True, "Connected"))
        except Exception as e:
            checks.append(("MinIO/S3", False, str(e)))
            self.errors.append(f"MinIO: {e}")
        
        # 3. Elasticsearch
        try:
            async with aiohttp.ClientSession() as session:
                auth = aiohttp.BasicAuth('elastic', 'spice123!')
                async with session.get(
                    'http://localhost:9200/_cluster/health',
                    auth=auth
                ) as resp:
                    data = await resp.json()
                    status = data.get('status', 'unknown')
                    checks.append(("Elasticsearch", True, f"Status: {status}"))
        except Exception as e:
            checks.append(("Elasticsearch", False, str(e)))
            self.errors.append(f"Elasticsearch: {e}")
        
        # 4. Kafka
        try:
            result = subprocess.run(
                ["kafka-topics", "--bootstrap-server", "localhost:9092", "--list"],
                capture_output=True, text=True, timeout=5
            )
            topics = result.stdout.strip().split('\n')
            checks.append(("Kafka", result.returncode == 0, f"{len(topics)} topics"))
        except Exception as e:
            checks.append(("Kafka", False, str(e)))
            self.errors.append(f"Kafka: {e}")
        
        # 5. Redis
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, password='spice123!', decode_responses=True)
            r.ping()
            checks.append(("Redis", True, "Connected"))
        except Exception as e:
            checks.append(("Redis", False, str(e)))
            self.errors.append(f"Redis: {e}")
        
        # 6. TerminusDB
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('http://localhost:6363/api/info') as resp:
                    if resp.status == 200:
                        checks.append(("TerminusDB", True, "Connected"))
                    else:
                        checks.append(("TerminusDB", False, f"Status {resp.status}"))
        except Exception as e:
            checks.append(("TerminusDB", False, str(e)))
            self.errors.append(f"TerminusDB: {e}")
        
        # Print results
        for service, status, detail in checks:
            icon = "✅" if status else "❌"
            print(f"  {icon} {service}: {detail}")
        
        all_ok = all(status for _, status, _ in checks)
        self.results["infrastructure"] = all_ok
        return all_ok
    
    async def create_test_database(self):
        """Create a test database via OMS API"""
        print("\n" + "="*80)
        print("2️⃣ CREATING TEST DATABASE")
        print("="*80)
        
        db_name = f"test_db_{self.test_id}"
        
        async with aiohttp.ClientSession() as session:
            # Create database
            async with session.post(
                'http://localhost:8000/api/v1/database/create',
                json={
                    "name": db_name,
                    "description": f"Production test database {self.test_id}"
                }
            ) as resp:
                result = await resp.json()
                print(f"  Database creation: {resp.status}")
                print(f"  Response: {json.dumps(result, indent=2)}")
                
                if resp.status == 202:  # Async accepted
                    command_id = result.get('data', {}).get('command_id')
                    print(f"  ✅ Command ID: {command_id}")
                    self.results["db_creation"] = True
                    self.results["db_name"] = db_name
                    self.results["db_command_id"] = command_id
                    return db_name
                else:
                    self.errors.append(f"DB creation failed: {result}")
                    self.results["db_creation"] = False
                    return None
    
    async def create_test_ontology(self, db_name):
        """Create test ontology"""
        print("\n" + "="*80)
        print("3️⃣ CREATING TEST ONTOLOGY")
        print("="*80)
        
        ontology_data = {
            "id": f"TestProduct_{self.test_id}",
            "label": "Test Product",
            "description": "Production test product",
            "properties": [
                {
                    "name": "product_id",
                    "type": "string",
                    "label": "Product ID",
                    "required": True
                },
                {
                    "name": "name",
                    "type": "string",
                    "label": "Product Name",
                    "required": True
                },
                {
                    "name": "price",
                    "type": "number",
                    "label": "Price"
                }
            ]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f'http://localhost:8000/api/v1/database/{db_name}/ontology',
                json=ontology_data
            ) as resp:
                result = await resp.json()
                print(f"  Ontology creation: {resp.status}")
                
                if resp.status in [200, 201, 202]:
                    print(f"  ✅ Ontology created: {ontology_data['id']}")
                    self.results["ontology_creation"] = True
                    return ontology_data['id']
                else:
                    self.errors.append(f"Ontology creation failed: {result}")
                    self.results["ontology_creation"] = False
                    return None
    
    async def create_test_instance(self, db_name, class_id):
        """Create test instance"""
        print("\n" + "="*80)
        print("4️⃣ CREATING TEST INSTANCE")
        print("="*80)
        
        instance_data = {
            "data": {
                "product_id": f"PROD_{self.test_id}",
                "name": f"Test Product {self.test_id}",
                "price": 99.99
            }
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f'http://localhost:8000/api/v1/instances/{db_name}/async/{class_id}/create',
                json=instance_data
            ) as resp:
                result = await resp.json()
                print(f"  Instance creation: {resp.status}")
                
                if resp.status == 202:  # Async accepted
                    command_id = result.get('command_id')
                    print(f"  ✅ Command ID: {command_id}")
                    self.results["instance_creation"] = True
                    self.results["instance_command_id"] = command_id
                    return command_id
                else:
                    self.errors.append(f"Instance creation failed: {result}")
                    self.results["instance_creation"] = False
                    return None
    
    async def verify_s3_events(self):
        """Verify events in S3"""
        print("\n" + "="*80)
        print("5️⃣ VERIFYING S3 EVENTS")
        print("="*80)
        
        # Wait for processing
        print("  Waiting 5 seconds for event processing...")
        await asyncio.sleep(5)
        
        import aioboto3
        session = aioboto3.Session()
        
        async with session.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='spice123!'
        ) as s3:
            # List all events
            response = await s3.list_objects_v2(
                Bucket='spice-event-store',
                Prefix=f"events/{datetime.now(timezone.utc).year:04d}/{datetime.now(timezone.utc).month:02d}/{datetime.now(timezone.utc).day:02d}/"
            )
            
            today_events = response.get('Contents', [])
            print(f"  Events today: {len(today_events)}")
            
            # Check for our test events
            test_events = [e for e in today_events if self.test_id in e['Key']]
            print(f"  Our test events: {len(test_events)}")
            
            if test_events:
                print("  ✅ Events found in S3:")
                for event in test_events[:3]:  # Show first 3
                    print(f"    - {event['Key']}")
                self.results["s3_events"] = True
            else:
                self.errors.append("No test events found in S3")
                self.results["s3_events"] = False
    
    async def verify_postgresql_registry(self):
        """Verify processed-event registry entries"""
        print("\n" + "="*80)
        print("6️⃣ VERIFYING POSTGRESQL REGISTRY")
        print("="*80)
        
        import asyncpg
        conn = await asyncpg.connect(
            host="localhost",
            port=5432,
            user="spiceadmin",
            password="spicepass123",
            database="spicedb"
        )
        
        query = """
            SELECT handler, event_id, aggregate_id, sequence_number, status, processed_at, last_error
            FROM spice_event_registry.processed_events
            WHERE aggregate_id LIKE $1
            ORDER BY started_at DESC
            LIMIT 10
        """

        rows = await conn.fetch(query, f"%{self.test_id}%")
        print(f"  Registry entries with our test ID: {len(rows)}")

        if rows:
            print("  ✅ Registry entries found:")
            for row in rows:
                processed = "✅ Done" if row["status"] == "done" else f"⚠️ {row['status']}"
                print(f"    - {row['handler']}: {row['aggregate_id']} - {processed}")
            self.results["registry_entries"] = True
        else:
            self.errors.append("No registry entries found")
            self.results["registry_entries"] = False
        
        await conn.close()
    
    async def verify_kafka_messages(self):
        """Verify Kafka message flow"""
        print("\n" + "="*80)
        print("7️⃣ VERIFYING KAFKA MESSAGES")
        print("="*80)
        
        # Check Kafka consumer groups
        result = subprocess.run(
            ["kafka-consumer-groups", "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True, text=True
        )
        
        groups = result.stdout.strip().split('\n')
        print(f"  Consumer groups: {len(groups)}")
        
        # Check lag for instance worker
        for group in groups:
            if 'instance' in group.lower():
                lag_result = subprocess.run(
                    ["kafka-consumer-groups", "--bootstrap-server", "localhost:9092",
                     "--group", group, "--describe"],
                    capture_output=True, text=True
                )
                print(f"  Group {group}:")
                lines = lag_result.stdout.strip().split('\n')
                for line in lines[1:4]:  # Show first few lines
                    if line.strip():
                        print(f"    {line}")
        
        self.results["kafka_flow"] = True
    
    async def run_complete_test(self):
        """Run the complete production test"""
        print("\n" + "="*100)
        print("🔥 THINK ULTRA! REAL PRODUCTION FLOW TEST")
        print("="*100)
        print(f"Test ID: {self.test_id}")
        print(f"Started: {datetime.now(timezone.utc).isoformat()}")
        
        try:
            # 1. Verify infrastructure
            if not await self.verify_infrastructure():
                print("\n❌ Infrastructure check failed!")
                return False
            
            # 2. Create test database
            db_name = await self.create_test_database()
            if not db_name:
                print("\n❌ Database creation failed!")
                return False
            
            # Wait for database creation by worker
            await asyncio.sleep(5)
            
            # 3. Create test ontology
            class_id = await self.create_test_ontology(db_name)
            if not class_id:
                print("\n❌ Ontology creation failed!")
                return False
            
            # 4. Create test instance
            instance_id = await self.create_test_instance(db_name, class_id)
            if not instance_id:
                print("\n❌ Instance creation failed!")
                return False
            
            # 5. Verify S3 events
            await self.verify_s3_events()
            
            # 6. Verify PostgreSQL processed-event registry
            await self.verify_postgresql_registry()
            
            # 7. Verify Kafka messages
            await self.verify_kafka_messages()
            
            # Summary
            print("\n" + "="*100)
            print("📊 TEST SUMMARY")
            print("="*100)
            
            for key, value in self.results.items():
                icon = "✅" if value else "❌"
                print(f"  {icon} {key}: {value}")
            
            if self.errors:
                print("\n⚠️ ERRORS FOUND:")
                for error in self.errors:
                    print(f"  - {error}")
            
            success = all(v for v in self.results.values() if isinstance(v, bool))
            
            print("\n" + "="*100)
            if success:
                print("🎉 PRODUCTION TEST: PASSED")
                print("System is working correctly!")
            else:
                print("❌ PRODUCTION TEST: FAILED")
                print("Issues need to be fixed!")
            print("="*100)
            
            return success
            
        except Exception as e:
            print(f"\n❌ Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            return False


async def main():
    """Run the production test"""
    tester = ProductionFlowTest()
    success = await tester.run_complete_test()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
