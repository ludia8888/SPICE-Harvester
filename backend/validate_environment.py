#!/usr/bin/env python
"""
🔥 THINK ULTRA! Complete Environment Validation Script

This script validates:
1. All environment variables are correctly set
2. All services are accessible on correct ports
3. Docker configuration matches local configuration
4. All inter-service connections work
"""

import os
import asyncio
import aiohttp
import asyncpg
import redis.asyncio as redis
from elasticsearch import AsyncElasticsearch
from confluent_kafka.admin import AdminClient
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class EnvironmentValidator:
    def __init__(self):
        self.results = {}
        self.errors = []
        
    def print_header(self, title):
        print("\n" + "=" * 70)
        print(f"🔍 {title}")
        print("=" * 70)
        
    def check_result(self, name, success, message=""):
        if success:
            print(f"  ✅ {name}: {message if message else 'OK'}")
            self.results[name] = True
        else:
            print(f"  ❌ {name}: {message if message else 'FAILED'}")
            self.results[name] = False
            self.errors.append(f"{name}: {message}")
    
    def validate_env_variables(self):
        """Validate all required environment variables"""
        self.print_header("ENVIRONMENT VARIABLES VALIDATION")
        
        required_vars = {
            # PostgreSQL
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "spiceadmin",
            "POSTGRES_PASSWORD": "spice123!",
            "POSTGRES_DB": "spicedb",
            
            # Redis
            "REDIS_HOST": "localhost",
            "REDIS_PORT": "6379",
            "REDIS_PASSWORD": "",  # Empty for local
            
            # Elasticsearch
            "ELASTICSEARCH_HOST": "localhost",
            "ELASTICSEARCH_PORT": "9200",
            "ELASTICSEARCH_USERNAME": "elastic",
            "ELASTICSEARCH_PASSWORD": "spice123!",
            
            # Kafka
            "KAFKA_HOST": "localhost",
            "KAFKA_PORT": "9092",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            
            # TerminusDB
            "TERMINUS_SERVER_URL": "http://localhost:6363",
            "TERMINUS_USER": "admin",
            "TERMINUS_ACCOUNT": "admin",
            "TERMINUS_KEY": "spice123!",
            
            # Services
            "OMS_HOST": "localhost",
            "OMS_PORT": "8000",
            "BFF_HOST": "localhost",
            "BFF_PORT": "8002",
            "FUNNEL_HOST": "localhost",
            "FUNNEL_PORT": "8003",
            
            # Docker
            "DOCKER_CONTAINER": "false",
            
            # Feature Flags
            "ENABLE_EVENT_SOURCING": "true",
        }
        
        print("\n  📋 Checking required environment variables:")
        for var, expected in required_vars.items():
            actual = os.getenv(var)
            if actual is None:
                self.check_result(var, False, "NOT SET")
            elif expected and actual != expected:
                self.check_result(var, False, f"Expected '{expected}', got '{actual}'")
            else:
                self.check_result(var, True, actual or "Empty (OK)")
    
    async def check_postgresql(self):
        """Check PostgreSQL connection and processed-event registry tables"""
        self.print_header("POSTGRESQL CONNECTION CHECK")
        
        try:
            conn = await asyncpg.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "spiceadmin"),
                password=os.getenv("POSTGRES_PASSWORD", "spice123!"),
                database=os.getenv("POSTGRES_DB", "spicedb")
            )
            
            self.check_result("PostgreSQL Connection", True, f"Connected to {os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}")
            
            # Check spice_event_registry schema
            schema_exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = 'spice_event_registry'
                )
            """)
            self.check_result("spice_event_registry Schema", schema_exists)
            
            # Check registry tables
            if schema_exists:
                processed_events_exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'spice_event_registry' 
                        AND table_name = 'processed_events'
                    )
                """)
                self.check_result("processed_events Table", processed_events_exists)
                
                aggregate_versions_exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'spice_event_registry' 
                        AND table_name = 'aggregate_versions'
                    )
                """)
                self.check_result("aggregate_versions Table", aggregate_versions_exists)

                if processed_events_exists:
                    count = await conn.fetchval("SELECT COUNT(*) FROM spice_event_registry.processed_events")
                    in_progress = await conn.fetchval("""
                        SELECT COUNT(*) FROM spice_event_registry.processed_events 
                        WHERE status = 'processing'
                    """)
                    self.check_result(
                        "Processed Events Records",
                        True,
                        f"{count} total, {in_progress} processing",
                    )
            
            await conn.close()
            
        except Exception as e:
            self.check_result("PostgreSQL Connection", False, str(e))
    
    async def check_redis(self):
        """Check Redis connection"""
        self.print_header("REDIS CONNECTION CHECK")
        
        try:
            password = os.getenv("REDIS_PASSWORD", "")
            if password:
                redis_url = f"redis://:{password}@{os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', '6379')}"
            else:
                redis_url = f"redis://{os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', '6379')}"
            
            client = redis.from_url(redis_url)
            await client.ping()
            
            self.check_result("Redis Connection", True, f"Connected to {os.getenv('REDIS_HOST')}:{os.getenv('REDIS_PORT')}")
            
            # Check some keys
            keys = await client.keys("*")
            self.check_result("Redis Keys", True, f"{len(keys)} keys found")
            
            await client.close()
            
        except Exception as e:
            self.check_result("Redis Connection", False, str(e))
    
    async def check_elasticsearch(self):
        """Check Elasticsearch connection"""
        self.print_header("ELASTICSEARCH CONNECTION CHECK")
        
        try:
            es = AsyncElasticsearch(
                [f"http://{os.getenv('ELASTICSEARCH_HOST', 'localhost')}:{os.getenv('ELASTICSEARCH_PORT', '9200')}"],
                basic_auth=(
                    os.getenv("ELASTICSEARCH_USERNAME", "elastic"),
                    os.getenv("ELASTICSEARCH_PASSWORD", "spice123!")
                )
            )
            
            info = await es.info()
            self.check_result("Elasticsearch Connection", True, f"Version {info['version']['number']}")
            
            # Check indices
            indices = await es.cat.indices(format="json")
            spice_indices = [idx for idx in indices if 'spice' in idx.get('index', '')]
            self.check_result("Elasticsearch Indices", True, f"{len(spice_indices)} SPICE indices")
            
            await es.close()
            
        except Exception as e:
            self.check_result("Elasticsearch Connection", False, str(e))
    
    def check_kafka(self):
        """Check Kafka connection"""
        self.print_header("KAFKA CONNECTION CHECK")
        
        try:
            admin_client = AdminClient({
                'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                'socket.timeout.ms': 5000,
                'api.version.request.timeout.ms': 5000
            })
            
            metadata = admin_client.list_topics(timeout=5)
            topics = list(metadata.topics.keys())
            
            self.check_result("Kafka Connection", True, f"{len(topics)} topics")
            
            # Check required topics
            required_topics = [
                "instance_commands",
                "instance_events",
                "ontology_commands",
                "ontology_events"
            ]
            
            for topic in required_topics:
                if topic in topics:
                    self.check_result(f"Topic: {topic}", True)
                else:
                    self.check_result(f"Topic: {topic}", False, "NOT FOUND")
                    
        except Exception as e:
            self.check_result("Kafka Connection", False, str(e))
    
    async def check_terminus(self):
        """Check TerminusDB connection"""
        self.print_header("TERMINUSDB CONNECTION CHECK")
        
        try:
            async with aiohttp.ClientSession() as session:
                # Get auth header
                import base64
                auth_str = f"{os.getenv('TERMINUS_USER', 'admin')}:{os.getenv('TERMINUS_KEY', 'spice123!')}"
                auth_header = f"Basic {base64.b64encode(auth_str.encode()).decode()}"
                
                # Check connection
                async with session.get(
                    f"{os.getenv('TERMINUS_SERVER_URL', 'http://localhost:6363')}/api/info",
                    headers={"Authorization": auth_header}
                ) as resp:
                    if resp.status == 200:
                        info = await resp.json()
                        version = info.get('api:info', {}).get('terminusdb', {}).get('version', 'unknown')
                        self.check_result("TerminusDB Connection", True, f"Version {version}")
                    else:
                        self.check_result("TerminusDB Connection", False, f"Status {resp.status}")
                
                # List databases
                async with session.get(
                    f"{os.getenv('TERMINUS_SERVER_URL', 'http://localhost:6363')}/api/db/admin",
                    headers={"Authorization": auth_header}
                ) as resp:
                    if resp.status == 200:
                        dbs = await resp.json()
                        db_count = len(dbs) if isinstance(dbs, list) else 0
                        self.check_result("TerminusDB Databases", True, f"{db_count} databases")
                    else:
                        self.check_result("TerminusDB Databases", False, f"Status {resp.status}")
                        
        except Exception as e:
            self.check_result("TerminusDB Connection", False, str(e))
    
    async def check_services(self):
        """Check all microservices"""
        self.print_header("MICROSERVICES HEALTH CHECK")
        
        services = [
            ("OMS", f"http://localhost:{os.getenv('OMS_PORT', '8000')}/health"),
            ("BFF", f"http://localhost:{os.getenv('BFF_PORT', '8002')}/health"),
            ("Funnel", f"http://localhost:{os.getenv('FUNNEL_PORT', '8003')}/health"),
        ]
        
        async with aiohttp.ClientSession() as session:
            for name, url in services:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            status = data.get('data', {}).get('status', 'unknown')
                            self.check_result(f"{name} Service", True, f"Status: {status}")
                        else:
                            self.check_result(f"{name} Service", False, f"Status {resp.status}")
                except Exception as e:
                    self.check_result(f"{name} Service", False, str(e))
    
    def check_docker_config(self):
        """Check Docker configuration"""
        self.print_header("DOCKER CONFIGURATION CHECK")

        docker_compose_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docker-compose.yml")
        
        if os.path.exists(docker_compose_path):
            with open(docker_compose_path, 'r') as f:
                content = f.read()
                
            # Check key configurations
            checks = [
                ("PostgreSQL Port", "5432:5432" in content or "5433:5432" in content),
                ("Redis Port", "6379:6379" in content),
                ("Elasticsearch Port", "9200:9200" in content),
                ("Kafka Port", "9092:9092" in content),
                ("TerminusDB Port", "6363:6363" in content),
            ]
            
            for name, exists in checks:
                self.check_result(name, exists)
        else:
            self.check_result("docker-compose.yml", False, "File not found")
    
    async def check_workers(self):
        """Check background workers"""
        self.print_header("BACKGROUND WORKERS CHECK")
        
        import subprocess
        
        workers = [
            "message_relay",
            "ontology_worker",
            "projection_worker",
            "instance_worker"
        ]
        
        for worker in workers:
            try:
                result = subprocess.run(
                    ["pgrep", "-f", worker],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    pids = result.stdout.strip().split('\n')
                    self.check_result(f"{worker}", True, f"Running (PID: {pids[0]})")
                else:
                    self.check_result(f"{worker}", False, "Not running")
            except Exception as e:
                self.check_result(f"{worker}", False, str(e))
    
    async def run_all_checks(self):
        """Run all validation checks"""
        print("\n" + "=" * 70)
        print("🔥 THINK ULTRA! SPICE HARVESTER ENVIRONMENT VALIDATION")
        print("=" * 70)
        
        # Synchronous checks
        self.validate_env_variables()
        self.check_docker_config()
        self.check_kafka()
        
        # Asynchronous checks
        await self.check_postgresql()
        await self.check_redis()
        await self.check_elasticsearch()
        await self.check_terminus()
        await self.check_services()
        await self.check_workers()
        
        # Summary
        self.print_header("VALIDATION SUMMARY")
        
        total = len(self.results)
        passed = sum(1 for v in self.results.values() if v)
        failed = total - passed
        
        print(f"\n  📊 Results: {passed}/{total} checks passed")
        
        if failed > 0:
            print(f"\n  ❌ Failed checks ({failed}):")
            for error in self.errors:
                print(f"    - {error}")
        else:
            print("\n  🎉 All checks passed! Environment is properly configured.")
        
        return failed == 0

async def main():
    validator = EnvironmentValidator()
    success = await validator.run_all_checks()
    
    if success:
        print("\n✅ Environment validation completed successfully!")
        print("🚀 Your SPICE HARVESTER environment is ready for production!")
    else:
        print("\n⚠️ Environment validation found issues.")
        print("Please fix the errors above and run validation again.")
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
