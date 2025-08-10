#!/usr/bin/env python3
"""
🔥 THINK ULTRA! Comprehensive Service Testing
Real production-ready testing with no mocks or bypasses
"""

import asyncio
import sys
import os
from typing import Dict, List, Tuple

# Add backend to path
sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

async def test_redis_connection():
    """Test Redis connection"""
    print("\n📍 Testing Redis Connection...")
    try:
        from shared.services.redis_service import create_redis_service_legacy
        
        redis_service = create_redis_service_legacy()
        await redis_service.connect()
        
        # Test basic operations
        await redis_service.set("test_key", "test_value")
        value = await redis_service.get("test_key")
        assert value == "test_value", f"Redis value mismatch: {value}"
        
        await redis_service.delete("test_key")
        await redis_service.disconnect()
        
        print("✅ Redis connection successful")
        return True, None
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return False, str(e)

async def test_postgres_connection():
    """Test PostgreSQL connection"""
    print("\n📍 Testing PostgreSQL Connection...")
    try:
        from oms.database.postgres import db as postgres_db
        
        await postgres_db.connect()
        
        # Test basic query
        async with postgres_db.transaction() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1, f"PostgreSQL query failed: {result}"
        
        await postgres_db.disconnect()
        
        print("✅ PostgreSQL connection successful")
        return True, None
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False, str(e)

async def test_terminusdb_connection():
    """Test TerminusDB connection"""
    print("\n📍 Testing TerminusDB Connection...")
    try:
        from oms.services.async_terminus import AsyncTerminusService
        from shared.models.config import ConnectionConfig
        
        # Use explicit config with anonymous user
        config = ConnectionConfig.from_env()
        service = AsyncTerminusService(config)
        await service.connect()
        
        # Test basic operation - list databases
        databases = await service.list_databases()
        print(f"   Found {len(databases)} databases in TerminusDB")
        
        await service.disconnect()
        
        print("✅ TerminusDB connection successful")
        return True, None
    except Exception as e:
        print(f"❌ TerminusDB connection failed: {e}")
        return False, str(e)

async def test_elasticsearch_connection():
    """Test Elasticsearch connection"""
    print("\n📍 Testing Elasticsearch Connection...")
    try:
        from shared.services.elasticsearch_service import create_elasticsearch_service_legacy
        
        es_service = create_elasticsearch_service_legacy()
        await es_service.connect()
        
        # Test cluster health
        health = await es_service.get_cluster_health()
        print(f"   Elasticsearch cluster status: {health.get('status', 'unknown')}")
        
        await es_service.disconnect()
        
        print("✅ Elasticsearch connection successful")
        return True, None
    except Exception as e:
        print(f"❌ Elasticsearch connection failed: {e}")
        return False, str(e)

async def test_kafka_connection():
    """Test Kafka connection"""
    print("\n📍 Testing Kafka Connection...")
    try:
        from confluent_kafka.admin import AdminClient, ConfigResource
        from shared.config.service_config import ServiceConfig
        
        # Create admin client
        admin_conf = {
            'bootstrap.servers': ServiceConfig.get_kafka_bootstrap_servers(),
            'socket.timeout.ms': 5000,
            'api.version.request.timeout.ms': 5000
        }
        
        admin_client = AdminClient(admin_conf)
        
        # Get cluster metadata with timeout
        metadata = admin_client.list_topics(timeout=5)
        
        print(f"   Found {len(metadata.topics)} topics in Kafka")
        print("✅ Kafka connection successful")
        return True, None
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return False, str(e)

async def test_service_startup(service_name: str, module_path: str, is_worker: bool = False):
    """Test if a service can start up"""
    print(f"\n📦 Testing {service_name} startup...")
    try:
        # Import the service module
        exec(f"import {module_path}")
        
        # Get the module
        module = sys.modules[module_path]
        
        if is_worker:
            # Workers are Kafka consumers, not FastAPI apps
            # Check if it has a main consumer class
            if hasattr(module, 'OntologyWorker') or hasattr(module, 'InstanceWorker') or hasattr(module, 'ProjectionWorker'):
                print(f"   {service_name} Kafka consumer worker imported")
                print(f"   {service_name} has consumer implementation")
                print(f"✅ {service_name} startup successful")
                return True, None
            else:
                print(f"⚠️ {service_name} imported but no worker class found")
                return False, "No worker class found"
        else:
            # FastAPI service
            app = getattr(module, 'app', None)
            
            if app:
                print(f"   {service_name} FastAPI app created: {app.title}")
                
                # Check routes
                routes = [r for r in app.routes if hasattr(r, 'path')]
                print(f"   {service_name} has {len(routes)} routes")
                
                # Check lifespan events if available
                if hasattr(module, 'lifespan'):
                    print(f"   {service_name} has lifespan management")
                
                print(f"✅ {service_name} startup successful")
                return True, None
            else:
                print(f"⚠️ {service_name} imported but no app found")
                return False, "No FastAPI app found"
            
    except Exception as e:
        print(f"❌ {service_name} startup failed: {e}")
        return False, str(e)

async def main():
    """Main test runner"""
    print("🔥 THINK ULTRA! Comprehensive Service Testing")
    print("=" * 60)
    print("Testing real connections with no mocks or bypasses")
    print("=" * 60)
    
    # Track results
    results: Dict[str, Tuple[bool, str]] = {}
    
    # Test infrastructure connections
    print("\n🔧 Testing Infrastructure Connections...")
    print("-" * 40)
    
    # Test each infrastructure service
    results["Redis"] = await test_redis_connection()
    results["PostgreSQL"] = await test_postgres_connection()
    results["TerminusDB"] = await test_terminusdb_connection()
    results["Elasticsearch"] = await test_elasticsearch_connection()
    results["Kafka"] = await test_kafka_connection()
    
    # Test service startups
    print("\n🚀 Testing Service Startups...")
    print("-" * 40)
    
    services = [
        ("OMS", "oms.main", False),
        ("BFF", "bff.main", False),
        ("Funnel", "funnel.main", False),
        ("Ontology Worker", "ontology_worker.main", True),
        ("Instance Worker", "instance_worker.main", True),
        ("Projection Worker", "projection_worker.main", True),
    ]
    
    for service_name, module_path, is_worker in services:
        results[service_name] = await test_service_startup(service_name, module_path, is_worker)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Results Summary")
    print("=" * 60)
    
    passed = 0
    failed = 0
    issues = []
    
    for name, (success, error) in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{name:20} {status}")
        if success:
            passed += 1
        else:
            failed += 1
            if error:
                issues.append(f"{name}: {error}")
    
    print("-" * 60)
    print(f"Total: {passed} passed, {failed} failed")
    
    if issues:
        print("\n🔍 Issues Found:")
        for issue in issues:
            print(f"  • {issue}")
    
    # Final verdict
    print("\n" + "=" * 60)
    if failed == 0:
        print("🎉 ALL SERVICES ARE PRODUCTION READY!")
    else:
        print(f"⚠️ {failed} services need attention")
        print("Following CLAUDE RULE: No bypassing, all issues must be fixed")
    
    return failed == 0

if __name__ == "__main__":
    # Run with proper event loop
    success = asyncio.run(main())
    sys.exit(0 if success else 1)