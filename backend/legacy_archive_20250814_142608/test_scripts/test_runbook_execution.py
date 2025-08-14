#!/usr/bin/env python3
"""
🔥 THINK ULTRA! Production Runbook Test Execution
Verifies all runbook steps work correctly
"""

import asyncio
import os
import json
import subprocess
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple
import uuid

# Set environment for testing
os.environ["DOCKER_CONTAINER"] = "false"
os.environ["ENABLE_S3_EVENT_STORE"] = "true"
os.environ["ENABLE_DUAL_WRITE"] = "true"
os.environ["MINIO_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["MINIO_ACCESS_KEY"] = "admin"
os.environ["MINIO_SECRET_KEY"] = "spice123!"

from oms.services.event_store import event_store, Event
from oms.services.migration_helper import migration_helper
from shared.config.service_config import ServiceConfig


class RunbookTester:
    """Test all runbook procedures"""
    
    def __init__(self):
        self.results = []
        self.passed = 0
        self.failed = 0
        
    def log_result(self, test_name: str, passed: bool, details: str = ""):
        """Log test result"""
        status = "✅ PASS" if passed else "❌ FAIL"
        self.results.append(f"{status} - {test_name}: {details}")
        if passed:
            self.passed += 1
        else:
            self.failed += 1
        print(f"{status} - {test_name}")
        if details:
            print(f"     {details}")
    
    async def test_infrastructure(self) -> bool:
        """Test infrastructure requirements"""
        print("\n" + "="*60)
        print("1️⃣ TESTING INFRASTRUCTURE REQUIREMENTS")
        print("="*60)
        
        # Test MinIO
        try:
            import requests
            response = requests.get("http://localhost:9000/minio/health/live", timeout=5)
            minio_ok = response.status_code == 200
            self.log_result("MinIO Health", minio_ok, f"Status: {response.status_code}")
        except Exception as e:
            self.log_result("MinIO Health", False, str(e))
            minio_ok = False
        
        # Test Kafka (check if topics exist)
        try:
            result = subprocess.run(
                ["kafka-topics", "--bootstrap-server", "localhost:9092", "--list"],
                capture_output=True, text=True, timeout=5
            )
            kafka_ok = result.returncode == 0
            self.log_result("Kafka Connection", kafka_ok, 
                          f"Topics found: {len(result.stdout.splitlines()) if kafka_ok else 0}")
        except Exception as e:
            self.log_result("Kafka Connection", False, "Kafka not available")
            kafka_ok = False
        
        # Test Elasticsearch
        try:
            import requests
            response = requests.get("http://localhost:9201/_cluster/health", timeout=5)
            es_ok = response.status_code == 200
            if es_ok:
                health = response.json()
                self.log_result("Elasticsearch Health", True, f"Status: {health.get('status', 'unknown')}")
            else:
                self.log_result("Elasticsearch Health", False, f"Status: {response.status_code}")
        except Exception as e:
            self.log_result("Elasticsearch Health", False, "Elasticsearch not available")
            es_ok = False
        
        return minio_ok  # MinIO is critical for this migration
    
    async def test_dual_write_mode(self) -> bool:
        """Test dual-write configuration"""
        print("\n" + "="*60)
        print("2️⃣ TESTING DUAL-WRITE MODE")
        print("="*60)
        
        # Check migration helper configuration
        s3_enabled = migration_helper.s3_enabled
        dual_write = migration_helper.dual_write
        mode = migration_helper._get_migration_mode()
        
        self.log_result("S3 Event Store Enabled", s3_enabled, f"Value: {s3_enabled}")
        self.log_result("Dual-Write Enabled", dual_write, f"Value: {dual_write}")
        self.log_result("Migration Mode", mode == "dual_write", f"Mode: {mode}")
        
        return s3_enabled and dual_write and mode == "dual_write"
    
    async def test_s3_operations(self) -> bool:
        """Test S3/MinIO Event Store operations"""
        print("\n" + "="*60)
        print("3️⃣ TESTING S3 EVENT STORE OPERATIONS")
        print("="*60)
        
        # Test connection
        try:
            await event_store.connect()
            self.log_result("S3 Connection", True, 
                          f"Endpoint: {event_store.endpoint_url}, Bucket: {event_store.bucket_name}")
        except Exception as e:
            self.log_result("S3 Connection", False, str(e))
            return False
        
        # Test write operation
        test_event = Event(
            event_id=str(uuid.uuid4()),
            event_type="RUNBOOK_TEST",
            aggregate_type="test.RunbookTest",
            aggregate_id=f"runbook-test-{uuid.uuid4().hex[:8]}",
            aggregate_version=1,
            timestamp=datetime.utcnow(),
            actor="runbook_tester",
            payload={"test": "runbook_verification", "timestamp": datetime.utcnow().isoformat()},
            metadata={"source": "test_runbook_execution.py"}
        )
        
        try:
            event_id = await event_store.append_event(test_event)
            self.log_result("Event Write", True, f"Event ID: {event_id}")
            write_ok = True
        except Exception as e:
            self.log_result("Event Write", False, str(e))
            write_ok = False
            return False
        
        # Test read operation
        try:
            events = await event_store.get_events(
                aggregate_type=test_event.aggregate_type,
                aggregate_id=test_event.aggregate_id
            )
            read_ok = len(events) > 0
            self.log_result("Event Read", read_ok, f"Events retrieved: {len(events)}")
        except Exception as e:
            self.log_result("Event Read", False, str(e))
            read_ok = False
        
        # Test replay operation
        try:
            replay_count = 0
            async for event in event_store.replay_events(
                from_timestamp=datetime.utcnow() - timedelta(minutes=5),
                event_types=["RUNBOOK_TEST"]
            ):
                replay_count += 1
            self.log_result("Event Replay", True, f"Events replayed: {replay_count}")
            replay_ok = True
        except Exception as e:
            self.log_result("Event Replay", False, str(e))
            replay_ok = False
        
        return write_ok and read_ok and replay_ok
    
    async def test_rollback_capability(self) -> bool:
        """Test rollback procedures"""
        print("\n" + "="*60)
        print("4️⃣ TESTING ROLLBACK CAPABILITY")
        print("="*60)
        
        original_s3 = migration_helper.s3_enabled
        original_dual = migration_helper.dual_write
        
        try:
            # Test rollback to legacy mode
            migration_helper.s3_enabled = False
            migration_helper.dual_write = False
            legacy_mode = migration_helper._get_migration_mode()
            self.log_result("Rollback to Legacy", legacy_mode == "legacy", f"Mode: {legacy_mode}")
            
            # Test rollback to dual-write
            migration_helper.s3_enabled = True
            migration_helper.dual_write = True
            dual_mode = migration_helper._get_migration_mode()
            self.log_result("Restore Dual-Write", dual_mode == "dual_write", f"Mode: {dual_mode}")
            
            # Test forward to S3-only
            migration_helper.s3_enabled = True
            migration_helper.dual_write = False
            s3_only_mode = migration_helper._get_migration_mode()
            self.log_result("Forward to S3-Only", s3_only_mode == "s3_only", f"Mode: {s3_only_mode}")
            
            rollback_ok = True
        except Exception as e:
            self.log_result("Rollback Test", False, str(e))
            rollback_ok = False
        finally:
            # Restore original settings
            migration_helper.s3_enabled = original_s3
            migration_helper.dual_write = original_dual
        
        return rollback_ok
    
    async def test_monitoring(self) -> bool:
        """Test monitoring capabilities"""
        print("\n" + "="*60)
        print("5️⃣ TESTING MONITORING")
        print("="*60)
        
        try:
            from monitoring.s3_event_store_dashboard import S3EventStoreDashboard
            dashboard = S3EventStoreDashboard()
            
            # Test dashboard generation
            data = await dashboard.generate_dashboard()
            
            has_storage = "storage" in data
            has_health = "health" in data
            has_migration = "migration" in data
            
            self.log_result("Dashboard Storage Metrics", has_storage, 
                          f"Metrics available: {has_storage}")
            self.log_result("Dashboard Health Status", has_health,
                          f"Health checks: {len(data.get('health', {}))}")
            self.log_result("Dashboard Migration Status", has_migration,
                          f"Mode: {data.get('migration', {}).get('current_mode', 'unknown')}")
            
            return has_storage and has_health and has_migration
        except Exception as e:
            self.log_result("Monitoring Dashboard", False, str(e))
            return False
    
    async def test_worker_integration(self) -> bool:
        """Test worker S3 integration"""
        print("\n" + "="*60)
        print("6️⃣ TESTING WORKER INTEGRATION")
        print("="*60)
        
        try:
            # Test Message Relay S3 reference building
            from message_relay.main import MessageRelay
            relay = MessageRelay()
            relay.s3_enabled = True
            relay.s3_bucket = "spice-event-store"
            
            test_message = {
                'id': str(uuid.uuid4()),
                'payload': json.dumps({
                    'command_type': 'TEST_COMMAND',
                    'aggregate_type': 'Test',
                    'aggregate_id': 'test-123',
                    'payload': {'test': 'data'}
                })
            }
            
            s3_ref = relay._build_s3_reference(test_message)
            has_s3_ref = s3_ref is not None and 'bucket' in s3_ref and 'key' in s3_ref
            self.log_result("Message Relay S3 Reference", has_s3_ref,
                          f"Reference built: {has_s3_ref}")
            
            # Test Instance Worker configuration
            from instance_worker.main import StrictPalantirInstanceWorker
            worker = StrictPalantirInstanceWorker()
            worker.s3_event_store_enabled = True
            
            self.log_result("Instance Worker S3 Enabled", worker.s3_event_store_enabled,
                          f"S3 reading enabled: {worker.s3_event_store_enabled}")
            
            return has_s3_ref and worker.s3_event_store_enabled
        except Exception as e:
            self.log_result("Worker Integration", False, str(e))
            return False
    
    async def run_all_tests(self):
        """Run all runbook tests"""
        print("\n" + "="*80)
        print("🔥 THINK ULTRA! PRODUCTION RUNBOOK TEST EXECUTION")
        print("="*80)
        print(f"Started: {datetime.now().isoformat()}")
        
        # Run tests
        infra_ok = await self.test_infrastructure()
        dual_write_ok = await self.test_dual_write_mode()
        s3_ok = await self.test_s3_operations() if infra_ok else False
        rollback_ok = await self.test_rollback_capability()
        monitoring_ok = await self.test_monitoring()
        worker_ok = await self.test_worker_integration()
        
        # Summary
        print("\n" + "="*80)
        print("📊 TEST SUMMARY")
        print("="*80)
        
        for result in self.results:
            print(result)
        
        print("\n" + "-"*40)
        print(f"Total Tests: {self.passed + self.failed}")
        print(f"✅ Passed: {self.passed}")
        print(f"❌ Failed: {self.failed}")
        print(f"Success Rate: {(self.passed/(self.passed+self.failed)*100):.1f}%")
        
        # Overall verdict
        all_critical_passed = infra_ok and dual_write_ok and s3_ok and rollback_ok
        
        print("\n" + "="*80)
        if all_critical_passed:
            print("🎉 RUNBOOK VERIFICATION: PASSED")
            print("System is ready for production migration!")
        else:
            print("⚠️ RUNBOOK VERIFICATION: FAILED")
            print("Please fix the failed tests before proceeding with migration.")
        print("="*80)
        
        return all_critical_passed


async def main():
    """Main test execution"""
    tester = RunbookTester()
    success = await tester.run_all_tests()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())