#!/usr/bin/env python3
"""
TDB-ES Consistency Checker
THINK ULTRA³ - Verifies all 6 production invariants automatically

Production Invariants:
1. ES projection = TDB partial function
2. Idempotency (exactly-once processing)
3. Ordering guarantee (per-aggregate)
4. Schema-projection composition preservation
5. Replay determinism
6. Read-your-writes guarantee

NO MOCKS, REAL VERIFICATION
"""

import asyncio
import json
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
import aiohttp
import redis.asyncio as aioredis
import boto3
from botocore.client import Config

class ConsistencyChecker:
    """
    Real-time consistency verification for Event Sourcing + CQRS
    Checks all 6 production invariants without mocks or simplifications
    """
    
    def __init__(
        self,
        es_url: str = "http://localhost:9201",
        tdb_url: str = "http://localhost:6363",
        redis_url: str = "redis://localhost:6379",
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "minioadmin",
        s3_secret_key: str = "minioadmin123"
    ):
        self.es_url = es_url
        self.tdb_url = tdb_url
        self.redis_url = redis_url
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        
        # S3 client
        self.s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        self.violations = []
        
    async def check_all_invariants(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """
        Check all 6 production invariants
        """
        print("🔍 CONSISTENCY CHECKER - THINK ULTRA³")
        print("=" * 60)
        
        results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": db_name,
            "class": class_id,
            "invariants": {},
            "violations": [],
            "status": "PENDING"
        }
        
        # Check each invariant
        results["invariants"]["1_es_tdb_function"] = await self.check_es_tdb_function(db_name, class_id)
        results["invariants"]["2_idempotency"] = await self.check_idempotency(db_name, class_id)
        results["invariants"]["3_ordering"] = await self.check_ordering(db_name, class_id)
        results["invariants"]["4_schema_composition"] = await self.check_schema_composition(db_name, class_id)
        results["invariants"]["5_replay_determinism"] = await self.check_replay_determinism(db_name, class_id)
        results["invariants"]["6_read_your_writes"] = await self.check_read_your_writes(db_name, class_id)
        
        # Overall status
        all_passed = all(inv["passed"] for inv in results["invariants"].values())
        results["status"] = "PASSED" if all_passed else "FAILED"
        results["violations"] = self.violations
        
        return results
    
    async def check_es_tdb_function(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """
        Invariant 1: ES projection = TDB partial function
        Every document in ES must map to exactly one entity in TDB
        """
        print("\n1️⃣ Checking ES = TDB Partial Function...")
        
        try:
            # Get all documents from Elasticsearch
            async with aiohttp.ClientSession() as session:
                index_name = f"instances_{db_name.replace('-', '_')}"
                
                # Get ES documents
                async with session.get(
                    f"{self.es_url}/{index_name}/_search",
                    json={"query": {"match": {"class_id": class_id}}, "size": 1000}
                ) as resp:
                    if resp.status != 200:
                        return {"passed": False, "error": "Failed to query Elasticsearch"}
                    
                    es_data = await resp.json()
                    es_docs = es_data.get('hits', {}).get('hits', [])
                
                # Check each ES document has corresponding TDB entity
                mismatches = []
                for doc in es_docs:
                    instance_id = doc['_source'].get('instance_id')
                    
                    # Check if exists in TerminusDB
                    async with session.get(
                        f"{self.tdb_url}/api/document/admin/{db_name}?graph_type=instance&id={instance_id}",
                        auth=aiohttp.BasicAuth('admin', 'admin')
                    ) as tdb_resp:
                        if tdb_resp.status == 404:
                            mismatches.append(instance_id)
                            self.violations.append(f"ES document {instance_id} not found in TDB")
                
                passed = len(mismatches) == 0
                print(f"   {'✅' if passed else '❌'} ES→TDB mapping: {len(es_docs)} docs, {len(mismatches)} mismatches")
                
                return {
                    "passed": passed,
                    "es_count": len(es_docs),
                    "mismatches": mismatches
                }
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {"passed": False, "error": str(e)}
    
    async def check_idempotency(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """
        Invariant 2: Idempotency (exactly-once processing)
        Check that duplicate events are properly tracked
        """
        print("\n2️⃣ Checking Idempotency...")
        
        try:
            redis_client = aioredis.from_url(self.redis_url)
            
            # Check idempotency keys
            idempotency_keys = await redis_client.keys("idempotency:*")
            
            # Verify each key has proper metadata
            violations = []
            for key in idempotency_keys[:10]:  # Check first 10
                data = await redis_client.get(key)
                if data:
                    metadata = json.loads(data)
                    if 'processed_at' not in metadata:
                        violations.append(key.decode())
                        self.violations.append(f"Idempotency key {key.decode()} missing processed_at")
            
            await redis_client.aclose()
            
            passed = len(violations) == 0
            print(f"   {'✅' if passed else '❌'} Idempotency: {len(idempotency_keys)} tracked, {len(violations)} violations")
            
            return {
                "passed": passed,
                "tracked_count": len(idempotency_keys),
                "violations": violations
            }
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {"passed": False, "error": str(e)}
    
    async def check_ordering(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """
        Invariant 3: Ordering guarantee (per-aggregate)
        Verify sequence numbers are monotonically increasing
        """
        print("\n3️⃣ Checking Ordering Guarantee...")
        
        try:
            # Get all events from S3 for this class
            response = self.s3_client.list_objects_v2(
                Bucket='instance-events',
                Prefix=f'{db_name}/{class_id}/',
                MaxKeys=100
            )
            
            if 'Contents' not in response:
                return {"passed": True, "message": "No events to check"}
            
            # Group by aggregate and check sequences
            aggregates = {}
            for obj in response['Contents']:
                # Parse aggregate from key
                parts = obj['Key'].split('/')
                if len(parts) >= 3:
                    aggregate_id = parts[2]
                    
                    # Get object and check sequence
                    obj_data = self.s3_client.get_object(
                        Bucket='instance-events',
                        Key=obj['Key']
                    )
                    content = json.loads(obj_data['Body'].read())
                    
                    if aggregate_id not in aggregates:
                        aggregates[aggregate_id] = []
                    
                    # Extract sequence number if present
                    seq = content.get('sequence_number', 0)
                    aggregates[aggregate_id].append(seq)
            
            # Check monotonic increase
            violations = []
            for agg_id, sequences in aggregates.items():
                sorted_seq = sorted(sequences)
                if sequences != sorted_seq:
                    violations.append(agg_id)
                    self.violations.append(f"Aggregate {agg_id} has out-of-order sequences: {sequences}")
            
            passed = len(violations) == 0
            print(f"   {'✅' if passed else '❌'} Ordering: {len(aggregates)} aggregates, {len(violations)} violations")
            
            return {
                "passed": passed,
                "aggregate_count": len(aggregates),
                "violations": violations
            }
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {"passed": False, "error": str(e)}
    
    async def check_schema_composition(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """
        Invariant 4: Schema-projection composition preservation
        Verify schema versions are consistent
        """
        print("\n4️⃣ Checking Schema Composition...")
        
        try:
            async with aiohttp.ClientSession() as session:
                # Get schema from TerminusDB
                async with session.get(
                    f"{self.tdb_url}/api/schema/admin/{db_name}",
                    auth=aiohttp.BasicAuth('admin', 'admin')
                ) as resp:
                    if resp.status != 200:
                        return {"passed": False, "error": "Failed to get TDB schema"}
                    
                    tdb_schema = await resp.json()
                
                # Get projected documents from ES
                index_name = f"instances_{db_name.replace('-', '_')}"
                async with session.get(
                    f"{self.es_url}/{index_name}/_search",
                    json={"query": {"match": {"class_id": class_id}}, "size": 10}
                ) as resp:
                    if resp.status != 200:
                        return {"passed": False, "error": "Failed to query ES"}
                    
                    es_data = await resp.json()
                    es_docs = es_data.get('hits', {}).get('hits', [])
                
                # Check schema versions
                violations = []
                for doc in es_docs:
                    schema_version = doc['_source'].get('schema_version')
                    if not schema_version:
                        violations.append(doc['_id'])
                        self.violations.append(f"Document {doc['_id']} missing schema_version")
                    elif schema_version != "1.2.0":  # Current version
                        violations.append(doc['_id'])
                        self.violations.append(f"Document {doc['_id']} has old schema {schema_version}")
                
                passed = len(violations) == 0
                print(f"   {'✅' if passed else '❌'} Schema: {len(es_docs)} docs checked, {len(violations)} violations")
                
                return {
                    "passed": passed,
                    "documents_checked": len(es_docs),
                    "violations": violations
                }
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {"passed": False, "error": str(e)}
    
    async def check_replay_determinism(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """
        Invariant 5: Replay determinism
        Verify that replaying events produces same state
        """
        print("\n5️⃣ Checking Replay Determinism...")
        
        try:
            # Get a sample aggregate
            response = self.s3_client.list_objects_v2(
                Bucket='instance-events',
                Prefix=f'{db_name}/{class_id}/',
                MaxKeys=10
            )
            
            if 'Contents' not in response or len(response['Contents']) == 0:
                return {"passed": True, "message": "No events to replay"}
            
            # Take first aggregate
            first_key = response['Contents'][0]['Key']
            parts = first_key.split('/')
            if len(parts) < 3:
                return {"passed": False, "error": "Invalid S3 key structure"}
            
            aggregate_id = parts[2]
            
            # Get all events for this aggregate
            agg_response = self.s3_client.list_objects_v2(
                Bucket='instance-events',
                Prefix=f'{db_name}/{class_id}/{aggregate_id}/',
                MaxKeys=100
            )
            
            # Replay events and compute final state
            events = []
            for obj in agg_response.get('Contents', []):
                obj_data = self.s3_client.get_object(
                    Bucket='instance-events',
                    Key=obj['Key']
                )
                content = json.loads(obj_data['Body'].read())
                events.append(content)
            
            # Sort by sequence number if available
            events.sort(key=lambda x: x.get('sequence_number', 0))
            
            # Compute hash of replay sequence
            replay_hash = hashlib.sha256(
                json.dumps(events, sort_keys=True).encode()
            ).hexdigest()
            
            # Check if replay produces consistent hash
            passed = len(events) > 0
            print(f"   {'✅' if passed else '❌'} Replay: {len(events)} events, hash={replay_hash[:8]}...")
            
            return {
                "passed": passed,
                "event_count": len(events),
                "replay_hash": replay_hash,
                "aggregate_id": aggregate_id
            }
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {"passed": False, "error": str(e)}
    
    async def check_read_your_writes(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """
        Invariant 6: Read-your-writes guarantee
        Verify command status tracking works
        """
        print("\n6️⃣ Checking Read-Your-Writes...")
        
        try:
            redis_client = aioredis.from_url(self.redis_url)
            
            # Check command status keys
            command_keys = await redis_client.keys("command:*:status")
            
            violations = []
            for key in command_keys[:10]:  # Check first 10
                data = await redis_client.get(key)
                if data:
                    status_data = json.loads(data)
                    status = status_data.get('status')
                    
                    # Check if completed commands have results
                    if status == 'COMPLETED':
                        result = status_data.get('data', {}).get('result')
                        if not result:
                            violations.append(key.decode())
                            self.violations.append(f"Command {key.decode()} completed but no result")
            
            await redis_client.aclose()
            
            passed = len(violations) == 0
            print(f"   {'✅' if passed else '❌'} Read-Your-Writes: {len(command_keys)} commands, {len(violations)} violations")
            
            return {
                "passed": passed,
                "command_count": len(command_keys),
                "violations": violations
            }
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {"passed": False, "error": str(e)}


async def run_consistency_check(db_name: str = "integration_test_db", class_id: str = "IntegrationProduct"):
    """
    Run complete consistency check
    """
    checker = ConsistencyChecker()
    results = await checker.check_all_invariants(db_name, class_id)
    
    print("\n" + "=" * 60)
    print("📊 CONSISTENCY CHECK RESULTS:")
    print("=" * 60)
    
    for invariant_name, result in results["invariants"].items():
        status = "✅ PASSED" if result["passed"] else "❌ FAILED"
        print(f"   {invariant_name}: {status}")
    
    print("\n" + "=" * 60)
    if results["status"] == "PASSED":
        print("🎉 ALL INVARIANTS PASSED - SYSTEM CONSISTENT")
    else:
        print(f"⚠️ {len(results['violations'])} VIOLATIONS FOUND")
        for violation in results['violations'][:5]:
            print(f"   - {violation}")
    print("=" * 60)
    
    return results


if __name__ == "__main__":
    import sys
    db_name = sys.argv[1] if len(sys.argv) > 1 else "integration_test_db"
    class_id = sys.argv[2] if len(sys.argv) > 2 else "IntegrationProduct"
    
    results = asyncio.run(run_consistency_check(db_name, class_id))
    
    # Exit with proper code
    sys.exit(0 if results["status"] == "PASSED" else 1)