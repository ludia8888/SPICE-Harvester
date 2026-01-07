#!/usr/bin/env python3
"""
🔥 Quick production test - verify Event Sourcing still works after debug cleanup
"""

import asyncio
import aiohttp
import os
import sys
from datetime import datetime

async def quick_production_test():
    """Quick test to verify production system works"""
    
    print("🔥 QUICK PRODUCTION TEST - POST DEBUG CLEANUP")
    print("=" * 50)
    
    test_db = f"prod_test_{datetime.now().strftime('%H%M%S')}"
    test_class = "production_test"
    admin_token = (os.getenv("ADMIN_TOKEN") or os.getenv("BFF_ADMIN_TOKEN") or "test-token").strip()
    if not admin_token:
        raise RuntimeError("ADMIN_TOKEN is required for production test")
    headers = {"X-Admin-Token": admin_token}
    
    async with aiohttp.ClientSession() as session:
        try:
            # 1. Create database
            print(f"1️⃣ Creating database: {test_db}")
            async with session.post(
                "http://localhost:8000/api/v1/database/create",
                json={"name": test_db, "description": "Production Test"},
                headers=headers,
            ) as resp:
                print(f"   Status: {resp.status}")
                assert resp.status == 202
                print("   ✅ Database creation accepted")
                
            # Wait for database to actually exist
            print(f"1.5️⃣ Waiting for database to be available...")
            for i in range(15):
                await asyncio.sleep(1)
                async with session.get(
                    f"http://localhost:8000/api/v1/database/exists/{test_db}",
                    headers=headers,
                ) as check_resp:
                    result = await check_resp.json()
                    if result.get('data', {}).get('exists'):
                        print(f"   ✅ Database confirmed after {i+1} seconds")
                        break
            else:
                raise Exception("Database not created in time")
                
            # 2. Create ontology
            print(f"2️⃣ Creating ontology: {test_class}")
            ontology_data = {
                "id": test_class,
                "label": "Production Test Class",
                "description": "Verify production system works",
                "properties": [
                    {
                        "name": "production_test_id",
                        "label": "Production Test ID",
                        "type": "string",
                        "required": True,
                        "primaryKey": True,
                    },
                    {"name": "test_field", "label": "Test Field", "type": "string", "required": True},
                    {"name": "score", "label": "Score", "type": "DECIMAL", "required": True},
                ]
            }
            
            async with session.post(
                f"http://localhost:8000/api/v1/database/{test_db}/ontology",
                json=ontology_data,
                headers=headers,
            ) as resp:
                print(f"   Status: {resp.status}")
                if resp.status != 202:
                    error_text = await resp.text()
                    print(f"   Error response: {error_text}")
                    raise Exception(f"Expected 202, got {resp.status}")
                print("   ✅ Ontology creation accepted")
                
            print(f"3️⃣ Waiting for processing...")
            await asyncio.sleep(8)  # Wait for processing
            
            print(f"4️⃣ Cleaning up...")
            expected_seq = 0
            cleanup_ok = False
            for _ in range(2):
                async with session.delete(
                    f"http://localhost:8000/api/v1/database/{test_db}",
                    params={"expected_seq": expected_seq},
                    headers=headers,
                ) as resp:
                    if resp.status in (200, 202):
                        cleanup_ok = True
                        print(f"   Cleanup status: {resp.status}")
                        break
                    if resp.status == 409:
                        try:
                            payload = await resp.json()
                        except Exception:
                            payload = {}
                        detail = payload.get("detail") if isinstance(payload, dict) else None
                        if isinstance(detail, dict):
                            expected_seq = detail.get("actual_seq") or detail.get("expected_seq") or expected_seq
                            continue
                    error_text = await resp.text()
                    raise Exception(f"Cleanup failed ({resp.status}): {error_text}")

            if not cleanup_ok:
                raise Exception("Cleanup failed after retry")
                
            print("✅ PRODUCTION SYSTEM WORKS PERFECTLY!")
            return True
            
        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False

if __name__ == "__main__":
    success = asyncio.run(quick_production_test())
    print("🎉 PRODUCTION READY!" if success else "❌ NEEDS ATTENTION")
    sys.exit(0 if success else 1)
